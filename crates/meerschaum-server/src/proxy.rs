//! The extension-host reverse proxy (§7.3): every request no Rust route matches is forwarded to
//! the Python API host over a Unix socket with its method, path, query, headers, cookies and
//! body unchanged. Bodies stream in both directions, and `Upgrade` requests (websockets) are
//! bridged byte-for-byte after the host answers `101 Switching Protocols`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderName, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use hyper_util::rt::TokioIo;

use crate::error::ApiError;
use crate::host::HostSupervisor;

/// Hop-by-hop headers (RFC 9110 §7.6.1), dropped on both legs unless the request upgrades.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// The connection to the API host: a Unix-domain stream.
#[cfg(unix)]
pub type HostStream = tokio::net::UnixStream;

/// The connection to the API host (Unix sockets only for now; see [`connect_host`]).
#[cfg(not(unix))]
pub type HostStream = tokio::net::TcpStream;

/// Connect to the API host's socket.
#[cfg(unix)]
pub async fn connect_host(socket: &Path) -> std::io::Result<HostStream> {
    tokio::net::UnixStream::connect(socket).await
}

/// Connect to the API host's socket. The host listens on a Unix socket, which this platform's
/// build does not support yet, so this always fails (the server then answers `502` for
/// unmatched routes, and MCP federation reports the host as unavailable).
#[cfg(not(unix))]
pub async fn connect_host(_socket: &Path) -> std::io::Result<HostStream> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "the Python API host is not supported on this platform yet (it needs Unix sockets)",
    ))
}

/// Forwards unmatched requests to the API host's Unix socket.
#[derive(Clone)]
pub struct HostProxy {
    socket: PathBuf,
    supervisor: Option<Arc<HostSupervisor>>,
}

impl std::fmt::Debug for HostProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostProxy")
            .field("socket", &self.socket)
            .finish()
    }
}

fn strip_hop_by_hop(headers: &mut HeaderMap, keep_upgrade: bool) {
    // Headers named by `Connection` are hop-by-hop too.
    let named: Vec<HeaderName> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|n| HeaderName::from_bytes(n.trim().as_bytes()).ok())
        .collect();
    for name in HOP_BY_HOP {
        if keep_upgrade && (*name == "upgrade" || *name == "connection") {
            continue;
        }
        headers.remove(*name);
    }
    if !keep_upgrade {
        for n in named {
            headers.remove(n);
        }
    }
}

fn bad_gateway(detail: &str) -> Response {
    let mut resp = ApiError::new(502, detail).into_response();
    *resp.status_mut() = StatusCode::BAD_GATEWAY;
    resp
}

impl HostProxy {
    /// A proxy to a host already listening on `socket`.
    pub fn new(socket: impl Into<PathBuf>) -> HostProxy {
        HostProxy {
            socket: socket.into(),
            supervisor: None,
        }
    }

    /// A proxy that starts (and supervises) the host on the first unmatched request.
    pub fn supervised(supervisor: Arc<HostSupervisor>) -> HostProxy {
        HostProxy {
            socket: supervisor.socket().to_path_buf(),
            supervisor: Some(supervisor),
        }
    }

    /// Whether requests may be forwarded: always for an unsupervised proxy, and while the
    /// manifest says a plugin needs the host for a supervised one (else the host never starts
    /// and unmatched routes answer `404`).
    pub fn is_active(&self) -> bool {
        self.supervisor.as_ref().is_none_or(|s| s.is_needed())
    }

    /// The supervisor, when the proxy starts the host on demand.
    pub fn supervisor(&self) -> Option<&Arc<HostSupervisor>> {
        self.supervisor.as_ref()
    }

    /// The host's socket.
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Forward a request to the host and return its response.
    pub async fn forward(&self, mut req: Request) -> Response {
        if let Some(sup) = &self.supervisor {
            if let Err(e) = sup.ensure_started().await {
                tracing::warn!("The API host is unavailable: {e}");
                return bad_gateway("The API host is unavailable.");
            }
        }
        let stream = match connect_host(&self.socket).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("Failed to connect to the API host: {e}");
                return bad_gateway("The API host is unavailable.");
            }
        };
        let (mut sender, conn) =
            match hyper::client::conn::http1::handshake(TokioIo::new(stream)).await {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!("Handshake with the API host failed: {e}");
                    return bad_gateway("The API host is unavailable.");
                }
            };
        tokio::spawn(async move {
            if let Err(e) = conn.with_upgrades().await {
                tracing::debug!("API host connection closed: {e}");
            }
        });

        let is_upgrade = req.headers().contains_key(header::UPGRADE);
        let client_upgrade = is_upgrade.then(|| hyper::upgrade::on(&mut req));
        let (mut parts, body) = req.into_parts();
        let path_and_query = parts
            .uri
            .path_and_query()
            .map(|p| p.as_str().to_string())
            .unwrap_or_else(|| "/".into());
        parts.uri = path_and_query
            .parse::<Uri>()
            .unwrap_or_else(|_| Uri::from_static("/"));
        strip_hop_by_hop(&mut parts.headers, is_upgrade);
        if !parts.headers.contains_key(header::HOST) {
            if let Ok(v) = "localhost".parse() {
                parts.headers.insert(header::HOST, v);
            }
        }
        let upstream_req = hyper::Request::from_parts(parts, body);
        let mut resp = match sender.send_request(upstream_req).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("The API host failed to answer: {e}");
                return bad_gateway("The API host failed to answer.");
            }
        };

        if resp.status() == StatusCode::SWITCHING_PROTOCOLS {
            let Some(client_upgrade) = client_upgrade else {
                return bad_gateway("The API host switched protocols without an upgrade request.");
            };
            let host_upgrade = hyper::upgrade::on(&mut resp);
            tokio::spawn(async move {
                match tokio::try_join!(client_upgrade, host_upgrade) {
                    Ok((client, host)) => {
                        let mut client = TokioIo::new(client);
                        let mut host = TokioIo::new(host);
                        let _ = tokio::io::copy_bidirectional(&mut client, &mut host).await;
                    }
                    Err(e) => tracing::warn!("Failed to bridge an upgraded connection: {e}"),
                }
            });
            let (parts, _) = resp.into_parts();
            return Response::from_parts(parts, Body::empty());
        }

        let (mut parts, body) = resp.into_parts();
        strip_hop_by_hop(&mut parts.headers, false);
        Response::from_parts(parts, Body::new(body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hop_by_hop_headers() {
        let mut h = HeaderMap::new();
        h.insert(header::CONNECTION, "keep-alive, x-private".parse().unwrap());
        h.insert("x-private", "1".parse().unwrap());
        h.insert(header::COOKIE, "a=b".parse().unwrap());
        h.insert(header::TRANSFER_ENCODING, "chunked".parse().unwrap());
        strip_hop_by_hop(&mut h, false);
        assert!(h.get("x-private").is_none());
        assert!(h.get(header::CONNECTION).is_none());
        assert!(h.get(header::TRANSFER_ENCODING).is_none());
        assert_eq!(h.get(header::COOKIE).unwrap(), "a=b");
    }
}
