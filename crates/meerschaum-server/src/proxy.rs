//! The extension-host reverse proxy (§7.3): every request no Rust route matches is forwarded to
//! the Python API host over a Unix socket (TCP loopback with a per-start token on Windows) with
//! its method, path, query, headers, cookies and body unchanged. Bodies stream in both
//! directions, and `Upgrade` requests (websockets) are bridged byte-for-byte after the host
//! answers `101 Switching Protocols`.
//!
//! The Dash web console (D21) is the host's: [`HostProxy::console`] says whether the host mounted
//! it (`GET /_mrsm/host/status`), and `/dash/*` goes to the host only then.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderName, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use hyper_util::rt::TokioIo;

use crate::error::ApiError;
use crate::host::{ConsoleStatus, HostEndpoint, HostSupervisor, PROXY_TOKEN_HEADER, STATUS_PATH};
use std::sync::atomic::{AtomicBool, Ordering};

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

/// The connection to the API host: a Unix-domain stream, or TCP on loopback.
pub enum HostStream {
    /// A Unix-domain stream.
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    /// A TCP stream to a loopback host (the Windows transport).
    Tcp(tokio::net::TcpStream),
}

impl tokio::io::AsyncRead for HostStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            HostStream::Unix(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            HostStream::Tcp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for HostStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            #[cfg(unix)]
            HostStream::Unix(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            HostStream::Tcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            HostStream::Unix(s) => std::pin::Pin::new(s).poll_flush(cx),
            HostStream::Tcp(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            HostStream::Unix(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            HostStream::Tcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Connect to the API host's Unix socket.
#[cfg(unix)]
pub async fn connect_host(socket: &Path) -> std::io::Result<HostStream> {
    tokio::net::UnixStream::connect(socket)
        .await
        .map(HostStream::Unix)
}

/// Connect to the API host's Unix socket. This platform has none: it always fails (use the TCP
/// endpoint, [`connect_endpoint`]).
#[cfg(not(unix))]
pub async fn connect_host(_socket: &Path) -> std::io::Result<HostStream> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "the Python API host does not listen on a Unix socket on this platform",
    ))
}

/// Connect to the API host at `endpoint`.
pub async fn connect_endpoint(endpoint: &HostEndpoint) -> std::io::Result<HostStream> {
    match endpoint {
        HostEndpoint::Unix(socket) => connect_host(socket).await,
        HostEndpoint::Tcp { addr, .. } => {
            let stream = tokio::net::TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            Ok(HostStream::Tcp(stream))
        }
    }
}

/// Forwards unmatched requests to the API host's Unix socket.
#[derive(Clone)]
pub struct HostProxy {
    socket: PathBuf,
    supervisor: Option<Arc<HostSupervisor>>,
    /// The console status of an unsupervised host (a supervised one keeps it per process).
    console: Arc<std::sync::Mutex<ConsoleStatus>>,
    /// Whether the unavailable console was already reported in the log.
    warned: Arc<AtomicBool>,
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
            console: Arc::default(),
            warned: Arc::default(),
        }
    }

    /// A proxy that starts (and supervises) the host on the first unmatched request.
    pub fn supervised(supervisor: Arc<HostSupervisor>) -> HostProxy {
        HostProxy {
            socket: supervisor.socket().to_path_buf(),
            supervisor: Some(supervisor),
            console: Arc::default(),
            warned: Arc::default(),
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

    /// Where the host listens now.
    pub fn endpoint(&self) -> HostEndpoint {
        match &self.supervisor {
            Some(supervisor) => supervisor.endpoint(),
            None => HostEndpoint::Unix(self.socket.clone()),
        }
    }

    /// Whether the host mounted the Dash console, as last reported for the current host process
    /// ([`ConsoleStatus::Unknown`] until [`HostProxy::refresh_console`] ran, and again after the
    /// host restarted).
    pub fn console_status(&self) -> ConsoleStatus {
        match &self.supervisor {
            Some(supervisor) => supervisor.console_status(),
            None => self
                .console
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        }
    }

    fn set_console_status(&self, status: ConsoleStatus) {
        match &self.supervisor {
            Some(supervisor) => supervisor.set_console_status(status),
            None => {
                *self
                    .console
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = status
            }
        }
    }

    /// Ask the host whether it mounted the Dash console (`GET /_mrsm/host/status`, else the
    /// registry's `console` object), starting the host when it is supervised and needed, and
    /// remember the answer for this host process. An unavailable console is logged once.
    pub async fn refresh_console(&self, secret: &[u8]) -> ConsoleStatus {
        // No host to ask: nothing to remember (a plugin installed later makes the host needed,
        // and the process that then starts is asked afresh) and nothing to warn about (the
        // start-up probe already did).
        if !self.is_active() {
            return ConsoleStatus::Unavailable("no API host is running".into());
        }
        let status = self.ask_console(secret).await;
        if let ConsoleStatus::Unavailable(reason) = &status
            && !self.warned.swap(true, Ordering::SeqCst)
        {
            tracing::warn!(
                "The web console is not served by the API host ({reason}); `/dash` shows a pointer page."
            );
        }
        self.set_console_status(status.clone());
        status
    }

    /// The console status, asking the host first when it was not asked yet.
    pub async fn console(&self, secret: &[u8]) -> ConsoleStatus {
        match self.console_status() {
            ConsoleStatus::Unknown => self.refresh_console(secret).await,
            known => known,
        }
    }

    async fn ask_console(&self, secret: &[u8]) -> ConsoleStatus {
        if let Some(document) = self.internal_get(STATUS_PATH, secret).await
            && let Some(status) = ConsoleStatus::from_document(&document)
        {
            return status;
        }
        if let Some(document) = self.registry(secret).await
            && let Some(status) = ConsoleStatus::from_document(&document)
        {
            return status;
        }
        ConsoleStatus::Unavailable(
            "the API host did not report a console (is the meerschaum package up to date?)".into(),
        )
    }

    /// `GET path` on the host with the internal token; the JSON body of a success.
    async fn internal_get(&self, path: &str, secret: &[u8]) -> Option<serde_json::Value> {
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri(path)
            .header("host", "localhost")
            .header(
                crate::mcp::federation::TOKEN_HEADER,
                crate::mcp::federation::internal_token(secret, None),
            )
            .body(Body::empty())
            .ok()?;
        let response = self.forward(request).await;
        if !response.status().is_success() {
            return None;
        }
        let bytes = axum::body::to_bytes(response.into_body(), 16 * 1024 * 1024)
            .await
            .ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// The host's plugin registry (`GET /_mrsm/host/registry`: MCP tools, routes and `web_pages`
    /// for the console), fetched with an internal token signed by `secret`. This starts the host
    /// when it is supervised and needed. `None` when the host is not needed or does not answer.
    pub async fn registry(&self, secret: &[u8]) -> Option<serde_json::Value> {
        if !self.is_active() {
            return None;
        }
        self.internal_get(crate::mcp::federation::REGISTRY_PATH, secret)
            .await
    }

    /// Forward a request to the host and return its response.
    pub async fn forward(&self, mut req: Request) -> Response {
        if let Some(sup) = &self.supervisor
            && let Err(e) = sup.ensure_started().await
        {
            tracing::warn!("The API host is unavailable: {e}");
            return bad_gateway("The API host is unavailable.");
        }
        let endpoint = self.endpoint();
        let stream = match connect_endpoint(&endpoint).await {
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
        // The per-start token of the TCP transport is the proxy's alone: a client's copy is
        // dropped, ours added.
        parts.headers.remove(PROXY_TOKEN_HEADER);
        if let Some(token) = endpoint.token()
            && let Ok(v) = token.parse()
        {
            parts.headers.insert(PROXY_TOKEN_HEADER, v);
        }
        if !parts.headers.contains_key(header::HOST)
            && let Ok(v) = "localhost".parse()
        {
            parts.headers.insert(header::HOST, v);
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
