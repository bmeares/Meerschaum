//! Per-team HTTP(S) egress proxy and administrator-declared TCP relays.
//!
//! A proxy resolves the destination itself, refuses every non-public answer and connects to a
//! pinned IP. It has no Docker socket, firewall privileges or API credential. Deploy it only
//! on its team's internal network plus a separate outbound bridge, without published ports.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use crate::error::ApiError;

/// One administrator-declared hostname, all its approved ports, and deployment-time DNS pins.
/// The client keeps the original hostname for DNS and TLS. Only this separate relay receives
/// the upstream IPs, so its own team-network DNS alias cannot send it back into itself.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedRelay {
    /// Original DNS name. It is never substituted into a connector's TLS configuration.
    pub host: String,
    /// Nonempty, bounded addresses resolved by the administrator outside the team network.
    pub addresses: Vec<IpAddr>,
    /// The only upstream ports; each listener forwards to the same numbered upstream port.
    pub ports: Vec<u16>,
}

impl PinnedRelay {
    /// Validate the same destination vocabulary as team settings, and bound listeners/answers.
    pub fn validate(&self) -> Result<(), String> {
        if self.addresses.is_empty()
            || self.addresses.len() > 64
            || self.ports.is_empty()
            || self.ports.len() > 64
        {
            return Err("A pinned relay needs 1–64 addresses and 1–64 ports.".into());
        }
        let mut settings = meerschaum_core::execution::Settings {
            relays: self
                .ports
                .iter()
                .map(|port| meerschaum_core::execution::Relay {
                    host: self.host.clone(),
                    port: *port,
                })
                .collect(),
            ..Default::default()
        };
        settings.normalize().map_err(|e| e.to_string())?;
        if settings.relays.iter().any(|r| r.host != self.host)
            || self.host.parse::<IpAddr>().is_ok()
        {
            return Err("A pinned relay needs a normalized DNS hostname.".into());
        }
        Ok(())
    }
}

/// Conservative public-address policy, including cloud metadata and embedded-address ranges.
/// IPv6 is restricted to global unicast and excludes transition, documentation and special
/// protocol blocks. Declared TCP relays are the administrator's explicit exception.
pub fn public_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(ip) => {
            let value = u32::from(ip);
            let in_range = |base: [u8; 4], bits: u32| {
                let mask = u32::MAX << (32 - bits);
                value & mask == u32::from(Ipv4Addr::from(base)) & mask
            };
            ![
                ([0, 0, 0, 0], 8), ([10, 0, 0, 0], 8), ([100, 64, 0, 0], 10),
                ([127, 0, 0, 0], 8), ([169, 254, 0, 0], 16), ([172, 16, 0, 0], 12),
                ([192, 0, 0, 0], 24), ([192, 0, 2, 0], 24), ([192, 88, 99, 0], 24),
                ([192, 168, 0, 0], 16), ([198, 18, 0, 0], 15), ([198, 51, 100, 0], 24),
                ([203, 0, 113, 0], 24), ([224, 0, 0, 0], 4), ([240, 0, 0, 0], 4),
            ].iter().any(|(base, bits)| in_range(*base, *bits))
                // Azure's platform virtual IP is publicly numbered but is not public egress.
                && ip != Ipv4Addr::new(168, 63, 129, 16)
        }
        IpAddr::V6(ip) => {
            let value = u128::from(ip);
            let in_range = |base: Ipv6Addr, bits: u32| {
                let mask = u128::MAX << (128 - bits);
                value & mask == u128::from(base) & mask
            };
            in_range(Ipv6Addr::new(0x2000, 0, 0, 0, 0, 0, 0, 0), 3)
                && !in_range(Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 0), 23)
                && !in_range(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0), 32)
                && !in_range(Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 0), 16)
                && !in_range(Ipv6Addr::new(0x3fff, 0, 0, 0, 0, 0, 0, 0), 20)
        }
    }
}

/// Resolve and validate the entire answer before choosing a pinned destination. Mixed public
/// and private answers are refused, rather than relying on DNS answer order.
async fn destinations(
    host: &str,
    port: u16,
    public_only: bool,
) -> Result<Vec<SocketAddr>, ApiError> {
    let addresses = tokio::time::timeout(
        Duration::from_secs(3),
        tokio::net::lookup_host((host, port)),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .ok_or_else(|| ApiError::new(502, "The egress destination could not be resolved."))?;
    let mut pinned = Vec::new();
    for address in addresses {
        if pinned.len() >= 64 || (public_only && !public_address(address.ip())) {
            return Err(ApiError::new(
                403,
                "This destination is not permitted by the executor's egress policy.",
            ));
        }
        if !pinned.contains(&address) {
            pinned.push(address);
        }
    }
    if pinned.is_empty() {
        return Err(ApiError::new(
            502,
            "The egress destination has no addresses.",
        ));
    }
    Ok(pinned)
}

async fn connect(addresses: &[SocketAddr]) -> Result<TcpStream, ApiError> {
    tokio::time::timeout(Duration::from_secs(5), async {
        for address in addresses {
            if let Ok(stream) = TcpStream::connect(address).await {
                return Ok(stream);
            }
        }
        Err(ApiError::new(
            502,
            "The egress destination could not be reached.",
        ))
    })
    .await
    .unwrap_or_else(|_| Err(ApiError::new(504, "The egress connection timed out.")))
}

fn strip_hop_headers(headers: &mut axum::http::HeaderMap) {
    let named: Vec<String> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(',').map(|s| s.trim().to_string()))
        .collect();
    for name in named {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}

/// A separate explicit HTTP proxy, serving no Meerschaum API routes.
pub fn proxy_router() -> Router {
    Router::new()
        .fallback(proxy)
        .with_state(Arc::new(Semaphore::new(128)))
}

async fn proxy(State(capacity): State<Arc<Semaphore>>, request: Request) -> Response {
    match forward(capacity, request).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn forward(capacity: Arc<Semaphore>, mut request: Request) -> Result<Response, ApiError> {
    let permit = capacity
        .try_acquire_owned()
        .map_err(|_| ApiError::new(429, "The egress proxy is at capacity."))?;
    let tunnel = request.method() == Method::CONNECT;
    let authority = request
        .uri()
        .authority()
        .ok_or_else(|| ApiError::new(400, "An absolute proxy destination is required."))?
        .clone();
    if authority.as_str().contains('@') {
        return Err(ApiError::new(400, "Invalid proxy destination."));
    }
    let host = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    let port = authority
        .port_u16()
        .unwrap_or(if tunnel { 443 } else { 80 });
    if (tunnel && port != 443)
        || (!tunnel && (request.uri().scheme_str() != Some("http") || port != 80))
    {
        return Err(ApiError::new(
            403,
            "The HTTP proxy permits HTTP on port 80 and CONNECT on port 443; use a declared relay for another port.",
        ));
    }
    let pinned = destinations(host, port, true).await?;
    let stream = connect(&pinned).await?;
    if tunnel {
        let upgrade = hyper::upgrade::on(&mut request);
        tokio::spawn(async move {
            let _permit = permit;
            if let Ok(upgraded) = upgrade.await {
                let mut client = TokioIo::new(upgraded);
                let mut target = stream;
                let _ = tokio::io::copy_bidirectional(&mut client, &mut target).await;
            }
        });
        return Ok(StatusCode::OK.into_response());
    }
    let target = request
        .uri()
        .path_and_query()
        .map_or("/", |p| p.as_str())
        .parse::<Uri>()
        .map_err(|_| ApiError::new(400, "Invalid proxy request path."))?;
    *request.uri_mut() = target;
    strip_hop_headers(request.headers_mut());
    request.headers_mut().insert(
        header::HOST,
        authority
            .as_str()
            .parse()
            .map_err(|_| ApiError::new(400, "Invalid destination authority."))?,
    );
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake::<_, Body>(TokioIo::new(stream))
            .await
            .map_err(|_| ApiError::new(502, "Could not initialize the egress connection."))?;
    tokio::spawn(async move {
        let _permit = permit;
        let _ = connection.await;
    });
    let mut response = tokio::time::timeout(Duration::from_secs(60), sender.send_request(request))
        .await
        .map_err(|_| ApiError::new(504, "The egress response timed out."))?
        .map_err(|_| ApiError::new(502, "The egress destination did not answer."))?;
    strip_hop_headers(response.headers_mut());
    Ok(response.map(Body::new))
}

/// Relay one explicitly approved TCP destination. The destination is fixed by an administrator
/// and may be private; clients cannot select another host or port through the data stream.
pub async fn serve_relay(
    listener: TcpListener,
    destination: meerschaum_core::execution::Relay,
) -> std::io::Result<()> {
    let mut settings = meerschaum_core::execution::Settings {
        relays: vec![destination],
        ..Default::default()
    };
    settings.normalize().map_err(std::io::Error::other)?;
    let destination = settings.relays.remove(0);
    let capacity = Arc::new(Semaphore::new(128));
    loop {
        let (mut client, _) = listener.accept().await?;
        let Ok(permit) = capacity.clone().try_acquire_owned() else {
            continue;
        };
        let destination = destination.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let Ok(pinned) = destinations(&destination.host, destination.port, false).await else {
                return;
            };
            let Ok(mut target) = connect(&pinned).await else {
                return;
            };
            let _ = tokio::io::copy_bidirectional(&mut client, &mut target).await;
        });
    }
}

/// Serve all approved ports for one DNS alias. Bytes, including TLS handshakes and certificates,
/// are passed unchanged. DNS is deliberately not refreshed from the alias-bearing namespace:
/// an administrator regenerates the stack to refresh its pinned upstream addresses.
pub async fn serve_pinned_relays(host: &str, destination: PinnedRelay) -> std::io::Result<()> {
    destination.validate().map_err(std::io::Error::other)?;
    let capacity = Arc::new(Semaphore::new(128));
    let mut listeners = Vec::new();
    // Bind every port before accepting any connection, so an incomplete declaration fails closed.
    for port in &destination.ports {
        let listener = TcpListener::bind((host, *port)).await?;
        let addresses = destination
            .addresses
            .iter()
            .map(|ip| SocketAddr::new(*ip, *port))
            .collect();
        listeners.push((listener, addresses));
    }
    futures_util::future::try_join_all(
        listeners.into_iter().map(|(listener, addresses)| {
            serve_pinned_listener(listener, addresses, capacity.clone())
        }),
    )
    .await
    .map(|_| ())
}

async fn serve_pinned_listener(
    listener: TcpListener,
    addresses: Vec<SocketAddr>,
    capacity: Arc<Semaphore>,
) -> std::io::Result<()> {
    let addresses = Arc::new(addresses);
    loop {
        let (mut client, _) = listener.accept().await?;
        let Ok(permit) = capacity.clone().try_acquire_owned() else {
            continue;
        };
        let addresses = addresses.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let Ok(mut target) = connect(&addresses).await else {
                return;
            };
            let _ = tokio::io::copy_bidirectional(&mut client, &mut target).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    #[test]
    fn public_policy_blocks_private_metadata_reserved_and_embedded_addresses() {
        for ip in [
            "0.0.0.0",
            "0.1.2.3",
            "10.1.2.3",
            "100.100.100.200",
            "127.0.0.1",
            "169.254.169.254",
            "172.31.255.255",
            "192.168.1.1",
            "192.0.0.1",
            "192.0.2.1",
            "192.88.99.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "255.255.255.255",
            "168.63.129.16",
            "::",
            "::1",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "fc00::1",
            "fe80::1",
            "ff02::1",
            "2001:db8::1",
            "2001::1",
            "2001:20::1",
            "2002:0808:0808::1",
            "3fff::1",
        ] {
            assert!(!public_address(ip.parse().unwrap()), "{ip}");
        }
        for ip in [
            "8.8.8.8",
            "1.1.1.1",
            "93.184.215.14",
            "2606:4700:4700::1111",
            "2001:4860:4860::8888",
        ] {
            assert!(public_address(ip.parse().unwrap()), "{ip}");
        }
    }

    #[tokio::test]
    async fn local_addresses_and_non_http_ports_are_refused_without_connecting() {
        for (method, destination) in [
            (Method::GET, "http://127.0.0.1/"),
            (Method::GET, "http://169.254.169.254/metadata"),
            (Method::CONNECT, "[::1]:443"),
            (Method::CONNECT, "example.com:5432"),
            (Method::GET, "http://example.com:8080/"),
            (Method::GET, "https://example.com/"),
        ] {
            let request = Request::builder()
                .method(method)
                .uri(destination)
                .body(Body::empty())
                .unwrap();
            assert_eq!(
                proxy_router().oneshot(request).await.unwrap().status(),
                StatusCode::FORBIDDEN,
                "{destination}"
            );
        }
    }

    #[tokio::test]
    async fn a_declared_relay_only_forwards_its_fixed_destination() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_port = target.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            let (mut socket, _) = target.accept().await.unwrap();
            let mut bytes = [0; 4];
            socket.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"ping");
            socket.write_all(b"pong").await.unwrap();
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let relay = tokio::spawn(serve_relay(
            listener,
            meerschaum_core::execution::Relay {
                host: "127.0.0.1".into(),
                port: target_port,
            },
        ));
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut bytes = [0; 4];
        client.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"pong");
        task.await.unwrap();
        drop(client);
        relay.abort();
    }

    #[tokio::test]
    async fn pinned_relay_retains_tls_hostname_verification_without_resolving_its_alias() {
        use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["database.example".into()]).unwrap();
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let config = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.der().clone()],
                PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into(),
            )
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_address = target.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (socket, _) = target.accept().await.unwrap();
                if let Ok(mut tls) = acceptor.accept(socket).await {
                    assert_eq!(tls.get_ref().1.server_name(), Some("database.example"));
                    let mut bytes = [0; 4];
                    tls.read_exact(&mut bytes).await.unwrap();
                    assert_eq!(&bytes, b"ping");
                    tls.write_all(b"pong").await.unwrap();
                    tls.shutdown().await.unwrap();
                }
            }
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let relay_address = listener.local_addr().unwrap();
        let relay = tokio::spawn(serve_pinned_listener(
            listener,
            vec![target_address],
            Arc::new(Semaphore::new(128)),
        ));
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.der().clone()).unwrap();
        let config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let mut tls = connector
            .connect(
                ServerName::try_from("database.example").unwrap(),
                TcpStream::connect(relay_address).await.unwrap(),
            )
            .await
            .unwrap();
        tls.write_all(b"ping").await.unwrap();
        let mut bytes = [0; 4];
        tls.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"pong");
        drop(tls);
        assert!(
            connector
                .connect(
                    ServerName::try_from("wrong.example").unwrap(),
                    TcpStream::connect(relay_address).await.unwrap()
                )
                .await
                .is_err(),
            "A relay must not disable normal certificate-name validation."
        );
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        relay.abort();
    }

    #[test]
    fn pinned_relay_declarations_are_bounded_and_only_name_fixed_ports() {
        let destination = PinnedRelay {
            host: "database.example".into(),
            addresses: vec!["10.23.45.67".parse().unwrap()],
            ports: vec![5432, 8883],
        };
        assert!(destination.validate().is_ok());
        for invalid in [
            PinnedRelay {
                ports: vec![0],
                ..destination.clone()
            },
            PinnedRelay {
                ports: vec![5432, 5432],
                ..destination.clone()
            },
            PinnedRelay {
                addresses: Vec::new(),
                ..destination.clone()
            },
            PinnedRelay {
                addresses: vec!["10.23.45.67".parse().unwrap(); 65],
                ..destination.clone()
            },
            PinnedRelay {
                host: "10.23.45.67".into(),
                ..destination.clone()
            },
        ] {
            assert!(invalid.validate().is_err());
        }
    }
}
