//! Native HTTPS (`meerschaum_server::tls`, `start api --keyfile/--certfile`): the API over
//! HTTP/1.1 and HTTP/2 (ALPN), TLS 1.2 and 1.3, a `Host` header and the peer address for HTTP/2
//! requests, Server-Sent Events, the web terminal's websocket over `wss://`, plain HTTP on the
//! HTTPS port (`/healthcheck` answered, the rest redirected), the loopback-only internal listener,
//! and certificates reloaded when their files change.
//!
//! Every certificate is generated here (rcgen): a test CA and leaves for `localhost`.

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::ConnectInfo;
use axum::http::{HeaderMap, Request, StatusCode, Version, header};
use axum::routing::get;
use bytes::Bytes;
use common::{SECRET, req, send};
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use meerschaum_server::tls::{Https, TlsEvent, TlsEvents, TlsFiles, TlsPath};
use rustls::pki_types::{CertificateDer, ServerName};
use serde_json::{Value as Json, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A test certificate authority.
struct Pki {
    ca: rcgen::Certificate,
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
}

impl Pki {
    fn new() -> Pki {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "Meerschaum test CA");
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let key = rcgen::KeyPair::generate().unwrap();
        let ca = params.self_signed(&key).unwrap();
        Pki {
            ca,
            issuer: rcgen::Issuer::new(params, key),
        }
    }

    fn root(&self) -> CertificateDer<'static> {
        self.ca.der().clone()
    }

    /// A key and a certificate for `names` signed by the CA (PEM).
    fn leaf(&self, names: &[&str]) -> (String, String, CertificateDer<'static>) {
        let key = rcgen::KeyPair::generate().unwrap();
        let params =
            rcgen::CertificateParams::new(names.iter().map(|n| n.to_string()).collect::<Vec<_>>())
                .unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        (key.serialize_pem(), cert.pem(), cert.der().clone())
    }
}

fn write_pair(dir: &Path, key: &str, cert: &str) -> TlsFiles {
    let (keyfile, certfile) = (dir.join("key.pem"), dir.join("cert.pem"));
    std::fs::write(&keyfile, key).unwrap();
    std::fs::write(&certfile, cert).unwrap();
    TlsFiles {
        keyfile: TlsPath {
            path: keyfile,
            source: "`--keyfile`".into(),
        },
        certfile: TlsPath {
            path: certfile,
            source: "`--certfile`".into(),
        },
    }
}

/// Serve `app` over HTTPS on a random loopback port.
async fn serve(app: Router, https: Https) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = meerschaum_server::tls::serve(listener, app, https).await;
    });
    addr
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// A client that trusts `root` and offers `alpn` (none: no ALPN extension), on `versions`.
fn connector(
    root: Option<&CertificateDer<'static>>,
    alpn: &[&[u8]],
    versions: &[&'static rustls::SupportedProtocolVersion],
) -> tokio_rustls::TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    if let Some(root) = root {
        roots.add(root.clone()).unwrap();
    }
    let mut config = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(versions)
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    tokio_rustls::TlsConnector::from(Arc::new(config))
}

const ALL_VERSIONS: &[&rustls::SupportedProtocolVersion] = rustls::ALL_VERSIONS;

async fn tls(
    addr: SocketAddr,
    connector: &tokio_rustls::TlsConnector,
) -> std::io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let tcp = TcpStream::connect(addr).await?;
    connector
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
}

/// A response's status, version and body.
struct Answer {
    status: StatusCode,
    version: Version,
    body: Bytes,
}

impl Answer {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
    fn json(&self) -> Json {
        serde_json::from_slice(&self.body).unwrap_or(Json::Null)
    }
}

async fn collect(response: hyper::Response<hyper::body::Incoming>) -> Answer {
    let (parts, body) = response.into_parts();
    Answer {
        status: parts.status,
        version: parts.version,
        body: body.collect().await.unwrap().to_bytes(),
    }
}

/// An HTTP/1.1 connection over `stream`.
async fn http1<S>(stream: S) -> hyper::client::conn::http1::SendRequest<Full<Bytes>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(conn);
    sender
}

/// An HTTP/2 connection over `stream`.
async fn http2<S>(stream: S) -> hyper::client::conn::http2::SendRequest<Full<Bytes>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    tokio::spawn(conn);
    sender
}

/// A request: HTTP/1.1 ones name the host in `Host`, HTTP/2 ones in the URI's authority.
fn request(
    version: Version,
    method: &str,
    addr: SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> Request<Full<Bytes>> {
    let authority = format!("localhost:{}", addr.port());
    let mut builder = if version == Version::HTTP_2 {
        Request::builder()
            .method(method)
            .uri(format!("https://{authority}{path}"))
    } else {
        Request::builder()
            .method(method)
            .uri(path)
            .header(header::HOST, &authority)
    };
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}

/// A raw plain-HTTP/1.1 exchange (the response head and body as text).
async fn plain(addr: SocketAddr, path: &str, host: Option<&str>) -> String {
    let mut tcp = TcpStream::connect(addr).await.unwrap();
    let host = host.map(|h| format!("Host: {h}\r\n")).unwrap_or_default();
    tcp.write_all(format!("GET {path} HTTP/1.1\r\n{host}Connection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), tcp.read_to_end(&mut out))
        .await
        .expect("an answer")
        .unwrap();
    String::from_utf8_lossy(&out).into_owned()
}

fn api_state(dir: &Path) -> meerschaum_server::AppState {
    common::state_with(common::config(dir)).0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_api_answers_over_http1_and_http2_with_plain_http_redirected() {
    let dir = tempfile::tempdir().unwrap();
    let pki = Pki::new();
    let (key, cert, _) = pki.leaf(&["localhost"]);
    let https = Https::new(write_pair(dir.path(), &key, &cert))
        .unwrap()
        .with_internal_listener()
        .unwrap();
    let internal = https.internal_addr().expect("the internal listener");
    assert!(internal.ip().is_loopback());
    let addr = serve(meerschaum_server::app(api_state(dir.path())), https).await;
    let root = pki.root();

    // HTTP/1.1 (ALPN `http/1.1`): the version, then a login.
    let stream = tls(addr, &connector(Some(&root), &[b"http/1.1"], ALL_VERSIONS))
        .await
        .unwrap();
    assert_eq!(stream.get_ref().1.alpn_protocol(), Some(&b"http/1.1"[..]));
    let mut h1 = http1(stream).await;
    let version = collect(
        h1.send_request(request(Version::HTTP_11, "GET", addr, "/version", &[], ""))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(version.status, StatusCode::OK, "{}", version.text());
    assert_eq!(version.version, Version::HTTP_11);
    assert_eq!(version.json(), json!(meerschaum_server::VERSION));
    let form = [("content-type", "application/x-www-form-urlencoded")];
    let login = collect(
        h1.send_request(request(
            Version::HTTP_11,
            "POST",
            addr,
            "/login",
            &form,
            "username=admin&password=pass",
        ))
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(login.status, StatusCode::OK, "{}", login.text());
    let token = login.json()["access_token"].as_str().unwrap().to_string();

    // HTTP/2 (ALPN `h2` first): the same routes, and the token works there too.
    let stream = tls(
        addr,
        &connector(Some(&root), &[b"h2", b"http/1.1"], ALL_VERSIONS),
    )
    .await
    .unwrap();
    assert_eq!(stream.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
    let mut h2 = http2(stream).await;
    let health = collect(
        h2.send_request(request(
            Version::HTTP_2,
            "GET",
            addr,
            "/healthcheck",
            &[],
            "",
        ))
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(health.version, Version::HTTP_2);
    assert_eq!(health.status, StatusCode::OK, "{}", health.text());
    let bearer = format!("Bearer {token}");
    let me = collect(
        h2.send_request(request(
            Version::HTTP_2,
            "GET",
            addr,
            "/users/me",
            &[("authorization", &bearer)],
            "",
        ))
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(me.status, StatusCode::OK, "{}", me.text());
    assert_eq!(me.json()["username"], json!("admin"), "{}", me.text());
    let login = collect(
        h2.send_request(request(
            Version::HTTP_2,
            "POST",
            addr,
            "/login",
            &form,
            "username=admin&password=wrong",
        ))
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(login.status, StatusCode::UNAUTHORIZED, "{}", login.text());

    // TLS 1.2 only, and a client without ALPN: HTTP/1.1.
    let stream = tls(
        addr,
        &connector(Some(&root), &[], &[&rustls::version::TLS12]),
    )
    .await
    .unwrap();
    assert_eq!(
        stream.get_ref().1.protocol_version(),
        Some(rustls::ProtocolVersion::TLSv1_2)
    );
    assert_eq!(stream.get_ref().1.alpn_protocol(), None);
    let mut old = http1(stream).await;
    let answer = collect(
        old.send_request(request(Version::HTTP_11, "GET", addr, "/version", &[], ""))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK);

    // A client that does not trust the CA refuses the certificate: this is real TLS.
    let refused = tls(addr, &connector(None, &[b"h2"], ALL_VERSIONS)).await;
    assert!(refused.is_err());

    // Plain HTTP on the HTTPS port: the healthcheck is answered (container healthchecks), the
    // rest is redirected to `https://` on the same host and port; no `Host`: `400`.
    let health = plain(addr, "/healthcheck", Some("localhost")).await;
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
    assert!(health.contains("\"success\":true"), "{health}");
    let host = format!("localhost:{}", addr.port());
    let redirect = plain(addr, "/dash/pipes?tab=1", Some(&host)).await;
    assert!(redirect.starts_with("HTTP/1.1 308"), "{redirect}");
    assert!(
        redirect.contains(&format!("location: https://{host}/dash/pipes?tab=1")),
        "{redirect}"
    );
    let login = plain(addr, "/login", Some(&host)).await;
    assert!(login.starts_with("HTTP/1.1 308"), "{login}");
    let hostless = plain(addr, "/", None).await;
    assert!(hostless.starts_with("HTTP/1.1 400"), "{hostless}");

    // The internal listener is plain HTTP on loopback, with the whole API.
    let version = plain(internal, "/version", Some("localhost")).await;
    assert!(version.starts_with("HTTP/1.1 200"), "{version}");
    assert!(version.contains(meerschaum_server::VERSION), "{version}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requests_carry_the_host_and_the_peer_over_both_protocols() {
    let dir = tempfile::tempdir().unwrap();
    let pki = Pki::new();
    let (key, cert, _) = pki.leaf(&["localhost"]);
    let https = Https::new(write_pair(dir.path(), &key, &cert)).unwrap();
    let echo = Router::new().route(
        "/echo",
        get(
            |headers: HeaderMap, ConnectInfo(peer): ConnectInfo<SocketAddr>| async move {
                let host = headers
                    .get(header::HOST)
                    .and_then(|h| h.to_str().ok())
                    .unwrap_or("-")
                    .to_string();
                format!("{host}|{}", peer.ip())
            },
        ),
    );
    let addr = serve(echo, https).await;
    let root = pki.root();
    let expected = format!("localhost:{}|127.0.0.1", addr.port());

    let mut h2 = http2(
        tls(addr, &connector(Some(&root), &[b"h2"], ALL_VERSIONS))
            .await
            .unwrap(),
    )
    .await;
    let answer = collect(
        h2.send_request(request(Version::HTTP_2, "GET", addr, "/echo", &[], ""))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(answer.version, Version::HTTP_2);
    assert_eq!(answer.text(), expected);

    let mut h1 = http1(
        tls(addr, &connector(Some(&root), &[b"http/1.1"], ALL_VERSIONS))
            .await
            .unwrap(),
    )
    .await;
    let answer = collect(
        h1.send_request(request(Version::HTTP_11, "GET", addr, "/echo", &[], ""))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(answer.text(), expected);
}

/// The engine's run event stream (`GET /engine/runs/{id}/events/stream`, Server-Sent Events) over
/// HTTPS, on HTTP/2 and HTTP/1.1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_sent_events_stream_over_https() {
    use meerschaum_connector::InstanceConnector;
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite:///{}", dir.path().join("store.db").display());
    let store: Arc<dyn InstanceConnector> = Arc::new(
        meerschaum_connector_sql::SqlConnector::from_uri("tlsstream", &uri)
            .await
            .unwrap(),
    );
    let users = Arc::new(common::MemoryInstance::new("fixture"));
    users.seed_user("admin", "pass", Some("admin"));
    let state = meerschaum_server::AppState::builder(common::config(dir.path()), users)
        .secret(SECRET.to_vec())
        .engine(Arc::new(
            meerschaum_server::engine_store::InstanceEngine::new(store, "sql:tlsstream"),
        ))
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let admin = common::login(&app, "admin", "pass", None).await;
    let run = json!({"run_id": "tls-run", "started": "2026-01-01T00:00:00Z", "status": "success",
                     "finished": "2026-01-01T00:01:00Z", "success": true,
                     "holder": meerschaum_core::holder::process_holder()});
    let recorded = send(
        &app,
        req(
            "POST",
            "/engine/runs",
            Some(&admin),
            Some(("application/json", json!([run]).to_string().into_bytes())),
        ),
    )
    .await;
    assert_eq!(recorded.status, StatusCode::OK, "{}", recorded.text());
    let now = chrono::Utc::now();
    let rows: Vec<Json> = ["run_started", "node_started", "node_finished", "run_finished"]
        .iter()
        .enumerate()
        .map(|(seq, event)| {
            json!({
                "ts": (now + chrono::TimeDelta::milliseconds(seq as i64)).to_rfc3339(),
                "run_id": "tls-run",
                "seq": seq,
                "pipe_ref": if event.starts_with("run_") { Json::Null } else { json!("sql:tlsstream|sql:src|m|None") },
                "event": event,
                "payload": {"event": event, "success": true, "message": "ok"},
            })
        })
        .collect();
    let written = send(
        &app,
        req(
            "POST",
            "/engine/pipes/engine_events/write",
            Some(&admin),
            Some((
                "application/json",
                Json::Array(rows).to_string().into_bytes(),
            )),
        ),
    )
    .await;
    assert_eq!(written.status, StatusCode::OK, "{}", written.text());

    let pki = Pki::new();
    let (key, cert, _) = pki.leaf(&["localhost"]);
    let https = Https::new(write_pair(dir.path(), &key, &cert)).unwrap();
    let addr = serve(app, https).await;
    let root = pki.root();
    let path = "/engine/runs/tls-run/events/stream";
    for alpn in [&b"h2"[..], &b"http/1.1"[..]] {
        let stream = tls(addr, &connector(Some(&root), &[alpn], ALL_VERSIONS))
            .await
            .unwrap();
        let auth = [("authorization", admin.as_str())];
        let response = if alpn == b"h2" {
            http2(stream)
                .await
                .send_request(request(Version::HTTP_2, "GET", addr, path, &auth, ""))
                .await
                .unwrap()
        } else {
            http1(stream)
                .await
                .send_request(request(Version::HTTP_11, "GET", addr, path, &auth, ""))
                .await
                .unwrap()
        };
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/event-stream",
            "{alpn:?}"
        );
        let body = tokio::time::timeout(Duration::from_secs(20), async {
            response.into_body().collect().await.unwrap().to_bytes()
        })
        .await
        .expect("the stream ends after run_finished");
        let text = String::from_utf8_lossy(&body);
        let events: Vec<&str> = text
            .lines()
            .filter_map(|l| l.strip_prefix("event:").map(str::trim))
            .collect();
        assert_eq!(
            events,
            [
                "run_started",
                "node_started",
                "node_finished",
                "run_finished"
            ],
            "{alpn:?}: {text}"
        );
    }
}

/// The web terminal (`/websocket/{session}`) over `wss://`: browsers open it on HTTP/1.1.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_web_terminal_runs_over_wss() {
    use meerschaum_server::webterm::{self, SessionLookup, ShellCommand, WebtermConfig};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    struct NoSessions;
    #[async_trait::async_trait]
    impl SessionLookup for NoSessions {
        async fn username(&self, _session_id: &str) -> Result<Option<String>, String> {
            Ok(None)
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(dir.path());
    // `--no-auth` grants every session a terminal: this test is about the transport.
    cfg.no_auth = true;
    let (state, _) = common::state_with(cfg);
    let mut shell = ShellCommand::new("sh");
    shell.env.push(("PS1".into(), "$ ".into()));
    let config = WebtermConfig::new(
        &state,
        Arc::new(NoSessions),
        Arc::new(|id: &str| format!("<html><body data-session=\"{id}\"></body></html>")),
        shell,
    )
    .with_tmux(None);
    let app = webterm::routes(state.clone(), config);
    let pki = Pki::new();
    let (key, cert, _) = pki.leaf(&["localhost"]);
    let addr = serve(
        app,
        Https::new(write_pair(dir.path(), &key, &cert)).unwrap(),
    )
    .await;
    let session = "11111111-1111-4111-8111-111111111111";
    let stream = tls(
        addr,
        &connector(Some(&pki.root()), &[b"http/1.1"], ALL_VERSIONS),
    )
    .await
    .unwrap();
    let mut request = format!("wss://localhost:{}/websocket/{session}", addr.port())
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "origin",
        format!("https://localhost:{}", addr.port())
            .parse()
            .unwrap(),
    );
    let (mut socket, response) = tokio_tungstenite::client_async(request, stream)
        .await
        .expect("the websocket handshake over TLS");
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(next(&mut socket).await, Some(json!(["setup", {}])));
    socket
        .send(Message::Text(
            json!(["stdin", "echo tls-$((40 + 2))-ok\r"])
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let mut out = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !out.split(['\r', '\n']).any(|line| line == "tls-42-ok") {
        assert!(tokio::time::Instant::now() < deadline, "no answer: {out:?}");
        match next(&mut socket).await {
            Some(message) if message[0] == "stdout" => {
                out.push_str(message[1].as_str().unwrap_or_default());
            }
            Some(_) => {}
            None => panic!("the terminal closed: {out:?}"),
        }
    }
    let _ = socket.close(None).await;
}

/// The next terminado message (pings skipped), or `None` once the socket closes or is quiet.
#[cfg(unix)]
async fn next<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> Option<Json>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio_tungstenite::tungstenite::Message;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) => return serde_json::from_str::<Json>(&text).ok(),
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return None,
                Some(Ok(_)) => continue,
            }
        }
    })
    .await
    .ok()
    .flatten()
}

/// The peer certificate of a fresh handshake.
async fn served(addr: SocketAddr, root: &CertificateDer<'static>) -> CertificateDer<'static> {
    let stream = tls(addr, &connector(Some(root), &[b"h2"], ALL_VERSIONS))
        .await
        .unwrap();
    stream.get_ref().1.peer_certificates().unwrap()[0].clone()
}

async fn wait_for(what: &str, mut done: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !done().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn certificates_reload_when_their_files_change() {
    let dir = tempfile::tempdir().unwrap();
    let pki = Pki::new();
    let root = pki.root();
    let (key_a, cert_a, der_a) = pki.leaf(&["localhost"]);
    let files = write_pair(dir.path(), &key_a, &cert_a);
    let events: Arc<Mutex<Vec<TlsEvent>>> = Arc::default();
    let sink: TlsEvents = {
        let events = events.clone();
        Arc::new(move |event| events.lock().unwrap().push(event))
    };
    let https = Https::new(files.clone())
        .unwrap()
        .with_events(sink)
        .with_reload_interval(Duration::from_millis(50));
    let addr = serve(Router::new().route("/", get(|| async { "ok" })), https).await;
    assert_eq!(served(addr, &root).await, der_a);

    // Half a renewal (the new key, the old certificate): still the old certificate, and one
    // warning once the state persists, not one per poll.
    let (key_b, cert_b, der_b) = pki.leaf(&["localhost"]);
    std::fs::write(&files.keyfile.path, &key_b).unwrap();
    let warnings = || {
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e, TlsEvent::Warning(_)))
            .count()
    };
    wait_for("a warning", async || warnings() == 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(warnings(), 1, "{:?}", events.lock().unwrap());
    let warning = events.lock().unwrap().last().unwrap().message().to_string();
    assert!(
        warning
            .starts_with("Could not reload the TLS certificate; still serving the previous one.")
            && warning.contains("does not belong to the certificate"),
        "{warning}"
    );
    assert_eq!(served(addr, &root).await, der_a);

    // The renewal completes: the new certificate is served, existing settings unchanged.
    std::fs::write(&files.certfile.path, &cert_b).unwrap();
    wait_for("the new certificate", async || {
        served(addr, &root).await == der_b
    })
    .await;
    let reloaded = events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| matches!(e, TlsEvent::Info(m) if m.starts_with("Reloaded the TLS certificate from")))
        .count();
    assert_eq!(reloaded, 1, "{:?}", events.lock().unwrap());

    // An atomic replacement (a new file renamed over the old one, as certbot's symlinks do).
    let (key_c, cert_c, der_c) = pki.leaf(&["localhost"]);
    std::fs::write(dir.path().join("key.new"), &key_c).unwrap();
    std::fs::write(dir.path().join("cert.new"), &cert_c).unwrap();
    std::fs::rename(dir.path().join("key.new"), &files.keyfile.path).unwrap();
    std::fs::rename(dir.path().join("cert.new"), &files.certfile.path).unwrap();
    wait_for("the renamed certificate", async || {
        served(addr, &root).await == der_c
    })
    .await;
}
