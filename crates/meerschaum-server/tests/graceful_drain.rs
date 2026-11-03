//! The server's drain (`crate::lifecycle`): `GET /healthcheck/ready` answers `503` while the
//! server starts or drains (`/healthcheck` is unchanged), and a draining listener stops
//! accepting, finishes the request in progress and completes once no connection is left.

mod common;

use std::io::{Read, Write};
use std::time::Duration;

use axum::http::StatusCode;
use common::{req, send};

#[tokio::test]
async fn ready_answers_503_while_starting_or_draining() {
    let dir = tempfile::tempdir().unwrap();
    let (state, _) = common::state_with(common::config(dir.path()));
    let app = meerschaum_server::app(state.clone());

    let r = send(&app, req("GET", "/healthcheck/ready", None, None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["success"], true);
    assert_eq!(r.headers["cache-control"], "no-store");

    state.lifecycle.set_starting(true);
    let r = send(&app, req("GET", "/healthcheck/ready", None, None)).await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.json()["message"], "The server is starting.");
    state.lifecycle.set_starting(false);

    state.lifecycle.begin_drain();
    let r = send(&app, req("GET", "/healthcheck/ready", None, None)).await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.json()["success"], false);
    assert_eq!(r.json()["message"], "The server is shutting down.");
    // Uptime monitors keep matching `/healthcheck`.
    let r = send(&app, req("GET", "/healthcheck", None, None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
}

/// The public preset keeps `/healthcheck/ready` with `/healthcheck`, and both are documented.
#[tokio::test]
async fn ready_is_public_and_documented() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(dir.path());
    cfg.public = true;
    let (state, _) = common::state_with(cfg);
    let app = meerschaum_server::app(state.clone());
    let r = send(&app, req("GET", "/healthcheck/ready", None, None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    // The preset still hides the rest of `misc`.
    assert!(send(&app, req("GET", "/info", None, None)).await.status != StatusCode::OK);
    let doc = serde_json::to_value(meerschaum_server::openapi::document(&state.config)).unwrap();
    assert!(
        doc["paths"]["/healthcheck/ready"]["get"].is_object(),
        "{doc}"
    );
    assert!(doc["paths"]["/healthcheck"]["get"].is_object());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_draining_listener_finishes_the_request_in_progress() {
    let dir = tempfile::tempdir().unwrap();
    let socket = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let mut cfg = common::config(dir.path());
    cfg.host = "127.0.0.1".into();
    cfg.port = port;
    // `POST /mcp` reads its body after authentication: without auth, at once.
    cfg.no_auth = true;
    let (state, _) = common::state_with(cfg);
    let mcp = std::sync::Arc::new(meerschaum_server::mcp::McpServer::for_state(&state));
    let serving = tokio::spawn(meerschaum_server::serve_with_tls(
        state.clone(),
        mcp,
        None,
        None,
    ));
    let ping = r#"{"jsonrpc": "2.0", "id": 7, "method": "ping"}"#;
    let sent = tokio::task::spawn_blocking(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut stream = loop {
            match std::net::TcpStream::connect(("127.0.0.1", port)) {
                Ok(stream) => break stream,
                Err(_) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Err(e) => panic!("{e}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        write!(
            stream,
            "POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            ping.len(),
            &ping[..10]
        )
        .unwrap();
        stream
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    state.lifecycle.begin_drain();
    tokio::time::sleep(Duration::from_millis(300)).await;
    // New connections are refused.
    assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
    let finished_early = serving.is_finished();
    let text = tokio::task::spawn_blocking(move || {
        let mut stream = sent;
        stream.write_all(&ping.as_bytes()[10..]).unwrap();
        let mut text = String::new();
        let _ = stream.read_to_string(&mut text);
        text
    })
    .await
    .unwrap();
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(!finished_early, "serving ended before the request did");
    // The connection closes after the response (no keep-alive while draining) and serving ends.
    tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .expect("the server kept serving")
        .unwrap()
        .unwrap();
}
