//! Request size limits (`api:limits`) and the API host's response timeout
//! (`api:host:response_seconds`): a body over its route's allowance gets `413` before it is read
//! (a declared length) or as soon as it streams past it (chunked), whether the server or the API
//! host would have read it; a host that does not start answering gets `504`.
//!
//! These tests use only configuration keys, so they also run against a server without the limits
//! (where they fail).
#![cfg(unix)]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::http::{Request, StatusCode};
use axum::routing::{get, post};
use common::{SECRET, login, req, send};
use futures_util::StreamExt;
use meerschaum_server::proxy::HostProxy;
use meerschaum_server::{AppState, ServerConfig, ServerFlags};
use serde_json::{Value as Json, json};

/// A config for `sql:fixture` under `root` with these `api` keys.
fn config_with_api(root: &Path, api: Json) -> ServerConfig {
    let map = serde_json::from_value::<meerschaum_core::Value>(json!({ "api": api }))
        .unwrap()
        .to_map();
    let mut cfg = ServerConfig::from_map(
        &map,
        &ServerFlags {
            instance: Some("sql:fixture".into()),
            ..Default::default()
        },
    );
    let paths = common::config(root);
    cfg.secret_key_path = paths.secret_key_path;
    cfg.plugins_archives_path = paths.plugins_archives_path;
    cfg.webterm_resources_path = paths.webterm_resources_path;
    cfg.root_dir = paths.root_dir;
    cfg
}

/// The application over an in-memory instance with an `admin` (`admin` / `pass`).
fn app_for(cfg: ServerConfig, host: Option<HostProxy>) -> Router {
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    let mut builder = AppState::builder(cfg, instance).secret(SECRET.to_vec());
    if let Some(host) = host {
        builder = builder.host(Arc::new(host));
    }
    meerschaum_server::app(builder.build().expect("state"))
}

/// `count` chunks of `size` bytes, with no declared length (sent chunked).
fn chunked(count: usize, size: usize) -> Body {
    Body::from_stream(futures_util::stream::iter(
        (0..count).map(move |_| Ok::<_, std::io::Error>(Bytes::from(vec![b'a'; size]))),
    ))
}

/// A body that declares nothing and never arrives.
fn never() -> Body {
    Body::from_stream(futures_util::stream::pending::<Result<Bytes, std::io::Error>>())
}

#[tokio::test]
async fn login_refuses_a_body_over_auth_bytes_before_reading_it() {
    let dir = tempfile::tempdir().unwrap();
    let app = app_for(common::config(dir.path()), None);
    // A declared length over 64 KiB: answered at once, the body (which never comes) unread.
    let request = Request::post("/login")
        .header("content-type", "application/x-www-form-urlencoded")
        .header("content-length", (1024 * 1024).to_string())
        .body(never())
        .unwrap();
    let r = tokio::time::timeout(Duration::from_secs(10), send(&app, request))
        .await
        .expect("the server waited for a body it should have refused");
    assert_eq!(r.status, StatusCode::PAYLOAD_TOO_LARGE, "{}", r.text());
    assert!(
        r.json()["detail"]
            .as_str()
            .unwrap_or("")
            .contains("api:limits"),
        "{}",
        r.text()
    );
    // Without a declared length, the body is refused when it streams past the limit.
    let request = Request::post("/login")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(chunked(20, 8 * 1024))
        .unwrap();
    let r = tokio::time::timeout(Duration::from_secs(10), send(&app, request))
        .await
        .expect("no answer");
    assert_eq!(r.status, StatusCode::PAYLOAD_TOO_LARGE, "{}", r.text());
    // An ordinary login is unaffected.
    login(&app, "admin", "pass", None).await;
}

#[tokio::test]
async fn a_null_limit_is_no_limit() {
    let dir = tempfile::tempdir().unwrap();
    let app = app_for(
        config_with_api(dir.path(), json!({"limits": {"auth_bytes": null}})),
        None,
    );
    let request = Request::post("/login")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(chunked(20, 8 * 1024))
        .unwrap();
    let r = send(&app, request).await;
    assert_ne!(r.status, StatusCode::PAYLOAD_TOO_LARGE, "{}", r.text());
}

#[tokio::test]
async fn pipe_data_sync_follows_data_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let app = app_for(
        config_with_api(dir.path(), json!({"limits": {"data_bytes": 1024}})),
        None,
    );
    let admin = login(&app, "admin", "pass", None).await;
    let r = send(
        &app,
        req(
            "POST",
            "/pipes/a/b/[None]/register",
            Some(&admin),
            Some((
                "application/json",
                br#"{"columns": {"datetime": "ts", "id": "id"}}"#.to_vec(),
            )),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    // One row, padded with trailing whitespace (still valid JSON) to an exact size.
    let padded = |len: usize| {
        let mut body =
            br#"{"ts": {"0": "2024-01-03T00:00:00+00:00"}, "id": {"0": 3}, "v": {"0": 1.5}}"#
                .to_vec();
        assert!(body.len() < len);
        body.resize(len, b' ');
        body
    };
    let path = "/pipes/a/b/[None]/data";
    let r = send(
        &app,
        req(
            "POST",
            path,
            Some(&admin),
            Some(("application/json", padded(1024))),
        ),
    )
    .await;
    assert_eq!(
        r.json(),
        json!([true, "Inserted 1, updated 0 rows."]),
        "{}",
        r.text()
    );
    let r = send(
        &app,
        req(
            "POST",
            path,
            Some(&admin),
            Some(("application/json", padded(1025))),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::PAYLOAD_TOO_LARGE, "{}", r.text());
    // The same body streamed without a declared length.
    let body = padded(1025);
    let (first, second) = body.split_at(600);
    let (first, second) = (first.to_vec(), second.to_vec());
    let request = Request::post(path)
        .header("authorization", &admin)
        .header("content-type", "application/json")
        .body(Body::from_stream(futures_util::stream::iter([
            Ok::<_, std::io::Error>(Bytes::from(first)),
            Ok(Bytes::from(second)),
        ])))
        .unwrap();
    let r = send(&app, request).await;
    assert_eq!(r.status, StatusCode::PAYLOAD_TOO_LARGE, "{}", r.text());
    // The other routes keep `body_bytes`: a 2 KiB registration body is read.
    let mut body = br#"{"columns": {"datetime": "ts", "id": "id"}}"#.to_vec();
    body.resize(2048, b' ');
    let r = send(
        &app,
        req(
            "POST",
            "/pipes/a/c/[None]/register",
            Some(&admin),
            Some(("application/json", body)),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
}

#[test]
fn unusable_limits_stop_the_server() {
    for (api, key) in [
        (json!({"limits": {"auth_bytes": -1}}), "auth_bytes"),
        (json!({"limits": {"body_bytes": "16MiB"}}), "body_bytes"),
        (json!({"limits": {"data_bytes": 0}}), "data_bytes"),
        (json!({"limits": {"proxy_bytes": 1e300}}), "proxy_bytes"),
        (json!({"limits": "large"}), "api:limits"),
        (
            json!({"host": {"response_seconds": -5}}),
            "response_seconds",
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config_with_api(dir.path(), api.clone());
        let instance = Arc::new(common::MemoryInstance::new("fixture"));
        let built = AppState::builder(cfg, instance)
            .secret(SECRET.to_vec())
            .build();
        let error = built
            .err()
            .unwrap_or_else(|| panic!("{api} was accepted"))
            .to_string();
        assert!(error.contains(key), "{api}: {error}");
    }
}

/// A fake API host on `socket`: `/plugin/size` answers how many body bytes it read,
/// `/plugin/slow` answers its headers at once and streams its body slowly, `/plugin/hang` never
/// answers.
async fn serve_host(socket: &Path) -> tokio::task::JoinHandle<()> {
    let _ = std::fs::remove_file(socket);
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    let app = Router::new()
        .route(
            "/plugin/size",
            post(|body: Body| async move {
                let mut stream = body.into_data_stream();
                let mut read = 0usize;
                while let Some(Ok(chunk)) = stream.next().await {
                    read += chunk.len();
                }
                format!("read {read}")
            }),
        )
        .route(
            "/plugin/slow",
            get(|| async {
                Body::from_stream(futures_util::stream::iter(0..3).then(|i| async move {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    Ok::<_, std::io::Error>(Bytes::from(format!("part{i};")))
                }))
            }),
        )
        .route(
            "/plugin/hang",
            get(|| async {
                std::future::pending::<()>().await;
                "never"
            }),
        );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    })
}

#[tokio::test]
async fn forwarded_bodies_are_counted_against_proxy_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("host.sock");
    let _host = serve_host(&socket).await;
    let app = app_for(common::config(dir.path()), Some(HostProxy::new(&socket)));
    let mib = 1024 * 1024;
    // A console callback or plugin upload of ordinary size passes, declared or chunked.
    let request = Request::post("/plugin/size")
        .header("content-type", "application/json")
        .body(Body::from(vec![b'a'; 256 * 1024]))
        .unwrap();
    let r = send(&app, request).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.text(), format!("read {}", 256 * 1024));
    let request = Request::post("/plugin/size")
        .body(chunked(4, 64 * 1024))
        .unwrap();
    let r = send(&app, request).await;
    assert_eq!(r.text(), format!("read {}", 256 * 1024));
    // Over the default 16 MiB, streamed: counted as it goes to the host, and the client gets
    // `413`, not `502`.
    let request = Request::post("/plugin/size")
        .body(chunked(17, mib))
        .unwrap();
    let r = tokio::time::timeout(Duration::from_secs(30), send(&app, request))
        .await
        .expect("no answer");
    assert_eq!(r.status, StatusCode::PAYLOAD_TOO_LARGE, "{}", r.text());
    assert!(r.text().contains("proxy_bytes"), "{}", r.text());
    // Declared over the limit: refused before the host is asked (the body never comes).
    let request = Request::post("/plugin/size")
        .header("content-length", (17 * mib).to_string())
        .body(never())
        .unwrap();
    let r = tokio::time::timeout(Duration::from_secs(10), send(&app, request))
        .await
        .expect("the proxy waited for a body it should have refused");
    assert_eq!(r.status, StatusCode::PAYLOAD_TOO_LARGE, "{}", r.text());
}

#[tokio::test]
async fn a_host_that_does_not_answer_is_a_gateway_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("host.sock");
    let _host = serve_host(&socket).await;
    let app = app_for(
        config_with_api(dir.path(), json!({"host": {"response_seconds": 0.3}})),
        Some(HostProxy::new(&socket)),
    );
    let r = tokio::time::timeout(
        Duration::from_secs(10),
        send(&app, req("GET", "/plugin/hang", None, None)),
    )
    .await
    .expect("the proxy waited for a host that never answers");
    assert_eq!(r.status, StatusCode::GATEWAY_TIMEOUT, "{}", r.text());
    // Only the wait for the headers is timed: a body that streams for longer is delivered.
    let r = send(&app, req("GET", "/plugin/slow", None, None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.text(), "part0;part1;part2;");
}
