//! `mrsm start api` and the web console (D21): the Dash console is the Python API host's, so the
//! server proxies `/dash` to it and, without a host that mounted it, shows the pointer page. The
//! web terminal's assets and the native-page fragments stay Rust routes. A temporary root and a
//! SQLite instance, driven in-process (no sockets, no port, no Python host).

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use meerschaum_cli::dispatch::Runtime;
use meerschaum_cli::native::{self, Session};
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_server::mcp::McpServer;
use meerschaum_server::{AppState, ServerConfig, ServerFlags};
use tower::ServiceExt;

struct Fixture {
    _root: tempfile::TempDir,
    _plugins: tempfile::TempDir,
    app: axum::Router,
}

fn app(flags: ServerFlags) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let config = Config::new(ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
        (
            "MRSM_PLUGINS_DIR".into(),
            plugins.path().display().to_string(),
        ),
    ]))
    .unwrap();
    let mut out = Vec::new();
    let mut rt = Runtime::system(&mut out);
    rt.config = Some(config);
    let session = Session::new();
    rt.native = Some(session.clone());
    let server_config = ServerConfig::from_config(rt.config(), &flags);
    let instance = session
        .instance(&server_config.instance_keys, rt.config())
        .unwrap();
    let manifest = native::session(&mut rt).manifest(rt.config()).unwrap();
    let state: AppState = AppState::builder(server_config, instance.conn())
        .build()
        .unwrap();
    let console = meerschaum_cli::server::console(&state, &manifest, &mut rt).unwrap();
    let mcp = Arc::new(McpServer::for_state(&state));
    let app = meerschaum_server::app_with_mcp_and_console(state, mcp, console);
    // The runtime (and its worker pool) must outlive the in-process requests.
    std::mem::forget(rt);
    std::mem::forget(session);
    Fixture {
        _root: root,
        _plugins: plugins,
        app,
    }
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Option<String>, String) {
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let location = response
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        location,
        String::from_utf8_lossy(&body).into_owned(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn dash_shows_the_pointer_page_without_a_python_console() {
    let fixture = tokio::task::spawn_blocking(|| app(ServerFlags::default()))
        .await
        .unwrap();
    let app = &fixture.app;
    for path in ["/dash", "/dash/"] {
        let (status, _, body) = get(app, path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert!(body.contains("pip install meerschaum[dash]"), "{body}");
    }
    // The Rust server has no console pages of its own any more.
    for path in [
        "/dash/login",
        "/dash/pipes",
        "/dash/pipelines",
        "/dash/assets/xterm.js",
    ] {
        let (status, _, _) = get(app, path).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
    }
    // PARITY: v4's index redirects to the console.
    let (status, location, _) = get(app, "/").await;
    assert!(status.is_redirection());
    assert_eq!(location.as_deref(), Some("/dash"));
    // The API routes are still served.
    let (status, _, body) = get(app, "/healthcheck").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // The web terminal's bundle is served next to them (outside `/dash`, which is Dash's).
    let (status, _, body) = get(app, "/webterm/assets/xterm.js").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.len() > 1000);
    // The native-page fragments need the host's internal token.
    let (status, _, _) = get(app, "/_mrsm/native-pages").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "no native page, no route");
}

#[tokio::test(flavor = "multi_thread")]
async fn no_dash_serves_no_console() {
    let fixture = tokio::task::spawn_blocking(|| {
        app(ServerFlags {
            no_dash: true,
            ..ServerFlags::default()
        })
    })
    .await
    .unwrap();
    let app = &fixture.app;
    let (status, _, _) = get(app, "/dash/login").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // PARITY: without the console the index redirects to the docs.
    let (status, location, _) = get(app, "/").await;
    assert!(status.is_redirection());
    assert_eq!(location.as_deref(), Some("/docs"));
}
