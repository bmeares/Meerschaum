//! The Python API host end to end (§7.2, §7.3, §8.4): the Rust server supervises a real
//! `python -m meerschaum.api.host` serving the synthetic plugin in `tests/host_plugin/hostdemo`
//! and proxies to it: plugin routes (plain, scoped, streaming in both directions, websocket),
//! the plugin's middleware (applied to proxied requests only), a static mount, the plugin-owned
//! Dash app with the core stores, MCP federation (`tools/list` merges the plugin's tools and
//! `tools/call` forwards with the caller's scopes), JWTs issued by either side validating on the
//! other, the allowlist and rate limits on proxied paths, and the manifest seam (restart, stop).
//!
//! Needs the repository's Python with the API extras (`fastapi`, `uvicorn`, `dash`, `a2wsgi`,
//! `python-jose`, `fastapi-login`; v4 keeps some in `test_root/venvs/mrsm`). When they are
//! missing the test is skipped with a message, unless `MRSM_REQUIRE_HOST_E2E=1`.
#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::{login, req, send};
use futures_util::{SinkExt, StreamExt};
use meerschaum_server::config::RateLimit;
use meerschaum_server::host::{
    FileManifest, HostManifest, HostSupervisor, PythonHostLauncher, SupervisorOptions,
    default_socket_path, spawn_manifest_watcher,
};
use meerschaum_server::mcp::McpServer;
use meerschaum_server::proxy::HostProxy;
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value as Json, json};

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

fn python() -> PathBuf {
    PythonHostLauncher::python_from_env()
}

/// The environment of every Python process: the working tree (not a stale install), the
/// synthetic plugin, the shared v4 venvs (no installs), and the temporary root.
fn python_env(root: &Path) -> Vec<(String, String)> {
    let repo = repo();
    let mut env = vec![
        ("PYTHONPATH".into(), repo.display().to_string()),
        ("MRSM_ROOT_DIR".into(), root.display().to_string()),
        (
            "MRSM_PLUGINS_DIR".into(),
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/host_plugin")
                .display()
                .to_string(),
        ),
        ("MRSM_NO_AUTO_INSTALL".into(), "1".into()),
        ("PYTHONDONTWRITEBYTECODE".into(), "1".into()),
    ];
    let venvs = repo.join("test_root/venvs");
    if venvs.join("mrsm").is_dir() {
        env.push(("MRSM_VENVS_DIR".into(), venvs.display().to_string()));
    }
    env
}

fn run_python(root: &Path, code: &str) -> std::io::Result<std::process::Output> {
    std::process::Command::new(python())
        .args(["-c", code])
        .envs(python_env(root))
        .env("MRSM_API_HOST", "1")
        .current_dir(root)
        .output()
}

/// Whether the host's dependencies import (else the test is skipped).
fn host_available(root: &Path) -> Result<(), String> {
    let code = "import meerschaum.utils.venv as v; v.activate_venv('mrsm'); \
                import fastapi, uvicorn, dash, a2wsgi, jose, fastapi_login";
    match run_python(root, code) {
        Ok(o) if o.status.success() => Ok(()),
        Ok(o) => Err(String::from_utf8_lossy(&o.stderr).into_owned()),
        Err(e) => Err(e.to_string()),
    }
}

/// A JWT minted by the Python side (`fastapi_login`, as v4's `/login` does).
fn python_jwt(root: &Path, sub: &str, scopes: &[&str]) -> String {
    let code = format!(
        "from datetime import timedelta\n\
         from meerschaum.api import manager\n\
         print(manager.create_access_token(data={{'sub': {sub:?}, 'scopes': {scopes:?}, \
         'type': 'user'}}, expires=timedelta(minutes=5)))"
    );
    let out = run_python(root, &code).expect("python");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .last()
        .unwrap_or("")
        .trim()
        .to_string()
}

fn manifest(plugins: &[&str]) -> HostManifest {
    let names: Vec<String> = plugins.iter().map(|p| p.to_string()).collect();
    HostManifest {
        api_plugins: names.clone(),
        dash_plugins: names.clone(),
        web_pages: names.clone(),
        mcp_tools: names,
    }
}

fn write_manifest(path: &Path, m: &HostManifest) {
    let v = json!({
        "api_plugins": m.api_plugins,
        "dash_plugins": m.dash_plugins,
        "web_pages": m.web_pages,
        "mcp_tools": m.mcp_tools,
    });
    std::fs::write(path, v.to_string()).unwrap();
}

fn app_for(
    cfg: ServerConfig,
    instance: Arc<common::MemoryInstance>,
    proxy: Arc<HostProxy>,
) -> (Router, Arc<McpServer>) {
    let state = AppState::builder(cfg, instance)
        .host(proxy)
        .build()
        .expect("state");
    let mcp = Arc::new(McpServer::for_state(&state));
    (meerschaum_server::app_with_mcp(state, mcp.clone()), mcp)
}

async fn rpc(app: &Router, auth: &str, method: &str, params: Json) -> Json {
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    let r = send(
        app,
        req(
            "POST",
            "/mcp",
            Some(auth),
            Some(("application/json", body.to_string().into_bytes())),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    r.json()
}

fn tool_names(list: &Json) -> Vec<String> {
    list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect()
}

async fn wait_for<F, Fut>(what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..600 {
        if f().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn python_host_end_to_end() {
    let root_dir = tempfile::Builder::new()
        .prefix("mrsm-host-e2e")
        .tempdir()
        .unwrap();
    let root = root_dir.path().to_path_buf();
    if let Err(e) = host_available(&root) {
        let required = std::env::var("MRSM_REQUIRE_HOST_E2E").is_ok_and(|v| v == "1");
        assert!(!required, "the API host's dependencies are missing:\n{e}");
        eprintln!("SKIPPED: the API host's dependencies are missing:\n{e}");
        return;
    }

    // The server and the host share the root, so the same secret file (created by the server).
    let cfg = ServerConfig::for_root("sql:local", &root);
    // The test serves the API router only (no console); `--no-dash` would also switch off the
    // host's plugin Dash apps, which this test exercises.
    let instance = Arc::new(common::MemoryInstance::new("local"));
    instance.seed_user("admin", "pass", Some("admin"));
    instance.seed_user("reader", "pass", None);

    let mut launcher = PythonHostLauncher::for_config(&cfg, python());
    launcher.env.extend(python_env(&root));
    let socket = default_socket_path(&root);
    let supervisor = HostSupervisor::new(
        &manifest(&["hostdemo"]),
        Arc::new(launcher),
        &socket,
        SupervisorOptions {
            ready_timeout: Duration::from_secs(120),
            backoff_initial: Duration::from_millis(200),
            backoff_max: Duration::from_secs(2),
        },
    );
    let proxy = Arc::new(HostProxy::supervised(supervisor.clone()));
    let (app, mcp) = app_for(cfg.clone(), instance.clone(), proxy.clone());
    assert_eq!(supervisor.starts(), 0, "the host starts on demand");

    // Plugin routes, through the plugin's middleware.
    let r = send(&app, req("GET", "/hostdemo/hello", None, None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json(), json!({"message": "Hello from hostdemo!"}));
    assert_eq!(r.headers["x-hostdemo"], "middleware");
    assert_eq!(supervisor.starts(), 1);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(socket.parent().unwrap())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700, "the socket's directory is private");
    }

    // Meerschaum's own routes are not wrapped by plugin middleware.
    let r = send(&app, req("GET", "/version", None, None)).await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(r.headers.get("x-hostdemo").is_none());

    // Method, path, query, headers, cookies and body arrive unchanged.
    let r = send(
        &app,
        Request::builder()
            .method("PATCH")
            .uri("/hostdemo/echo?a=1&b=%20x")
            .header("cookie", "flavor=mint; other=1")
            .header("x-demo", "yes")
            .body(Body::from("payload"))
            .unwrap(),
    )
    .await;
    assert_eq!(
        r.json(),
        json!({
            "method": "PATCH", "path": "/hostdemo/echo", "query": "a=1&b=%20x",
            "cookie": "mint", "header": "yes", "body": "payload",
        })
    );

    // Streaming bodies in both directions.
    let r = send(&app, req("GET", "/hostdemo/stream", None, None)).await;
    assert_eq!(r.text(), "chunk 0\nchunk 1\nchunk 2\n");
    let chunks = futures_util::stream::iter(
        (0..16).map(|_| Ok::<_, std::io::Error>(bytes::Bytes::from(vec![b'x'; 65536]))),
    );
    let r = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/hostdemo/upload")
            .body(Body::from_stream(chunks))
            .unwrap(),
    )
    .await;
    assert_eq!(r.text(), (16 * 65536).to_string());

    // A static mount.
    let r = send(&app, req("GET", "/hostdemo/static/hello.txt", None, None)).await;
    assert_eq!(r.text(), "hello from a static mount\n");

    // The plugin's Dash app, with the core stores and the plugin's assets.
    let r = send(&app, req("GET", "/dash/hostdemo/", None, None)).await;
    assert_eq!(r.status, StatusCode::OK);
    let html = r.text();
    assert!(html.contains("id=\"mrsm-theme-dark\""), "{html}");
    assert!(
        html.contains("/dash/hostdemo/assets/hostdemo.css"),
        "{html}"
    );
    let r = send(&app, req("GET", "/dash/hostdemo/_dash-layout", None, None)).await;
    let layout = r.text();
    for id in ["session-store", "mrsm-location", "dbc-dark-store"] {
        assert!(layout.contains(&format!("\"{id}\"")), "{id}: {layout}");
    }
    let r = send(
        &app,
        req("GET", "/dash/hostdemo/_dash-dependencies", None, None),
    )
    .await;
    assert!(r.text().contains("hostdemo-session"), "{}", r.text());

    // JWTs issued by the Rust server validate in the host.
    let admin = login(&app, "admin", "pass", Some("hostdemo:read+pipes:read")).await;
    let r = send(&app, req("GET", "/hostdemo/whoami", Some(&admin), None)).await;
    assert_eq!(r.json(), json!({"username": "admin"}), "{}", r.text());
    let r = send(&app, req("GET", "/hostdemo/scoped", Some(&admin), None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json(), json!({"scopes": ["hostdemo:read", "pipes:read"]}));
    let reader = login(&app, "admin", "pass", Some("pipes:read")).await;
    let r = send(&app, req("GET", "/hostdemo/scoped", Some(&reader), None)).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = send(&app, req("GET", "/hostdemo/whoami", None, None)).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED, "{}", r.text());

    // ... and JWTs issued by the Python side validate in the Rust server, with their scopes.
    let py_jwt = format!("Bearer {}", python_jwt(&root, "admin", &["hostdemo:read"]));
    let r = send(&app, req("GET", "/mcp", Some(&py_jwt), None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let info = r.json();
    let names: Vec<&str> = info["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["hostdemo_add"]);

    // MCP federation: the plugin's tools merge into `tools/list` per the caller's scopes ...
    let full = login(&app, "admin", "pass", None).await;
    let all = tool_names(&rpc(&app, &full, "tools/list", json!({})).await);
    for t in [
        "hostdemo_add",
        "hostdemo_reset",
        "list_pipes",
        "register_pipe",
    ] {
        assert!(all.contains(&t.to_string()), "{t} in {all:?}");
    }
    let mut sorted = all.clone();
    sorted.sort();
    assert_eq!(all, sorted, "tools/list is sorted by name");
    let read = tool_names(&rpc(&app, &reader, "tools/list", json!({})).await);
    assert!(!read.iter().any(|t| t.starts_with("hostdemo")), "{read:?}");
    assert!(read.contains(&"list_pipes".to_string()));

    // ... and calls are forwarded to the host with the caller's scopes.
    let r = rpc(
        &app,
        &admin,
        "tools/call",
        json!({"name": "hostdemo_add", "arguments": {"a": 2, "b": 3}}),
    )
    .await;
    assert_eq!(r["result"]["isError"], json!(false), "{r}");
    assert_eq!(r["result"]["structuredContent"], json!({"sum": 5}));
    let r = rpc(
        &app,
        &admin,
        "tools/call",
        json!({"name": "hostdemo_reset", "arguments": {}}),
    )
    .await;
    assert_eq!(r["result"]["isError"], json!(true));
    assert_eq!(
        r["result"]["content"][0]["text"],
        "Missing required scope for 'hostdemo_reset': hostdemo:write."
    );
    let r = rpc(
        &app,
        &admin,
        "tools/call",
        json!({"name": "hostdemo_add", "arguments": {"a": 1}}),
    )
    .await;
    assert_eq!(
        r["result"]["content"][0]["text"],
        "Missing required argument 'b'."
    );
    let r = rpc(&app, &full, "resources/list", json!({})).await;
    assert!(r.to_string().contains("hostdemo://readme"), "{r}");
    let r = rpc(
        &app,
        &admin,
        "resources/read",
        json!({"uri": "hostdemo://readme"}),
    )
    .await;
    assert_eq!(
        r["result"]["contents"][0]["text"],
        "Hostdemo is a synthetic plugin."
    );
    let r = rpc(
        &app,
        &admin,
        "prompts/get",
        json!({"name": "hostdemo_greet", "arguments": {"who": "Ada"}}),
    )
    .await;
    assert_eq!(
        r["result"]["messages"][0]["content"]["text"],
        "Say hello to Ada."
    );

    // The host's internal routes are never reachable from clients.
    let r = send(&app, req("GET", "/_mrsm/host/registry", Some(&full), None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert!(r.unmatched);

    // Websockets are bridged.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served = app.clone();
    tokio::spawn(async move { axum::serve(listener, served).await.unwrap() });
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/hostdemo/ws"))
        .await
        .unwrap();
    use tokio_tungstenite::tungstenite::Message as M;
    for word in ["a", "bb"] {
        ws.send(M::Text(word.into())).await.unwrap();
        let reply = ws.next().await.unwrap().unwrap();
        assert_eq!(reply, M::Text(format!("echo: {word}").into()));
    }
    drop(ws);

    // The allowlist and rate limits apply to proxied paths, on the Rust side.
    let mut limited = cfg.clone();
    limited.routes_allowlist = vec!["version".into(), "misc".into()];
    limited.rate_limits.insert(
        "other".into(),
        RateLimit {
            requests: 2,
            per: Duration::from_secs(3600),
        },
    );
    let (limited_app, _) = app_for(limited, instance.clone(), proxy.clone());
    for _ in 0..2 {
        let r = send(&limited_app, req("GET", "/hostdemo/hello", None, None)).await;
        assert_eq!(r.status, StatusCode::OK);
    }
    let r = send(&limited_app, req("GET", "/hostdemo/hello", None, None)).await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS);
    // A core group outside the allowlist is not served by a plugin either.
    let r = send(&limited_app, req("GET", "/pipes/keys", None, None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert!(r.unmatched, "not forwarded to the host");

    // The manifest seam: a change restarts the host and refetches the registry; a manifest
    // without host plugins stops it.
    let manifest_path = root.join("manifest.json");
    write_manifest(&manifest_path, &manifest(&["hostdemo"]));
    let watcher = spawn_manifest_watcher(
        Arc::new(FileManifest(manifest_path.clone())),
        supervisor.clone(),
        mcp.federation.clone(),
        Duration::from_millis(100),
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        supervisor.starts(),
        1,
        "an unchanged manifest keeps the host"
    );
    let mut changed = manifest(&["hostdemo"]);
    changed.web_pages.clear();
    write_manifest(&manifest_path, &changed);
    wait_for("the restart", || async { supervisor.starts() == 2 }).await;
    let r = send(&app, req("GET", "/hostdemo/hello", None, None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());

    write_manifest(&manifest_path, &HostManifest::default());
    wait_for("the stop", || async { !supervisor.is_running().await }).await;
    let r = send(&app, req("GET", "/hostdemo/hello", None, None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert!(r.unmatched);
    let all = tool_names(&rpc(&app, &full, "tools/list", json!({})).await);
    assert!(!all.iter().any(|t| t.starts_with("hostdemo")), "{all:?}");
    assert_eq!(
        supervisor.starts(),
        2,
        "no plugin needs the host: it is not restarted"
    );

    watcher.abort();
    supervisor.shutdown().await;
}
