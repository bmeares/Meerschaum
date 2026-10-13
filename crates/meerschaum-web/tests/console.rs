#[path = "../../meerschaum-server/tests/common/mod.rs"]
mod common;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use common::{config, send, state_with};
use meerschaum_web::{
    PageContent, PluginPage, WebConfig,
    sessions::{InstanceSessions, SessionStore},
};
use serde_json::json;
use std::sync::Arc;

async fn login(app: &axum::Router) -> (String, String) {
    let response = send(
        app,
        Request::post("/dash/login")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("username=admin&password=pass"))
            .unwrap(),
    )
    .await;
    assert_eq!(
        response.status,
        StatusCode::SEE_OTHER,
        "{}",
        response.text()
    );
    let cookie = response.headers["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    assert!(
        response.headers["set-cookie"]
            .to_str()
            .unwrap()
            .contains("HttpOnly; SameSite=Lax")
    );
    let response = send(
        app,
        Request::get("/dash")
            .header("cookie", &cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let html = response.text();
    let csrf = html
        .split("name=\"csrf\" value=\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap()
        .to_string();
    (cookie, csrf)
}

#[tokio::test]
async fn login_pages_fragments_csrf_logout() {
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();
    let response = send(&app, Request::get("/dash").body(Body::empty()).unwrap()).await;
    assert_eq!(response.status, StatusCode::SEE_OTHER);
    assert_eq!(response.headers["location"], "/dash/login");
    let (cookie, csrf) = login(&app).await;
    for page in [
        "/dash",
        "/dash/pipes",
        "/dash/jobs",
        "/dash/plugins",
        "/dash/users",
        "/dash/tokens",
        "/dash/settings",
        "/dash/pipelines",
        "/dash/webterm",
    ] {
        let response = send(
            &app,
            Request::get(page)
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(
            response.status,
            StatusCode::OK,
            "{page}: {}",
            response.text()
        );
        assert!(response.text().contains("<main id=\"content\""));
        assert!(response.text().contains("<nav aria-label=\"Console\">"));
        assert_eq!(response.headers["cache-control"], "no-store");
    }
    let fragment = send(
        &app,
        Request::get("/dash/pipes")
            .header("cookie", &cookie)
            .header("hx-request", "true")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert!(fragment.text().starts_with("<h1>"));
    assert!(!fragment.text().contains("<html"));
    let denied = send(
        &app,
        Request::post("/dash/logout")
            .header("cookie", &cookie)
            .body(Body::from("csrf=wrong"))
            .unwrap(),
    )
    .await;
    assert_eq!(denied.status, StatusCode::FORBIDDEN);
    let response = send(
        &app,
        Request::post("/dash/logout")
            .header("cookie", &cookie)
            .body(Body::from(format!("csrf={csrf}")))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status, StatusCode::SEE_OTHER);
    assert!(
        response.headers["set-cookie"]
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );
    let response = send(
        &app,
        Request::get("/dash")
            .header("cookie", &cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status, StatusCode::SEE_OTHER);
}

#[tokio::test]
async fn plugin_contract_redirects_and_dash_ownership() {
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let mut cfg = WebConfig::for_state(&state);
    cfg.dash_plugins.push("weather".into());
    cfg.legacy_pages.insert("forecast".into(), "weather".into());
    cfg.legacy_pages
        .insert("weather/history".into(), "weather".into());
    cfg.pages.push(PluginPage {
        path: "report".into(),
        page_group: "Reports".into(),
        login_required: false,
        skip_navbar: false,
        dark_theme: None,
        content: PageContent::Html(
            "<section data-mrsm-figure='{\"data\":[]}'><h2>Report</h2></section>".into(),
        ),
    });
    let app = meerschaum_web::app(state.clone(), cfg.clone()).unwrap();
    let response = send(
        &app,
        Request::get("/dash/report").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK);
    assert!(response.text().contains("data-mrsm-figure="));
    let response = send(
        &app,
        Request::get("/dash/forecast?day=1")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.headers["location"], "/dash/weather/forecast?day=1");
    for path in [
        "/dash/weather/history",
        "/dash/weather/_dash-layout",
        "/dash/unknown",
    ] {
        let response = send(&app, Request::get(path).body(Body::empty()).unwrap()).await;
        assert!(response.unmatched, "{path} was intercepted");
    }
    assert_eq!(
        meerschaum_web::legacy_target("weather", "weather/history"),
        "/dash/weather/history"
    );
    cfg.pages[0].path = "weather/bad".into();
    assert!(meerschaum_web::router(state, cfg).is_err());
}

#[tokio::test]
async fn session_expiry_and_shared_instances() {
    let root = tempfile::tempdir().unwrap();
    let (_, instance) = state_with(config(root.path()));
    let rust = InstanceSessions::new(instance.clone());
    let host = InstanceSessions::new(instance);
    let id = uuid::Uuid::new_v4().to_string();
    rust.set(&id, json!({"username":"admin"}), 100)
        .await
        .unwrap();
    assert_eq!(host.get(&id).await.unwrap().unwrap()["username"], "admin");
    host.set(&id, json!({"username":"admin", "admin":true}), 100)
        .await
        .unwrap();
    assert_eq!(rust.get(&id).await.unwrap().unwrap()["admin"], true);
    host.delete(&id).await.unwrap();
    assert!(rust.get(&id).await.unwrap().is_none());
    rust.set(&id, json!({"username":"admin"}), -1)
        .await
        .unwrap();
    assert!(rust.get(&id).await.unwrap().is_none());
}

#[tokio::test]
async fn python_host_reads_and_writes_sqlite_sessions() {
    use meerschaum_sql::{instance::SqlConnector, uri::ConnectionSpec};
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let path = root.path().join("web_sessions.sqlite");
    let spec = ConnectionSpec::from_uri(
        &format!("sqlite:///{}", path.display()),
        Some("web_sessions"),
        None,
    )
    .unwrap();
    let instance = Arc::new(SqlConnector::connect("web_sessions", spec).await.unwrap());
    let store = InstanceSessions::new(instance.clone());
    let id = uuid::Uuid::new_v4().to_string();
    store
        .set(&id, json!({"username":"synthetic"}), 100)
        .await
        .unwrap();
    let output = std::process::Command::new("python3").args(["-c", r#"
import json, sqlite3, sys, time
conn = sqlite3.connect(sys.argv[1])
row = conn.execute('SELECT data, expires FROM mrsm_sessions WHERE session_id = ?', (sys.argv[2],)).fetchone()
assert json.loads(row[0]) == {'username':'synthetic'}
assert row[1] > time.time()
conn.execute('UPDATE mrsm_sessions SET data = ? WHERE session_id = ?', (json.dumps({'username':'python-host','admin':False}), sys.argv[2]))
conn.commit()
"#, path.to_str().unwrap(), &id]).env("MRSM_ROOT_DIR", root.path()).env("MRSM_PLUGINS_DIR", plugins.path()).env("PYTHONPATH", std::env::current_dir().unwrap()).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        store.get(&id).await.unwrap().unwrap()["username"],
        "python-host"
    );
    store.delete(&id).await.unwrap();
    assert!(store.get(&id).await.unwrap().is_none());
}

#[tokio::test]
async fn assets_are_local_and_no_dash_is_respected() {
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();
    for path in [
        "htmx.min.js",
        "plotly.min.js",
        "maplibre-gl.js",
        "cytoscape.min.js",
        "xterm.js",
    ] {
        let response = send(
            &app,
            Request::get(format!("/dash/assets/{path}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.headers["content-type"], "text/javascript");
        assert!(response.body.len() > 1000);
    }
    let mut cfg = config(root.path());
    cfg.no_dash = true;
    let (state, _) = state_with(cfg);
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();
    let response = send(&app, Request::get("/dash").body(Body::empty()).unwrap()).await;
    assert!(response.unmatched);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_browser_renders_page_contract() {
    let browser = [
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
        "/usr/bin/google-chrome",
    ]
    .into_iter()
    .find(|p| std::path::Path::new(p).exists());
    let Some(browser) = browser else {
        eprintln!("No installed Chromium; smoke test skipped");
        return;
    };
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let mut cfg = WebConfig::for_state(&state);
    cfg.pages.push(PluginPage { path:"smoke".into(), page_group:"Smoke".into(), login_required:false, skip_navbar:false, dark_theme:None, content:PageContent::Html("<div data-mrsm-figure='{\"data\":[{\"x\":[1,2],\"y\":[2,3],\"type\":\"scatter\"}]}'></div>".into()) });
    let app = meerschaum_web::app(state, cfg).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(45),
        tokio::process::Command::new(browser)
            .args([
                "--headless",
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--disable-background-networking",
                "--no-first-run",
                "--no-default-browser-check",
                "--dump-dom",
                "--virtual-time-budget=10000",
            ])
            .arg(format!(
                "--user-data-dir={}",
                root.path().join("browser").display()
            ))
            .arg(format!("http://{address}/dash/smoke"))
            .env("HOME", root.path())
            .env("XDG_CONFIG_HOME", root.path())
            .env("XDG_CACHE_HOME", root.path())
            .env("MRSM_ROOT_DIR", root.path())
            .env("MRSM_PLUGINS_DIR", plugins.path())
            .kill_on_drop(true)
            .output(),
    )
    .await;
    server.abort();
    let output = output.expect("browser timeout").expect("browser launch");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let dom = String::from_utf8_lossy(&output.stdout);
    assert!(
        dom.contains("data-rendered=\"true\""),
        "Plotly did not render: {dom}"
    );
    assert!(dom.contains("main id=\"content\""));
}

#[tokio::test]
async fn cache_protocol_matches_python_sessions() {
    use meerschaum_valkey::{MemoryBackend, backend::ValkeyBackend};
    use meerschaum_web::sessions::ValkeySessions;
    let cache = Arc::new(MemoryBackend::default());
    let store = ValkeySessions(cache.clone());
    let id = uuid::Uuid::new_v4().to_string();
    cache
        .set_ex(
            &format!("mrsm_session_id:{id}"),
            r#"{"username":"admin","admin":true}"#,
            100,
        )
        .await
        .unwrap();
    assert_eq!(store.get(&id).await.unwrap().unwrap()["username"], "admin");
    store
        .set(&id, json!({"username":"rust"}), 100)
        .await
        .unwrap();
    assert_eq!(
        cache
            .get(&format!("mrsm_session_id:{id}"))
            .await
            .unwrap()
            .unwrap(),
        r#"{"username":"rust"}"#
    );
    store.delete(&id).await.unwrap();
    assert!(store.get(&id).await.unwrap().is_none());
}

#[tokio::test]
async fn pipe_filters_escaping_and_mutations_use_api() {
    use meerschaum_connector::InstanceConnector;
    use meerschaum_core::Pipe;
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = state_with(config(root.path()));
    for (metric, tag) in [("temperature", "weather"), ("humidity", "other")] {
        let pipe = Pipe::builder("plugin:synthetic", metric).instance("sql:fixture").parameters(serde_json::from_value(json!({"tags":[tag],"note":"<script>alert(1)</script>","columns":{"datetime":"time"},"dtypes":{"time":"int","value":"int"}})).unwrap()).build().unwrap();
        assert!(instance.register_pipe(&pipe).await.success);
    }
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();
    let (cookie, csrf) = login(&app).await;
    let response = send(
        &app,
        Request::get("/dash/pipes?tags=weather")
            .header("cookie", &cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK);
    assert!(response.text().contains("temperature"));
    assert!(!response.text().contains("humidity"));
    assert!(response.text().contains("&lt;script&gt;"));
    assert!(!response.text().contains("<script>alert"));
    let response = send(
        &app,
        Request::get("/dash/pipes?metric_keys=_temperature")
            .header("cookie", &cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert!(response.text().contains("<h2>plugin:synthetic / humidity"));
    assert!(
        !response
            .text()
            .contains("<h2>plugin:synthetic / temperature")
    );
    let body = format!(
        "csrf={csrf}&op=pipe-sync&target=%2Fpipes%2Fplugin%253Asynthetic%2Ftemperature%2FNone&payload=%5B%7B%22time%22%3A1%2C%22value%22%3A2%7D%5D"
    );
    let response = send(
        &app,
        Request::post("/dash/action")
            .header("cookie", &cookie)
            .body(Body::from(body))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert!(response.text().contains("true"), "{}", response.text());
    let response = send(
        &app,
        Request::get("/dash/pipes?preview=%2Fpipes%2Fplugin%253Asynthetic%2Ftemperature%2FNone")
            .header("cookie", &cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert!(
        response.text().contains("data-mrsm-figure="),
        "{}",
        response.text()
    );
    let response = send(
        &app,
        Request::post("/dash/action")
            .header("cookie", &cookie)
            .body(Body::from(format!(
                "csrf={csrf}&op=pipe-edit&target=%2Fusers%2Fadmin&payload=%7B%7D"
            )))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
}

#[cfg(unix)]
#[tokio::test]
async fn rust_login_is_seen_by_python_dash_host() {
    use meerschaum_server::{AppState, proxy::HostProxy};
    use meerschaum_sql::{instance::SqlConnector, uri::ConnectionSpec};
    use tokio::io::{AsyncBufReadExt, BufReader};
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let database = root.path().join("web_host.sqlite");
    let socket = root.path().join("host.sock");
    let spec = ConnectionSpec::from_uri(
        &format!("sqlite:///{}", database.display()),
        Some("web_host"),
        None,
    )
    .unwrap();
    let instance = Arc::new(SqlConnector::connect("web_host", spec).await.unwrap());
    let mut cfg = meerschaum_server::ServerConfig::for_root("sql:web_host", root.path());
    cfg.no_auth = true;
    let state = AppState::builder(cfg, instance)
        .secret(common::SECRET.to_vec())
        .host(Arc::new(HostProxy::new(&socket)))
        .build()
        .unwrap();
    let mut cfg = WebConfig::for_state(&state);
    cfg.dash_plugins.push("synthetic".into());
    cfg.pages.push(PluginPage {
        path: "host-fragment".into(),
        page_group: "Test".into(),
        login_required: true,
        skip_navbar: false,
        dark_theme: None,
        content: PageContent::Route("/fragment".into()),
    });
    let app = meerschaum_web::app(state, cfg).unwrap();
    let mut child = tokio::process::Command::new("python3").args(["-u", "-c", r#"
import http.server, socketserver, sqlite3, json, sys
from http.cookies import SimpleCookie
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_GET(self):
        cookie = SimpleCookie(self.headers.get('Cookie', ''))
        sid = cookie.get('mrsm-session-id')
        conn = sqlite3.connect(sys.argv[2])
        row = conn.execute('SELECT data FROM mrsm_sessions WHERE session_id=?', (sid.value if sid else '',)).fetchone()
        user = json.loads(row[0])['username'] if row else 'anonymous'
        body = ('<section><h2>Python '+user+'</h2><div data-mrsm-figure=\'{"data":[]}\'></div></section>').encode()
        self.send_response(200); self.send_header('Content-Type','text/html'); self.send_header('Content-Length',str(len(body))); self.end_headers(); self.wfile.write(body)
server = socketserver.UnixStreamServer(sys.argv[1], Handler)
print('ready', flush=True)
server.serve_forever()
"#, socket.to_str().unwrap(), database.to_str().unwrap()]).env("MRSM_ROOT_DIR", root.path()).env("MRSM_PLUGINS_DIR", plugins.path()).env("PYTHONPATH", std::env::current_dir().unwrap()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).kill_on_drop(true).spawn().unwrap();
    let mut ready = String::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        BufReader::new(child.stdout.take().unwrap()).read_line(&mut ready),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(ready.trim(), "ready");
    let response = send(
        &app,
        Request::post("/dash/login")
            .body(Body::from("username=synthetic&password=test"))
            .unwrap(),
    )
    .await;
    assert_eq!(
        response.status,
        StatusCode::SEE_OTHER,
        "{}",
        response.text()
    );
    let cookie = response.headers["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    for path in ["/dash/synthetic/", "/dash/host-fragment"] {
        let response = send(
            &app,
            Request::get(path)
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK);
        assert!(
            response.text().contains("Python synthetic"),
            "{}",
            response.text()
        );
        if path == "/dash/host-fragment" {
            assert!(response.text().contains("<nav"));
        }
    }
    child.kill().await.unwrap();
    child.wait().await.unwrap();
}

#[tokio::test]
async fn registration_logs_in_and_rejects_cross_site_forms() {
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();
    let denied = send(
        &app,
        Request::post("/dash/login")
            .header("origin", "https://external.invalid")
            .header("host", "localhost")
            .body(Body::from("username=admin&password=pass"))
            .unwrap(),
    )
    .await;
    assert_eq!(denied.status, StatusCode::FORBIDDEN);
    let registered = send(
        &app,
        Request::post("/dash/register")
            .body(Body::from("username=synthetic_new&password=pass"))
            .unwrap(),
    )
    .await;
    assert_eq!(
        registered.status,
        StatusCode::SEE_OTHER,
        "{}",
        registered.text()
    );
    assert_eq!(registered.headers["location"], "/dash");
    assert!(registered.headers.contains_key("set-cookie"));
}

#[tokio::test]
async fn sessions_cannot_be_exposed_through_pipe_aliases() {
    use meerschaum_connector::InstanceConnector;
    use meerschaum_core::Pipe;
    let root = tempfile::tempdir().unwrap();
    let mut cfg = config(root.path());
    cfg.no_auth = true;
    let (state, instance) = state_with(cfg);
    let pipe = Pipe::builder("plugin:synthetic", "session-alias")
        .instance("sql:fixture")
        .parameters(serde_json::from_value(json!({"target":"mrsm_sessions"})).unwrap())
        .build()
        .unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();
    for suffix in ["data", "docs", "csv", "attributes", "rowcount"] {
        let response = send(
            &app,
            Request::get(format!(
                "/pipes/plugin:synthetic/session-alias/None/{suffix}"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await;
        assert_eq!(
            response.status,
            StatusCode::FORBIDDEN,
            "{suffix}: {}",
            response.text()
        );
    }
    let response = send(
        &app,
        Request::post("/pipes/plugin:synthetic/new/None/register")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"target":"MRSM_SESSIONS"}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn pipeline_nodes_show_newest_run_status() {
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let pipe_ref = "sql:fixture|plugin:synthetic|temperature|None";
    state.engine.record_runs(serde_json::from_value(json!([
        {"run_id":"old","pipe_ref":pipe_ref,"started":"2026-01-01T00:00:00Z","finished":"2026-01-01T01:00:00Z","status":"failed"},
        {"run_id":"new","pipe_ref":pipe_ref,"started":"2026-01-02T00:00:00Z","finished":"2026-01-02T01:00:00Z","status":"success"}
    ])).unwrap()).await;
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();
    let (cookie, _) = login(&app).await;
    let response = send(
        &app,
        Request::get("/dash/pipelines")
            .header("cookie", cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK);
    assert!(response.text().contains(&format!("{pipe_ref}: success")));
    assert!(!response.text().contains(&format!("{pipe_ref}: failed")));
    assert!(response.text().contains("data-mrsm-figure="));
}
