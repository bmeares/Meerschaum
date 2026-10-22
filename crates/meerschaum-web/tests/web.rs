//! What the Rust server keeps of the web console (D21): the shared session store, the web
//! terminal's page and assets, the job-log stream and native plugin pages as fragments.
#[path = "../../meerschaum-server/tests/common/mod.rs"]
mod common;
use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use common::{config, send, state_with};
use meerschaum_core::{Map, SuccessTuple};
use meerschaum_server::{
    AppState,
    backends::{CreateJob, JobsBackend, LogEvent, LogMonitor},
};
use meerschaum_web::{
    PageContent, PageHandler, PluginPage, ShellCommand, WebConfig, WebtermSettings,
    sessions::{InstanceSessions, SessionStore},
};
use serde_json::{Value as Json, json};
use std::sync::{Arc, Mutex};

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
    use meerschaum_connector_sql::{instance::SqlConnector, uri::ConnectionSpec};
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
async fn cache_protocol_matches_python_sessions() {
    use meerschaum_connector_valkey::{MemoryBackend, backend::ValkeyBackend};
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

fn terminal_config(state: &AppState) -> WebConfig {
    let mut web = WebConfig::for_state(state);
    web.webterm = Some(WebtermSettings::new(ShellCommand::new("sh")));
    web
}

fn asset_on_disk(name: &str) -> Vec<u8> {
    std::fs::read(format!("{}/assets/{name}", env!("CARGO_MANIFEST_DIR"))).unwrap()
}

fn gunzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Read;
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_end(&mut out)
        .unwrap();
    out
}

#[tokio::test]
async fn the_terminal_assets_are_embedded_compressed_and_negotiated() {
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let app = meerschaum_web::app(state.clone(), terminal_config(&state)).unwrap();
    let get = |name: &str, accept: Option<&str>, none_match: Option<&str>| {
        let mut request = Request::get(format!("/webterm/assets/{name}"));
        if let Some(accept) = accept {
            request = request.header("accept-encoding", accept);
        }
        if let Some(tag) = none_match {
            request = request.header("if-none-match", tag);
        }
        request.body(Body::empty()).unwrap()
    };
    let raw = asset_on_disk("xterm.js");

    // A client that accepts gzip gets the embedded bytes untouched.
    let gz = send(&app, get("xterm.js", Some("br, gzip;q=0.8"), None)).await;
    assert_eq!(gz.status, StatusCode::OK);
    assert_eq!(gz.headers["content-encoding"], "gzip");
    assert_eq!(gz.headers["content-type"], "text/javascript");
    assert_eq!(gz.headers["vary"], "Accept-Encoding");
    assert_eq!(gz.headers["cache-control"], "no-cache");
    assert_eq!(gz.headers["content-length"], gz.body.len().to_string());
    assert!(gz.body.len() * 2 < raw.len(), "{} bytes", gz.body.len());
    assert_eq!(gunzip(&gz.body), raw);
    let gz_tag = gz.headers["etag"].to_str().unwrap().to_string();
    assert!(
        gz_tag.starts_with('"') && gz_tag.ends_with("-gzip\""),
        "{gz_tag}"
    );

    // Everyone else gets the file itself, with its own validator.
    for accept in [None, Some("identity"), Some("gzip;q=0"), Some("br")] {
        let plain = send(&app, get("xterm.js", accept, None)).await;
        assert_eq!(plain.status, StatusCode::OK, "{accept:?}");
        assert!(
            plain.headers.get("content-encoding").is_none(),
            "{accept:?}"
        );
        assert_eq!(plain.headers["content-length"], raw.len().to_string());
        assert_eq!(plain.body, raw, "{accept:?}");
        assert_ne!(plain.headers["etag"].to_str().unwrap(), gz_tag);
    }

    // Revalidation: a matching tag is a 304 with no body, a tag of the other encoding is not.
    let cached = send(&app, get("xterm.js", Some("gzip"), Some(&gz_tag))).await;
    assert_eq!(cached.status, StatusCode::NOT_MODIFIED);
    assert!(cached.body.is_empty());
    let other = send(&app, get("xterm.js", None, Some(&gz_tag))).await;
    assert_eq!(other.status, StatusCode::OK);

    // Every file the terminal page loads is there, with the right type.
    for (name, mime) in [
        ("xterm.css", "text/css"),
        ("webterm.css", "text/css"),
        ("addon-fit.js", "text/javascript"),
        ("addon-unicode11.js", "text/javascript"),
        ("webterm.js", "text/javascript"),
    ] {
        let r = send(&app, get(name, Some("gzip"), None)).await;
        assert_eq!(r.status, StatusCode::OK, "{name}");
        assert_eq!(r.headers["content-type"], mime, "{name}");
        assert_eq!(gunzip(&r.body), asset_on_disk(name), "{name}");
    }
    // The console's libraries are the Dash app's now: nothing else is served here.
    for gone in [
        "plotly.min.js",
        "htmx.min.js",
        "maplibre-gl.mjs",
        "cytoscape.min.js",
        "console.js",
    ] {
        let r = send(&app, get(gone, Some("gzip"), None)).await;
        assert_ne!(r.status, StatusCode::OK, "{gone}");
    }
    // `/dash/assets/` is Dash's: the Rust server serves nothing of its own there.
    let r = send(
        &app,
        Request::get("/dash/assets/xterm.js")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_ne!(r.status, StatusCode::OK);
}

#[tokio::test]
async fn the_standalone_terminal_page_loads_its_own_assets() {
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let app = meerschaum_web::app(state.clone(), terminal_config(&state)).unwrap();
    let id = uuid::Uuid::new_v4();
    let r = send(
        &app,
        Request::get(format!("/webterm/{id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    // Unauthenticated: the page that says to log in (the Dash console's login).
    assert!(r.text().contains("href=\"/dash/login\""), "{}", r.text());

    // A logged-in session gets the terminal page.
    let (state, _) = state_with(config(root.path()));
    let web = terminal_config(&state);
    let sessions = web.sessions.clone();
    let app = meerschaum_web::app(state.clone(), web).unwrap();
    sessions
        .set(&id.to_string(), json!({"username": "admin"}), 100)
        .await
        .unwrap();
    let r = send(
        &app,
        Request::get(format!("/webterm/{id}"))
            .header("cookie", format!("mrsm-session-id={id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let html = r.text();
    assert_eq!(r.status, StatusCode::OK, "{html}");
    assert!(
        html.contains(&format!("data-ws=\"/websocket/{id}\"")),
        "{html}"
    );
    for asset in [
        "xterm.css",
        "webterm.css",
        "xterm.js",
        "addon-fit.js",
        "webterm.js",
    ] {
        assert!(
            html.contains(&format!("/webterm/assets/{asset}")),
            "{asset}"
        );
    }
    assert!(!html.contains("/dash/assets"), "{html}");
}

#[tokio::test]
async fn no_terminal_no_assets_and_no_dash_no_routes() {
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    // No terminal configured: no assets, no terminal routes.
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();
    let r = send(
        &app,
        Request::get("/webterm/assets/xterm.js")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_ne!(r.status, StatusCode::OK);

    let mut cfg = config(root.path());
    cfg.no_dash = true;
    let (state, _) = state_with(cfg);
    let app = meerschaum_web::app(state.clone(), terminal_config(&state)).unwrap();
    for path in [
        "/webterm/assets/xterm.js",
        "/logs/x/stream",
        "/_mrsm/native-pages",
    ] {
        let r = send(&app, Request::get(path).body(Body::empty()).unwrap()).await;
        assert!(r.unmatched, "{path}");
    }
}

/// Jobs that exist by name and replay a script of log events.
#[derive(Default)]
struct ScriptedJobs {
    scripts: Mutex<Vec<(String, Vec<LogEvent>)>>,
    /// The senders of silent jobs (`execute-held*`), kept so their streams stay open.
    held: Mutex<Vec<tokio::sync::mpsc::Sender<LogEvent>>>,
}

impl ScriptedJobs {
    fn add(&self, name: &str, events: Vec<LogEvent>) {
        self.scripts.lock().unwrap().push((name.into(), events));
    }
}

#[async_trait]
impl JobsBackend for ScriptedJobs {
    async fn list(&self) -> indexmap::IndexMap<String, Json> {
        Default::default()
    }
    // Every scripted job was started by `bob` (`POST /execute` records the creator).
    async fn get(&self, name: &str) -> Option<Json> {
        self.exists(name)
            .await
            .then(|| json!({"daemon": {"properties": {"mrsm_owner": "bob"}}}))
    }
    async fn exists(&self, name: &str) -> bool {
        self.scripts.lock().unwrap().iter().any(|(n, _)| n == name)
    }
    async fn create(&self, _: &str, _: Vec<String>, _: Option<Map>) -> CreateJob {
        CreateJob::Started(SuccessTuple::success())
    }
    async fn delete(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn start(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn stop(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn pause(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn logs(&self, _name: &str) -> Option<String> {
        None
    }
    async fn stop_time(&self, _name: &str) -> Option<chrono::DateTime<chrono::Utc>> {
        None
    }
    async fn is_blocking_on_stdin(&self, _name: &str) -> bool {
        false
    }
    async fn prompt_kwargs(&self, _name: &str) -> Map {
        Map::new()
    }
    async fn monitor_logs(&self, name: &str) -> Option<LogMonitor> {
        let events = self
            .scripts
            .lock()
            .unwrap()
            .iter()
            .find(|(n, _)| n == name)?
            .1
            .clone();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let (input, _input_rx) = tokio::sync::mpsc::channel(1);
        for event in events {
            tx.send(event).await.unwrap();
        }
        // The sender ends with the script (a job that stopped without a `Stopped` event), unless
        // the job is a silent one that keeps running.
        if name.starts_with("execute-held") {
            self.held.lock().unwrap().push(tx);
        }
        Some(LogMonitor { events: rx, input })
    }
}

fn stream_app(cfg: meerschaum_server::ServerConfig) -> (Router, WebConfig, Arc<ScriptedJobs>) {
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    instance.seed_user("bob", "pw", None);
    instance.seed_user("eve", "pw", None);
    let jobs = Arc::new(ScriptedJobs::default());
    let state = AppState::builder(cfg, instance)
        .secret(common::SECRET.to_vec())
        .jobs(jobs.clone())
        .build()
        .unwrap();
    let web = WebConfig::for_state(&state);
    let app = meerschaum_web::app(state, web.clone()).unwrap();
    (app, web, jobs)
}

fn script() -> Vec<LogEvent> {
    vec![
        LogEvent::Line("first line\n".into()),
        LogEvent::Line("second line".into()),
        LogEvent::StdinRequested,
        LogEvent::Stopped(SuccessTuple::ok("all done")),
        LogEvent::Line("never sent: the stream ends after `stopped`".into()),
    ]
}

#[tokio::test]
async fn the_job_log_stream_serves_sse_to_a_bearer() {
    let root = tempfile::tempdir().unwrap();
    let (app, _web, jobs) = stream_app(config(root.path()));
    jobs.add("execute-abc12345", script());
    let admin = common::login(&app, "admin", "pass", None).await;

    let r = send(
        &app,
        common::req("GET", "/logs/execute-abc12345/stream", Some(&admin), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.headers["content-type"], "text/event-stream");
    assert_eq!(r.headers["cache-control"], "no-store");
    let text = r.text();
    assert!(text.contains("data: first line"), "{text}");
    assert!(text.contains("data: second line"), "{text}");
    assert!(text.contains("event: stdin"), "{text}");
    assert!(text.contains("event: stopped\ndata: all done"), "{text}");
    assert!(!text.contains("never sent"), "{text}");

    // Unknown jobs are 404; no credentials are 401; a bearer without `jobs:read` is 403.
    let r = send(
        &app,
        common::req("GET", "/logs/nope/stream", Some(&admin), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = send(
        &app,
        common::req("GET", "/logs/execute-abc12345/stream", None, None),
    )
    .await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    let bob = common::login(&app, "bob", "pw", Some("pipes:read")).await;
    let r = send(
        &app,
        common::req("GET", "/logs/execute-abc12345/stream", Some(&bob), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());

    // A credential may hold jobs:read while withholding logs:read, even for an admin.
    let restricted = common::login(&app, "admin", "pass", Some("jobs:read")).await;
    let r = send(
        &app,
        common::req(
            "GET",
            "/logs/execute-abc12345/stream",
            Some(&restricted),
            None,
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(r.text().contains("logs:read"), "{}", r.text());
}

#[tokio::test]
async fn the_job_log_stream_accepts_the_console_session_cookie() {
    let root = tempfile::tempdir().unwrap();
    let (app, web, jobs) = stream_app(config(root.path()));
    jobs.add("execute-cookie", script());
    let id = uuid::Uuid::new_v4().to_string();
    web.sessions
        .set(&id, json!({"username": "admin"}), 100)
        .await
        .unwrap();
    let cookie = format!("mrsm-session-id={id}");
    let stream = |cookie: Option<&str>| {
        let mut request = Request::get("/logs/execute-cookie/stream");
        if let Some(cookie) = cookie {
            request = request.header("cookie", cookie);
        }
        request.body(Body::empty()).unwrap()
    };
    let r = send(&app, stream(Some(&cookie))).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert!(r.text().contains("event: stopped"));

    // No cookie, a cookie that is not a session, and a session of nobody: 401, never a redirect
    // (an `EventSource` cannot follow one to a login page).
    assert_eq!(
        send(&app, stream(None)).await.status,
        StatusCode::UNAUTHORIZED
    );
    let unknown = format!("mrsm-session-id={}", uuid::Uuid::new_v4());
    assert_eq!(
        send(&app, stream(Some(&unknown))).await.status,
        StatusCode::UNAUTHORIZED
    );
    let ghost = uuid::Uuid::new_v4().to_string();
    web.sessions
        .set(&ghost, json!({"username": "ghost"}), 100)
        .await
        .unwrap();
    assert_eq!(
        send(&app, stream(Some(&format!("mrsm-session-id={ghost}"))))
            .await
            .status,
        StatusCode::UNAUTHORIZED
    );
    // A session that was logged out.
    web.sessions.delete(&id).await.unwrap();
    assert_eq!(
        send(&app, stream(Some(&cookie))).await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn the_job_log_stream_follows_the_consoles_execute_rule() {
    // PARITY: the rule of the console's Exec box (v4 `api/dash/sessions.py`), which `POST /execute`
    // applies too (one function, `is_user_allowed_to_execute_console`):
    // `api:permissions:actions:non_admin` (default true, false under `--secure`). v4's other key,
    // `system:api:permissions:actions:non_admin` (`/actions`, the job routes), is not consulted.
    let root = tempfile::tempdir().unwrap();
    let stream = |auth: &str| common::req("GET", "/logs/execute-rule/stream", Some(auth), None);
    let session_stream = |id: &str| {
        Request::get("/logs/execute-rule/stream")
            .header("cookie", format!("mrsm-session-id={id}"))
            .body(Body::empty())
            .unwrap()
    };
    for (console_key, system_key, user_allowed) in [
        (true, false, true),
        (true, true, true),
        (false, true, false),
        (false, false, false),
    ] {
        let mut cfg = config(root.path());
        cfg.actions_non_admin = console_key;
        cfg.system_actions_non_admin = system_key;
        let (app, web, jobs) = stream_app(cfg);
        jobs.add("execute-rule", script());
        let label = format!("console key {console_key}, system key {system_key}");

        // An administrator always may.
        let admin = common::login(&app, "admin", "pass", None).await;
        assert_eq!(
            send(&app, stream(&admin)).await.status,
            StatusCode::OK,
            "{label}"
        );
        // A user (the job's creator) may exactly when the console key allows it.
        let bob = common::login(&app, "bob", "pw", None).await;
        let r = send(&app, stream(&bob)).await;
        let expected = if user_allowed {
            StatusCode::OK
        } else {
            StatusCode::FORBIDDEN
        };
        assert_eq!(r.status, expected, "{label}: {}", r.text());
        if !user_allowed {
            assert!(r.text().contains("administrator"), "{label}: {}", r.text());
        }
        // The same for a console session.
        let id = uuid::Uuid::new_v4().to_string();
        web.sessions
            .set(&id, json!({"username": "bob"}), 100)
            .await
            .unwrap();
        let r = send(&app, session_stream(&id)).await;
        assert_eq!(r.status, expected, "{label} (session): {}", r.text());
    }
}

#[tokio::test]
async fn the_job_log_stream_is_the_creators_and_the_administrators() {
    let root = tempfile::tempdir().unwrap();
    let (app, _web, jobs) = stream_app(config(root.path()));
    jobs.add("execute-rule", script());
    jobs.add("plain-job", script());
    let (admin, bob, eve) = (
        common::login(&app, "admin", "pass", None).await,
        common::login(&app, "bob", "pw", None).await,
        common::login(&app, "eve", "pw", None).await,
    );
    let get = |auth: &str, name: &str| {
        common::req("GET", &format!("/logs/{name}/stream"), Some(auth), None)
    };
    assert_eq!(
        send(&app, get(&bob, "execute-rule")).await.status,
        StatusCode::OK
    );
    assert_eq!(
        send(&app, get(&admin, "execute-rule")).await.status,
        StatusCode::OK
    );
    // Another user cannot even tell that the job exists.
    let r = send(&app, get(&eve, "execute-rule")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    assert!(!r.text().contains("first line"));
    // Jobs outside the console's family keep v4's visibility.
    assert_eq!(
        send(&app, get(&eve, "plain-job")).await.status,
        StatusCode::OK
    );
}

/// Open a stream and keep the response (and so the stream) alive.
async fn open_stream(app: &Router, auth: &str, name: &str) -> axum::response::Response {
    use tower::ServiceExt;
    app.clone()
        .oneshot(common::req(
            "GET",
            &format!("/logs/{name}/stream"),
            Some(auth),
            None,
        ))
        .await
        .expect("infallible")
}

#[tokio::test]
async fn log_streams_are_bounded_in_total_and_per_user() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = config(root.path());
    cfg.system_actions_non_admin = true;
    cfg.log_streams_max = 3;
    cfg.log_streams_per_user = 2;
    let (app, _web, jobs) = stream_app(cfg);
    jobs.add("execute-held", vec![]);
    let admin = common::login(&app, "admin", "pass", None).await;
    let bob = common::login(&app, "bob", "pw", None).await;

    let first = open_stream(&app, &admin, "execute-held").await;
    assert_eq!(first.status(), StatusCode::OK);
    let second = open_stream(&app, &admin, "execute-held").await;
    assert_eq!(second.status(), StatusCode::OK);
    // The user's limit.
    let third = open_stream(&app, &admin, "execute-held").await;
    assert_eq!(third.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(third.headers().contains_key("retry-after"));
    // Another user still has room, up to the server's limit.
    let bobs = open_stream(&app, &bob, "execute-held").await;
    assert_eq!(bobs.status(), StatusCode::OK);
    let over = open_stream(&app, &bob, "execute-held").await;
    assert_eq!(over.status(), StatusCode::TOO_MANY_REQUESTS);
    // A stream that ends frees its slot.
    drop(first);
    assert_eq!(
        open_stream(&app, &admin, "execute-held").await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn log_streams_are_rate_limited_like_the_jobs_group() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = config(root.path());
    cfg.rate_limits.insert(
        "jobs".into(),
        meerschaum_server::config::RateLimit {
            requests: 1,
            per: std::time::Duration::from_secs(3600),
        },
    );
    let (app, _web, jobs) = stream_app(cfg);
    jobs.add("execute-limited", script());
    let admin = common::login(&app, "admin", "pass", None).await;
    let stream = || common::req("GET", "/logs/execute-limited/stream", Some(&admin), None);
    assert_eq!(send(&app, stream()).await.status, StatusCode::OK);
    let r = send(&app, stream()).await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS, "{}", r.text());
    assert!(r.headers.contains_key("retry-after"));
}

#[tokio::test(start_paused = true)]
async fn a_silent_stream_ends_when_its_session_does() {
    use futures_util::StreamExt;
    use tower::ServiceExt;
    let root = tempfile::tempdir().unwrap();
    let (app, web, jobs) = stream_app(config(root.path()));
    jobs.add("execute-held-session", vec![]);
    let id = uuid::Uuid::new_v4().to_string();
    web.sessions
        .set(&id, json!({"username": "admin"}), 3600)
        .await
        .unwrap();
    let response = app
        .clone()
        .oneshot(
            Request::get("/logs/execute-held-session/stream")
                .header("cookie", format!("mrsm-session-id={id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    // Alive while the session is: the keep-alive comments arrive, the stream does not end.
    let frame = tokio::time::timeout(std::time::Duration::from_secs(30), body.next()).await;
    assert!(matches!(frame, Ok(Some(Ok(_)))), "{frame:?}");
    // Logout: the stream ends although the job never logs again.
    web.sessions.delete(&id).await.unwrap();
    let ended = tokio::time::timeout(std::time::Duration::from_secs(120), async {
        while let Some(frame) = body.next().await {
            frame.unwrap();
        }
    })
    .await;
    assert!(ended.is_ok(), "the stream outlived its session");
}

#[tokio::test]
async fn the_job_log_stream_follows_the_jobs_route_group() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = config(root.path());
    cfg.routes_allowlist = vec!["pipes".into()];
    let (app, _web, jobs) = stream_app(cfg);
    jobs.add("execute-x", script());
    let r = send(
        &app,
        Request::get("/logs/execute-x/stream")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

struct Echo;

#[async_trait]
impl PageHandler for Echo {
    async fn render(&self, query: &str, session: &Json) -> (String, StatusCode) {
        (
            format!(
                "<section data-user=\"{}\" data-query=\"{query}\"></section>",
                session["username"].as_str().unwrap_or("")
            ),
            StatusCode::OK,
        )
    }
}

fn page(path: &str, login_required: bool, content: PageContent) -> PluginPage {
    PluginPage {
        plugin: "native_demo".into(),
        path: path.into(),
        page_group: "Demo".into(),
        login_required,
        skip_navbar: false,
        dark_theme: Some(true),
        content,
    }
}

fn token_request(uri: &str, user: Option<&str>) -> Request<Body> {
    let mut request = Request::get(uri).header(
        "x-mrsm-host-token",
        meerschaum_server::mcp::federation::internal_token(common::SECRET, None),
    );
    if let Some(user) = user {
        request = request.header("x-mrsm-user", user);
    }
    request.body(Body::empty()).unwrap()
}

#[tokio::test]
async fn native_pages_are_fragments_for_the_dash_host() {
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let mut web = WebConfig::for_state(&state);
    web.pages = vec![
        page("native/report", true, PageContent::Handler(Arc::new(Echo))),
        page(
            "/open/",
            false,
            PageContent::Html("<p data-mrsm-figure='{}'></p>".into()),
        ),
    ];
    let app = meerschaum_web::app(state, web).unwrap();

    let list = send(&app, token_request("/_mrsm/native-pages", None)).await;
    assert_eq!(list.status, StatusCode::OK, "{}", list.text());
    let pages = list.json();
    assert_eq!(pages[0]["path"], "native/report");
    assert_eq!(pages[0]["plugin"], "native_demo");
    assert_eq!(pages[0]["page_group"], "Demo");
    assert_eq!(pages[0]["login_required"], true);
    assert_eq!(pages[0]["dark_theme"], true);
    assert_eq!(pages[1]["path"], "open", "slashes are trimmed");

    let r = send(
        &app,
        token_request("/_mrsm/native-pages/native/report?a=1&b=2", Some("alice")),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(
        r.text(),
        "<section data-user=\"alice\" data-query=\"a=1&b=2\"></section>"
    );
    assert_eq!(r.headers["cache-control"], "no-store");
    assert_eq!(r.headers["x-content-type-options"], "nosniff");

    // Login required without a user, an open page without one, an unknown page.
    let r = send(
        &app,
        token_request("/_mrsm/native-pages/native/report", None),
    )
    .await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    let r = send(&app, token_request("/_mrsm/native-pages/open", None)).await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(r.text().contains("data-mrsm-figure"));
    let r = send(
        &app,
        token_request("/_mrsm/native-pages/nope", Some("alice")),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn native_pages_need_the_internal_token() {
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let mut web = WebConfig::for_state(&state);
    web.pages = vec![page("open", false, PageContent::Html("<p></p>".into()))];
    let app = meerschaum_web::app(state, web).unwrap();
    let plain = |uri: &str| Request::get(uri).body(Body::empty()).unwrap();
    for uri in ["/_mrsm/native-pages", "/_mrsm/native-pages/open"] {
        assert_eq!(
            send(&app, plain(uri)).await.status,
            StatusCode::FORBIDDEN,
            "{uri}"
        );
        // A user's own bearer is not the host's token.
        let admin = common::login(&app, "admin", "pass", None).await;
        let r = send(&app, common::req("GET", uri, Some(&admin), None)).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{uri}");
        // A token signed with another secret.
        let forged = meerschaum_server::mcp::federation::internal_token(b"another-secret", None);
        let r = send(
            &app,
            Request::get(uri)
                .header("x-mrsm-host-token", forged)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{uri}");
    }
}

#[tokio::test]
async fn bad_or_duplicate_native_page_paths_are_refused() {
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let html = || PageContent::Html("<p></p>".into());
    for bad in [
        "", "/", "a/../b", "a//b", "a b", "a*", "{x}", "a?b", "a%2fb", "a\\b",
    ] {
        let mut web = WebConfig::for_state(&state);
        web.pages = vec![page(bad, false, html())];
        assert!(
            meerschaum_web::router(state.clone(), web).is_err(),
            "{bad:?}"
        );
    }
    let mut web = WebConfig::for_state(&state);
    web.pages = vec![page("same", false, html()), page("/same/", false, html())];
    let error = meerschaum_web::router(state.clone(), web).unwrap_err();
    assert!(error.contains("Duplicate page"), "{error}");
    // No pages, no routes.
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();
    let r = send(&app, token_request("/_mrsm/native-pages", None)).await;
    assert!(r.unmatched || r.status == StatusCode::NOT_FOUND);
}
