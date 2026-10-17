#[path = "../../meerschaum-server/tests/common/mod.rs"]
mod common;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use common::config as config_for;
use common::{config, send, state_with};
use meerschaum_web::{
    PageContent, PluginPage, WebConfig,
    sessions::{InstanceSessions, SessionStore},
};
use serde_json::json;
use std::sync::Arc;

async fn login(app: &axum::Router) -> (String, String) {
    login_as(app, "admin").await
}

async fn login_as(app: &axum::Router, username: &str) -> (String, String) {
    let response = send(
        app,
        Request::post("/dash/login")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(format!("username={username}&password=pass")))
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

/// `application/x-www-form-urlencoded` pairs, percent-encoding everything but unreserved bytes.
fn encoded(pairs: &[(&str, &str)]) -> String {
    let enc = |s: &str| {
        s.bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    (b as char).to_string()
                }
                _ => format!("%{b:02X}"),
            })
            .collect::<String>()
    };
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", enc(k), enc(v)))
        .collect::<Vec<_>>()
        .join("&")
}

async fn post_action(app: &axum::Router, cookie: &str, pairs: &[(&str, &str)]) -> common::Resp {
    send(
        app,
        Request::post("/dash/action")
            .header("cookie", cookie)
            .body(Body::from(encoded(pairs)))
            .unwrap(),
    )
    .await
}

async fn tokens_page(app: &axum::Router, cookie: &str) -> String {
    let page = send(
        app,
        Request::get("/dash/tokens")
            .header("cookie", cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(page.status, StatusCode::OK, "{}", page.text());
    page.text()
}

/// The token ID in a registration response (the first UUID after `"id":`; the page's CSRF token,
/// also a UUID, comes earlier).
fn first_uuid(text: &str) -> String {
    let at = text.find("id&quot;:&quot;").expect("an id field");
    text.as_bytes()[at..]
        .windows(36)
        .find(|w| {
            w.iter().enumerate().all(|(i, b)| match i {
                8 | 13 | 18 | 23 => *b == b'-',
                _ => b.is_ascii_hexdigit(),
            })
        })
        .map(|w| String::from_utf8_lossy(w).into_owned())
        .expect("a uuid")
}

#[tokio::test]
async fn the_token_forms_grant_all_scopes_with_a_star_kept_literally() {
    let root = tempfile::tempdir().unwrap();
    let (state, _instance) = state_with(config(root.path()));
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();
    let (cookie, csrf) = login(&app).await;

    // The registration form: the switch is on by default, the checklist and a custom field
    // follow it, and the wording is 4.1.6's.
    let html = tokens_page(&app, &cookie).await;
    assert!(
        html.contains("Grant all scopes (`*`, including custom and future scopes)"),
        "{html}"
    );
    assert!(
        html.contains(r#"name="all_scopes" value="1" checked"#),
        "the switch is on for a new token: {html}"
    );
    assert!(
        html.contains(r#"name="scopes" value="pipes:read" checked"#),
        "{html}"
    );
    assert!(html.contains(r#"name="custom_scopes""#), "{html}");
    assert!(html.contains("Deselect all"), "{html}");

    fn register_pairs<'a>(csrf: &'a str, extra: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
        let mut pairs = vec![
            ("csrf", csrf),
            ("op", "token-register"),
            ("target", ""),
            ("payload", "{}"),
            ("token_form", "1"),
            ("expiration", ""),
        ];
        pairs.extend_from_slice(extra);
        pairs
    }

    // Switch on: `["*"]` is stored, whatever the checklist says.
    let response = post_action(
        &app,
        &cookie,
        &register_pairs(
            &csrf,
            &[
                ("label", "everything"),
                ("all_scopes", "1"),
                ("scopes", "pipes:read"),
            ],
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let star_id = first_uuid(&response.text());

    // Switch off: the checked built-in scopes plus the custom ones (space or comma separated).
    let response = post_action(
        &app,
        &cookie,
        &register_pairs(
            &csrf,
            &[
                ("label", "plug"),
                ("scopes", "pipes:read"),
                ("custom_scopes", "myplugin:read, myplugin:write"),
            ],
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let text = response.text();
    let plug_id = first_uuid(&text);

    // Nothing selected: refused, as 4.1.6 does ("Select at least one scope.").
    let response = post_action(&app, &cookie, &register_pairs(&csrf, &[("label", "none")])).await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert!(response.text().contains("Select at least one scope."));

    // The edit forms: the token holding `*` shows the switch on, the other off with its own scopes
    // and the custom ones in the field.
    let html = tokens_page(&app, &cookie).await;
    assert!(
        html.contains(&format!(
            r#"id="all-scopes-{star_id}" name="all_scopes" value="1" checked"#
        )),
        "{html}"
    );
    assert!(
        html.contains(&format!(
            r#"id="all-scopes-{plug_id}" name="all_scopes" value="1">"#
        )),
        "{html}"
    );
    assert!(
        html.contains(r#"value="myplugin:read myplugin:write""#),
        "{html}"
    );
    let plug_form = {
        let at = html
            .find(&format!(r#"name="target" value="{plug_id}""#))
            .unwrap();
        let start = html[..at].rfind("<form").unwrap();
        &html[start..start + html[start..].find("</form>").unwrap()]
    };
    assert!(
        plug_form.contains(r#"name="scopes" value="pipes:read" checked"#),
        "{plug_form}"
    );
    assert!(
        plug_form.contains(r#"name="scopes" value="pipes:write">"#),
        "{plug_form}"
    );

    // Saving the `*` token with the switch still on keeps `["*"]`; turning it off with a custom
    // scope stores exactly that.
    let base = |id: &str| {
        let marker = format!(r#"name="target" value="{id}""#);
        let at = html.find(&marker).expect("edit form");
        let form = &html[html[..at].rfind("<form").unwrap()..];
        form.split(r#"name="payload" value=""#)
            .nth(1)
            .unwrap()
            .split('"')
            .next()
            .unwrap()
            .replace("&quot;", "\"")
            .replace("&amp;", "&")
    };
    let edit = |id: &str, extra: &[(&str, &str)]| {
        let payload = base(id);
        let mut pairs = vec![
            ("csrf".to_string(), csrf.clone()),
            ("op".to_string(), "token-edit".to_string()),
            ("target".to_string(), id.to_string()),
            ("payload".to_string(), payload),
            ("token_form".to_string(), "1".to_string()),
            ("label".to_string(), "edited".to_string()),
            ("expiration".to_string(), String::new()),
        ];
        pairs.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        pairs
    };
    let post_edit = |pairs: Vec<(String, String)>| {
        let borrowed: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        encoded(&borrowed)
    };
    let response = send(
        &app,
        Request::post("/dash/action")
            .header("cookie", &cookie)
            .body(Body::from(post_edit(edit(
                &star_id,
                &[("all_scopes", "1")],
            ))))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let html = tokens_page(&app, &cookie).await;
    assert!(
        html.contains(&format!(
            r#"id="all-scopes-{star_id}" name="all_scopes" value="1" checked"#
        )),
        "`*` survived an edit with the switch on: {html}"
    );
    assert!(html.contains(r#"value="edited""#), "{html}");
    let response = send(
        &app,
        Request::post("/dash/action")
            .header("cookie", &cookie)
            .body(Body::from(post_edit(edit(
                &star_id,
                &[("custom_scopes", "myplugin:admin")],
            ))))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let html = tokens_page(&app, &cookie).await;
    assert!(
        html.contains(&format!(
            r#"id="all-scopes-{star_id}" name="all_scopes" value="1">"#
        )),
        "the switch is off after saving without it: {html}"
    );
    assert!(html.contains(r#"value="myplugin:admin""#), "{html}");
}

#[tokio::test]
async fn the_token_form_starts_from_the_owners_scopes_and_refuses_more() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = state_with(config(root.path()));
    instance.seed_user("ron", "pass", None);
    instance.seed_user_scopes("ron", &["pipes:read", "jobs:read", "myplugin:read"]);
    instance.seed_user("star", "pass", None);
    instance.seed_user_scopes("star", &["*"]);
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();

    // A restricted owner: the switch is off, their scopes are checked, their custom scope is in
    // the field, and the form says what the limit is.
    let (cookie, csrf) = login_as(&app, "ron").await;
    let html = tokens_page(&app, &cookie).await;
    let register = &html[html.find(r#"id="all-scopes-register""#).unwrap()..];
    assert!(
        register.starts_with(r#"id="all-scopes-register" name="all_scopes" value="1">"#),
        "{register}"
    );
    assert!(register.contains(r#"name="scopes" value="pipes:read" checked"#));
    assert!(register.contains(r#"name="scopes" value="pipes:write">"#));
    assert!(register.contains(r#"value="myplugin:read""#));
    assert!(
        html.contains("A token carries at most your scopes"),
        "{html}"
    );

    let ask = |extra: &[(&str, &str)]| {
        let mut pairs = vec![
            ("csrf", csrf.as_str()),
            ("op", "token-register"),
            ("target", ""),
            ("payload", "{}"),
            ("token_form", "1"),
            ("label", "t"),
            ("expiration", ""),
        ];
        pairs.extend_from_slice(extra);
        encoded(&pairs)
    };
    let post = |body: String| {
        send(
            &app,
            Request::post("/dash/action")
                .header("cookie", &cookie)
                .body(Body::from(body))
                .unwrap(),
        )
    };
    // Their own scopes work; `*` and a scope they lack are refused, naming what is disallowed.
    let ok = post(ask(&[
        ("scopes", "pipes:read"),
        ("custom_scopes", "myplugin:read"),
    ]))
    .await;
    assert_eq!(ok.status, StatusCode::OK, "{}", ok.text());
    let star = post(ask(&[("all_scopes", "1")])).await;
    assert_eq!(star.status, StatusCode::FORBIDDEN, "{}", star.text());
    assert!(
        star.text().contains("&#39;*&#39;") || star.text().contains("'*'"),
        "{}",
        star.text()
    );
    let more = post(ask(&[("scopes", "pipes:read"), ("scopes", "pipes:write")])).await;
    assert_eq!(more.status, StatusCode::FORBIDDEN, "{}", more.text());
    let text = more.text();
    assert!(
        text.contains("pipes:write") && text.contains("Cannot grant scopes"),
        "{text}"
    );

    // A holder of `*` starts with the switch on.
    let (cookie, _) = login_as(&app, "star").await;
    let html = tokens_page(&app, &cookie).await;
    assert!(
        html.contains(r#"id="all-scopes-register" name="all_scopes" value="1" checked"#),
        "{html}"
    );
    assert!(!html.contains("A token carries at most your scopes"));
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

/// A temporary SQLite instance (`sql:eng`) with a three-pipe chain that has run once.
async fn engine_fixture() -> (
    tempfile::TempDir,
    Arc<meerschaum_sql::SqlConnector>,
    Arc<meerschaum_engine::StaticRegistry>,
    meerschaum_engine::EngineConfig,
) {
    use meerschaum_connector::InstanceConnector;
    use meerschaum_engine::{
        Depth, Engine, EngineConfig, PipeRef, RunOptions, Selection, StaticRegistry,
    };
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite:///{}", dir.path().join("eng.db").display());
    let conn = Arc::new(
        meerschaum_sql::SqlConnector::from_uri("eng", &uri)
            .await
            .unwrap(),
    );
    let instance: Arc<dyn InstanceConnector> = conn.clone();
    let source: Arc<dyn meerschaum_connector::Connector> = conn.clone();
    let registry = Arc::new(
        StaticRegistry::new()
            .with_instance("sql:eng", instance)
            .with_source("sql:eng", source)
            .with_table_reference(|pipe: &meerschaum_core::Pipe| {
                let target = pipe.target(None).map_err(|e| e.to_string())?;
                Ok(meerschaum_sql::ident::sql_item_name(
                    &target,
                    meerschaum_sql::Flavor::Sqlite,
                    None,
                ))
            }),
    );
    let config = EngineConfig {
        home_instance: "sql:eng".into(),
        lease_dir: dir.path().join("leases"),
        state_flush: std::time::Duration::from_millis(20),
        ..EngineConfig::default()
    };
    assert!(
        conn.exec("CREATE TABLE src (ts TEXT, sensor_id INTEGER, value REAL)")
            .await
            .is_some()
    );
    assert!(
        conn.exec("INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 2.0), ('2026-01-01 01:00:00', 1, 4.0)")
            .await
            .is_some()
    );
    let cols = json!({"datetime": "ts", "id": "sensor_id"});
    let dtypes = json!({"ts": "datetime", "sensor_id": "int", "value": "float"});
    let defs = [
        ("a_raw", "SELECT ts, sensor_id, value FROM src"),
        (
            "b_clean",
            "SELECT ts, sensor_id, value FROM {{ Pipe('sql:eng', 'a_raw', instance='sql:eng') }}",
        ),
        (
            "c_copy",
            "SELECT ts, sensor_id, value FROM {{ Pipe('sql:eng', 'b_clean', instance='sql:eng') }}",
        ),
    ];
    for (metric, sql) in defs {
        let params = json!({"sql": sql, "columns": cols, "dtypes": dtypes});
        let params = meerschaum_core::Value::from_json_str(&params.to_string())
            .unwrap()
            .to_map();
        let pipe = meerschaum_core::Pipe::builder("sql:eng", metric)
            .instance("sql:eng")
            .parameters(params)
            .build()
            .unwrap();
        assert!(conn.register_pipe(&pipe).await.success);
    }
    let engine = Engine::new(registry.clone(), config.clone());
    let result = engine
        .run(RunOptions {
            selection: Some(
                Selection::refs([PipeRef::new("sql:eng", "sql:eng", "c_copy", None)])
                    .with_parents(Some(Depth::All)),
            ),
            ..Default::default()
        })
        .await;
    assert!(result.success, "{}", result.message);
    (dir, conn, registry, config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipelines_page_and_partials_render_engine_state() {
    let (_dir, _conn, registry, config) = engine_fixture().await;
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config_for(root.path()));
    let reader = Arc::new(meerschaum_engine::EngineReader::new(registry, config));
    let app = meerschaum_web::app(
        state.clone(),
        WebConfig::for_state(&state).with_engine(reader),
    )
    .unwrap();
    let (cookie, _) = login(&app).await;
    let get = |uri: &'static str| {
        let cookie = cookie.clone();
        let app = app.clone();
        async move {
            send(
                &app,
                Request::get(uri)
                    .header("cookie", cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        }
    };
    let page = get("/dash/pipelines").await;
    assert_eq!(page.status, StatusCode::OK, "{}", page.text());
    let html = page.text();
    // DAG: three nodes, two edges, status and lag in the cytoscape spec; Gantt; runs table.
    assert!(html.contains("data-mrsm-graph="), "{html}");
    assert!(
        html.contains("&quot;status&quot;:&quot;succeeded&quot;"),
        "{html}"
    );
    assert!(html.contains("caught up"), "{html}");
    assert!(html.contains("data-mrsm-figure="));
    assert!(html.contains("<h2>Runs</h2>"));
    assert!(html.contains("data-mrsm-events=\"/dash/pipelines/events?run_id="));
    assert!(html.contains("hx-get=\"/dash/pipelines/graph\""));
    assert!(html.contains("sql:eng|sql:eng|c_copy|None"));

    let graph = get("/dash/pipelines/graph").await;
    assert_eq!(graph.status, StatusCode::OK);
    assert!(graph.text().contains("Ledger lag"));
    let gantt = get("/dash/pipelines/gantt").await;
    assert!(gantt.text().contains("Planned start"), "{}", gantt.text());
    let runs = get("/dash/pipelines/runs?status=succeeded").await;
    assert!(
        runs.text().contains("<td>succeeded</td>"),
        "{}",
        runs.text()
    );
    let none = get("/dash/pipelines/runs?status=failed").await;
    assert!(none.text().contains("No runs match."));
    let unauth = send(
        &app,
        Request::get("/dash/pipelines/runs")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(unauth.status, StatusCode::SEE_OTHER);

    // The live stream ends once the (finished) run's events are delivered.
    let run_id = runs
        .text()
        .split("run_id=")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap()
        .to_string();
    let uri: &'static str =
        Box::leak(format!("/dash/pipelines/events?run_id={run_id}").into_boxed_str());
    let stream = get(uri).await;
    assert_eq!(stream.status, StatusCode::OK);
    let text = stream.text();
    assert!(text.contains("run_started"), "{text}");
    assert!(text.contains("event: end"), "{text}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipe_card_shows_parents_children_with_lag() {
    let (_dir, _conn, registry, config) = engine_fixture().await;
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config_for(root.path()));
    let reader = Arc::new(meerschaum_engine::EngineReader::new(registry, config));
    // Same relations the card renders, from the reader the console uses.
    let view = reader.pipeline(&[]).await.unwrap();
    assert_eq!(view.nodes.len(), 3);
    assert_eq!(view.edges.len(), 2);
    let _ = state;
}

#[tokio::test]
async fn host_registry_populates_pages_and_navbar() {
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let mut cfg = WebConfig::for_state(&state);
    let registry = json!({"web_pages": [
        {"path": "stats", "page_group": "Reports", "page_key": "stats", "login_required": true,
         "skip_navbar": false, "dark_theme": null, "function": "stats_page", "mode": "html",
         "route": "/_pages/stats"},
        {"path": "forecast", "page_group": "Weather", "page_key": "forecast", "login_required": true,
         "mode": "dash", "function": "forecast_layout", "route": "/dash/weather/forecast"},
        // The host reports v4's registered paths (`/dash/<path>`).
        {"path": "/dash/outlook", "page_group": "Weather", "page_key": "outlook", "login_required": true,
         "mode": "dash", "function": "outlook_layout", "route": "/dash/weather/outlook"},
        {"path": "/dash/summary", "page_group": "Reports", "page_key": "summary", "login_required": true,
         "mode": "html", "function": "summary_page", "route": "/_pages/summary"},
    ]});
    assert_eq!(cfg.apply_host_registry(&registry).unwrap(), 4);
    assert_eq!(cfg.legacy_pages["outlook"], "weather");
    assert!(cfg.pages.iter().any(|p| p.path == "summary"));
    assert!(matches!(&cfg.pages[0].content, PageContent::Route(r) if r == "/_pages/stats"));
    assert_eq!(cfg.legacy_pages["forecast"], "weather");
    assert!(
        cfg.apply_host_registry(&json!({"web_pages": [{"path": "x", "mode": "dash"}]}))
            .is_err()
    );
    let app = meerschaum_web::app(state.clone(), cfg).unwrap();
    let (cookie, _) = login(&app).await;
    let page = send(
        &app,
        Request::get("/dash/pipes")
            .header("cookie", &cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let html = page.text();
    assert!(html.contains("href=\"/dash/stats\""), "{html}");
    assert!(html.contains("href=\"/dash/weather/forecast\""), "{html}");
    let redirect = send(
        &app,
        Request::get("/dash/forecast")
            .header("cookie", &cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(redirect.headers["location"], "/dash/weather/forecast");
}

/// `api:permissions:routes:allowlist` without `dash` hides the console's own pages exactly like
/// v4's `_console_pages_allowed`: the login page, assets and plugin pages are still served.
#[tokio::test]
async fn dash_route_group_gates_console_pages_only() {
    let root = tempfile::tempdir().unwrap();
    let mut server_config = config(root.path());
    server_config.routes_allowlist = vec!["pipes".into(), "version".into()];
    let (state, _) = state_with(server_config);
    let mut cfg = WebConfig::for_state(&state);
    cfg.dash_plugins.push("weather".into());
    cfg.legacy_pages.insert("forecast".into(), "weather".into());
    cfg.pages.push(PluginPage {
        path: "report".into(),
        page_group: "Reports".into(),
        login_required: false,
        skip_navbar: false,
        dark_theme: None,
        content: PageContent::Html("<h2>Report</h2>".into()),
    });
    let app = meerschaum_web::app(state.clone(), cfg).unwrap();
    for path in [
        "/dash",
        "/dash/",
        "/dash/dashboard",
        "/dash/pipes",
        "/dash/jobs",
        "/dash/plugins",
        "/dash/users",
        "/dash/tokens",
        "/dash/settings",
        "/dash/pipelines",
        "/dash/pipelines/events",
        "/dash/register",
        "/dash/job-logs/anything",
    ] {
        let response = send(&app, Request::get(path).body(Body::empty()).unwrap()).await;
        assert_eq!(response.status, StatusCode::NOT_FOUND, "{path} is hidden");
    }
    for path in ["/dash/action", "/dash/plugins/upload"] {
        let response = send(&app, Request::post(path).body(Body::empty()).unwrap()).await;
        assert_eq!(
            response.status,
            StatusCode::NOT_FOUND,
            "POST {path} is hidden"
        );
    }
    // The login page and assets stay; the navbar of a plugin page has no core links.
    let login_page = send(
        &app,
        Request::get("/dash/login").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(login_page.status, StatusCode::OK);
    assert!(!login_page.text().contains("href=\"/dash/pipes\""));
    let asset = send(
        &app,
        Request::get("/dash/assets/console.js")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(asset.status, StatusCode::OK);
    let report = send(
        &app,
        Request::get("/dash/report").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(report.status, StatusCode::OK);
    assert!(report.text().contains("Report"));
    assert!(!report.text().contains("href=\"/dash/jobs\""));
    let redirect = send(
        &app,
        Request::get("/dash/forecast").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(redirect.headers["location"], "/dash/weather/forecast");
    // A wildcard allowlist (the default) serves everything.
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();
    let (cookie, _) = login(&app).await;
    let response = send(
        &app,
        Request::get("/dash/pipes")
            .header("cookie", &cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK);
}

fn multipart_body(boundary: &str, fields: &[(&str, &str)], archive: Option<&[u8]>) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, value) in fields {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .as_bytes(),
        );
    }
    if let Some(archive) = archive {
        body.extend_from_slice(
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"archive\"; filename=\"x.tar.gz\"\r\nContent-Type: application/gzip\r\n\r\n")
                .as_bytes(),
        );
        body.extend_from_slice(archive);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

fn upload_request(cookie: &str, boundary: &str, body: Vec<u8>) -> Request<Body> {
    Request::post("/dash/plugins/upload")
        .header("cookie", cookie)
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(Body::from(body))
        .unwrap()
}

/// The console's archive upload registers the plugin through `POST /plugins/{name}` with the
/// session user's credentials and stores the archive; it enforces CSRF, the registration
/// permission, ownership and the same size limit as the API route.
#[tokio::test]
async fn plugin_archive_upload_uses_the_api_route() {
    let root = tempfile::tempdir().unwrap();
    let mut server_config = config(root.path());
    server_config.registration_plugins = true;
    let (state, _) = state_with(server_config.clone());
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();
    let (cookie, csrf) = login(&app).await;
    let page = send(
        &app,
        Request::get("/dash/plugins")
            .header("cookie", &cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert!(page.text().contains("action=\"/dash/plugins/upload\""));
    assert!(page.text().contains("enctype=\"multipart/form-data\""));

    let archive: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let fields = [
        ("csrf", csrf.as_str()),
        ("name", "w7demo"),
        ("version", "1.2.3"),
        ("description", "A demo"),
    ];
    let response = send(
        &app,
        upload_request(&cookie, "XB", multipart_body("XB", &fields, Some(&archive))),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert!(
        response.text().contains("Success") || response.text().contains("w7demo"),
        "{}",
        response.text()
    );
    let stored = std::fs::read(server_config.plugins_archives_path.join("w7demo.tar.gz")).unwrap();
    assert_eq!(stored, archive);
    let downloaded = send(
        &app,
        Request::get("/plugins/w7demo").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(downloaded.status, StatusCode::OK);
    assert_eq!(downloaded.body.as_ref(), archive.as_slice());

    // A wrong CSRF token, a bad name and a missing archive are refused.
    let bad_csrf = [("csrf", "wrong"), ("name", "w7demo")];
    let response = send(
        &app,
        upload_request(
            &cookie,
            "XB",
            multipart_body("XB", &bad_csrf, Some(&archive)),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::FORBIDDEN);
    let bad_name = [("csrf", csrf.as_str()), ("name", "../evil")];
    let response = send(
        &app,
        upload_request(
            &cookie,
            "XB",
            multipart_body("XB", &bad_name, Some(&archive)),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    let no_archive = [("csrf", csrf.as_str()), ("name", "w7other")];
    let response = send(
        &app,
        upload_request(&cookie, "XB", multipart_body("XB", &no_archive, None)),
    )
    .await;
    assert_eq!(response.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(
        !server_config
            .plugins_archives_path
            .join("w7other.tar.gz")
            .exists()
    );

    // Without a session the upload redirects to the login page.
    let response = send(
        &app,
        Request::post("/dash/plugins/upload")
            .header("content-type", "multipart/form-data; boundary=XB")
            .body(Body::from(multipart_body("XB", &fields, Some(&archive))))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status, StatusCode::SEE_OTHER);

    // The API route's `registration.plugins` permission applies to console uploads.
    let root = tempfile::tempdir().unwrap();
    let mut closed = config(root.path());
    closed.registration_plugins = false;
    let (state, _) = state_with(closed.clone());
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();
    let (cookie, csrf) = login(&app).await;
    let fields = [("csrf", csrf.as_str()), ("name", "w7demo")];
    let response = send(
        &app,
        upload_request(&cookie, "XB", multipart_body("XB", &fields, Some(&archive))),
    )
    .await;
    assert!(
        response
            .text()
            .contains("has not allowed plugin registration"),
        "{}",
        response.text()
    );
    assert!(!closed.plugins_archives_path.join("w7demo.tar.gz").exists());
}

/// Archives above the API route's limit are rejected by the console form and by the API alike,
/// and nothing is stored; one below the old 2 MiB axum default is accepted.
#[tokio::test]
async fn plugin_archive_upload_enforces_the_api_size_limit() {
    let root = tempfile::tempdir().unwrap();
    let mut server_config = config(root.path());
    server_config.registration_plugins = true;
    let (state, _) = state_with(server_config.clone());
    let app = meerschaum_web::app(state.clone(), WebConfig::for_state(&state)).unwrap();
    let (cookie, csrf) = login(&app).await;
    let limit = meerschaum_server::routes::plugins::ARCHIVE_MAX_BYTES;

    let big = vec![7u8; 3 * 1024 * 1024];
    let fields = [("csrf", csrf.as_str()), ("name", "w7big")];
    let response = send(
        &app,
        upload_request(&cookie, "XB", multipart_body("XB", &fields, Some(&big))),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "3 MiB is accepted");
    assert_eq!(
        std::fs::metadata(server_config.plugins_archives_path.join("w7big.tar.gz"))
            .unwrap()
            .len(),
        big.len() as u64
    );

    let too_big = vec![1u8; limit + 1];
    let fields = [("csrf", csrf.as_str()), ("name", "w7huge")];
    let response = send(
        &app,
        upload_request(&cookie, "XB", multipart_body("XB", &fields, Some(&too_big))),
    )
    .await;
    assert_eq!(
        response.status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "{}",
        response.text()
    );
    assert!(
        !server_config
            .plugins_archives_path
            .join("w7huge.tar.gz")
            .exists()
    );

    // The API route itself refuses the same body (it reports the missing `archive` field, like
    // a body that fails to parse does in v4's `File(...)` parameter).
    let token = common::login(&app, "admin", "pass", None).await;
    let response = send(
        &app,
        Request::post("/plugins/w7huge")
            .header("authorization", token)
            .header("content-type", "multipart/form-data; boundary=XB")
            .body(Body::from(multipart_body("XB", &[], Some(&too_big))))
            .unwrap(),
    )
    .await;
    assert!(response.status.is_client_error(), "{}", response.status);
    assert!(
        !server_config
            .plugins_archives_path
            .join("w7huge.tar.gz")
            .exists()
    );
}
