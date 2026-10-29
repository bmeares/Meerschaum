//! A single-file Rust plugin at parity with packaged plugins, through `mrsm`:
//!
//! - a `#[make_connector(instance)]` module serving as the instance of `register`, `sync`,
//!   `show pipes` and `delete pipes`;
//! - a top-level `fn subscribe` feeding `sync pipes -c plugin:<name> --listen`;
//! - a `#[web_page]` served to the web console as a native page fragment;
//! - `#[api_route]` paths checked at build time with the server's own rules.
//!
//! Every run uses a temporary root and plugins directory and a scrubbed environment; all data is
//! synthetic.

#[path = "../../meerschaum-plugin/tests/support/compile_single_file.rs"]
#[cfg(unix)]
mod compile_single_file;

/// The single-file build check (`meerschaum_plugin::api::check_route_path`) and the server's
/// startup check agree on every path.
#[test]
fn route_paths_are_checked_with_the_servers_rules() {
    for path in [
        "/",
        "/a",
        "/a/",
        "/a/b/c",
        "/a/{id}",
        "/a/{id:int}",
        "/a/{id:float}",
        "/a/{u:uuid}",
        "/a/{s:str}",
        "/a/{x:float32}",
        "/a/{*rest}",
        "/a/{rest:path}",
        "/a/{*rest}/b",
        "/a/x{*rest}",
        "/a/{name}.json",
        "/a/v{n:int}",
        "/a/{a}{b}",
        "/a/{a}/{a}",
        "/a/{1a}",
        "/a/{",
        "/a/}",
        "/a/:id",
        "/a/*rest",
        "/a/../b",
        "/a/./b",
        "/a?b",
        "/a#b",
        "/a%20",
        "/a b",
        "//a",
        "a",
        "/{id}",
        "/pipes/x",
        "/metadata",
        "/pipelines",
        "/actions/x",
        "/execute",
        "/connectors",
        "/jobs",
        "/jobsx",
        "/logs",
        "/users",
        "/tokens",
        "/plugins",
        "/version",
        "/login",
        "/engine",
        "/info",
        "/healthcheck",
        "/favicon.ico",
        "/id",
        "/chaining",
        "/docs",
        "/redoc",
        "/openapi.json",
        "/mcp",
        "/webterm",
        "/websocket",
        "/_mrsm",
        "/_mrsmx/a",
        "/dash",
        "/dash/x",
        "/_pages/x",
        "/static/x",
        "/my-plugin/items/{id}",
    ] {
        assert_eq!(
            meerschaum_plugin::api::check_route_path(path),
            meerschaum_server::native::check_route_path(path),
            "{path}"
        );
    }
}

#[cfg(unix)]
mod end_to_end {
    use super::compile_single_file;
    use std::process::{Child, Command, Output};
    use std::time::{Duration, Instant};

    use meerschaum_connector::DataQuery;
    use meerschaum_connector_sql::{
        SqlConnector,
        flavor::Flavor,
        uri::{ConnectionSpec, ConnectorAttributes},
    };
    use meerschaum_core::Pipe;

    const SOURCE: &str = r##"//! An instance, a plugin-level stream and a console page in one file.
use std::path::PathBuf;

fn store_path() -> PathBuf {
    PathBuf::from(std::env::var("SFP_STORE").expect("SFP_STORE"))
}

fn load() -> serde_json::Map<String, Value> {
    std::fs::read_to_string(store_path())
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default()
}

fn save(store: &serde_json::Map<String, Value>) {
    std::fs::write(store_path(), Value::Object(store.clone()).to_string()).unwrap();
}

fn key(pipe: &Pipe) -> String {
    format!("{}|{}|{}", pipe.connector_keys(), pipe.metric_key(), pipe.location_key().unwrap_or(""))
}

/// Synthetic readings `1..=3`.
fn fetch() -> Vec<Value> {
    (1..=3).map(|id| json!({"id": id, "value": id * 10})).collect()
}

/// Emits `fetch.count` one-row batches, then waits.
fn subscribe(pipe: &Pipe, kwargs: &Value) -> impl FnMut(Duration) -> Batch {
    let count = pipe.parameters()["fetch"]["count"].as_i64().unwrap_or(4);
    let chunksize = kwargs["chunksize"].clone();
    let mut next = 0;
    move |timeout| {
        if next >= count {
            std::thread::sleep(timeout);
            return Batch::Timeout;
        }
        next += 1;
        Batch::Rows(vec![json!({"id": next, "chunksize": chunksize})])
    }
}

/// Pipes and rows in a JSON file.
#[make_connector(instance)]
mod jsonstore {
    use super::*;

    fn register_pipe(pipe: &Pipe) -> SuccessTuple {
        let mut store = load();
        store.insert(key(pipe), json!({"keys": pipe.keys(), "parameters": pipe.parameters(), "rows": []}));
        save(&store);
        SuccessTuple::ok("Registered in the JSON store.")
    }

    fn pipe_exists(pipe: &Pipe) -> bool {
        load().contains_key(&key(pipe))
    }

    fn get_pipe_id(pipe: &Pipe) -> Option<i64> {
        load().keys().position(|k| *k == key(pipe)).map(|i| i as i64 + 1)
    }

    fn get_pipe_attributes(pipe: &Pipe) -> Value {
        match load().get(&key(pipe)) {
            Some(entry) => json!({"parameters": entry["parameters"]}),
            None => json!({}),
        }
    }

    fn fetch_pipes_keys() -> Value {
        Value::Array(load().values().map(|e| {
            json!([e["keys"]["connector_keys"], e["keys"]["metric_key"], e["keys"]["location_key"]])
        }).collect())
    }

    fn sync_pipe(pipe: &Pipe, data: Vec<Value>) -> (bool, String) {
        let mut store = load();
        let Some(entry) = store.get_mut(&key(pipe)) else {
            return (false, format!("{} is not registered.", key(pipe)));
        };
        let rows = entry["rows"].as_array_mut().unwrap();
        let mut inserted = 0;
        for row in data {
            if !rows.contains(&row) {
                rows.push(row);
                inserted += 1;
            }
        }
        save(&store);
        (true, format!("Inserted {inserted} rows into the JSON store."))
    }

    fn get_pipe_data(pipe: &Pipe) -> Vec<Value> {
        load().get(&key(pipe)).and_then(|e| e["rows"].as_array().cloned()).unwrap_or_default()
    }

    fn get_pipe_rowcount(pipe: &Pipe) -> i64 {
        get_pipe_data(pipe).len() as i64
    }

    fn get_sync_time(pipe: &Pipe, newest: bool) -> Option<i64> {
        let ids = get_pipe_data(pipe).iter().filter_map(|r| r["id"].as_i64()).collect::<Vec<_>>();
        if newest { ids.into_iter().max() } else { ids.into_iter().min() }
    }

    fn get_pipe_columns_types(pipe: &Pipe) -> Value {
        if get_pipe_data(pipe).is_empty() { json!({}) } else { json!({"id": "BIGINT", "value": "BIGINT"}) }
    }

    fn drop_pipe(pipe: &Pipe) -> SuccessTuple {
        let mut store = load();
        if let Some(entry) = store.get_mut(&key(pipe)) {
            entry["rows"] = json!([]);
        }
        save(&store);
        SuccessTuple::success()
    }

    fn delete_pipe(pipe: &Pipe) -> SuccessTuple {
        let mut store = load();
        store.remove(&key(pipe));
        save(&store);
        SuccessTuple::success()
    }
}

/// A report page.
#[web_page("sfp/report", login_required = false, page_group = "Parity")]
fn report(page: &PageRequest) -> String {
    format!("<p>Report for {} ({}).</p>", page.username.as_deref().unwrap_or("guest"), page.query_param("q").unwrap_or_default())
}

/// A page that needs a session.
#[web_page]
fn private() -> Result<String> {
    Ok("<p>Private.</p>".into())
}
"##;

    struct Home {
        root: tempfile::TempDir,
        plugins: tempfile::TempDir,
    }

    impl Home {
        fn new() -> Home {
            let home = Home {
                root: tempfile::tempdir().unwrap(),
                plugins: tempfile::tempdir().unwrap(),
            };
            std::fs::create_dir_all(home.root.path().join("sqlite")).unwrap();
            std::fs::write(home.plugins.path().join("sfp.rs"), SOURCE).unwrap();
            let builds = home.root.path().join("cache/native_build");
            let found = meerschaum_plugin::discovery::discover_report_in(
                &[home.plugins.path().into()],
                &builds,
            );
            assert!(found.skipped.is_empty(), "{:?}", found.skipped);
            let candidate = &found.candidates[0];
            compile_single_file::compile(candidate);
            std::fs::write(
                builds.join("sfp/mrsm-source-fingerprint"),
                &candidate.fingerprint,
            )
            .unwrap();
            home
        }

        fn store(&self) -> std::path::PathBuf {
            self.root.path().join("store.json")
        }

        fn command(&self) -> Command {
            let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
            command
                .env_clear()
                .env("PATH", std::env::var_os("PATH").unwrap_or_default())
                .env("HOME", self.root.path())
                .env("MRSM_ROOT_DIR", self.root.path())
                .env("MRSM_PLUGINS_DIR", self.plugins.path())
                .env("MRSM_NO_UPDATE_CHECK", "1")
                .env("MRSM_NOASK", "true")
                .env("SFP_STORE", self.store())
                .env(
                    "MRSM_CONFIG",
                    r#"{"meerschaum":{"instance":"sql:local"},"formatting":{"ansi":false,"unicode":false}}"#,
                );
            command
        }

        fn run(&self, args: &[&str]) -> (bool, String) {
            let Output {
                status,
                stdout,
                stderr,
            } = self.command().args(args).output().unwrap();
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            );
            assert!(
                !text.contains("Building native plugin"),
                "the fixture build must be reused: {text}"
            );
            (status.success(), text)
        }

        fn stored(&self) -> serde_json::Value {
            serde_json::from_str(&std::fs::read_to_string(self.store()).unwrap_or_default())
                .unwrap_or_default()
        }
    }

    fn wait_for(deadline: Duration, mut done: impl FnMut() -> bool) -> bool {
        let until = Instant::now() + deadline;
        while Instant::now() < until {
            if done() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        done()
    }

    fn interrupt(child: &mut Child) {
        let _ = Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status();
        let deadline = Instant::now() + Duration::from_secs(15);
        while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn a_single_file_instance_registers_syncs_lists_and_deletes_pipes() {
        let home = Home::new();
        let key = "plugin:sfp|readings|";
        let (ok, out) = home.run(&[
            "register",
            "pipes",
            "-c",
            "plugin:sfp",
            "-m",
            "readings",
            "-i",
            "jsonstore:test",
            "--params",
            r#"{"columns": {"datetime": "id"}, "dtypes": {"id": "int", "value": "int"}}"#,
        ]);
        assert!(ok, "{out}");
        assert_eq!(
            home.stored()[key]["parameters"]["columns"]["datetime"],
            "id",
            "{out}"
        );
        let (ok, out) = home.run(&["sync", "pipes", "-i", "jsonstore:test", "--nopretty"]);
        assert!(ok, "{out}");
        assert!(out.contains(r#""success": true"#), "{out}");
        let ids = |home: &Home| -> Vec<i64> {
            home.stored()[key]["rows"]
                .as_array()
                .map(|rows| rows.iter().filter_map(|r| r["id"].as_i64()).collect())
                .unwrap_or_default()
        };
        assert_eq!(ids(&home), vec![1, 2, 3], "{out}");
        let (ok, out) = home.run(&["show", "pipes", "-i", "jsonstore:test", "--nopretty"]);
        assert!(ok, "{out}");
        assert!(out.contains("plugin:sfp"), "{out}");
        assert!(out.contains("readings"), "{out}");
        let (ok, out) = home.run(&[
            "delete",
            "pipes",
            "-c",
            "plugin:sfp",
            "-i",
            "jsonstore:test",
            "-y",
            "-f",
        ]);
        assert!(ok, "{out}");
        assert!(home.stored().get(key).is_none(), "{out}");
    }

    fn sqlite(rt: &tokio::runtime::Runtime, root: &std::path::Path) -> SqlConnector {
        let attrs = ConnectorAttributes {
            flavor: Some(Flavor::Sqlite),
            database: Some(
                root.join("sqlite/mrsm_local.db")
                    .to_string_lossy()
                    .into_owned(),
            ),
            ..Default::default()
        };
        rt.block_on(SqlConnector::connect(
            "local",
            ConnectionSpec::from_attributes(&attrs).unwrap(),
        ))
        .unwrap()
    }

    #[test]
    fn a_plugin_level_subscribe_feeds_a_listen_sync() {
        let home = Home::new();
        let (ok, out) = home.run(&[
            "register",
            "pipes",
            "-c",
            "plugin:sfp",
            "-m",
            "stream",
            "-i",
            "sql:local",
            "--params",
            r#"{"fetch": {"count": 4}, "columns": {"datetime": "id"}, "dtypes": {"id": "int"}}"#,
        ]);
        assert!(ok, "{out}");
        let log_path = home.root.path().join("listen.log");
        let log = std::fs::File::create(&log_path).unwrap();
        let mut child = home
            .command()
            .args([
                "sync",
                "pipes",
                "-c",
                "plugin:sfp",
                "-m",
                "stream",
                "-i",
                "sql:local",
                "--listen",
                "--chunksize",
                "2",
            ])
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let conn = sqlite(&rt, home.root.path());
        let pipe = Pipe::builder("plugin:sfp", "stream")
            .instance("sql:local")
            .build()
            .unwrap();
        let rowcount = || {
            rt.block_on(conn.get_pipe_rowcount(&pipe, &DataQuery::default()))
                .ok()
                .flatten()
                .unwrap_or(0)
        };
        let landed = wait_for(Duration::from_secs(90), || rowcount() >= 4);
        interrupt(&mut child);
        let output = std::fs::read_to_string(&log_path).unwrap_or_default();
        assert!(landed, "rows: {}\n{output}", rowcount());
        assert_eq!(rowcount(), 4, "{output}");
        assert!(output.contains("Listening for new data"), "{output}");
        let data = rt
            .block_on(conn.get_pipe_data(&pipe, &DataQuery::default()))
            .unwrap()
            .unwrap();
        let chunksize = data
            .columns()
            .iter()
            .position(|c| c.name == "chunksize")
            .expect("a chunksize column");
        assert_eq!(
            data.row(0)[chunksize],
            meerschaum_kernel::Value::Int(2),
            "{output}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_single_file_web_page_is_served_to_the_console() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use meerschaum_cli::dispatch::Runtime;
        use meerschaum_cli::native::{self, Session};
        use meerschaum_core::config::{Config, ConfigOptions};
        use meerschaum_server::mcp::McpServer;
        use meerschaum_server::mcp::federation::{TOKEN_HEADER, internal_token};
        use meerschaum_server::{AppState, ServerConfig, ServerFlags};
        use tower::ServiceExt;

        let (home, app, token) = tokio::task::spawn_blocking(|| {
            let home = Home::new();
            let config = Config::new(ConfigOptions::with_env(vec![
                (
                    "MRSM_ROOT_DIR".into(),
                    home.root.path().display().to_string(),
                ),
                (
                    "MRSM_PLUGINS_DIR".into(),
                    home.plugins.path().display().to_string(),
                ),
            ]))
            .unwrap();
            let mut out = Vec::new();
            let mut rt = Runtime::system(&mut out);
            rt.config = Some(config);
            let session = Session::new();
            rt.native = Some(session.clone());
            let server_config = ServerConfig::from_config(rt.config(), &ServerFlags::default());
            let instance = session
                .instance(&server_config.instance_keys, rt.config())
                .unwrap();
            let manifest = native::session(&mut rt).manifest(rt.config()).unwrap();
            let state: AppState = AppState::builder(server_config, instance.conn())
                .build()
                .unwrap();
            let token = internal_token(state.secret(), None);
            let console = meerschaum_cli::server::console(&state, &manifest, &mut rt).unwrap();
            let mcp = std::sync::Arc::new(McpServer::for_state(&state));
            let app = meerschaum_server::app_with_mcp_and_console(state, mcp, console);
            // The runtime (and its plugins) must outlive the in-process requests.
            std::mem::forget(rt);
            std::mem::forget(session);
            (home, app, token)
        })
        .await
        .unwrap();
        let get = |uri: &str, user: Option<&str>| {
            let mut request = Request::get(uri).header(TOKEN_HEADER, &token);
            if let Some(user) = user {
                request = request.header("x-mrsm-user", user);
            }
            let app = app.clone();
            let request = request.body(Body::empty()).unwrap();
            async move {
                let response = app.oneshot(request).await.unwrap();
                let status = response.status();
                let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap();
                (status, String::from_utf8_lossy(&body).into_owned())
            }
        };
        let (status, body) = get("/_mrsm/native-pages", None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let pages: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            pages,
            serde_json::json!([
                {"path": "sfp/report", "page_group": "Parity", "login_required": false,
                    "skip_navbar": false, "dark_theme": true, "plugin": "sfp"},
                {"path": "private", "page_group": "sfp", "login_required": true,
                    "skip_navbar": false, "dark_theme": true, "plugin": "sfp"},
            ])
        );
        let (status, body) = get("/_mrsm/native-pages/sfp/report?q=a%20b", Some("ada")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body, "<p>Report for ada (a b).</p>");
        let (status, body) = get("/_mrsm/native-pages/sfp/report", None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body, "<p>Report for guest ().</p>");
        // A page that requires login needs the console session's user.
        let (status, _) = get("/_mrsm/native-pages/private", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, body) = get("/_mrsm/native-pages/private", Some("ada")).await;
        assert_eq!((status, body.as_str()), (StatusCode::OK, "<p>Private.</p>"));
        drop(home);
    }
}
