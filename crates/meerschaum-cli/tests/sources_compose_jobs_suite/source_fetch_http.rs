//! Real authenticated HTTP + owned SQLite canonical-source broker acceptance.

use std::sync::Arc;
use std::time::{Duration, Instant};

use meerschaum_cli::native::Session;
use meerschaum_cli::source_fetch::backend::NativeSqlSources;
use meerschaum_connector::InstanceConnector;
use meerschaum_connector_api::source_fetch::{self, Request};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::users::User;
use meerschaum_core::{Config, Map, Pipe, Value};
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value as Json, json};

fn map(value: Json) -> Map {
    serde_json::from_value::<Value>(value).unwrap().to_map()
}

struct Served {
    _root: tempfile::TempDir,
    main: Arc<SqlConnector>,
    source: Arc<SqlConnector>,
    client: reqwest::Client,
    origin: String,
    bearer: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Served {
    async fn new(timeout: Duration) -> Self {
        let parent = std::env::var_os("MRSM_ROOT_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        std::fs::create_dir_all(&parent).unwrap();
        let root = tempfile::Builder::new()
            .prefix("source-broker-")
            .tempdir_in(parent)
            .unwrap();
        let main_uri = format!("sqlite:///{}", root.path().join("main.db").display());
        let source_uri = format!("sqlite:///{}", root.path().join("source.db").display());
        let main = Arc::new(SqlConnector::from_uri("main", &main_uri).await.unwrap());
        let mut spec = meerschaum_connector_sql::uri::ConnectionSpec::from_uri(
            &source_uri,
            Some("source"),
            None,
        )
        .unwrap();
        spec.pool_size = 1;
        spec.max_overflow = 0;
        let source = Arc::new(SqlConnector::connect("source", spec).await.unwrap());
        let mut protected_user = User::new(
            "protected_fixture_user",
            Some("synthetic-internal-credential"),
        );
        assert!(source.register_user(&mut protected_user).await.success);
        for name in ["alice", "bob"] {
            let mut user = User::new(name, Some("synthetic-pass"));
            user.attributes = Some(map(json!({"role":"editor"})));
            let result = main.register_user(&mut user).await;
            assert!(result.success, "{}", result.message);
        }
        for (name, user) in [("Data", "alice"), ("Other", "bob")] {
            let mut team = Team::new(name);
            team.add_member(user, TeamRole::Member);
            let result = main.register_team(&mut team).await;
            assert!(result.success, "{}", result.message);
        }
        for statement in [
            "CREATE TABLE public_rows (id INTEGER, value TEXT)",
            "INSERT INTO public_rows VALUES (1, 'one'), (2, 'two'), (3, 'three')",
            "CREATE TABLE secret_rows (value TEXT)",
            "INSERT INTO secret_rows VALUES ('synthetic-private-data')",
            "CREATE VIEW credential_view AS SELECT * FROM mrsm_users",
            "CREATE VIEW hidden_view AS SELECT * FROM secret_rows",
        ] {
            source.driver().exec(statement).await.unwrap();
        }
        let hidden = Pipe::builder("plugin:private", "hidden").instance("sql:source")
            .parameters(map(json!({"target":"secret_rows","access":{"owner":"bob","team":"Other","visibility":"private"}})))
            .build().unwrap();
        assert!(source.register_pipe(&hidden).await.success);
        let config = Arc::new(
            Config::new(meerschaum_core::config::ConfigOptions::with_env(vec![(
                "MRSM_ROOT_DIR".into(),
                root.path().display().to_string(),
            )]))
            .unwrap(),
        );
        config.set_config(map(
            json!({"meerschaum":{"instance":"sql:main","connectors":{"sql":{
            "main":{"uri":main_uri},"source":{"uri":source_uri,"teams":["Data"]}}}}}),
        ));
        let session = Session::with_runtime(tokio::runtime::Handle::current());
        session.register_sql("sql:main", main.clone());
        session.register_sql("sql:source", source.clone());
        let mut cfg = ServerConfig::for_root("sql:main", root.path());
        cfg.no_dash = true;
        cfg.no_webterm = true;
        cfg.allow_multiple_instances = true;
        cfg.allowed_instance_keys = vec!["sql:main".into(), "sql:source".into()];
        cfg.connectors = map(json!({"sql":{"main":{},"source":{"teams":["Data"]}}}));
        let state = AppState::builder(cfg, main.clone())
            .secret(b"synthetic-source-broker-signing-key-0123456789".to_vec())
            .source_fetch(Arc::new(
                NativeSqlSources::new(session, config)
                    .with_timeout(timeout)
                    .unwrap(),
            ))
            .build()
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, meerschaum_server::app(state)).await;
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let response = client
            .post(format!("{origin}/login"))
            .header("content-type", "application/x-www-form-urlencoded")
            .body("username=alice&password=synthetic-pass")
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let login: Json = response.json().await.unwrap();
        let bearer = login["access_token"].as_str().unwrap().to_string();
        Self {
            _root: root,
            main,
            source,
            client,
            origin,
            bearer,
            task,
        }
    }

    async fn pipe(&self, metric: &str, definition: &str) -> Pipe {
        let pipe = Pipe::builder("sql:source", metric)
            .instance("sql:main")
            .parameters(map(
                json!({"columns":{"id":"id"},"fetch":{"definition":definition},
                "access":{"owner":"alice","team":"Data","visibility":"team"}}),
            ))
            .build()
            .unwrap();
        let result = self.main.register_pipe(&pipe).await;
        assert!(result.success, "{}", result.message);
        pipe
    }

    async fn fetch(&self, request: &Request) -> (u16, Json) {
        let response = self
            .client
            .post(format!(
                "{}{}",
                self.origin,
                source_fetch::path(&request.pipe)
            ))
            .bearer_auth(&self.bearer)
            .json(request)
            .send()
            .await
            .unwrap();
        (response.status().as_u16(), response.json().await.unwrap())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_sql_source_is_authorized_bounded_and_never_accepts_client_definitions() {
    let server = Served::new(Duration::from_secs(2)).await;
    let pipe = server
        .pipe("allowed", "SELECT id, value FROM public_rows")
        .await;
    let mut request = Request::for_pipe("sql:source", &pipe, &Default::default()).unwrap();
    let (status, result) = server.fetch(&request).await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result["rows"].as_array().unwrap().len(), 3);
    assert_eq!(result["complete"], true);
    request.row_limit = 2;
    let (status, result) = server.fetch(&request).await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result["complete"], false);
    assert_eq!(result["rows"].as_array().unwrap().len(), 2);
    request.limit = Some(1);
    let (status, result) = server.fetch(&request).await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result["complete"], true);
    assert_eq!(result["rows"].as_array().unwrap().len(), 1);
    let mut spoof = request.clone();
    spoof.instance_keys = "sql:other".into();
    assert_eq!(server.fetch(&spoof).await.0, 403);
    let mut spoof = request.clone();
    spoof.source_keys = "sql:main".into();
    assert_eq!(server.fetch(&spoof).await.0, 400);
    let mut encoded = serde_json::to_value(&request).unwrap();
    encoded["definition"] = "SELECT * FROM mrsm_users".into();
    let response = server
        .client
        .post(format!(
            "{}{}",
            server.origin,
            source_fetch::path(&request.pipe)
        ))
        .bearer_auth(&server.bearer)
        .json(&encoded)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 400);
    let response = server
        .client
        .post(format!(
            "{}{}?instance_keys=sql:source",
            server.origin,
            source_fetch::path(&request.pipe)
        ))
        .bearer_auth(&server.bearer)
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 400);
    let mut user = User::new("alice", None);
    user.attributes = Some(map(json!({"role":"viewer"})));
    assert!(
        server
            .main
            .set_user_attributes(&user, user.attributes.as_ref().unwrap())
            .await
            .success
    );
    assert_eq!(
        server.fetch(&request).await.0,
        403,
        "fresh role no longer grants sql:read"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stored_sql_templates_and_views_cannot_read_sibling_or_internal_data() {
    let server = Served::new(Duration::from_secs(2)).await;
    let public = Pipe::builder("plugin:catalog", "public")
        .instance("sql:main")
        .parameters(map(json!({"target":"public_rows","sql_name":"secret_rows",
            "access":{"owner":"alice","team":"Data","visibility":"team"}})))
        .build()
        .unwrap();
    assert!(server.main.register_pipe(&public).await.success);
    let positive = server
        .pipe(
            "template_allowed",
            "SELECT * FROM {{ Pipe('plugin:catalog', 'public') }}",
        )
        .await;
    let (status, result) = server
        .fetch(&Request::for_pipe("sql:source", &positive, &Default::default()).unwrap())
        .await;
    assert_eq!(status, 200, "{result}");
    for (metric, definition) in [
        ("direct_private", "SELECT * FROM secret_rows"),
        ("internal", "SELECT * FROM mrsm_users"),
        ("credential_view", "SELECT * FROM credential_view"),
        ("hidden_view", "SELECT * FROM hidden_view"),
        ("qualified_view", "SELECT * FROM main.credential_view"),
        (
            "view_cte_confusion",
            "WITH credential_view AS (SELECT 1) SELECT * FROM main.credential_view",
        ),
        (
            "eponymous",
            "SELECT * FROM dbstat WHERE EXISTS (WITH dbstat AS (SELECT 1) SELECT 1)",
        ),
        (
            "expanded_private",
            "SELECT * FROM {{ Pipe('plugin:catalog', 'public').parameters['sql_name'] }}",
        ),
        ("file_function", "SELECT readfile('/synthetic-secret')"),
    ] {
        let pipe = server.pipe(metric, definition).await;
        let (status, result) = server
            .fetch(&Request::for_pipe("sql:source", &pipe, &Default::default()).unwrap())
            .await;
        assert_eq!(status, 403, "{metric}: {result}");
        assert!(!result.to_string().contains("synthetic-internal-credential"));
        assert!(!result.to_string().contains("synthetic-private-data"));
    }
    let mut poisoned = server
        .pipe(
            "config_field",
            "SELECT '{{ self.parameters.hidden }}' AS value",
        )
        .await;
    let mut parameters = poisoned.raw_parameters().clone();
    parameters.insert("hidden".into(), "MRSM{env:SYNTHETIC_SECRET}".into());
    poisoned.set_parameters(parameters);
    assert!(server.main.edit_pipe(&poisoned, false).await.success);
    assert_eq!(
        server
            .fetch(&Request::for_pipe("sql:source", &poisoned, &Default::default()).unwrap())
            .await
            .0,
        403
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expensive_query_vm_is_interrupted_and_the_connection_budget_is_cleared() {
    let server = Served::new(Duration::from_millis(30)).await;
    let joins = (0..24)
        .map(|index| format!("CROSS JOIN public_rows AS p{index}"))
        .collect::<Vec<_>>()
        .join(" ");
    let pipe = server
        .pipe(
            "expensive",
            &format!("SELECT count(*) AS n FROM public_rows {joins}"),
        )
        .await;
    let started = Instant::now();
    let (status, result) = server
        .fetch(&Request::for_pipe("sql:source", &pipe, &Default::default()).unwrap())
        .await;
    assert!(matches!(status, 422 | 503), "{status}: {result}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "VM progress budget must stop the query itself"
    );
    // One physical connection: after its budget expires, unrelated reads still work.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let frame = server.source.read("SELECT 1 AS value").await.unwrap();
    assert_eq!(frame.height(), 1);
    let good = server
        .pipe("after_interrupt", "SELECT * FROM public_rows")
        .await;
    let (status, result) = server
        .fetch(&Request::for_pipe("sql:source", &good, &Default::default()).unwrap())
        .await;
    assert_eq!(status, 200, "{result}");
}
