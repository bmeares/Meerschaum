//! Opt-in, real authenticated PG broker acceptance on a root-owned isolated fixture only.

use std::sync::Arc;
use std::time::{Duration, Instant};

use meerschaum_cli::native::Session;
use meerschaum_cli::source_fetch::postgres::PostgresSources;
use meerschaum_connector::{DataQuery, DateBound, InstanceConnector};
use meerschaum_connector_api::source_fetch::{self, Request};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::uri::{ConnectionOptions, ConnectionSpec};
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::users::User;
use meerschaum_core::{Config, Map, Pipe, Value};
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value as Json, json};

fn map(value: Json) -> Map {
    serde_json::from_value::<Value>(value).unwrap().to_map()
}
fn quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
fn table(schema: &str, name: &str) -> String {
    format!("{}.{}", quote(schema), quote(name))
}

struct Served {
    _root: tempfile::TempDir,
    main: Arc<SqlConnector>,
    source: Arc<SqlConnector>,
    admin: Arc<SqlConnector>,
    schema: String,
    fdw: String,
    private: Vec<Pipe>,
    protected_user: User,
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
    async fn new(mut spec: ConnectionSpec) -> Self {
        // Never use the default database URI, owner ports or a remote database by accident.
        assert!(matches!(spec.host.as_deref(), Some("127.0.0.1" | "::1")));
        assert!(spec.port.is_some_and(|port| port > 1024 && port != 5432));
        assert!(
            spec.database
                .as_deref()
                .is_some_and(|name| name.starts_with("tpr_"))
        );
        let id = uuid::Uuid::new_v4().simple().to_string();
        let schema = format!("tpr_source_{}", &id[..12]);
        let fdw = format!("tpr_fdw_{}", &id[..12]);
        let parent = std::env::var_os("MRSM_ROOT_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        std::fs::create_dir_all(&parent).unwrap();
        let root = tempfile::Builder::new()
            .prefix("source-pg-")
            .tempdir_in(parent)
            .unwrap();
        let main = Arc::new(
            SqlConnector::from_uri(
                "main",
                &format!("sqlite:///{}", root.path().join("main.db").display()),
            )
            .await
            .unwrap(),
        );
        let admin = Arc::new(
            SqlConnector::connect("pg_fixture_admin", spec.clone())
                .await
                .unwrap(),
        );
        admin
            .driver()
            .exec(&format!("CREATE SCHEMA {}", quote(&schema)))
            .await
            .unwrap();
        spec.label = Some("source".into());
        spec.schema = Some(schema.clone());
        spec.pool_size = 1;
        spec.max_overflow = 0;
        spec.options = Some(ConnectionOptions::Str(format!(
            "-c search_path={schema},public"
        )));
        let source = Arc::new(SqlConnector::connect("source", spec).await.unwrap());
        let mut protected_user = User::new(
            &format!("tpr_source_{}", &id[..12]),
            Some("synthetic-pg-pass"),
        );
        protected_user.attributes = Some(map(json!({"private":"synthetic_pg_internal_secret"})));
        assert!(source.register_user(&mut protected_user).await.success);
        for name in ["alice", "bob"] {
            let mut user = User::new(name, Some("synthetic-pass"));
            user.attributes = Some(map(json!({"role":"editor"})));
            assert!(main.register_user(&mut user).await.success);
        }
        for (name, username) in [("Data", "alice"), ("Other", "bob")] {
            let mut team = Team::new(name);
            team.add_member(username, TeamRole::Member);
            assert!(main.register_team(&mut team).await.success);
        }
        for statement in [
            format!(
                "CREATE TABLE {} (id INTEGER PRIMARY KEY, value TEXT, created TIMESTAMPTZ)",
                table(&schema, "rows")
            ),
            format!(
                "INSERT INTO {} VALUES (1,'one','2026-01-01T00:00:00Z'),(2,'two','2026-01-02T00:00:00Z'),(3,'three','2026-01-03T00:00:00Z')",
                table(&schema, "rows")
            ),
            format!(
                "CREATE TABLE {} (id INTEGER PRIMARY KEY, label TEXT)",
                table(&schema, "lookup")
            ),
            format!(
                "INSERT INTO {} VALUES(1,'a'),(2,'b'),(3,'c')",
                table(&schema, "lookup")
            ),
            format!(
                "CREATE TABLE {} (value TEXT)",
                table(&schema, "private_rows")
            ),
            format!(
                "INSERT INTO {} VALUES ('synthetic_pg_private_data')",
                table(&schema, "private_rows")
            ),
            format!(
                "CREATE VIEW {} AS SELECT * FROM {}",
                table(&schema, "credential_view"),
                source.instance_table_name("mrsm_users")
            ),
            format!(
                "CREATE MATERIALIZED VIEW {} AS SELECT * FROM {}",
                table(&schema, "credential_mat"),
                source.instance_table_name("mrsm_users")
            ),
            format!(
                "CREATE VIEW {} AS SELECT * FROM {}",
                table(&schema, "hidden_view"),
                table(&schema, "private_rows")
            ),
            format!(
                "CREATE TABLE {} (id INTEGER, value TEXT)",
                table(&schema, "public_parent")
            ),
            format!(
                "CREATE TABLE {} () INHERITS ({})",
                table(&schema, "private_child"),
                table(&schema, "public_parent")
            ),
            format!(
                "INSERT INTO {} VALUES (1,'synthetic_pg_inherited_private')",
                table(&schema, "private_child")
            ),
            format!(
                "CREATE TABLE {} (id INTEGER)",
                table(&schema, "foreign_parent")
            ),
            "CREATE EXTENSION IF NOT EXISTS postgres_fdw".into(),
            format!(
                "CREATE SERVER {} FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.0.1', port '1', dbname 'tpr_unused')",
                quote(&fdw)
            ),
            format!(
                "CREATE FOREIGN TABLE {} (id INTEGER) SERVER {}",
                table(&schema, "foreign_rows"),
                quote(&fdw)
            ),
            format!(
                "ALTER FOREIGN TABLE {} INHERIT {}",
                table(&schema, "foreign_rows"),
                table(&schema, "foreign_parent")
            ),
            format!("CREATE DOMAIN {} AS TEXT", table(&schema, "user_domain")),
            format!(
                "CREATE TABLE {} (value {})",
                table(&schema, "custom_type_rows"),
                table(&schema, "user_domain")
            ),
            format!("CREATE TABLE {} (id INTEGER)", table(&schema, "ΔRows")),
            format!("INSERT INTO {} VALUES (7)", table(&schema, "ΔRows")),
            format!(
                "CREATE FUNCTION {}(text) RETURNS text LANGUAGE SQL SECURITY DEFINER AS $$ SELECT 'synthetic_pg_function_escape'::text $$",
                table(&schema, "lower")
            ),
            format!(
                "CREATE FUNCTION {}(text,text) RETURNS text LANGUAGE SQL SECURITY DEFINER AS $$ SELECT value FROM {} LIMIT 1 $$",
                table(&schema, "text_plus"),
                table(&schema, "private_rows")
            ),
            format!(
                "CREATE OPERATOR {}.+ (LEFTARG=text, RIGHTARG=text, FUNCTION={})",
                quote(&schema),
                table(&schema, "text_plus")
            ),
        ] {
            admin.driver().exec(&statement).await.unwrap();
        }
        let mut private = Vec::new();
        for target in ["private_rows", "private_child"] {
            let pipe = Pipe::builder(&format!("plugin:tpr_private_{}",&id[..12]), target).instance("sql:source")
                .parameters(map(json!({"schema":schema,"target":target,"access":{"owner":"bob","team":"Other","visibility":"private"}}))).build().unwrap();
            assert!(source.register_pipe(&pipe).await.success);
            private.push(pipe);
        }
        let config = Arc::new(
            Config::new(meerschaum_core::config::ConfigOptions::with_env(vec![(
                "MRSM_ROOT_DIR".into(),
                root.path().display().to_string(),
            )]))
            .unwrap(),
        );
        config.set_config(map(json!({"meerschaum":{"instance":"sql:main","connectors":{"sql":{"main":{},"source":{"teams":["Data"]}}}}})));
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
            .secret(b"synthetic-pg-source-broker-key-0123456789".to_vec())
            .source_fetch(Arc::new(
                PostgresSources::new(session, config)
                    .with_timeout(Duration::from_secs(1))
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
            admin,
            schema,
            fdw,
            private,
            protected_user,
            client,
            origin,
            bearer,
            task,
        }
    }

    async fn pipe(&self, metric: &str, definition: &str, integer_axis: bool) -> Pipe {
        let pipe=Pipe::builder("sql:source",metric).instance("sql:main").parameters(map(json!({
            "columns":{"id":"id","datetime":if integer_axis {"id"} else {"created"}},
            "dtypes":{"id":"int64","created":"datetime64[ns, UTC]"},"fetch":{"definition":definition},
            "access":{"owner":"alice","team":"Data","visibility":"team"}}))).build().unwrap();
        assert!(self.main.register_pipe(&pipe).await.success);
        pipe
    }
    async fn fetch(&self, pipe: &Pipe, query: DataQuery) -> (u16, Json) {
        let request = Request::for_pipe("sql:source", pipe, &query).unwrap();
        let response = self
            .client
            .post(format!(
                "{}{}",
                self.origin,
                source_fetch::path(&request.pipe)
            ))
            .bearer_auth(&self.bearer)
            .json(&request)
            .send()
            .await
            .unwrap();
        (response.status().as_u16(), response.json().await.unwrap())
    }
    async fn cleanup(&self) {
        for pipe in &self.private {
            assert!(self.source.delete_pipe(pipe).await.success);
        }
        assert!(self.source.delete_user(&self.protected_user).await.success);
        self.source.driver().close().await;
        self.admin
            .driver()
            .exec(&format!("DROP SERVER {} CASCADE", quote(&self.fdw)))
            .await
            .unwrap();
        self.admin
            .driver()
            .exec(&format!("DROP SCHEMA {} CASCADE", quote(&self.schema)))
            .await
            .unwrap();
        self.admin.driver().close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_pg_broker_pins_data_and_rejects_untrusted_object_paths() {
    let Some(uri) = std::env::var_os("MRSM_TEST_SOURCE_PG_URI") else {
        eprintln!("set MRSM_TEST_SOURCE_PG_URI to an owned loopback tpr_* PostgreSQL fixture");
        return;
    };
    let spec = ConnectionSpec::from_uri(&uri.to_string_lossy(), Some("source"), None)
        .unwrap_or_else(|_| panic!("Invalid owned PG fixture settings"));
    let server = Served::new(spec).await;
    let original_budget = server
        .source
        .read("SHOW statement_timeout")
        .await
        .unwrap()
        .row(0)[0]
        .py_str();
    let base = server
        .pipe(
            "basic",
            &format!(
                "SELECT id,value,created FROM {}",
                table(&server.schema, "rows")
            ),
            false,
        )
        .await;
    let (status, result) = server.fetch(&base, DataQuery::default()).await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result["rows"].as_array().unwrap().len(), 3);
    let filtered = DataQuery {
        begin: DateBound::parse("2026-01-02T00:00:00Z"),
        end: DateBound::parse("2026-01-04T00:00:00Z"),
        params: Some(map(json!({"id":[2,3]}))),
        ..Default::default()
    };
    let definition = server
        .source
        .get_pipe_metadef_for_sync(
            &base,
            &meerschaum_connector_sql::instance::fetch::FetchArgs {
                begin: meerschaum_connector_sql::instance::fetch::metadef_bound(
                    filtered.begin.as_ref(),
                ),
                end: meerschaum_connector_sql::instance::fetch::metadef_bound(
                    filtered.end.as_ref(),
                ),
                check_existing: false,
                params: filtered.params.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    let (status, result) = server.fetch(&base, filtered).await;
    assert_eq!(
        status, 200,
        "{result}; synthetic canonical query: {definition:?}"
    );
    assert_eq!(result["rows"].as_array().unwrap().len(), 2);
    let integer = server
        .pipe(
            "integer",
            &format!(
                "SELECT id,value,created FROM {}",
                table(&server.schema, "rows")
            ),
            true,
        )
        .await;
    let (status, result) = server
        .fetch(
            &integer,
            DataQuery {
                begin: Some(DateBound::Int(2)),
                end: Some(DateBound::Int(3)),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result["rows"][0]["id"], 2);
    let join = server
        .pipe(
            "joined",
            &format!(
                "SELECT a.id,a.value,b.label,a.created FROM {} a JOIN {} b ON a.id=b.id",
                table(&server.schema, "rows"),
                table(&server.schema, "lookup")
            ),
            false,
        )
        .await;
    assert_eq!(server.fetch(&join, DataQuery::default()).await.0, 200);
    let public = Pipe::builder("plugin:catalog", "public")
        .instance("sql:main")
        .parameters(map(
            json!({"target":"rows","access":{"owner":"alice","team":"Data","visibility":"team"}}),
        ))
        .build()
        .unwrap();
    assert!(server.main.register_pipe(&public).await.success);
    let templated = server
        .pipe(
            "template",
            "SELECT id,value,created FROM {{ Pipe('plugin:catalog','public') }}",
            false,
        )
        .await;
    let (status, result) = server.fetch(&templated, DataQuery::default()).await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result["rows"].as_array().unwrap().len(), 3);
    let functions = server
        .pipe(
            "functions",
            &format!(
                "SELECT id,lower(value) AS value,created,id + 2 * id AS computed FROM {}",
                table(&server.schema, "rows")
            ),
            false,
        )
        .await;
    let (status, result) = server
        .fetch(
            &functions,
            DataQuery {
                order: Some("asc".into()),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result["rows"][0]["value"], "one");
    assert_eq!(result["rows"][1]["computed"], 6);
    for (metric, definition) in [
        (
            "credential_view",
            format!("SELECT * FROM {}", table(&server.schema, "credential_view")),
        ),
        (
            "credential_mat",
            format!("SELECT * FROM {}", table(&server.schema, "credential_mat")),
        ),
        (
            "cte_view_self",
            "WITH credential_view AS (SELECT * FROM credential_view) SELECT * FROM credential_view".into(),
        ),
        (
            "cte_view_forward",
            "WITH first AS (SELECT * FROM credential_view), credential_view AS (SELECT 1) SELECT * FROM first".into(),
        ),
        (
            "hidden_view",
            format!("SELECT * FROM {}", table(&server.schema, "hidden_view")),
        ),
        (
            "inheritance",
            format!("SELECT * FROM {}", table(&server.schema, "public_parent")),
        ),
        (
            "foreign",
            format!("SELECT * FROM {}", table(&server.schema, "foreign_rows")),
        ),
        (
            "foreign_parent",
            format!("SELECT * FROM {}", table(&server.schema, "foreign_parent")),
        ),
        (
            "custom_type",
            format!(
                "SELECT * FROM {}",
                table(&server.schema, "custom_type_rows")
            ),
        ),
        (
            "cast",
            format!(
                "SELECT CAST('x' AS {}) AS value",
                table(&server.schema, "user_domain")
            ),
        ),
        (
            "schema_function",
            format!(
                "SELECT {}(value) AS value FROM {}",
                table(&server.schema, "lower"),
                table(&server.schema, "rows")
            ),
        ),
    ] {
        let pipe = server.pipe(metric, &definition, false).await;
        let (status, result) = server.fetch(&pipe, DataQuery::default()).await;
        assert_eq!(status, 403, "{metric}: {result}");
        assert!(!result.to_string().contains("synthetic_pg_internal_secret"));
        assert!(!result.to_string().contains("synthetic_pg_private_data"));
    }
    let operator = server
        .pipe(
            "operator",
            &format!(
                "SELECT value + value AS value FROM {}",
                table(&server.schema, "rows")
            ),
            false,
        )
        .await;
    let (status, result) = server.fetch(&operator, DataQuery::default()).await;
    assert!(matches!(status, 403 | 422), "{status}: {result}");
    assert!(!result.to_string().contains("synthetic_pg_private_data"));
    let unicode = server
        .pipe(
            "unicode",
            &format!("SELECT id FROM {}", table(&server.schema, "ΔRows")),
            true,
        )
        .await;
    let (status, result) = server.fetch(&unicode, DataQuery::default()).await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result["rows"][0]["id"], 7);
    let joins = (0..24)
        .map(|i| format!("CROSS JOIN {} AS p{i}", table(&server.schema, "rows")))
        .collect::<Vec<_>>()
        .join(" ");
    let expensive = server
        .pipe(
            "timeout",
            &format!(
                "SELECT count(*) AS value FROM {} {joins}",
                table(&server.schema, "rows")
            ),
            false,
        )
        .await;
    let started = Instant::now();
    let namespace_probe = async {
        let deadline = Instant::now() + Duration::from_secs(4);
        loop {
            let held = server.admin.read("SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='relation' AND relation='pg_catalog.pg_namespace'::regclass AND mode='ShareLock' AND granted LIMIT 1").await.unwrap();
            if held.height() != 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "broker namespace lock not observed"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut rename = server.admin.driver().begin().await.unwrap();
        rename
            .exec("SET LOCAL lock_timeout = '200ms'")
            .await
            .unwrap();
        rename
            .exec("SET LOCAL statement_timeout = '500ms'")
            .await
            .unwrap();
        let result = rename
            .exec(&format!(
                "ALTER SCHEMA {} RENAME TO {}",
                quote(&server.schema),
                quote(&format!("{}_replacement", server.schema))
            ))
            .await;
        assert!(
            result.is_err(),
            "schema rename crossed broker namespace pin"
        );
        rename.rollback().await.unwrap();
    };
    let ((status, result), ()) = tokio::join!(
        server.fetch(&expensive, DataQuery::default()),
        namespace_probe,
    );
    assert!(matches!(status, 422 | 503), "{status}: {result}");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(
        server
            .source
            .read("SHOW statement_timeout")
            .await
            .unwrap()
            .row(0)[0]
            .py_str(),
        original_budget
    );
    assert_eq!(server.fetch(&base, DataQuery::default()).await.0, 200);
    let mut locker = server.admin.driver().begin().await.unwrap();
    locker
        .exec(&format!(
            "LOCK TABLE ONLY {} IN ACCESS EXCLUSIVE MODE",
            table(&server.schema, "rows")
        ))
        .await
        .unwrap();
    let started = Instant::now();
    let (status, result) = server.fetch(&base, DataQuery::default()).await;
    assert!(matches!(status, 422 | 503), "{status}: {result}");
    assert!(started.elapsed() < Duration::from_secs(5));
    locker.rollback().await.unwrap();
    assert_eq!(server.fetch(&base, DataQuery::default()).await.0, 200);
    let mut namespace_locker = server.admin.driver().begin().await.unwrap();
    namespace_locker
        .exec("LOCK TABLE ONLY pg_catalog.pg_namespace IN ROW EXCLUSIVE MODE")
        .await
        .unwrap();
    let started = Instant::now();
    let (status, result) = server.fetch(&base, DataQuery::default()).await;
    assert!(matches!(status, 501 | 503), "{status}: {result}");
    assert!(started.elapsed() < Duration::from_secs(5));
    namespace_locker.rollback().await.unwrap();
    assert_eq!(server.fetch(&base, DataQuery::default()).await.0, 200);
    server.cleanup().await;
}
