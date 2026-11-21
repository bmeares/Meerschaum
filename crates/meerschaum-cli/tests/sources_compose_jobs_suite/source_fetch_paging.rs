//! Real SQLite snapshot paging, callback rotation and native chunked sync acceptance.

#[path = "../source_fetch_paging/metadata_pause.rs"]
mod metadata_pause;

use std::sync::Arc;
use std::time::{Duration, Instant};

use meerschaum_cli::native::Session;
use meerschaum_cli::source_fetch_backend::NativeSqlSources;
use meerschaum_connector::{Connector, InstanceConnector};
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::source_fetch::{self, PageRequest, PageResponse, Request};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::team_scope::api_execution::{Alias, VERSION as ALIAS_VERSION};
use meerschaum_core::team_scope::write_private;
use meerschaum_core::users::User;
use meerschaum_core::{Config, Map, Pipe, Value};
use meerschaum_server::executor::callbacks::{self, Grant, Purpose};
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value as Json, json};
use uuid::Uuid;

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
    state: AppState,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Served {
    async fn new(timeout: Duration) -> Self {
        Self::with_metadata_pause(timeout, None).await
    }

    async fn with_metadata_pause(
        timeout: Duration,
        pause: Option<Arc<metadata_pause::Pause>>,
    ) -> Self {
        let parent = std::env::var_os("MRSM_ROOT_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        std::fs::create_dir_all(&parent).unwrap();
        let root = tempfile::Builder::new()
            .prefix("source-broker-")
            .tempdir_in(parent)
            .unwrap();
        let main_uri = format!("sqlite:///{}", root.path().join("main.db").display());
        // Two distinct logical connectors and physical pools share this fixture-owned DB.
        let source_uri = main_uri.clone();
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
        let sources = Arc::new(
            NativeSqlSources::new(session, config)
                .with_timeout(timeout)
                .unwrap(),
        );
        let backend: Arc<dyn meerschaum_server::backends::SourceFetchBackend> = match pause {
            Some(pause) => Arc::new(metadata_pause::Sources {
                inner: sources,
                pause,
            }),
            None => sources,
        };
        let state = AppState::builder(cfg, main.clone())
            .secret(b"synthetic-source-broker-signing-key-0123456789".to_vec())
            .source_fetch(backend)
            .build()
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app = meerschaum_server::app(state.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
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
            state,
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

    async fn request_page(&self, request: &PageRequest, bearer: &str) -> (u16, Json) {
        let response = self
            .client
            .post(format!(
                "{}{}",
                self.origin,
                source_fetch::page_path(&request.request.pipe)
            ))
            .bearer_auth(bearer)
            .json(request)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let bytes = response.bytes().await.unwrap();
        assert!(bytes.len() <= source_fetch::MAX_PAGE_BYTES);
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    async fn page(&self, request: &PageRequest, bearer: &str) -> PageResponse {
        let (status, result) = self.request_page(request, bearer).await;
        assert_eq!(status, 200, "{result}");
        let response: PageResponse = serde_json::from_value(result).unwrap();
        response.validate(request).unwrap();
        response
    }

    fn snapshots(&self) -> usize {
        let path = self._root.path().join("executors/source-snapshots");
        match std::fs::read_dir(path) {
            Ok(entries) => entries
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().starts_with("snapshot-"))
                .count(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => panic!("private snapshot directory: {error}"),
        }
    }

    async fn grant(&self) -> Grant {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {}", self.bearer).parse().unwrap(),
        );
        let caller = meerschaum_server::access::require_fresh(
            &self.state,
            &headers,
            &["sql:read", "pipes:read"],
        )
        .await
        .unwrap();
        let team = self.main.get_team("Data").await.unwrap().unwrap();
        callbacks::register(
            &self.state,
            &caller,
            Uuid::new_v4(),
            Some(Uuid::parse_str(team.id.as_deref().unwrap()).unwrap()),
            &["sql:read", "pipes:read"],
            Purpose::Command,
        )
        .await
        .unwrap()
    }

    async fn credential(&self, grant: &Grant, file: &std::path::Path) -> String {
        let credential = callbacks::credential(&self.state, grant.id, grant.launch_id)
            .await
            .unwrap();
        write_private(file, &serde_json::to_string(&credential).unwrap()).unwrap();
        credential.token
    }

    fn alias(&self, keys: &str, credential_file: &std::path::Path) -> APIConnector {
        APIConnector::from_execution_alias(&Alias {
            version: ALIAS_VERSION,
            logical_keys: keys.into(),
            callback_url: self.origin.clone(),
            credential_file: credential_file.into(),
        })
        .unwrap()
    }
}

fn request(pipe: &Pipe) -> PageRequest {
    PageRequest {
        version: source_fetch::PAGE_VERSION,
        request: Request::for_pipe("sql:source", pipe, &Default::default()).unwrap(),
        cursor: None,
        close: false,
    }
}

async fn seed_large(server: &Served, rows: usize) {
    server
        .source
        .driver()
        .exec("CREATE TABLE large_rows (id INTEGER PRIMARY KEY, value INTEGER)")
        .await
        .unwrap();
    server.source.driver().exec(&format!("WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < {rows}) INSERT INTO large_rows SELECT n, n * 10 FROM numbers")).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn more_than_100k_rows_are_one_frozen_snapshot_across_callback_rotation_and_same_db_writes() {
    const ROWS: usize = 100_321;
    let server = Served::new(Duration::from_secs(20)).await;
    seed_large(&server, ROWS).await;
    let pipe = server
        .pipe("large", "SELECT id, value FROM large_rows ORDER BY id")
        .await;
    let grant = server.grant().await;
    let file = server._root.path().join("credential.json");
    let first_token = server.credential(&grant, &file).await;
    let alias = server.alias("sql:source", &file);
    assert!(
        alias
            .fetch(&pipe, &Default::default())
            .await
            .unwrap_err()
            .to_string()
            .contains("complete"),
        "ordinary fetch remains complete-only at its 100k transport cap"
    );
    let mut request = request(&pipe);
    let mut response = server.page(&request, &first_token).await;
    assert!(!response.complete);
    assert_eq!(server.snapshots(), 1);
    let mut rows = response.data.rows.len();
    let mut expected_id = 1usize;
    let check = |response: &PageResponse, expected_id: &mut usize| {
        for row in &response.data.rows {
            assert_eq!(
                row.get("id").unwrap().py_str().parse::<usize>().unwrap(),
                *expected_id
            );
            assert_eq!(
                row.get("value").unwrap().py_str().parse::<usize>().unwrap(),
                *expected_id * 10
            );
            *expected_id += 1;
        }
    };
    check(&response, &mut expected_id);
    // These complete before requesting page 2, against the exact same physical SQLite DB.
    // Neither a source read transaction nor the global schema version may pin this stream.
    tokio::time::timeout(Duration::from_secs(3), server.source.driver().exec(
        "DELETE FROM large_rows WHERE id > 50000; CREATE TABLE unrelated_destination (id INTEGER)"))
        .await.expect("source transaction must be released before the first page").unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let rotated = server.credential(&grant, &file).await;
    assert_ne!(first_token, rotated);
    while !response.complete {
        request.cursor = response.cursor;
        response = server.page(&request, &rotated).await;
        rows += response.data.rows.len();
        check(&response, &mut expected_id);
    }
    assert_eq!(rows, ROWS);
    assert_eq!(expected_id, ROWS + 1);
    assert_eq!(
        server.snapshots(),
        0,
        "EOF releases every page file and quota"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_page_rechecks_canonical_definition_acl_query_position_and_source_dependencies() {
    let server = Served::new(Duration::from_secs(20)).await;
    seed_large(&server, 5000).await;
    let grant = server.grant().await;
    let file = server._root.path().join("credential.json");
    let token = server.credential(&grant, &file).await;
    for change in ["definition", "acl", "filter", "position", "close"] {
        let mut pipe = server
            .pipe(change, "SELECT id, value FROM large_rows ORDER BY id")
            .await;
        let mut request = request(&pipe);
        let first = server.page(&request, &token).await;
        request.cursor = first.cursor;
        assert_eq!(server.snapshots(), 1);
        match change {
            "definition" => {
                let mut params = pipe.raw_parameters().clone();
                params.insert(
                    "fetch".into(),
                    map(json!({"definition":"SELECT id, value FROM large_rows WHERE id < 4500"}))
                        .into(),
                );
                pipe.set_parameters(params);
                assert!(server.main.edit_pipe(&pipe, false).await.success);
                assert_eq!(server.request_page(&request, &token).await.0, 409);
            }
            "acl" => {
                let mut params = pipe.raw_parameters().clone();
                params.insert(
                    "access".into(),
                    map(json!({"owner":"bob","team":"Other","visibility":"private"})).into(),
                );
                pipe.set_parameters(params);
                assert!(server.main.edit_pipe(&pipe, false).await.success);
                assert_eq!(server.request_page(&request, &token).await.0, 404);
            }
            "filter" => {
                request.request.params = Some(map(json!({"id":1})));
                assert_eq!(server.request_page(&request, &token).await.0, 409);
            }
            "position" => {
                request.cursor.as_mut().unwrap().page += 1;
                assert_eq!(server.request_page(&request, &token).await.0, 409);
            }
            "close" => {
                request.close = true;
                assert_eq!(server.request_page(&request, &token).await.0, 200);
                request.close = false;
                assert_eq!(server.request_page(&request, &token).await.0, 410);
            }
            _ => unreachable!(),
        }
        assert_eq!(
            server.snapshots(),
            0,
            "{change} releases snapshot files immediately"
        );
    }
    let mut dependency = Pipe::builder("plugin:catalog", "dependency").instance("sql:main")
        .parameters(map(json!({"target":"large_rows","access":{"owner":"alice","team":"Data","visibility":"team"}})))
        .build().unwrap();
    assert!(server.main.register_pipe(&dependency).await.success);
    let pipe = server
        .pipe(
            "dependency",
            "SELECT id, value FROM {{ Pipe('plugin:catalog', 'dependency') }} ORDER BY id",
        )
        .await;
    let mut request = request(&pipe);
    request.cursor = server.page(&request, &token).await.cursor;
    dependency.set_parameters(map(json!({"target":"large_rows","access":{"owner":"bob","team":"Other","visibility":"private"}})));
    assert!(server.main.edit_pipe(&dependency, false).await.success);
    assert_eq!(server.request_page(&request, &token).await.0, 404);
    assert_eq!(server.snapshots(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_native_chunk_stream_closes_its_snapshot_and_failed_build_reuses_the_pool() {
    let server = Served::new(Duration::from_secs(2)).await;
    seed_large(&server, 5000).await;
    let pipe = server
        .pipe("abandoned", "SELECT id, value FROM large_rows ORDER BY id")
        .await;
    let grant = server.grant().await;
    let file = server._root.path().join("credential.json");
    server.credential(&grant, &file).await;
    let alias = server.alias("sql:source", &file);
    let mut chunks = alias
        .fetch_chunked(&pipe, &Default::default(), true)
        .await
        .unwrap()
        .unwrap()
        .chunks;
    let first = chunks.next_chunk().await.unwrap().unwrap();
    let meerschaum_kernel::enforce::PipeInput::Frame(frame) = first else {
        panic!("native frame page expected")
    };
    assert_eq!(frame.height(), source_fetch::PAGE_ROWS);
    assert_eq!(server.snapshots(), 1);
    drop(chunks);
    let deadline = Instant::now() + Duration::from_secs(3);
    while server.snapshots() != 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        server.snapshots(),
        0,
        "dropping native stream releases its private spool"
    );
    let joins = (0..24)
        .map(|index| format!("CROSS JOIN public_rows p{index}"))
        .collect::<Vec<_>>()
        .join(" ");
    let expensive = server
        .pipe(
            "expensive",
            &format!("SELECT count(*) AS n FROM public_rows {joins}"),
        )
        .await;
    let started = Instant::now();
    let (status, result) = server
        .request_page(&request(&expensive), &server.bearer)
        .await;
    assert!(matches!(status, 422 | 503), "{status}: {result}");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(
        server.snapshots(),
        0,
        "failed construction removes partial files"
    );
    let frame = server.source.read("SELECT 1 AS value").await.unwrap();
    assert_eq!(
        frame.height(),
        1,
        "VM handler and transaction are cleared before pool reuse"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_native_sync_consumes_large_pages_on_the_same_sqlite_db_and_refuses_replace_before_registration()
 {
    const ROWS: usize = 100_321;
    let server = Served::new(Duration::from_secs(20)).await;
    seed_large(&server, ROWS).await;
    let pipe = server
        .pipe("native", "SELECT id, value FROM large_rows ORDER BY id")
        .await;
    let grant = server.grant().await;
    let file = server._root.path().join("credential.json");
    server.credential(&grant, &file).await;
    let source = Arc::new(server.alias("sql:source", &file));
    let target = Arc::new(server.alias("sql:main", &file));
    let registry = Arc::new(
        meerschaum_engine::StaticRegistry::new()
            .with_source("sql:source", source)
            .with_instance("sql:main", target),
    );
    let engine = meerschaum_engine::Engine::new(
        registry,
        meerschaum_engine::EngineConfig {
            home_instance: "sql:main".into(),
            lease_dir: server._root.path().join("leases"),
            ..Default::default()
        },
    );
    // Keep the actual callback provider rotating throughout the native sync.
    let state = server.state.clone();
    let rotating_file = file.clone();
    let rotating_grant = grant.clone();
    let rotation = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let credential =
                callbacks::credential(&state, rotating_grant.id, rotating_grant.launch_id)
                    .await
                    .unwrap();
            write_private(&rotating_file, &serde_json::to_string(&credential).unwrap()).unwrap();
        }
    });
    let result = tokio::time::timeout(
        Duration::from_secs(120),
        engine.sync_pipe(
            &pipe,
            None,
            &meerschaum_engine::SyncArgs {
                begin: meerschaum_engine::SyncBegin::Unbounded,
                ..Default::default()
            },
        ),
    )
    .await;
    rotation.abort();
    let result = result.expect("large native sync must finish");
    assert!(result.success, "{}", result.message);
    let count = server
        .main
        .get_pipe_rowcount(&pipe, &Default::default())
        .await
        .unwrap();
    assert_eq!(count, Some(ROWS as i64));
    assert_eq!(server.snapshots(), 0);
    let missing = Pipe::builder("sql:source", "replace_unregistered")
        .instance("sql:main")
        .parameters(map(
            json!({"fetch":{"definition":"SELECT id, value FROM large_rows"}}),
        ))
        .build()
        .unwrap();
    assert!(server.main.get_pipe_id(&missing).await.unwrap().is_none());
    let result = engine
        .sync_pipe(
            &missing,
            None,
            &meerschaum_engine::SyncArgs {
                replace: true,
                ..Default::default()
            },
        )
        .await;
    assert!(!result.success);
    assert!(
        result.message.contains("registered destination"),
        "{}",
        result.message
    );
    assert!(
        server.main.get_pipe_id(&missing).await.unwrap().is_none(),
        "replacement refusal precedes destination registration"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_complete_fetch_bounds_aggregate_copied_column_keys_before_retaining_all_rows() {
    let server = Served::new(Duration::from_secs(20)).await;
    seed_large(&server, 2000).await;
    // Small integer data on disk, but a large complete JSON result due to copied column
    // keys. Counting only value bytes or native column buffers misses this bound.
    let columns = (0..128)
        .map(|index| {
            let name = format!("c{index:03}_{}", "column_key_".repeat(46));
            format!("id AS \"{name}\"")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let pipe = server
        .pipe("wide_keys", &format!("SELECT {columns} FROM large_rows"))
        .await;
    let request = Request::for_pipe("sql:source", &pipe, &Default::default()).unwrap();
    let response = server
        .client
        .post(format!(
            "{}{}",
            server.origin,
            source_fetch::path(&request.pipe)
        ))
        .bearer_auth(&server.bearer)
        .json(&request)
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let result: Json = response.json().await.unwrap();
    assert_eq!(status, 413, "{result}");
    assert!(
        result.get("rows").is_none(),
        "an over-budget complete fetch never becomes partial success"
    );
    assert_eq!(server.snapshots(), 0);
    let good = server
        .pipe("after_aggregate_bound", "SELECT id, value FROM public_rows")
        .await;
    let request = Request::for_pipe("sql:source", &good, &Default::default()).unwrap();
    let response = server
        .client
        .post(format!(
            "{}{}",
            server.origin,
            source_fetch::path(&request.pipe)
        ))
        .bearer_auth(&server.bearer)
        .json(&request)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let result: Json = response.json().await.unwrap();
    assert_eq!(result["rows"].as_array().unwrap().len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn atomic_source_replacement_materializes_large_rows_and_replays_a_freshly_authorized_receipt_once()
 {
    use meerschaum_connector_api::source_replace::{self, Phase};
    const ROWS: usize = 100_321;
    let server = Served::new(Duration::from_secs(20)).await;
    seed_large(&server, ROWS).await;
    let pipe = server
        .pipe(
            "atomic_large",
            "SELECT id, value, CAST('x' AS BLOB) AS payload FROM large_rows ORDER BY id",
        )
        .await;
    let old = meerschaum_kernel::docs::frame_from_docs(
        &[(-1_i64, -10_i64), (0, 0)]
            .into_iter()
            .map(|(id, value)| {
                [
                    ("id".into(), meerschaum_kernel::value::Value::Int(id)),
                    ("value".into(), meerschaum_kernel::value::Value::Int(value)),
                ]
                .into_iter()
                .collect()
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    assert!(
        server
            .main
            .sync_pipe(&pipe, old, &Default::default())
            .await
            .success
    );
    let grant = server.grant().await;
    let file = server._root.path().join("atomic-credential.json");
    let mut token = server.credential(&grant, &file).await;
    let mut request = source_replace::Request {
        version: source_replace::VERSION,
        source: Request::for_pipe("sql:source", &pipe, &Default::default()).unwrap(),
        phase: Phase::Prepare,
        id: None,
        enforce_dtypes: true,
    };
    let send = |request: source_replace::Request, token: String| {
        let client = server.client.clone();
        let origin = server.origin.clone();
        async move {
            let phase = request.phase;
            let started = Instant::now();
            eprintln!("Atomic HTTP phase {phase:?} started.");
            let response = client
                .post(format!(
                    "{}{}",
                    origin,
                    source_replace::path(&request.source.pipe)
                ))
                .bearer_auth(token)
                .json(&request)
                .send()
                .await
                .unwrap();
            let status = response.status().as_u16();
            eprintln!(
                "Atomic HTTP phase {phase:?} finished after {:?} (status {status}).",
                started.elapsed()
            );
            let result: Json = response.json().await.unwrap();
            assert_eq!(status, 200, "{result}");
            serde_json::from_value::<source_replace::Response>(result).unwrap()
        }
    };
    let prepared = send(request.clone(), token).await;
    let source_replace::Response::Prepared { id, summary } = prepared else {
        panic!("prepared replacement expected")
    };
    assert_eq!(summary.rows, ROWS as u64);
    assert_eq!(
        server
            .main
            .get_pipe_rowcount(&pipe, &Default::default())
            .await
            .unwrap(),
        Some(2),
        "prepare changes no destination rows or metadata"
    );
    request.phase = Phase::Commit;
    request.id = Some(id);
    token = server.credential(&grant, &file).await;
    let committed = tokio::time::timeout(Duration::from_secs(120), send(request.clone(), token))
        .await
        .expect("atomic large commit must finish");
    let source_replace::Response::Committed { result } = committed else {
        panic!("committed replacement expected")
    };
    assert!(result.success);
    assert_eq!((result.deleted, result.inserted), (2, ROWS as u64));
    assert_eq!(
        server.snapshots(),
        0,
        "a completion receipt retains no source pages"
    );
    let target = server
        .main
        .pipe_table_name(&server.main.sql_pipe(&pipe).unwrap());
    server
        .main
        .driver()
        .exec(&format!(
            "INSERT INTO {target} (id, value) VALUES (200000, 42)"
        ))
        .await
        .unwrap();
    token = server.credential(&grant, &file).await;
    let replay = send(request, token).await;
    let source_replace::Response::Committed { result: replay } = replay else {
        panic!("committed receipt expected")
    };
    assert_eq!(
        (replay.deleted, replay.inserted),
        (result.deleted, result.inserted)
    );
    assert_eq!(
        server
            .main
            .get_pipe_rowcount(&pipe, &Default::default())
            .await
            .unwrap(),
        Some(ROWS as i64 + 1),
        "replay never reruns the window DELETE"
    );
    assert_eq!(server.snapshots(), 0);

    // Exercise selection by the real native engine too, rather than only the HTTP DTO.
    server.credential(&grant, &file).await;
    let registry = Arc::new(
        meerschaum_engine::StaticRegistry::new()
            .with_source("sql:source", Arc::new(server.alias("sql:source", &file)))
            .with_instance("sql:main", Arc::new(server.alias("sql:main", &file))),
    );
    let engine = meerschaum_engine::Engine::new(
        registry,
        meerschaum_engine::EngineConfig {
            home_instance: "sql:main".into(),
            lease_dir: server._root.path().join("replacement-engine-leases"),
            ..Default::default()
        },
    );
    let result = tokio::time::timeout(
        Duration::from_secs(120),
        engine.sync_pipe(
            &pipe,
            None,
            &meerschaum_engine::SyncArgs {
                begin: meerschaum_engine::SyncBegin::Unbounded,
                replace: true,
                ..Default::default()
            },
        ),
    )
    .await
    .expect("native atomic replacement must finish");
    assert!(result.success, "{}", result.message);
    assert_eq!(
        (result.deleted, result.inserted),
        (ROWS as u64 + 1, ROWS as u64)
    );
    assert_eq!(
        server
            .main
            .get_pipe_rowcount(&pipe, &Default::default())
            .await
            .unwrap(),
        Some(ROWS as i64)
    );
    assert_eq!(
        server.snapshots(),
        0,
        "native selection retains no source page files"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_scope_revoked_during_final_metadata_read_refuses_rows_and_reuses_native_pool() {
    let pause = Arc::new(metadata_pause::Pause::default());
    let server = Served::with_metadata_pause(Duration::from_secs(20), Some(pause.clone())).await;
    let pipe = server
        .pipe(
            "last_metadata",
            "SELECT id, value FROM public_rows ORDER BY id",
        )
        .await;
    let mut alice = User::new("alice", None);
    let original = server.main.get_user_attributes(&alice).await.unwrap();
    alice.attributes = original.clone();
    pause.final_page_metadata_read();
    let request = request(&pipe);
    let pending = server.request_page(&request, &server.bearer);
    let revoke = async {
        tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
            .await
            .expect("the actual final native metadata read must be held");
        alice.attributes = Some(map(json!({"role":"custom", "scopes":["pipes:read"]})));
        let result = server.main.edit_user(&alice).await;
        assert!(result.success, "{}", result.message);
        pause.release.notify_one();
    };
    let ((status, result), ()) = tokio::join!(pending, revoke);
    assert_eq!(status, 403, "{result}");
    assert!(
        result.get("data").is_none(),
        "revoked source authority returns no rows"
    );
    assert_eq!(server.snapshots(), 0, "refusal creates no private snapshot");
    alice.attributes = original;
    assert!(server.main.edit_user(&alice).await.success);
    let response = server.page(&request, &server.bearer).await;
    assert_eq!(
        response.data.rows.len(),
        3,
        "fresh source authority reuses the actual pool"
    );
}
