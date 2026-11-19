//! Real GeoPackage feature sources through native read-only pools and authenticated HTTP.
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use meerschaum_cli::native::Session;
use meerschaum_cli::source_fetch_backend::NativeSqlSources;
use meerschaum_connector::InstanceConnector;
use meerschaum_connector_api::source_fetch::{self, PageRequest, PageResponse, Request};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::users::User;
use meerschaum_core::{Config, Map, Pipe, Value};
use meerschaum_kernel::Geometry;
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_kernel::value::Value as KValue;
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value as Json, json};

fn map(value: Json) -> Map {
    serde_json::from_value::<Value>(value).unwrap().to_map()
}
struct Instances(Arc<SqlConnector>);
impl meerschaum_server::backends::InstanceResolver for Instances {
    fn resolve(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        (keys == "sql:source").then(|| self.0.clone() as Arc<dyn InstanceConnector>)
    }
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
    async fn new() -> Self {
        let parent = std::env::var_os("MRSM_ROOT_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        std::fs::create_dir_all(&parent).unwrap();
        let root = tempfile::Builder::new()
            .prefix("source-gpkg-")
            .tempdir_in(parent)
            .unwrap();
        let main_uri = format!("sqlite:///{}", root.path().join("main.db").display());
        let source_uri = format!(
            "geopackage:///{}",
            root.path().join("source.gpkg").display()
        );
        let main = Arc::new(SqlConnector::from_uri("main", &main_uri).await.unwrap());
        let source = Arc::new(SqlConnector::from_uri("source", &source_uri).await.unwrap());
        for username in ["alice", "bob"] {
            let mut user = User::new(username, Some("synthetic-pass"));
            user.attributes = Some(map(json!({"role":"editor"})));
            assert!(main.register_user(&mut user).await.success);
        }
        for (name, username) in [("Data", "alice"), ("Other", "bob")] {
            let mut team = Team::new(name);
            team.add_member(username, TeamRole::Member);
            assert!(main.register_team(&mut team).await.success);
        }
        let features = Pipe::builder("manual:geo", "features")
            .instance("sql:source")
            .parameters(map(json!({"target":"feature_rows", "columns":{"id":"id"},
            "dtypes":{"geometry":"geometry[Point, 4326]"}, "indices":{"geometry":"geometry"},
            "access":{"owner":"alice","team":"Data","visibility":"team"}})))
            .build()
            .unwrap();
        assert!(source.register_pipe(&features).await.success);
        let docs = [
            (1, Some("POINT (1 2)")),
            (2, Some("POINT (5 8)")),
            (3, None),
        ]
        .into_iter()
        .map(|(id, geometry)| {
            [
                ("id".into(), KValue::Int(id)),
                (
                    "geometry".into(),
                    geometry
                        .map(|wkt| KValue::Geometry(Geometry::from_wkt(wkt).unwrap()))
                        .unwrap_or(KValue::Null),
                ),
            ]
            .into_iter()
            .collect()
        })
        .collect();
        let result = source
            .sync(&features, PipeInput::Docs(docs), &Default::default())
            .await;
        assert!(result.success, "{}", result.message);
        let private = Pipe::builder("manual:geo", "private_features")
            .instance("sql:source")
            .parameters(map(
                json!({"target":"private_features", "columns":{"id":"id"},
            "dtypes":{"geometry":"geometry[Point, 4326]"}, "indices":{"geometry":"geometry"},
            "access":{"owner":"bob","team":"Other","visibility":"private"}}),
            ))
            .build()
            .unwrap();
        assert!(source.register_pipe(&private).await.success);
        let docs = vec![
            [
                ("id".into(), KValue::Int(1)),
                (
                    "geometry".into(),
                    KValue::Geometry(Geometry::from_wkt("POINT (99 100)").unwrap()),
                ),
            ]
            .into_iter()
            .collect(),
        ];
        let result = source
            .sync(&private, PipeInput::Docs(docs), &Default::default())
            .await;
        assert!(result.success, "{}", result.message);
        source.driver().exec("CREATE VIEW feature_view AS SELECT * FROM feature_rows; CREATE VIEW private_view AS SELECT * FROM private_features").await.unwrap();
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
        cfg.registration_pipes = true;
        cfg.allowed_instance_keys = vec!["sql:main".into(), "sql:source".into()];
        cfg.connectors = map(json!({"sql":{"main":{},"source":{"teams":["Data"]}}}));
        let state = AppState::builder(cfg, main.clone())
            .resolver(Arc::new(Instances(source.clone())))
            .secret(b"synthetic-gpkg-source-signing-key-0123456789".to_vec())
            .source_fetch(Arc::new(NativeSqlSources::new(session, config)))
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
        assert!(self.main.register_pipe(&pipe).await.success);
        pipe
    }
    async fn fetch(&self, pipe: &Pipe) -> (u16, Json) {
        let request = Request::for_pipe("sql:source", pipe, &Default::default()).unwrap();
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
        let status = response.status().as_u16();
        let bytes = response.bytes().await.unwrap();
        assert!(bytes.len() <= source_fetch::MAX_RESPONSE_BYTES);
        (status, serde_json::from_slice(&bytes).unwrap())
    }
    async fn page(&self, request: &PageRequest) -> (u16, Json) {
        let response = self
            .client
            .post(format!(
                "{}{}",
                self.origin,
                source_fetch::page_path(&request.request.pipe)
            ))
            .bearer_auth(&self.bearer)
            .json(request)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let bytes = response.bytes().await.unwrap();
        assert!(bytes.len() <= source_fetch::MAX_PAGE_BYTES);
        (status, serde_json::from_slice(&bytes).unwrap())
    }
    fn snapshots(&self) -> usize {
        std::fs::read_dir(self._root.path().join("executors/source-snapshots"))
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|entry| entry.file_name().to_string_lossy().starts_with("snapshot-"))
                    .count()
            })
            .unwrap_or(0)
    }
}
fn page_request(pipe: &Pipe) -> PageRequest {
    PageRequest {
        version: source_fetch::PAGE_VERSION,
        request: Request::for_pipe("sql:source", pipe, &Default::default()).unwrap(),
        cursor: None,
        close: false,
    }
}
fn geometry(value: &Json) -> Geometry {
    let blob = base64::engine::general_purpose::STANDARD
        .decode(value.as_str().unwrap())
        .unwrap();
    assert!(blob.starts_with(b"GP"));
    let (wkb, _, _) = meerschaum_kernel::geometry::gpkg_wkb_to_wkb(&blob).unwrap();
    Geometry::from_wkb(&wkb).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_gpkg_features_and_all_spatial_scalars_work_on_the_bounded_source_pool() {
    let server = Served::new().await;
    let pipe = server
        .pipe(
            "features",
            "SELECT id, geometry FROM feature_rows ORDER BY id",
        )
        .await;
    let (status, response) = server.fetch(&pipe).await;
    assert_eq!(status, 200, "{response}");
    let result: source_fetch::Response = serde_json::from_value(response.clone()).unwrap();
    assert!(result.complete);
    assert_eq!(result.rows.len(), 3);
    assert_eq!(
        geometry(&response["rows"][0]["geometry"]).to_wkt(),
        "POINT (1 2)"
    );
    assert_eq!(
        geometry(&response["rows"][1]["geometry"]).to_wkt(),
        "POINT (5 8)"
    );
    assert!(response["rows"][2]["geometry"].is_null());
    let replacement = meerschaum_connector_api::source_replace::Request {
        version: meerschaum_connector_api::source_replace::VERSION,
        source: Request::for_pipe("sql:source", &pipe, &Default::default()).unwrap(),
        phase: meerschaum_connector_api::source_replace::Phase::Prepare,
        id: None,
        enforce_dtypes: true,
    };
    let response = server
        .client
        .post(format!(
            "{}{}",
            server.origin,
            meerschaum_connector_api::source_replace::path(&replacement.source.pipe)
        ))
        .bearer_auth(&server.bearer)
        .json(&replacement)
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let result: Json = response.json().await.unwrap();
    assert_eq!(
        status, 503,
        "atomic GeoPackage replacement remains explicitly unsupported: {result}"
    );
    assert!(!server.main.pipe_exists(&pipe).await.unwrap());
    let mut transaction = server
        .source
        .driver()
        .begin_source_snapshot()
        .await
        .unwrap();
    let frame = transaction.read_frame("SELECT ST_MinX(geometry) AS minx, ST_MaxX(geometry) AS maxx, ST_MinY(geometry) AS miny, ST_MaxY(geometry) AS maxy, ST_IsEmpty(geometry) AS empty FROM feature_rows WHERE id = 1").await.unwrap();
    for (column, expected) in [
        ("minx", 1.),
        ("maxx", 1.),
        ("miny", 2.),
        ("maxy", 2.),
        ("empty", 0.),
    ] {
        assert_eq!(
            frame
                .column(column)
                .unwrap()
                .data
                .get(0)
                .py_str()
                .parse::<f64>()
                .unwrap(),
            expected
        );
    }
    transaction.rollback().await.unwrap();
    let application_id = server
        .source
        .driver()
        .value("PRAGMA application_id")
        .await
        .unwrap()
        .unwrap()
        .as_i64();
    assert_eq!(
        application_id,
        Some(meerschaum_connector_sql::instance::geopackage::GPKG_APPLICATION_ID)
    );
    let rtree = server
        .source
        .driver()
        .value("SELECT count(*) FROM rtree_feature_rows_geometry")
        .await
        .unwrap()
        .unwrap()
        .as_i64();
    assert_eq!(rtree, Some(2));
    assert_eq!(server.snapshots(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gpkg_paging_freezes_feature_rows_then_releases_the_source_before_rtree_writes() {
    const ROWS: usize = 5001;
    let server = Served::new().await;
    server.source.driver().exec(&format!("WITH RECURSIVE numbers(n) AS (SELECT 4 UNION ALL SELECT n + 1 FROM numbers WHERE n < {ROWS}) INSERT INTO feature_rows (id, geometry) SELECT n, (SELECT geometry FROM feature_rows WHERE id = 1) FROM numbers")).await.unwrap();
    let pipe = server
        .pipe("paged", "SELECT id, geometry FROM feature_rows ORDER BY id")
        .await;
    let mut request = page_request(&pipe);
    let (status, result) = server.page(&request).await;
    assert_eq!(status, 200, "{result}");
    let mut response: PageResponse = serde_json::from_value(result).unwrap();
    assert!(!response.complete);
    assert_eq!(server.snapshots(), 1);
    let mut seen = response.data.rows.len();
    tokio::time::timeout(
        Duration::from_secs(3),
        server.source.driver().exec(
            "DELETE FROM feature_rows WHERE id > 2000; CREATE TABLE unrelated_rows (id INTEGER)",
        ),
    )
    .await
    .expect("the source transaction must be released before destination/spatial-index writes")
    .unwrap();
    while !response.complete {
        request.cursor = response.cursor.clone();
        let (status, result) = server.page(&request).await;
        assert_eq!(status, 200, "{result}");
        response = serde_json::from_value(result).unwrap();
        response.validate(&request).unwrap();
        seen += response.data.rows.len();
    }
    assert_eq!(seen, ROWS);
    assert_eq!(
        response.data.rows.last().unwrap()["id"].as_i64(),
        Some(ROWS as i64)
    );
    assert_eq!(server.snapshots(), 0);
    assert_eq!(
        server.fetch(&pipe).await.1["rows"]
            .as_array()
            .unwrap()
            .len(),
        2000
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gpkg_hidden_layers_views_spatial_indices_and_private_catalog_metadata_are_refused() {
    let server = Served::new().await;
    for (index, definition) in [
        "SELECT * FROM private_features",
        "SELECT * FROM feature_view",
        "SELECT * FROM private_view",
        "SELECT * FROM rtree_feature_rows_geometry",
        "SELECT * FROM rtree_feature_rows_geometry_node",
        "SELECT * FROM gpkg_contents",
        "SELECT * FROM gpkg_geometry_columns",
        "SELECT * FROM gpkg_extensions",
        "SELECT * FROM gpkg_spatial_ref_sys",
        "SELECT * FROM gpkg_ogr_contents",
    ]
    .into_iter()
    .enumerate()
    {
        let pipe = server.pipe(&format!("refused_{index}"), definition).await;
        let (status, result) = server.fetch(&pipe).await;
        assert_eq!(status, 403, "{definition}: {result}");
        assert!(result.get("rows").is_none());
        let (status, result) = server.page(&page_request(&pipe)).await;
        assert_eq!(status, 403, "paged {definition}: {result}");
        assert!(result.get("data").is_none());
        assert_eq!(server.snapshots(), 0);
    }
    let valid = server
        .pipe("still_allowed", "SELECT id FROM feature_rows ORDER BY id")
        .await;
    assert_eq!(server.fetch(&valid).await.0, 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gpkg_native_row_and_complete_response_byte_caps_fail_bounded_and_keep_pools_usable() {
    let server = Served::new().await;
    server.source.driver().exec("CREATE TABLE wide_rows (payload BLOB); INSERT INTO wide_rows VALUES (zeroblob(10485760)); CREATE TABLE wide_ids (id INTEGER PRIMARY KEY); WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < 1000) INSERT INTO wide_ids SELECT n FROM numbers").await.unwrap();
    let huge = server
        .pipe("native_row", "SELECT payload FROM wide_rows")
        .await;
    for (status, result) in [
        server.fetch(&huge).await,
        server.page(&page_request(&huge)).await,
    ] {
        assert!(matches!(status, 413 | 422), "{status}: {result}");
        assert!(result.get("rows").is_none() && result.get("data").is_none());
        assert_eq!(server.snapshots(), 0);
    }
    let ordinary = server
        .source
        .driver()
        .read_frame("SELECT payload FROM wide_rows")
        .await
        .unwrap();
    let KValue::Bytes(blob) = ordinary.column("payload").unwrap().data.get(0) else {
        panic!("ordinary GeoPackage pool must still return the complete BLOB");
    };
    assert_eq!(
        blob.len(),
        10 * 1024 * 1024,
        "the source-only native cap does not alter the ordinary pool"
    );
    drop(blob);
    drop(ordinary);
    let columns = (0..64)
        .map(|index| format!("id AS \"column_{index}_{}\"", "x".repeat(1200)))
        .collect::<Vec<_>>()
        .join(", ");
    let wide = server
        .pipe("response_bytes", &format!("SELECT {columns} FROM wide_ids"))
        .await;
    let (status, result) = server.fetch(&wide).await;
    assert_eq!(status, 413, "{result}");
    assert!(result.get("rows").is_none());
    let valid = server
        .pipe(
            "after_byte_refusal",
            "SELECT id, geometry FROM feature_rows ORDER BY id",
        )
        .await;
    assert_eq!(server.fetch(&valid).await.0, 200);
    assert_eq!(server.page(&page_request(&valid)).await.0, 200);
    assert_eq!(server.snapshots(), 0);
}

fn frame_rows(frame: &meerschaum_kernel::Frame) -> Vec<Vec<KValue>> {
    (0..frame.height()).map(|index| frame.row(index)).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gpkg_registration_and_legacy_data_targets_cannot_mutate_catalogs_or_spatial_storage() {
    let server = Served::new().await;
    for (index, target) in [
        "gpkg_contents",
        "GPkg_Geometry_Columns",
        "main.\"gpkg_extensions\"",
        "\"gpkg_contents\"",
        "rtree_feature_rows_geometry",
        "main.\"RTREE_feature_rows_geometry_node\"",
    ]
    .into_iter()
    .enumerate()
    {
        let internal = Pipe::builder("mrsm", "reserved_target_probe")
            .instance("sql:source")
            .parameters(map(json!({"target":target})))
            .build()
            .unwrap();
        {
            let _allow = meerschaum_core::protected::allow(true);
            assert!(
                meerschaum_connector::protected_refusal(server.source.as_ref(), &internal, "sync")
                    .is_some(),
                "reserved targets remain protected for internal-key pipes and allow-protected: {target}"
            );
            assert!(
                meerschaum_connector::protected_refusal(server.main.as_ref(), &internal, "sync")
                    .is_none(),
                "ordinary SQLite does not reserve this namespace: {target}"
            );
        }
        let metadata_before = server
            .source
            .driver()
            .read_frame("SELECT * FROM gpkg_contents ORDER BY table_name")
            .await
            .unwrap();
        let rtree_before = server
            .source
            .driver()
            .read_frame("SELECT * FROM rtree_feature_rows_geometry_node ORDER BY nodeno")
            .await
            .unwrap();
        let metric = format!("forged_target_{index}");
        let route = format!("{}/pipes/manual:geo/{metric}/None", server.origin);
        let registered = server
            .client
            .post(format!("{route}/register?instance_keys=sql:source"))
            .bearer_auth(&server.bearer)
            .json(&json!({"target":target}))
            .send()
            .await
            .unwrap();
        let status = registered.status().as_u16();
        let result: Json = registered.json().await.unwrap();
        assert_eq!(status, 403, "register {target}: {result}");
        let pipe = Pipe::builder("manual:geo", &metric).instance("sql:source")
            .parameters(map(json!({"target":"ordinary_target", "access":{"owner":"alice","team":"Data","visibility":"team"}})))
            .build().unwrap();
        assert!(
            server.source.get_pipe_id(&pipe).await.unwrap().is_none(),
            "refusal registers nothing"
        );
        // Simulate an old registration created before reserved-target enforcement. Neither
        // migration history nor a caller's metadata/write permission grants catalog access.
        assert!(server.source.register_pipe(&pipe).await.success);
        let mut parameters = pipe.raw_parameters().clone();
        parameters.insert("target".into(), Value::Str(target.into()));
        server
            .source
            .driver()
            .raw_execute(
                "UPDATE mrsm_pipes SET parameters = ? WHERE connector_keys = ? AND metric_key = ?",
                &[
                    meerschaum_connector_sql::driver::SqlParam::Text(
                        Value::Map(parameters).to_json_string(),
                    ),
                    meerschaum_connector_sql::driver::SqlParam::Text("manual:geo".into()),
                    meerschaum_connector_sql::driver::SqlParam::Text(metric.clone()),
                ],
            )
            .await
            .unwrap();
        let written = server
            .client
            .post(format!("{route}/data?instance_keys=sql:source"))
            .bearer_auth(&server.bearer)
            .json(&json!([{"id":1,"table_name":"private_features","min_x":0}]))
            .send()
            .await
            .unwrap();
        let status = written.status().as_u16();
        let result: Json = written.json().await.unwrap();
        assert_eq!(status, 403, "data {target}: {result}");
        assert_eq!(
            frame_rows(
                &server
                    .source
                    .driver()
                    .read_frame("SELECT * FROM gpkg_contents ORDER BY table_name")
                    .await
                    .unwrap()
            ),
            frame_rows(&metadata_before)
        );
        assert_eq!(
            frame_rows(
                &server
                    .source
                    .driver()
                    .read_frame("SELECT * FROM rtree_feature_rows_geometry_node ORDER BY nodeno")
                    .await
                    .unwrap()
            ),
            frame_rows(&rtree_before)
        );
    }
    // The restriction belongs to actual GeoPackage instances; these names remain ordinary
    // data tables on SQLite, with the same authenticated user/registration/data APIs.
    let route = format!("{}/pipes/manual:geo/sqlite_namespace/None", server.origin);
    let registered = server
        .client
        .post(format!("{route}/register?instance_keys=sql:main"))
        .bearer_auth(&server.bearer)
        .json(&json!({"target":"gpkg_user_rows"}))
        .send()
        .await
        .unwrap();
    let status = registered.status().as_u16();
    let result: Json = registered.json().await.unwrap();
    assert_eq!(status, 200, "SQLite registration: {result}");
    assert_eq!(result[0], true);
    let written = server
        .client
        .post(format!("{route}/data?instance_keys=sql:main"))
        .bearer_auth(&server.bearer)
        .json(&json!([{"id":1}]))
        .send()
        .await
        .unwrap();
    let status = written.status().as_u16();
    let result: Json = written.json().await.unwrap();
    assert_eq!(status, 200, "SQLite data: {result}");
    assert_eq!(result[0], true);
    assert_eq!(
        server
            .main
            .driver()
            .value("SELECT count(*) FROM gpkg_user_rows")
            .await
            .unwrap()
            .unwrap()
            .as_i64(),
        Some(1)
    );
}
