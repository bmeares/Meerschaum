//! The API server over a real SQL instance (SQLite always; the PostgreSQL family and
//! TimescaleDB when their `MRSM_TEST_*_URI` is set), driven by `meerschaum-connector-api`'s
//! client over a loopback socket: what `Pipe.sync()` persists on the server
//! (`_persist_new_special_columns`, `register`, `validate_indices`, the compression policy) and
//! `POST /metadata`.

use std::net::SocketAddr;
use std::sync::Arc;

use indexmap::IndexMap;
use meerschaum_connector::pipe_ops::{self, Doc};
use meerschaum_connector::{InstanceConnector, SyncOptions};
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::{PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::value::Value as KValue;
use meerschaum_server::{AppState, ServerConfig};

const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef0123456789abcdef";

struct Served {
    conn: Arc<SqlConnector>,
    client: APIConnector,
    port: u16,
    _dir: tempfile::TempDir,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn unique(tag: &str) -> String {
    format!(
        "w17bs_{tag}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    )
}

/// Serve `conn` (with `--no-auth`) on a random loopback port and build a client for it.
async fn serve(conn: Arc<SqlConnector>) -> Served {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerConfig::for_root(&conn.keys(), dir.path());
    cfg.no_auth = true;
    cfg.no_webterm = true;
    cfg.no_dash = true;
    let instance: Arc<dyn InstanceConnector> = conn.clone();
    let state = AppState::builder(cfg, instance)
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = meerschaum_server::app(state);
    let task = tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await;
    });
    let mut attrs = Map::new();
    attrs.insert("host".into(), Value::Str("127.0.0.1".into()));
    attrs.insert("port".into(), Value::Int(port as i64));
    let client = APIConnector::new(Some("w17bs"), attrs).unwrap();
    Served {
        conn,
        client,
        port,
        _dir: dir,
        task,
    }
}

async fn sqlite() -> (Served, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite:///{}", dir.path().join("w17bs.db").display());
    let conn = Arc::new(SqlConnector::from_uri("w17bs", &uri).await.unwrap());
    (serve(conn).await, dir)
}

/// The server-flavored instances the environment provides.
async fn server_flavors() -> Vec<(&'static str, Arc<SqlConnector>)> {
    let mut out = Vec::new();
    for (var, label) in [
        ("MRSM_TEST_PG_URI", "w17pg"),
        ("MRSM_TEST_POSTGIS_URI", "w17postgis"),
        ("MRSM_TEST_TIMESCALE_URI", "w17ts"),
    ] {
        if let Ok(uri) = std::env::var(var) {
            let conn = SqlConnector::from_uri(label, &uri).await.unwrap();
            out.push((var, Arc::new(conn)));
        }
    }
    out
}

fn pipe_for(conn: &SqlConnector, metric: &str, params: serde_json::Value) -> Pipe {
    let Value::Map(params) = Value::from_json_str(&params.to_string()).unwrap() else {
        panic!("parameters must be an object");
    };
    Pipe::builder("plugin:w17bs", metric)
        .instance(&conn.keys())
        .parameters(params)
        .build()
        .unwrap()
}

/// A frame of documents (no dtype enforcement beyond what the pipe declares).
async fn frame(conn: &SqlConnector, pipe: &Pipe, docs: Vec<Vec<(&str, KValue)>>) -> Frame {
    let docs: Vec<Doc> = docs
        .into_iter()
        .map(|d| d.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
        .collect();
    let ctx = pipe_ops::dtypes_context(conn, pipe, pipe.parameters())
        .await
        .unwrap();
    pipe_enforce_dtypes(PipeInput::Docs(docs), &ctx).unwrap()
}

fn json_doc(v: serde_json::Value) -> KValue {
    KValue::Json(v)
}

async fn stored_dtypes(conn: &SqlConnector, pipe: &Pipe) -> IndexMap<String, String> {
    let attrs = conn.get_pipe_attributes(pipe).await.unwrap();
    let params = attrs
        .get("parameters")
        .and_then(|v| v.as_map().cloned())
        .unwrap_or_default();
    params
        .get("dtypes")
        .and_then(|v| v.as_map().cloned())
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k, v.py_str()))
        .collect()
}

fn expected(pairs: &[(&str, &str)]) -> IndexMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

async fn cleanup(conn: &SqlConnector, pipe: &Pipe) {
    let _ = conn.drop_pipe(pipe).await;
    let _ = conn.delete_pipe(pipe).await;
}

/// v4 over the API: the client sends JSON, so the server sees dicts and lists (json) but only
/// strings for a UUID / Decimal / bytes column. `get_special_cols` of that frame is
/// `{'j': 'json', 'l': 'json'}`, and the server persists exactly that, registering the pipe first
/// (`Pipe.sync` calls `register()` for an unregistered pipe).
async fn sync_over_the_api_persists_new_json_columns(served: &Served, tag: &str) {
    let conn = &served.conn;
    let pipe = pipe_for(
        conn,
        &unique(tag),
        serde_json::json!({"columns": {"id": "id"}}),
    );
    assert!(conn.get_pipe_id(&pipe).await.unwrap().is_none());
    let df = frame(
        conn,
        &pipe,
        vec![vec![
            ("id", KValue::Int(1)),
            ("j", json_doc(serde_json::json!({"a": 1}))),
            ("l", json_doc(serde_json::json!([1, 2]))),
            (
                "u",
                KValue::Str("6c2a5e0c-2d4c-4a3f-8c8e-4b0c7f1c9a11".into()),
            ),
            ("n", KValue::Str("1.5".into())),
            ("b", KValue::Str("eHk=".into())),
        ]],
    )
    .await;
    let res = served
        .client
        .sync_pipe(&pipe, df, &SyncOptions::default())
        .await;
    assert!(res.success, "{}", res.message);
    assert!(
        conn.get_pipe_id(&pipe).await.unwrap().is_some(),
        "registered by the sync"
    );
    assert_eq!(
        stored_dtypes(conn, &pipe).await,
        expected(&[("j", "json"), ("l", "json")])
    );
    let attrs = conn.get_pipe_attributes(&pipe).await.unwrap();
    let columns = attrs["parameters"].as_map().unwrap()["columns"].clone();
    assert_eq!(columns.as_map().unwrap()["id"], Value::Str("id".into()));

    // A second sync with one more dict column adds only that column; nothing else is rewritten.
    let df = frame(
        conn,
        &pipe,
        vec![vec![
            ("id", KValue::Int(2)),
            ("j", json_doc(serde_json::json!({"a": 2}))),
            ("l", json_doc(serde_json::json!([3]))),
            ("k", json_doc(serde_json::json!({"x": true}))),
        ]],
    )
    .await;
    let res = served
        .client
        .sync_pipe(&pipe, df, &SyncOptions::default())
        .await;
    assert!(res.success, "{}", res.message);
    assert_eq!(
        stored_dtypes(conn, &pipe).await,
        expected(&[("j", "json"), ("l", "json"), ("k", "json")])
    );
    assert_eq!(
        conn.get_pipe_rowcount(&pipe, &Default::default())
            .await
            .unwrap(),
        Some(2)
    );
    cleanup(conn, &pipe).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_sync_persists_new_json_columns() {
    let (served, _dir) = sqlite().await;
    sync_over_the_api_persists_new_json_columns(&served, "sqlite").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_flavors_sync_persists_new_json_columns() {
    for (var, conn) in server_flavors().await {
        eprintln!("{var}");
        let served = serve(conn).await;
        sync_over_the_api_persists_new_json_columns(&served, "flavor").await;
    }
}

/// `get_special_cols` of frames holding Python-side objects (a library user's `pipe.sync(df)` on
/// the server) finds all four special kinds; the helper persists them as v4's
/// `_persist_new_special_columns` does and records `new_special_cols` for `sync_pipe`.
async fn the_helper_persists_every_special_kind(conn: &Arc<SqlConnector>, tag: &str) {
    let mut pipe = pipe_for(
        conn,
        &unique(tag),
        serde_json::json!({"columns": {"id": "id"}}),
    );
    assert!(conn.register_pipe(&pipe).await.success);
    let df = frame(
        conn,
        &pipe,
        vec![vec![
            ("id", KValue::Int(1)),
            ("j", json_doc(serde_json::json!({"a": 1}))),
            ("u", KValue::Uuid(uuid::Uuid::new_v4())),
            (
                "n",
                KValue::Decimal(meerschaum_kernel::Decimal::parse("1.5").unwrap()),
            ),
            ("b", KValue::Bytes(b"xy".to_vec())),
        ]],
    )
    .await;
    let ctx = pipe_ops::dtypes_context(conn.as_ref(), &pipe, pipe.parameters())
        .await
        .unwrap();
    let mut extra = Map::new();
    let new = pipe_ops::persist_new_special_columns(
        conn.as_ref(),
        &mut pipe,
        &df,
        &ctx.dtypes,
        true,
        &mut extra,
    )
    .await;
    let want = expected(&[
        ("j", "json"),
        ("u", "uuid"),
        ("b", "bytes"),
        ("n", "numeric"),
    ]);
    assert_eq!(new, want);
    assert_eq!(stored_dtypes(conn, &pipe).await, want);
    let recorded = extra["new_special_cols"].as_map().unwrap();
    assert_eq!(recorded.len(), 4);

    // Once persisted they are not new any more: no edit, no `new_special_cols`.
    let ctx = pipe_ops::dtypes_context(conn.as_ref(), &pipe, pipe.parameters())
        .await
        .unwrap();
    let mut extra = Map::new();
    let new = pipe_ops::persist_new_special_columns(
        conn.as_ref(),
        &mut pipe,
        &df,
        &ctx.dtypes,
        true,
        &mut extra,
    )
    .await;
    assert!(new.is_empty() && extra.is_empty());
    cleanup(conn, &pipe).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_the_helper_persists_every_special_kind() {
    let (served, _dir) = sqlite().await;
    the_helper_persists_every_special_kind(&served.conn, "helper").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_flavors_the_helper_persists_every_special_kind() {
    for (_, conn) in server_flavors().await {
        the_helper_persists_every_special_kind(&conn, "helperflavor").await;
    }
}

/// `Pipe.sync()` runs `validate_indices()` first: a `json` column cannot be an index role.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_refuses_a_json_index_column() {
    let (served, _dir) = sqlite().await;
    let conn = &served.conn;
    let pipe = pipe_for(
        conn,
        &unique("jsonidx"),
        serde_json::json!({"columns": {"id": "j"}, "dtypes": {"j": "json"}}),
    );
    assert!(conn.register_pipe(&pipe).await.success);
    let df = frame(
        conn,
        &pipe,
        vec![vec![("j", json_doc(serde_json::json!({"a": 1})))]],
    )
    .await;
    let res = served
        .client
        .sync_pipe(&pipe, df, &SyncOptions::default())
        .await;
    assert!(!res.success);
    assert!(
        res.message
            .contains("Cannot use a 'json' column as an index column: 'j' (as the 'id' index)"),
        "{}",
        res.message
    );
}

/// `compress` on a plain instance is not an error for the sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_sync_with_compress_succeeds() {
    let (served, _dir) = sqlite().await;
    let conn = &served.conn;
    let pipe = pipe_for(
        conn,
        &unique("compress"),
        serde_json::json!({"columns": {"id": "id"}, "compress": {"after": "30 days"}}),
    );
    let df = frame(conn, &pipe, vec![vec![("id", KValue::Int(1))]]).await;
    let res = served
        .client
        .sync_pipe(&pipe, df, &SyncOptions::default())
        .await;
    assert!(res.success, "{}", res.message);
}

/// After a successful sync of a `compress` pipe the server installs the columnstore policy
/// (`apply_compression_policy`), which v4's `Pipe.sync()` does on the server's pipe.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timescaledb_sync_applies_the_compression_policy() {
    let Ok(uri) = std::env::var("MRSM_TEST_TIMESCALE_URI") else {
        return;
    };
    let conn = Arc::new(SqlConnector::from_uri("w17ts", &uri).await.unwrap());
    let served = serve(conn.clone()).await;
    let metric = unique("tscompress");
    let pipe = pipe_for(
        &conn,
        &metric,
        serde_json::json!({
            "columns": {"datetime": "dt", "id": "id"},
            "compress": {"after": "30 days"},
        }),
    );
    let df = frame(
        &conn,
        &pipe,
        vec![vec![
            ("dt", KValue::Str("2026-01-01 00:00:00".into())),
            ("id", KValue::Int(1)),
        ]],
    )
    .await;
    let res = served
        .client
        .sync_pipe(&pipe, df, &SyncOptions::default())
        .await;
    assert!(res.success, "{}", res.message);
    let target = pipe.target(None).unwrap();
    let jobs = conn
        .value(&format!(
            "SELECT COUNT(*) FROM timescaledb_information.jobs \
             WHERE proc_name = 'policy_compression' AND hypertable_name = '{target}'"
        ))
        .await;
    let n = match jobs {
        Some(meerschaum_connector_sql::driver::SqlValue::Int(n)) => n,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(n, 1, "one columnstore policy for {target}");
    cleanup(&conn, &pipe).await;
}

/// `POST /metadata` creates the instance tables (v4: `get_tables(mrsm_instance=conn)`) and
/// answers `true`; a second call is idempotent.
async fn post_metadata(served: &Served) -> (u16, String) {
    // A raw request over the loopback socket: the client's `create_metadata()` is v4's
    // always-`False` quirk.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", served.port))
        .await
        .unwrap();
    s.write_all(
        b"POST /metadata HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    )
    .await
    .unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_create_metadata_creates_the_instance_tables() {
    let (served, _dir) = sqlite().await;
    let conn = &served.conn;
    for table in ["mrsm_pipes", "mrsm_users", "mrsm_plugins"] {
        assert!(
            !conn.table_exists(table, conn.instance_schema()).await,
            "{table} exists before"
        );
    }
    let (status, body) = post_metadata(&served).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.trim_end().ends_with("true"), "{body}");
    for table in ["mrsm_pipes", "mrsm_users", "mrsm_plugins"] {
        assert!(
            conn.table_exists(table, conn.instance_schema()).await,
            "{table} missing"
        );
    }
    let (status, body) = post_metadata(&served).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.trim_end().ends_with("true"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_flavors_create_metadata_is_idempotent() {
    for (var, conn) in server_flavors().await {
        let served = serve(conn.clone()).await;
        for _ in 0..2 {
            let (status, body) = post_metadata(&served).await;
            assert_eq!(status, 200, "{var}: {body}");
            assert!(body.trim_end().ends_with("true"), "{var}: {body}");
        }
        assert!(
            conn.table_exists("mrsm_pipes", conn.instance_schema())
                .await,
            "{var}"
        );
    }
}
