//! Engine state on an API instance (§5.8, R2) lives on the server's own instance and survives a
//! server restart: the engine reaches its `mrsm:engine_*` pipes through
//! `/engine/pipes/{metric}/*` (the `APIConnector` routes them there), and `start api` keeps
//! them with `meerschaum_server::engine_store::InstanceEngine`, as wired here.
//!
//! A graph run over `api:w17bg1` writes runs, ledger entries and cursors; the server is
//! stopped and started again on the same SQLite instance; the state is still there and a second
//! run finds the child caught up (`skip`), which it could only do from the persisted cursor.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::InstanceConnector;
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_engine::{
    Action, Depth, Engine, EngineConfig, NodeStatus, PipeRef, RunOptions, RunResult, Selection,
    StateSource, StaticRegistry,
};
use meerschaum_server::engine_store::InstanceEngine;
use meerschaum_server::{AppState, ServerConfig};

const API: &str = "api:w17bg1";
const SRC: &str = "sql:w17bg1src";
const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef0123456789abcdef";

struct Server {
    port: u16,
    task: tokio::task::JoinHandle<()>,
}

/// `start api` over the SQLite instance at `db` (no auth), engine state on that instance.
async fn serve(db: &Path, root: &Path) -> Server {
    let conn = Arc::new(
        SqlConnector::from_uri("w17bg1srv", &format!("sqlite:///{}", db.display()))
            .await
            .unwrap(),
    );
    let mut cfg = ServerConfig::for_root(&conn.keys(), root);
    cfg.no_auth = true;
    cfg.no_webterm = true;
    cfg.no_dash = true;
    let instance: Arc<dyn InstanceConnector> = conn.clone();
    let state = AppState::builder(cfg, instance.clone())
        .secret(SECRET.to_vec())
        .engine(Arc::new(InstanceEngine::new(instance, &conn.keys())))
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
    Server { port, task }
}

fn client(port: u16) -> Arc<APIConnector> {
    let mut attrs = Map::new();
    attrs.insert("host".into(), Value::Str("127.0.0.1".into()));
    attrs.insert("port".into(), Value::Int(port as i64));
    Arc::new(APIConnector::new(Some("w17bg1"), attrs).unwrap())
}

fn engine(api: &Arc<APIConnector>, src: &Arc<SqlConnector>, dir: &Path) -> Engine {
    let instance: Arc<dyn InstanceConnector> = api.clone();
    let source: Arc<dyn meerschaum_connector::Connector> = src.clone();
    let registry = Arc::new(
        StaticRegistry::new()
            .with_instance(API, instance)
            .with_source(SRC, source),
    );
    Engine::new(
        registry,
        EngineConfig {
            home_instance: API.into(),
            lease_dir: dir.join("leases"),
            lease_ttl: Duration::from_secs(10),
            lease_poll: Duration::from_millis(50),
            state_flush: Duration::from_millis(20),
            holder: format!("w17bg1:{}", std::process::id()),
            ..EngineConfig::default()
        },
    )
}

fn params(sql: &str, parent: Option<&str>) -> Map {
    let mut j = serde_json::json!({
        "sql": sql,
        "columns": {"datetime": "ts", "id": "id"},
        "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
    });
    if let Some(p) = parent {
        j["parents"] = serde_json::json!([format!("Pipe('{SRC}', '{p}', instance='{API}')")]);
    }
    Value::from_json_str(&j.to_string()).unwrap().to_map()
}

async fn run(engine: &Engine, child: &PipeRef) -> RunResult {
    let result = engine
        .run(RunOptions {
            selection: Some(Selection::refs([child.clone()]).with_parents(Some(Depth::All))),
            ..Default::default()
        })
        .await;
    assert!(result.success, "{}", result.message);
    assert!(
        !result.message.contains("warning"),
        "the engine state was not persisted: {}",
        result.message
    );
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_instance_engine_state_survives_a_server_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("server.db");
    let src = Arc::new(
        SqlConnector::from_uri(
            "w17bg1src",
            &format!("sqlite:///{}", dir.path().join("src.db").display()),
        )
        .await
        .unwrap(),
    );
    assert!(
        src.exec("CREATE TABLE src (ts TIMESTAMP, id INTEGER, v DOUBLE PRECISION)")
            .await
            .is_some()
    );
    let values: Vec<String> = (0..6)
        .map(|h| format!("('2026-01-01 {h:02}:00:00', {}, {h}.5)", h % 2))
        .collect();
    assert!(
        src.exec(&format!(
            "INSERT INTO src (ts, id, v) VALUES {}",
            values.join(", ")
        ))
        .await
        .is_some()
    );

    let server = serve(&db, dir.path()).await;
    let api = client(server.port);
    let raw = Pipe::builder(SRC, "w17bg1_raw")
        .instance(API)
        .parameters(params("SELECT ts, id, v FROM src", None))
        .build()
        .unwrap();
    let clean = Pipe::builder(SRC, "w17bg1_clean")
        .instance(API)
        .parameters(params(
            "SELECT ts, id, v FROM src WHERE v > 1",
            Some("w17bg1_raw"),
        ))
        .build()
        .unwrap();
    for p in [&raw, &clean] {
        let res = api.register_pipe(p).await;
        assert!(res.success, "{}", res.message);
    }
    let (raw_ref, clean_ref) = (
        PipeRef::new(API, SRC, "w17bg1_raw", None),
        PipeRef::new(API, SRC, "w17bg1_clean", None),
    );

    let first = run(&engine(&api, &src, dir.path()), &clean_ref).await;
    let record = first.record.as_ref().unwrap();
    assert!(
        record
            .nodes
            .iter()
            .all(|n| n.status == NodeStatus::Succeeded),
        "{:?}",
        record.nodes
    );
    let run_id = first.run_id().unwrap().to_string();

    // The state is on the server's instance, in its own tables.
    let server_conn = SqlConnector::from_uri("w17bg1chk", &format!("sqlite:///{}", db.display()))
        .await
        .unwrap();
    for table in [
        "mrsm_engine_runs",
        "mrsm_engine_nodes",
        "mrsm_engine_ledger",
        "mrsm_engine_cursors",
        "mrsm_engine_state",
    ] {
        assert!(server_conn.table_exists(table, None).await, "{table}");
    }

    // Restart the server (a new process would see exactly this database).
    server.task.abort();
    let _ = server.task.await;
    let server = serve(&db, dir.path()).await;
    let api = client(server.port);
    let engine = engine(&api, &src, dir.path());
    let state = engine.state().clone();

    let stored = state
        .load_run(&run_id)
        .await
        .unwrap()
        .expect("the run survived the restart");
    assert_eq!(stored.nodes.len(), 2, "{:?}", stored.nodes);
    let cursor = state
        .cursor(API, &raw_ref, &clean_ref)
        .await
        .unwrap()
        .expect("the cursor survived the restart");
    assert!(cursor.parent_watermark_at_cursor.is_some());
    assert!(state.latest_seq(API, &raw_ref).await.unwrap().is_some());
    assert!(state.definition_hash(&clean_ref).await.unwrap().is_some());

    // Nothing new upstream: the child is caught up from the persisted cursor.
    let second = run(&engine, &clean_ref).await;
    let node = second.plan.as_ref().unwrap().node(&clean_ref).unwrap();
    assert_eq!(node.action, Action::Skip, "{}", node.reason);
    let runs = state.recent_runs(10).await.unwrap();
    assert!(runs.iter().any(|r| r.run_id == run_id));
    assert_eq!(runs.len(), 2);

    // Leases go to the server's instance too.
    let lease = Pipe::builder(SRC, "w17bg1_lease")
        .instance(API)
        .build()
        .unwrap();
    let ttl = Duration::from_secs(10);
    assert!(api.acquire_pipe_lease(&lease, "a", ttl).await.unwrap());
    assert!(!api.acquire_pipe_lease(&lease, "b", ttl).await.unwrap());
    assert!(api.release_pipe_lease(&lease, "a").await.unwrap());

    // Dropping the child forgets its cursor and definition hash (keyed on this client's
    // `api:` pipe refs, which only the client knows); the parent's ledger stays.
    let res = InstanceConnector::drop_pipe(api.as_ref(), &clean).await;
    assert!(res.success, "{}", res.message);
    assert!(
        state
            .cursor(API, &raw_ref, &clean_ref)
            .await
            .unwrap()
            .is_none()
    );
    assert!(state.definition_hash(&clean_ref).await.unwrap().is_none());
    assert!(state.latest_seq(API, &raw_ref).await.unwrap().is_some());

    server.task.abort();
}
