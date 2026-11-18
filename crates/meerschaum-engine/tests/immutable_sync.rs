mod common;

use chrono::{TimeZone, Utc};
use common::{Env, env};
use meerschaum_connector::DataQuery;
use meerschaum_core::Pipe;
use meerschaum_engine::SyncArgs;

async fn fixture(strategy: &str) -> (Env, Pipe) {
    let env = env("immutable_sync").await;
    env.exec("CREATE TABLE upstream (ts TEXT NOT NULL, sensor INTEGER NOT NULL)")
        .await;
    env.exec("INSERT INTO upstream VALUES ('2024-05-25 12:00:00', 1)")
        .await;
    let pipe = env.pipe(
        "readings",
        &serde_json::json!({
            "columns": {"datetime": "ts", "id": "sensor"},
            "dtypes": {"ts": "datetime64[ns, UTC]", "sensor": "int"},
            "fetch": {"definition": "SELECT ts, sensor FROM upstream"},
            "sync": {"immutable": true, "strategy": strategy},
        }),
    );
    (env, pipe)
}

async fn sync_at(env: &Env, pipe: &Pipe, month: u32) {
    let args = SyncArgs {
        sync_timestamp: Utc.with_ymd_and_hms(2026, month, 10, 0, 0, 0).unwrap(),
        ..SyncArgs::default()
    };
    let result = env.engine().sync_pipe(pipe, None, &args).await;
    assert!(result.success, "{}", result.message);
}

async fn target_count(env: &Env, pipe: &Pipe) -> i64 {
    env.conn
        .get_pipe_rowcount(pipe, &DataQuery::default())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn unbounded_iterative_sync_recovers_sparse_old_rows() {
    let (env, pipe) = fixture("unbounded-simple").await;
    sync_at(&env, &pipe, 1).await;
    env.exec("INSERT INTO upstream VALUES ('1972-01-03 12:00:00', 1), ('2024-05-28 12:00:00', 1)")
        .await;
    sync_at(&env, &pipe, 1).await;
    assert_eq!(target_count(&env, &pipe).await, 3);
}

#[tokio::test]
async fn monthly_daily_counts_checkpoint_survives_engine_restart() {
    let (env, pipe) = fixture("simple-monthly-daily-row-count").await;
    sync_at(&env, &pipe, 1).await;
    env.exec("INSERT INTO upstream VALUES ('2020-08-20 12:00:00', 1), ('2024-05-28 12:00:00', 1)")
        .await;
    sync_at(&env, &pipe, 1).await;
    assert_eq!(target_count(&env, &pipe).await, 2);
    sync_at(&env, &pipe, 2).await;
    assert_eq!(target_count(&env, &pipe).await, 3);
}

#[tokio::test]
async fn postgres_watermarks_and_sparse_cpi() {
    use std::sync::Arc;

    use meerschaum_connector_sql::SqlConnector;
    use meerschaum_engine::{Engine, StaticRegistry};

    let Ok(uri) = std::env::var("MRSM_TEST_PG_URI") else {
        assert!(std::env::var_os("MRSM_TEST_ISOLATED").is_none());
        eprintln!("Skipping postgresql: set MRSM_TEST_PG_URI.");
        return;
    };
    let env = env("immutable_pg").await;
    let connection = Arc::new(SqlConnector::from_uri("immutable_pg", &uri).await.unwrap());
    let keys = "sql:immutable_pg";
    let registry = Arc::new(
        StaticRegistry::new()
            .with_instance(keys, connection.clone())
            .with_source(keys, connection.clone()),
    );
    let engine = Engine::new(registry, env.config());
    let nonce = uuid::Uuid::new_v4().simple();
    let source = format!("imm_src_{nonce}");
    connection
        .exec(&format!(
            "CREATE TABLE {source} (ts TIMESTAMPTZ NOT NULL, sensor TEXT NOT NULL)"
        ))
        .await
        .unwrap();
    connection.exec(&format!(
        "INSERT INTO {source} VALUES ('2024-05-25 12:00:00+00', 'fast'), ('2024-05-25 10:00:00+00', 'slow''s')"
    )).await.unwrap();
    let mut pipes = Vec::new();
    for (index, strategy) in ["slow-id", "append", "join", "iterative-cpisync"]
        .iter()
        .enumerate()
    {
        let target = format!("imm_dst_{index}_{nonce}");
        let pipe = Pipe::builder(keys, &target)
            .instance(keys)
            .parameters(common::json_map(&serde_json::json!({
                "target": target, "hypertable": false,
                "columns": {"datetime": "ts", "id": "sensor"},
                "dtypes": {"ts": "datetime64[ns, UTC]", "sensor": "string"},
                "fetch": {"definition": format!("SELECT ts, sensor FROM {source}")},
                "sync": {"immutable": true, "strategy": strategy},
            })))
            .build()
            .unwrap();
        let result = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
        assert!(result.success, "{strategy}: {}", result.message);
        pipes.push((pipe, strategy));
    }
    connection.exec(&format!(
        "INSERT INTO {source} VALUES ('2024-05-25 11:00:00.000017+00', 'slow''s'), ('2024-05-25 01:00:00+00', 'new')"
    )).await.unwrap();
    for (pipe, strategy) in pipes {
        let result = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
        assert!(result.success, "{strategy}: {}", result.message);
        assert_eq!(
            connection
                .get_pipe_rowcount(&pipe, &DataQuery::default())
                .await
                .unwrap(),
            Some(4),
            "{strategy}"
        );
        assert!(connection.drop_pipe(&pipe).await.success);
        assert!(connection.delete_pipe(&pipe).await.success);
    }
    connection
        .exec(&format!("DROP TABLE {source}"))
        .await
        .unwrap();
}
