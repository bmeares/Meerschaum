//! SQL fetch chunks preserve the v4 result envelope and accumulate native row counts.
mod common;

use common::*;
use meerschaum_engine::SyncArgs;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_fetch_chunk_result_and_empty_chunk() {
    let env = env("wiring_chunks").await;
    env.exec("CREATE TABLE wiring_source (id INTEGER, value INTEGER)")
        .await;
    env.exec("INSERT INTO wiring_source VALUES (1, 10), (2, 20), (3, 30)")
        .await;
    let pipe = env.pipe(
        "chunked",
        &json!({
            "columns": {"id": "id", "datetime": "id"},
            "dtypes": {"id": "int", "value": "int"},
            "fetch": {"definition": "SELECT * FROM wiring_source"}
        }),
    );
    let engine = env.engine();
    let args = SyncArgs {
        chunksize: Some(2),
        ..Default::default()
    };
    let result = engine.sync_pipe(&pipe, None, &args).await;
    assert!(result.success, "{}", result.message);
    assert_eq!(result.inserted, 3);
    assert!(result.message.starts_with("Synced 2 chunks to Pipe("));
    assert!(result.message.contains("(2 succeeded, 0 failed):"));
    assert!(
        result
            .message
            .contains("1 - 2\nInserted 2, updated 0 rows.")
    );
    assert!(result.message.contains("Synced a chunk (1 row)"));

    let empty = env.pipe(
        "empty",
        &json!({
            "fetch": {"definition": "SELECT * FROM wiring_source WHERE 1 = 0"}
        }),
    );
    let result = engine.sync_pipe(&empty, None, &args).await;
    assert!(result.success, "{}", result.message);
    assert_eq!(result.rows_written(), 0);
    assert!(result.message.starts_with("Synced 1 chunk to Pipe("));
}
