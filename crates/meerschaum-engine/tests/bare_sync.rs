//! A bare `pipe.sync()` (R1) through [`Engine::sync_pipe`]: v4 behavior, no graph traversal,
//! no capping, and no engine state written.

mod common;

use common::*;
use meerschaum_engine::SyncArgs;
use meerschaum_engine::runs::EnginePipe;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_sync_is_v4_sync() {
    let fx: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/v4_sync/v4_sync.json")).unwrap();
    let env = env("bare").await;
    env.exec(fx["create"].as_str().unwrap()).await;
    for stmt in fx["phases"][0]["statements"].as_array().unwrap() {
        env.exec(stmt.as_str().unwrap()).await;
    }
    let pipes: Vec<_> = fx["pipes"].as_array().unwrap().iter().take(2).collect();
    let engine = env.engine();
    // v4 order: a_raw, then b_clean, each a plain `pipe.sync()`; registration happens on sync.
    for p in &pipes {
        let pipe = env.pipe(p["metric"].as_str().unwrap(), &p["parameters"]);
        let result = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
        assert!(result.success, "{}", result.message);
        assert!(result.inserted > 0, "{result:?}");
        assert!(env.conn.get_pipe_id(&pipe).await.unwrap().is_some());
    }
    for p in &pipes {
        let metric = p["metric"].as_str().unwrap();
        let want = Table::from_fixture(&fx["phases"][0]["expected"][metric]);
        assert_eq!(env.table(metric).await, want, "{metric}");
    }
    // No ledger, cursors or hashes: the engine tables were never created.
    for kind in [EnginePipe::Ledger, EnginePipe::Cursors, EnginePipe::State] {
        let pipe = meerschaum_engine::instance_state::engine_pipe(INSTANCE, kind);
        assert!(!env.conn.pipe_exists(&pipe).await.unwrap(), "{kind:?}");
    }
    // A second bare sync finds nothing new (`begin=''` backtracks from the sync time).
    let pipe = env.pipe("a_raw", &pipes[0]["parameters"]);
    let again = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
    assert!(again.success, "{}", again.message);
    assert_eq!(again.rows_written(), 0, "{}", again.message);
}
