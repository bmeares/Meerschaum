//! The engine resolves `{{ Pipe(...) }}` through its instance-backed resolver for every node
//! it syncs (graph runs, flat runs and a bare `sync_pipe`), and never writes the resolved text
//! back into the stored parameters (raw versus resolved, `PARAMETER_CALL_SITES.md`).

mod common;

use common::*;
use meerschaum_engine::{Depth, KeyFilter, RunOptions, Selection, SyncArgs};
use serde_json::json;

async fn setup(tag: &str) -> Env {
    let env = env(tag).await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec(
        "INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0), ('2026-01-01 01:00:00', 1, 2.0)",
    )
    .await;
    env.register(
        "a_raw",
        &json!({
            "sql": "SELECT ts, id, v FROM src",
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        }),
    )
    .await;
    // `b_clean` reads A's target table through a template, and A's target is a renamed table.
    env.register(
        "b_clean",
        &json!({
            "sql": "SELECT ts, id, v * 2 AS v FROM {{ Pipe('sql:eng', 'a_raw', instance='sql:eng') }}",
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        }),
    )
    .await;
    env
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_run_resolves_templates_and_keeps_raw_parameters() {
    let env = setup("templates_graph").await;
    let engine = env.engine();
    let result = engine
        .run(RunOptions {
            selection: Some(
                Selection::refs([env.pipe_ref("b_clean")]).with_parents(Some(Depth::All)),
            ),
            ..Default::default()
        })
        .await;
    assert!(result.success, "{}", result.message);
    assert_eq!(env.rowcount("a_raw").await, 2);
    assert_eq!(env.rowcount("b_clean").await, 2);

    let stored = env
        .conn
        .get_pipe_attributes(&env.pipe("b_clean", &json!({})))
        .await
        .unwrap();
    let text = format!("{:?}", stored.get("parameters"));
    assert!(
        text.contains("{{ Pipe("),
        "stored parameters were resolved: {text}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flat_run_and_bare_sync_resolve_templates_too() {
    let env = setup("templates_flat").await;
    let engine = env.engine();
    // Flat: both nodes are roots; `a_raw` sorts first, so its table exists for `b_clean`.
    let result = engine
        .run(RunOptions {
            selection: Some(Selection::filter(KeyFilter::default())),
            ..Default::default()
        })
        .await;
    assert!(result.success, "{}", result.message);
    assert_eq!(env.rowcount("b_clean").await, 2);

    // A bare `pipe.sync()` (no graph, no known pipes) resolves through the registry and the
    // instance's registered parameters.
    env.exec("INSERT INTO src VALUES ('2026-01-01 02:00:00', 1, 3.0)")
        .await;
    let a = env.pipe("a_raw", &json!({}));
    assert!(
        engine
            .sync_pipe(&a, None, &SyncArgs::default())
            .await
            .success
    );
    let b = env.pipe("b_clean", &json!({}));
    let res = engine.sync_pipe(&b, None, &SyncArgs::default()).await;
    assert!(res.success, "{}", res.message);
    assert_eq!(env.rowcount("b_clean").await, 3);
}
