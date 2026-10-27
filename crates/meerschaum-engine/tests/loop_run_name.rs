//! `LoopOptions::run_name`: every lap of a saved Pipeline's loop (`sync pipelines NAME --loop`)
//! is a run named after the Pipeline, like a single `sync pipelines NAME` run; a configured
//! `RunOptions::run_id` still names the first lap only.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::*;
use meerschaum_engine::{EventKind, LoopOptions, RunOptions, Selection};

async fn source(env: &Env) {
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec("INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0)")
        .await;
    env.register(
        "m",
        &serde_json::json!({
            "sql": "SELECT ts, id, v FROM src",
            "columns": {"datetime": "ts", "id": "id"},
        }),
    )
    .await;
}

/// The ids of the runs two laps of a loop start, in order.
async fn lap_run_ids(env: &Env, run_id: Option<&str>, run_name: Option<&str>) -> Vec<String> {
    let engine = env.engine();
    let mut events = engine.subscribe();
    let opts = RunOptions {
        selection: Some(Selection::refs([env.pipe_ref("m")])),
        run_id: run_id.map(str::to_string),
        ..Default::default()
    };
    let result = engine
        .run_loop(
            opts,
            LoopOptions {
                run_loop: true,
                min_seconds: 0.0,
                max_laps: Some(2),
                run_name: run_name.map(str::to_string),
                ..Default::default()
            },
            Arc::new(|_| {}),
        )
        .await;
    assert_eq!(result.laps.len(), 2);
    let mut ids = Vec::new();
    loop {
        match events.try_recv() {
            Ok(event) => {
                if matches!(event.kind, EventKind::RunStarted { .. }) {
                    ids.push(event.run_id);
                }
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(_) => break,
        }
    }
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_lap_of_a_named_loop_is_named_after_it() {
    let env = env("loop_run_name").await;
    source(&env).await;

    let named = lap_run_ids(&env, None, Some("daily")).await;
    assert_eq!(named.len(), 2, "{named:?}");
    assert!(named.iter().all(|id| id.starts_with("daily-")), "{named:?}");
    assert_eq!(named.iter().collect::<BTreeSet<_>>().len(), 2, "{named:?}");

    // Without a name, laps keep the default ids.
    let plain = lap_run_ids(&env, None, None).await;
    assert_eq!(plain.len(), 2, "{plain:?}");
    assert!(
        plain.iter().all(|id| !id.starts_with("daily-")),
        "{plain:?}"
    );

    // A configured id names the first lap only; the next lap is named after the loop.
    let configured = lap_run_ids(&env, Some("my-correlation-id"), Some("daily")).await;
    assert_eq!(configured.len(), 2, "{configured:?}");
    assert_eq!(configured[0], "my-correlation-id");
    assert!(configured[1].starts_with("daily-"), "{configured:?}");
}
