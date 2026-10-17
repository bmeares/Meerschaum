//! The engine-state write path (§5.8): per user sync, the engine adds one ledger `INSERT`
//! (before the data commit) and one cursor upsert (after it), and nothing else in steady
//! state. Measured with the SQLite instance's query log.

mod common;

use common::*;
use meerschaum_engine::{Depth, RunOptions, Selection};

fn writes_to(log: &[String], table: &str) -> Vec<String> {
    log.iter()
        .map(|q| q.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|q| {
            let head = q.to_ascii_uppercase();
            (head.starts_with("INSERT INTO")
                || head.starts_with("UPDATE")
                || head.starts_with("DELETE")
                || head.starts_with("CREATE")
                || head.starts_with("DROP"))
                && q.contains(table)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_state_adds_one_ledger_insert_and_one_cursor_upsert_per_sync() {
    let fx: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/v4_sync/v4_sync.json")).unwrap();
    let env = env("state_cost").await;
    env.exec(fx["create"].as_str().unwrap()).await;
    for p in fx["pipes"].as_array().unwrap().iter().take(2) {
        env.register(p["metric"].as_str().unwrap(), &p["parameters"])
            .await;
    }
    let engine = env.engine_with(|c| c.persist_runs = false);
    let opts = || RunOptions {
        selection: Some(Selection::refs([env.pipe_ref("b_clean")]).with_parents(Some(Depth::All))),
        ..Default::default()
    };
    for phase in 0..2 {
        for stmt in fx["phases"][phase]["statements"].as_array().unwrap() {
            env.exec(stmt.as_str().unwrap()).await;
        }
        if phase == 1 {
            env.conn.enable_query_log();
        }
        let r = engine.run(opts()).await;
        assert!(r.success, "{}", r.message);
    }
    let log = env.conn.take_query_log();

    // a_raw (a parent) wrote its changed range once, before its data commit.
    let ledger: Vec<String> = writes_to(&log, "\"mrsm_engine_ledger\"")
        .into_iter()
        .filter(|q| q.starts_with("INSERT"))
        .collect();
    assert_eq!(ledger.len(), 1, "{ledger:#?}");
    // b_clean (the child) advanced its cursor with one upsert into the cursors table.
    let cursors: Vec<String> = writes_to(&log, "\"mrsm_engine_cursors\"")
        .into_iter()
        .filter(|q| q.starts_with("INSERT INTO \"mrsm_engine_cursors\""))
        .collect();
    assert_eq!(cursors.len(), 1, "{cursors:#?}");
    // The definition hashes are unchanged: no `engine_state` writes.
    assert!(writes_to(&log, "\"mrsm_engine_state\"").is_empty());
    // Ledger order: the entry precedes a_raw's data commit.
    let pos = |needle: &str| log.iter().position(|q| q.contains(needle)).unwrap();
    assert!(
        pos("INSERT INTO \"mrsm_engine_ledger\"") < pos("INSERT INTO \"sql_eng_a_raw\""),
        "the ledger entry must be written before the data commit"
    );
}
