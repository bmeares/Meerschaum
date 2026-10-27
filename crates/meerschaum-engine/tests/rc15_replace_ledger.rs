//! A replacing sync (rc15 Q19, `fetch: {replace: true}`) may delete rows of its window without
//! writing anything in their place. Its ledger entry covers the whole window, not only the
//! cells of the rows it wrote, so a child re-syncs the window and drops those rows too.

mod common;

use common::env;
use meerschaum_engine::{Depth, RunOptions, Selection};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_a_parent_replace_deletes_leave_its_children() {
    let e = env("rc15_replace_ledger").await;
    e.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    e.exec(
        "INSERT INTO src VALUES \
         ('2024-01-01 00:00:00', 1, 1.0), ('2024-01-02 00:00:00', 1, 2.0), \
         ('2024-01-03 00:00:00', 1, 3.0), ('2024-01-04 00:00:00', 1, 4.0)",
    )
    .await;
    let params = |sql: &str, backtrack_minutes: i64| {
        serde_json::json!({
            "sql": sql,
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
            // One day per ledger cell.
            "verify": {"chunk_minutes": 1440},
            "fetch": {"replace": true, "backtrack_minutes": backtrack_minutes},
        })
    };
    // The parent's backtrack window reaches every row; the child's does not, so the child
    // re-syncs older days only where its parent's ledger entries say something changed.
    e.register("parent", &params("SELECT ts, id, v FROM src", 14400))
        .await;
    e.register(
        "child",
        &params(
            "SELECT ts, id, v FROM {{ Pipe('sql:eng', 'parent', instance='sql:eng') }}",
            1,
        ),
    )
    .await;
    let engine = e.engine();
    let run_child = || {
        engine.run(RunOptions {
            selection: Some(Selection::refs([e.pipe_ref("child")]).with_parents(Some(Depth::All))),
            ..Default::default()
        })
    };
    let run = run_child().await;
    assert!(run.success, "{}", run.message);
    assert_eq!(e.rowcount("parent").await, 4);
    assert_eq!(e.rowcount("child").await, 4);

    // A day leaves the source: the parent's replace deletes it and writes nothing in its cell.
    // The child must still re-sync that cell and drop the row. (The parent's latest day stays,
    // so the child's window, which ends at its parents' latest data, still covers it.)
    e.exec("DELETE FROM src WHERE ts = '2024-01-02 00:00:00'")
        .await;
    let run = run_child().await;
    assert!(run.success, "{}", run.message);
    assert_eq!(e.rowcount("parent").await, 3, "{}", run.message);
    assert_eq!(
        e.table("child").await,
        e.table("parent").await,
        "the child kept a row its parent's replace deleted: {}",
        run.message
    );
}
