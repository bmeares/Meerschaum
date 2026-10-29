//! Regression: in-place syncs of several pipes in one SQLite file, from independent connections,
//! must all succeed.
//!
//! SQLite serializes writers. A deferred `BEGIN` that reads first and writes later (the in-place
//! session reads the staged table's datetime bounds, then stages the backtrack set) holds a read
//! lock when it asks for the write lock, and SQLite fails that request with `SQLITE_BUSY` at once,
//! without waiting for the busy timeout, whenever another connection holds the write lock
//! (waiting could deadlock the two). Parallel test processes sharing one database file hit this as
//! `Failed to execute queries. ... (code: 5) database is locked`. The in-place session therefore
//! begins with `BEGIN IMMEDIATE` (`SqlDriver::begin_write`), which waits for the write lock.

mod common;
use common::*;
use meerschaum_connector::SyncOptions;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::ident::sql_item_name;
use meerschaum_connector_sql::instance::inplace::InplaceObserver;
use meerschaum_connector_sql::queries::metadef::Bound;
use meerschaum_kernel::enforce::PipeInput;
use std::sync::Arc;

const WRITERS: usize = 10;
const ROUNDS: usize = 4;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_inplace_syncs_in_one_sqlite_file_never_report_a_locked_database() {
    let ctx = ctx("sqlite", "w18fx10").await.expect("sqlite context");
    let mut tasks = vec![];
    for n in 0..WRITERS {
        let conn = Arc::new(
            SqlConnector::connect(&format!("w18fx10_{n}"), ctx.conn.spec().clone())
                .await
                .unwrap(),
        );
        let mut source = ctx.pipe(
            &ctx.conn.keys(),
            "inplace",
            Some(&format!("src{n}")),
            r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
        );
        let target = conn.sql_pipe(&source).unwrap().target;
        let definition = format!(
            "SELECT * FROM {}",
            sql_item_name(&target, conn.flavor(), None)
        );
        let dest = ctx.pipe(
            &ctx.conn.keys(),
            "inplace",
            Some(&format!("dest{n}")),
            &serde_json::json!({
                "columns": {"datetime": "dt", "id": "id"},
                "fetch": {"definition": definition, "backtrack_minutes": 1440},
            })
            .to_string(),
        );
        tasks.push(tokio::spawn(async move {
            let mut failures = vec![];
            for round in 0..ROUNDS {
                let docs = (0..5u32)
                    .map(|k| {
                        doc(vec![
                            ("dt", ndt(2023, 1, 1 + round as u32, 0, k)),
                            ("id", i((round as i64) * 10 + i64::from(k))),
                        ])
                    })
                    .collect();
                let synced = conn
                    .sync_mut(&mut source, PipeInput::Docs(docs), &SyncOptions::default())
                    .await;
                if !synced.success {
                    failures.push(format!("source sync: {}", synced.message));
                }
                let result = conn
                    .sync_pipe_inplace(&dest, None, Bound::Default, Bound::None, true)
                    .await;
                if !result.success {
                    failures.push(format!("in-place sync: {}", result.message));
                }
            }
            failures
        }));
    }
    let mut failures = vec![];
    for task in tasks {
        failures.extend(task.await.unwrap());
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The observer of an in-place sync (the engine's dirty-range ledger) writes through another
/// connection of the same SQLite file while the sync is between two sessions. The session that
/// follows takes the write lock when it begins, so it must begin after the observer, or the
/// observer's write waits for a lock its own caller holds and fails with "database is locked".
struct LedgerObserver {
    writer: SqlConnector,
    ledger: meerschaum_core::Pipe,
    calls: std::sync::atomic::AtomicUsize,
}

#[meerschaum_connector::async_trait]
impl InplaceObserver for LedgerObserver {
    async fn before_merge(&self, delta: &meerschaum_kernel::Frame) -> Result<(), String> {
        // A new row every time: a write that finds nothing to insert takes no write lock.
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let docs = vec![doc(vec![
            ("dt", ndt(2023, 1, 1, 0, 0)),
            ("id", i((call * 1000 + delta.height()) as i64)),
        ])];
        let result = self
            .writer
            .sync(&self.ledger, PipeInput::Docs(docs), &SyncOptions::default())
            .await;
        if result.success {
            Ok(())
        } else {
            Err(result.message)
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_observer_that_writes_does_not_wait_for_the_session_that_follows_it() {
    let ctx = ctx("sqlite", "w18fx10obs").await.expect("sqlite context");
    let writer = SqlConnector::connect("w18fx10_ledger", ctx.conn.spec().clone())
        .await
        .unwrap();
    let mut source = ctx.pipe(
        &ctx.conn.keys(),
        "inplace",
        Some("src"),
        r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
    );
    let target = ctx.conn.sql_pipe(&source).unwrap().target;
    let dest = ctx.pipe(
        &ctx.conn.keys(),
        "inplace",
        Some("dest"),
        &serde_json::json!({
            "columns": {"datetime": "dt", "id": "id"},
            "fetch": {
                "definition": format!("SELECT * FROM {}", sql_item_name(&target, ctx.conn.flavor(), None)),
                "backtrack_minutes": 1440,
            },
        })
        .to_string(),
    );
    let observer = LedgerObserver {
        writer,
        ledger: ctx.pipe(
            "mrsm",
            "ledger",
            None,
            r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
        ),
        calls: Default::default(),
    };
    for round in 0..3u32 {
        let docs = (0..3u32)
            .map(|k| {
                doc(vec![
                    ("dt", ndt(2023, 1, 1 + round, 0, k)),
                    ("id", i(i64::from(round * 10 + k))),
                ])
            })
            .collect();
        ok(ctx
            .conn
            .sync_mut(&mut source, PipeInput::Docs(docs), &SyncOptions::default())
            .await);
        let started = std::time::Instant::now();
        let result = ctx
            .conn
            .sync_pipe_inplace_observed(&dest, None, Bound::Default, Bound::None, true, &observer)
            .await;
        assert!(result.success, "{}", result.message);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(4),
            "the in-place sync waited for a lock: {:?}",
            started.elapsed()
        );
    }
    assert!(observer.calls.load(std::sync::atomic::Ordering::SeqCst) >= 3);
}
