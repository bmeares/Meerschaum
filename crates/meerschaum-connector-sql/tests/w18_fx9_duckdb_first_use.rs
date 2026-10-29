//! Regression: connectors that first use the same DuckDB file at the same moment must not fail.
//!
//! One process shares one DuckDB database per file, so independent connectors are separate
//! connections to it and DuckDB's optimistic concurrency applies: two transactions creating the
//! same catalog entry raise "Catalog write-write conflict" in the loser. Every lazily created
//! internal object (the lease table, the instance tables, indexes, sequences, pipe target tables)
//! goes through the same path, so the driver retries an autocommit statement that lost such a
//! conflict (the statement was rolled back whole) and the idempotent `IF NOT EXISTS` / existence
//! checks around it then see the winner's table.
#![cfg(feature = "duckdb")]

mod common;
use common::*;
use meerschaum_connector_sql::SqlConnector;
use std::time::Duration;

const CONNECTORS: usize = 6;
const ROUNDS: usize = 12;

fn skip_without_duckdb() -> bool {
    if std::env::var("MRSM_TEST_DUCKDB_PATH").is_err() {
        eprintln!("Skipping duckdb: set MRSM_TEST_DUCKDB_PATH.");
        return true;
    }
    false
}

/// A fresh file; every round starts with no tables at all.
async fn fresh_ctx(round: usize) -> Ctx {
    ctx("duckdb", &format!("w18fx9_r{round}"))
        .await
        .expect("duckdb context")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_use_of_one_file_never_conflicts() {
    if skip_without_duckdb() {
        return;
    }
    for round in 0..ROUNDS {
        let ctx = fresh_ctx(round).await;
        let mut conns = vec![];
        for n in 0..CONNECTORS {
            conns.push(std::sync::Arc::new(
                SqlConnector::connect(&format!("w18fx9_{n}"), ctx.conn.spec().clone())
                    .await
                    .unwrap(),
            ));
        }
        // Each connector races: the lease table, the instance tables (via registration and a
        // sync, which also creates the pipe's target table and indexes), and a shared pipe.
        let shared = ctx.pipe(
            "_shared",
            "m",
            None,
            r#"{"columns":{"datetime":"dt","id":"id"}}"#,
        );
        let tasks = conns.iter().enumerate().map(|(n, conn)| {
            let own = ctx.pipe(
                &format!("_own{n}"),
                "m",
                None,
                r#"{"columns":{"datetime":"dt","id":"id"}}"#,
            );
            let shared = shared.clone();
            let conn = std::sync::Arc::clone(conn);
            tokio::spawn(async move {
                let lease = conn
                    .acquire_pipe_lease(&own, &format!("h{n}"), Duration::from_secs(5))
                    .await;
                let docs = vec![doc(vec![
                    ("dt", ndt(2024, 1, 1, 0, 0)),
                    ("id", i(n as i64)),
                ])];
                let own_sync = conn
                    .sync(
                        &own,
                        meerschaum_kernel::enforce::PipeInput::Docs(docs.clone()),
                        &Default::default(),
                    )
                    .await;
                let shared_sync = conn
                    .sync(
                        &shared,
                        meerschaum_kernel::enforce::PipeInput::Docs(docs),
                        &Default::default(),
                    )
                    .await;
                (lease, own_sync, shared_sync)
            })
        });
        let handles: Vec<_> = tasks.collect();
        let mut results = vec![];
        for handle in handles {
            results.push(handle.await.expect("task"));
        }
        for (n, (lease, own_sync, shared_sync)) in results.into_iter().enumerate() {
            assert!(lease.unwrap_or_else(|e| panic!("round {round} lease {n}: {e}")));
            assert!(
                own_sync.success,
                "round {round} own sync {n}: {}",
                own_sync.message
            );
            assert!(
                shared_sync.success,
                "round {round} shared sync {n}: {}",
                shared_sync.message
            );
        }
        assert_eq!(
            ctx.rowcount(&shared, Default::default()).await,
            Some(CONNECTORS as i64),
            "round {round}: every connector's row landed once"
        );
    }
}
