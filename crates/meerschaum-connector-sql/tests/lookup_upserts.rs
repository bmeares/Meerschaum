//! Physical UPSERT counts and logical dependency invalidation are separate contracts.
mod common;

use common::*;
use meerschaum_connector::SyncOptions;
use meerschaum_connector_sql::instance::inplace::InplaceObserver;
use meerschaum_connector_sql::queries::metadef::Bound;
use meerschaum_kernel::{Frame, enforce::PipeInput};
use std::sync::Mutex;

#[derive(Default)]
struct Changes(Mutex<Vec<usize>>);

#[meerschaum_connector::async_trait]
impl InplaceObserver for Changes {
    async fn before_merge(&self, delta: &Frame) -> Result<(), String> {
        self.0.lock().unwrap().push(delta.height());
        Ok(())
    }
}

flavor_tests!(
    unchanged_upserts_do_not_invalidate_dependents,
    |c: Ctx| async move {
        let params = serde_json::json!({"columns":{"datetime":"ts","id":"id"},
        "dtypes":{"ts":"int","id":"int","v":"int"},"upsert":true,"null_indices":false});
        let mut source = c.pipe("test", "lookup_source", None, &params.to_string());
        let rows =
            |value| PipeInput::Docs(vec![doc(vec![("ts", i(10)), ("id", i(1)), ("v", value)])]);
        ok(c.conn
            .sync_mut(&mut source, rows(i(2)), &SyncOptions::default())
            .await);
        let source_table = c.conn.pipe_table_name(&c.conn.sql_pipe(&source).unwrap());
        let mut destination_params = params;
        destination_params["sql"] = serde_json::json!(format!("SELECT * FROM {source_table}"));
        let destination = c.pipe(
            &c.conn.keys(),
            "lookup_result",
            None,
            &destination_params.to_string(),
        );
        let changes = Changes::default();
        for (index, value) in [
            i(2),
            i(2),
            i(-987654321),
            meerschaum_kernel::value::Value::Null,
            meerschaum_kernel::value::Value::Null,
            i(3),
        ]
        .into_iter()
        .enumerate()
        {
            ok(c.conn
                .sync_mut(&mut source, rows(value), &SyncOptions::default())
                .await);
            c.conn.enable_query_log();
            let result = c
                .conn
                .sync_pipe_inplace_observed(
                    &destination,
                    None,
                    Bound::None,
                    Bound::None,
                    true,
                    &changes,
                )
                .await;
            assert!(result.success, "{}", result.message);
            if index > 0
                && matches!(
                    c.flavor,
                    "sqlite" | "postgresql" | "postgis" | "timescaledb" | "timescaledb-ha"
                )
            {
                assert!(
                    result.message.contains("Upserted 1"),
                    "physical counts remain unchanged: {}",
                    result.message
                );
            }
        }
        assert_eq!(*changes.0.lock().unwrap(), vec![1, 0, 1, 1, 0, 1]);

        if matches!(
            c.flavor,
            "postgresql" | "postgis" | "timescaledb" | "timescaledb-ha"
        ) {
            let target = c
                .conn
                .pipe_table_name(&c.conn.sql_pipe(&destination).unwrap());
            let log = c.conn.take_query_log();
            let query = log
                .iter()
                .find(|q| q.contains("_mrsm_changed") && q.contains(&target))
                .expect("in-place observer query");
            let stage = query
                .split("\nFROM ")
                .nth(1)
                .unwrap()
                .split_whitespace()
                .next()
                .unwrap();
            for sql in [
                format!(
                    "INSERT INTO {target} (ts, id, v) SELECT n + 100, n + 10000, n FROM generate_series(1, 50000) n"
                ),
                format!("ANALYZE {target}"),
                format!("ANALYZE {source_table}"),
            ] {
                assert!(c.conn.exec(&sql).await.is_some());
            }
            let explain = format!(
                "EXPLAIN WITH changed_source AS (SELECT * FROM {source_table}) {}",
                query.replace(stage, "changed_source")
            );
            let df = c.conn.read(&explain).await.unwrap();
            let plan = meerschaum_connector::pipe_ops::frame_to_docs(&df)
                .into_iter()
                .flat_map(|doc| doc.into_values().map(|value| value.py_str()))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                (plan.contains("Index Scan") || plan.contains("Index Only Scan"))
                    && plan.contains("Index Cond:"),
                "a small batch must probe the conflict index instead of scanning history: {plan}"
            );
        }
        for pipe in [&destination, &source] {
            ok(c.conn.drop_pipe(pipe).await);
        }
    }
);
