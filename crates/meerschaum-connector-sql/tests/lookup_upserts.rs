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

flavor_tests!(
    integer_axis_observation_uses_destination_assignment_bounds,
    |c: Ctx| async move {
        if !matches!(
            c.flavor,
            "postgresql" | "postgis" | "timescaledb" | "timescaledb-ha"
        ) {
            return;
        }
        let mut source = c.pipe(
            "test",
            "cast_source",
            None,
            r#"{
            "columns":{"id":"id"},"dtypes":{"id":"int","ts":"float","v":"int"},
            "upsert":true,"null_indices":false
        }"#,
        );
        let rows = |time, value| {
            PipeInput::Docs(vec![doc(vec![
                ("id", i(1)),
                ("ts", fl(time)),
                ("v", i(value)),
            ])])
        };
        ok(c.conn
            .sync_mut(&mut source, rows(1.6, 10), &SyncOptions::default())
            .await);
        let source_table = c.conn.pipe_table_name(&c.conn.sql_pipe(&source).unwrap());
        let params = serde_json::json!({
            "columns":{"datetime":"ts","id":"id"},
            "dtypes":{"ts":"int","id":"int","v":"int"},
            "static":true,"upsert":true,"null_indices":false,
            "sql":format!("SELECT * FROM {source_table}"),
        });
        let mut target = c.pipe(&c.conn.keys(), "cast_target", None, &params.to_string());
        ok(c.conn
            .sync_mut(
                &mut target,
                PipeInput::Docs(vec![doc(vec![("id", i(1)), ("ts", i(2)), ("v", i(10))])]),
                &SyncOptions::default(),
            )
            .await);
        let changes = Changes::default();
        for (time, value) in [(1.6, 10), (1.6, 12), (2.6, 12), (2.6, 12)] {
            ok(c.conn
                .sync_mut(&mut source, rows(time, value), &SyncOptions::default())
                .await);
            ok(c.conn
                .sync_pipe_inplace_observed(&target, None, Bound::None, Bound::None, true, &changes)
                .await);
        }
        let data = c.docs(&target).await;
        for pipe in [&target, &source] {
            ok(c.conn.drop_pipe(pipe).await);
        }
        assert_eq!(*changes.0.lock().unwrap(), vec![0, 1, 1, 0]);
        assert_eq!(data.len(), 2);
        assert!(data.iter().all(|row| row["v"] == i(12)));
    }
);

flavor_tests!(
    unchanged_upserts_prune_compressed_history,
    |c: Ctx| async move {
        if !matches!(
            c.flavor,
            "postgresql" | "postgis" | "timescaledb" | "timescaledb-ha"
        ) || c
            .conn
            .value("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')")
            .await
            != Some(meerschaum_connector_sql::driver::SqlValue::Bool(true))
        {
            return;
        }
        let params = serde_json::json!({
            "columns":{"datetime":"ts","id":"id"},
            "dtypes":{"ts":"datetime64[ms, UTC]","id":"int","v":"int"},
            "upsert":true,"null_indices":false,
        });
        let mut source = c.pipe("test", "recent_source", None, &params.to_string());
        ok(c.conn
            .sync_mut(
                &mut source,
                PipeInput::Docs(vec![doc(vec![
                    ("ts", udt(2026, 1, 1, 0, 0)),
                    ("id", i(1)),
                    ("v", i(2)),
                ])]),
                &SyncOptions::default(),
            )
            .await);
        let source_table = c.conn.pipe_table_name(&c.conn.sql_pipe(&source).unwrap());
        assert!(c.conn.exec(&format!(
            "INSERT INTO {source_table} (ts, id, v) SELECT TIMESTAMPTZ '2026-01-01' + n * INTERVAL '1 minute', 1, 2 FROM generate_series(1, 151) n"
        )).await.is_some());
        let mut target_params = params;
        target_params["sql"] = serde_json::json!(format!("SELECT * FROM {source_table}"));
        let target = c.pipe(
            &c.conn.keys(),
            "recent_target",
            None,
            &target_params.to_string(),
        );
        ok(c.conn
            .sync_pipe_inplace(&target, None, Bound::None, Bound::None, true)
            .await);
        let target_table = c.conn.pipe_table_name(&c.conn.sql_pipe(&target).unwrap());
        for sql in [
            format!(
                "SELECT create_hypertable('{target_table}'::regclass, 'ts', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE, migrate_data => TRUE)"
            ),
            format!(
                "ALTER TABLE {target_table} SET (timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'ts')"
            ),
            format!(
                "INSERT INTO {target_table} (ts, id, v) SELECT make_timestamptz(y, 1, 1, 0, 0, 0, 'UTC') + n * INTERVAL '1 second', n % 10, n FROM generate_series(2022, 2024) y CROSS JOIN generate_series(1, 3000) n"
            ),
            format!(
                "SELECT compress_chunk(chunk) FROM show_chunks('{target_table}'::regclass, older_than => TIMESTAMPTZ '2025-01-01') chunk"
            ),
            format!("ANALYZE {target_table}"),
            format!("ANALYZE {source_table}"),
        ] {
            assert!(c.conn.exec(&sql).await.is_some(), "{sql}");
        }
        let target_name = c.conn.sql_pipe(&target).unwrap().target;
        assert!(matches!(c.conn.value(&format!(
            "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = 'public' AND hypertable_name = '{target_name}' AND is_compressed"
        )).await, Some(meerschaum_connector_sql::driver::SqlValue::Int(n)) if n >= 3));
        let changes = Changes::default();
        c.conn.enable_query_log();
        let result = c
            .conn
            .sync_pipe_inplace_observed(&target, None, Bound::None, Bound::None, true, &changes)
            .await;
        let log = c.conn.take_query_log();
        let query = log
            .iter()
            .find(|q| q.contains("_mrsm_changed") && q.contains(&target_table))
            .expect("in-place observer query");
        let stage = query
            .split("\nFROM ")
            .nth(1)
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap();
        let staged = meerschaum_connector_sql::ident::sql_item_name(
            &format!("{}_staged", c.prefix),
            c.conn.flavor(),
            Some("public"),
        );
        // In-place staging tables have not been ANALYZEd when the observer reads them.
        assert!(
            c.conn
                .exec(&format!(
                    "CREATE TABLE {staged} AS SELECT * FROM {source_table}"
                ))
                .await
                .is_some()
        );
        let explain = format!("EXPLAIN {}", query.replace(stage, &staged));
        let plan = c.conn.read(&explain).await.map(|frame| {
            meerschaum_connector::pipe_ops::frame_to_docs(&frame)
                .into_iter()
                .flat_map(|doc| doc.into_values().map(|v| v.py_str()))
                .collect::<Vec<_>>()
                .join("\n")
        });
        assert!(c.conn.exec(&format!("DROP TABLE {staged}")).await.is_some());
        for pipe in [&target, &source] {
            ok(c.conn.drop_pipe(pipe).await);
        }
        ok(result);
        assert_eq!(*changes.0.lock().unwrap(), vec![0]);
        let plan = plan.expect("observer plan");
        assert!(
            !plan.contains("DecompressChunk") && !plan.contains("ColumnarScan"),
            "A recent unchanged batch must exclude older compressed history: {plan}"
        );
    }
);

#[meerschaum_connector::async_trait]
impl InplaceObserver for Changes {
    async fn before_merge(&self, delta: &Frame) -> Result<(), String> {
        self.0.lock().unwrap().push(delta.height());
        Ok(())
    }
}

flavor_tests!(
    numeric_storage_survives_floating_sql_upserts,
    |c: Ctx| async move {
        if !matches!(
            c.flavor,
            "postgresql" | "postgis" | "timescaledb" | "timescaledb-ha"
        ) {
            return;
        }
        let params = serde_json::json!({
            "columns":{"id":"id"},"dtypes":{"id":"int","v":"float"},
            "upsert":true,"null_indices":false,
        });
        let rows = |value| PipeInput::Docs(vec![doc(vec![("id", i(1)), ("v", fl(value))])]);
        let mut source = c.pipe("test", "numeric_source", None, &params.to_string());
        ok(c.conn
            .sync_mut(&mut source, rows(1.25), &SyncOptions::default())
            .await);
        let source_table = c.conn.pipe_table_name(&c.conn.sql_pipe(&source).unwrap());
        let mut target_params = params;
        target_params["sql"] = serde_json::json!(format!("SELECT * FROM {source_table}"));
        let mut destination = c.pipe(
            &c.conn.keys(),
            "numeric_target",
            None,
            &target_params.to_string(),
        );
        ok(c.conn
            .sync_mut(&mut destination, rows(1.25), &SyncOptions::default())
            .await);
        let sp = c.conn.sql_pipe(&destination).unwrap();
        let target = c.conn.pipe_table_name(&sp);
        assert!(
            c.conn
                .exec(&format!(
                    "ALTER TABLE {target} ALTER COLUMN v TYPE NUMERIC USING v::numeric"
                ))
                .await
                .is_some()
        );
        let changes = Changes::default();
        let mut stored_types = Vec::new();
        for value in [1.25, 2.5, 2.5] {
            ok(c.conn
                .sync_mut(&mut source, rows(value), &SyncOptions::default())
                .await);
            ok(c.conn
                .sync_pipe_inplace_observed(
                    &destination,
                    None,
                    Bound::None,
                    Bound::None,
                    true,
                    &changes,
                )
                .await);
            let types = c
                .conn
                .get_table_cols_types(&sp.target, Some("public"))
                .await;
            stored_types.push(types["v"].to_uppercase());
            assert_eq!(
                c.conn
                    .value(&format!("SELECT v::double precision FROM {target}"))
                    .await,
                Some(meerschaum_connector_sql::driver::SqlValue::Float(value))
            );
        }
        // A real change to textual data must still evolve a dynamic schema.
        assert!(
            c.conn
                .exec(&format!(
                    "ALTER TABLE {source_table} ALTER COLUMN v TYPE TEXT USING v::text"
                ))
                .await
                .is_some()
        );
        assert!(
            c.conn
                .exec(&format!("UPDATE {source_table} SET v = 'textual value'"))
                .await
                .is_some()
        );
        ok(c.conn
            .sync_pipe_inplace_observed(
                &destination,
                None,
                Bound::None,
                Bound::None,
                true,
                &changes,
            )
            .await);
        let textual = c.conn.value(&format!("SELECT v FROM {target}")).await;
        for pipe in [&destination, &source] {
            ok(c.conn.drop_pipe(pipe).await);
        }
        assert_eq!(
            stored_types,
            vec!["NUMERIC"; 3],
            "Compatible numeric input must not rewrite the destination schema."
        );
        assert_eq!(*changes.0.lock().unwrap(), vec![0, 1, 0, 1]);
        assert_eq!(
            textual,
            Some(meerschaum_connector_sql::driver::SqlValue::Text(
                "textual value".into()
            ))
        );
    }
);

flavor_tests!(
    all_null_upserts_do_not_invalidate_dependents,
    |c: Ctx| async move {
        if !meerschaum_connector_sql::queries::templates::has_update_query(&format!(
            "{}-upsert",
            c.flavor
        )) {
            return;
        }
        let mut source = c.pipe("test", "null_source", None,
        r#"{"columns":{"id":"id"},"dtypes":{"id":"int","v":"int"},"upsert":true,"null_indices":true}"#);
        ok(c.conn
            .sync_mut(
                &mut source,
                PipeInput::Docs(vec![doc(vec![
                    ("id", meerschaum_kernel::value::Value::Null),
                    ("v", meerschaum_kernel::value::Value::Null),
                ])]),
                &SyncOptions::default(),
            )
            .await);
        let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&source).unwrap());
        let destination = c.pipe(
            &c.conn.keys(),
            "null_result",
            None,
            &serde_json::json!({
                "columns":{"id":"id"},"dtypes":{"id":"int","v":"int"},
                "upsert":true,"null_indices":true,"sql":format!("SELECT * FROM {table}")
            })
            .to_string(),
        );
        let changes = Changes::default();
        for _ in 0..2 {
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
        }
        assert_eq!(*changes.0.lock().unwrap(), vec![1, 0]);
        assert_eq!(c.rowcount(&destination, Default::default()).await, Some(1));
        for pipe in [&destination, &source] {
            ok(c.conn.drop_pipe(pipe).await);
        }
    }
);

flavor_tests!(upserts_compare_stored_json_values, |c: Ctx| async move {
    if !matches!(
        c.flavor,
        "postgresql" | "postgis" | "timescaledb" | "timescaledb-ha"
    ) {
        return;
    }
    for (stored_type, is_static) in ["TEXT", "JSON"]
        .into_iter()
        .flat_map(|kind| [false, true].map(move |fixed| (kind, fixed)))
    {
        let source_params = serde_json::json!({
            "columns":{"datetime":"ts","id":"id"},
            "dtypes":{"ts":"int","id":"int","v":"json"},
            "upsert":true,"null_indices":false,
        });
        let rows =
            |value| PipeInput::Docs(vec![doc(vec![("ts", i(10)), ("id", i(1)), ("v", value)])]);
        let mut source = c.pipe(
            "test",
            &format!("json_source_{stored_type}_{is_static}"),
            None,
            &source_params.to_string(),
        );
        ok(c.conn
            .sync_mut(
                &mut source,
                rows(j(r#"{"value":2}"#)),
                &SyncOptions::default(),
            )
            .await);
        let source_table = c.conn.pipe_table_name(&c.conn.sql_pipe(&source).unwrap());
        let mut target_params = source_params;
        target_params["static"] = serde_json::json!(is_static);
        target_params["dtypes"]["v"] = serde_json::json!(if stored_type == "TEXT" {
            "string"
        } else {
            "json"
        });
        target_params["sql"] = serde_json::json!(format!("SELECT * FROM {source_table}"));
        let mut destination = c.pipe(
            &c.conn.keys(),
            &format!("json_target_{stored_type}_{is_static}"),
            None,
            &target_params.to_string(),
        );
        ok(c.conn
            .sync_mut(
                &mut destination,
                rows(meerschaum_kernel::value::Value::Null),
                &SyncOptions::default(),
            )
            .await);
        let target = c
            .conn
            .pipe_table_name(&c.conn.sql_pipe(&destination).unwrap());
        if stored_type == "JSON" {
            assert!(
                c.conn
                    .exec(&format!(
                        "ALTER TABLE {target} ALTER COLUMN v TYPE JSON USING v::json"
                    ))
                    .await
                    .is_some()
            );
        }
        let changes = Changes::default();
        for value in [
            j(r#"{"value":2}"#),
            j(r#"{"value":2}"#),
            j(r#"{"value":3}"#),
            meerschaum_kernel::value::Value::Null,
            meerschaum_kernel::value::Value::Null,
        ] {
            ok(c.conn
                .sync_mut(&mut source, rows(value.clone()), &SyncOptions::default())
                .await);
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
            assert!(result.success, "{stored_type}: {}", result.message);
            let data = c.docs(&destination).await;
            let actual = &data[0]["v"];
            match (&value, actual) {
                (meerschaum_kernel::value::Value::Null, meerschaum_kernel::value::Value::Null) => {}
                (
                    meerschaum_kernel::value::Value::Json(expected),
                    meerschaum_kernel::value::Value::Str(actual),
                ) => assert_eq!(
                    &serde_json::from_str::<serde_json::Value>(actual).unwrap(),
                    &expected.to_json()
                ),
                _ => assert_eq!(actual, &value),
            }
        }
        assert_eq!(*changes.0.lock().unwrap(), vec![1, 0, 1, 1, 0]);
        for pipe in [&destination, &source] {
            ok(c.conn.drop_pipe(pipe).await);
        }
    }
});

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_upserts_compare_numeric_values_after_text_affinity() {
    let c = ctx("sqlite", "affinity").await.unwrap();
    let mut source = c.pipe("test", "affinity_source", None,
        r#"{"columns":{"id":"id"},"dtypes":{"id":"int","v":"float"},"upsert":true,"null_indices":false}"#);
    ok(c.conn
        .sync_mut(
            &mut source,
            PipeInput::Docs(vec![doc(vec![("id", i(1)), ("v", fl(1.0))])]),
            &SyncOptions::default(),
        )
        .await);
    let source_table = c.conn.pipe_table_name(&c.conn.sql_pipe(&source).unwrap());
    let mut destination = c.pipe(
        &c.conn.keys(),
        "affinity_result",
        None,
        &serde_json::json!({
            "columns":{"id":"id"},"dtypes":{"id":"int","v":"string"},
            "upsert":true,"null_indices":false,"sql":format!("SELECT * FROM {source_table}")
        })
        .to_string(),
    );
    ok(c.conn
        .sync_mut(
            &mut destination,
            PipeInput::Docs(vec![doc(vec![("id", i(1)), ("v", s("1.00"))])]),
            &SyncOptions::default(),
        )
        .await);
    let changes = Changes::default();
    for _ in 0..2 {
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
    }
    assert_eq!(*changes.0.lock().unwrap(), vec![1, 0]);
    let target = c
        .conn
        .pipe_table_name(&c.conn.sql_pipe(&destination).unwrap());
    assert_eq!(
        c.conn.value(&format!("SELECT v FROM {target}")).await,
        Some(meerschaum_connector_sql::driver::SqlValue::Text(
            "1.0".into()
        ))
    );
    for pipe in [&destination, &source] {
        ok(c.conn.drop_pipe(pipe).await);
    }
}

flavor_tests!(
    unchanged_upserts_do_not_invalidate_dependents,
    |c: Ctx| async move {
        // DuckDB has no native in-place UPSERT. Its ordinary diff path deliberately keeps
        // v4's sentinel normalization; it cannot satisfy this UPSERT-specific contract.
        if !meerschaum_connector_sql::queries::templates::has_update_query(&format!(
            "{}-upsert",
            c.flavor
        )) {
            eprintln!(
                "Skipping {}: native in-place UPSERT is unsupported.",
                c.flavor
            );
            return;
        }
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
        let target = c
            .conn
            .pipe_table_name(&c.conn.sql_pipe(&destination).unwrap());
        let value_column =
            meerschaum_connector_sql::ident::sql_item_name("v", c.conn.flavor(), None);
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
            assert_eq!(
                c.conn
                    .value(&format!("SELECT {value_column} FROM {target}"))
                    .await,
                c.conn
                    .value(&format!("SELECT {value_column} FROM {source_table}"))
                    .await,
                "the native UPSERT must preserve the actual value at step {index}"
            );
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
