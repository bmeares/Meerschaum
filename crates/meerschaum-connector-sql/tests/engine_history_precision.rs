//! MySQL/MariaDB engine history retains microseconds on fresh and upgraded storage.

mod common;

use chrono::{TimeDelta, TimeZone, Utc};
use common::*;
use meerschaum_connector::{InstanceConnector, SyncOptions, pipe_ops::frame_to_docs};
use meerschaum_connector_sql::dtypes::get_db_type_from_pd_type;
use meerschaum_connector_sql::queries::table::{
    CreateTableOptions, QueryOrDtypes, get_create_table_queries,
};
use meerschaum_core::{Map, Value};
use meerschaum_kernel::enforce::{PipeDtypesContext, PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};

async fn history_precision(flavor: &'static str) {
    let Some(ctx) = ctx(flavor, "history_time").await else {
        return;
    };
    let base =
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + TimeDelta::microseconds(123_456);
    for legacy in [false, true] {
        for metric in ["engine_runs", "engine_nodes", "engine_sync_status"] {
            let status = metric == "engine_sync_status";
            let (identity, began, finished) = if status {
                ("pipe_ref", "last_attempt", "last_success")
            } else {
                ("run_id", "started", "finished")
            };
            let mut pipe = ctx
                .conn
                .engine_state_pipe(&ctx.conn.keys(), metric)
                .unwrap();
            pipe.parameters_mut().insert(
                "target".into(),
                // Canonical engine-state targets deliberately stay short enough that their
                // composite index names never enter v4's lossy identifier truncation.
                Value::Str(format!(
                    "h{}_{metric}_{}",
                    &ctx.prefix[6..14],
                    u8::from(legacy)
                )),
            );
            let sp = ctx.conn.sql_pipe(&pipe).unwrap();
            let table = ctx.conn.pipe_table_name(&sp);
            let dtypes: indexmap::IndexMap<String, String> = pipe.parameters()["dtypes"]
                .as_map()
                .unwrap()
                .iter()
                .map(|(name, dtype)| (name.clone(), dtype.py_str()))
                .collect();
            if legacy {
                let types = dtypes
                    .iter()
                    .map(|(name, dtype)| {
                        let physical = if matches!(name.as_str(), "run_id" | "pipe_ref") {
                            "VARCHAR(255)".into()
                        } else {
                            get_db_type_from_pd_type(dtype, Some(ctx.conn.flavor()))
                        };
                        (name.clone(), physical)
                    })
                    .collect();
                let queries = get_create_table_queries(
                    &QueryOrDtypes::Dtypes(types),
                    &sp.target,
                    ctx.conn.flavor(),
                    &CreateTableOptions {
                        parse_dtypes: false,
                        ..Default::default()
                    },
                )
                .unwrap();
                assert!(ctx.conn.exec_queries_all(&queries, true, true).await);
                if status {
                    ctx.conn.driver().exec(&format!(
                        "INSERT INTO {table} (pipe_ref, last_attempt) VALUES ('legacy-row', '2025-01-01 00:00:00')"
                    )).await.unwrap();
                } else {
                    ctx.conn.driver().exec(&format!(
                    "INSERT INTO {table} (started, run_id, {extra}) VALUES ('2025-01-01 00:00:00', 'legacy-row', {value})",
                    extra=if metric=="engine_nodes" { "pipe_ref" } else { "success" },
                    value=if metric=="engine_nodes" { "'fixture|legacy|member|None'" } else { "TRUE" },
                    )).await.unwrap();
                }
            }
            let context = PipeDtypesContext {
                explicit_dtypes: dtypes.clone(),
                dtypes,
                enforce: true,
                tz_aware: true,
                mixed_numerics: true,
            };
            // A batch above64 rows exercises the staged-upsert path as well as the final
            // target. Both tables must keep fractional timestamps.
            let docs: Vec<_> = (0..65)
                .map(|index| {
                    let started = base + TimeDelta::microseconds(index);
                    let mut row = doc(vec![
                        (identity, s(&format!("run-{index:03}"))),
                        (
                            began,
                            KValue::Datetime(DateTimeValue::utc(started.naive_utc())),
                        ),
                        (
                            finished,
                            KValue::Datetime(DateTimeValue::utc(
                                (started + TimeDelta::microseconds(234_567)).naive_utc(),
                            )),
                        ),
                    ]);
                    if metric == "engine_nodes" {
                        row.insert(
                            "pipe_ref".into(),
                            s(&format!("fixture|member-{index}|None")),
                        );
                        row.insert("actual_started".into(), row["started"].clone());
                    } else {
                        row.insert("success".into(), KValue::Bool(true));
                        if status {
                            row.insert("registration_id".into(), s("fixture-generation"));
                        }
                    }
                    row
                })
                .collect();
            let mut extra = Map::new();
            extra.insert("enforce".into(), Value::Bool(false));
            extra.insert("_check_temporary_tables".into(), Value::Bool(false));
            let options = SyncOptions {
                check_existing: true,
                extra,
                ..Default::default()
            };
            let write = |rows| pipe_enforce_dtypes(PipeInput::Docs(rows), &context).unwrap();
            let result =
                InstanceConnector::sync_pipe(&ctx.conn, &pipe, write(docs.clone()), &options).await;
            assert!(
                result.success,
                "{flavor} {metric} legacy={legacy}: {}",
                result.message
            );
            let frame = InstanceConnector::get_pipe_data(&ctx.conn, &pipe, &Default::default())
                .await
                .unwrap()
                .unwrap();
            let rows = frame_to_docs(&frame);
            assert_eq!(rows.len(), 65 + usize::from(legacy));
            for index in 0..65 {
                let row = rows
                    .iter()
                    .find(|row| row[identity].py_str() == format!("run-{index:03}"))
                    .unwrap();
                assert_eq!(
                    as_naive_utc(&row[began]),
                    Some((base + TimeDelta::microseconds(index)).naive_utc())
                );
                assert_eq!(
                    as_naive_utc(&row[finished]),
                    Some((base + TimeDelta::microseconds(index + 234_567)).naive_utc())
                );
            }
            if legacy {
                let row = rows
                    .iter()
                    .find(|row| row[identity].py_str() == "legacy-row")
                    .unwrap();
                assert_eq!(as_naive_utc(&row[began]), Some(naive(2025, 1, 1, 0, 0, 0)));
            }
            ctx.conn.enable_query_log();
            let result = InstanceConnector::sync_pipe(
                &ctx.conn,
                &pipe,
                write(vec![docs[0].clone()]),
                &options,
            )
            .await;
            assert!(result.success, "{}", result.message);
            let log = ctx.conn.take_query_log();
            assert!(
                !log.iter().any(|query| query.contains("DATETIME_PRECISION")),
                "{log:#?}"
            );
            assert!(ctx.conn.drop_pipe(&pipe).await.success);
        }
    }
    // The public dtype contract deliberately remains compatible with v4.
    assert_eq!(
        get_db_type_from_pd_type("datetime", Some(ctx.conn.flavor())),
        "DATETIME"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_history_retains_microseconds_after_upgrade_and_staged_writes() {
    history_precision("mysql").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mariadb_history_retains_microseconds_after_upgrade_and_staged_writes() {
    history_precision("mariadb").await;
}
