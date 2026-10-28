//! Oracle internal readiness tolerates stale plans, never unrelated catalog/constraint errors.
#![cfg(feature = "oracle")]

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use meerschaum_connector::sync_metrics::{MetricsQuery, SyncMetric};
use meerschaum_connector::sync_status::{SyncOutcome, SyncSource};
use meerschaum_connector::{DataQuery, InstanceConnector};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::dtypes::get_db_type_from_pd_type;
use meerschaum_connector_sql::flavor::Flavor;
use meerschaum_connector_sql::ident::sql_item_name;
use meerschaum_connector_sql::queries::table::{
    CreateTableOptions, QueryOrDtypes, get_create_table_queries,
};
use meerschaum_connector_sql::uri::ConnectionSpec;
use meerschaum_core::{Pipe, Value};

const INSTANCE: &str = "sql:oracle_init";

async fn connector() -> Option<SqlConnector> {
    let Ok(uri) = std::env::var("MRSM_TEST_ORACLE_URI") else {
        eprintln!("Skipping owned Oracle proof: MRSM_TEST_ORACLE_URI is unset.");
        return None;
    };
    assert_eq!(std::env::var("MRSM_TEST_ISOLATED").as_deref(), Ok("1"));
    let mut spec = ConnectionSpec::from_uri(&uri, Some("oracle_init"), None).unwrap();
    assert_eq!(spec.flavor, Flavor::Oracle);
    spec.pool_size = 1;
    spec.max_overflow = 0;
    Some(SqlConnector::connect("oracle_init", spec).await.unwrap())
}

fn q(name: &str) -> String {
    sql_item_name(name, Flavor::Oracle, None)
}

async fn empty_metrics(conn: &SqlConnector) -> Pipe {
    let mut pipe = conn.engine_state_pipe(INSTANCE, "engine_metrics").unwrap();
    let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
    pipe.parameters_mut()
        .insert("target".into(), Value::Str(format!("oi_{suffix}")));
    let sp = conn.sql_pipe(&pipe).unwrap();
    let dtypes: IndexMap<_, _> = pipe.parameters()["dtypes"]
        .as_map()
        .unwrap()
        .iter()
        .map(|(name, dtype)| {
            let physical = if dtype.py_str() == "string" {
                "NVARCHAR2(255)".into()
            } else {
                get_db_type_from_pd_type(&dtype.py_str(), Some(Flavor::Oracle))
            };
            (name.clone(), physical)
        })
        .collect();
    let queries = get_create_table_queries(
        &QueryOrDtypes::Dtypes(dtypes),
        &sp.target,
        Flavor::Oracle,
        &CreateTableOptions {
            parse_dtypes: false,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(conn.exec_queries_all(&queries, true, true).await);
    pipe
}

#[tokio::test]
async fn oracle_stale_internal_plans_are_idempotent_but_ordinary_plans_are_not() {
    let Some(first) = connector().await else {
        return;
    };
    let second = connector().await.unwrap();
    let pipe = empty_metrics(&first).await;
    let first_plan = first.get_create_index_queries(&pipe).await.unwrap();
    let stale = second.get_create_index_queries(&pipe).await.unwrap();
    assert_eq!(first_plan, stale);
    assert!(first_plan.contains_key("registration_time"));
    for query in first_plan.values().flatten() {
        first.driver().exec(query).await.unwrap();
    }
    // Every plan was obtained before the first DDL; no timing/retry can hide the race.
    for query in stale.values().flatten() {
        second.driver().exec(query).await.unwrap_or_else(|error| {
            panic!("stale DDL must recognize the exact completed definition:\n{query}\n{error}")
        });
    }
    for (keys, temporary) in [("sql:synthetic", true), ("mrsm", false)] {
        let ordinary = Pipe::builder(keys, "ordinary")
            .instance(INSTANCE)
            .parameters(pipe.parameters().clone())
            .kwarg("temporary", temporary)
            .build()
            .unwrap();
        let plan = first.get_create_index_queries(&ordinary).await.unwrap();
        let datetime = plan
            .values()
            .flatten()
            .find(|s| s.starts_with("CREATE INDEX "))
            .expect("ordinary runtime renderer still emits unguarded datetime DDL");
        assert!(first.driver().exec(datetime).await.is_err());
    }
    let table = first.pipe_table_name(&first.sql_pipe(&pipe).unwrap());
    first
        .driver()
        .exec(&format!("DROP TABLE {table} PURGE"))
        .await
        .unwrap();
}

#[tokio::test]
async fn oracle_internal_guards_preserve_wrong_objects_columns_uniqueness_and_duplicate_rows() {
    let Some(conn) = connector().await else {
        return;
    };
    for collision in ["table", "columns", "uniqueness", "constraint", "duplicates"] {
        let pipe = empty_metrics(&conn).await;
        let sp = conn.sql_pipe(&pipe).unwrap();
        let table = conn.pipe_table_name(&sp);
        let names = conn.get_pipe_index_names(&pipe).unwrap();
        let plan = conn.get_create_index_queries(&pipe).await.unwrap();
        let unique_name = &names["unique"];
        let unique_queries = &plan[&q(unique_name)];
        assert_eq!(unique_queries.len(), 2);
        let query = if matches!(collision, "table" | "columns") {
            &plan["registration_time"][0]
        } else if collision == "constraint" {
            &unique_queries[1]
        } else {
            &unique_queries[0]
        };
        let other = q(&format!(
            "o_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        ));
        match collision {
            "table" => {
                conn.driver()
                    .exec(&format!(
                        "CREATE TABLE {other} (registration_id NVARCHAR2(255), ts_us NUMBER)"
                    ))
                    .await
                    .unwrap();
                conn.driver()
                    .exec(&format!(
                        "CREATE INDEX {} ON {other} (registration_id, ts_us)",
                        q(&names["registration_time"])
                    ))
                    .await
                    .unwrap();
            }
            "columns" => {
                conn.driver()
                    .exec(&format!(
                        "CREATE INDEX {} ON {table} (ts_us, registration_id)",
                        q(&names["registration_time"])
                    ))
                    .await
                    .unwrap();
            }
            "uniqueness" => {
                conn.driver()
                    .exec(&format!(
                        "CREATE INDEX {} ON {table} (ts_us, event_id)",
                        q(unique_name)
                    ))
                    .await
                    .unwrap();
            }
            "constraint" => {
                conn.driver()
                    .exec(&format!(
                        "ALTER TABLE {table} ADD CONSTRAINT {} UNIQUE (ts_us, run_id)",
                        q(&unique_name.replace("IX_", "UQ_"))
                    ))
                    .await
                    .unwrap();
            }
            "duplicates" => {
                for _ in 0..2 {
                    conn.driver()
                        .exec(&format!(
                            "INSERT INTO {table} (ts_us, event_id) VALUES (1, 'duplicate')"
                        ))
                        .await
                        .unwrap();
                }
            }
            _ => unreachable!(),
        }
        let error = conn.driver().exec(query).await.unwrap_err();
        let message = error.to_string();
        let expected = match collision {
            "constraint" => message.contains("ORA-02264") || message.contains("ORA-02261"),
            "duplicates" => message.contains("ORA-01452"),
            _ => message.contains("ORA-00955") || message.contains("ORA-01408"),
        };
        assert!(expected, "{collision}: {error}");
        if collision == "duplicates" {
            assert!(error.to_string().contains("ORA-01452"), "{error}");
            conn.driver()
                .exec(&format!("DELETE FROM {table}"))
                .await
                .unwrap();
            assert!(conn.create_pipe_indices(&pipe, None).await.success);
        }
        conn.driver()
            .exec(&format!("DROP TABLE {table} PURGE"))
            .await
            .unwrap();
        if collision == "table" {
            conn.driver()
                .exec(&format!("DROP TABLE {other} PURGE"))
                .await
                .unwrap();
        }
    }
}

fn source() -> Pipe {
    Pipe::builder("sql:synthetic", "oracle_first_batch")
        .instance(INSTANCE)
        .build()
        .unwrap()
}

struct Children(Vec<Child>);
impl Drop for Children {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oracle_independent_processes_keep_every_cold_metrics_batch() {
    let Some(conn) = connector().await else {
        return;
    };
    assert!(conn.register_pipe(&source()).await.success);
    let dir = tempfile::tempdir_in(std::env::var_os("MRSM_ROOT_DIR").unwrap()).unwrap();
    let mut children = Children(Vec::new());
    for writer in 0..4 {
        let root = dir.path().join(format!("root_{writer}"));
        let plugins = root.join("plugins");
        std::fs::create_dir_all(&plugins).unwrap();
        children.0.push(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "oracle_metrics_writer_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env_remove("TMUX")
                .env_remove("TMUX_PANE")
                .env("MRSM_ROOT_DIR", &root)
                .env("MRSM_PLUGINS_DIR", &plugins)
                .env("MRSM_ORACLE_METRICS_GATE", dir.path())
                .env("MRSM_ORACLE_METRICS_WRITER", writer.to_string())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
    }
    let deadline = Instant::now() + Duration::from_secs(45);
    while (0..4).any(|n| !dir.path().join(format!("ready_{n}")).exists()) {
        assert!(
            Instant::now() < deadline,
            "children did not reach cold start gate"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    std::fs::write(dir.path().join("go"), []).unwrap();
    while let Some(child) = children.0.pop() {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let result = conn
        .get_engine_metrics(&MetricsQuery::default())
        .await
        .unwrap();
    assert_eq!(result.summary.syncs, 4);
    assert_eq!(result.summary.rows, 4);
    assert_eq!(
        result
            .history
            .iter()
            .map(|m| &m.event_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        4
    );
    for metric in ["engine_metrics", "engine_runs", "engine_nodes"] {
        let pipe = conn.engine_state_pipe(INSTANCE, metric).unwrap();
        assert!(conn.pipe_exists(&pipe).await.unwrap());
        let table = conn.pipe_table_name(&conn.sql_pipe(&pipe).unwrap());
        conn.driver()
            .exec(&format!("DROP TABLE {table} PURGE"))
            .await
            .unwrap();
    }
    assert!(conn.delete_pipe(&source()).await.success);
}

#[tokio::test]
async fn oracle_internal_limits_keep_large_pages_and_default_lookahead() {
    let Some(conn) = connector().await else {
        return;
    };
    let pipe = source();
    assert!(conn.register_pipe(&pipe).await.success);
    assert!(conn.get_pipe_id(&pipe).await.unwrap().is_some());
    conn.initialize_engine_metrics().await.unwrap();
    let metrics: Vec<_> = (0..1005)
        .map(|_| {
            SyncMetric::from_outcome(
                &pipe,
                &SyncOutcome::new(true, "owned", SyncSource::Python, None).with_counts(1, 0, 0),
            )
        })
        .collect();
    // Match bounded writer batches; the reader must independently honor large pages.
    for batch in metrics.chunks(200) {
        conn.record_sync_metrics(batch).await.unwrap();
    }
    let default = conn
        .get_engine_metrics(&MetricsQuery::default())
        .await
        .unwrap();
    assert_eq!(default.summary.syncs, 1000);
    assert_eq!(default.history.len(), 1000);
    assert!(
        default.truncated,
        "the 1001st lookahead row must remain visible to truncation"
    );
    let complete = conn
        .get_engine_metrics(&MetricsQuery {
            limit: Some(2000),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(complete.summary.syncs, 1005);
    assert_eq!(complete.history.len(), 1005);
    assert!(!complete.truncated);
    let stored = conn.engine_state_pipe(INSTANCE, "engine_metrics").unwrap();
    let query = DataQuery {
        limit: Some(1500),
        ..Default::default()
    };
    assert!(
        conn.get_pipe_data_query(&stored, &query, &Default::default())
            .await
            .unwrap()
            .contains("WHERE ROWNUM <= 1500")
    );
    assert_eq!(
        conn.get_pipe_data(&stored, &query)
            .await
            .unwrap()
            .unwrap()
            .height(),
        1005
    );
    // Ordinary v4 SQL remains byte-for-byte in the original IN-list form.
    let ordinary = Pipe::builder("sql:synthetic", "ordinary")
        .instance(INSTANCE)
        .parameters(stored.parameters().clone())
        .build()
        .unwrap();
    let sql = conn
        .get_pipe_data_query(&ordinary, &query, &Default::default())
        .await
        .unwrap();
    assert!(
        sql.contains("WHERE ROWNUM IN (1, 2, 3, ") && sql.ends_with(", 1500)"),
        "{sql}"
    );
    assert!(conn.delete_pipe(&pipe).await.success);
    for metric in ["engine_metrics", "engine_runs", "engine_nodes"] {
        let pipe = conn.engine_state_pipe(INSTANCE, metric).unwrap();
        let table = conn.pipe_table_name(&conn.sql_pipe(&pipe).unwrap());
        conn.driver()
            .exec(&format!("DROP TABLE {table} PURGE"))
            .await
            .unwrap();
    }
}

#[tokio::test]
#[ignore = "invoked by the owned multi-process test"]
async fn oracle_metrics_writer_child() {
    let conn = connector()
        .await
        .expect("parent provides the owned Oracle URI");
    let pipe = source();
    assert!(conn.get_pipe_id(&pipe).await.unwrap().is_some());
    let metric = SyncMetric::from_outcome(
        &pipe,
        &SyncOutcome::new(true, "owned", SyncSource::Python, None).with_counts(1, 0, 0),
    );
    let writer = std::env::var("MRSM_ORACLE_METRICS_WRITER").unwrap();
    let gate = std::path::PathBuf::from(std::env::var_os("MRSM_ORACLE_METRICS_GATE").unwrap());
    std::fs::write(gate.join(format!("ready_{writer}")), []).unwrap();
    let deadline = Instant::now() + Duration::from_secs(45);
    while !gate.join("go").exists() {
        assert!(Instant::now() < deadline, "parent did not release gate");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let write = async {
        conn.initialize_engine_metrics().await.unwrap();
        conn.record_sync_metrics(&[metric]).await.unwrap();
    };
    tokio::time::timeout(Duration::from_secs(40), write)
        .await
        .unwrap();
}
