//! Engine state lives in the internal schema (§5.8, GAP-001).
//!
//! (a) A graph run on a schema flavor (PostgreSQL, TimescaleDB, MSSQL) writes every
//! `mrsm_engine_*` table into `_mrsm_internal`, suffixed with the instance schema, and nothing
//! named `mrsm_engine_*` into the instance schema; the tables keep their native partitioning
//! (PostgreSQL) or hypertables (TimescaleDB). Schema-less flavors (MySQL, MariaDB) keep the
//! tables beside the instance tables.
//!
//! (b) Two connectors with the same label on one database, whose schemas differ, run pipelines
//! with identical pipe keys: their runs, ledger entries and cursors stay separate.
//!
//! Gated on `MRSM_TEST_PG_URI`, `MRSM_TEST_TIMESCALE_URI`, `MRSM_TEST_MSSQL_URI`,
//! `MRSM_TEST_MYSQL_URI` and `MRSM_TEST_MARIADB_URI`. Schemas are `w17bg1_<tag>` and dropped
//! at the end, together with the namespaced engine tables.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{json_map, temp_dir};
use meerschaum_connector::{DataQuery, InstanceConnector};
use meerschaum_connector_sql::driver::SqlValue;
use meerschaum_connector_sql::instance::engine_state_namespace;
use meerschaum_connector_sql::queries::table::get_create_schema_if_not_exists_queries;
use meerschaum_connector_sql::uri::ConnectionSpec;
use meerschaum_connector_sql::{Flavor, SqlConnector};
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_engine::instance_state::bound_from_json;
use meerschaum_engine::runs::EnginePipe;
use meerschaum_engine::{
    Depth, Engine, EngineConfig, NodeStatus, PipeRef, RunOptions, RunResult, Selection,
    StateSource, StaticRegistry,
};

const LABEL: &str = "w17bg1";
const KEYS: &str = "sql:w17bg1";
const INTERNAL: &str = "_mrsm_internal";

fn uri(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|u| !u.is_empty())
}

async fn connect(uri: &str, schema: Option<&str>) -> Arc<SqlConnector> {
    let mut spec = ConnectionSpec::from_uri(uri, Some(LABEL), None).expect("valid URI");
    spec.schema = schema.map(str::to_string);
    Arc::new(SqlConnector::connect(LABEL, spec).await.expect("connects"))
}

fn registry(conn: &Arc<SqlConnector>) -> Arc<StaticRegistry> {
    let instance: Arc<dyn InstanceConnector> = conn.clone();
    let source: Arc<dyn meerschaum_connector::Connector> = conn.clone();
    let sql = conn.clone();
    Arc::new(
        StaticRegistry::new()
            .with_instance(KEYS, instance)
            .with_source(KEYS, source)
            .with_table_reference(move |pipe: &Pipe| {
                let sp = sql.sql_pipe(pipe).map_err(|e| e.to_string())?;
                Ok(sql.pipe_table_name(&sp))
            }),
    )
}

fn quoted(flavor: Flavor, schema: Option<&str>, table: &str) -> String {
    meerschaum_connector_sql::ident::sql_item_name(table, flavor, schema)
}

async fn count(conn: &SqlConnector, sql: &str) -> i64 {
    match conn.driver().value(sql).await.expect(sql) {
        Some(SqlValue::Int(i)) => i,
        Some(SqlValue::UInt(u)) => u as i64,
        Some(SqlValue::Decimal(d)) => d.to_string().parse().unwrap(),
        Some(SqlValue::Text(s)) => s.parse().unwrap(),
        other => panic!("{sql}: {other:?}"),
    }
}

/// The names of the `mrsm_engine_*` tables in `schema` (`None`: the connection's database).
async fn engine_tables(conn: &SqlConnector, schema: Option<&str>) -> Vec<String> {
    let filter = match schema {
        Some(s) => format!("table_schema = '{s}'"),
        None => "table_schema = DATABASE()".to_string(),
    };
    let sql = format!(
        "SELECT table_name FROM information_schema.tables WHERE {filter} AND table_name LIKE 'mrsm!_engine!_%' ESCAPE '!'"
    );
    let df = conn.driver().read_frame(&sql).await.expect(&sql);
    let mut names: Vec<String> = meerschaum_connector::pipe_ops::frame_to_docs(&df)
        .into_iter()
        .filter_map(|d| d.into_values().next().map(|v| v.py_str()))
        .collect();
    names.sort();
    names
}

/// One instance (a connector in `schema`) with a source table of `hours` hourly rows on `day`
/// and the pipes
/// `raw` (reads the source) and `clean` (reads `raw` through `{{ Pipe() }}`).
struct Chain {
    conn: Arc<SqlConnector>,
    schema: Option<String>,
    src: String,
    pipes: Vec<Pipe>,
    raw: PipeRef,
    clean: PipeRef,
    dir: std::path::PathBuf,
}

impl Chain {
    async fn new(uri: &str, schema: Option<&str>, tag: &str, day: &str, hours: u32) -> Chain {
        let conn = connect(uri, schema).await;
        let flavor = conn.flavor();
        if let Some(s) = schema {
            for q in get_create_schema_if_not_exists_queries(Some(s), flavor).unwrap() {
                let _ = conn.driver().exec(&q).await;
            }
        }
        let src = quoted(flavor, conn.schema(), &format!("w17bg1_src_{tag}"));
        let ts = if flavor == Flavor::Mssql {
            "DATETIME2"
        } else {
            "TIMESTAMP"
        };
        let float = if flavor == Flavor::Mssql {
            "FLOAT"
        } else {
            "DOUBLE PRECISION"
        };
        conn.driver()
            .exec(&format!(
                "CREATE TABLE {src} (ts {ts} NULL, id INTEGER NULL, v {float} NULL)"
            ))
            .await
            .expect("create source");
        let values: Vec<String> = (0..hours)
            .map(|h| format!("('{day} {h:02}:00:00', {}, {h}.5)", h % 2))
            .collect();
        conn.driver()
            .exec(&format!(
                "INSERT INTO {src} (ts, id, v) VALUES {}",
                values.join(", ")
            ))
            .await
            .expect("insert source");
        let params = |sql: String| {
            json_map(&serde_json::json!({
                "sql": sql,
                "columns": {"datetime": "ts", "id": "id"},
                "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
            }))
        };
        // Identical keys on every instance of the test.
        let (raw, clean) = (format!("w17bg1_raw_{tag}"), format!("w17bg1_clean_{tag}"));
        let raw_pipe = Pipe::builder(KEYS, &raw)
            .instance(KEYS)
            .parameters(params(format!("SELECT ts, id, v FROM {src}")))
            .build()
            .unwrap();
        let clean_pipe = Pipe::builder(KEYS, &clean)
            .instance(KEYS)
            .parameters(params(format!(
                "SELECT ts, id, v FROM {{{{ Pipe('{KEYS}', '{raw}', instance='{KEYS}') }}}}"
            )))
            .build()
            .unwrap();
        for p in [&raw_pipe, &clean_pipe] {
            let res = conn.register_pipe(p).await;
            assert!(res.success, "{}", res.message);
        }
        Chain {
            schema: schema.map(str::to_string),
            src,
            pipes: vec![raw_pipe, clean_pipe],
            raw: PipeRef::new(KEYS, KEYS, &raw, None),
            clean: PipeRef::new(KEYS, KEYS, &clean, None),
            dir: temp_dir(&format!("w17bg1_{tag}")),
            conn,
        }
    }

    fn engine(&self) -> Engine {
        Engine::new(
            registry(&self.conn),
            EngineConfig {
                home_instance: KEYS.into(),
                lease_dir: self.dir.join("leases"),
                lease_ttl: Duration::from_secs(10),
                lease_poll: Duration::from_millis(50),
                state_flush: Duration::from_millis(20),
                holder: format!("w17bg1:{}", std::process::id()),
                ..EngineConfig::default()
            },
        )
    }

    async fn run(&self) -> RunResult {
        let result = self
            .engine()
            .run(RunOptions {
                selection: Some(
                    Selection::refs([self.clean.clone()]).with_parents(Some(Depth::All)),
                ),
                ..Default::default()
            })
            .await;
        assert!(result.success, "{}", result.message);
        result
    }

    /// [`Chain::run`] where every node syncs (the first run).
    async fn first_run(&self) -> RunResult {
        let result = self.run().await;
        let record = result.record.as_ref().unwrap();
        assert!(
            record
                .nodes
                .iter()
                .all(|n| n.status == NodeStatus::Succeeded),
            "{:?}",
            record.nodes
        );
        result
    }

    /// Drop the pipes, the source, every engine table of this instance and its schema.
    async fn cleanup(self, run_ids: &[String]) {
        let conn = self.conn.clone();
        for p in &self.pipes {
            let _ = InstanceConnector::drop_pipe(conn.as_ref(), p).await;
            let _ = InstanceConnector::delete_pipe(conn.as_ref(), p).await;
        }
        let _ = conn
            .driver()
            .exec(&format!("DROP TABLE IF EXISTS {}", self.src))
            .await;
        if conn.flavor().no_schema() {
            // Shared tables beside the instance tables: remove only this run's rows.
            let state = self.engine().state().clone();
            for run_id in run_ids {
                let mut params = Map::new();
                params.insert("run_id".into(), Value::Str(run_id.clone()));
                for kind in [EnginePipe::Runs, EnginePipe::Nodes, EnginePipe::Events] {
                    let _ = state.clear_rows(KEYS, kind, params.clone(), None).await;
                }
            }
        } else {
            for kind in EnginePipe::ALL {
                if let Ok(Some(p)) =
                    InstanceConnector::engine_state_pipe(conn.as_ref(), KEYS, kind.metric_key())
                        .await
                {
                    let _ = InstanceConnector::drop_pipe(conn.as_ref(), &p).await;
                }
            }
            if let Some(s) = &self.schema {
                let sql = if conn.flavor() == Flavor::Mssql {
                    format!("DROP SCHEMA IF EXISTS [{s}]")
                } else {
                    format!("DROP SCHEMA IF EXISTS \"{s}\" CASCADE")
                };
                let _ = conn.driver().exec(&sql).await;
            }
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The run's ledger entries of `raw`, as the latest end bound.
async fn ledger_extent(chain: &Chain) -> Option<meerschaum_core::success::Bound> {
    let state = chain.engine().state().clone();
    let mut params = Map::new();
    params.insert(
        "pipe_ref".into(),
        Value::Str(chain.raw.as_str().to_string()),
    );
    let docs = state
        .read_rows(KEYS, EnginePipe::Ledger, params, None)
        .await
        .unwrap();
    assert!(!docs.is_empty(), "no ledger entries for {}", chain.raw);
    docs.iter()
        .filter_map(|d| match d.get("end")? {
            meerschaum_kernel::value::Value::Json(j) => bound_from_json(j),
            meerschaum_kernel::value::Value::Str(s) => {
                bound_from_json(&serde_json::from_str(s).ok()?)
            }
            _ => None,
        })
        .max_by_key(|b| format!("{b:?}"))
}

/// (a) + (b) on one schema flavor.
async fn schema_flavor(var: &str) {
    let Some(uri) = uri(var) else {
        eprintln!("skipped: {var} is not set");
        return;
    };
    let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let (schema_a, schema_b) = (
        format!("w17bg1_{tag}"),
        format!("w17bg1_{tag}_hashed_namespace"),
    );
    // Different days, so each instance's ledger ranges are recognizable.
    let a = Chain::new(&uri, Some(&schema_a), &tag, "2026-01-01", 6).await;
    let b = Chain::new(&uri, Some(&schema_b), &tag, "2026-06-01", 3).await;
    let flavor = a.conn.flavor();
    let (ns_a, ns_b) = (
        engine_state_namespace(flavor, Some(&schema_a)).unwrap(),
        engine_state_namespace(flavor, Some(&schema_b)).unwrap(),
    );
    assert_eq!(ns_a, schema_a);
    assert_eq!(ns_b.len(), 16);

    let run_a = a.first_run().await;
    let run_b = b.first_run().await;

    // (a) Placement: every table in `_mrsm_internal`, none in the instance schemas.
    for (chain, schema, ns) in [(&a, &schema_a, &ns_a), (&b, &schema_b, &ns_b)] {
        assert!(
            engine_tables(&chain.conn, Some(schema)).await.is_empty(),
            "engine tables in the instance schema {schema}"
        );
        let internal = engine_tables(&chain.conn, Some(INTERNAL)).await;
        for kind in [
            EnginePipe::State,
            EnginePipe::Ledger,
            EnginePipe::Cursors,
            EnginePipe::Runs,
            EnginePipe::Nodes,
        ] {
            let name = format!("mrsm_{}_{ns}", kind.metric_key());
            assert!(internal.contains(&name), "{name} not in {internal:?}");
        }
    }
    // Native partitioning and hypertables carry over (as in the instance schema before).
    match flavor {
        Flavor::Timescaledb | Flavor::TimescaledbHa => {
            for kind in ["engine_runs", "engine_nodes"] {
                let n = count(
                    &a.conn,
                    &format!(
                        "SELECT COUNT(*) FROM timescaledb_information.hypertables WHERE hypertable_schema = '{INTERNAL}' AND hypertable_name = 'mrsm_{kind}_{ns_a}'"
                    ),
                )
                .await;
                assert_eq!(n, 1, "mrsm_{kind}_{ns_a} is a hypertable");
            }
        }
        Flavor::Postgresql | Flavor::Postgis => {
            for kind in ["engine_runs", "engine_nodes", "engine_ledger"] {
                let n = count(
                    &a.conn,
                    &format!(
                        "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '{INTERNAL}' AND c.relname = 'mrsm_{kind}_{ns_a}' AND c.relkind = 'p'"
                    ),
                )
                .await;
                assert_eq!(n, 1, "mrsm_{kind}_{ns_a} is partitioned");
            }
        }
        _ => {}
    }

    // (b) Separate state: each instance sees only its own run, its own cursor and its own
    // ledger extent (January vs June), though the pipe keys and labels are identical.
    let state_a = a.engine().state().clone();
    let state_b = b.engine().state().clone();
    let runs_a: Vec<String> = state_a
        .recent_runs(50)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.run_id)
        .collect();
    let runs_b: Vec<String> = state_b
        .recent_runs(50)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.run_id)
        .collect();
    let (id_a, id_b) = (
        run_a.run_id().unwrap().to_string(),
        run_b.run_id().unwrap().to_string(),
    );
    assert_eq!(runs_a, vec![id_a.clone()]);
    assert_eq!(runs_b, vec![id_b.clone()]);
    assert!(state_a.load_run(&id_b).await.unwrap().is_none());
    let cursor_a = state_a
        .cursor(KEYS, &a.raw, &a.clean)
        .await
        .unwrap()
        .unwrap();
    let cursor_b = state_b
        .cursor(KEYS, &b.raw, &b.clean)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        cursor_a.parent_watermark_at_cursor, cursor_b.parent_watermark_at_cursor,
        "each cursor follows its own parent"
    );
    let (ext_a, ext_b) = (ledger_extent(&a).await, ledger_extent(&b).await);
    assert!(ext_a.is_some() && ext_b.is_some());
    assert_ne!(ext_a, ext_b, "each ledger holds its own instance's ranges");
    let rows = |c: &Chain| {
        let conn = c.conn.clone();
        let pipe = c.pipes[1].clone();
        async move {
            conn.get_pipe_rowcount(&pipe, &DataQuery::default())
                .await
                .unwrap()
        }
    };
    assert_eq!(rows(&a).await, Some(6));
    assert_eq!(rows(&b).await, Some(3));

    // A second run on A: its child is caught up (A's cursor), B is untouched.
    let again = a.run().await;
    assert_eq!(
        again
            .record
            .as_ref()
            .unwrap()
            .node(&a.clean)
            .unwrap()
            .status,
        NodeStatus::Skipped
    );
    assert_eq!(state_b.recent_runs(50).await.unwrap().len(), 1);

    a.cleanup(&[id_a]).await;
    b.cleanup(&[id_b]).await;
}

/// (a) on a schema-less flavor: the tables stay in the connection's database.
async fn schemaless_flavor(var: &str) {
    let Some(uri) = uri(var) else {
        eprintln!("skipped: {var} is not set");
        return;
    };
    let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let chain = Chain::new(&uri, None, &tag, "2026-01-01", 4).await;
    let result = chain.first_run().await;
    let tables = engine_tables(&chain.conn, None).await;
    for kind in [
        "engine_state",
        "engine_ledger",
        "engine_cursors",
        "engine_runs",
        "engine_nodes",
    ] {
        assert!(
            tables.contains(&format!("mrsm_{kind}")),
            "mrsm_{kind} not in {tables:?}"
        );
    }
    assert!(
        !tables.iter().any(|t| t.contains(&tag)),
        "no namespaced tables on a schema-less flavor: {tables:?}"
    );
    let id = result.run_id().unwrap().to_string();
    let stored = chain
        .engine()
        .state()
        .load_run(&id)
        .await
        .unwrap()
        .expect("the run row was persisted");
    assert_eq!(
        stored.nodes.len(),
        2,
        "{:?} {}",
        stored.nodes,
        result.message
    );
    // Forget this run's pipe state, then its run rows.
    for p in &chain.pipes {
        let _ = meerschaum_connector::pipe_ops::forget_engine_state(chain.conn.as_ref(), p).await;
    }
    chain.cleanup(&[id]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgresql_state_in_internal_schema() {
    schema_flavor("MRSM_TEST_PG_URI").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timescaledb_state_in_internal_schema() {
    schema_flavor("MRSM_TEST_TIMESCALE_URI").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mssql_state_in_internal_schema() {
    schema_flavor("MRSM_TEST_MSSQL_URI").await;
}

const LATIN1_DB: &str = "w17bg1_latin1";

/// A MySQL database whose default charset is latin1 (MySQL 5.7's default): the engine's state
/// tables are created beside the instance tables with utf8mb4 text columns, so planner reasons
/// (`−`, `→`) and sync messages with emoji are recorded and read back unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_latin1_database_keeps_unicode_state() {
    let Some(uri) = uri("MRSM_TEST_MYSQL_URI") else {
        eprintln!("skipped: MRSM_TEST_MYSQL_URI is not set");
        return;
    };
    let admin = connect(&uri, None).await;
    let _ = admin
        .driver()
        .exec(&format!("DROP DATABASE IF EXISTS `{LATIN1_DB}`"))
        .await;
    admin
        .driver()
        .exec(&format!(
            "CREATE DATABASE `{LATIN1_DB}` CHARACTER SET latin1 COLLATE latin1_swedish_ci"
        ))
        .await
        .expect("create the latin1 database");
    let (base, query) = match uri.split_once('?') {
        Some((b, q)) => (b, format!("?{q}")),
        None => (uri.as_str(), String::new()),
    };
    let db_uri = format!("{}/{LATIN1_DB}{query}", &base[..base.rfind('/').unwrap()]);

    let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let chain = Chain::new(&db_uri, None, &tag, "2026-01-01", 4).await;
    let run = chain.first_run().await;
    let id = run.run_id().unwrap().to_string();
    let state = chain.engine().state().clone();

    // The planner's own symbols survive the round trip.
    let stored = state.load_run(&id).await.unwrap().expect("the run row");
    assert_eq!(stored.nodes.len(), 2, "{:?} {}", stored.nodes, run.message);
    let raw = stored.node(&chain.raw).unwrap();
    assert!(
        raw.reason.contains("sync time \u{2212} backtrack"),
        "{}",
        raw.reason
    );

    // A sync message with emoji (v4's success lines) and arrows.
    let extra = PipeRef::new(KEYS, KEYS, &format!("w17bg1_extra_{tag}"), None);
    let mut row = meerschaum_engine::NodeRow::new(
        &id,
        &extra,
        meerschaum_engine::Action::Skip,
        "watermark advanced 10:00 \u{2192} 11:00; [a, b) \u{222a} [c, d)",
    );
    row.message = Some("\u{2705} Synced 3 rows \u{1f389} \u{2014} caf\u{e9}".into());
    let doc = meerschaum_engine::instance_state::node_doc(&row, stored.run.started);
    state
        .write_rows(KEYS, EnginePipe::Nodes, vec![doc])
        .await
        .expect("a latin1 database records unicode node rows");
    let reread = state.load_run(&id).await.unwrap().unwrap();
    let back = reread.node(&extra).expect("the extra node row");
    assert_eq!(back.reason, row.reason);
    assert_eq!(back.message, row.message);

    // Every text column of every engine table is utf8mb4, in a database whose default is latin1.
    let not_utf8mb4 = count(
        &chain.conn,
        &format!(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '{LATIN1_DB}' \
             AND table_name LIKE 'mrsm!_engine!_%' ESCAPE '!' \
             AND data_type IN ('text', 'tinytext', 'mediumtext', 'longtext', 'varchar', 'char') \
             AND (character_set_name IS NULL OR character_set_name <> 'utf8mb4')"
        ),
    )
    .await;
    assert_eq!(not_utf8mb4, 0);
    let utf8mb4 = count(
        &chain.conn,
        &format!(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '{LATIN1_DB}' \
             AND table_name LIKE 'mrsm!_engine!_%' ESCAPE '!' AND character_set_name = 'utf8mb4'"
        ),
    )
    .await;
    assert!(utf8mb4 > 0);
    let default = count(
        &chain.conn,
        &format!(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '{LATIN1_DB}' \
             AND default_character_set_name = 'latin1'"
        ),
    )
    .await;
    assert_eq!(default, 1, "the database default is latin1");

    let _ = std::fs::remove_dir_all(&chain.dir);
    drop(chain);
    let _ = admin
        .driver()
        .exec(&format!("DROP DATABASE IF EXISTS `{LATIN1_DB}`"))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mariadb_state_beside_instance_tables() {
    schemaless_flavor("MRSM_TEST_MARIADB_URI").await;
}
