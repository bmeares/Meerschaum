//! Direct SQLite-backed tests for the native in-place sync seam (`InstanceConnector::sync_pipe_inplace`)
//! inside `meerschaum-engine` — no CLI or Python wrappers, no `PluginSync` adapters.

use std::sync::{Arc, Mutex};

use meerschaum_connector::{DataQuery, InstanceConnector, async_trait};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::success::SyncResult;
use meerschaum_core::{Pipe, Value};
use meerschaum_engine::sync::{DeltaObserver, SyncArgs, SyncEnv, sync_pipe};
use meerschaum_engine::{
    Depth, Engine, EngineConfig, FaultInjector, PipeRef, RunOptions, Selection, StateSource,
    StaticRegistry,
};
use meerschaum_kernel::Frame;

fn temp_dir(prefix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "mrsm_engine_{prefix}_{}_{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

struct RecordingObserver {
    sql: Arc<SqlConnector>,
    append_sql: Option<String>,
    fail_before_commit: bool,
    seen_rows: Mutex<Vec<usize>>,
    seen_datetime_is_typed: Mutex<Vec<bool>>,
    after_commits: Mutex<Vec<u64>>,
}

#[async_trait]
impl DeltaObserver for RecordingObserver {
    async fn before_commit(&self, _pipe: &Pipe, delta: &Frame) -> Result<(), String> {
        self.seen_rows
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(delta.height());
        if let Some(col) = delta.column("ts") {
            let typed = col.data.dtype_str().starts_with("datetime64");
            self.seen_datetime_is_typed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(typed);
        }
        if self.fail_before_commit {
            return Err("observer aborted before commit".into());
        }
        if let Some(sql) = &self.append_sql {
            assert!(self.sql.exec(sql).await.is_some());
        }
        Ok(())
    }

    async fn after_commit(&self, _pipe: &Pipe, result: &SyncResult) -> Result<(), String> {
        self.after_commits
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(result.inserted);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_inplace_sync_observes_enforced_deltas_and_isolates_snapshot() {
    let dir = temp_dir("inplace_obs");
    let uri = format!("sqlite:///{}", dir.join("obs.db").display());
    let keys = "sql:inpl_obs";
    let sql = Arc::new(SqlConnector::from_uri("inpl_obs", &uri).await.unwrap());
    assert!(sql.supports_inplace_sync());
    assert!(
        sql.exec_queries(
            &[
                "CREATE TABLE obs_src (ts TIMESTAMP, id INTEGER, v INTEGER)".into(),
                "INSERT INTO obs_src VALUES ('2024-01-01 00:00:00', 1, 10)".into(),
            ],
            true,
            true,
        )
        .await
        .iter()
        .all(Option::is_some)
    );

    let params = Value::from_json_str(
        r#"{
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "int"},
            "fetch": {"definition": "SELECT ts, id, v FROM obs_src"}
        }"#,
    )
    .unwrap()
    .to_map();
    let pipe = Pipe::builder(keys, "obs_pipe")
        .instance(keys)
        .parameters(params)
        .build()
        .unwrap();

    let registry = StaticRegistry::new()
        .with_instance(keys, sql.clone())
        .with_source(keys, sql.clone())
        .with_inplace_sync(true);
    let defaults = Default::default();
    let env = SyncEnv {
        delivery: None,
        registry: &registry,
        defaults: &defaults,
        default_backtrack_minutes: 1440,
        filter_params_index_limit: 250,
        known: None,
    };

    // Append a second row during `before_commit`: the in-place sync must commit only the
    // observed snapshot (1 row), enforce datetime dtypes for the observer, and call `after_commit`.
    let obs = RecordingObserver {
        sql: sql.clone(),
        append_sql: Some("INSERT INTO obs_src VALUES ('2024-01-02 00:00:00', 2, 20)".into()),
        fail_before_commit: false,
        seen_rows: Mutex::new(Vec::new()),
        seen_datetime_is_typed: Mutex::new(Vec::new()),
        after_commits: Mutex::new(Vec::new()),
    };
    let first = sync_pipe(&env, &pipe, None, &SyncArgs::default(), Some(&obs)).await;
    assert!(first.success, "{}", first.message);
    assert_eq!(first.inserted, 1);
    assert_eq!(*obs.seen_rows.lock().unwrap(), vec![1]);
    assert_eq!(*obs.seen_datetime_is_typed.lock().unwrap(), vec![true]);
    assert_eq!(*obs.after_commits.lock().unwrap(), vec![1]);
    assert_eq!(
        sql.get_pipe_rowcount(&pipe, &DataQuery::default())
            .await
            .unwrap(),
        Some(1)
    );

    // Observer refusal aborts the merge with nothing written.
    let failing = RecordingObserver {
        sql: sql.clone(),
        append_sql: None,
        fail_before_commit: true,
        seen_rows: Mutex::new(Vec::new()),
        seen_datetime_is_typed: Mutex::new(Vec::new()),
        after_commits: Mutex::new(Vec::new()),
    };
    let aborted = sync_pipe(&env, &pipe, None, &SyncArgs::default(), Some(&failing)).await;
    assert!(!aborted.success);
    assert!(aborted.message.contains("observer aborted before commit"));
    assert!(failing.after_commits.lock().unwrap().is_empty());
    assert_eq!(
        sql.get_pipe_rowcount(&pipe, &DataQuery::default())
            .await
            .unwrap(),
        Some(1)
    );

    // Disabling in-place sync on the registry falls back to the fetch path and still succeeds.
    let no_inplace_registry = StaticRegistry::new()
        .with_instance(keys, sql.clone())
        .with_source(keys, sql.clone())
        .with_inplace_sync(false);
    let fallback_env = SyncEnv {
        registry: &no_inplace_registry,
        ..env.clone()
    };
    let fallback = sync_pipe(&fallback_env, &pipe, None, &SyncArgs::default(), None).await;
    assert!(fallback.success, "{}", fallback.message);
    assert_eq!(
        sql.get_pipe_rowcount(&pipe, &DataQuery::default())
            .await
            .unwrap(),
        Some(2)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_run_records_ledger_natively_through_inplace_seam() {
    let dir = temp_dir("inplace_dag");
    let uri = format!("sqlite:///{}", dir.join("dag.db").display());
    let keys = "sql:inpl_dag";
    let sql = Arc::new(SqlConnector::from_uri("inpl_dag", &uri).await.unwrap());
    assert!(
        sql.exec_queries(
            &[
                "CREATE TABLE dag_src (ts TIMESTAMP, id INTEGER, v INTEGER)".into(),
                "INSERT INTO dag_src VALUES ('2024-01-01 00:00:00', 1, 10), ('2024-01-01 01:00:00', 2, 20)".into(),
            ],
            true,
            true,
        )
        .await
        .iter()
        .all(Option::is_some)
    );

    let params = |definition: &str| {
        Value::from_json_str(
            &serde_json::json!({
                "fetch": {"definition": definition},
                "columns": {"datetime": "ts", "id": "id"},
                "dtypes": {"ts": "datetime"},
            })
            .to_string(),
        )
        .unwrap()
        .to_map()
    };
    let parent = Pipe::builder(keys, "dag_parent")
        .instance(keys)
        .parameters(params("SELECT ts, id, v FROM dag_src"))
        .build()
        .unwrap();
    let child = Pipe::builder(keys, "dag_child")
        .instance(keys)
        .parameters(params(
            "SELECT ts, id, v FROM {{ Pipe('sql:inpl_dag', 'dag_parent', instance='sql:inpl_dag') }}",
        ))
        .build()
        .unwrap();
    for p in [&parent, &child] {
        assert!(sql.register_pipe(p).await.success);
    }

    let table_ref = sql.clone();
    let registry = Arc::new(
        StaticRegistry::new()
            .with_instance(keys, sql.clone())
            .with_source(keys, sql.clone())
            .with_inplace_sync(true)
            .with_table_reference(move |pipe| {
                let sp = table_ref.sql_pipe(pipe).map_err(|e| e.to_string())?;
                Ok(table_ref.pipe_table_name(&sp))
            }),
    );
    let config = EngineConfig {
        home_instance: keys.into(),
        lease_dir: dir.join("leases"),
        lease_poll: std::time::Duration::from_millis(50),
        ..EngineConfig::default()
    };
    let engine = Engine::new(registry.clone(), config.clone());
    let run_opts = || RunOptions {
        selection: Some(
            Selection::refs([PipeRef::new(keys, keys, "dag_child", None)])
                .with_parents(Some(Depth::All)),
        ),
        ..Default::default()
    };

    // First run (table creation) writes typed ledger entries and feeds the child.
    let first = engine.run(run_opts()).await;
    assert!(first.success, "{}", first.message);
    let parent_ref = PipeRef::new(keys, keys, "dag_parent", None);
    let entries = engine
        .state()
        .ledger_after(keys, &parent_ref, 0)
        .await
        .unwrap();
    assert!(
        !entries.is_empty(),
        "in-place sync recorded no ledger entry"
    );
    assert!(
        entries.iter().all(|e| e.begin.is_some() && e.end.is_some()),
        "{entries:?}"
    );
    for p in [&parent, &child] {
        assert_eq!(
            sql.get_pipe_rowcount(p, &DataQuery::default())
                .await
                .unwrap(),
            Some(2)
        );
    }

    // Second run (merge path) appends another ledger entry and updates the child.
    assert!(
        sql.exec("INSERT INTO dag_src VALUES ('2024-01-01 02:00:00', 3, 30)")
            .await
            .is_some()
    );
    let second = engine.run(run_opts()).await;
    assert!(second.success, "{}", second.message);
    let after = engine
        .state()
        .ledger_after(keys, &parent_ref, 0)
        .await
        .unwrap();
    let newest = |e: &[meerschaum_engine::LedgerEntry]| e.iter().map(|x| x.seq).max();
    assert!(newest(&after) > newest(&entries), "{after:?}");
    for p in [&parent, &child] {
        assert_eq!(
            sql.get_pipe_rowcount(p, &DataQuery::default())
                .await
                .unwrap(),
            Some(3)
        );
    }

    // Fault injection on ledger write prevents the merge from committing.
    struct RefuseLedger;
    impl FaultInjector for RefuseLedger {
        fn after_ledger_write(&self, _node: &PipeRef) -> Result<(), String> {
            Err("ledger refused".into())
        }
    }
    assert!(
        sql.exec("INSERT INTO dag_src VALUES ('2024-01-01 03:00:00', 4, 40)")
            .await
            .is_some()
    );
    let refused = Engine::new(registry, config)
        .with_faults(Arc::new(RefuseLedger))
        .run(run_opts())
        .await;
    assert!(!refused.success);
    assert_eq!(
        sql.get_pipe_rowcount(&parent, &DataQuery::default())
            .await
            .unwrap(),
        Some(3),
        "nothing merged after the ledger observer refused"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
