//! Construct an engine from the CLI's process-owned connector and worker sessions.
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use meerschaum_connector::PipesKeysQuery;
use meerschaum_engine::selection::Targets;
use meerschaum_engine::{Depth, Engine, EngineConfig, PipeRef, Selection, StaticRegistry};

use super::super::pipes::SelectedPipe;
use super::super::{Instance, Session};
use crate::builtin::ActionError;
use crate::dispatch::Runtime;

/// The registries of one engine: the Python adapter (listen / callback controls) and the
/// registry the engine resolves connectors through (native plugins before the adapter).
pub struct EngineRegistry {
    pub python: Arc<meerschaum_engine::python::PythonRegistry>,
    pub registry: Arc<dyn meerschaum_engine::ConnectorRegistry>,
    pub config: EngineConfig,
}

/// What [`registry_with`] builds beyond the home instance.
pub struct RegistryOptions {
    /// The selection the engine will run: its targets (pipe refs, or a key filter over the
    /// home instance's pipes) and traversal depths. Only the connectors of the pipes it can
    /// reach are built (see [`registry_with`]).
    pub selection: Selection,
    /// Sync the home instance's own `sql:` pipes in place (`INSERT ... SELECT`, flat CLI runs;
    /// gated on `system:experimental:inplace_sync`).
    pub inplace: bool,
    /// Connectors the host built itself, by keys, used in place of the session's configured
    /// ones (the Python façade's ad hoc `sql:` connectors, which are in no config registry).
    pub overrides: HashMap<String, Instance>,
}

impl Default for RegistryOptions {
    fn default() -> Self {
        RegistryOptions {
            selection: Selection::refs([]),
            inplace: false,
            overrides: HashMap::new(),
        }
    }
}

/// The engine registry of a CLI selection: [`registry_with`] over the selected pipes, with
/// every ancestor and descendant in reach (the callers' traversal depth is not known here, so
/// this covers any `--parents` / `--children` / `--resume`). Pipes unrelated to the selection
/// never have their connectors built.
///
/// `inplace` is accepted for compatibility and ignored: every engine run syncs a `sql:` pipe
/// whose source is its instance in place, graph runs included. PARITY: v4 does (so the table
/// keeps the definition's column types); a graph run records its ledger from the in-place
/// sync's staged rows (`Inplace::sync_native`), so it no longer needs the frame path, which
/// re-enforced configured dtypes and altered, e.g., NUMERIC columns to TEXT.
pub fn registry(
    config_ref: &meerschaum_core::config::Config,
    session: &Session,
    inst: &Instance,
    pipes: &[SelectedPipe],
    inplace: bool,
) -> Result<EngineRegistry, ActionError> {
    let _ = inplace;
    registry_with(
        config_ref,
        session,
        inst,
        RegistryOptions {
            selection: Selection::refs(pipes.iter().map(|p| PipeRef::from_pipe(&p.pipe)))
                .with_parents(Some(Depth::All))
                .with_children(Some(Depth::All)),
            inplace: true,
            overrides: HashMap::new(),
        },
    )
}

/// The connector keys a selection can touch: the targets' sources and instances, plus, for
/// every pipe the selection reaches in the home instance's graph, its source, its instance
/// and the instances its `{{ Pipe() }}` references name (their table references).
fn reachable_connector_keys(
    graph: &meerschaum_engine::Graph,
    records: &[meerschaum_engine::PipeRecord],
    selection: &Selection,
    defaults: &meerschaum_core::pipe::PipeDefaults,
) -> BTreeSet<String> {
    let targets: Vec<PipeRef> = match &selection.targets {
        Targets::Refs(refs) => refs.clone(),
        Targets::Filter(filter) => records
            .iter()
            .filter(|r| filter.matches(r))
            .map(meerschaum_engine::PipeRecord::pipe_ref)
            .collect(),
    };
    let mut keys = BTreeSet::new();
    for t in &targets {
        let parts = t.parts();
        keys.insert(parts.connector_keys.to_string());
        keys.insert(parts.instance_keys.to_string());
    }
    for r in graph.reach(targets, selection.parents, selection.children) {
        let Some(node) = graph.node(&r) else { continue };
        keys.insert(node.record.connector_keys.clone());
        keys.insert(node.record.instance_keys.clone());
        let mut params = node.pipe.raw_parameters().clone();
        params.shift_remove("reference");
        params.shift_remove("references");
        for referenced in meerschaum_core::template::referenced_pipes_in_parameters(
            &params,
            Some(&node.pipe),
            defaults,
        ) {
            keys.insert(referenced.instance_keys().to_string());
        }
    }
    keys
}

/// The single engine registry builder (the CLI and `meerschaum-py` both use it): native `sql:`,
/// `api:` and `valkey:` connectors (overrides first, then the session's), the `{{ Pipe() }}`
/// table references of SQL instances, the process's Python worker pool and native plugins.
///
/// Only the connectors the selection can reach are built: the home instance's pipes are read
/// once (`fetch_pipes_keys`) into the graph, and the targets plus the pipes within the
/// selection's traversal depths contribute their sources and instances. A pipe that is not
/// related to the selection never has its connector built, so an unreachable or private
/// connector of some other pipe is never contacted.
pub fn registry_with(
    config_ref: &meerschaum_core::config::Config,
    session: &Session,
    inst: &Instance,
    options: RegistryOptions,
) -> Result<EngineRegistry, ActionError> {
    let RegistryOptions {
        selection,
        inplace,
        overrides,
    } = options;
    let resolve = |keys: &str| -> Option<Instance> {
        overrides
            .get(keys)
            .cloned()
            .or_else(|| session.instance(keys, config_ref).ok())
    };
    let mut native = StaticRegistry::new().with_instance(&inst.keys(), inst.conn());
    if inplace
        && config_ref
            .get(&["system", "experimental", "inplace_sync"])
            .is_none_or(|v| v.truthy())
        && let Instance::Sql(sql) = inst
    {
        native = native.with_plugin_sync(&inst.keys(), Arc::new(Inplace(sql.clone())));
    }
    let engine_defaults = EngineConfig::from_config(config_ref, Some(&config_ref.paths().root_dir));
    let home_only = StaticRegistry::new().with_instance(&inst.keys(), inst.conn());
    let records = session
        .block_on(meerschaum_engine::engine::fetch_records(
            &home_only,
            &engine_defaults.defaults,
            &inst.keys(),
            &PipesKeysQuery::default(),
        ))
        .unwrap_or_default();
    let graph = meerschaum_engine::Graph::build(
        records.clone(),
        &meerschaum_engine::GraphOptions {
            defaults: engine_defaults.defaults.clone(),
            implicit_edges: engine_defaults.implicit_edges,
            default_backtrack_minutes: engine_defaults.default_backtrack_minutes as f64,
        },
    );
    let mut wanted =
        reachable_connector_keys(&graph, &records, &selection, &engine_defaults.defaults);
    wanted.extend(overrides.keys().cloned());
    wanted.insert(inst.keys());
    let mut sql_instances = HashMap::new();
    for keys in wanted {
        if !matches!(keys.split(':').next(), Some("sql" | "api" | "valkey")) {
            continue;
        }
        if let Some(conn) = resolve(&keys) {
            native = native
                .with_instance(&keys, conn.conn())
                .with_source(&keys, conn.conn());
            if let Instance::Sql(sql) = conn {
                native = native.with_fetch_chunksize(&keys, sql.settings.chunksize.max(1) as usize);
                sql_instances.insert(keys, sql);
            }
        }
    }
    native = native.with_table_reference(move |pipe| {
        if let Some(sql) = sql_instances.get(pipe.instance_keys()) {
            let pipe = sql.sql_pipe(pipe).map_err(|e| e.to_string())?;
            Ok(sql.pipe_table_name(&pipe))
        } else {
            pipe.target(None).map_err(|e| e.to_string())
        }
    });
    let mut config = EngineConfig::from_config(config_ref, Some(&config_ref.paths().root_dir));
    config.home_instance = inst.keys();
    let python = meerschaum_engine::python::PythonRegistry::new(
        Arc::new(native),
        session.pool(config_ref),
        session.python_manifest(config_ref)?,
        config.clone(),
    );
    let registry = crate::native_plugins::NativeRegistry::new(
        session.native_plugins(config_ref)?,
        config_ref,
        python.clone(),
    );
    Ok(EngineRegistry {
        python,
        registry,
        config,
    })
}

/// PARITY: v4 returns the SQL in-place sync message directly (without a chunk envelope).
struct Inplace(Arc<meerschaum_sql::SqlConnector>);
#[meerschaum_connector::async_trait]
impl meerschaum_engine::PluginSync for Inplace {
    async fn sync_native(
        &self,
        pipe: &meerschaum_core::Pipe,
        args: &meerschaum_engine::SyncArgs,
        _env: &meerschaum_engine::sync::SyncEnv<'_>,
        observer: Option<&dyn meerschaum_engine::sync::DeltaObserver>,
    ) -> meerschaum_core::success::SyncResult {
        use meerschaum_engine::SyncBegin;
        use meerschaum_sql::instance::fetch::metadef_bound;
        use meerschaum_sql::queries::metadef::Bound;
        // A progress-only observer (chunk events) must not divert the direct in-place sync.
        let Some(observer) = observer.filter(|o| o.records_deltas()) else {
            return self.sync(pipe, args).await;
        };
        // PARITY: v4 syncs a pipe whose source is its instance in place, so the table keeps
        // the definition's column types (a frame round trip would re-enforce the configured
        // dtypes and alter, e.g., NUMERIC to TEXT). The ledger is recorded from the in-place
        // sync's own staged rows right before it merges them, so it covers exactly what
        // commits, with no second read of the source.
        struct Ledger<'a> {
            pipe: &'a meerschaum_core::Pipe,
            observer: &'a dyn meerschaum_engine::sync::DeltaObserver,
            dtypes: Option<meerschaum_kernel::enforce::PipeDtypesContext>,
        }
        #[meerschaum_connector::async_trait]
        impl meerschaum_sql::instance::inplace::InplaceObserver for Ledger<'_> {
            async fn before_merge(&self, delta: &meerschaum_kernel::Frame) -> Result<(), String> {
                // The staged rows come back with the database's types (SQLite returns
                // timestamps as text): enforce the pipe's dtypes so the ledger sees datetimes
                // (or integers) on the pipe's axis, as on the frame path.
                let enforced = self.dtypes.as_ref().and_then(|ctx| {
                    meerschaum_kernel::enforce::pipe_enforce_dtypes(
                        meerschaum_kernel::enforce::PipeInput::Frame(delta.clone()),
                        ctx,
                    )
                    .ok()
                });
                self.observer
                    .before_commit(self.pipe, enforced.as_ref().unwrap_or(delta))
                    .await
            }
        }
        let dtypes = meerschaum_connector::pipe_ops::dtypes_context(
            self.0.as_ref(),
            pipe,
            pipe.parameters(),
        )
        .await
        .ok();
        let begin = match &args.begin {
            SyncBegin::Default => Bound::Default,
            SyncBegin::At(b) => metadef_bound(Some(b)),
            SyncBegin::Unbounded => Bound::None,
        };
        let result = self
            .0
            .sync_pipe_inplace_observed(
                pipe,
                args.params.as_ref(),
                begin,
                metadef_bound(args.end.as_ref()),
                args.check_existing,
                &Ledger {
                    pipe,
                    observer,
                    dtypes,
                },
            )
            .await;
        let mut result =
            meerschaum_engine::sync::result_from_message(result.success, result.message);
        if result.success {
            if let Err(e) = observer.after_commit(pipe, &result).await {
                result = meerschaum_core::success::SyncResult::new(false, e);
            }
        }
        result
    }
    async fn sync(
        &self,
        pipe: &meerschaum_core::Pipe,
        args: &meerschaum_engine::SyncArgs,
    ) -> meerschaum_core::success::SyncResult {
        use meerschaum_engine::SyncBegin;
        use meerschaum_sql::instance::fetch::metadef_bound;
        use meerschaum_sql::queries::metadef::Bound;
        let begin = match &args.begin {
            SyncBegin::Default => Bound::Default,
            SyncBegin::At(b) => metadef_bound(Some(b)),
            SyncBegin::Unbounded => Bound::None,
        };
        let result = self
            .0
            .sync_pipe_inplace(
                pipe,
                args.params.as_ref(),
                begin,
                metadef_bound(args.end.as_ref()),
                args.check_existing,
            )
            .await;
        meerschaum_engine::sync::result_from_message(result.success, result.message)
    }
}

pub fn build(
    rt: &mut Runtime<'_>,
    session: &Session,
    inst: &Instance,
    pipes: &[SelectedPipe],
    inplace: bool,
    listen: bool,
    stream_kwargs: Option<meerschaum_core::Map>,
) -> Result<Engine, ActionError> {
    let EngineRegistry {
        python,
        registry,
        config,
    } = registry(rt.config(), session, inst, pipes, inplace)?;
    if listen {
        python.enable_legacy_streams();
    }
    if let Some(kwargs) = stream_kwargs {
        python.set_stream_kwargs(kwargs);
    }
    Ok(Engine::new(registry, config).with_cadence(Arc::new(JobsCadence)))
}

/// `parameters['schedule']` in continuous mode, parsed with the `-s` grammar of
/// `meerschaum-jobs` (the same text `--schedule` accepts).
struct JobsCadence;

struct JobsTimer(meerschaum_jobs::schedule::Trigger);

impl meerschaum_engine::Timer for JobsTimer {
    fn first(
        &mut self,
        _now: chrono::DateTime<chrono::Utc>,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        self.0.next().map(|t| t.to_utc())
    }
    fn after(
        &mut self,
        after: chrono::DateTime<chrono::Utc>,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        self.0.next_after(after.fixed_offset()).map(|t| t.to_utc())
    }
}

impl meerschaum_engine::Cadence for JobsCadence {
    fn timer(
        &self,
        schedule: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Box<dyn meerschaum_engine::Timer>, String> {
        meerschaum_jobs::parse_schedule(schedule, now.fixed_offset())
            .map(|t| Box::new(JobsTimer(t)) as Box<dyn meerschaum_engine::Timer>)
            .map_err(|e| e.py_message())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_connector::{DataQuery, async_trait};
    use meerschaum_core::{Pipe, Value};
    use meerschaum_engine::PluginSync;
    use meerschaum_engine::sync::{DeltaObserver, SyncEnv};
    use meerschaum_kernel::Frame;

    struct AppendAfterSnapshot(Arc<meerschaum_sql::SqlConnector>);
    #[async_trait]
    impl DeltaObserver for AppendAfterSnapshot {
        async fn before_commit(&self, _pipe: &Pipe, delta: &Frame) -> Result<(), String> {
            assert_eq!(delta.height(), 1);
            let results = self
                .0
                .exec_queries(&["INSERT INTO wiring_source VALUES (2)".into()], true, true)
                .await;
            assert!(results.iter().all(Option::is_some));
            Ok(())
        }
    }

    #[test]
    fn pipe_schedule_uses_the_jobs_grammar() {
        use meerschaum_engine::Cadence;
        let now = chrono::Utc::now();
        let mut timer = JobsCadence.timer("every 2 seconds", now).unwrap();
        let first = timer.first(now).unwrap();
        assert!(first >= now);
        let after = timer.after(first).unwrap();
        assert!(after > first);
        assert!(JobsCadence.timer("every blue moons", now).is_err());
    }

    #[tokio::test]
    async fn inplace_with_ledger_commits_only_recorded_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let uri = format!("sqlite:///{}", root.path().join("wiring.db").display());
        let sql = Arc::new(
            meerschaum_sql::SqlConnector::from_uri("wiring", &uri)
                .await
                .unwrap(),
        );
        let queries = [
            "CREATE TABLE wiring_source (id INTEGER)".into(),
            "INSERT INTO wiring_source VALUES (1)".into(),
        ];
        assert!(
            sql.exec_queries(&queries, true, true)
                .await
                .iter()
                .all(Option::is_some)
        );
        let params = Value::from_json_str(r#"{"columns":{"datetime":"id"},"dtypes":{"id":"int"},"fetch":{"definition":"SELECT * FROM wiring_source"}}"#).unwrap().to_map();
        let pipe = Pipe::builder("sql:wiring", "snapshot")
            .instance("sql:wiring")
            .parameters(params)
            .build()
            .unwrap();
        let native = StaticRegistry::new()
            .with_instance("sql:wiring", sql.clone())
            .with_source("sql:wiring", sql.clone());
        let defaults = Default::default();
        let env = SyncEnv {
            registry: &native,
            defaults: &defaults,
            default_backtrack_minutes: 1440,
            filter_params_index_limit: 250,
            known: None,
        };
        let result = Inplace(sql.clone())
            .sync_native(
                &pipe,
                &Default::default(),
                &env,
                Some(&AppendAfterSnapshot(sql.clone())),
            )
            .await;
        assert!(result.success, "{}", result.message);
        assert_eq!(
            sql.get_pipe_rowcount(&pipe, &DataQuery::default())
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(result.inserted, 1);
    }

    /// PostgreSQL-family (`MRSM_TEST_POSTGIS_URI`; empty: the disposable container on 5449):
    /// a flat engine run syncs a pipe whose source is its instance in place even while it
    /// records ledger entries for its child, so a definition yielding NUMERIC keeps NUMERIC
    /// although `dtypes` says `float` (v4's in-place behavior). An observer error merges
    /// nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn inplace_ledger_keeps_the_definitions_numeric_types() {
        use meerschaum_engine::{
            Engine, EngineConfig, FaultInjector, PipeRef, RunOptions, Selection, StateSource,
        };
        let Ok(raw) = std::env::var("MRSM_TEST_POSTGIS_URI") else {
            eprintln!("skipped: MRSM_TEST_POSTGIS_URI is not set");
            return;
        };
        let base = if raw.is_empty() {
            "postgis://test:test1234@localhost:5449/testdb".to_string()
        } else {
            raw
        };
        let schema = "w9s_numeric";
        let sep = if base.contains('?') { '&' } else { '?' };
        let uri = format!("{base}{sep}schema={schema}");
        let keys = "sql:w9num";
        let sql = Arc::new(
            meerschaum_sql::SqlConnector::from_uri("w9num", &uri)
                .await
                .unwrap(),
        );
        let _ = sql
            .exec(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
            .await;
        let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
        let src = format!("\"{schema}\".\"w9s_src_{tag}\"");
        let insert = |rows: std::ops::Range<u32>| {
            let values: Vec<String> = rows
                .map(|h| format!("('2026-01-01 {h:02}:00:00', {h}, {h}.125)"))
                .collect();
            format!("INSERT INTO {src} (ts, id, v) VALUES {}", values.join(", "))
        };
        assert!(
            sql.exec(&format!(
                "CREATE TABLE {src} (ts TIMESTAMPTZ, id INTEGER, v NUMERIC(12, 3))"
            ))
            .await
            .is_some()
        );
        assert!(sql.exec(&insert(0..4)).await.is_some());
        let (parent, child) = (format!("w9s_num_{tag}"), format!("w9s_numchild_{tag}"));
        let params = |definition: String| {
            Value::from_json_str(
                &serde_json::json!({
                    "sql": definition,
                    "columns": {"datetime": "ts", "id": "id"},
                    "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
                })
                .to_string(),
            )
            .unwrap()
            .to_map()
        };
        let parent_pipe = Pipe::builder(keys, &parent)
            .instance(keys)
            .parameters(params(format!("SELECT ts, id, v * 2 AS v FROM {src}")))
            .build()
            .unwrap();
        let child_pipe = Pipe::builder(keys, &child)
            .instance(keys)
            .parameters(params(format!(
                "SELECT ts, id, v FROM {{{{ Pipe('{keys}', '{parent}', instance='{keys}') }}}}"
            )))
            .build()
            .unwrap();
        for p in [&parent_pipe, &child_pipe] {
            assert!(sql.register_pipe(p).await.success);
        }
        let table_ref = sql.clone();
        let registry = Arc::new(
            StaticRegistry::new()
                .with_instance(keys, sql.clone())
                .with_source(keys, sql.clone())
                .with_plugin_sync(keys, Arc::new(Inplace(sql.clone())))
                .with_table_reference(move |pipe| {
                    let sp = table_ref.sql_pipe(pipe).map_err(|e| e.to_string())?;
                    Ok(table_ref.pipe_table_name(&sp))
                }),
        );
        let dir = tempfile::tempdir().unwrap();
        let config = EngineConfig {
            home_instance: keys.into(),
            lease_dir: dir.path().join("leases"),
            lease_poll: std::time::Duration::from_millis(50),
            ..EngineConfig::default()
        };
        // A flat run of the parent: it has a child, so its sync records ledger entries.
        let options = || RunOptions {
            selection: Some(Selection::refs([PipeRef::new(keys, keys, &parent, None)])),
            ..Default::default()
        };
        let parent_ref = PipeRef::new(keys, keys, &parent, None);
        let v_type = || async {
            let df = sql
                .read(&format!(
                    "SELECT data_type FROM information_schema.columns \
                     WHERE table_schema = '{schema}' AND table_name = 'sql_w9num_{parent}' \
                     AND column_name = 'v'"
                ))
                .await
                .unwrap();
            df.column("data_type").unwrap().data.get(0).py_str()
        };
        let rowcount = || async {
            sql.get_pipe_rowcount(&parent_pipe, &DataQuery::default())
                .await
                .unwrap()
                .unwrap_or(0)
        };

        let engine = Engine::new(registry.clone(), config.clone());
        let first = engine.run(options()).await;
        assert!(first.success, "{}", first.message);
        assert_eq!(rowcount().await, 4);
        assert_eq!(v_type().await, "numeric");
        let seq = engine.state().latest_seq(keys, &parent_ref).await.unwrap();
        assert!(seq.is_some(), "the first sync wrote a ledger entry");

        assert!(sql.exec(&insert(4..6)).await.is_some());
        let second = engine.run(options()).await;
        assert!(second.success, "{}", second.message);
        assert_eq!(rowcount().await, 6);
        assert_eq!(v_type().await, "numeric", "the second sync kept NUMERIC");
        let seq2 = engine.state().latest_seq(keys, &parent_ref).await.unwrap();
        assert!(seq2 > seq, "{seq:?} -> {seq2:?}");
        let entries = engine
            .state()
            .ledger_after(keys, &parent_ref, seq.unwrap())
            .await
            .unwrap();
        assert!(
            entries.iter().any(|e| e.end.is_some()),
            "the in-place delta's datetimes reached the ledger: {entries:?}"
        );

        // An observer error (a failed ledger write) merges nothing.
        struct RefuseLedger;
        impl FaultInjector for RefuseLedger {
            fn after_ledger_write(&self, _node: &PipeRef) -> Result<(), String> {
                Err("ledger refused".into())
            }
        }
        assert!(sql.exec(&insert(6..8)).await.is_some());
        let refused = Engine::new(registry, config)
            .with_faults(Arc::new(RefuseLedger))
            .run(options())
            .await;
        assert!(!refused.success);
        assert!(refused.message.contains("failed"), "{}", refused.message);
        assert_eq!(
            rowcount().await,
            6,
            "nothing merged after the observer refused"
        );
        assert_eq!(v_type().await, "numeric");

        for p in [&parent_pipe, &child_pipe] {
            let _ = sql.drop_pipe(p).await;
            let _ = sql.delete_pipe(p).await;
        }
        let _ = sql.exec(&format!("DROP TABLE IF EXISTS {src}")).await;
    }

    /// SQLite returns timestamps as text: the in-place sync's staged rows are dtype-enforced
    /// before the ledger reads them, so a graph run still records the changed range (wave 9).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn inplace_ledger_on_sqlite_records_datetime_ranges() {
        use meerschaum_engine::StateSource;
        use meerschaum_engine::{Depth, Engine, EngineConfig, PipeRef, RunOptions, Selection};
        let root = tempfile::tempdir().unwrap();
        let uri = format!("sqlite:///{}", root.path().join("w9inpl.db").display());
        let keys = "sql:w9inpl";
        let sql = Arc::new(
            meerschaum_sql::SqlConnector::from_uri("w9inpl", &uri)
                .await
                .unwrap(),
        );
        let queries = [
            "CREATE TABLE w9_src (ts TIMESTAMP, id INTEGER, v INTEGER)".to_string(),
            "INSERT INTO w9_src VALUES ('2024-01-01 00:00:00', 1, 1), ('2024-01-01 01:00:00', 2, 5)"
                .to_string(),
        ];
        assert!(
            sql.exec_queries(&queries, true, true)
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
        let raw = Pipe::builder(keys, "w9_raw")
            .instance(keys)
            .parameters(params("SELECT * FROM w9_src"))
            .build()
            .unwrap();
        let child = Pipe::builder(keys, "w9_child")
            .instance(keys)
            .parameters(params(
                "SELECT ts, id, v FROM {{ Pipe('sql:w9inpl', 'w9_raw', instance='sql:w9inpl') }}",
            ))
            .build()
            .unwrap();
        for p in [&raw, &child] {
            assert!(sql.register_pipe(p).await.success);
        }
        let table_ref = sql.clone();
        let registry = Arc::new(
            StaticRegistry::new()
                .with_instance(keys, sql.clone())
                .with_source(keys, sql.clone())
                .with_plugin_sync(keys, Arc::new(Inplace(sql.clone())))
                .with_table_reference(move |pipe| {
                    let sp = table_ref.sql_pipe(pipe).map_err(|e| e.to_string())?;
                    Ok(table_ref.pipe_table_name(&sp))
                }),
        );
        let engine = Engine::new(
            registry,
            EngineConfig {
                home_instance: keys.into(),
                lease_dir: root.path().join("leases"),
                lease_poll: std::time::Duration::from_millis(50),
                ..EngineConfig::default()
            },
        );
        let run = engine
            .run(RunOptions {
                selection: Some(
                    Selection::refs([PipeRef::new(keys, keys, "w9_child", None)])
                        .with_parents(Some(Depth::All)),
                ),
                ..Default::default()
            })
            .await;
        assert!(run.success, "{}", run.message);
        let raw_ref = PipeRef::new(keys, keys, "w9_raw", None);
        let entries = engine
            .state()
            .ledger_after(keys, &raw_ref, 0)
            .await
            .unwrap();
        assert!(
            !entries.is_empty(),
            "the in-place sync recorded no ledger entry"
        );
        assert!(
            entries.iter().all(|e| e.begin.is_some() && e.end.is_some()),
            "{entries:?}"
        );
        for p in [&raw, &child] {
            assert_eq!(
                sql.get_pipe_rowcount(p, &DataQuery::default())
                    .await
                    .unwrap(),
                Some(2)
            );
        }

        // The table exists now: the merge path. SQLite has one writer, so the ledger write
        // must not wait on the in-place session's lock.
        assert!(
            sql.exec("INSERT INTO w9_src VALUES ('2024-01-01 02:00:00', 3, 7)")
                .await
                .is_some()
        );
        let second = engine
            .run(RunOptions {
                selection: Some(
                    Selection::refs([PipeRef::new(keys, keys, "w9_child", None)])
                        .with_parents(Some(Depth::All)),
                ),
                ..Default::default()
            })
            .await;
        assert!(second.success, "{}", second.message);
        let after = engine
            .state()
            .ledger_after(keys, &raw_ref, 0)
            .await
            .unwrap();
        // (Consumed entries are compacted: compare the newest sequence numbers.)
        let newest = |e: &[meerschaum_engine::LedgerEntry]| e.iter().map(|x| x.seq).max();
        assert!(newest(&after) > newest(&entries), "{after:?}");
        for p in [&raw, &child] {
            assert_eq!(
                sql.get_pipe_rowcount(p, &DataQuery::default())
                    .await
                    .unwrap(),
                Some(3)
            );
        }
    }
}
