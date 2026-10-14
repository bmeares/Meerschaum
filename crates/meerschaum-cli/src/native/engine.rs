//! Construct an engine from the CLI's process-owned connector and worker sessions.
use std::collections::HashMap;
use std::sync::Arc;

use meerschaum_connector::{PipesKeys, PipesKeysQuery};
use meerschaum_engine::{Engine, EngineConfig, StaticRegistry};

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

pub fn registry(
    config_ref: &meerschaum_core::config::Config,
    session: &Session,
    inst: &Instance,
    pipes: &[SelectedPipe],
    inplace: bool,
) -> Result<EngineRegistry, ActionError> {
    let mut native = StaticRegistry::new().with_instance(&inst.keys(), inst.conn());
    if inplace
        && config_ref
            .get(&["system", "experimental", "inplace_sync"])
            .is_none_or(|v| v.truthy())
        && let Instance::Sql(sql) = inst
    {
        native = native.with_plugin_sync(&inst.keys(), Arc::new(Inplace(sql.clone())));
    }
    let mut sources: Vec<String> = pipes
        .iter()
        .map(|p| p.pipe.connector_keys().to_owned())
        .collect();
    // Resolve every registered source so traversed parents and children are available too.
    if let Ok(keys) = session.block_on(inst.conn().fetch_pipes_keys(&PipesKeysQuery::default())) {
        let entries = match keys {
            PipesKeys::List(v) => v,
            PipesKeys::Dict(m) => m.into_values().collect(),
        };
        sources.extend(entries.into_iter().map(|e| e.connector_keys));
    }
    sources.push(inst.keys());
    sources.sort();
    sources.dedup();
    let mut sql_instances = HashMap::new();
    for keys in sources {
        if !matches!(keys.split(':').next(), Some("sql" | "api" | "valkey")) {
            continue;
        }
        if let Ok(conn) = session.instance(&keys, config_ref) {
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
        env: &meerschaum_engine::sync::SyncEnv<'_>,
        observer: Option<&dyn meerschaum_engine::sync::DeltaObserver>,
    ) -> meerschaum_core::success::SyncResult {
        use meerschaum_connector::pipe_ops;
        use meerschaum_engine::sync::{determine_begin, sync_pipe};
        use meerschaum_kernel::enforce::PipeInput;
        use meerschaum_sql::instance::fetch::{FetchArgs, metadef_bound};
        // A progress-only observer (chunk events) must not divert the direct in-place sync.
        let Some(observer) = observer.filter(|o| o.records_deltas()) else {
            return self.sync(pipe, args).await;
        };
        // Commit exactly the snapshot recorded by the ledger. Re-running an in-place
        // SELECT after recording its delta could commit concurrent source changes which
        // were absent from that delta. A supplied frame preserves the direct SQL message.
        let prepare = async {
            let ctx = pipe_ops::dtypes_context(self.0.as_ref(), pipe, pipe.parameters())
                .await
                .map_err(|e| e.to_string())?;
            let begin = determine_begin(
                env,
                self.0.as_ref(),
                pipe,
                &ctx.dtypes,
                &args.begin,
                args.end.as_ref(),
                args.check_existing,
            )
            .await
            .map_err(|e| e.to_string())?;
            let fetch_args = FetchArgs {
                begin: metadef_bound(begin.as_ref()),
                end: metadef_bound(args.end.as_ref()),
                check_existing: args.check_existing,
                params: args.params.clone(),
                ..Default::default()
            };
            let df = self
                .0
                .fetch(pipe, &fetch_args)
                .await
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("No data were fetched for {pipe}."))?;
            Ok::<_, String>(df)
        }
        .await;
        let df = match prepare {
            Ok(df) => df,
            Err(error) => return meerschaum_core::success::SyncResult::new(false, error),
        };
        let mut result =
            sync_pipe(env, pipe, Some(PipeInput::Frame(df)), args, Some(observer)).await;
        // Let the enclosing sync timer include the source read.
        result.duration = None;
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
}
