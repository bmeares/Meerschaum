//! The Syncing Engine from Python (design §5.2, D4, D5, R1): `pipe.sync(parents=, children=)`,
//! `mrsm.Pipeline` and the run history.
//!
//! The engine is Rust-only by design (D14), so these functions are bound in every `MRSM_CORE`
//! mode: the Python façade calls them whenever a graph traversal is asked for, and a bare
//! `pipe.sync()` never reaches this module (R1). Requests and results cross as JSON text, which
//! keeps the boundary one function per operation and the result shapes documented in one place
//! (`meerschaum/core/Pipeline/`).
//!
//! The registry is the CLI's (`meerschaum_cli::native::registry_with`, the single
//! builder): native `sql:`, `api:` and `valkey:` connectors, then the Python worker pool and
//! native plugins for every other connector. `sql:` connectors the Python process built ad hoc
//! (not in any config registry) arrive as `connectors` attributes and are passed to it as
//! overrides, used in place of the configured ones.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use chrono::{DateTime, Utc};
use meerschaum_cli::native::session::ConfiguredSqlProvider;
use meerschaum_cli::native::{EngineRegistry, Instance, RegistryOptions, Session, registry_with};
use meerschaum_core::config::Config;
use meerschaum_core::success::Bound;
use meerschaum_engine::read::{EngineReader, RunFilter, RunStatus};
use meerschaum_engine::selection::KeyFilter;
use meerschaum_engine::{
    ConnectorRegistry, Depth, Engine, EngineConfig, Event, EventSink, NodeSyncOptions, PipeRef,
    RunOptions, RunResult, Selection,
};
use meerschaum_sql::SqlConnector;
use meerschaum_sql::instance::InstanceSettings;
use meerschaum_sql::uri::{ConnectionSpec, ConnectorAttributes};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use serde_json::{Value as Json, json};

fn config() -> PyResult<&'static Config> {
    static CONFIG: OnceLock<Result<Config, String>> = OnceLock::new();
    CONFIG
        .get_or_init(|| Config::from_env().map_err(|e| e.to_string()))
        .as_ref()
        .map_err(|e| PyRuntimeError::new_err(e.clone()))
}

/// One session (runtime, connectors, worker pool) per process: v4 job daemons fork, and a
/// forked child must not reuse its parent's runtime threads.
fn session() -> Session {
    static SESSION: Mutex<Option<(u32, Session)>> = Mutex::new(None);
    let pid = std::process::id();
    let mut slot = SESSION.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((owner, session)) = slot.as_ref()
        && *owner == pid
    {
        return session.clone();
    }
    let session = Session::new();
    *slot = Some((pid, session.clone()));
    session
}

fn value_err(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}

fn runtime_err(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

fn parse_request(request: &str) -> PyResult<Json> {
    serde_json::from_str(request).map_err(|e| value_err(format!("Invalid engine request: {e}")))
}

fn str_field<'a>(request: &'a Json, key: &str) -> Option<&'a str> {
    request.get(key).and_then(Json::as_str)
}

/// Python's `True` / positive `int` depth.
fn depth_field(request: &Json, key: &str) -> Option<Depth> {
    match request.get(key)? {
        Json::Bool(true) => Some(Depth::All),
        Json::Number(n) => n
            .as_i64()
            .filter(|n| *n > 0)
            .map(|n| Depth::N(u32::try_from(n).unwrap_or(u32::MAX))),
        _ => None,
    }
}

/// A sync bound: an ISO 8601 timestamp (UTC when naive) or an integer. `''` and `null` are the
/// engine default.
fn bound_field(request: &Json, key: &str) -> PyResult<Option<Bound>> {
    match request.get(key) {
        None | Some(Json::Null) => Ok(None),
        Some(Json::Number(n)) => Ok(n.as_i64().map(Bound::Int)),
        Some(Json::String(s)) if s.is_empty() => Ok(None),
        Some(Json::String(s)) => {
            let parsed = DateTime::parse_from_rfc3339(s)
                .map(|d| d.with_timezone(&Utc))
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                        .map(|d| d.and_utc())
                })
                .map_err(|_| value_err(format!("Invalid {key} '{s}'.")))?;
            Ok(Some(Bound::Datetime(parsed)))
        }
        Some(other) => Err(value_err(format!("Invalid {key}: {other}"))),
    }
}

fn sql_attributes(
    label: &str,
    attrs: &serde_json::Map<String, Json>,
) -> PyResult<ConnectorAttributes> {
    let mut out = ConnectorAttributes {
        label: Some(label.to_string()),
        ..Default::default()
    };
    for (key, value) in attrs {
        if value.is_null() {
            continue;
        }
        let text = match value {
            Json::String(s) => s.clone(),
            Json::Number(n) => n.to_string(),
            Json::Bool(b) => b.to_string(),
            _ => continue,
        };
        match key.as_str() {
            "flavor" => out.flavor = Some(text.parse().map_err(|e| value_err(format!("{e:?}")))?),
            "uri" => out.uri = Some(text),
            "host" => out.host = Some(text),
            "port" => {
                out.port = Some(
                    text.parse()
                        .map_err(|_| value_err(format!("Invalid port '{text}'.")))?,
                )
            }
            "username" => out.username = Some(text),
            "password" => out.password = Some(text),
            "database" => out.database = Some(text),
            "schema" => out.schema = Some(text),
            "label" => {}
            _ => out.extra.push((key.clone(), text)),
        }
    }
    Ok(out)
}

struct Built {
    engine: Engine,
    registry: Arc<dyn ConnectorRegistry>,
    config: EngineConfig,
    instance_keys: String,
}

/// Build the engine for a request through the CLI's registry builder
/// (`meerschaum_cli::native::registry_with`): the instance, every source
/// connector the instance's pipes name, the Python worker pool and native plugins, with the
/// `sql:` connectors the Python process built itself in place of configured ones.
fn build(session: &Session, request: &Json) -> PyResult<Built> {
    let config = config()?;
    let instance_keys = str_field(request, "instance")
        .ok_or_else(|| value_err("An engine request needs an `instance`."))?
        .to_string();

    // sql: connectors the Python process built itself.
    let mut overrides: HashMap<String, Instance> = HashMap::new();
    if let Some(Json::Object(connectors)) = request.get("connectors") {
        for (keys, attrs) in connectors {
            let Json::Object(attrs) = attrs else { continue };
            let label = keys.split_once(':').map_or(keys.as_str(), |(_, l)| l);
            let ca = sql_attributes(label, attrs)?;
            let spec =
                ConnectionSpec::from_attributes(&ca).map_err(|e| value_err(e.to_string()))?;
            let settings = InstanceSettings::from_config(spec.flavor, config);
            let conn = session
                .block_on(SqlConnector::connect_with(label, spec, settings))
                .map_err(|e| runtime_err(format!("Cannot connect to '{keys}': {e}")))?;
            conn.set_instance_provider(Arc::new(ConfiguredSqlProvider::new(config)));
            overrides.insert(keys.clone(), Instance::Sql(Arc::new(conn)));
        }
    }
    let home = match overrides.get(&instance_keys) {
        Some(inst) => inst.clone(),
        None => session.instance(&instance_keys, config).map_err(|e| {
            runtime_err(format!(
                "Cannot resolve the instance connector '{instance_keys}': {}",
                e.message
            ))
        })?,
    };
    // Only the pipes this request can reach have their connectors built. `--resume` re-plans a
    // stored selection, so every ancestor and descendant of the targets is in reach.
    let mut reach = selection(request, &instance_keys)?
        .unwrap_or_else(|| Selection::filter(KeyFilter::default()));
    if request.get("resume").is_some_and(|r| !r.is_null()) {
        reach = reach
            .with_parents(Some(Depth::All))
            .with_children(Some(Depth::All));
    }
    let EngineRegistry {
        registry,
        mut config,
        ..
    } = registry_with(
        config,
        session,
        &home,
        RegistryOptions {
            selection: reach,
            // PARITY: same-instance `sql:` pipes sync in place, as v4's `pipe.sync()` does,
            // so a graph run keeps the definition's column types (see `registry`).
            inplace: true,
            overrides,
        },
    )
    .map_err(|e| runtime_err(e.message))?;
    config.home_instance = home.keys();
    Ok(Built {
        engine: Engine::new(registry.clone(), config.clone()),
        registry,
        config,
        instance_keys,
    })
}

fn selection(request: &Json, instance_keys: &str) -> PyResult<Option<Selection>> {
    let base = if let Some(Json::Array(targets)) = request.get("targets")
        && !targets.is_empty()
    {
        let mut refs = Vec::new();
        for target in targets {
            let part = |i: usize| target.get(i).and_then(Json::as_str);
            let (Some(ck), Some(mk)) = (part(0), part(1)) else {
                return Err(value_err(format!("Invalid target {target}.")));
            };
            refs.push(PipeRef::new(
                part(3).unwrap_or(instance_keys),
                ck,
                mk,
                part(2),
            ));
        }
        Selection::refs(refs)
    } else if let Some(filter) = request.get("filter") {
        let mut filter: KeyFilter = serde_json::from_value(filter.clone())
            .map_err(|e| value_err(format!("Invalid filter: {e}")))?;
        filter
            .instance_keys
            .get_or_insert_with(|| instance_keys.to_string());
        Selection::filter(filter)
    } else {
        return Ok(None);
    };
    Ok(Some(
        base.with_parents(depth_field(request, "parents"))
            .with_children(depth_field(request, "children")),
    ))
}

fn run_options(request: &Json, instance_keys: &str) -> PyResult<RunOptions> {
    let sync = request.get("sync").cloned().unwrap_or(Json::Null);
    let mut node = NodeSyncOptions::default();
    if let Some(v) = sync.get("check_existing").and_then(Json::as_bool) {
        node.check_existing = v;
    }
    if let Some(v) = sync.get("enforce_dtypes").and_then(Json::as_bool) {
        node.enforce_dtypes = v;
    }
    if let Some(v) = sync.get("force").and_then(Json::as_bool) {
        node.force = v;
    }
    if let Some(v) = sync.get("retries").and_then(Json::as_u64) {
        node.retries = u32::try_from(v).unwrap_or(u32::MAX);
    }
    if let Some(v) = sync.get("min_seconds").and_then(Json::as_f64) {
        node.min_seconds = v;
    }
    match sync.get("chunksize") {
        Some(Json::Null) => node.chunksize = None,
        Some(Json::Number(n)) => node.chunksize = n.as_i64(),
        _ => {}
    }
    if let Some(params) = sync.get("params").filter(|p| p.is_object()) {
        node.params = Some(
            meerschaum_core::Value::from_json_str(&params.to_string())
                .map_err(|e| value_err(e.to_string()))?
                .to_map(),
        );
    }
    if let Some(extra) = sync.get("extra").filter(|p| p.is_object()) {
        node.extra = meerschaum_core::Value::from_json_str(&extra.to_string())
            .map_err(|e| value_err(e.to_string()))?
            .to_map();
    }
    Ok(RunOptions {
        selection: selection(request, instance_keys)?,
        instances: vec![instance_keys.to_string()],
        begin: bound_field(request, "begin")?,
        end: bound_field(request, "end")?,
        yes: request.get("yes").and_then(Json::as_bool).unwrap_or(false),
        resume: str_field(request, "resume").map(str::to_owned),
        workers: request
            .get("workers")
            .and_then(Json::as_u64)
            .filter(|w| *w > 0)
            .map(|w| w as usize),
        sync: node,
        run_id: str_field(request, "run_id").map(str::to_owned),
        ..Default::default()
    })
}

fn to_json_string<T: serde::Serialize>(value: &T) -> PyResult<String> {
    serde_json::to_string(value).map_err(|e| runtime_err(e.to_string()))
}

/// `engine_plan(request) -> json`: the plan only, no data movement.
///
/// Result: `{"plan": <Plan>, "text": <plan table>, "critical_path": [...], "warnings": [...]}`.
#[pyfunction]
pub fn engine_plan(py: Python<'_>, request: &str) -> PyResult<String> {
    let request = parse_request(request)?;
    let session = session();
    let built = build(&session, &request)?;
    let opts = run_options(&request, &built.instance_keys)?;
    let planned = py
        .allow_threads(|| session.block_on(built.engine.plan(&opts)))
        .map_err(runtime_err)?;
    to_json_string(&json!({
        "plan": planned.plan,
        "text": planned.plan.to_string(),
        "warnings": planned.plan.warnings,
    }))
}

/// Delivers a run's events to the Python thread that called `engine_run`.
struct EventChannel(std::sync::mpsc::Sender<Event>);

impl EventSink for EventChannel {
    fn emit(&self, event: Event) {
        let _ = self.0.send(event);
    }
}

/// How often the calling thread wakes up to check for signals (CTRL-C) while it waits.
const SIGNAL_POLL: std::time::Duration = std::time::Duration::from_millis(100);

/// Run `opts` on an engine thread and hand each event (as its JSON text) to `on_event` on the
/// calling thread, between waits that release the GIL: the sync work never holds the GIL, and
/// the callback runs in the thread that called `Pipeline.sync()`.
///
/// If `on_event` raises (the Python wrapper only lets `BaseException`s that are not
/// `Exception`s through, such as `KeyboardInterrupt`) or a signal handler raises while waiting,
/// the run is cancelled (its in-flight node syncs are dropped, as in a crash: the engine's
/// write ordering keeps the instance consistent) and the exception propagates.
fn run_with_events(
    py: Python<'_>,
    session: &Session,
    engine: Engine,
    mut opts: RunOptions,
    on_event: PyObject,
) -> PyResult<RunResult> {
    let (tx, rx) = std::sync::mpsc::channel::<Event>();
    // `allow_threads` needs a `Sync` receiver.
    let rx = Mutex::new(rx);
    opts.on_event = Some(Arc::new(EventChannel(tx)));
    let (cancel, cancelled) = tokio::sync::oneshot::channel::<()>();
    let worker = {
        let session = session.clone();
        std::thread::Builder::new()
            .name("mrsm-engine-run".into())
            .spawn(move || {
                session.block_on(async move {
                    tokio::select! {
                        result = engine.run(opts) => Some(result),
                        _ = cancelled => None,
                    }
                })
            })
            .map_err(|e| runtime_err(format!("Cannot start the engine run: {e}")))?
    };
    let mut raised: Option<PyErr> = None;
    loop {
        let next = py.allow_threads(|| {
            rx.lock()
                .unwrap_or_else(|e| e.into_inner())
                .recv_timeout(SIGNAL_POLL)
        });
        match next {
            Ok(event) => {
                let text = to_json_string(&event)?;
                if let Err(e) = on_event.call1(py, (text,)) {
                    raised = Some(e);
                    break;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if let Err(e) = py.check_signals() {
                    raised = Some(e);
                    break;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    if let Some(e) = raised {
        let _ = cancel.send(());
        let _ = py.allow_threads(|| worker.join());
        return Err(e);
    }
    py.allow_threads(|| worker.join())
        .map_err(|_| runtime_err("The engine run panicked."))?
        .ok_or_else(|| runtime_err("The engine run was cancelled."))
}

/// `engine_run(request, on_event=None) -> json`: plan and execute one engine run.
///
/// `on_event`, when given, is called with each event's JSON text as it happens (see
/// [`run_with_events`]); the Python façade decodes it (`meerschaum/core/Pipeline/_events.py`).
///
/// Result: `{"success", "message", "run_id", "plan", "text", "outcomes": [...], "record",
/// "critical_path", "warnings", "duration_seconds"}`.
#[pyfunction]
#[pyo3(signature = (request, on_event=None))]
pub fn engine_run(py: Python<'_>, request: &str, on_event: Option<PyObject>) -> PyResult<String> {
    let request = parse_request(request)?;
    let session = session();
    let built = build(&session, &request)?;
    let opts = run_options(&request, &built.instance_keys)?;
    let result = match on_event {
        Some(callback) => run_with_events(py, &session, built.engine, opts, callback)?,
        None => py.allow_threads(|| session.block_on(built.engine.run(opts))),
    };
    let outcomes: Vec<Json> = result
        .outcomes
        .iter()
        .map(|o| {
            json!({
                "pipe_ref": o.pipe_ref,
                "pipe": o.pipe,
                "result": o.result,
            })
        })
        .collect();
    to_json_string(&json!({
        "success": result.success,
        "message": result.message,
        "run_id": result.record.as_ref().map(|r| r.run.run_id.clone()),
        "plan": result.plan,
        "text": result.plan.as_ref().map(|p| p.to_string()),
        "outcomes": outcomes,
        "record": result.record,
        "critical_path": result.critical_path,
        "warnings": result.warnings,
        "duration_seconds": result.duration.as_secs_f64(),
    }))
}

fn reader(request: &Json) -> PyResult<(Session, EngineReader)> {
    let session = session();
    let built = build(&session, request)?;
    Ok((session, EngineReader::new(built.registry, built.config)))
}

/// `engine_runs(request) -> json`: run history, newest first (`limit`, `status`, `pipe`).
#[pyfunction]
pub fn engine_runs(py: Python<'_>, request: &str) -> PyResult<String> {
    let request = parse_request(request)?;
    let (session, reader) = reader(&request)?;
    let status = match str_field(&request, "status") {
        Some("running") => Some(RunStatus::Running),
        Some("succeeded") => Some(RunStatus::Succeeded),
        Some("failed") => Some(RunStatus::Failed),
        Some(other) => return Err(value_err(format!("Unknown run status '{other}'."))),
        None => None,
    };
    let filter = RunFilter {
        run_id: str_field(&request, "run_id").map(str::to_owned),
        status,
        pipe: str_field(&request, "pipe").map(str::to_owned),
        since: None,
        limit: request
            .get("limit")
            .and_then(Json::as_u64)
            .map(|n| n as usize),
    };
    let runs = py
        .allow_threads(|| session.block_on(reader.runs(&filter)))
        .map_err(|e| runtime_err(e.to_string()))?;
    to_json_string(&runs)
}

/// `engine_run_detail(request) -> json`: one run with node rows and Gantt bars (`null` when
/// the run is unknown).
#[pyfunction]
pub fn engine_run_detail(py: Python<'_>, request: &str) -> PyResult<String> {
    let request = parse_request(request)?;
    let (session, reader) = reader(&request)?;
    let run_id = str_field(&request, "run_id").ok_or_else(|| value_err("`run_id` is required."))?;
    let detail = py
        .allow_threads(|| session.block_on(reader.run_detail(run_id)))
        .map_err(|e| runtime_err(e.to_string()))?;
    to_json_string(&detail)
}

/// `engine_run_events(request) -> json`: persisted events of a run (`after_seq` optional).
#[pyfunction]
pub fn engine_run_events(py: Python<'_>, request: &str) -> PyResult<String> {
    let request = parse_request(request)?;
    let (session, reader) = reader(&request)?;
    let run_id = str_field(&request, "run_id").ok_or_else(|| value_err("`run_id` is required."))?;
    let after = request.get("after_seq").and_then(Json::as_u64);
    let rows = py
        .allow_threads(|| session.block_on(reader.events_after(run_id, after)))
        .map_err(|e| runtime_err(e.to_string()))?;
    to_json_string(&rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_is_true_or_a_positive_count() {
        let depth = |v: Json| depth_field(&json!({ "parents": v }), "parents");
        assert_eq!(depth(json!(true)), Some(Depth::All));
        assert_eq!(depth(json!(2)), Some(Depth::N(2)));
        assert_eq!(depth(json!(0)), None);
        assert_eq!(depth(json!(false)), None);
        assert_eq!(depth(Json::Null), None);
        assert_eq!(depth_field(&json!({}), "parents"), None);
    }

    #[test]
    fn bounds_parse_iso_ints_and_defaults() {
        let bound = |v: Json| bound_field(&json!({ "begin": v }), "begin").unwrap();
        assert_eq!(bound(json!("")), None);
        assert_eq!(bound(Json::Null), None);
        assert_eq!(bound(json!(5)), Some(Bound::Int(5)));
        let naive = bound(json!("2024-01-02T03:04:05")).unwrap();
        let aware = bound(json!("2024-01-02T03:04:05+00:00")).unwrap();
        assert_eq!(naive, aware);
        assert!(bound_field(&json!({ "begin": "not a date" }), "begin").is_err());
    }

    #[test]
    fn selection_prefers_targets_and_applies_depths() {
        let request = json!({
            "targets": [["sql:a", "m", null, "sql:i"], ["sql:a", "n", "loc"]],
            "parents": true,
            "children": 1,
        });
        let selection = selection(&request, "sql:home").unwrap().unwrap();
        assert_eq!(selection.parents, Some(Depth::All));
        assert_eq!(selection.children, Some(Depth::N(1)));
        assert!(!selection.is_flat());
        let flat = selection_for_test(&json!({ "targets": [["sql:a", "m"]] }));
        assert!(flat.is_flat());
        assert!(
            super::selection(&json!({ "targets": [["sql:a"]] }), "sql:home").is_err(),
            "a target needs connector and metric keys"
        );
        assert!(super::selection(&json!({}), "sql:home").unwrap().is_none());
    }

    fn selection_for_test(request: &Json) -> Selection {
        super::selection(request, "sql:home").unwrap().unwrap()
    }

    #[test]
    fn sql_attributes_map_known_keys_and_keep_the_rest() {
        let attrs = json!({
            "flavor": "sqlite",
            "database": "/tmp/x.db",
            "port": 5432,
            "host": null,
            "sslmode": "require",
        });
        let out = sql_attributes("lbl", attrs.as_object().unwrap()).unwrap();
        assert_eq!(out.label.as_deref(), Some("lbl"));
        assert_eq!(out.database.as_deref(), Some("/tmp/x.db"));
        assert_eq!(out.port, Some(5432));
        assert!(out.host.is_none());
        assert_eq!(
            out.extra,
            vec![("sslmode".to_string(), "require".to_string())]
        );
    }
}
