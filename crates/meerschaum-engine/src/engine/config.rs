//! [`EngineConfig`]: the engine's settings read from `system.engine.*`, and which events a run
//! persists ([`EventGranularity`]).

use std::path::PathBuf;
use std::time::Duration;

use meerschaum_core::pipe::PipeDefaults;
use meerschaum_core::{Config, Value};

use crate::events::EventKind;
use crate::executor::Limits;

/// Which events are persisted to `mrsm:engine_events` (`system.engine.events`, default `all`).
///
/// `all` keeps the chunk-level events (`chunk_started`, `chunk_finished`, `node_planned`,
/// `stream_flushed`) that the console's live run view and `GET /engine/runs/{run_id}/events/stream`
/// replay; they roughly double the rows of a run whose nodes sync one chunk each. Retention
/// (`system.engine.run_history_days`) prunes them with the rest of the run's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EventGranularity {
    /// None.
    Off,
    /// Run and node transitions (no `node_planned`, `chunk_*` or `stream_flushed`).
    Nodes,
    /// Every event.
    #[default]
    All,
}

impl EventGranularity {
    pub(super) fn keeps(&self, kind: &EventKind) -> bool {
        match self {
            EventGranularity::Off => false,
            EventGranularity::All => true,
            EventGranularity::Nodes => !matches!(
                kind,
                EventKind::NodePlanned { .. }
                    | EventKind::ChunkStarted { .. }
                    | EventKind::ChunkFinished { .. }
                    | EventKind::StreamFlushed { .. }
            ),
        }
    }
}

/// Engine configuration (`system.engine.*` plus the pipe defaults it needs).
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Pipe defaults (`meerschaum:instance`, `verify.chunk_minutes`, ...).
    pub defaults: PipeDefaults,
    /// The home instance: runs, nodes and events are stored here, and pipes are read from it
    /// when a run names no instances.
    pub home_instance: String,
    /// `pipes:parameters:fetch:backtrack_minutes`.
    pub default_backtrack_minutes: i64,
    /// `pipes:sync:filter_params_index_limit`.
    pub filter_params_index_limit: usize,
    /// `system.engine.implicit_edges` (R7).
    pub implicit_edges: bool,
    /// The global worker count.
    pub workers: usize,
    /// `system.engine.limits` and `system.engine.python_workers`.
    pub limits: Limits,
    /// `system.engine.auto_rebuild`.
    pub auto_rebuild: bool,
    /// `system.engine.inclusive_cap`: cap children at their parents' watermark plus one unit
    /// (see [`PlanOptions::inclusive_cap`](crate::plan::PlanOptions::inclusive_cap)).
    pub inclusive_cap: bool,
    /// `system.engine.flat_ledger`: write ledger entries in flat runs (laps) too.
    pub flat_ledger: bool,
    /// Persist runs, nodes and events (`false`: memory only).
    pub persist_runs: bool,
    /// Store history for loop laps whose entire plan is skipped.
    pub persist_skipped_laps: bool,
    /// `system.engine.events`: persisted event granularity.
    pub events: EventGranularity,
    /// `system.engine.state_flush_ms`.
    pub state_flush: Duration,
    /// `system.engine.run_history_days`.
    pub run_history_days: i64,
    /// Maximum wait for a phase of sync hooks.
    pub sync_hook_timeout: Duration,
    /// Lease TTL (renewed every third).
    pub lease_ttl: Duration,
    /// How long to wait for a lease held elsewhere.
    pub lease_wait: Duration,
    /// How often to poll a held lease.
    pub lease_poll: Duration,
    /// Where machine-local fallback lease files live (`{root}/cache/engine_leases`).
    pub lease_dir: PathBuf,
    /// The lease holder name (a job name, else `mrsm:<host>:<pid>`,
    /// [`meerschaum_core::holder::process_holder`]).
    pub holder: String,
    /// EWMA smoothing of durations.
    pub ewma_alpha: f64,
    /// Expected duration of a node without history (`system.engine.default_duration_seconds`,
    /// default 30): the planned length of its Gantt bar and its CPM duration.
    pub default_duration: Duration,
    /// `system.engine.chunk_pipelining` (R6; accepted, not implemented).
    pub chunk_pipelining: bool,
    /// `system.engine.debounce_seconds`: the minimum time between two triggered runs of a
    /// child of a stream source when the child sets no `min_seconds` of its own (`None`: use
    /// the loop's `--min-seconds`). See `docs/design/v5.0.0.md` §5.10.
    pub debounce_seconds: Option<f64>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            defaults: PipeDefaults::default(),
            home_instance: "sql:main".into(),
            default_backtrack_minutes: 1440,
            filter_params_index_limit: 250,
            implicit_edges: true,
            workers: 4,
            limits: Limits::default(),
            auto_rebuild: false,
            inclusive_cap: true,
            flat_ledger: true,
            persist_runs: true,
            persist_skipped_laps: true,
            events: EventGranularity::All,
            state_flush: Duration::from_millis(500),
            run_history_days: 30,
            sync_hook_timeout: Duration::from_secs(5),
            lease_ttl: Duration::from_secs(60),
            lease_wait: Duration::from_secs(30),
            lease_poll: Duration::from_millis(500),
            lease_dir: std::env::temp_dir().join("mrsm_engine_leases"),
            holder: meerschaum_core::holder::process_holder(),
            ewma_alpha: 0.3,
            default_duration: Duration::from_secs(30),
            chunk_pipelining: false,
            debounce_seconds: None,
        }
    }
}

impl EngineConfig {
    /// Bound duration samples per pipe while retaining the smoothing factor's recent tail.
    pub(crate) fn duration_history_limit(&self) -> usize {
        let alpha = self.ewma_alpha.clamp(f64::EPSILON, 1.0);
        ((1e-6_f64.ln() / (-alpha).ln_1p()).ceil() as usize).clamp(64, 1024)
    }

    /// Read `system.engine.*` from a config registry; `root_dir` locates the lease fallback.
    pub fn from_config(config: &Config, root_dir: Option<&std::path::Path>) -> EngineConfig {
        let d = EngineConfig::default();
        let get = |keys: &[&str]| config.get(keys);
        let int = |keys: &[&str]| get(keys).as_ref().and_then(meerschaum_core::pyfmt::py_int);
        let flag = |keys: &[&str], default: bool| get(keys).map_or(default, |v| v.truthy());
        let mut limits = Limits::default();
        if let Some(Value::Map(m)) = get(&["system", "engine", "limits", "connectors"]) {
            for (k, v) in m {
                if let Some(n) = meerschaum_core::pyfmt::py_int(&v) {
                    limits.connectors.insert(k, n.max(0) as usize);
                }
            }
        }
        if let Some(Value::Map(m)) = get(&["system", "engine", "limits", "instances"]) {
            for (k, v) in m {
                if let Some(n) = meerschaum_core::pyfmt::py_int(&v) {
                    limits.instances.insert(k, n.max(0) as usize);
                }
            }
        }
        limits.python_workers =
            int(&["system", "engine", "python_workers"]).map(|n| n.max(0) as usize);
        let defaults = PipeDefaults::from_config(config);
        EngineConfig {
            home_instance: defaults.instance_keys.clone(),
            defaults,
            default_backtrack_minutes: int(&["pipes", "parameters", "fetch", "backtrack_minutes"])
                .unwrap_or(d.default_backtrack_minutes),
            filter_params_index_limit: int(&["pipes", "sync", "filter_params_index_limit"])
                .map_or(d.filter_params_index_limit, |n| n.max(0) as usize),
            implicit_edges: flag(&["system", "engine", "implicit_edges"], d.implicit_edges),
            workers: int(&["system", "engine", "workers"]).map_or(d.workers, |n| n.max(1) as usize),
            limits,
            auto_rebuild: flag(&["system", "engine", "auto_rebuild"], d.auto_rebuild),
            inclusive_cap: flag(&["system", "engine", "inclusive_cap"], d.inclusive_cap),
            flat_ledger: flag(&["system", "engine", "flat_ledger"], d.flat_ledger),
            persist_runs: flag(&["system", "engine", "persist_runs"], d.persist_runs),
            persist_skipped_laps: flag(
                &["system", "engine", "persist_skipped_laps"],
                d.persist_skipped_laps,
            ),
            events: match get(&["system", "engine", "events"])
                .map(|v| meerschaum_core::pyfmt::py_str(&v))
            {
                Some(s) if s == "all" => EventGranularity::All,
                Some(s) if s == "nodes" => EventGranularity::Nodes,
                Some(s) if s == "off" || s == "False" => EventGranularity::Off,
                _ => d.events,
            },
            state_flush: int(&["system", "engine", "state_flush_ms"])
                .map_or(d.state_flush, |ms| Duration::from_millis(ms.max(0) as u64)),
            sync_hook_timeout: get(&["system", "engine", "sync_hook_timeout_seconds"])
                .and_then(|v| v.as_f64())
                .and_then(|v| Duration::try_from_secs_f64(v).ok())
                .filter(|d| !d.is_zero())
                .unwrap_or(d.sync_hook_timeout),
            run_history_days: int(&["system", "engine", "run_history_days"])
                .unwrap_or(d.run_history_days),
            lease_dir: root_dir
                .map(|r| r.join("cache").join("engine_leases"))
                .unwrap_or(d.lease_dir),
            chunk_pipelining: flag(&["system", "engine", "chunk_pipelining"], false),
            default_duration: get(&["system", "engine", "default_duration_seconds"])
                .as_ref()
                .and_then(|v| v.as_f64())
                .filter(|s| s.is_finite() && *s >= 0.0)
                .map_or(d.default_duration, Duration::from_secs_f64),
            debounce_seconds: get(&["system", "engine", "debounce_seconds"])
                .as_ref()
                .and_then(|v| v.as_f64())
                .filter(|s| *s >= 0.0),
            ..d
        }
    }
}
