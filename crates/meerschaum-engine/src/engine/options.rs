//! The options of one run ([`RunOptions`]) and of continuous mode ([`LoopOptions`]).

use std::collections::BTreeSet;
use std::sync::Arc;

use meerschaum_core::success::Bound;

use crate::events::EventSink;
use crate::native::NodeSyncOptions;
use crate::node::PipeRef;
use crate::plan::Plan;
use crate::selection::Selection;
use crate::stream::StopToken;

/// Asks the user to confirm rebuilds (interactive runs).
pub type ConfirmFn = Arc<dyn Fn(&Plan) -> bool + Send + Sync>;

/// One engine run's options (`sync pipes` flags, `pipe.sync(parents=...)`, `Pipeline.run`).
#[derive(Clone, Default)]
pub struct RunOptions {
    /// The selection.
    pub selection: Option<Selection>,
    /// The instances to read pipes from (default: the home instance).
    pub instances: Vec<String>,
    /// `--begin`.
    pub begin: Option<Bound>,
    /// `--end`.
    pub end: Option<Bound>,
    /// `--yes` / `force` for rebuilds.
    pub yes: bool,
    /// A person can confirm rebuilds.
    pub interactive: bool,
    /// Asked when a rebuild needs confirmation (interactive runs).
    pub confirm: Option<ConfirmFn>,
    /// `--resume <run_id>`.
    pub resume: Option<String>,
    /// `-w`: overrides the configured worker count.
    pub workers: Option<usize>,
    /// Per-node sync flags.
    pub sync: NodeSyncOptions,
    /// Use this run ID.
    pub run_id: Option<String>,
    /// Frozen registered definitions admitted to this run (identity and revision).
    pub pipeline_definitions: Vec<serde_json::Value>,
    /// Nodes to leave out of a flat selection (stream sources handled by continuous mode).
    pub exclude: BTreeSet<PipeRef>,
    /// Nodes held back from this run, with every descendant of theirs (any selection): used by
    /// the stream triggers to honor a child's `min_seconds`. Unlike `exclude`, a held node is
    /// removed after the selection is resolved, so its in-selection parents still run.
    pub hold: BTreeSet<PipeRef>,
    /// `--chunk-pipelining` for this run (R6); `None` uses `system.engine.chunk_pipelining`.
    pub chunk_pipelining: Option<bool>,
    /// Also deliver this run's events here (every event, in emission order, regardless of
    /// `system.engine.events`), in addition to [`Engine::subscribe`](super::Engine::subscribe). The Python bindings use
    /// it for `Pipeline.sync(on_event=...)`. `emit` must not block.
    pub on_event: Option<Arc<dyn EventSink>>,
}

impl std::fmt::Debug for RunOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunOptions")
            .field("selection", &self.selection)
            .field("instances", &self.instances)
            .field("resume", &self.resume)
            .field("workers", &self.workers)
            .finish_non_exhaustive()
    }
}

/// Continuous-mode options.
#[derive(Debug, Clone, Default)]
pub struct LoopOptions {
    /// `--loop`: keep running laps.
    pub run_loop: bool,
    /// `--listen`: keep stream sources (and v4-pattern connectors) running after the laps.
    pub listen: bool,
    /// `--min-seconds` (v4 default 1).
    pub min_seconds: f64,
    /// Stop after this many laps (tests, `--loop` with a budget).
    pub max_laps: Option<u64>,
    /// The stop signal (`stop jobs`, CTRL-C).
    pub stop: StopToken,
    /// Name each lap's run after this (`{name}-{timestamp}-{suffix}`,
    /// [`crate::runs::new_named_run_id`]): a saved Pipeline's loop. `None`: the default ids. A
    /// configured [`RunOptions::run_id`] still names the first lap only.
    pub run_name: Option<String>,
}
