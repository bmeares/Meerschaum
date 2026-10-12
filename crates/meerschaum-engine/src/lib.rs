//! `meerschaum-engine`: the dependency-aware Syncing Engine (`docs/design/v5.0.0.md` §5).
//!
//! The engine understands the pipe graph, decides which syncs need to run and how wide their
//! bounds must be, runs independent branches in parallel, and records what happened. This crate
//! is pure logic written against traits; it holds no instance connector of its own:
//!
//! | Module | Spec | What it does |
//! |---|---|---|
//! | [`node`] | §5.1 | [`PipeRef`] node identity and [`PipeRecord`] (one `fetch_pipes_keys` row) |
//! | [`graph`] | §5.1 | The implicit pipe graph: declared and `{{ Pipe() }}` edges, cycles, stale references |
//! | [`selection`] | §5.2, D4, D5 | [`Selection`] / [`Pipeline`]: targets plus traversal depths, key filters |
//! | [`axis`] | §5.4 | Datetime/integer axes, half-open intervals, interval union, epoch-aligned grids |
//! | [`ledger`] | §5.4 | Ledger entries, edge cursors, range mapping across edges, compaction, definition hash |
//! | [`state`] | §5.3, §5.8 | [`StateSource`] / [`StateWriter`] traits and an in-memory implementation |
//! | [`mod@plan`] | §5.3 | The planner: one [`Action`] per node with bounds, chunks and a reason |
//! | [`schedule`] | §5.5 | EWMA durations, HEFT upward rank, CPM earliest/latest/slack |
//! | [`executor`] | §5.5, D11 | Kahn ready queue on tokio, semaphores, leases, retries, failure propagation |
//! | [`events`] | §5.7 | The typed event stream and [`EventSink`] |
//! | [`runs`] | §5.8 | Run history rows and the internal `mrsm:engine_*` pipes |
//! | [`stream`] | §5.6 | Micro-batching triggers for stream sources |
//! | [`viz`] | §5.9 | The static ASCII Gantt chart and DAG listing |
//!
//! The real `Pipe::sync` (a [`SyncExecutor`]), the instance-backed [`StateSource`] and
//! [`LeaseSource`], continuous mode and chunk pipelining arrive in later waves.

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod axis;
pub mod events;
pub mod executor;
pub mod graph;
pub mod ledger;
pub mod node;
pub mod plan;
pub mod runs;
pub mod schedule;
pub mod selection;
pub mod state;
pub mod stream;
pub mod viz;

pub use axis::{Axis, AxisKind, Interval};
pub use events::{Event, EventKind, EventSink};
pub use executor::{
    ExecuteContext, ExecutorOptions, LeaseSource, Limits, RetryPolicy, SyncExecutor, SyncRequest,
    execute,
};
pub use graph::{Edge, EdgeOptions, Graph, GraphError, GraphOptions, GraphWarning, Node};
pub use ledger::{EdgeCursor, LedgerEntry, definition_hash};
pub use node::{PipeRecord, PipeRef};
pub use plan::{Action, Begin, Bounds, Plan, PlanOptions, PlannedNode, Span, plan};
pub use runs::{NodeRow, NodeStatus, RunRecord, RunRow};
pub use schedule::{DurationSource, EwmaDurations, Schedule};
pub use selection::{Depth, KeyFilter, Pipeline, Selection, Subgraph, Targets};
pub use state::{MemoryState, StateError, StateSource, StateWriter};

/// Re-exported from `meerschaum-core`: a sync bound on a datetime or integer axis.
pub use meerschaum_core::success::{Bound, SyncResult};
