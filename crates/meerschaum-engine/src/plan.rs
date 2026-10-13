//! The planner (§5.3): one [`Action`] per selected node.
//!
//! | Action | When | Bounds |
//! |---|---|---|
//! | `skip` | no unconsumed ledger entries on any in-selection parent edge, no parent watermark advanced since its edge cursor moved, definition unchanged | none |
//! | `incremental` | a parent watermark advanced since the cursor moved (or entries exist only at the tail) | `[own watermark − backtrack, min(parent watermarks))` |
//! | `backfill` | unconsumed dirty ranges from parents that start before the tail | the dirty ranges mapped onto this node's chunk grid |
//! | `rebuild` | the definition hash changed | the full range, capped (cleared first) |
//! | `rebuild_pending` | `rebuild` without `--yes` in a non-interactive context | none; descendants run against the stale data |
//! | `observe` | the node is a stream source | none |
//! | `root` | no parents in the selection | v4 semantics (`begin=''`) |
//! | `upstream_failed` | a parent failed, or a parent's instance cannot be reached | none |
//!
//! **The edge cursor is the primary signal**; the watermark comparison is the fallback for
//! edges without a cursor (their first engine run). Inside an engine run a child is never
//! synced past `min(parent watermarks)` (the capping rule). Flat selections keep v4 semantics:
//! every node is `root`.
//!
//! The static [`plan`] evaluates every node against the state **as it is now**. A node whose
//! parents will run first is marked [`PlannedNode::provisional`]: the executor re-plans it with
//! [`plan_node`] once its parents finish, against the state their syncs produced.

use std::collections::{BTreeSet, HashMap};
use std::fmt;

use meerschaum_core::success::Bound;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::axis::{Axis, AxisKind, Interval, get_chunk_bounds, union};
use crate::graph::{Edge, Graph};
use crate::ledger::{definition_changed, dirty_intervals, map_to_child, map_watermark};
use crate::node::PipeRef;
use crate::selection::Subgraph;
use crate::state::StateSource;

/// What the engine does with a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// Nothing changed upstream.
    Skip,
    /// Sync the tail: from the node's own watermark minus backtrack, capped.
    Incremental,
    /// Re-sync the dirty ranges reported by parents' ledger entries.
    Backfill,
    /// Clear and fully re-sync because the definition changed.
    Rebuild,
    /// A rebuild is needed but was not confirmed (non-interactive context).
    RebuildPending,
    /// A stream source: watermark and ledger are read only.
    Observe,
    /// No parents in the selection: a v4 sync.
    Root,
    /// A parent failed or cannot be reached.
    UpstreamFailed,
}

impl Action {
    /// The snake_case name.
    pub fn name(&self) -> &'static str {
        match self {
            Action::Skip => "skip",
            Action::Incremental => "incremental",
            Action::Backfill => "backfill",
            Action::Rebuild => "rebuild",
            Action::RebuildPending => "rebuild_pending",
            Action::Observe => "observe",
            Action::Root => "root",
            Action::UpstreamFailed => "upstream_failed",
        }
    }

    /// Whether the action syncs data.
    pub fn runs(&self) -> bool {
        matches!(
            self,
            Action::Root | Action::Incremental | Action::Backfill | Action::Rebuild
        )
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The lower bound of a sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Begin {
    /// v4's `begin=''`: the sync time minus the backtrack window, resolved by the sync.
    /// Serialized as `""`.
    Default,
    /// No lower bound. Serialized as `null`.
    Unbounded,
    /// An explicit bound.
    At(Bound),
}

impl Begin {
    /// The explicit bound, if any.
    pub fn bound(&self) -> Option<Bound> {
        match self {
            Begin::At(b) => Some(*b),
            _ => None,
        }
    }
}

impl Serialize for Begin {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Begin::Default => s.serialize_str(""),
            Begin::Unbounded => s.serialize_none(),
            Begin::At(b) => b.serialize(s),
        }
    }
}

impl<'de> Deserialize<'de> for Begin {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Begin, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Bound(Bound),
            Str(String),
        }
        match Option::<Repr>::deserialize(d)? {
            None => Ok(Begin::Unbounded),
            Some(Repr::Bound(b)) => Ok(Begin::At(b)),
            Some(Repr::Str(s)) if s.is_empty() => Ok(Begin::Default),
            Some(Repr::Str(s)) => Err(serde::de::Error::custom(format!(
                "invalid begin '{s}': expected '', null or a bound"
            ))),
        }
    }
}

/// A sync's overall bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bounds {
    /// Lower bound.
    pub begin: Begin,
    /// Exclusive upper bound (`None` = unbounded).
    pub end: Option<Bound>,
}

/// A half-open range `[begin, end)` of bounds (a dirty range or a chunk).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    /// Inclusive lower bound.
    pub begin: Option<Bound>,
    /// Exclusive upper bound.
    pub end: Option<Bound>,
}

/// What a successful sync consumes from one parent edge: the cursor to write afterwards.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Consumption {
    /// The parent.
    pub parent_ref: PipeRef,
    /// The child (this node).
    pub child_ref: PipeRef,
    /// Where the cursor lives (the child's instance).
    pub state_instance: String,
    /// The parent sequence number to advance to.
    pub seq: i64,
    /// The parent watermark observed while planning.
    pub parent_watermark: Option<Bound>,
}

/// One node's plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlannedNode {
    /// The node.
    pub pipe_ref: PipeRef,
    /// The action.
    pub action: Action,
    /// Overall bounds (`None` when nothing syncs).
    pub bounds: Option<Bounds>,
    /// The dirty ranges of a `backfill` (on the node's chunk grid, capped).
    #[serde(default)]
    pub ranges: Vec<Span>,
    /// The chunk list (empty when the sync chooses its own chunks: `root`, `rebuild`, or an
    /// unbounded range).
    #[serde(default)]
    pub chunks: Vec<Span>,
    /// Why.
    pub reason: String,
    /// Parents run first; the executor re-plans this node when they finish.
    #[serde(default)]
    pub provisional: bool,
    /// A `rebuild` waiting for interactive confirmation.
    #[serde(default)]
    pub requires_confirmation: bool,
    /// Clear the pipe before syncing (`rebuild`).
    #[serde(default)]
    pub clear: bool,
    /// Cursors to write after a successful sync.
    #[serde(default)]
    pub consumes: Vec<Consumption>,
    /// The node's current definition hash.
    pub definition_hash: String,
    /// The stored definition hash.
    #[serde(default)]
    pub stored_definition_hash: Option<String>,
    /// The node's own watermark while planning.
    #[serde(default)]
    pub watermark: Option<Bound>,
    /// Planning warnings for this node.
    #[serde(default)]
    pub warnings: Vec<String>,
}

impl PlannedNode {
    fn new(pipe_ref: &PipeRef, definition_hash: &str) -> PlannedNode {
        PlannedNode {
            pipe_ref: pipe_ref.clone(),
            action: Action::Skip,
            bounds: None,
            ranges: Vec::new(),
            chunks: Vec::new(),
            reason: String::new(),
            provisional: false,
            requires_confirmation: false,
            clear: false,
            consumes: Vec::new(),
            definition_hash: definition_hash.to_string(),
            stored_definition_hash: None,
            watermark: None,
            warnings: Vec::new(),
        }
    }

    fn set(mut self, action: Action, reason: impl Into<String>) -> PlannedNode {
        self.action = action;
        self.reason = reason.into();
        self
    }
}

/// Planner options.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PlanOptions {
    /// `pipeline.plan(begin=...)`: the lower bound for roots, and a clamp for everyone else.
    pub begin: Option<Bound>,
    /// `pipeline.plan(end=...)`: the upper bound for roots, and a clamp for everyone else.
    pub end: Option<Bound>,
    /// Whether a person can confirm a rebuild (a TTY, not a job, `--loop` or the API).
    pub interactive: bool,
    /// `--yes` / `force`.
    pub yes: bool,
    /// `system.engine.auto_rebuild`.
    pub auto_rebuild: bool,
    /// `--resume`: nodes that already succeeded in the resumed run (planned as `skip`).
    pub completed: BTreeSet<PipeRef>,
    /// The resumed run's ID, for reasons.
    pub resumed_from: Option<String>,
    /// Above this many chunks a range is listed as one chunk (the sync chunks it itself).
    pub max_chunks: usize,
    /// Widen the cap past each parent's watermark by one unit (one minute on a datetime axis,
    /// 1 on an integer axis), as Python's `get_chunk_bounds` widens its end, so a child also
    /// syncs the rows at exactly its parents' watermark. `false` keeps the strict half-open
    /// cap `[.., min(parent watermarks))` the golden fixtures were written with.
    pub inclusive_cap: bool,
}

impl Default for PlanOptions {
    fn default() -> Self {
        PlanOptions {
            begin: None,
            end: None,
            interactive: false,
            yes: false,
            auto_rebuild: false,
            completed: BTreeSet::new(),
            resumed_from: None,
            max_chunks: 10_000,
            inclusive_cap: false,
        }
    }
}

/// A parent's state as seen by a child being planned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParentState {
    /// Planned to sync in this run (static plan only).
    WillRun,
    /// Will not sync in this run (`skip`, `observe`, `rebuild_pending`).
    Idle,
    /// Synced successfully in this run.
    Succeeded,
    /// Failed, or itself `upstream_failed`.
    Failed,
}

impl ParentState {
    /// The state implied by a planned action.
    pub fn from_action(action: Action) -> ParentState {
        match action {
            a if a.runs() => ParentState::WillRun,
            Action::UpstreamFailed => ParentState::Failed,
            _ => ParentState::Idle,
        }
    }
}

/// A plan: one [`PlannedNode`] per selected node, in dependency order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Plan {
    /// The nodes in dependency order.
    pub nodes: Vec<PlannedNode>,
    /// In-selection edges `(parent, child)`.
    pub edges: Vec<(PipeRef, PipeRef)>,
    /// Whether the selection is flat (v4 semantics).
    pub flat: bool,
    /// Selection and graph warnings.
    pub warnings: Vec<String>,
}

impl Plan {
    /// A node's plan.
    pub fn node(&self, pipe_ref: &PipeRef) -> Option<&PlannedNode> {
        self.nodes.iter().find(|n| n.pipe_ref == *pipe_ref)
    }

    /// Whether any rebuild waits for confirmation.
    pub fn needs_confirmation(&self) -> bool {
        self.nodes.iter().any(|n| n.requires_confirmation)
    }

    /// The user confirmed: every pending-confirmation rebuild runs.
    pub fn confirm_rebuilds(&mut self) {
        for n in &mut self.nodes {
            n.requires_confirmation = false;
        }
    }

    /// The user declined: every pending-confirmation rebuild becomes `rebuild_pending`.
    pub fn decline_rebuilds(&mut self) {
        for n in &mut self.nodes {
            if n.requires_confirmation {
                n.requires_confirmation = false;
                n.action = Action::RebuildPending;
                n.bounds = None;
                n.clear = false;
                n.consumes.clear();
                n.reason = format!("{}; declined", n.reason);
            }
        }
    }
}

/// One unit past a watermark for [`PlanOptions::inclusive_cap`]: a minute on a datetime axis
/// (`get_chunk_bounds`: `end += timedelta(minutes=1)`), 1 on an integer axis.
fn cap_unit(axis: &Axis) -> i64 {
    match axis.kind {
        AxisKind::Datetime => 60_000_000,
        _ => 1,
    }
}

/// Format a bound for humans: `2026-10-12 09:00` or an integer.
pub fn fmt_bound(b: &Bound) -> String {
    match b {
        Bound::Int(i) => i.to_string(),
        Bound::Datetime(dt) => {
            use chrono::Timelike;
            if dt.second() == 0 && dt.nanosecond() == 0 {
                dt.format("%Y-%m-%d %H:%M").to_string()
            } else if dt.nanosecond() == 0 {
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
            }
        }
    }
}

fn fmt_opt(b: Option<&Bound>) -> String {
    b.map_or("None".to_string(), fmt_bound)
}

fn fmt_bounds(b: &Bounds) -> String {
    let begin = match &b.begin {
        Begin::Default => "''".to_string(),
        Begin::Unbounded => "None".to_string(),
        Begin::At(x) => fmt_bound(x),
    };
    format!("[{begin}, {})", fmt_opt(b.end.as_ref()))
}

impl fmt::Display for Plan {
    /// The plan table: `node | action | bounds | chunks | reason`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let instances: BTreeSet<&str> = self
            .nodes
            .iter()
            .map(|n| n.pipe_ref.instance_keys())
            .collect();
        let rows: Vec<[String; 5]> = self
            .nodes
            .iter()
            .map(|n| {
                let label = if instances.len() > 1 {
                    n.pipe_ref.label_with_instance()
                } else {
                    n.pipe_ref.label()
                };
                let mut action = n.action.to_string();
                if n.requires_confirmation {
                    action.push('?');
                }
                if n.provisional {
                    action.push('*');
                }
                let bounds = if n.action == Action::Backfill && n.ranges.len() > 1 {
                    n.ranges
                        .iter()
                        .map(|s| {
                            fmt_bounds(&Bounds {
                                begin: s.begin.map_or(Begin::Unbounded, Begin::At),
                                end: s.end,
                            })
                        })
                        .collect::<Vec<_>>()
                        .join(" ∪ ")
                } else {
                    n.bounds.as_ref().map_or("-".to_string(), fmt_bounds)
                };
                let chunks = if n.chunks.is_empty() {
                    "-".to_string()
                } else {
                    n.chunks.len().to_string()
                };
                [label, action, bounds, chunks, n.reason.clone()]
            })
            .collect();
        let header = ["node", "action", "bounds", "chunks", "reason"];
        let mut widths = header.map(|h| h.chars().count());
        for row in &rows {
            for (i, cell) in row.iter().enumerate().take(4) {
                widths[i] = widths[i].max(cell.chars().count());
            }
        }
        let line = |f: &mut fmt::Formatter<'_>, cells: [&str; 5]| -> fmt::Result {
            for (i, cell) in cells.iter().enumerate() {
                if i == 4 {
                    writeln!(f, "{cell}")?;
                } else {
                    let pad = widths[i] - cell.chars().count();
                    write!(f, "{cell}{}  ", " ".repeat(pad))?;
                }
            }
            Ok(())
        };
        line(f, header)?;
        for row in &rows {
            line(f, [&row[0], &row[1], &row[2], &row[3], &row[4]])?;
        }
        if self.nodes.iter().any(|n| n.provisional) {
            writeln!(f, "* re-planned after its parents finish")?;
        }
        if self.needs_confirmation() {
            writeln!(f, "? rebuild needs confirmation (--yes)")?;
        }
        for w in &self.warnings {
            writeln!(f, "warning: {w}")?;
        }
        Ok(())
    }
}

/// Plan every node of a subgraph against the current state.
pub async fn plan<S: StateSource>(
    graph: &Graph,
    sub: &Subgraph,
    state: &S,
    options: &PlanOptions,
) -> Plan {
    let mut parents: HashMap<PipeRef, ParentState> = HashMap::new();
    let mut nodes = Vec::with_capacity(sub.order.len());
    for r in &sub.order {
        let n = plan_node(graph, sub, r, &parents, state, options).await;
        // A provisional node is re-planned when its parents finish and may run then even
        // if it is planned `skip` now, so its children are provisional too.
        let as_parent = match ParentState::from_action(n.action) {
            ParentState::Idle if n.provisional && n.action == Action::Skip => ParentState::WillRun,
            s => s,
        };
        parents.insert(r.clone(), as_parent);
        nodes.push(n);
    }
    Plan {
        nodes,
        edges: sub
            .edges
            .iter()
            .map(|e| (e.parent.clone(), e.child.clone()))
            .collect(),
        flat: sub.flat,
        warnings: sub.warnings.clone(),
    }
}

/// How an edge looks to the child being planned.
#[derive(Debug)]
enum Signal {
    /// No cursor: compare watermarks (the fallback).
    NoHistory { advanced: bool },
    /// Unconsumed entries.
    Dirty { count: usize, first: i64, last: i64 },
    /// The parent watermark moved since the cursor, without entries.
    Advanced,
    /// Nothing new.
    Quiet,
}

struct EdgeEval {
    label: String,
    signal: Signal,
    /// Dirty intervals on the child's axis.
    dirty: Vec<Interval>,
    parent_watermark: Option<Bound>,
    previous_watermark: Option<Bound>,
    /// The parent watermark moved since the cursor (with or without entries).
    watermark_advanced: bool,
}

impl EdgeEval {
    fn advanced(&self) -> bool {
        self.watermark_advanced || matches!(self.signal, Signal::NoHistory { advanced: true })
    }

    fn describe(&self) -> String {
        let wm = fmt_opt(self.parent_watermark.as_ref());
        match &self.signal {
            Signal::Dirty { count, first, last } => {
                let seqs = if first == last {
                    format!("seq {first}")
                } else {
                    format!("seq {first}–{last}")
                };
                let noun = if *count == 1 { "entry" } else { "entries" };
                let mut text = format!(
                    "{count} unconsumed ledger {noun} from {} ({seqs})",
                    self.label
                );
                if self.watermark_advanced {
                    text.push_str(&format!(
                        ", watermark advanced {} → {wm}",
                        fmt_opt(self.previous_watermark.as_ref())
                    ));
                }
                text
            }
            Signal::Advanced => format!(
                "{} watermark advanced {} → {wm} since its cursor",
                self.label,
                fmt_opt(self.previous_watermark.as_ref())
            ),
            Signal::NoHistory { advanced: true } if self.parent_watermark.is_none() => format!(
                "no cursor on the edge from {} (first engine run)",
                self.label
            ),
            Signal::NoHistory { advanced: true } => format!(
                "no cursor on the edge from {} (first engine run); its watermark {wm} is ahead",
                self.label
            ),
            Signal::NoHistory { advanced: false } => format!(
                "no cursor on the edge from {}; its watermark {wm} is not ahead",
                self.label
            ),
            Signal::Quiet if self.parent_watermark.is_none() => format!(
                "{} unchanged since its cursor (no unconsumed entries)",
                self.label
            ),
            Signal::Quiet => format!(
                "{} unchanged since its cursor (watermark {wm}, no unconsumed entries)",
                self.label
            ),
        }
    }
}

fn advanced_since(previous: Option<&Bound>, now: Option<&Bound>, axis: &Axis) -> bool {
    match (previous, now) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some(p), Some(n)) => match (axis.pos(p), axis.pos(n)) {
            (Some(p), Some(n)) => n > p,
            _ => p != n,
        },
    }
}

fn to_bounds(axis: &Axis, iv: &Interval) -> Bounds {
    Bounds {
        begin: iv
            .begin
            .and_then(|b| axis.bound(b))
            .map_or(Begin::Unbounded, Begin::At),
        end: iv.end.and_then(|e| axis.bound(e)),
    }
}

fn to_span(axis: &Axis, iv: &Interval) -> Span {
    Span {
        begin: iv.begin.and_then(|b| axis.bound(b)),
        end: iv.end.and_then(|e| axis.bound(e)),
    }
}

fn chunks_for(
    axis: &Axis,
    ranges: &[Interval],
    max: usize,
    warnings: &mut Vec<String>,
) -> Vec<Span> {
    let mut out = Vec::new();
    for iv in ranges {
        match (iv.begin, iv.end) {
            (Some(b), Some(e)) => {
                let chunks = get_chunk_bounds(
                    Some(b),
                    Some(e),
                    true,
                    axis.chunk,
                    true,
                    axis.kind == AxisKind::Datetime,
                    None,
                    None,
                );
                match chunks {
                    Ok(c) if c.len() <= max => out.extend(
                        c.into_iter()
                            .map(|(b, e)| to_span(axis, &Interval::new(b, e))),
                    ),
                    Ok(c) => {
                        warnings.push(format!(
                            "{} chunks exceed the plan limit of {max}; listed as one range",
                            c.len()
                        ));
                        out.push(to_span(axis, iv));
                    }
                    Err(e) => {
                        warnings.push(e);
                        out.push(to_span(axis, iv));
                    }
                }
            }
            _ => out.push(to_span(axis, iv)),
        }
    }
    out
}

/// Plan one node given its parents' states. Used by [`plan`] and by the executor to re-plan a
/// provisional node once its parents finished.
pub async fn plan_node<S: StateSource>(
    graph: &Graph,
    sub: &Subgraph,
    pipe_ref: &PipeRef,
    parents: &HashMap<PipeRef, ParentState>,
    state: &S,
    options: &PlanOptions,
) -> PlannedNode {
    let node = graph
        .node(pipe_ref)
        .expect("selected nodes come from the graph");
    let axis = &node.axis;
    let mut out = PlannedNode::new(pipe_ref, &node.definition_hash);

    if options.completed.contains(pipe_ref) {
        let run = options.resumed_from.as_deref().unwrap_or("the resumed run");
        return out.set(Action::Skip, format!("already succeeded in {run}"));
    }
    let user_v4 = Bounds {
        begin: options.begin.map_or(Begin::Default, Begin::At),
        end: options.end,
    };
    if sub.flat {
        out.bounds = Some(user_v4);
        return out.set(
            Action::Root,
            "flat selection: v4 sync semantics (no capping, no ledger planning)",
        );
    }
    if node.is_stream() {
        return out.set(
            Action::Observe,
            "stream source: never run in a one-shot sync; watermark and ledger are read only",
        );
    }

    let in_edges: Vec<&Edge> = sub.parent_edges(pipe_ref).collect();
    let label = |e: &Edge| {
        if e.cross_instance {
            e.parent.label_with_instance()
        } else {
            e.parent.label()
        }
    };
    if let Some(e) = in_edges
        .iter()
        .find(|e| parents.get(&e.parent) == Some(&ParentState::Failed))
    {
        return out.set(
            Action::UpstreamFailed,
            format!("parent {} failed", label(e)),
        );
    }
    out.provisional = in_edges
        .iter()
        .any(|e| parents.get(&e.parent) == Some(&ParentState::WillRun));

    match state.definition_hash(pipe_ref).await {
        Ok(h) => out.stored_definition_hash = h,
        Err(e) => out
            .warnings
            .push(format!("cannot read the definition hash: {e}")),
    }
    match state.watermark(pipe_ref).await {
        Ok(w) => out.watermark = w,
        Err(e) => out.warnings.push(format!("cannot read the watermark: {e}")),
    }
    let own_pos = out.watermark.as_ref().and_then(|w| axis.pos(w));

    // Evaluate every in-selection parent edge.
    let mut evals: Vec<EdgeEval> = Vec::with_capacity(in_edges.len());
    let mut caps: Vec<i64> = Vec::new();
    for e in &in_edges {
        let parent = graph.node(&e.parent).expect("edges join graph nodes");
        let unreachable = |what: &str, err: crate::state::StateError| {
            format!("cannot read {what} of {}: {err}", label(e))
        };
        let pwm = match state.watermark(&e.parent).await {
            Ok(w) => w,
            Err(err) => {
                return out.set(Action::UpstreamFailed, unreachable("the watermark", err));
            }
        };
        let cursor = match state.cursor(&e.state_instance, &e.parent, pipe_ref).await {
            Ok(c) => c,
            Err(err) => {
                return out.set(Action::UpstreamFailed, unreachable("the edge cursor", err));
            }
        };
        let mapped_wm = pwm
            .as_ref()
            .and_then(|w| map_watermark(w, &parent.axis, axis, &e.options));
        if let Some(c) = mapped_wm {
            caps.push(if options.inclusive_cap {
                c.saturating_add(cap_unit(axis))
            } else {
                c
            });
        }
        let mut eval = EdgeEval {
            label: label(e),
            signal: Signal::Quiet,
            dirty: Vec::new(),
            parent_watermark: pwm,
            previous_watermark: None,
            watermark_advanced: false,
        };
        let seq = match cursor {
            None => {
                let latest = match state.latest_seq(&e.state_instance, &e.parent).await {
                    Ok(s) => s,
                    Err(err) => {
                        return out.set(Action::UpstreamFailed, unreachable("the ledger", err));
                    }
                };
                let advanced = if parent.axis.kind == AxisKind::None || axis.kind == AxisKind::None
                {
                    true
                } else {
                    match (mapped_wm, own_pos) {
                        (None, _) => false,
                        (Some(_), None) => true,
                        (Some(p), Some(o)) => p > o,
                    }
                };
                eval.signal = Signal::NoHistory { advanced };
                latest.unwrap_or(0)
            }
            Some(c) => {
                eval.previous_watermark = c.parent_watermark_at_cursor;
                eval.watermark_advanced = advanced_since(
                    c.parent_watermark_at_cursor.as_ref(),
                    eval.parent_watermark.as_ref(),
                    &parent.axis,
                );
                let entries = match state
                    .ledger_after(&e.state_instance, &e.parent, c.seq)
                    .await
                {
                    Ok(entries) => entries,
                    Err(err) => {
                        return out.set(Action::UpstreamFailed, unreachable("the ledger", err));
                    }
                };
                if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
                    eval.signal = Signal::Dirty {
                        count: entries.len(),
                        first: first.seq,
                        last: last.seq,
                    };
                    let dirty = dirty_intervals(&entries, &parent.axis);
                    eval.dirty = map_to_child(&dirty, &parent.axis, axis, &e.options);
                    last.seq.max(c.seq)
                } else {
                    if eval.watermark_advanced {
                        eval.signal = Signal::Advanced;
                    }
                    c.seq
                }
            }
        };
        out.consumes.push(crate::plan::Consumption {
            parent_ref: e.parent.clone(),
            child_ref: pipe_ref.clone(),
            state_instance: e.state_instance.clone(),
            seq,
            parent_watermark: eval.parent_watermark,
        });
        evals.push(eval);
    }
    let cap = caps.into_iter().min();

    // Rebuild takes precedence over everything but failures and streams.
    if definition_changed(out.stored_definition_hash.as_deref(), &node.definition_hash) {
        let cap_iv = if in_edges.is_empty() {
            Interval::full()
        } else {
            Interval::new(None, cap)
        };
        let what = "definition changed (sql, fetch or dtypes)";
        if options.yes || options.auto_rebuild {
            out.bounds = Some(to_bounds(axis, &cap_iv));
            out.clear = true;
            return out.set(
                Action::Rebuild,
                format!("{what}: clear, then re-sync the full range"),
            );
        }
        if options.interactive {
            out.bounds = Some(to_bounds(axis, &cap_iv));
            out.clear = true;
            out.requires_confirmation = true;
            return out.set(
                Action::Rebuild,
                format!("{what}: clear, then re-sync the full range (confirm or pass --yes)"),
            );
        }
        out.consumes.clear();
        return out.set(
            Action::RebuildPending,
            format!(
                "{what}: rebuild needs --yes or system.engine.auto_rebuild; \
                 descendants run against the stale data"
            ),
        );
    }

    if in_edges.is_empty() {
        out.bounds = Some(user_v4);
        return out.set(
            Action::Root,
            "no parents in the selection: v4 sync (begin = sync time − backtrack)",
        );
    }

    let any_entries = evals
        .iter()
        .any(|e| matches!(e.signal, Signal::Dirty { .. }));
    let any_advanced = evals.iter().any(EdgeEval::advanced);
    let describe = |evals: &[EdgeEval], pick: &dyn Fn(&EdgeEval) -> bool| -> String {
        evals
            .iter()
            .filter(|e| pick(e))
            .map(EdgeEval::describe)
            .collect::<Vec<_>>()
            .join("; ")
    };
    let changed = |e: &EdgeEval| matches!(e.signal, Signal::Dirty { .. }) || e.advanced();
    let quiet_reason = || format!("{}; definition unchanged", describe(&evals, &|_| true));

    if axis.kind == AxisKind::None {
        if !any_entries && !any_advanced {
            out.consumes.clear();
            return out.set(Action::Skip, quiet_reason());
        }
        out.bounds = Some(Bounds {
            begin: Begin::Unbounded,
            end: None,
        });
        let action = if any_entries {
            out.ranges = vec![Span {
                begin: None,
                end: None,
            }];
            Action::Backfill
        } else {
            Action::Incremental
        };
        return out.set(
            action,
            format!(
                "{}; no datetime column: full re-sync (filter_existing removes unchanged rows)",
                describe(&evals, &changed)
            ),
        );
    }

    let dirty = union(evals.iter().flat_map(|e| e.dirty.iter().copied()));
    if dirty.is_empty() && !any_advanced {
        out.consumes.clear();
        return out.set(Action::Skip, quiet_reason());
    }

    let tail_begin = own_pos.map(|p| p.saturating_sub(axis.backtrack));
    let user = Interval::new(
        options.begin.as_ref().and_then(|b| axis.pos(b)),
        options.end.as_ref().and_then(|b| axis.pos(b)),
    );
    for (name, b) in [("begin", &options.begin), ("end", &options.end)] {
        if let Some(b) = b {
            if axis.pos(b).is_none() {
                out.warnings.push(format!(
                    "the requested {name} {} does not match this pipe's axis; ignored",
                    fmt_bound(b)
                ));
            }
        }
    }
    let cap_iv = Interval::new(None, cap);
    let historical =
        tail_begin.is_some_and(|t| dirty.iter().any(|iv| iv.begin.is_none_or(|b| b < t)));

    if !historical {
        let iv = Interval::new(tail_begin, cap).intersect(&user);
        if iv.is_empty() {
            out.consumes.clear();
            return out.set(
                Action::Skip,
                format!(
                    "{}; but the window [own watermark − backtrack, min parent watermark) is \
                     empty or outside the requested bounds",
                    describe(&evals, &changed)
                ),
            );
        }
        out.bounds = Some(to_bounds(axis, &iv));
        out.chunks = chunks_for(axis, &[iv], options.max_chunks, &mut out.warnings);
        return out.set(
            Action::Incremental,
            format!(
                "{}; sync from own watermark − backtrack to min parent watermark",
                describe(&evals, &changed)
            ),
        );
    }

    let mut ranges = dirty.clone();
    if any_advanced {
        ranges.push(Interval::new(tail_begin, None));
    }
    let ranges: Vec<Interval> = union(
        ranges
            .into_iter()
            .map(|iv| iv.intersect(&cap_iv).intersect(&user)),
    );
    if ranges.is_empty() {
        out.consumes.clear();
        return out.set(
            Action::Skip,
            format!(
                "{}; but the dirty ranges lie past min parent watermark or outside the requested \
                 bounds",
                describe(&evals, &changed)
            ),
        );
    }
    let hull = Interval::new(ranges[0].begin, ranges[ranges.len() - 1].end);
    out.bounds = Some(to_bounds(axis, &hull));
    out.ranges = ranges.iter().map(|iv| to_span(axis, iv)).collect();
    out.chunks = chunks_for(axis, &ranges, options.max_chunks, &mut out.warnings);
    let tail = tail_begin
        .and_then(|t| axis.bound(t))
        .map_or("None".to_string(), |b| fmt_bound(&b));
    out.set(
        Action::Backfill,
        format!(
            "{}; dirty ranges start before the tail ({tail}): backfill them on the chunk grid",
            describe(&evals, &changed)
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{GraphOptions, GraphWarning};
    use crate::node::PipeRecord;
    use crate::selection::{Depth, Pipeline, Selection};
    use crate::state::{MemoryState, MemoryStateData};
    use chrono::{TimeZone, Utc};
    use meerschaum_core::Value;

    fn rec(metric: &str, json: &str) -> PipeRecord {
        PipeRecord::new("sql:main", "sql:main", metric, None)
            .with_parameters(Value::from_json_str(json).unwrap().to_map())
    }

    fn r(metric: &str) -> PipeRef {
        PipeRef::new("sql:main", "sql:main", metric, None)
    }

    fn dt(day: u32, hour: u32) -> Bound {
        Bound::Datetime(Utc.with_ymd_and_hms(2026, 10, day, hour, 0, 0).unwrap())
    }

    async fn plan_for(
        records: Vec<PipeRecord>,
        state: MemoryStateData,
        target: &str,
        options: PlanOptions,
    ) -> Plan {
        let graph = Graph::build(records, &GraphOptions::default());
        let sub = Pipeline::new(Selection::refs([r(target)]).with_parents(Some(Depth::All)))
            .resolve(&graph)
            .unwrap();
        plan(&graph, &sub, &MemoryState::new(state), &options).await
    }

    fn pair() -> Vec<PipeRecord> {
        vec![
            rec(
                "p",
                r#"{"columns": {"datetime": "ts"}, "verify": {"chunk_days": 1}}"#,
            ),
            rec(
                "c",
                r#"{"columns": {"datetime": "ts"}, "verify": {"chunk_days": 1},
                    "fetch": {"backtrack_minutes": 0}, "parents": ["Pipe('sql:main', 'p')"]}"#,
            ),
        ]
    }

    #[tokio::test]
    async fn watermark_fallback_without_a_cursor() {
        // First engine run, the child is behind: incremental from its own watermark.
        let mut state = MemoryStateData::default();
        state.watermarks.insert(r("p"), Some(dt(12, 6)));
        state.watermarks.insert(r("c"), Some(dt(12, 3)));
        let p = plan_for(pair(), state.clone(), "c", PlanOptions::default()).await;
        let c = p.node(&r("c")).unwrap();
        assert_eq!(c.action, Action::Incremental);
        assert_eq!(
            c.bounds,
            Some(Bounds {
                begin: Begin::At(dt(12, 3)),
                end: Some(dt(12, 6))
            })
        );
        // The child is level with its parent: skip.
        state.watermarks.insert(r("c"), Some(dt(12, 6)));
        let p = plan_for(pair(), state, "c", PlanOptions::default()).await;
        assert_eq!(p.node(&r("c")).unwrap().action, Action::Skip);
    }

    #[tokio::test]
    async fn user_bounds_clamp_and_roots_take_them_verbatim() {
        let mut state = MemoryStateData::default();
        state.watermarks.insert(r("p"), Some(dt(12, 6)));
        state.watermarks.insert(r("c"), Some(dt(10, 0)));
        let options = PlanOptions {
            begin: Some(dt(11, 0)),
            end: Some(dt(12, 0)),
            ..PlanOptions::default()
        };
        let p = plan_for(pair(), state.clone(), "c", options.clone()).await;
        assert_eq!(
            p.node(&r("p")).unwrap().bounds,
            Some(Bounds {
                begin: Begin::At(dt(11, 0)),
                end: Some(dt(12, 0))
            })
        );
        let c = p.node(&r("c")).unwrap();
        assert_eq!(c.action, Action::Incremental);
        assert_eq!(
            c.bounds,
            Some(Bounds {
                begin: Begin::At(dt(11, 0)),
                end: Some(dt(12, 0))
            })
        );
        assert_eq!(c.chunks.len(), 1);
        // Requested bounds entirely past the window: nothing to do.
        let options = PlanOptions {
            begin: Some(dt(13, 0)),
            ..PlanOptions::default()
        };
        let p = plan_for(pair(), state, "c", options).await;
        assert_eq!(p.node(&r("c")).unwrap().action, Action::Skip);
    }

    #[tokio::test]
    async fn decline_and_confirm() {
        let records = vec![rec("p", r#"{"sql": "SELECT 1"}"#)];
        let mut state = MemoryStateData::default();
        state.definition_hashes.insert(r("p"), "v1:old".into());
        let options = PlanOptions {
            interactive: true,
            ..PlanOptions::default()
        };
        let mut p = plan_for(records.clone(), state.clone(), "p", options.clone()).await;
        assert!(p.needs_confirmation());
        assert!(p.to_string().contains("rebuild?"));
        p.decline_rebuilds();
        assert_eq!(p.nodes[0].action, Action::RebuildPending);
        let mut p = plan_for(records.clone(), state.clone(), "p", options).await;
        p.confirm_rebuilds();
        assert!(!p.needs_confirmation());
        assert_eq!(p.nodes[0].action, Action::Rebuild);
        assert_eq!(
            p.nodes[0].bounds,
            Some(Bounds {
                begin: Begin::Unbounded,
                end: None
            })
        );
        // A hash from another scheme is not a change.
        state.definition_hashes.insert(r("p"), "v0:old".into());
        let p = plan_for(records, state, "p", PlanOptions::default()).await;
        assert_eq!(p.nodes[0].action, Action::Root);
    }

    #[test]
    fn begin_serde() {
        for (b, json) in [
            (Begin::Default, r#""""#),
            (Begin::Unbounded, "null"),
            (Begin::At(Bound::Int(5)), "5"),
        ] {
            assert_eq!(serde_json::to_string(&b).unwrap(), json);
            assert_eq!(serde_json::from_str::<Begin>(json).unwrap(), b);
        }
        assert!(serde_json::from_str::<Begin>(r#""x""#).is_err());
    }

    #[test]
    fn integer_parent_without_a_unit_warns() {
        let graph = Graph::build(
            [
                rec(
                    "landing",
                    r#"{"columns": {"datetime": "ts_s"}, "dtypes": {"ts_s": "int"}}"#,
                ),
                rec(
                    "clean",
                    r#"{"columns": {"datetime": "ts"}, "parents": ["Pipe('sql:main', 'landing')"]}"#,
                ),
            ],
            &GraphOptions::default(),
        );
        assert!(matches!(
            graph.warnings(),
            [GraphWarning::AxisUnitInferred { unit, .. }] if unit == "microsecond"
        ));
    }
}
