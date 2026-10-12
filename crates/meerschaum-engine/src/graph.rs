//! The implicit pipe graph (§5.1).
//!
//! Edges come from three sources, combined and deduplicated:
//!
//! 1. `parameters['parents']` / `parent` (edge parent → self);
//! 2. `parameters['children']` / `child` (edge self → child);
//! 3. `{{ Pipe(...) }}` references anywhere in the parameters (SQL definitions, `fetch`
//!    definitions, other values), extracted statically by core's template parser. On by
//!    default (`system.engine.implicit_edges`, R7).
//!
//! `reference` / `references` mean parameter inheritance only and never produce an edge. A
//! reference to an unknown pipe is a warning (corpora contain stale references), not an error.
//! A cycle is a planning error raised when a selection's induced subgraph contains it
//! ([`Graph::topo_order`]). Cross-instance edges keep their ledger entries and cursor on the
//! **child's** instance ([`Edge::state_instance`]).
//!
//! The internal `mrsm` pipes (`connector_keys == "mrsm"`, the engine state pipes of §5.8) are
//! excluded from the graph.

use std::collections::{BTreeSet, HashMap};
use std::fmt;

use chrono::TimeDelta;
use indexmap::IndexMap;
use meerschaum_core::dtypes::PrecisionUnit;
use meerschaum_core::pipe::{ChunkInterval, PipeDefaults, pipe_from_value};
use meerschaum_core::pyfmt::{py_repr, py_str};
use meerschaum_core::template::referenced_pipes_in_parameters;
use meerschaum_core::{Map, Pipe, Value};
use serde::{Deserialize, Serialize};

use crate::axis::{Axis, AxisKind};
use crate::ledger::definition_hash;
use crate::node::{PipeRecord, PipeRef};

/// Options for building a [`Graph`].
#[derive(Debug, Clone, PartialEq)]
pub struct GraphOptions {
    /// Configuration defaults for constructing pipes (default instance, chunk minutes).
    pub defaults: PipeDefaults,
    /// `system.engine.implicit_edges` (R7, default `true`): add edges for `{{ Pipe() }}`.
    pub implicit_edges: bool,
    /// `pipes:parameters:fetch:backtrack_minutes` (default 1440).
    pub default_backtrack_minutes: f64,
}

impl Default for GraphOptions {
    fn default() -> Self {
        GraphOptions {
            defaults: PipeDefaults::default(),
            implicit_edges: true,
            default_backtrack_minutes: 1440.0,
        }
    }
}

/// How far an edge widens a mapped range: a duration, or a count of axis units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Extent {
    /// A duration in microseconds (`lookback_days: 7`).
    Micros(i64),
    /// A bare number (`lookback: 7`): raw units on an integer axis, minutes on a datetime
    /// axis (the convention of `get_chunk_interval`'s integer argument).
    Units(i64),
}

impl Extent {
    /// The extent in the given axis's units.
    pub fn to_units(&self, axis: &Axis) -> i64 {
        match (self, axis.kind) {
            (Extent::Micros(us), _) => axis.delta_units(TimeDelta::microseconds(*us)),
            (Extent::Units(n), AxisKind::Datetime) => n.saturating_mul(60_000_000),
            (Extent::Units(n), _) => *n,
        }
    }
}

/// Per-edge overrides of the range mapping (§5.4), declared next to the reference. They
/// describe the child's query: `lookback` is how far back from `t` the child's row at `t` reads
/// the parent (a trailing window), `lookahead` how far forward ([`crate::ledger::map_to_child`]).
/// `axis.unit` is the parent's integer unit when `precision` does not say:
///
/// ```yaml
/// parents:
///   - {connector: sql:main, metric: raw_events, lookback_days: 7}
///   - {connector: plugin:mqtt, metric: landing, axis: {unit: ms}}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeOptions {
    /// The child reads the parent this far back: mapped ranges' ends move up by this much.
    pub lookback: Option<Extent>,
    /// The child reads the parent this far ahead: mapped ranges' begins move down by this much.
    pub lookahead: Option<Extent>,
    /// The **parent's** integer-axis unit, when it cannot be inferred from `precision`.
    #[serde(default, with = "crate::axis::opt_unit")]
    pub axis_unit: Option<PrecisionUnit>,
}

impl EdgeOptions {
    /// Whether every option is unset.
    pub fn is_default(&self) -> bool {
        *self == EdgeOptions::default()
    }

    /// Split a relation dict into the `Pipe(**kwargs)` part and the edge options.
    ///
    /// Recognized keys: `lookback` / `lookahead` (a number), `lookback_<unit>` /
    /// `lookahead_<unit>` with `<unit>` in `seconds`, `minutes`, `hours`, `days`, `weeks`, and
    /// `axis` (a dict with `unit`, or a unit string).
    pub fn split(map: &Map) -> Result<(Map, EdgeOptions), String> {
        let mut kwargs = Map::new();
        let mut opts = EdgeOptions::default();
        for (key, value) in map {
            let target = match key.strip_prefix("lookback") {
                Some(suffix) => Some((&mut opts.lookback, suffix)),
                None => key
                    .strip_prefix("lookahead")
                    .map(|suffix| (&mut opts.lookahead, suffix)),
            };
            if let Some((slot, suffix)) = target {
                *slot = Some(parse_extent(key, suffix, value)?);
                continue;
            }
            if key == "axis" {
                let unit = match value {
                    Value::Map(m) => m.get("unit").map(py_str),
                    Value::Str(s) => Some(s.clone()),
                    _ => None,
                }
                .ok_or_else(|| format!("invalid edge axis {}", py_repr(value)))?;
                opts.axis_unit = Some(
                    PrecisionUnit::parse(&unit)
                        .ok_or_else(|| format!("invalid edge axis unit '{unit}'"))?,
                );
                continue;
            }
            kwargs.insert(key.clone(), value.clone());
        }
        Ok((kwargs, opts))
    }
}

fn parse_extent(key: &str, suffix: &str, value: &Value) -> Result<Extent, String> {
    let number = value
        .as_f64()
        .ok_or_else(|| format!("'{key}' must be a number, got {}", py_repr(value)))?;
    let micros_per = match suffix {
        "" => {
            return Ok(Extent::Units(number as i64));
        }
        "_seconds" => 1e6,
        "_minutes" => 60e6,
        "_hours" => 3_600e6,
        "_days" => 86_400e6,
        "_weeks" => 604_800e6,
        other => return Err(format!("unknown edge option '{key}' (suffix '{other}')")),
    };
    Ok(Extent::Micros((number * micros_per).round() as i64))
}

/// Where an edge was declared. An edge can have several sources.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeSources {
    /// Declared in the child's `parents` / `parent`.
    pub parents: bool,
    /// Declared in the parent's `children` / `child`.
    pub children: bool,
    /// Implied by a `{{ Pipe() }}` reference in the child's parameters.
    pub template: bool,
}

impl fmt::Display for EdgeSources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names = Vec::new();
        if self.parents {
            names.push("parents");
        }
        if self.children {
            names.push("children");
        }
        if self.template {
            names.push("template");
        }
        f.write_str(&names.join("+"))
    }
}

/// A data edge `parent → child`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    /// The upstream node.
    pub parent: PipeRef,
    /// The downstream node.
    pub child: PipeRef,
    /// Where the edge was declared.
    pub sources: EdgeSources,
    /// Mapping overrides.
    pub options: EdgeOptions,
    /// Whether the endpoints live on different instances.
    pub cross_instance: bool,
    /// The instance that stores this edge's ledger entries and cursor: the child's (§5.1).
    pub state_instance: String,
}

/// A graph node: one registered pipe.
#[derive(Debug, Clone)]
pub struct Node {
    /// The node identity.
    pub pipe_ref: PipeRef,
    /// The record the node was built from.
    pub record: PipeRecord,
    /// The core pipe (no I/O).
    pub pipe: Pipe,
    /// The datetime axis, chunk grid and backtrack window.
    pub axis: Axis,
    /// The definition hash of `sql`, `fetch` and `dtypes` (§5.4).
    pub definition_hash: String,
}

impl Node {
    /// Whether the node is a stream source (§5.6).
    pub fn is_stream(&self) -> bool {
        self.record.streaming
    }
}

/// A non-fatal problem found while building the graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GraphWarning {
    /// A record could not be turned into a pipe.
    InvalidRecord {
        /// The record's `pipe_ref`.
        node: PipeRef,
        /// Why.
        error: String,
    },
    /// The same `pipe_ref` appeared twice; the first record wins.
    DuplicateRecord {
        /// The node.
        node: PipeRef,
    },
    /// A relation value could not be parsed into a pipe.
    InvalidReference {
        /// The declaring node.
        node: PipeRef,
        /// `parents` or `children`.
        relation: String,
        /// The offending value (Python `repr`).
        value: String,
        /// Why.
        error: String,
    },
    /// A reference to a pipe that is not registered (stale).
    StaleReference {
        /// The declaring node.
        node: PipeRef,
        /// The missing pipe.
        missing: PipeRef,
        /// `parents`, `children` or `template`.
        via: String,
    },
    /// A pipe declares itself as its own parent or child.
    SelfReference {
        /// The node.
        node: PipeRef,
        /// `parents` or `children`.
        via: String,
    },
    /// Both sides of an edge declare different mapping options; the first wins.
    ConflictingEdgeOptions {
        /// The parent.
        parent: PipeRef,
        /// The child.
        child: PipeRef,
    },
    /// An integer-axis parent feeds a datetime child (or the reverse) and its unit had to be
    /// inferred; set `precision` on the pipe or `axis: {unit: ...}` on the edge.
    AxisUnitInferred {
        /// The parent.
        parent: PipeRef,
        /// The child.
        child: PipeRef,
        /// The unit assumed.
        unit: String,
    },
    /// The node's axis could not be computed (an invalid `precision` or chunk setting).
    InvalidAxis {
        /// The node.
        node: PipeRef,
        /// Why.
        error: String,
    },
}

impl GraphWarning {
    /// The node this warning is about (the declaring node or the child).
    pub fn subject(&self) -> &PipeRef {
        match self {
            GraphWarning::InvalidRecord { node, .. }
            | GraphWarning::DuplicateRecord { node }
            | GraphWarning::InvalidReference { node, .. }
            | GraphWarning::StaleReference { node, .. }
            | GraphWarning::SelfReference { node, .. }
            | GraphWarning::InvalidAxis { node, .. } => node,
            GraphWarning::ConflictingEdgeOptions { child, .. }
            | GraphWarning::AxisUnitInferred { child, .. } => child,
        }
    }
}

impl fmt::Display for GraphWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GraphWarning::InvalidRecord { node, error } => {
                write!(f, "invalid pipe {}: {error}", node.label())
            }
            GraphWarning::DuplicateRecord { node } => {
                write!(f, "duplicate pipe {}; keeping the first", node.label())
            }
            GraphWarning::InvalidReference {
                node,
                relation,
                value,
                error,
            } => write!(
                f,
                "{}: cannot parse {relation} reference {value}: {error}",
                node.label()
            ),
            GraphWarning::StaleReference { node, missing, via } => write!(
                f,
                "stale reference: {} references {} ({via}), which is not registered",
                node.label(),
                missing.label_with_instance()
            ),
            GraphWarning::SelfReference { node, via } => {
                write!(f, "{} lists itself in {via}; ignored", node.label())
            }
            GraphWarning::ConflictingEdgeOptions { parent, child } => write!(
                f,
                "edge {} → {} declares different mapping options on each side; using the first",
                parent.label(),
                child.label()
            ),
            GraphWarning::AxisUnitInferred {
                parent,
                child,
                unit,
            } => write!(
                f,
                "edge {} → {} crosses an integer/datetime axis; assuming the integer axis is in \
                 {unit} (set `precision` or `axis: {{unit: ...}}` on the edge)",
                parent.label(),
                child.label()
            ),
            GraphWarning::InvalidAxis { node, error } => {
                write!(
                    f,
                    "{}: invalid datetime axis settings: {error}",
                    node.label()
                )
            }
        }
    }
}

/// A planning error.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum GraphError {
    /// The selection contains a cycle. `path` starts and ends with the same node.
    #[error("cycle in the pipe graph: {}", format_path(.path))]
    Cycle {
        /// The cycle, first node repeated at the end.
        path: Vec<PipeRef>,
    },
}

fn format_path(path: &[PipeRef]) -> String {
    path.iter()
        .map(PipeRef::label)
        .collect::<Vec<_>>()
        .join(" → ")
}

/// The pipe graph: nodes in record order and deduplicated edges.
#[derive(Debug, Clone, Default)]
pub struct Graph {
    nodes: IndexMap<PipeRef, Node>,
    edges: Vec<Edge>,
    parents: HashMap<PipeRef, Vec<usize>>,
    children: HashMap<PipeRef, Vec<usize>>,
    warnings: Vec<GraphWarning>,
}

fn backtrack_minutes(params: &Map, default: f64) -> f64 {
    params
        .get("fetch")
        .and_then(|f| f.get("backtrack_minutes"))
        .and_then(Value::as_f64)
        .unwrap_or(default)
}

/// Compute a node's axis. TODO(core-merge): `get_backtrack_interval` belongs in core.
fn node_axis(pipe: &Pipe, options: &GraphOptions) -> Result<Axis, String> {
    let Some(dt_col) = pipe.column("datetime") else {
        return Ok(Axis::none());
    };
    let dtype = pipe
        .dtype(&dt_col)
        .unwrap_or_else(|| "datetime".to_string());
    let is_int = dtype.to_lowercase().contains("int");
    let chunk = pipe
        .get_chunk_interval(None, &options.defaults)
        .map_err(|e| e.to_string())?;
    let minutes = backtrack_minutes(pipe.parameters(), options.default_backtrack_minutes);
    let backtrack = TimeDelta::microseconds((minutes * 60e6).round() as i64);
    if !is_int {
        let chunk = match chunk {
            ChunkInterval::Delta(d) => d,
            ChunkInterval::Int(i) => TimeDelta::minutes(i),
        };
        return Ok(Axis::datetime(&dt_col, chunk, backtrack));
    }
    let explicit = pipe
        .parameters()
        .get("precision")
        .is_some_and(Value::truthy);
    let derived = pipe
        .precision(&options.defaults)
        .map_err(|e| e.to_string())?;
    let chunk = match chunk {
        ChunkInterval::Int(i) => i,
        ChunkInterval::Delta(d) => d.num_minutes(),
    };
    let mut axis = Axis::int(&dt_col, explicit.then_some(derived.unit), chunk, 0);
    axis.derived_unit = Some(derived.unit);
    // `get_backtrack_interval`: raw minutes without `precision`, else scaled by its scalar.
    axis.backtrack = if explicit {
        (minutes * 60.0 * derived.unit.scalar()) as i64
    } else {
        minutes as i64
    };
    Ok(axis)
}

impl Graph {
    /// Build the graph from registered pipes (any instance, any `fetch_pipes_keys` form).
    pub fn build(records: impl IntoIterator<Item = PipeRecord>, options: &GraphOptions) -> Graph {
        let mut graph = Graph::default();
        for record in records {
            if record.connector_keys == "mrsm" {
                continue;
            }
            let pipe = match record.to_pipe(&options.defaults) {
                Ok(p) => p,
                Err(e) => {
                    graph.warnings.push(GraphWarning::InvalidRecord {
                        node: record.pipe_ref(),
                        error: e.to_string(),
                    });
                    continue;
                }
            };
            let pipe_ref = PipeRef::from_pipe(&pipe);
            if graph.nodes.contains_key(&pipe_ref) {
                graph
                    .warnings
                    .push(GraphWarning::DuplicateRecord { node: pipe_ref });
                continue;
            }
            let axis = node_axis(&pipe, options).unwrap_or_else(|error| {
                graph.warnings.push(GraphWarning::InvalidAxis {
                    node: pipe_ref.clone(),
                    error,
                });
                Axis::none()
            });
            let definition_hash = definition_hash(pipe.parameters());
            graph.nodes.insert(
                pipe_ref.clone(),
                Node {
                    pipe_ref,
                    record,
                    pipe,
                    axis,
                    definition_hash,
                },
            );
        }

        let mut index: HashMap<(PipeRef, PipeRef), usize> = HashMap::new();
        let node_refs: Vec<PipeRef> = graph.nodes.keys().cloned().collect();
        for node_ref in &node_refs {
            let pipe = graph.nodes[node_ref].pipe.clone();
            for (relation, upstream) in [("parents", true), ("children", false)] {
                for value in pipe.relation_values(relation) {
                    let Some((other, opts)) =
                        graph.parse_relation(node_ref, &pipe, relation, &value, options)
                    else {
                        continue;
                    };
                    let (parent, child) = if upstream {
                        (other, node_ref.clone())
                    } else {
                        (node_ref.clone(), other)
                    };
                    graph.add_edge(&mut index, node_ref, parent, child, relation, opts);
                }
            }
            if options.implicit_edges {
                let mut params = pipe.parameters().clone();
                params.shift_remove("reference");
                params.shift_remove("references");
                for referenced in
                    referenced_pipes_in_parameters(&params, Some(&pipe), &options.defaults)
                {
                    let parent = PipeRef::from_pipe(&referenced);
                    if &parent == node_ref {
                        // A pipe reading its own table (e.g. an incremental `MAX(ts)`) is not a
                        // dependency.
                        continue;
                    }
                    graph.add_edge(
                        &mut index,
                        node_ref,
                        parent,
                        node_ref.clone(),
                        "template",
                        EdgeOptions::default(),
                    );
                }
            }
        }

        for edge in &graph.edges {
            let (p, c) = (&graph.nodes[&edge.parent], &graph.nodes[&edge.child]);
            let crosses = matches!(
                (p.axis.kind, c.axis.kind),
                (AxisKind::Int, AxisKind::Datetime)
            );
            if crosses && edge.options.axis_unit.is_none() && p.axis.explicit_unit.is_none() {
                let unit = p
                    .axis
                    .derived_unit
                    .map_or("an unknown unit".to_string(), |u| u.name().to_string());
                graph.warnings.push(GraphWarning::AxisUnitInferred {
                    parent: edge.parent.clone(),
                    child: edge.child.clone(),
                    unit,
                });
            }
        }
        graph
    }

    fn parse_relation(
        &mut self,
        node_ref: &PipeRef,
        pipe: &Pipe,
        relation: &str,
        value: &Value,
        options: &GraphOptions,
    ) -> Option<(PipeRef, EdgeOptions)> {
        let invalid = |error: String| GraphWarning::InvalidReference {
            node: node_ref.clone(),
            relation: relation.to_string(),
            value: py_repr(value),
            error,
        };
        let (value, opts) = match value {
            Value::Map(m) => match EdgeOptions::split(m) {
                Ok((kwargs, opts)) => (Value::Map(kwargs), opts),
                Err(e) => {
                    self.warnings.push(invalid(e));
                    return None;
                }
            },
            other => (other.clone(), EdgeOptions::default()),
        };
        match pipe_from_value(&value, Some(pipe), &options.defaults) {
            Ok(Some(p)) => Some((PipeRef::from_pipe(&p), opts)),
            Ok(None) => {
                self.warnings
                    .push(invalid("not a valid Pipe(...) reference".into()));
                None
            }
            Err(e) => {
                self.warnings.push(invalid(e.to_string()));
                None
            }
        }
    }

    fn add_edge(
        &mut self,
        index: &mut HashMap<(PipeRef, PipeRef), usize>,
        declaring: &PipeRef,
        parent: PipeRef,
        child: PipeRef,
        via: &str,
        options: EdgeOptions,
    ) {
        if parent == child {
            self.warnings.push(GraphWarning::SelfReference {
                node: declaring.clone(),
                via: via.to_string(),
            });
            return;
        }
        for endpoint in [&parent, &child] {
            if !self.nodes.contains_key(endpoint) {
                self.warnings.push(GraphWarning::StaleReference {
                    node: declaring.clone(),
                    missing: endpoint.clone(),
                    via: via.to_string(),
                });
                return;
            }
        }
        let key = (parent.clone(), child.clone());
        let i = match index.get(&key) {
            Some(&i) => i,
            None => {
                let cross_instance = parent.instance_keys() != child.instance_keys();
                let state_instance = child.instance_keys().to_string();
                self.edges.push(Edge {
                    parent: parent.clone(),
                    child: child.clone(),
                    sources: EdgeSources::default(),
                    options: EdgeOptions::default(),
                    cross_instance,
                    state_instance,
                });
                let i = self.edges.len() - 1;
                index.insert(key, i);
                self.parents.entry(child.clone()).or_default().push(i);
                self.children.entry(parent.clone()).or_default().push(i);
                i
            }
        };
        let edge = &mut self.edges[i];
        match via {
            "parents" => edge.sources.parents = true,
            "children" => edge.sources.children = true,
            _ => edge.sources.template = true,
        }
        if !options.is_default() {
            if edge.options.is_default() {
                edge.options = options;
            } else if edge.options != options {
                self.warnings
                    .push(GraphWarning::ConflictingEdgeOptions { parent, child });
            }
        }
    }

    /// The nodes, in record order.
    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.values()
    }

    /// The number of nodes.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the graph has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// A node by reference.
    pub fn node(&self, pipe_ref: &PipeRef) -> Option<&Node> {
        self.nodes.get(pipe_ref)
    }

    /// The record order of a node (used to break ties deterministically).
    pub fn index_of(&self, pipe_ref: &PipeRef) -> Option<usize> {
        self.nodes.get_index_of(pipe_ref)
    }

    /// All edges.
    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    /// The edges into a node.
    pub fn parent_edges<'a>(&'a self, pipe_ref: &PipeRef) -> impl Iterator<Item = &'a Edge> + 'a {
        self.parents
            .get(pipe_ref)
            .into_iter()
            .flatten()
            .map(|&i| &self.edges[i])
    }

    /// The edges out of a node.
    pub fn child_edges<'a>(&'a self, pipe_ref: &PipeRef) -> impl Iterator<Item = &'a Edge> + 'a {
        self.children
            .get(pipe_ref)
            .into_iter()
            .flatten()
            .map(|&i| &self.edges[i])
    }

    /// Warnings collected while building.
    pub fn warnings(&self) -> &[GraphWarning] {
        &self.warnings
    }

    /// The instances a parent's ledger entries must be written to: its own, plus each
    /// cross-instance child's (§5.1).
    pub fn ledger_instances(&self, pipe_ref: &PipeRef) -> Vec<String> {
        let mut out = vec![pipe_ref.instance_keys().to_string()];
        for e in self.child_edges(pipe_ref) {
            if !out.contains(&e.state_instance) {
                out.push(e.state_instance.clone());
            }
        }
        out
    }

    /// A dependency order of the nodes in `within` using only edges between them (Kahn's
    /// algorithm, ties broken by record order). Fails with the cycle path if there is one.
    pub fn topo_order(&self, within: &BTreeSet<PipeRef>) -> Result<Vec<PipeRef>, GraphError> {
        let mut indegree: HashMap<&PipeRef, usize> = within.iter().map(|r| (r, 0)).collect();
        for e in &self.edges {
            if within.contains(&e.parent) && within.contains(&e.child) {
                *indegree.get_mut(&e.child).expect("in within") += 1;
            }
        }
        let mut ready: BTreeSet<(usize, &PipeRef)> = indegree
            .iter()
            .filter(|(_, d)| **d == 0)
            .map(|(r, _)| (self.index_of(r).unwrap_or(usize::MAX), *r))
            .collect();
        let mut out = Vec::with_capacity(within.len());
        while let Some(item) = ready.pop_first() {
            let r = item.1;
            out.push(r.clone());
            for e in self.child_edges(r) {
                if let Some(d) = indegree.get_mut(&e.child) {
                    if within.contains(r) {
                        *d -= 1;
                        if *d == 0 {
                            ready.insert((self.index_of(&e.child).unwrap_or(usize::MAX), &e.child));
                        }
                    }
                }
            }
        }
        if out.len() < within.len() {
            let path = self
                .find_cycle(within)
                .expect("Kahn's algorithm stalled, so a cycle exists");
            return Err(GraphError::Cycle { path });
        }
        Ok(out)
    }

    /// A cycle among the nodes in `within`, as a path whose first node is repeated at the end.
    pub fn find_cycle(&self, within: &BTreeSet<PipeRef>) -> Option<Vec<PipeRef>> {
        #[derive(Clone, Copy, PartialEq)]
        enum Mark {
            Unvisited,
            OnStack,
            Done,
        }
        let mut marks: HashMap<&PipeRef, Mark> =
            within.iter().map(|r| (r, Mark::Unvisited)).collect();
        let mut starts: Vec<&PipeRef> = within.iter().collect();
        starts.sort_by_key(|r| self.index_of(r));
        for start in starts {
            if marks[start] != Mark::Unvisited {
                continue;
            }
            // Iterative DFS: (node, next child-edge index).
            let mut stack: Vec<(&PipeRef, usize)> = vec![(start, 0)];
            marks.insert(start, Mark::OnStack);
            while let Some((node, i)) = stack.last().copied() {
                let next = self
                    .children
                    .get(node)
                    .and_then(|v| v.get(i))
                    .map(|&ei| &self.edges[ei].child);
                match next {
                    None => {
                        marks.insert(node, Mark::Done);
                        stack.pop();
                    }
                    Some(child) => {
                        stack.last_mut().expect("non-empty").1 += 1;
                        match marks.get(child) {
                            Some(Mark::Unvisited) => {
                                marks.insert(child, Mark::OnStack);
                                stack.push((child, 0));
                            }
                            Some(Mark::OnStack) => {
                                let pos = stack
                                    .iter()
                                    .position(|(n, _)| *n == child)
                                    .expect("on stack");
                                let mut path: Vec<PipeRef> =
                                    stack[pos..].iter().map(|(n, _)| (*n).clone()).collect();
                                path.push(child.clone());
                                return Some(path);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(metric: &str, params: &str) -> PipeRecord {
        PipeRecord::new("sql:main", "sql:main", metric, None).with_parameters(
            Value::from_json_str(params)
                .unwrap()
                .as_map()
                .unwrap()
                .clone(),
        )
    }

    fn r(metric: &str) -> PipeRef {
        PipeRef::new("sql:main", "sql:main", metric, None)
    }

    #[test]
    fn edges_from_all_sources() {
        let g = Graph::build(
            [
                rec("raw", r#"{"children": ["Pipe('sql:main', 'clean')"]}"#),
                rec(
                    "clean",
                    r#"{"parents": [{"connector": "sql:main", "metric": "raw", "lookback_days": 7}],
                        "sql": "SELECT * FROM {{ Pipe('sql:main', 'raw') }}"}"#,
                ),
                rec(
                    "rollup",
                    r#"{"fetch": {"definition": "SELECT * FROM {{Pipe('sql:main', 'clean')}} WHERE ts > (SELECT MAX(ts) FROM {{ Pipe('sql:main', 'rollup') }})"},
                        "reference": "Pipe('sql:main', 'raw')"}"#,
                ),
            ],
            &GraphOptions::default(),
        );
        assert_eq!(g.edges().len(), 2, "{:?}", g.edges());
        let e = &g.edges()[0];
        assert_eq!((&e.parent, &e.child), (&r("raw"), &r("clean")));
        assert!(e.sources.parents && e.sources.children && e.sources.template);
        assert_eq!(
            e.options.lookback,
            Some(Extent::Micros(
                TimeDelta::days(7).num_microseconds().unwrap()
            ))
        );
        let e = &g.edges()[1];
        assert_eq!((&e.parent, &e.child), (&r("clean"), &r("rollup")));
        assert!(e.sources.template && !e.sources.parents);
        assert!(g.warnings().is_empty(), "{:?}", g.warnings());
    }

    #[test]
    fn stale_and_cycles() {
        let g = Graph::build(
            [
                rec(
                    "a",
                    r#"{"parents": ["Pipe('sql:main', 'c')", "Pipe('sql:main', 'gone')"]}"#,
                ),
                rec("b", r#"{"parents": ["Pipe('sql:main', 'a')"]}"#),
                rec(
                    "c",
                    r#"{"sql": "SELECT * FROM {{ Pipe('sql:main', 'b') }}"}"#,
                ),
            ],
            &GraphOptions::default(),
        );
        assert!(matches!(
            &g.warnings()[0],
            GraphWarning::StaleReference { missing, .. } if *missing == r("gone")
        ));
        let all: BTreeSet<PipeRef> = g.nodes().map(|n| n.pipe_ref.clone()).collect();
        let err = g.topo_order(&all).unwrap_err();
        let GraphError::Cycle { path } = &err;
        assert_eq!(path, &vec![r("a"), r("b"), r("c"), r("a")]);
        assert_eq!(
            err.to_string(),
            "cycle in the pipe graph: sql:main/a → sql:main/b → sql:main/c → sql:main/a"
        );
        let sub: BTreeSet<PipeRef> = [r("a"), r("b")].into_iter().collect();
        assert_eq!(g.topo_order(&sub).unwrap(), vec![r("a"), r("b")]);
    }

    #[test]
    fn cross_instance_edge_state_lives_on_child() {
        let parent = PipeRecord::new("sql:a", "plugin:x", "raw", None);
        let child = PipeRecord::new("sql:b", "sql:b", "agg", None).with_parameters(
            Value::from_json_str(r#"{"parents": ["Pipe('plugin:x', 'raw', instance='sql:a')"]}"#)
                .unwrap()
                .as_map()
                .unwrap()
                .clone(),
        );
        let g = Graph::build([parent, child], &GraphOptions::default());
        let e = &g.edges()[0];
        assert!(e.cross_instance);
        assert_eq!(e.state_instance, "sql:b");
        assert_eq!(
            g.ledger_instances(&e.parent),
            vec!["sql:a".to_string(), "sql:b".to_string()]
        );
    }

    #[test]
    fn internal_pipes_are_excluded() {
        let g = Graph::build(
            [PipeRecord::new("sql:main", "mrsm", "engine_ledger", None)],
            &GraphOptions::default(),
        );
        assert!(g.is_empty());
    }
}
