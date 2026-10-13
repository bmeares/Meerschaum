//! The selection API (§5.2, D4, D5).
//!
//! A [`Pipeline`] is a [`Selection`] over the implicit graph: a set of target pipes plus
//! traversal depths. `parents` / `children` mean transitive ancestors / descendants, limited by
//! an optional depth. A pipeline is not stored as its own entity. Resolving it yields the
//! induced [`Subgraph`] in dependency order.
//!
//! A selection without traversal (`parents` and `children` both unset) is **flat**: the CLI's
//! `sync pipes -c/-m/-l/-t` without `--parents` / `--children`, which keeps v4 semantics.

use std::collections::{BTreeSet, HashMap, VecDeque};

use meerschaum_core::static_config;
use meerschaum_core::{Value, pyfmt::py_str};
use serde::{Deserialize, Serialize};

use crate::graph::{Edge, Graph, GraphError};
use crate::node::{PipeRecord, PipeRef};

/// How far to traverse: all the way, or `N` hops.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Depth {
    /// Transitively (`parents=True`, `--parents`).
    All,
    /// At most `N` hops (`parents=1`, `--parents 1`). `N(0)` traverses nothing.
    N(u32),
}

impl Depth {
    /// Python-style argument: `True` → [`Depth::All`], a positive `int` → [`Depth::N`],
    /// `False` / `None` / `0` → no traversal.
    pub fn from_value(value: &Value) -> Option<Depth> {
        match value {
            Value::Bool(true) => Some(Depth::All),
            Value::Int(n) if *n > 0 => Some(Depth::N(u32::try_from(*n).unwrap_or(u32::MAX))),
            _ => None,
        }
    }

    fn allows(&self, hops: u32) -> bool {
        match self {
            Depth::All => true,
            Depth::N(n) => hops <= *n,
        }
    }
}

/// The CLI's key filters (`-c`, `-m`, `-l`, `-t`), with `_` negation, matched in memory with
/// the semantics of `SQLConnector.fetch_pipes_keys`:
///
/// - an empty list or `['*']` does not filter;
/// - positive values must match one of them, negated values (`_x`) must match none;
/// - a location of `None` / `[None]` / `null` matches pipes without a location;
/// - each tag argument is a comma-separated group whose positive tags must all be present;
///   groups are OR'd; a negated tag in any group excludes pipes carrying it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyFilter {
    /// `-c` connector keys.
    #[serde(default)]
    pub connector_keys: Vec<String>,
    /// `-m` metric keys.
    #[serde(default)]
    pub metric_keys: Vec<String>,
    /// `-l` location keys.
    #[serde(default)]
    pub location_keys: Vec<String>,
    /// `-t` tags.
    #[serde(default)]
    pub tags: Vec<String>,
    /// `-i` instance keys (when the graph spans several instances).
    #[serde(default)]
    pub instance_keys: Option<String>,
}

fn negation_prefix() -> &'static str {
    &static_config().system.fetch_pipes_keys.negation_prefix
}

/// Port of `separate_negation_values`: `(included, excluded)` with the prefix stripped.
pub fn separate_negation_values(values: &[String]) -> (Vec<String>, Vec<String>) {
    let prefix = negation_prefix();
    let mut inc = Vec::new();
    let mut exc = Vec::new();
    for v in values {
        match v.strip_prefix(prefix) {
            Some(rest) => exc.push(rest.to_string()),
            None => inc.push(v.clone()),
        }
    }
    (inc, exc)
}

fn matches_keys(values: &[String], actual: &str) -> bool {
    if values.is_empty() || values == ["*"] {
        return true;
    }
    let (inc, exc) = separate_negation_values(values);
    (inc.is_empty() || inc.iter().any(|v| v == actual)) && !exc.iter().any(|v| v == actual)
}

impl KeyFilter {
    /// Whether every filter is empty.
    pub fn is_empty(&self) -> bool {
        self.connector_keys.is_empty()
            && self.metric_keys.is_empty()
            && self.location_keys.is_empty()
            && self.tags.is_empty()
            && self.instance_keys.is_none()
    }

    /// Whether a record passes the filters.
    pub fn matches(&self, record: &PipeRecord) -> bool {
        if let Some(i) = &self.instance_keys {
            if *i != record.instance_keys {
                return false;
            }
        }
        let locations: Vec<String> = self
            .location_keys
            .iter()
            .map(|l| {
                let (neg, bare) = match l.strip_prefix(negation_prefix()) {
                    Some(rest) => (true, rest),
                    None => (false, l.as_str()),
                };
                let bare = if matches!(bare, "[None]" | "None" | "null") {
                    "None"
                } else {
                    bare
                };
                if neg {
                    format!("{}{bare}", negation_prefix())
                } else {
                    bare.to_string()
                }
            })
            .collect();
        if !matches_keys(&self.connector_keys, &record.connector_keys)
            || !matches_keys(&self.metric_keys, &record.metric_key)
            || !matches_keys(&locations, record.location_key.as_deref().unwrap_or("None"))
        {
            return false;
        }
        if self.tags.is_empty() {
            return true;
        }
        let pipe_tags: Vec<String> = match record.parameters.get("tags") {
            Some(Value::List(items)) => items.iter().map(py_str).collect(),
            _ => Vec::new(),
        };
        let mut any_positive_group = false;
        let mut matched_group = false;
        for group in &self.tags {
            let parts: Vec<String> = group.split(',').map(str::to_string).collect();
            let (inc, exc) = separate_negation_values(&parts);
            if exc.iter().any(|t| pipe_tags.contains(t)) {
                return false;
            }
            if !inc.is_empty() {
                any_positive_group = true;
                if inc.iter().all(|t| pipe_tags.contains(t)) {
                    matched_group = true;
                }
            }
        }
        !any_positive_group || matched_group
    }
}

/// What a selection starts from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Targets {
    /// Explicit pipes (`mrsm.Pipeline([p1, p2])`, `pipe.sync(parents=...)`).
    Refs(Vec<PipeRef>),
    /// Key filters (`mrsm sync pipes -c ... -t ...`).
    Filter(KeyFilter),
}

/// A set of target pipes plus traversal depths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selection {
    /// The targets.
    pub targets: Targets,
    /// Ancestors to include.
    #[serde(default)]
    pub parents: Option<Depth>,
    /// Descendants to include.
    #[serde(default)]
    pub children: Option<Depth>,
}

impl Selection {
    /// Explicit targets.
    pub fn refs(targets: impl IntoIterator<Item = PipeRef>) -> Selection {
        Selection {
            targets: Targets::Refs(targets.into_iter().collect()),
            parents: None,
            children: None,
        }
    }

    /// Filtered targets.
    pub fn filter(filter: KeyFilter) -> Selection {
        Selection {
            targets: Targets::Filter(filter),
            parents: None,
            children: None,
        }
    }

    /// Builder-style: include ancestors.
    pub fn with_parents(mut self, depth: Option<Depth>) -> Selection {
        self.parents = depth;
        self
    }

    /// Builder-style: include descendants.
    pub fn with_children(mut self, depth: Option<Depth>) -> Selection {
        self.children = depth;
        self
    }

    /// Whether no traversal was requested (v4 semantics, §5.3 R1).
    pub fn is_flat(&self) -> bool {
        !matches!(self.parents, Some(Depth::All) | Some(Depth::N(1..)))
            && !matches!(self.children, Some(Depth::All) | Some(Depth::N(1..)))
    }
}

/// A selection over the implicit graph (D4). Not persisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pipeline {
    /// The selection.
    pub selection: Selection,
}

/// The induced subgraph of a selection, in dependency order.
#[derive(Debug, Clone, Default)]
pub struct Subgraph {
    /// The selected nodes in dependency order.
    pub order: Vec<PipeRef>,
    /// The nodes the selection started from.
    pub targets: BTreeSet<PipeRef>,
    /// Every graph edge between selected nodes.
    pub edges: Vec<Edge>,
    /// Whether the selection is flat (no traversal requested).
    pub flat: bool,
    /// Graph warnings about selected nodes, plus selection warnings.
    pub warnings: Vec<String>,
    parents: HashMap<PipeRef, Vec<usize>>,
    children: HashMap<PipeRef, Vec<usize>>,
}

impl Subgraph {
    fn new(graph: &Graph, order: Vec<PipeRef>, targets: BTreeSet<PipeRef>, flat: bool) -> Subgraph {
        let set: BTreeSet<&PipeRef> = order.iter().collect();
        let mut sub = Subgraph {
            order: Vec::new(),
            targets,
            flat,
            ..Default::default()
        };
        for e in graph.edges() {
            if set.contains(&e.parent) && set.contains(&e.child) {
                sub.edges.push(e.clone());
                let i = sub.edges.len() - 1;
                sub.parents.entry(e.child.clone()).or_default().push(i);
                sub.children.entry(e.parent.clone()).or_default().push(i);
            }
        }
        sub.order = order;
        sub
    }

    /// Whether a node is selected.
    pub fn contains(&self, pipe_ref: &PipeRef) -> bool {
        self.order.contains(pipe_ref)
    }

    /// In-selection edges into a node.
    pub fn parent_edges<'a>(&'a self, pipe_ref: &PipeRef) -> impl Iterator<Item = &'a Edge> + 'a {
        self.parents
            .get(pipe_ref)
            .into_iter()
            .flatten()
            .map(|&i| &self.edges[i])
    }

    /// In-selection edges out of a node.
    pub fn child_edges<'a>(&'a self, pipe_ref: &PipeRef) -> impl Iterator<Item = &'a Edge> + 'a {
        self.children
            .get(pipe_ref)
            .into_iter()
            .flatten()
            .map(|&i| &self.edges[i])
    }

    /// The subgraph without `remove` (a hold-back, see `RunOptions::hold`). Edges to and from
    /// removed nodes disappear with them.
    pub fn without(&self, graph: &Graph, remove: &BTreeSet<PipeRef>) -> Subgraph {
        let order: Vec<PipeRef> = self
            .order
            .iter()
            .filter(|r| !remove.contains(*r))
            .cloned()
            .collect();
        let targets = self
            .targets
            .iter()
            .filter(|r| !remove.contains(*r))
            .cloned()
            .collect();
        let mut sub = Subgraph::new(graph, order, targets, self.flat);
        sub.warnings = self.warnings.clone();
        sub
    }

    /// Every in-selection descendant of a node.
    pub fn descendants(&self, pipe_ref: &PipeRef) -> BTreeSet<PipeRef> {
        let mut out = BTreeSet::new();
        let mut queue = VecDeque::from([pipe_ref.clone()]);
        while let Some(r) = queue.pop_front() {
            for e in self.child_edges(&r) {
                if out.insert(e.child.clone()) {
                    queue.push_back(e.child.clone());
                }
            }
        }
        out
    }
}

fn traverse(
    graph: &Graph,
    start: &BTreeSet<PipeRef>,
    depth: Depth,
    upstream: bool,
    out: &mut BTreeSet<PipeRef>,
) {
    let mut queue: VecDeque<(PipeRef, u32)> = start.iter().map(|r| (r.clone(), 0)).collect();
    let mut seen: BTreeSet<PipeRef> = start.clone();
    while let Some((r, hops)) = queue.pop_front() {
        let next: Vec<PipeRef> = if upstream {
            graph.parent_edges(&r).map(|e| e.parent.clone()).collect()
        } else {
            graph.child_edges(&r).map(|e| e.child.clone()).collect()
        };
        for n in next {
            if depth.allows(hops + 1) && seen.insert(n.clone()) {
                out.insert(n.clone());
                queue.push_back((n, hops + 1));
            }
        }
    }
}

impl Pipeline {
    /// A pipeline over a selection.
    pub fn new(selection: Selection) -> Pipeline {
        Pipeline { selection }
    }

    /// Resolve the selection against the graph: targets, then ancestors and descendants up to
    /// their depths, then the induced subgraph in dependency order.
    ///
    /// A cycle inside the selection is an error, except for a flat selection, which keeps v4
    /// semantics: it falls back to record order with a warning.
    pub fn resolve(&self, graph: &Graph) -> Result<Subgraph, GraphError> {
        let mut warnings = Vec::new();
        let targets: BTreeSet<PipeRef> = match &self.selection.targets {
            Targets::Refs(refs) => refs
                .iter()
                .filter(|r| {
                    let known = graph.node(r).is_some();
                    if !known {
                        warnings.push(format!(
                            "{} is not registered; it is not part of the selection",
                            r.label_with_instance()
                        ));
                    }
                    known
                })
                .cloned()
                .collect(),
            Targets::Filter(f) => graph
                .nodes()
                .filter(|n| f.matches(&n.record))
                .map(|n| n.pipe_ref.clone())
                .collect(),
        };
        let mut selected = targets.clone();
        if let Some(depth) = self.selection.parents {
            traverse(graph, &targets, depth, true, &mut selected);
        }
        if let Some(depth) = self.selection.children {
            traverse(graph, &targets, depth, false, &mut selected);
        }
        let flat = self.selection.is_flat();
        let order = match graph.topo_order(&selected) {
            Ok(order) => order,
            Err(err) if flat => {
                warnings.push(format!("{err}; syncing in registration order"));
                let mut order: Vec<PipeRef> = selected.iter().cloned().collect();
                order.sort_by_key(|r| graph.index_of(r));
                order
            }
            Err(err) => return Err(err),
        };
        let mut sub = Subgraph::new(graph, order, targets, flat);
        if flat {
            // Flat selections never order on a cycle: drop edges so the order above holds.
            if graph.find_cycle(&selected).is_some() {
                sub.edges.clear();
                sub.parents.clear();
                sub.children.clear();
            }
        }
        let mut graph_warnings: Vec<String> = graph
            .warnings()
            .iter()
            .filter(|w| selected.contains(w.subject()))
            .map(ToString::to_string)
            .collect();
        graph_warnings.extend(warnings);
        sub.warnings = graph_warnings;
        Ok(sub)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphOptions;

    fn rec(metric: &str, parents: &[&str], tags: &[&str]) -> PipeRecord {
        let parents: Vec<Value> = parents
            .iter()
            .map(|p| Value::Str(format!("Pipe('sql:main', '{p}')")))
            .collect();
        let tags: Vec<Value> = tags.iter().map(|t| Value::from(*t)).collect();
        let mut params = meerschaum_core::Map::new();
        params.insert("parents".into(), Value::List(parents));
        params.insert("tags".into(), Value::List(tags));
        PipeRecord::new("sql:main", "sql:main", metric, None).with_parameters(params)
    }

    fn r(m: &str) -> PipeRef {
        PipeRef::new("sql:main", "sql:main", m, None)
    }

    fn graph() -> Graph {
        // a → b → c → d, and x → c
        Graph::build(
            [
                rec("d", &["c"], &["report"]),
                rec("c", &["b", "x"], &["mid", "daily"]),
                rec("b", &["a"], &["mid"]),
                rec("a", &[], &["raw"]),
                rec("x", &[], &["raw", "geo"]),
            ],
            &GraphOptions::default(),
        )
    }

    #[test]
    fn depths() {
        let g = graph();
        let sub = Pipeline::new(Selection::refs([r("c")]).with_parents(Some(Depth::N(1))))
            .resolve(&g)
            .unwrap();
        assert_eq!(sub.order, vec![r("b"), r("x"), r("c")]);
        let sub = Pipeline::new(Selection::refs([r("c")]).with_parents(Some(Depth::All)))
            .resolve(&g)
            .unwrap();
        assert_eq!(sub.order, vec![r("a"), r("b"), r("x"), r("c")]);
        assert!(!sub.flat);
        let sub = Pipeline::new(Selection::refs([r("b")]).with_children(Some(Depth::All)))
            .resolve(&g)
            .unwrap();
        assert_eq!(sub.order, vec![r("b"), r("c"), r("d")]);
        assert_eq!(sub.edges.len(), 2);
        assert_eq!(sub.descendants(&r("b")).len(), 2);
        let sub = Pipeline::new(Selection::refs([r("b")]))
            .resolve(&g)
            .unwrap();
        assert!(sub.flat);
        assert_eq!(sub.order, vec![r("b")]);
    }

    #[test]
    fn key_filters() {
        let g = graph();
        let sel = |f: KeyFilter| -> Vec<PipeRef> {
            let mut v = Pipeline::new(Selection::filter(f))
                .resolve(&g)
                .unwrap()
                .order;
            v.sort();
            v
        };
        let f = KeyFilter {
            tags: vec!["raw".into()],
            ..Default::default()
        };
        assert_eq!(sel(f), vec![r("a"), r("x")]);
        let f = KeyFilter {
            tags: vec!["raw,geo".into(), "report".into()],
            ..Default::default()
        };
        assert_eq!(sel(f), vec![r("d"), r("x")]);
        let f = KeyFilter {
            tags: vec!["_mid".into()],
            ..Default::default()
        };
        assert_eq!(sel(f), vec![r("a"), r("d"), r("x")]);
        let f = KeyFilter {
            metric_keys: vec!["_a".into(), "_b".into()],
            location_keys: vec!["None".into()],
            ..Default::default()
        };
        assert_eq!(sel(f), vec![r("c"), r("d"), r("x")]);
        let f = KeyFilter {
            location_keys: vec!["_None".into()],
            ..Default::default()
        };
        assert!(sel(f).is_empty());
        let f = KeyFilter {
            connector_keys: vec!["*".into()],
            ..Default::default()
        };
        assert_eq!(sel(f).len(), 5);
    }

    #[test]
    fn depth_from_value() {
        assert_eq!(Depth::from_value(&Value::Bool(true)), Some(Depth::All));
        assert_eq!(Depth::from_value(&Value::Int(2)), Some(Depth::N(2)));
        assert_eq!(Depth::from_value(&Value::Int(0)), None);
        assert_eq!(Depth::from_value(&Value::Null), None);
    }
}
