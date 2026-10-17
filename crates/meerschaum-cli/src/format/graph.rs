//! `show graph`: the selection as a vertical box-drawing tree.
//!
//! The engine owns the data ([`Subgraph`]); this module owns the look. A graph is drawn
//! top-down, children under parents:
//!
//! ```text
//! Pipe graph on sql:main: 6 pipes, 6 edges, 1 root, depth 3
//!
//! ● sql:main/raw_events
//! ├── ◆ sql:main/events_clean  lookback 7d
//! │   ├── ◆ sql:main/daily  (2 parents)
//! │   │   └── ○ sql:main/report ★
//! │   └── ○ sql:main/hourly
//! └┄┄ ↗ sql:main/daily (shown above)
//! ```
//!
//! - A pipe that has several parents is drawn once in full, under the first parent the walk
//!   reaches, and as a short dimmed back-reference (`↗ ... (shown above)`) under the others.
//! - Roots are `●`, pipes with children `◆`, leaves `○`; pipes the selection named directly are
//!   bold and carry `★`. Colors are the ones `Pipe(...)` uses in v4 (`formatting:pipes`); the CLI prints them only on a terminal.
//! - `┄┄` marks an edge that only exists because of a `{{ Pipe() }}` reference (no `parents` /
//!   `children` entry names it). Edge options (`lookback`, `lookahead`, `axis`) follow the pipe.
//! - Pipes without any edge in the selection are listed together under one heading instead of
//!   one component each.
//! - Fan-outs beyond [`FAN_LIMIT`] children collapse to `… and N more children`
//!   (`--full` / `--debug` list everything).
//! - Lines are cut to the terminal width (optional suffixes go first, then the label gets a
//!   `…`). Without Unicode the tree is `|-- `, `` `-- `` and `|   `; without ANSI the layout
//!   stays the same, only the colors go.

use std::collections::{BTreeSet, HashMap, HashSet};

use meerschaum_engine::PipeRef;
use meerschaum_engine::graph::{Edge, Extent};
use meerschaum_engine::selection::Subgraph;

use super::Formatter;
use super::rich::style::Style;

/// A node with more children than this collapses its list.
pub const FAN_LIMIT: usize = 12;
/// How many children a collapsed fan-out still shows.
pub const FAN_SHOW: usize = 10;

/// What [`render`] draws: the pipes, the selection and the edges between them.
#[derive(Debug, Clone, Default)]
pub struct GraphModel {
    /// The nodes in dependency order.
    pub nodes: Vec<PipeRef>,
    /// The pipes the selection started from.
    pub targets: BTreeSet<PipeRef>,
    /// The edges between the nodes (cycles included).
    pub edges: Vec<Edge>,
    /// Cycles among the nodes, each path with its first node repeated at the end.
    pub cycles: Vec<Vec<PipeRef>>,
}

impl GraphModel {
    /// The model of a resolved selection.
    pub fn from_subgraph(sub: &Subgraph) -> GraphModel {
        GraphModel {
            nodes: sub.order.clone(),
            targets: sub.targets.clone(),
            edges: sub.display_edges().to_vec(),
            cycles: sub.cycles.clone(),
        }
    }
}

/// The styles of each part of a line.
#[derive(Debug, Clone, Copy, Default)]
pub struct GraphTheme {
    /// Connector keys (`formatting:pipes:__repr__:ansi:styles:connector`).
    pub connector: Style,
    /// Metric keys.
    pub metric: Style,
    /// Location keys.
    pub location: Style,
    /// `/` and `@` between the keys.
    pub punctuation: Style,
    /// `@instance`.
    pub instance: Style,
    /// Tree guides, back-references, edge options (`formatting:pipes:ansi:styles:guide`).
    pub dim: Style,
    /// The `●` of a root.
    pub root: Style,
    /// The `○` of a leaf.
    pub leaf: Style,
    /// A selected pipe (bold) and its `★`.
    pub target: Style,
    /// A pipe in a cycle and the cycle notes.
    pub cycle: Style,
    /// The header line.
    pub header: Style,
}

impl GraphTheme {
    /// No colors at all.
    pub fn plain() -> GraphTheme {
        GraphTheme::default()
    }

    /// The v4 pipe colors from the config. Plain when ANSI is off.
    pub fn from_formatter(f: &Formatter<'_>) -> GraphTheme {
        if !f.settings.ansi || f.settings.no_color {
            return GraphTheme::plain();
        }
        let repr = |key: &str| {
            Style::parse(&f.formatting_str(&["pipes", "__repr__", "ansi", "styles", key]))
                .unwrap_or_default()
        };
        let guide = Style::parse(&f.formatting_str(&["pipes", "ansi", "styles", "guide"]))
            .ok()
            .filter(|s| !s.is_null())
            .unwrap_or_else(|| Style::parse("dim").unwrap_or_default());
        let pen = |definition: &str| Style::parse(definition).unwrap_or_default();
        GraphTheme {
            connector: repr("connector"),
            metric: repr("metric"),
            location: repr("location"),
            punctuation: repr("punctuation"),
            instance: repr("instance"),
            dim: guide,
            root: pen("bold green"),
            leaf: pen("yellow"),
            target: pen("bold yellow"),
            cycle: pen("bold red"),
            header: pen("bold"),
        }
    }
}

/// How the graph is laid out.
#[derive(Debug, Clone)]
pub struct GraphOptions {
    /// Box-drawing characters (else ASCII).
    pub unicode: bool,
    /// The terminal width in columns.
    pub width: usize,
    /// List every child instead of collapsing wide fan-outs.
    pub full: bool,
    /// The configured default instance (`@instance` is shown for the others).
    pub default_instance: String,
}

struct Glyphs {
    tee: &'static str,
    last: &'static str,
    bar: &'static str,
    gap: &'static str,
    tee_implicit: &'static str,
    last_implicit: &'static str,
    root: &'static str,
    node: &'static str,
    leaf: &'static str,
    target: &'static str,
    cycle: &'static str,
    arrow: &'static str,
    again: &'static str,
    more: &'static str,
}

const UNICODE: Glyphs = Glyphs {
    tee: "├── ",
    last: "└── ",
    bar: "│   ",
    gap: "    ",
    tee_implicit: "├┄┄ ",
    last_implicit: "└┄┄ ",
    root: "●",
    node: "◆",
    leaf: "○",
    target: "★",
    cycle: "⟲",
    arrow: " → ",
    again: "↗",
    more: "…",
};

const ASCII: Glyphs = Glyphs {
    tee: "|-- ",
    last: "`-- ",
    bar: "|   ",
    gap: "    ",
    tee_implicit: "|~~ ",
    last_implicit: "`~~ ",
    root: "@",
    node: "+",
    leaf: "o",
    target: "*",
    cycle: "%",
    arrow: " -> ",
    again: "^",
    more: "...",
};

/// One run of identically styled text.
struct Seg {
    text: String,
    style: Style,
    /// Optional detail, dropped first when the line is too wide.
    optional: bool,
    /// Part of the pipe label, shortened with `…` when the line is still too wide.
    label: bool,
}

fn seg(text: impl Into<String>, style: Style) -> Seg {
    Seg {
        text: text.into(),
        style,
        optional: false,
        label: false,
    }
}

fn optional(text: impl Into<String>, style: Style) -> Seg {
    Seg {
        optional: true,
        ..seg(text, style)
    }
}

fn width_of(segs: &[Seg]) -> usize {
    segs.iter().map(|s| s.text.chars().count()).sum()
}

/// Cut a line to `budget` visible columns.
fn fit(mut segs: Vec<Seg>, budget: usize, ellipsis: &str) -> Vec<Seg> {
    while width_of(&segs) > budget {
        match segs.iter().rposition(|s| s.optional) {
            Some(i) => {
                segs.remove(i);
            }
            None => break,
        }
    }
    let total = width_of(&segs);
    if total <= budget {
        return segs;
    }
    // Shorten the label from its end, leaving room for the ellipsis.
    let mut cut = total - budget + ellipsis.chars().count();
    let mut last_label = None;
    for i in (0..segs.len()).rev() {
        if !segs[i].label || cut == 0 {
            continue;
        }
        let len = segs[i].text.chars().count();
        let take = len.min(cut);
        segs[i].text = segs[i].text.chars().take(len - take).collect();
        cut -= take;
        if len > take {
            last_label = Some(i);
        }
    }
    match last_label {
        Some(i) => segs[i].text.push_str(ellipsis),
        None => {
            // The label is gone: cut the whole line.
            let text: String = segs
                .iter()
                .map(|s| s.text.as_str())
                .collect::<String>()
                .chars()
                .take(budget.saturating_sub(ellipsis.chars().count()))
                .collect();
            return vec![seg(format!("{text}{ellipsis}"), Style::null())];
        }
    }
    segs.retain(|s| !s.text.is_empty());
    segs
}

fn paint(segs: &[Seg]) -> String {
    segs.iter().map(|s| s.style.render(&s.text)).collect()
}

/// `7d`, `90m`, `30s`: the coarsest unit that divides the duration evenly.
fn extent_text(extent: &Extent) -> String {
    match extent {
        Extent::Micros(us) => {
            let secs = us / 1_000_000;
            if *us % 1_000_000 != 0 {
                return format!("{}ms", us / 1000);
            }
            for (unit, name) in [(86_400, "d"), (3_600, "h"), (60, "m")] {
                if secs != 0 && secs % unit == 0 {
                    return format!("{}{name}", secs / unit);
                }
            }
            format!("{secs}s")
        }
        Extent::Units(n) => n.to_string(),
    }
}

/// `lookback 7d`, `lookahead 1h`, `axis ms` of an edge.
fn edge_details(edge: &Edge) -> Vec<String> {
    let mut details = Vec::new();
    if let Some(lookback) = &edge.options.lookback {
        details.push(format!("lookback {}", extent_text(lookback)));
    }
    if let Some(lookahead) = &edge.options.lookahead {
        details.push(format!("lookahead {}", extent_text(lookahead)));
    }
    if let Some(unit) = &edge.options.axis_unit {
        details.push(format!("axis {}", unit.abbreviation()));
    }
    details
}

/// An edge nothing but a `{{ Pipe() }}` reference declares.
fn is_implicit(edge: &Edge) -> bool {
    edge.sources.template && !edge.sources.parents && !edge.sources.children
}

struct Painter<'a> {
    theme: &'a GraphTheme,
    glyphs: &'static Glyphs,
    show_instance: bool,
    default_instance: &'a str,
    targets: &'a BTreeSet<PipeRef>,
}

impl Painter<'_> {
    fn label_segs(&self, r: &PipeRef, bold: bool) -> Vec<Seg> {
        let p = r.parts();
        let t = self.theme;
        let bolded = |style: Style| {
            if bold {
                style.add(&t.target.without_color())
            } else {
                style
            }
        };
        let mut parts = vec![
            (p.connector_keys.to_string(), bolded(t.connector)),
            ("/".to_string(), t.punctuation),
            (p.metric_key.to_string(), bolded(t.metric)),
        ];
        if let Some(location) = p.location_key {
            parts.push(("/".to_string(), t.punctuation));
            parts.push((location.to_string(), bolded(t.location)));
        }
        if self.show_instance || p.instance_keys != self.default_instance {
            parts.push(("@".to_string(), t.punctuation));
            parts.push((p.instance_keys.to_string(), t.instance));
        }
        parts
            .into_iter()
            .map(|(text, style)| Seg {
                label: true,
                ..seg(text, style)
            })
            .collect()
    }

    fn plain_label(&self, r: &PipeRef) -> String {
        self.label_segs(r, false)
            .into_iter()
            .map(|s| s.text)
            .collect()
    }
}

/// Tree state shared by the walk.
struct Walk<'a> {
    painter: Painter<'a>,
    /// Child edges of each node, in dependency order.
    children: HashMap<&'a PipeRef, Vec<&'a Edge>>,
    parent_count: HashMap<&'a PipeRef, usize>,
    /// Members of a cycle.
    in_cycle: HashSet<&'a PipeRef>,
    drawn: HashSet<&'a PipeRef>,
    lines: Vec<Vec<Seg>>,
    full: bool,
    budget: usize,
    ellipsis: &'static str,
    used: Used,
}

/// Which markers appear (they decide the legend).
#[derive(Default)]
struct Used {
    root: bool,
    node: bool,
    leaf: bool,
    target: bool,
    again: bool,
    implicit: bool,
    cycle: bool,
}

impl<'a> Walk<'a> {
    fn push(&mut self, segs: Vec<Seg>) {
        let line = fit(segs, self.budget, self.ellipsis);
        self.lines.push(line);
    }

    fn node_segs(
        &mut self,
        r: &'a PipeRef,
        edge: Option<&Edge>,
        prefix: &str,
        branch: &str,
    ) -> Vec<Seg> {
        let t = self.painter.theme;
        let g = self.painter.glyphs;
        let mut segs = vec![seg(format!("{prefix}{branch}"), t.dim)];
        let has_children = self.children.get(r).is_some_and(|c| !c.is_empty());
        let (glyph, style) = if self.in_cycle.contains(r) {
            self.used.cycle = true;
            (g.cycle, t.cycle)
        } else if edge.is_none() {
            self.used.root = true;
            (g.root, t.root)
        } else if has_children {
            self.used.node = true;
            (g.node, t.dim)
        } else {
            self.used.leaf = true;
            (g.leaf, t.leaf)
        };
        segs.push(seg(format!("{glyph} "), style));
        let targeted = self.painter.targets.contains(r);
        segs.extend(self.painter.label_segs(r, targeted));
        if targeted {
            self.used.target = true;
            segs.push(optional(format!(" {}", g.target), t.target));
        }
        if self.in_cycle.contains(r) {
            segs.push(optional("  (cycle)", t.cycle));
        }
        let parents = self.parent_count.get(r).copied().unwrap_or(0);
        if parents > 1 {
            segs.push(optional(format!("  ({parents} parents)"), t.dim));
        }
        if let Some(edge) = edge {
            let details = edge_details(edge);
            if !details.is_empty() {
                segs.push(optional(format!("  {}", details.join(", ")), t.dim));
            }
        }
        segs
    }

    fn walk(&mut self, r: &'a PipeRef, edge: Option<&'a Edge>, prefix: &str, last: bool) {
        let g = self.painter.glyphs;
        let implicit = edge.is_some_and(is_implicit);
        let branch = match (edge.is_some(), last, implicit) {
            (false, _, _) => "",
            (true, true, false) => g.last,
            (true, false, false) => g.tee,
            (true, true, true) => g.last_implicit,
            (true, false, true) => g.tee_implicit,
        };
        if implicit {
            self.used.implicit = true;
        }
        let t = self.painter.theme;
        if !self.drawn.insert(r) {
            // Already drawn in full somewhere above.
            self.used.again = true;
            let mut segs = vec![seg(format!("{prefix}{branch}"), t.dim)];
            segs.push(seg(format!("{} ", g.again), t.dim));
            segs.push(Seg {
                label: true,
                ..seg(self.painter.plain_label(r), t.dim)
            });
            segs.push(optional(" (shown above)", t.dim));
            if let Some(edge) = edge {
                let details = edge_details(edge);
                if !details.is_empty() {
                    segs.push(optional(format!("  {}", details.join(", ")), t.dim));
                }
            }
            self.push(segs);
            return;
        }
        let segs = self.node_segs(r, edge, prefix, branch);
        self.push(segs);
        let children: Vec<&'a Edge> = self.children.get(r).cloned().unwrap_or_default();
        let hidden = if !self.full && children.len() > FAN_LIMIT {
            children.len() - FAN_SHOW
        } else {
            0
        };
        let shown = children.len() - hidden;
        let child_prefix = match (edge.is_some(), last) {
            (false, _) => String::new(),
            (true, true) => format!("{prefix}{}", g.gap),
            (true, false) => format!("{prefix}{}", g.bar),
        };
        for (i, child) in children.iter().take(shown).enumerate() {
            let is_last = i + 1 == children.len();
            self.walk(&child.child, Some(child), &child_prefix, is_last);
        }
        if hidden > 0 {
            let text = format!(
                "{child_prefix}{}{} and {hidden} more children",
                g.last, g.more
            );
            // Hidden children that have children of their own hide whole subtrees.
            let deep = children[shown..]
                .iter()
                .filter(|e| self.children.get(&e.child).is_some_and(|c| !c.is_empty()))
                .count();
            let mut segs = vec![seg(text, t.dim)];
            if deep > 0 {
                segs.push(optional(format!(" ({deep} with descendants)"), t.dim));
            }
            let line = fit(segs, self.budget, self.ellipsis);
            self.lines.push(line);
        }
    }
}

/// The depth of the deepest chain (a lone pipe is 1).
fn depth(model: &GraphModel) -> usize {
    let mut depth: HashMap<&PipeRef, usize> = HashMap::new();
    let mut parents: HashMap<&PipeRef, Vec<&PipeRef>> = HashMap::new();
    for e in &model.edges {
        parents.entry(&e.child).or_default().push(&e.parent);
    }
    let mut deepest = 0;
    for r in &model.nodes {
        let d = parents
            .get(r)
            .into_iter()
            .flatten()
            .filter_map(|p| depth.get(p))
            .max()
            .map_or(1, |d| d + 1);
        depth.insert(r, d);
        deepest = deepest.max(d);
    }
    deepest
}

/// Connected components (ignoring edge direction), as a representative per node.
fn components(model: &GraphModel) -> HashMap<&PipeRef, usize> {
    let index: HashMap<&PipeRef, usize> = model
        .nodes
        .iter()
        .enumerate()
        .map(|(i, r)| (r, i))
        .collect();
    let mut parent: Vec<usize> = (0..model.nodes.len()).collect();
    fn find(parent: &mut [usize], mut x: usize) -> usize {
        while parent[x] != x {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        x
    }
    for e in &model.edges {
        if let (Some(&a), Some(&b)) = (index.get(&e.parent), index.get(&e.child)) {
            let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
            if ra != rb {
                parent[rb.max(ra)] = rb.min(ra);
            }
        }
    }
    model
        .nodes
        .iter()
        .enumerate()
        .map(|(i, r)| (r, find(&mut parent, i)))
        .collect()
}

/// Draw the graph (header, tree, legend), one `String` with a trailing newline.
pub fn render(model: &GraphModel, theme: &GraphTheme, opts: &GraphOptions) -> String {
    let glyphs: &'static Glyphs = if opts.unicode { &UNICODE } else { &ASCII };
    let instances: BTreeSet<&str> = model.nodes.iter().map(|r| r.instance_keys()).collect();
    // When the selection is the whole graph (a flat selection), marking every pipe says nothing.
    let no_targets = BTreeSet::new();
    let marked = if model.targets.len() < model.nodes.len() {
        &model.targets
    } else {
        &no_targets
    };
    let painter = Painter {
        theme,
        glyphs,
        show_instance: instances.len() > 1,
        default_instance: &opts.default_instance,
        targets: marked,
    };
    let mut children: HashMap<&PipeRef, Vec<&Edge>> = HashMap::new();
    let mut parent_count: HashMap<&PipeRef, usize> = HashMap::new();
    let order: HashMap<&PipeRef, usize> = model
        .nodes
        .iter()
        .enumerate()
        .map(|(i, r)| (r, i))
        .collect();
    for e in &model.edges {
        children.entry(&e.parent).or_default().push(e);
        *parent_count.entry(&e.child).or_default() += 1;
    }
    for edges in children.values_mut() {
        edges.sort_by_key(|e| order.get(&e.child).copied().unwrap_or(usize::MAX));
    }
    let in_cycle: HashSet<&PipeRef> = model.cycles.iter().flatten().collect();
    let comp = components(model);
    let connected = |r: &PipeRef| children.contains_key(r) || parent_count.contains_key(r);
    let roots: Vec<&PipeRef> = model
        .nodes
        .iter()
        .filter(|r| !parent_count.contains_key(r) && connected(r))
        .collect();
    let loners: Vec<&PipeRef> = model.nodes.iter().filter(|r| !connected(r)).collect();
    let mut walk = Walk {
        painter,
        children,
        parent_count,
        in_cycle,
        drawn: HashSet::new(),
        lines: Vec::new(),
        full: opts.full,
        budget: opts.width.saturating_sub(1).max(20),
        ellipsis: if opts.unicode { "…" } else { "..." },
        used: Used::default(),
    };
    let mut previous: Option<usize> = None;
    for root in &roots {
        let component = comp.get(root).copied();
        if let (Some(prev), Some(cur)) = (previous, component)
            && prev != cur
        {
            walk.lines.push(Vec::new());
        }
        previous = component;
        walk.walk(root, None, "", true);
    }
    // A cycle has no root: start the walk at its first node (in dependency order).
    let cycle_nodes: Vec<&PipeRef> = model
        .nodes
        .iter()
        .filter(|r| walk.in_cycle.contains(r))
        .collect();
    for node in cycle_nodes {
        if walk.drawn.contains(node) {
            continue;
        }
        let component = comp.get(node).copied();
        if !walk.lines.is_empty() && previous != component {
            walk.lines.push(Vec::new());
        }
        previous = component;
        walk.walk(node, None, "", true);
    }
    if !loners.is_empty() {
        if !walk.lines.is_empty() {
            walk.lines.push(Vec::new());
        }
        let t = theme;
        let heading = format!("Not connected to the rest ({})", loners.len());
        walk.lines.push(vec![seg(heading, t.dim)]);
        let hidden = if !opts.full && loners.len() > FAN_LIMIT {
            loners.len() - FAN_SHOW
        } else {
            0
        };
        let shown = loners.len() - hidden;
        for r in loners.iter().take(shown) {
            let targeted = marked.contains(*r);
            let mut segs = vec![seg(format!("{} ", glyphs.root), t.root)];
            segs.extend(walk.painter.label_segs(r, targeted));
            if targeted {
                walk.used.target = true;
                segs.push(optional(format!(" {}", glyphs.target), t.target));
            }
            walk.used.root = true;
            walk.drawn.insert(r);
            let line = fit(segs, walk.budget, walk.ellipsis);
            walk.lines.push(line);
        }
        if hidden > 0 {
            let text = format!("{} and {hidden} more pipes", glyphs.more);
            let line = fit(vec![optional(text, t.dim)], walk.budget, walk.ellipsis);
            walk.lines.push(line);
        }
    }
    let drawn = walk.drawn.len();
    let total = model.nodes.len();
    let roots_n = model
        .nodes
        .iter()
        .filter(|r| !walk.parent_count.contains_key(r))
        .count();
    let mut out = String::new();
    let header = format!(
        "Pipe graph on {}: {} pipe{}, {} edge{}, {} root{}, depth {}",
        if instances.len() > 1 {
            format!("{} instances", instances.len())
        } else {
            instances
                .iter()
                .next()
                .map_or_else(|| opts.default_instance.clone(), |i| (*i).to_string())
        },
        total,
        plural(total),
        model.edges.len(),
        plural(model.edges.len()),
        roots_n,
        plural(roots_n),
        depth(model)
    );
    let header = fit(vec![seg(header, theme.header)], walk.budget, walk.ellipsis);
    out.push_str(&paint(&header));
    out.push_str("\n\n");
    for line in &walk.lines {
        out.push_str(&paint(line));
        out.push('\n');
    }
    // The legend lists only the markers on screen.
    let u = &walk.used;
    let mut legend: Vec<String> = Vec::new();
    if u.root {
        legend.push(format!("{} root", glyphs.root));
    }
    if u.node {
        legend.push(format!("{} has children", glyphs.node));
    }
    if u.leaf {
        legend.push(format!("{} leaf", glyphs.leaf));
    }
    if u.target {
        legend.push(format!("{} selected", glyphs.target));
    }
    if u.again {
        legend.push(format!("{} shown above", glyphs.again));
    }
    if u.cycle {
        legend.push(format!("{} in a cycle", glyphs.cycle));
    }
    if u.implicit {
        let dashes: String = glyphs.tee_implicit.trim_end().chars().skip(1).collect();
        legend.push(format!("{dashes} implicit {{{{ Pipe() }}}} edge"));
    }
    // Notes: cycles, then what was collapsed. They are never dropped, only cut to the width.
    let mut notes: Vec<Vec<Seg>> = Vec::new();
    for cycle in &model.cycles {
        let path: Vec<String> = cycle.iter().map(|r| walk.painter.plain_label(r)).collect();
        let text = format!("{} cycle: {}", glyphs.cycle, path.join(glyphs.arrow));
        notes.push(fit(
            vec![seg(text, theme.cycle)],
            walk.budget,
            walk.ellipsis,
        ));
    }
    if drawn < total {
        let note = format!("Showing {drawn} of {total} pipes; --full lists every pipe.");
        notes.push(fit(vec![seg(note, theme.dim)], walk.budget, walk.ellipsis));
    }
    if !notes.is_empty() {
        out.push('\n');
        for note in &notes {
            out.push_str(&paint(note));
            out.push('\n');
        }
    }
    if legend.len() > 1 || u.again || u.implicit || u.cycle {
        // Wrap the items to the width, so no marker loses its explanation on a narrow terminal.
        let mut lines: Vec<String> = vec![String::new()];
        for item in legend {
            let current = lines.last_mut().expect("one line");
            let used = current.chars().count();
            if used == 0 {
                *current = item;
            } else if used + 3 + item.chars().count() <= walk.budget {
                current.push_str("   ");
                current.push_str(&item);
            } else {
                lines.push(item);
            }
        }
        out.push('\n');
        for line in lines {
            let line = fit(vec![seg(line, theme.dim)], walk.budget, walk.ellipsis);
            out.push_str(&paint(&line));
            out.push('\n');
        }
    }
    out
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_engine::graph::{EdgeOptions, EdgeSources};

    fn r(metric: &str) -> PipeRef {
        PipeRef::new("sql:main", "sql:main", metric, None)
    }

    fn edge(parent: &str, child: &str) -> Edge {
        Edge {
            parent: r(parent),
            child: r(child),
            sources: EdgeSources {
                parents: true,
                ..Default::default()
            },
            options: EdgeOptions::default(),
            cross_instance: false,
            state_instance: "sql:main".into(),
        }
    }

    fn model(nodes: &[&str], edges: Vec<Edge>, targets: &[&str]) -> GraphModel {
        GraphModel {
            nodes: nodes.iter().map(|n| r(n)).collect(),
            targets: targets.iter().map(|n| r(n)).collect(),
            edges,
            cycles: Vec::new(),
        }
    }

    fn opts(unicode: bool, width: usize) -> GraphOptions {
        GraphOptions {
            unicode,
            width,
            full: false,
            default_instance: "sql:main".into(),
        }
    }

    fn plain(model: &GraphModel, unicode: bool, width: usize) -> String {
        render(model, &GraphTheme::plain(), &opts(unicode, width))
    }

    #[test]
    fn a_chain_is_a_vertical_tree() {
        let m = model(
            &["raw", "clean", "report"],
            vec![edge("raw", "clean"), edge("clean", "report")],
            &["report"],
        );
        let text = plain(&m, true, 80);
        let expected = "\
Pipe graph on sql:main: 3 pipes, 2 edges, 1 root, depth 3

● sql:main/raw
└── ◆ sql:main/clean
    └── ○ sql:main/report ★

● root   ◆ has children   ○ leaf   ★ selected
";
        assert_eq!(text, expected);
    }

    #[test]
    fn a_diamond_draws_the_merge_once_and_refers_back_to_it() {
        let m = model(
            &["a", "b", "c", "d"],
            vec![
                edge("a", "b"),
                edge("a", "c"),
                edge("b", "d"),
                edge("c", "d"),
            ],
            &[],
        );
        let text = plain(&m, true, 80);
        let expected = "\
Pipe graph on sql:main: 4 pipes, 4 edges, 1 root, depth 3

● sql:main/a
├── ◆ sql:main/b
│   └── ○ sql:main/d  (2 parents)
└── ◆ sql:main/c
    └── ↗ sql:main/d (shown above)

● root   ◆ has children   ○ leaf   ↗ shown above
";
        assert_eq!(text, expected);
    }

    #[test]
    fn a_wide_fan_out_collapses_and_full_lists_everything() {
        let names: Vec<String> = (0..50).map(|i| format!("leaf{i:02}")).collect();
        let mut nodes: Vec<&str> = vec!["hub"];
        nodes.extend(names.iter().map(String::as_str));
        let edges = names.iter().map(|n| edge("hub", n)).collect();
        let m = model(&nodes, edges, &["hub"]);
        let text = plain(&m, true, 80);
        assert!(
            text.contains("Showing 11 of 51 pipes; --full lists every pipe."),
            "{text}"
        );
        assert!(text.contains("├── ○ sql:main/leaf09"), "{text}");
        assert!(!text.contains("leaf10"), "{text}");
        assert!(text.contains("└── … and 40 more children"), "{text}");
        let mut all = opts(true, 80);
        all.full = true;
        let text = render(&m, &GraphTheme::plain(), &all);
        assert!(text.contains("└── ○ sql:main/leaf49"), "{text}");
        assert!(!text.contains("more children"), "{text}");
        assert!(!text.contains("Showing"), "{text}");
    }

    #[test]
    fn hidden_children_never_leave_a_dangling_back_reference() {
        // `shared` hangs off the 13th child of `hub` and off `side`: the walk reaches it under
        // `side` in full, so no "shown above" points at a hidden place.
        let mut names: Vec<String> = vec!["side".into()];
        names.extend((0..13).map(|i| format!("c{i:02}")));
        let mut nodes: Vec<&str> = vec!["hub"];
        nodes.extend(names.iter().map(String::as_str));
        nodes.push("shared");
        let mut edges: Vec<Edge> = names.iter().map(|n| edge("hub", n)).collect();
        edges.push(edge("c12", "shared"));
        edges.push(edge("side", "shared"));
        let m = model(&nodes, edges, &[]);
        let text = plain(&m, true, 100);
        assert!(!text.contains("shown above"), "{text}");
        assert!(
            text.contains("└── ○ sql:main/shared") || text.contains("├── ○ sql:main/shared"),
            "{text}"
        );
    }

    #[test]
    fn ascii_mode_uses_ascii_guides() {
        let m = model(
            &["a", "b", "c"],
            vec![edge("a", "b"), edge("a", "c")],
            &["c"],
        );
        let text = plain(&m, false, 80);
        let expected = "\
Pipe graph on sql:main: 3 pipes, 2 edges, 1 root, depth 2

@ sql:main/a
|-- o sql:main/b
`-- o sql:main/c *

@ root   o leaf   * selected
";
        assert_eq!(text, expected);
        assert!(text.is_ascii(), "{text}");
    }

    #[test]
    fn colors_use_the_pipe_styles_and_the_plain_layout_matches() {
        let m = model(&["a", "b"], vec![edge("a", "b")], &["b"]);
        let styled = GraphTheme {
            connector: Style::parse("green").unwrap(),
            metric: Style::parse("bright_blue").unwrap(),
            dim: Style::parse("dim").unwrap(),
            ..GraphTheme::plain()
        };
        let colored = render(&m, &styled, &opts(true, 80));
        assert!(colored.contains("\x1b["), "{colored}");
        let stripped = crate::format::remove_ansi(&colored);
        assert_eq!(stripped, plain(&m, true, 80));
    }

    #[test]
    fn independent_components_are_separated_and_loners_share_one_list() {
        let m = model(
            &["a", "b", "x", "y", "lone1", "lone2"],
            vec![edge("a", "b"), edge("x", "y")],
            &[],
        );
        let text = plain(&m, true, 80);
        let expected = "\
Pipe graph on sql:main: 6 pipes, 2 edges, 4 roots, depth 2

● sql:main/a
└── ○ sql:main/b

● sql:main/x
└── ○ sql:main/y

Not connected to the rest (2)
● sql:main/lone1
● sql:main/lone2

● root   ○ leaf
";
        assert_eq!(text, expected);
    }

    #[test]
    fn edge_options_and_implicit_edges_are_marked() {
        let mut lookback = edge("a", "b");
        lookback.options.lookback = Some(Extent::Micros(7 * 86_400 * 1_000_000));
        lookback.options.lookahead = Some(Extent::Micros(90 * 60 * 1_000_000));
        let mut implicit = edge("a", "c");
        implicit.sources = EdgeSources {
            template: true,
            ..Default::default()
        };
        let m = model(&["a", "b", "c"], vec![lookback, implicit], &[]);
        let text = plain(&m, true, 100);
        assert!(
            text.contains("├── ○ sql:main/b  lookback 7d, lookahead 90m"),
            "{text}"
        );
        assert!(text.contains("└┄┄ ○ sql:main/c"), "{text}");
        assert!(text.contains("┄┄ implicit {{ Pipe() }} edge"), "{text}");
    }

    #[test]
    fn narrow_terminals_drop_details_then_shorten_the_label() {
        let m = model(
            &["a", "a_pipe_with_a_remarkably_long_metric_name"],
            vec![edge("a", "a_pipe_with_a_remarkably_long_metric_name")],
            &["a_pipe_with_a_remarkably_long_metric_name"],
        );
        let text = plain(&m, true, 40);
        for line in text.lines() {
            assert!(line.chars().count() <= 39, "{line:?} in\n{text}");
        }
        assert!(text.contains('…'), "{text}");
        assert!(!text.contains('★') || text.lines().all(|l| l.chars().count() <= 39));
    }

    #[test]
    fn instances_show_when_they_differ_from_the_default() {
        let other = PipeRef::new("sql:other", "sql:other", "events", None);
        let mut cross = edge("a", "b");
        cross.child = other.clone();
        cross.cross_instance = true;
        let m = GraphModel {
            nodes: vec![r("a"), other],
            targets: BTreeSet::new(),
            edges: vec![cross],
            cycles: Vec::new(),
        };
        let text = plain(&m, true, 100);
        assert!(text.contains("Pipe graph on 2 instances"), "{text}");
        assert!(text.contains("● sql:main/a@sql:main"), "{text}");
        assert!(text.contains("○ sql:other/events@sql:other"), "{text}");
    }

    #[test]
    fn a_cycle_shows_its_real_edges_and_is_marked() {
        let mut m = model(&["e", "f", "z"], vec![edge("e", "f"), edge("f", "e")], &[]);
        m.cycles = vec![vec![r("e"), r("f"), r("e")]];
        let text = plain(&m, true, 80);
        let expected = "\
Pipe graph on sql:main: 3 pipes, 2 edges, 1 root, depth 2

⟲ sql:main/e  (cycle)
└── ⟲ sql:main/f  (cycle)
    └── ↗ sql:main/e (shown above)

Not connected to the rest (1)
● sql:main/z

⟲ cycle: sql:main/e → sql:main/f → sql:main/e

● root   ↗ shown above   ⟲ in a cycle
";
        assert_eq!(text, expected);
    }

    #[test]
    fn narrow_terminals_keep_the_legend_for_every_marker() {
        let m = model(
            &["a", "b", "c", "d"],
            vec![
                edge("a", "b"),
                edge("a", "c"),
                edge("b", "d"),
                edge("c", "d"),
            ],
            &[],
        );
        let text = plain(&m, true, 30);
        for line in text.lines() {
            assert!(line.chars().count() <= 29, "{line:?} in\n{text}");
        }
        let legend: String = text.lines().rev().take(3).collect::<Vec<_>>().join(" ");
        for word in ["root", "has children", "leaf", "shown above"] {
            assert!(legend.contains(word), "{word} in the legend: {text}");
        }
        assert!(text.contains("↗ "), "{text}");
    }

    #[test]
    fn a_collapsed_fan_out_counts_the_hidden_children_that_have_descendants() {
        let names: Vec<String> = (0..16).map(|i| format!("c{i:02}")).collect();
        let mut nodes: Vec<&str> = vec!["hub", "deep1", "deep2"];
        nodes.extend(names.iter().map(String::as_str));
        let mut edges: Vec<Edge> = names.iter().map(|n| edge("hub", n)).collect();
        edges.push(edge("c12", "deep1"));
        edges.push(edge("c14", "deep2"));
        let m = model(&nodes, edges, &[]);
        let text = plain(&m, true, 100);
        assert!(
            text.contains("… and 6 more children (2 with descendants)"),
            "{text}"
        );
    }
}
