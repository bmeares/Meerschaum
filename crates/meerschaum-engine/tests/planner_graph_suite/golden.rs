//! Planner golden fixtures (§14): every `tests/fixtures/*.json` is a graph, a selection, a
//! state and the expected plan, written by hand from §5.3. See `tests/fixtures/README.md`.

use std::path::{Path, PathBuf};

use meerschaum_engine::graph::GraphOptions;
use meerschaum_engine::plan::Consumption;
use meerschaum_engine::state::MemoryStateData;
use meerschaum_engine::{
    Action, Bounds, Graph, MemoryState, PipeRecord, PipeRef, Pipeline, PlanOptions, Selection,
    Span, plan,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    description: String,
    records: Vec<PipeRecord>,
    #[serde(default = "yes")]
    implicit_edges: bool,
    selection: Selection,
    #[serde(default)]
    options: PlanOptions,
    #[serde(default)]
    state: MemoryStateData,
    expected: Expected,
}

fn yes() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    warnings: Vec<String>,
    #[serde(default)]
    nodes: Vec<ExpectedNode>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedNode {
    node: PipeRef,
    action: Action,
    #[serde(default)]
    bounds: Option<Bounds>,
    #[serde(default)]
    ranges: Option<Vec<Span>>,
    #[serde(default)]
    chunks: Option<Vec<Span>>,
    #[serde(default)]
    chunk_count: Option<usize>,
    #[serde(default)]
    provisional: bool,
    #[serde(default)]
    requires_confirmation: bool,
    #[serde(default)]
    reason_contains: Vec<String>,
    #[serde(default)]
    consumes: Option<Vec<ExpectedConsumption>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedConsumption {
    parent_ref: PipeRef,
    state_instance: String,
    seq: i64,
}

fn fixture_paths() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("fixtures directory")
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    paths.sort();
    paths
}

/// Run one fixture; return the mismatches.
async fn check(path: &Path) -> Vec<String> {
    let name = path.file_stem().unwrap().to_string_lossy().to_string();
    let text = std::fs::read_to_string(path).expect("read fixture");
    let fx: Fixture = match serde_json::from_str(&text) {
        Ok(f) => f,
        Err(e) => return vec![format!("{name}: invalid fixture: {e}")],
    };
    assert!(!fx.description.is_empty(), "{name}: describe the fixture");
    let mut errs = Vec::new();
    let graph = Graph::build(
        fx.records,
        &GraphOptions {
            implicit_edges: fx.implicit_edges,
            ..GraphOptions::default()
        },
    );
    let mut state = fx.state;
    for (r, h) in state.definition_hashes.iter_mut() {
        if h == "@current" {
            match graph.node(r) {
                Some(n) => *h = n.definition_hash.clone(),
                None => errs.push(format!("{name}: @current hash for unknown node {r}")),
            }
        }
    }
    let state = MemoryState::new(state);
    let sub = match Pipeline::new(fx.selection).resolve(&graph) {
        Ok(sub) => sub,
        Err(e) => {
            match &fx.expected.error {
                Some(expected) if e.to_string().contains(expected.as_str()) => {}
                Some(expected) => {
                    errs.push(format!("{name}: error `{e}` does not contain `{expected}`"))
                }
                None => errs.push(format!("{name}: unexpected error `{e}`")),
            }
            return errs;
        }
    };
    if let Some(expected) = &fx.expected.error {
        errs.push(format!("{name}: expected error `{expected}`, got a plan"));
        return errs;
    }
    let plan = plan(&graph, &sub, &state, &fx.options).await;
    println!("── {name}\n{plan}");
    for w in &fx.expected.warnings {
        if !plan.warnings.iter().any(|pw| pw.contains(w.as_str())) {
            errs.push(format!(
                "{name}: no warning contains `{w}`: {:?}",
                plan.warnings
            ));
        }
    }
    if fx.expected.warnings.is_empty() && !plan.warnings.is_empty() {
        errs.push(format!("{name}: unexpected warnings {:?}", plan.warnings));
    }
    let got: Vec<&PipeRef> = plan.nodes.iter().map(|n| &n.pipe_ref).collect();
    let want: Vec<&PipeRef> = fx.expected.nodes.iter().map(|n| &n.node).collect();
    if got != want {
        errs.push(format!("{name}: node order {got:?} != expected {want:?}"));
        return errs;
    }
    for (n, x) in plan.nodes.iter().zip(&fx.expected.nodes) {
        let label = format!("{name}: {}", n.pipe_ref.label());
        let mut diff = |what: &str, got: String, want: String| {
            if got != want {
                errs.push(format!("{label}: {what} = {got}, expected {want}"));
            }
        };
        diff(
            "action",
            format!("{:?}", n.action),
            format!("{:?}", x.action),
        );
        diff(
            "bounds",
            format!("{:?}", n.bounds),
            format!("{:?}", x.bounds),
        );
        diff(
            "provisional",
            n.provisional.to_string(),
            x.provisional.to_string(),
        );
        diff(
            "requires_confirmation",
            n.requires_confirmation.to_string(),
            x.requires_confirmation.to_string(),
        );
        if let Some(ranges) = &x.ranges {
            diff("ranges", format!("{:?}", n.ranges), format!("{ranges:?}"));
        }
        if let Some(chunks) = &x.chunks {
            diff("chunks", format!("{:?}", n.chunks), format!("{chunks:?}"));
        }
        if let Some(count) = x.chunk_count {
            diff("chunk count", n.chunks.len().to_string(), count.to_string());
        }
        if let Some(consumes) = &x.consumes {
            let got: Vec<(String, String, i64)> = n
                .consumes
                .iter()
                .map(|c: &Consumption| (c.parent_ref.to_string(), c.state_instance.clone(), c.seq))
                .collect();
            let want: Vec<(String, String, i64)> = consumes
                .iter()
                .map(|c| (c.parent_ref.to_string(), c.state_instance.clone(), c.seq))
                .collect();
            diff("consumes", format!("{got:?}"), format!("{want:?}"));
        }
        for needle in &x.reason_contains {
            if !n.reason.contains(needle.as_str()) {
                errs.push(format!(
                    "{label}: reason `{}` does not contain `{needle}`",
                    n.reason
                ));
            }
        }
    }
    errs
}

#[tokio::test]
async fn planner_golden_fixtures() {
    let paths = fixture_paths();
    assert!(
        paths.len() >= 10,
        "expected the §14 fixture set, found {}",
        paths.len()
    );
    let mut errs = Vec::new();
    for p in &paths {
        errs.extend(check(p).await);
    }
    assert!(errs.is_empty(), "\n{}", errs.join("\n"));
}
