//! The Syncing Engine's read-only actions (v5, `docs/design/v5.0.0.md` §5.2, §5.3, §5.7, §5.9):
//! `plan pipes`, `show graph`, `show runs` and `show run <id>`.
//!
//! None of them moves data. `plan pipes` and `show graph` resolve the selection against the
//! implicit pipe graph exactly as `sync pipes` does (same filters, `--parents`, `--children`,
//! `--begin`, `--end`, `--resume`); `show runs` reads the run history the engine stores on the
//! instance (`mrsm:engine_runs` and `mrsm:engine_nodes`).

use std::fmt::Write as _;

use meerschaum_core::SuccessTuple;
use meerschaum_engine::viz;
use meerschaum_engine::{Action, EngineReader, NodeStatus, PipeRef, RunFilter, RunRecord};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::graph::{GraphModel, GraphOptions, GraphTheme};
use crate::format::progress::Progress;
use crate::value::ArgsMap;

use super::pipes::selected_pipes;
use super::sync::{SyncArgs, engine, run_options};
use super::{flag, info, int, session, warn, words};

fn json(rt: &mut Runtime<'_>, value: &serde_json::Value) {
    rt.print(&format!("{value}\n"));
}

/// `plan pipes`: print the plan for the selection without syncing.
pub fn plan_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let session = session(rt);
    let progress = Progress::start(rt, args, "Selecting pipes");
    let (inst, pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() && !args.contains_key("resume") {
        return Ok(SuccessTuple::fail("No pipes selected."));
    }
    progress.set_message(format!("Reading {} pipe registrations", pipes.len()));
    let engine = engine::build(rt, &session, &inst, &pipes, true, false, None)?;
    let sync_args = SyncArgs::from_args(args);
    let opts = run_options(args, &inst, &pipes, &sync_args);
    progress.set_message("Planning the run");
    let planned = match session.block_on(engine.plan(&opts)) {
        Ok(planned) => planned,
        Err(error) => return Ok(SuccessTuple::fail(error)),
    };
    // Provisional nodes as the executor re-plans them once their parents have synced: a
    // backfill starts on the chunk grid, not at the tail window the static plan shows.
    let expected = match session.block_on(engine.expected_plan(&planned, &opts)) {
        Ok(plan) => plan,
        Err(error) => return Ok(SuccessTuple::fail(error)),
    };
    drop(progress);
    if nopretty {
        match serde_json::to_value(&expected) {
            Ok(value) => json(rt, &value),
            Err(error) => return Ok(SuccessTuple::fail(error.to_string())),
        }
        return Ok(SuccessTuple::success());
    }
    for warning in &planned.subgraph.warnings {
        warn(rt, warning);
    }
    let plan = &expected;
    rt.print(&format!("{plan}"));
    let runs = plan
        .nodes
        .iter()
        .filter(|n| n.action.runs() && !n.provisional)
        .count();
    let provisional = plan.nodes.iter().filter(|n| n.provisional).count();
    let mut summary = format!(
        "Planned {} node{} ({runs} would sync",
        plan.nodes.len(),
        if plan.nodes.len() == 1 { "" } else { "s" }
    );
    if provisional > 0 {
        let _ = write!(
            summary,
            ", {provisional} re-planned after their parents finish, shown as if they sync new rows"
        );
    }
    info(rt, &format!("{summary}). Nothing was synced."));
    Ok(SuccessTuple::success())
}

/// `show graph`: the selection as a DAG.
pub fn show_graph(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let session = session(rt);
    let progress = Progress::start(rt, args, "Selecting pipes");
    let (inst, pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes selected."));
    }
    progress.set_message(format!("Reading {} pipe registrations", pipes.len()));
    let engine = engine::build(rt, &session, &inst, &pipes, true, false, None)?;
    let sync_args = SyncArgs::from_args(args);
    let opts = run_options(args, &inst, &pipes, &sync_args);
    progress.set_message("Resolving the pipe graph");
    let (_graph, sub, _selection) = match session.block_on(engine.resolve(&opts)) {
        Ok(resolved) => resolved,
        Err(error) => return Ok(SuccessTuple::fail(error)),
    };
    drop(progress);
    if nopretty {
        // The real edges, cycles included (a cycle makes the planner's `sub.edges` empty).
        let nodes: Vec<serde_json::Value> = sub
            .order
            .iter()
            .map(|r| {
                serde_json::json!({
                    "pipe_ref": r,
                    "label": r.label(),
                    "target": sub.targets.contains(r),
                    "parents": sub
                        .display_edges()
                        .iter()
                        .filter(|e| &e.child == r)
                        .map(|e| &e.parent)
                        .collect::<Vec<_>>(),
                })
            })
            .collect();
        let edges: Vec<(&PipeRef, &PipeRef)> = sub
            .display_edges()
            .iter()
            .map(|e| (&e.parent, &e.child))
            .collect();
        json(
            rt,
            &serde_json::json!({
                "nodes": nodes,
                "edges": edges,
                "cycles": sub.cycles,
                "warnings": sub.warnings,
            }),
        );
        return Ok(SuccessTuple::success());
    }
    for warning in &sub.warnings {
        warn(rt, warning);
    }
    // `--full` (or `--debug`) lists every child instead of collapsing wide fan-outs.
    let full = flag(args, "full") || flag(args, "debug");
    let instance = inst.keys().to_string();
    // Colors only on a terminal (`rt.live`: stdout is a TTY) and when `formatting:ansi` allows.
    let color = rt.live;
    let text = rt.with_formatter(|f| {
        crate::format::graph::render(
            &GraphModel::from_subgraph(&sub),
            &if color {
                GraphTheme::from_formatter(f)
            } else {
                GraphTheme::plain()
            },
            &GraphOptions {
                unicode: f.settings.unicode,
                width: f.settings.width,
                full,
                default_instance: instance,
            },
        )
    });
    rt.print(&text);
    Ok(SuccessTuple::success())
}

/// A table with left-aligned, space-padded columns.
fn table(header: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = header.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }
    let mut out = String::new();
    let mut line = |cells: Vec<&str>| {
        let last = cells.len() - 1;
        for (i, cell) in cells.iter().enumerate() {
            if i == last {
                let _ = writeln!(out, "{cell}");
            } else {
                let pad = widths[i] - cell.chars().count();
                let _ = write!(out, "{cell}{}  ", " ".repeat(pad));
            }
        }
    };
    line(header.to_vec());
    for row in rows {
        line(row.iter().map(String::as_str).collect());
    }
    out
}

fn reader(
    rt: &mut Runtime<'_>,
    args: &ArgsMap,
) -> Result<(super::Instance, super::Session, EngineReader), ActionError> {
    let inst = super::get_instance(args, rt)?;
    let session = session(rt);
    let bundle = engine::registry(rt.config(), &session, &inst, &[], false)?;
    let reader = EngineReader::new(bundle.registry, bundle.config);
    Ok((inst, session, reader))
}

/// `show runs` (the run history) or `show run <id>` (one run's Gantt chart).
pub fn show_runs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let progress = Progress::start(rt, args, "Reading the run history");
    let (_inst, session, reader) = reader(rt, args)?;
    let ids = words(args, "action");
    if let Some(run_id) = ids.iter().find(|w| !w.is_empty()) {
        progress.set_message(format!("Reading run {run_id}"));
        let result = show_run(rt, &session, &reader, run_id, nopretty);
        drop(progress);
        return result;
    }
    let filter = RunFilter {
        limit: Some(
            int(args, "last")
                .and_then(|n| usize::try_from(n).ok())
                .unwrap_or(10),
        ),
        ..Default::default()
    };
    let runs = session.block_on(reader.runs(&filter));
    drop(progress);
    let runs = match runs {
        Ok(runs) => runs,
        Err(error) => {
            return Ok(SuccessTuple::fail(format!(
                "Cannot read the run history: {error}"
            )));
        }
    };
    if nopretty {
        match serde_json::to_value(&runs) {
            Ok(value) => json(rt, &value),
            Err(error) => return Ok(SuccessTuple::fail(error.to_string())),
        }
        return Ok(SuccessTuple::success());
    }
    if runs.is_empty() {
        info(rt, "No runs have been recorded on this instance.");
        return Ok(SuccessTuple::success());
    }
    let rows: Vec<Vec<String>> = runs
        .iter()
        .map(|r| {
            let ok = r.counts.get("succeeded").copied().unwrap_or(0);
            let mut targets: Vec<String> =
                r.run.targets.iter().take(2).map(|t| t.label()).collect();
            if r.run.targets.len() > 2 {
                targets.push(format!("+{} more", r.run.targets.len() - 2));
            }
            vec![
                r.run.run_id.clone(),
                r.run.started.format("%Y-%m-%d %H:%M:%S").to_string(),
                r.status.name().to_string(),
                viz::fmt_duration(r.duration_seconds),
                format!("{ok}/{}", r.run.num_nodes),
                targets.join(", "),
            ]
        })
        .collect();
    let text = table(
        &[
            "run",
            "started (UTC)",
            "status",
            "wall",
            "ok/nodes",
            "targets",
        ],
        &rows,
    );
    rt.print(&text);
    info(rt, "Run `show run <run>` for a run's Gantt chart.");
    Ok(SuccessTuple::success())
}

fn show_run(
    rt: &mut Runtime<'_>,
    session: &super::Session,
    reader: &EngineReader,
    run_id: &str,
    nopretty: bool,
) -> ActionResult {
    let detail = match session.block_on(reader.run_detail(run_id)) {
        Ok(Some(detail)) => detail,
        Ok(None) => return Ok(SuccessTuple::fail(format!("No run '{run_id}' was found."))),
        Err(error) => {
            return Ok(SuccessTuple::fail(format!(
                "Cannot read run {run_id}: {error}"
            )));
        }
    };
    if nopretty {
        match serde_json::to_value(&detail) {
            Ok(value) => json(rt, &value),
            Err(error) => return Ok(SuccessTuple::fail(error.to_string())),
        }
        return Ok(SuccessTuple::success());
    }
    let record = RunRecord {
        run: detail.summary.run.clone(),
        nodes: detail.nodes.clone(),
    };
    rt.print(&gantt(&record, rt.columns));
    if let Some(message) = &record.run.message
        && detail.summary.status.name() == "failed"
    {
        warn(rt, message);
    }
    Ok(SuccessTuple::success())
}

/// The critical path of a recorded run: the executed nodes with no slack, in planned order.
pub fn critical_path(record: &RunRecord) -> Vec<PipeRef> {
    let mut path: Vec<_> = record
        .nodes
        .iter()
        .filter(|n| {
            n.slack_seconds.is_some_and(|s| s <= 1e-9)
                && matches!(n.status, NodeStatus::Succeeded | NodeStatus::Failed)
        })
        .collect();
    path.sort_by(|a, b| {
        a.planned_start_seconds
            .partial_cmp(&b.planned_start_seconds)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    path.into_iter().map(|n| n.pipe_ref.clone()).collect()
}

/// The static Gantt chart of a run, sized to the terminal.
pub fn gantt(record: &RunRecord, columns: usize) -> String {
    let label_w = record
        .nodes
        .iter()
        .map(|n| n.pipe_ref.label().chars().count())
        .max()
        .unwrap_or(4);
    // `label  bar  dur summary(28) mark`: 2 + 2 + 7 + 29 + 1 columns around the bar.
    let width = columns.saturating_sub(label_w + 41).clamp(10, 60);
    let mut text = viz::render_gantt(record, &critical_path(record), width);
    if record
        .nodes
        .iter()
        .any(|n| n.action == Action::Rebuild && n.status == NodeStatus::RebuildPending)
    {
        text.push_str("rebuild_pending: re-run with --yes to rebuild.\n");
    }
    text
}
