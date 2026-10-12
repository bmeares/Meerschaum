//! Visualization (§5.9): the static ASCII Gantt summary printed after a run off a TTY, and the
//! ASCII DAG of a selection (`mrsm show graph`). The live `ratatui` view and the console's
//! Plotly timeline render from the same [`RunRecord`] and event stream in later waves.

use std::collections::HashMap;
use std::fmt::Write as _;

use crate::node::PipeRef;
use crate::runs::{NodeStatus, RunRecord};
use crate::selection::Subgraph;

/// `48s`, `2m14s`, `1h02m`.
pub fn fmt_duration(seconds: f64) -> String {
    let s = seconds.max(0.0).round() as u64;
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{}h{:02}m", s / 3600, (s % 3600) / 60)
    }
}

fn thousands(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Render a finished run as a static ASCII Gantt chart.
pub fn render_gantt(run: &RunRecord, critical_path: &[PipeRef], width: usize) -> String {
    let width = width.max(10);
    let start = run.run.started;
    let end = run
        .run
        .finished
        .or_else(|| run.nodes.iter().filter_map(|n| n.finished).max())
        .unwrap_or(start);
    let wall = (end - start).num_milliseconds().max(1) as f64 / 1000.0;
    let mut out = String::new();
    let path = critical_path
        .iter()
        .map(|r| r.parts().metric_key.to_string())
        .collect::<Vec<_>>()
        .join(" → ");
    let _ = write!(
        out,
        "run {}  {} nodes  {} workers  wall {}",
        run.run.run_id,
        run.nodes.len(),
        run.run.workers,
        fmt_duration(wall)
    );
    if !path.is_empty() {
        let _ = write!(out, "  critical path: {path}");
    }
    out.push('\n');
    let label_w = run
        .nodes
        .iter()
        .map(|n| n.pipe_ref.parts().metric_key.chars().count())
        .max()
        .unwrap_or(4);
    let cell = |t: chrono::DateTime<chrono::Utc>| -> usize {
        let secs = (t - start).num_milliseconds().max(0) as f64 / 1000.0;
        ((secs / wall) * width as f64).round().min(width as f64) as usize
    };
    for n in &run.nodes {
        let label = n.pipe_ref.parts().metric_key;
        let pad = label_w - label.chars().count();
        let (bar, dur, summary, mark) = match (n.status, n.started, n.finished) {
            (NodeStatus::Succeeded | NodeStatus::Failed, Some(s), Some(f)) => {
                let (a, b) = (cell(s), cell(f).max(cell(s) + 1).min(width));
                let after = if n.status == NodeStatus::Succeeded {
                    "░".repeat(width - b)
                } else {
                    " ".repeat(width - b)
                };
                let bar = format!("{}{}{after}", "░".repeat(a), "█".repeat(b - a));
                let secs = n
                    .duration_seconds
                    .unwrap_or_else(|| (f - s).num_milliseconds() as f64 / 1000.0);
                let summary = if n.status == NodeStatus::Succeeded {
                    let mut parts = Vec::new();
                    if n.inserted > 0 {
                        parts.push(format!("+{} ins", thousands(n.inserted)));
                    }
                    if n.updated > 0 {
                        parts.push(format!("~{} upd", thousands(n.updated)));
                    }
                    if n.upserted > 0 {
                        parts.push(format!("+{} ups", thousands(n.upserted)));
                    }
                    if n.deleted > 0 {
                        parts.push(format!("-{} del", thousands(n.deleted)));
                    }
                    if parts.is_empty() {
                        parts.push("no changes".into());
                    }
                    parts.join("  ")
                } else {
                    let msg = n.message.clone().unwrap_or_default();
                    let first = msg
                        .lines()
                        .next()
                        .unwrap_or("")
                        .chars()
                        .take(40)
                        .collect::<String>();
                    format!("error: {first}")
                };
                let mark = if n.status == NodeStatus::Succeeded {
                    "✔"
                } else {
                    "✘"
                };
                (bar, fmt_duration(secs), summary, mark)
            }
            (status, _, _) => {
                let word = match status {
                    NodeStatus::Skipped => "skip",
                    NodeStatus::UpstreamFailed => "upstream_failed",
                    NodeStatus::RebuildPending => "rebuild_pending",
                    NodeStatus::Observed => "observe",
                    NodeStatus::Pending => "never ran",
                    _ => "?",
                };
                let mut bar = "·".to_string();
                bar.push_str(&" ".repeat(width - 1));
                (bar, String::new(), word.to_string(), "○")
            }
        };
        let _ = writeln!(
            out,
            "{label}{}  {bar}  {dur:<6} {summary:<28} {mark}",
            " ".repeat(pad)
        );
    }
    out
}

/// Render a selection as an indented DAG: each node at the depth of its longest path from a
/// root, with its in-selection parents listed when it has more than one or is not directly
/// below the previous line.
pub fn render_graph(sub: &Subgraph) -> String {
    let mut depth: HashMap<&PipeRef, usize> = HashMap::new();
    for r in &sub.order {
        let d = sub
            .parent_edges(r)
            .filter_map(|e| depth.get(&e.parent))
            .max()
            .map_or(0, |d| d + 1);
        depth.insert(r, d);
    }
    let mut out = String::new();
    for r in &sub.order {
        let parents: Vec<String> = sub.parent_edges(r).map(|e| e.parent.label()).collect();
        let marker = if sub.targets.contains(r) { "*" } else { "" };
        let _ = write!(out, "{}{}{marker}", "  ".repeat(depth[r]), r.label());
        if !parents.is_empty() {
            let _ = write!(out, "  ← {}", parents.join(", "));
        }
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::Action;
    use crate::runs::{NodeRow, RunRow};
    use chrono::{TimeDelta, Utc};

    #[test]
    fn gantt() {
        let t0 = Utc::now();
        let r = |m: &str| PipeRef::new("sql:main", "sql:main", m, None);
        let mut a = NodeRow::new("run", &r("raw_events"), Action::Root, "");
        a.status = NodeStatus::Succeeded;
        a.started = Some(t0);
        a.finished = Some(t0 + TimeDelta::seconds(48));
        a.duration_seconds = Some(48.0);
        a.inserted = 12_301;
        let mut b = NodeRow::new("run", &r("report"), Action::Incremental, "");
        b.status = NodeStatus::UpstreamFailed;
        let run = RecordBuilder::run(t0, t0 + TimeDelta::seconds(96), vec![a, b]);
        let text = render_gantt(&run, &[r("raw_events")], 20);
        assert!(text.contains("wall 1m36s"), "{text}");
        assert!(text.contains("+12,301 ins"), "{text}");
        assert!(text.contains("██████████░░░░░░░░░░"), "{text}");
        assert!(text.contains("upstream_failed"), "{text}");
        assert_eq!(fmt_duration(3720.0), "1h02m");
    }

    struct RecordBuilder;

    impl RecordBuilder {
        fn run(
            started: chrono::DateTime<Utc>,
            finished: chrono::DateTime<Utc>,
            nodes: Vec<NodeRow>,
        ) -> RunRecord {
            RunRecord {
                run: RunRow {
                    run_id: "01JA".into(),
                    started,
                    finished: Some(finished),
                    success: Some(true),
                    message: None,
                    targets: vec![],
                    workers: 4,
                    num_nodes: nodes.len(),
                    resumed_from: None,
                    holder: "test".into(),
                },
                nodes,
            }
        }
    }
}
