//! The inline live view of a graph run: one row per running node at the bottom of the terminal
//! while `sync pipes --parents / --children / --resume` and `sync pipelines` run on a terminal.
//!
//! ```text
//!   · Pipe('sql:src', 'raw') incremental: 2 new rows upstream        <- printed into scrollback
//!   ✔ Pipe('sql:src', 'raw') +2 ins ~0 upd +0 ups
//! ⠹ Syncing 1/3 pipes · 2 running  4.2s  w: watch                     <- the live region
//!   Pipe('sql:a', 'b')  incremental  3/10 chunks  +1.2k ins ~0 upd +0 ups  2.4k rows/s  3.1s
//!   Pipe('sql:c', 'd')  backfill · attempt 2  1 chunk  +0 ins ~0 upd +0 ups  0.8s
//! ```
//!
//! Two halves:
//!
//! - [`LiveRun`] is the pure model: it folds engine [`Event`]s into the running rows and returns
//!   the permanent lines (plan decisions, retries, finished nodes) the caller prints above the
//!   region. [`LiveRun::lines`] renders the region for a terminal size. Unit tests drive it with
//!   synthetic event sequences.
//! - [`Region`] paints those lines in place with cursor movement (no alternate screen). It is
//!   process-wide like [`crate::format::progress`]: `Runtime::print` / `eprint`, the prompts and
//!   forwarded worker output call [`suspend`] / [`resume`] through the progress hooks, so the
//!   region is erased before any other output and redrawn after it. A signal that ends the process
//!   erases it (`clear_for_signal`) and gives the terminal its modes back.
//!
//! The region never wraps: every line is cut to the terminal width minus one column, so the number
//! of rows to erase is the number of lines drawn. A terminal made narrower since the last draw
//! reflows those lines; the erase then counts the rows each one takes at the new width.
//!
//! [`Keys`] reads single key presses (`w` opens the full-screen run monitor) without raw mode:
//! only canonical input and echo are turned off, so Ctrl-C still raises SIGINT and `\n` output
//! still returns the carriage.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use meerschaum_engine::runs::{NodeStatus, RunRecord};
use meerschaum_engine::{ChunkSource, Event, EventKind, NodeOutcome, PipeRef};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

/// The redraw period of the region (10 Hz).
pub const REDRAW: Duration = Duration::from_millis(100);

const UNICODE_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const ASCII_FRAMES: &[&str] = &["|", "/", "-", "\\"];

/// How the region and the permanent lines look.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveStyle {
    /// Braille spinner and `·` / `…` separators (else ASCII). The finished-node glyphs
    /// (`✔ ✘ ○`) are the ones graph runs always printed.
    pub unicode: bool,
    /// ANSI colors.
    pub color: bool,
}

impl LiveStyle {
    fn paint(&self, code: &str, text: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }

    fn dot(&self) -> &'static str {
        if self.unicode { "·" } else { "-" }
    }

    fn ellipsis(&self) -> &'static str {
        if self.unicode { "…" } else { "..." }
    }
}

/// Row counts of a node so far.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Counts {
    inserted: u64,
    updated: u64,
    upserted: u64,
}

impl Counts {
    fn add(&mut self, other: Counts) {
        self.inserted += other.inserted;
        self.updated += other.updated;
        self.upserted += other.upserted;
    }

    fn total(&self) -> u64 {
        self.inserted + self.updated + self.upserted
    }

    fn is_zero(&self) -> bool {
        self.total() == 0
    }
}

/// One running node.
#[derive(Debug, Clone)]
struct Running {
    label: String,
    /// The run this node belongs to (a stream's triggered children run alongside the first).
    run_id: String,
    /// Events were missed since this row last changed: it may have finished meanwhile, so it is
    /// hidden until its next event.
    stale: bool,
    action: String,
    attempt: u32,
    started: Instant,
    /// The last attempt failed and another one is scheduled.
    retrying: bool,
    /// Planned windows of this node, when the executor said how many.
    windows: Option<usize>,
    windows_done: usize,
    fetches_done: usize,
    /// Counts of finished windows (or of the fetch chunks they contained).
    committed: Counts,
    /// Counts of the fetch chunks since the last window ended.
    pending: Counts,
    /// Rows handed to the sync (fetch chunks).
    rows: u64,
}

impl Running {
    fn new(pipe_ref: &PipeRef, run_id: &str, now: Instant) -> Running {
        Running {
            label: pipe_ref.label(),
            run_id: run_id.to_string(),
            stale: false,
            action: String::new(),
            attempt: 1,
            started: now,
            retrying: false,
            windows: None,
            windows_done: 0,
            fetches_done: 0,
            committed: Counts::default(),
            pending: Counts::default(),
            rows: 0,
        }
    }

    fn counts(&self) -> Counts {
        let mut counts = self.committed;
        counts.add(self.pending);
        counts
    }
}

/// The model of the live view: running rows, run totals and the permanent lines.
#[derive(Debug)]
pub struct LiveRun {
    style: LiveStyle,
    /// Print plan, retry and finished lines (graph runs). `--loop` / `--listen` keep their v4
    /// per-pipe output and only show the rows.
    node_lines: bool,
    /// The first run seen (the one `w` opens).
    run_id: Option<String>,
    /// The runs going on (a stream's triggered children run alongside the first).
    runs: HashSet<String>,
    started: Option<Instant>,
    total: usize,
    finished: usize,
    failed: usize,
    running: IndexMap<PipeRef, Running>,
    planned: HashMap<PipeRef, (String, String)>,
    printed: HashSet<PipeRef>,
    missed: u64,
    /// Plan and retry lines are cut to this many columns (the terminal width).
    width: Option<usize>,
}

impl LiveRun {
    /// An empty model. `node_lines`: return the permanent node lines (graph runs).
    pub fn new(style: LiveStyle, node_lines: bool) -> LiveRun {
        LiveRun {
            style,
            node_lines,
            run_id: None,
            runs: HashSet::new(),
            started: None,
            total: 0,
            finished: 0,
            failed: 0,
            running: IndexMap::new(),
            planned: HashMap::new(),
            printed: HashSet::new(),
            missed: 0,
            width: None,
        }
    }

    /// Cut the plan and retry lines to `columns` (the terminal width; the full reasons are in
    /// `plan pipes` and `show run`). Finished lines are never cut.
    pub fn set_width(&mut self, columns: usize) {
        self.width = Some(columns);
    }

    fn cut(&self, line: String) -> String {
        match self.width {
            Some(columns) if visible_width(&line) >= columns => {
                let plain = strip_sgr(&line);
                truncate(&plain, columns.saturating_sub(1).max(10), self.style)
            }
            _ => line,
        }
    }

    /// The current run (from its first event; with runs alongside, the first one).
    pub fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }

    /// Events lost to a lagging receiver so far.
    pub fn missed_events(&self) -> u64 {
        self.missed
    }

    /// The pipes currently shown as running, in start order.
    pub fn running_labels(&self) -> Vec<String> {
        self.visible().map(|r| r.label.clone()).collect()
    }

    /// The rows to show (not stale after a gap).
    fn visible(&self) -> impl Iterator<Item = &Running> {
        self.running.values().filter(|r| !r.stale)
    }

    fn row(&mut self, pipe_ref: &PipeRef, run_id: &str, now: Instant) -> &mut Running {
        let row = self
            .running
            .entry(pipe_ref.clone())
            .or_insert_with(|| Running::new(pipe_ref, run_id, now));
        row.stale = false;
        row
    }

    /// Fold one event in; returns the permanent lines (without newlines) to print above the region.
    pub fn apply(&mut self, event: &Event, now: Instant) -> Vec<String> {
        if self.run_id.is_none() {
            self.run_id = Some(event.run_id.clone());
            self.started.get_or_insert(now);
        }
        if let EventKind::RunStarted { nodes, .. } = &event.kind {
            if self.runs.is_empty() {
                // A new run (or the next lap of a loop).
                self.total = *nodes;
                self.finished = 0;
                self.failed = 0;
                self.running.clear();
                self.planned.clear();
                self.printed.clear();
                self.started = Some(now);
                self.run_id = Some(event.run_id.clone());
            } else if !self.runs.contains(&event.run_id) {
                // Another run alongside this one (a stream's triggered children): add it up.
                self.total += nodes;
            }
            self.runs.insert(event.run_id.clone());
            return Vec::new();
        }
        if let EventKind::RunFinished { .. } = &event.kind {
            if self.runs.remove(&event.run_id) {
                self.running.retain(|_, row| row.run_id != event.run_id);
            }
            return Vec::new();
        }
        let Some(pipe_ref) = event.pipe_ref.as_ref() else {
            return Vec::new();
        };
        let label = pipe_ref.label();
        let style = self.style;
        let run_id = event.run_id.as_str();
        if let Some(row) = self.running.get_mut(pipe_ref) {
            row.stale = false;
        }
        let mut lines = Vec::new();
        match &event.kind {
            EventKind::NodePlanned {
                action,
                reason,
                provisional,
            } => {
                use meerschaum_engine::Action;
                // Only final decisions: a provisional plan is planned again once the parents
                // finish. Skips and pending rebuilds get their own final line.
                let runs = !*provisional
                    && !matches!(
                        action,
                        Action::Skip
                            | Action::Observe
                            | Action::UpstreamFailed
                            | Action::RebuildPending
                    );
                let key = (action.to_string(), reason.clone());
                if runs && self.planned.get(pipe_ref) != Some(&key) {
                    let reason = if reason.is_empty() {
                        String::new()
                    } else {
                        format!(": {}", first_line(reason))
                    };
                    lines.push(self.cut(format!(
                        "  {} {label} {}{reason}",
                        style.paint("2", style.dot()),
                        style.paint("36", &key.0),
                    )));
                    self.planned.insert(pipe_ref.clone(), key);
                }
            }
            EventKind::NodeStarted { action, attempt } => {
                let row = self.row(pipe_ref, run_id, now);
                // Another attempt starts over.
                *row = Running::new(pipe_ref, run_id, now);
                row.action = action.to_string();
                row.attempt = *attempt;
            }
            EventKind::ChunkStarted { source, total, .. } => {
                let row = self.row(pipe_ref, run_id, now);
                if *source == ChunkSource::Window && total.is_some() {
                    row.windows = *total;
                }
            }
            EventKind::ChunkFinished {
                source,
                rows,
                inserted,
                updated,
                upserted,
                ..
            } => {
                let row = self.row(pipe_ref, run_id, now);
                let counts = Counts {
                    inserted: *inserted,
                    updated: *updated,
                    upserted: *upserted,
                };
                match source {
                    ChunkSource::Fetch => {
                        row.fetches_done += 1;
                        row.rows += rows.unwrap_or(0);
                        row.pending.add(counts);
                    }
                    ChunkSource::Window => {
                        row.windows_done += 1;
                        // A window's counts already hold its fetch chunks' counts.
                        if counts.is_zero() {
                            let pending = row.pending;
                            row.committed.add(pending);
                        } else {
                            row.committed.add(counts);
                        }
                        row.pending = Counts::default();
                    }
                }
            }
            EventKind::StreamFlushed {
                rows,
                inserted,
                updated,
                upserted,
                ..
            } => {
                if let Some(row) = self.running.get_mut(pipe_ref) {
                    row.rows += rows;
                    row.committed.add(Counts {
                        inserted: *inserted,
                        updated: *updated,
                        upserted: *upserted,
                    });
                }
            }
            EventKind::NodeFailed {
                error,
                attempt,
                will_retry: true,
                ..
            } => {
                if let Some(row) = self.running.get_mut(pipe_ref) {
                    row.retrying = true;
                }
                lines.push(self.cut(format!(
                    "  {} {label} attempt {attempt} failed, retrying: {}",
                    style.paint("33", if style.unicode { "↻" } else { "~" }),
                    first_line(error)
                )));
            }
            EventKind::NodeFinished { result } => {
                self.done(pipe_ref, false);
                lines.push(self.finished_line(
                    &label,
                    result.inserted,
                    result.updated,
                    result.upserted,
                ));
            }
            EventKind::NodeFailed {
                error,
                will_retry: false,
                ..
            } => {
                self.done(pipe_ref, true);
                lines.push(format!(
                    "  {} {label} {}",
                    style.paint("31", "\u{2718}"),
                    first_line(error)
                ));
            }
            EventKind::NodeSkipped { reason } => {
                self.done(pipe_ref, false);
                lines.push(format!(
                    "  {} {label} {reason}",
                    style.paint("2", "\u{25cb}")
                ));
            }
            EventKind::NodeRebuildPending { reason } => {
                self.done(pipe_ref, false);
                lines.push(format!(
                    "  {} {label} rebuild pending: {}",
                    style.paint("33", "\u{25cb}"),
                    first_line(reason)
                ));
            }
            _ => {}
        }
        if self.node_lines { lines } else { Vec::new() }
    }

    fn done(&mut self, pipe_ref: &PipeRef, failed: bool) {
        self.running.shift_remove(pipe_ref);
        if self.printed.insert(pipe_ref.clone()) {
            self.finished += 1;
            if failed {
                self.failed += 1;
            }
        }
    }

    /// The receiver lagged and `count` events were dropped: note the gap. Every running row is
    /// hidden until its next event (it may have finished in the gap); [`LiveRun::resync`] prints
    /// the lost final lines from the run's results once it ends.
    pub fn missed(&mut self, count: u64) -> Vec<String> {
        self.missed += count;
        for row in self.running.values_mut() {
            row.stale = true;
        }
        if !self.node_lines {
            return Vec::new();
        }
        vec![format!(
            "  {} {count} live event{} missed; the results below are read from the run itself.",
            self.style.paint("33", self.style.ellipsis()),
            if count == 1 { " was" } else { "s were" }
        )]
    }

    /// The run ended: print the nodes whose final line never printed (a lagging receiver) from
    /// the run's outcomes, then the skipped and pending nodes from its `record`, and drop the
    /// running rows.
    pub fn resync(&mut self, outcomes: &[NodeOutcome], record: Option<&RunRecord>) -> Vec<String> {
        let style = self.style;
        let mut lines = Vec::new();
        for outcome in outcomes {
            if self.printed.contains(&outcome.pipe_ref) {
                continue;
            }
            let label = outcome.pipe_ref.label();
            let result = &outcome.result;
            self.done(&outcome.pipe_ref, !result.success);
            lines.push(if result.success {
                self.finished_line(&label, result.inserted, result.updated, result.upserted)
            } else {
                format!(
                    "  {} {label} {}",
                    style.paint("31", "\u{2718}"),
                    first_line(&result.message)
                )
            });
        }
        for node in record.map(|r| r.nodes.as_slice()).unwrap_or_default() {
            if self.printed.contains(&node.pipe_ref) {
                continue;
            }
            let label = node.pipe_ref.label();
            let line = match node.status {
                NodeStatus::Skipped | NodeStatus::Observed => {
                    format!("  {} {label} {}", style.paint("2", "\u{25cb}"), node.reason)
                }
                NodeStatus::UpstreamFailed => format!(
                    "  {} {label} upstream_failed: {}",
                    style.paint("2", "\u{25cb}"),
                    node.reason
                ),
                NodeStatus::RebuildPending => format!(
                    "  {} {label} rebuild pending: {}",
                    style.paint("33", "\u{25cb}"),
                    first_line(&node.reason)
                ),
                NodeStatus::Succeeded => {
                    self.finished_line(&label, node.inserted, node.updated, node.upserted)
                }
                NodeStatus::Failed => format!(
                    "  {} {label} {}",
                    style.paint("31", "\u{2718}"),
                    first_line(node.message.as_deref().unwrap_or(&node.reason))
                ),
                NodeStatus::Pending => continue,
            };
            self.done(&node.pipe_ref, node.status == NodeStatus::Failed);
            lines.push(line);
        }
        self.running.clear();
        self.runs.clear();
        if self.node_lines { lines } else { Vec::new() }
    }

    fn finished_line(&self, label: &str, inserted: u64, updated: u64, upserted: u64) -> String {
        format!(
            "  {} {label} +{inserted} ins ~{updated} upd +{upserted} ups",
            self.style.paint("32", "\u{2714}"),
        )
    }

    /// The region for a terminal of `width` × `height`: a header and one row per running node
    /// (empty when no run is going on). `tick` animates the spinner; `hint` is the key help
    /// (`w: watch`). No line is wider than `width - 1` columns.
    pub fn lines(
        &self,
        now: Instant,
        tick: usize,
        width: usize,
        height: usize,
        hint: Option<&str>,
    ) -> Vec<String> {
        if self.runs.is_empty() && self.running.is_empty() {
            return Vec::new();
        }
        let rows: Vec<&Running> = self.visible().collect();
        let style = self.style;
        let budget = width.saturating_sub(1).max(10);
        let frames = if style.unicode {
            UNICODE_FRAMES
        } else {
            ASCII_FRAMES
        };
        let spinner = frames[tick % frames.len()];
        let dot = style.dot();
        let mut header = Vec::new();
        let total = self.total.max(self.finished + rows.len());
        header.push((0, format!("Syncing {}/{total} pipes", self.finished)));
        header.push((2, format!("{dot} {} running", rows.len())));
        if self.failed > 0 {
            header.push((1, format!("{dot} {} failed", self.failed)));
        }
        if self.missed > 0 {
            header.push((3, format!("{dot} {} events missed", self.missed)));
        }
        let elapsed = self
            .started
            .map(|s| elapsed_text(now.saturating_duration_since(s)))
            .unwrap_or_default();
        header.push((1, format!(" {elapsed}")));
        if let Some(hint) = hint {
            header.push((4, format!(" {hint}")));
        }
        let mut lines = vec![fit_header(spinner, &header, budget, style)];

        // Header + rows (+ the "more" row) within the terminal height, keeping a row of
        // scrollback visible.
        let max_rows = height.saturating_sub(2).max(1);
        let shown = if rows.len() > max_rows {
            max_rows.saturating_sub(1).max(1)
        } else {
            rows.len()
        };
        for row in rows.iter().take(shown) {
            lines.push(row_line(row, now, budget, style));
        }
        let more = rows.len() - shown;
        if more > 0 {
            lines.push(truncate(
                &format!("  {} and {more} more running", style.ellipsis()),
                budget,
                style,
            ));
        }
        lines
    }
}

/// `header` parts by priority (lower first), kept in order while they fit.
fn fit_header(spinner: &str, parts: &[(u8, String)], budget: usize, style: LiveStyle) -> String {
    let mut used = UnicodeWidthStr::width(spinner) + 1;
    let mut keep = vec![false; parts.len()];
    let mut order: Vec<usize> = (0..parts.len()).collect();
    order.sort_by_key(|i| parts[*i].0);
    for i in order {
        let w = UnicodeWidthStr::width(parts[i].1.as_str()) + 1;
        if used + w <= budget || parts[i].0 == 0 {
            keep[i] = true;
            used += w;
        }
    }
    let mut text = String::new();
    for (i, (prio, part)) in parts.iter().enumerate() {
        if !keep[i] {
            continue;
        }
        let part = if *prio == 0 {
            truncate(part, budget.saturating_sub(2), style)
        } else {
            part.clone()
        };
        text.push(' ');
        text.push_str(&if *prio == 0 {
            part
        } else {
            style.paint("2", &part)
        });
    }
    format!("{}{text}", style.paint("36", spinner))
}

fn row_line(row: &Running, now: Instant, budget: usize, style: LiveStyle) -> String {
    let dot = style.dot();
    let elapsed_d = now.saturating_duration_since(row.started);
    let elapsed = elapsed_text(elapsed_d);
    let mut action = if row.action.is_empty() {
        "starting".to_string()
    } else {
        row.action.clone()
    };
    if row.retrying {
        action = format!("{action} {dot} retrying");
    } else if row.attempt > 1 {
        action = format!("{action} {dot} attempt {}", row.attempt);
    }
    let chunks = match row.windows {
        Some(total) if total > 1 => Some(format!("{}/{total} chunks", row.windows_done.min(total))),
        _ if row.fetches_done > 0 => Some(format!(
            "{} chunk{}",
            row.fetches_done,
            if row.fetches_done == 1 { "" } else { "s" }
        )),
        _ => None,
    };
    let counts = row.counts();
    let counts_text =
        (row.fetches_done > 0 || row.windows_done > 0 || !counts.is_zero()).then(|| {
            format!(
                "+{} ins ~{} upd +{} ups",
                compact(counts.inserted),
                compact(counts.updated),
                compact(counts.upserted)
            )
        });
    let processed = if row.rows > 0 {
        row.rows
    } else {
        counts.total()
    };
    let secs = elapsed_d.as_secs_f64();
    let rate = (processed > 0 && secs >= 1.0)
        .then(|| format!("{} rows/s", compact((processed as f64 / secs) as u64)));
    // (priority, text): lower priorities are kept first; the display order is the vector's.
    let parts: Vec<(u8, Option<String>)> = vec![
        (0, Some(action)),
        (2, chunks),
        (3, counts_text),
        (4, rate),
        (1, Some(elapsed)),
    ];
    let parts: Vec<(u8, String)> = parts
        .into_iter()
        .filter_map(|(p, t)| t.map(|t| (p, t)))
        .collect();
    const MIN_NAME: usize = 12;
    let indent = 2;
    let name_w = UnicodeWidthStr::width(row.label.as_str());
    let mut keep = vec![false; parts.len()];
    let mut used = indent + name_w.min(MIN_NAME);
    let mut order: Vec<usize> = (0..parts.len()).collect();
    order.sort_by_key(|i| parts[*i].0);
    for i in order {
        let w = UnicodeWidthStr::width(parts[i].1.as_str()) + 2;
        if used + w <= budget {
            keep[i] = true;
            used += w;
        }
    }
    let rest: usize = parts
        .iter()
        .zip(&keep)
        .filter(|(_, k)| **k)
        .map(|((_, t), _)| UnicodeWidthStr::width(t.as_str()) + 2)
        .sum();
    // Cut in the middle: pipes of one connector keep their metric and location apart.
    let name = truncate_middle(
        &row.label,
        budget.saturating_sub(indent + rest).max(1),
        style,
    );
    let mut text = format!("  {name}");
    for ((prio, part), k) in parts.iter().zip(&keep) {
        if !*k {
            continue;
        }
        text.push_str("  ");
        text.push_str(&match prio {
            0 if row.retrying => style.paint("33", part),
            0 => style.paint("36", part),
            _ => style.paint("2", part),
        });
    }
    text
}

/// The first line of a message.
fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or_default()
}

/// `1234` → `1.2k`, `5600000` → `5.6M`.
pub fn compact(n: u64) -> String {
    match n {
        0..1_000 => n.to_string(),
        1_000..1_000_000 => format!("{:.1}k", n as f64 / 1e3),
        1_000_000..1_000_000_000 => format!("{:.1}M", n as f64 / 1e6),
        _ => format!("{:.1}B", n as f64 / 1e9),
    }
}

fn elapsed_text(elapsed: Duration) -> String {
    let secs = elapsed.as_secs();
    if secs < 10 {
        format!("{:.1}s", elapsed.as_secs_f64())
    } else if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m{:02}s", secs / 60, secs % 60)
    } else {
        format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60)
    }
}

/// Cut `text` (plain, no escapes) to `max` columns with an ellipsis.
fn truncate(text: &str, max: usize, style: LiveStyle) -> String {
    if UnicodeWidthStr::width(text) <= max {
        return text.to_string();
    }
    let ellipsis = style.ellipsis();
    let keep = max.saturating_sub(UnicodeWidthStr::width(ellipsis));
    let mut out = String::new();
    let mut used = 0;
    for c in text.chars() {
        let w = UnicodeWidthChar::width(c).unwrap_or(0);
        if used + w > keep {
            break;
        }
        used += w;
        out.push(c);
    }
    out.push_str(ellipsis);
    out
}

/// Cut `text` (plain) to `max` columns, dropping its middle: `Pipe('sql:…', 'mid_3')`.
fn truncate_middle(text: &str, max: usize, style: LiveStyle) -> String {
    if UnicodeWidthStr::width(text) <= max {
        return text.to_string();
    }
    let ellipsis = style.ellipsis();
    let room = max.saturating_sub(UnicodeWidthStr::width(ellipsis));
    if room < 4 {
        return truncate(text, max, style);
    }
    // A third from the start, the rest from the end (the metric and location).
    let head_room = room / 3;
    let mut head = String::new();
    let mut used = 0;
    for c in text.chars() {
        let w = UnicodeWidthChar::width(c).unwrap_or(0);
        if used + w > head_room {
            break;
        }
        used += w;
        head.push(c);
    }
    let tail_room = room - used;
    let mut tail = Vec::new();
    let mut used = 0;
    for c in text.chars().rev() {
        let w = UnicodeWidthChar::width(c).unwrap_or(0);
        if used + w > tail_room {
            break;
        }
        used += w;
        tail.push(c);
    }
    let tail: String = tail.into_iter().rev().collect();
    format!("{head}{ellipsis}{tail}")
}

/// The visible width of a line that may hold SGR escapes.
pub fn visible_width(text: &str) -> usize {
    let mut width = 0;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for c in chars.by_ref() {
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        width += UnicodeWidthChar::width(c).unwrap_or(0);
    }
    width
}

// ---------------------------------------------------------------------------------------------
// The painter.

/// Where the region is drawn.
#[derive(Clone)]
pub enum RegionSink {
    /// The process's stdout (where `Runtime::print` writes on a terminal).
    Stdout,
    /// A test writer and a fixed terminal size.
    Custom {
        /// Receives the bytes.
        write: Arc<dyn Fn(&str) + Send + Sync>,
        /// `(columns, rows)`.
        size: Arc<dyn Fn() -> (usize, usize) + Send + Sync>,
    },
}

impl RegionSink {
    fn write(&self, text: &str) {
        use std::io::Write as _;
        match self {
            RegionSink::Stdout => {
                let mut out = std::io::stdout().lock();
                let _ = out.write_all(text.as_bytes());
                let _ = out.flush();
            }
            RegionSink::Custom { write, .. } => write(text),
        }
    }

    fn size(&self) -> (usize, usize) {
        match self {
            RegionSink::Stdout => terminal_size::terminal_size_of(std::io::stdout())
                .map(|(w, h)| (w.0 as usize, h.0 as usize))
                .filter(|(w, h)| *w > 0 && *h > 0)
                .unwrap_or((80, 24)),
            RegionSink::Custom { size, .. } => size(),
        }
    }
}

struct Painter {
    sink: RegionSink,
    /// The lines that should be on screen.
    lines: Vec<String>,
    /// The visible widths of the lines on screen (empty: nothing drawn).
    drawn: Vec<usize>,
    /// Writers between [`suspend`] and [`resume`] (the main thread's prints and the worker
    /// output forwarder may overlap): the region is drawn only at zero.
    depth: usize,
    /// The last output ended without a newline: do not draw until one is written.
    partial: bool,
    /// The cursor was hidden.
    cursor_hidden: bool,
    /// The unit tests of this crate run in parallel threads of one process, and every
    /// `Runtime::print` calls the hooks: there only the thread that started the region sees it.
    #[cfg(test)]
    owner: std::thread::ThreadId,
}

static ACTIVE: AtomicBool = AtomicBool::new(false);
static PAINTER: Mutex<Option<Painter>> = Mutex::new(None);

fn painter() -> MutexGuard<'static, Option<Painter>> {
    PAINTER.lock().unwrap_or_else(|e| e.into_inner())
}

/// Whether the hooks may touch `painter` from this thread (always, outside the unit tests).
fn owned(painter: &Painter) -> bool {
    #[cfg(test)]
    {
        painter.owner == std::thread::current().id()
    }
    #[cfg(not(test))]
    {
        let _ = painter;
        true
    }
}

/// The bytes that erase `drawn` lines (cursor at the end of the last one) on a terminal of
/// `cols` columns, leaving the cursor at column 0 of the first.
pub fn erase_sequence(drawn: &[usize], cols: usize) -> String {
    if drawn.is_empty() {
        return String::new();
    }
    let cols = cols.max(1);
    // A narrower terminal reflows each line onto ceil(width / cols) rows.
    let rows: usize = drawn.iter().map(|w| w.div_ceil(cols).max(1)).sum();
    let mut out = String::from("\r");
    if rows > 1 {
        out.push_str(&format!("\x1b[{}A", rows - 1));
    }
    out.push_str("\x1b[J");
    out
}

impl Painter {
    fn erase_text(&mut self) -> String {
        let (cols, _) = self.sink.size();
        let text = erase_sequence(&self.drawn, cols);
        self.drawn.clear();
        text
    }

    fn erase(&mut self) {
        let text = self.erase_text();
        if !text.is_empty() {
            self.sink.write(&text);
        }
    }

    fn draw(&mut self) {
        if self.depth > 0 || self.partial || PROMPT.load(Ordering::SeqCst) {
            return;
        }
        let (cols, _) = self.sink.size();
        let budget = cols.saturating_sub(1).max(1);
        let mut text = self.erase_text();
        if !self.lines.is_empty() && !self.cursor_hidden {
            text.push_str("\x1b[?25l");
            self.cursor_hidden = true;
        }
        let mut widths = Vec::with_capacity(self.lines.len());
        for (i, line) in self.lines.iter().enumerate() {
            if i > 0 {
                text.push('\n');
            }
            let w = visible_width(line);
            if w > budget {
                // Never let a line wrap (the erase counts lines): drop the styling and cut it.
                let plain = strip_sgr(line);
                let cut = truncate(
                    &plain,
                    budget,
                    LiveStyle {
                        unicode: true,
                        color: false,
                    },
                );
                widths.push(UnicodeWidthStr::width(cut.as_str()));
                text.push_str(&cut);
            } else {
                widths.push(w);
                text.push_str(line);
            }
        }
        if self.lines.is_empty() && self.cursor_hidden {
            text.push_str("\x1b[?25h");
            self.cursor_hidden = false;
        }
        self.drawn = widths;
        if !text.is_empty() {
            self.sink.write(&text);
        }
    }

    fn close(&mut self) {
        let mut text = self.erase_text();
        if self.cursor_hidden {
            text.push_str("\x1b[?25h");
            self.cursor_hidden = false;
        }
        if !text.is_empty() {
            self.sink.write(&text);
        }
    }
}

fn strip_sgr(text: &str) -> String {
    let mut out = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for c in chars.by_ref() {
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

/// The live region on the terminal; dropping it erases it.
pub struct Region {
    _private: (),
}

impl Region {
    /// Start the region on `sink` (one per process; a second one replaces the first).
    pub fn start(sink: RegionSink) -> Region {
        let mut p = painter();
        if let Some(old) = p.as_mut() {
            old.close();
        }
        *p = Some(Painter {
            sink,
            lines: Vec::new(),
            drawn: Vec::new(),
            depth: 0,
            partial: false,
            cursor_hidden: false,
            #[cfg(test)]
            owner: std::thread::current().id(),
        });
        #[cfg(unix)]
        if matches!(p.as_ref().map(|p| &p.sink), Some(RegionSink::Stdout)) {
            stop_signal::install();
        }
        PROMPT.store(false, Ordering::SeqCst);
        ACTIVE.store(true, Ordering::SeqCst);
        Region { _private: () }
    }

    /// Replace the lines and redraw them now (unless output is being written).
    pub fn show(&self, lines: Vec<String>) {
        if let Some(p) = painter().as_mut() {
            p.lines = lines;
            p.draw();
        }
    }

    /// Replace the lines without drawing: the next redraw (after a print) shows them.
    pub fn set(&self, lines: Vec<String>) {
        if let Some(p) = painter().as_mut() {
            p.lines = lines;
        }
    }

    /// Erase the region and show the cursor (before a full-screen view takes the terminal).
    pub fn hide(&self) {
        if let Some(p) = painter().as_mut() {
            p.lines.clear();
            p.close();
        }
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        let mut p = painter();
        if let Some(mut painter) = p.take() {
            painter.close();
        }
        ACTIVE.store(false, Ordering::SeqCst);
        PROMPT.store(false, Ordering::SeqCst);
    }
}

/// Erase the region and keep it off while other output is written (`Runtime::print` / `eprint`,
/// prompts, worker output). Pair with [`resume`].
pub fn suspend() {
    if !ACTIVE.load(Ordering::SeqCst) {
        return;
    }
    if let Some(p) = painter().as_mut().filter(|p| owned(p)) {
        p.erase();
        p.depth += 1;
    }
}

/// Let the region come back after output was written: it is redrawn once every writer that
/// suspended it is done, when the cursor is at the start of a line (else once a newline is
/// written).
pub fn resume(ended_with_newline: bool) {
    if !ACTIVE.load(Ordering::SeqCst) {
        return;
    }
    if let Some(p) = painter().as_mut().filter(|p| owned(p)) {
        p.depth = p.depth.saturating_sub(1);
        p.partial = !ended_with_newline;
        p.draw();
    }
}

/// A Python worker is waiting for input (its output ended without a newline, like `input()`'s
/// prompt) or wrote a full line again. While it waits, the region stays erased and the terminal
/// has its own modes back (canonical input and echo), so the answer is typed into the worker,
/// not read as keys. Call it before the prompt is written. No-op without a region.
pub(crate) fn worker_prompt(waiting: bool) {
    if !ACTIVE.load(Ordering::SeqCst) {
        return;
    }
    PROMPT.store(waiting, Ordering::SeqCst);
    if waiting {
        keys::restore_saved();
    }
}

/// Whether a Python worker is waiting for input (its last output ended without a newline).
pub fn worker_prompting() -> bool {
    PROMPT.load(Ordering::SeqCst)
}

static PROMPT: AtomicBool = AtomicBool::new(false);

// Worker output held while a full-screen view owns the terminal.

/// How much worker output is held while the run monitor is open (the oldest is dropped beyond).
const HELD_BYTES: usize = 1 << 20;

struct HeldOutput {
    chunks: std::collections::VecDeque<(bool, Vec<u8>)>,
    bytes: usize,
    dropped: usize,
}

static HELD: Mutex<Option<HeldOutput>> = Mutex::new(None);

fn held() -> MutexGuard<'static, Option<HeldOutput>> {
    HELD.lock().unwrap_or_else(|e| e.into_inner())
}

/// Hold Python worker output from now on (the run monitor is about to take the terminal), until
/// [`release_worker_output`].
pub fn hold_worker_output() {
    held().get_or_insert(HeldOutput {
        chunks: std::collections::VecDeque::new(),
        bytes: 0,
        dropped: 0,
    });
}

/// Keep `bytes` for later when output is held; `false` means write it now.
pub(crate) fn hold_or_pass(stderr: bool, bytes: &[u8]) -> bool {
    let mut held = held();
    let Some(held) = held.as_mut() else {
        return false;
    };
    held.chunks.push_back((stderr, bytes.to_vec()));
    held.bytes += bytes.len();
    while held.bytes > HELD_BYTES
        && let Some((_, old)) = held.chunks.pop_front()
    {
        held.bytes -= old.len();
        held.dropped += old.len();
    }
    true
}

/// Stop holding worker output and hand what was held to `write` (`stderr`, bytes) in order,
/// preceded by a note on stderr when the oldest was dropped. Output that arrives meanwhile is
/// written after it.
pub fn release_worker_output(write: impl Fn(bool, &[u8])) {
    loop {
        let (chunks, dropped) = {
            let mut guard = held();
            let Some(held) = guard.as_mut() else {
                return;
            };
            if held.chunks.is_empty() {
                *guard = None;
                return;
            }
            held.bytes = 0;
            (
                std::mem::take(&mut held.chunks),
                std::mem::take(&mut held.dropped),
            )
        };
        if dropped > 0 {
            write(
                true,
                format!("[{dropped} bytes of plugin output were dropped while the run monitor was open]\n")
                    .as_bytes(),
            );
        }
        for (stderr, bytes) in chunks {
            write(stderr, &bytes);
        }
    }
}

/// The size of the terminal on stdout as `(columns, rows)` (80 × 24 when unknown).
pub fn stdout_size() -> (usize, usize) {
    RegionSink::Stdout.size()
}

/// Whether a region is active (for tests).
pub fn is_active() -> bool {
    ACTIVE.load(Ordering::SeqCst)
}

/// A signal is about to end the process (or cancel the action): erase the region, show the
/// cursor and give the terminal its input modes back. Safe from any thread.
pub(crate) fn clear_for_signal() {
    if ACTIVE.load(Ordering::SeqCst)
        && let Ok(mut p) = PAINTER.try_lock()
        && let Some(painter) = p.as_mut().filter(|p| owned(p))
    {
        painter.lines.clear();
        painter.close();
    }
    keys::restore_saved();
}

/// Ctrl-Z while the region is shown: erase it, show the cursor and give the terminal its modes
/// back before the process stops (the shell gets a clean terminal), then let the next redraw and
/// [`Keys::pressed`] take it again once the process continues.
#[cfg(unix)]
mod stop_signal {
    use std::sync::Once;

    static INSTALLED: Once = Once::new();

    /// Watch SIGTSTP (once per process). The handler then stops the process as the default
    /// action would, so Ctrl-Z behaves as before everywhere else.
    pub(super) fn install() {
        INSTALLED.call_once(|| {
            use signal_hook::consts::SIGTSTP;
            let Ok(mut signals) = signal_hook::iterator::Signals::new([SIGTSTP]) else {
                return;
            };
            let _ = std::thread::Builder::new()
                .name("mrsm-live-tstp".into())
                .spawn(move || {
                    for signal in signals.forever() {
                        if super::ACTIVE.load(std::sync::atomic::Ordering::SeqCst)
                            && let Some(painter) = super::painter().as_mut()
                        {
                            // Keep the lines: the next redraw after `fg` shows them again.
                            painter.close();
                        }
                        super::keys::restore_saved();
                        let _ = signal_hook::low_level::emulate_default_handler(signal);
                    }
                });
        });
    }
}

// ---------------------------------------------------------------------------------------------
// Keys.

pub use keys::Keys;

mod keys {
    use std::sync::Mutex;

    #[cfg(unix)]
    static SAVED: Mutex<Option<rustix::termios::Termios>> = Mutex::new(None);
    #[cfg(not(unix))]
    static SAVED: Mutex<Option<()>> = Mutex::new(None);

    /// Single key presses from the terminal while a run is shown inline: canonical input and
    /// echo are off (not raw mode: Ctrl-C still raises SIGINT), so typing never writes over the
    /// region. Dropping it restores the modes.
    pub struct Keys {
        /// The modes are wanted (started on a terminal and not paused).
        want: bool,
        read: bool,
    }

    impl Keys {
        /// Turn echo off when stdin is a terminal; `read`: also read the keys (else they stay
        /// queued for whatever reads the terminal next).
        pub fn start(read: bool) -> Keys {
            Keys {
                want: enable(),
                read,
            }
        }

        /// A reader that never touches the terminal.
        pub fn inert() -> Keys {
            Keys {
                want: false,
                read: false,
            }
        }

        /// Whether keys are read now.
        pub fn is_on(&self) -> bool {
            self.want && is_saved()
        }

        /// Give the terminal back (before a full-screen view), until [`Keys::resume`].
        pub fn pause(&mut self) {
            if self.want {
                restore_saved();
                self.want = false;
            }
        }

        /// Read keys again after [`Keys::pause`].
        pub fn resume(&mut self) {
            if !self.want {
                self.want = enable();
            }
        }

        /// The keys pressed since the last call (never blocks). The modes are taken again when
        /// something gave them back meanwhile (a worker's prompt was answered, or the process
        /// continued after Ctrl-Z); nothing is read while a worker waits for input.
        pub fn pressed(&mut self) -> Vec<char> {
            if !self.want || super::worker_prompting() {
                return Vec::new();
            }
            if !is_saved() && !enable() {
                return Vec::new();
            }
            if !self.read {
                return Vec::new();
            }
            read_pending()
        }
    }

    impl Drop for Keys {
        fn drop(&mut self) {
            self.pause();
        }
    }

    #[cfg(unix)]
    fn enable() -> bool {
        use rustix::termios::{
            LocalModes, OptionalActions, SpecialCodeIndex, tcgetattr, tcsetattr,
        };
        let stdin = std::io::stdin();
        if !std::io::IsTerminal::is_terminal(&stdin) {
            return false;
        }
        // Under the lock: a worker's prompt (which sets the flag, then restores under the lock)
        // either sees these modes and restores them, or is seen here.
        let mut saved = SAVED.lock().unwrap_or_else(|e| e.into_inner());
        if super::worker_prompting() {
            return false;
        }
        let Ok(original) = tcgetattr(&stdin) else {
            return false;
        };
        let mut modes = original.clone();
        modes
            .local_modes
            .remove(LocalModes::ICANON | LocalModes::ECHO);
        // Blocking reads (VMIN 1): a Python worker that reads its inherited stdin before its
        // prompt has come through here waits for the answer instead of getting end-of-file.
        // `read_pending` polls before it reads, so this thread never blocks.
        modes.special_codes[SpecialCodeIndex::VMIN] = 1;
        modes.special_codes[SpecialCodeIndex::VTIME] = 0;
        if tcsetattr(&stdin, OptionalActions::Now, &modes).is_err() {
            return false;
        }
        saved.get_or_insert(original);
        true
    }

    fn is_saved() -> bool {
        SAVED.lock().unwrap_or_else(|e| e.into_inner()).is_some()
    }

    #[cfg(unix)]
    pub(super) fn restore_saved() {
        use rustix::termios::{OptionalActions, tcsetattr};
        let mut saved = SAVED.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(original) = saved.take() {
            let _ = tcsetattr(std::io::stdin(), OptionalActions::Now, &original);
        }
    }

    #[cfg(unix)]
    fn read_pending() -> Vec<char> {
        use rustix::event::{PollFd, PollFlags, Timespec, poll};
        let stdin = std::io::stdin();
        let mut keys = Vec::new();
        loop {
            let mut fds = [PollFd::new(&stdin, PollFlags::IN)];
            let zero = Timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            match poll(&mut fds, Some(&zero)) {
                Ok(n) if n > 0 => {}
                _ => break,
            }
            let mut buf = [0u8; 64];
            match rustix::io::read(&stdin, &mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => keys.extend(String::from_utf8_lossy(&buf[..n]).chars()),
            }
        }
        keys
    }

    #[cfg(not(unix))]
    fn enable() -> bool {
        let on = std::io::IsTerminal::is_terminal(&std::io::stdin()) && !super::worker_prompting();
        if on {
            *SAVED.lock().unwrap_or_else(|e| e.into_inner()) = Some(());
        }
        on
    }

    #[cfg(not(unix))]
    pub(super) fn restore_saved() {
        SAVED.lock().unwrap_or_else(|e| e.into_inner()).take();
    }

    #[cfg(not(unix))]
    fn read_pending() -> Vec<char> {
        use crossterm::event::{Event, KeyCode, KeyEventKind, poll, read};
        let mut keys = Vec::new();
        while let Ok(true) = poll(std::time::Duration::ZERO) {
            if let Ok(Event::Key(key)) = read()
                && key.kind == KeyEventKind::Press
                && let KeyCode::Char(c) = key.code
            {
                keys.push(c);
            }
        }
        keys
    }
}

#[cfg(test)]
mod tests;
