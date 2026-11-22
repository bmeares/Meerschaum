//! How the run monitor words engine events: one line and tone per event kind, and pipe labels.

use meerschaum_engine::events::EventRow;
use meerschaum_engine::{Bound, ChunkSource, Event, EventKind, PipeRef};

use crate::tui::theme::Tone;

/// `connector:metric[:location]`.
pub fn short_label(pipe_ref: &PipeRef) -> String {
    let parts = pipe_ref.parts();
    match parts.location_key {
        Some(location) => format!("{}:{}:{location}", parts.connector_keys, parts.metric_key),
        None => format!("{}:{}", parts.connector_keys, parts.metric_key),
    }
}

pub(super) fn bound_str(bound: &Option<Bound>) -> String {
    match bound {
        None => "..".into(),
        Some(Bound::Datetime(dt)) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        Some(Bound::Int(i)) => i.to_string(),
    }
}

pub(super) fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or("").trim()
}

fn counts(inserted: u64, updated: u64, upserted: u64) -> String {
    let mut parts = Vec::new();
    if inserted > 0 {
        parts.push(format!("+{inserted} ins"));
    }
    if updated > 0 {
        parts.push(format!("~{updated} upd"));
    }
    if upserted > 0 {
        parts.push(format!("{upserted} ups"));
    }
    if parts.is_empty() {
        "no changes".into()
    } else {
        parts.join(", ")
    }
}

/// The log text and tone of an event (also the line `watch run` prints off a terminal).
pub fn describe(kind: &EventKind) -> (String, Tone) {
    match kind {
        EventKind::RunStarted {
            nodes,
            workers,
            targets,
        } => (
            format!(
                "run started: {nodes} node{}, {workers} worker{}, {} target{}",
                if *nodes == 1 { "" } else { "s" },
                if *workers == 1 { "" } else { "s" },
                targets.len(),
                if targets.len() == 1 { "" } else { "s" },
            ),
            Tone::Accent,
        ),
        EventKind::NodePlanned {
            action,
            reason,
            provisional,
        } => (
            format!(
                "planned {action}{}: {reason}",
                if *provisional { " (provisional)" } else { "" }
            ),
            Tone::Dim,
        ),
        EventKind::NodeStarted { action, attempt } => (
            if *attempt > 1 {
                format!("started {action} (attempt {attempt})")
            } else {
                format!("started {action}")
            },
            Tone::Active,
        ),
        EventKind::ChunkStarted {
            chunk,
            begin,
            end,
            source,
            rows,
            total,
        } => {
            let label = match (source, total) {
                (ChunkSource::Window, Some(total)) => format!("chunk {}/{total}", chunk + 1),
                (ChunkSource::Window, None) => format!("chunk {}", chunk + 1),
                (ChunkSource::Fetch, _) => format!("fetch chunk {}", chunk + 1),
            };
            let rows = rows.map(|r| format!(", {r} rows")).unwrap_or_default();
            (
                format!(
                    "{label} started [{} -> {}]{rows}",
                    bound_str(begin),
                    bound_str(end)
                ),
                Tone::Dim,
            )
        }
        EventKind::ChunkFinished {
            chunk,
            success,
            message,
            source,
            inserted,
            updated,
            upserted,
            duration_ms,
            ..
        } => {
            let label = match source {
                ChunkSource::Window => format!("chunk {}", chunk + 1),
                ChunkSource::Fetch => format!("fetch chunk {}", chunk + 1),
            };
            let took = duration_ms
                .map(|ms| format!(" in {:.1}s", ms as f64 / 1000.0))
                .unwrap_or_default();
            if *success {
                (
                    format!(
                        "{label} done: {}{took}",
                        counts(*inserted, *updated, *upserted)
                    ),
                    Tone::Normal,
                )
            } else {
                (
                    format!("{label} failed: {}", first_line(message)),
                    Tone::Bad,
                )
            }
        }
        EventKind::NodeFinished { result } => {
            if result.success {
                (
                    format!(
                        "succeeded: {}",
                        counts(result.inserted, result.updated, result.upserted)
                    ),
                    Tone::Good,
                )
            } else {
                (
                    format!("failed: {}", first_line(&result.message)),
                    Tone::Bad,
                )
            }
        }
        EventKind::NodeFailed {
            error,
            attempt,
            will_retry,
            ..
        } => (
            format!(
                "attempt {attempt} failed{}: {}",
                if *will_retry { ", retrying" } else { "" },
                first_line(error)
            ),
            if *will_retry { Tone::Warn } else { Tone::Bad },
        ),
        EventKind::NodeSkipped { reason } => (format!("skipped: {reason}"), Tone::Warn),
        EventKind::NodeRebuildPending { reason } => (
            format!("rebuild pending (re-run with --yes): {reason}"),
            Tone::Warn,
        ),
        EventKind::WatermarkAdvanced { from, to } => (
            format!("watermark {} -> {}", bound_str(from), bound_str(to)),
            Tone::Dim,
        ),
        EventKind::StreamFlushed {
            rows,
            trigger,
            success,
            inserted,
            updated,
            upserted,
            ..
        } => {
            let trigger = serde_json::to_value(trigger)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default();
            (
                format!(
                    "flushed {rows} rows ({trigger}): {}",
                    counts(*inserted, *updated, *upserted)
                ),
                if *success { Tone::Normal } else { Tone::Bad },
            )
        }
        EventKind::ChildrenTriggered {
            run_id,
            nodes,
            held,
            ..
        } => (
            format!("triggered run {run_id} over {nodes} nodes ({held} held)"),
            Tone::Accent,
        ),
        EventKind::StreamRestarted { error } => (
            format!("stream restarted: {}", first_line(error)),
            Tone::Warn,
        ),
        EventKind::RunFinished { success, message } => (
            format!(
                "run {}: {}",
                if *success { "succeeded" } else { "failed" },
                first_line(message)
            ),
            if *success { Tone::Good } else { Tone::Bad },
        ),
    }
}

/// One event as a plain line: `HH:MM:SS  node  text`.
pub fn event_line(event: &Event) -> String {
    let (text, _) = describe(&event.kind);
    match &event.pipe_ref {
        Some(r) => format!(
            "{}  {}  {text}",
            event.ts.format("%H:%M:%S"),
            short_label(r)
        ),
        None => format!("{}  {text}", event.ts.format("%H:%M:%S")),
    }
}

/// A persisted event row as an [`Event`] (`None` for a payload this version cannot read).
pub fn event_of_row(row: &EventRow) -> Option<Event> {
    let mut payload: serde_json::Value = serde_json::from_str(&row.payload).ok()?;
    let fields = payload.as_object_mut()?;
    // The scoped API returns the event type in its own column and may redact the summary.
    fields.insert("event".into(), row.event.clone().into());
    if row.event == "run_finished" {
        fields.entry("message").or_insert_with(|| "".into());
    }
    let kind: EventKind = serde_json::from_value(payload).ok()?;
    Some(Event {
        run_id: row.run_id.clone(),
        seq: row.seq,
        ts: row.ts,
        pipe_ref: row.pipe_ref.clone(),
        kind,
    })
}
