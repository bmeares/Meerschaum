//! Engine runs, nodes, events and state as a caller may read them (AC7): a run is visible when
//! at least one of its pipes is (a run without pipes is visible); the nodes of pipes the caller
//! cannot see are anonymized (`pipe_ref` `"hidden"`, details dropped), and a run's `targets`
//! and `selection` lose them. The engine's raw state rows (`/engine/pipes/*`) are filtered the
//! same way, and writing state for a pipe needs edit rights on it.

use std::collections::{HashMap, HashSet};

use meerschaum_connector_api::models::{EngineRunEvent, RunRecord};
use meerschaum_core::Value;
use serde_json::Value as Json;

use super::RefFilter;

/// The `pipe_ref` a hidden node or event is reported with.
pub const HIDDEN_REF: &str = "hidden";

/// Raw run headers used only for authorization; no private storage fields are exposed.
pub(crate) fn headers_from_rows(rows: &[Json]) -> Vec<RunRecord> {
    rows.iter()
        .filter_map(|row| {
            Some(RunRecord {
                run_id: row.get("run_id")?.as_str()?.to_string(),
                pipe_ref: None,
                started: chrono::Utc::now(),
                finished: None,
                status: String::new(),
                extra: serde_json::from_value(row.clone()).unwrap_or_default(),
            })
        })
        .collect()
}

pub(crate) fn targets_of(record: &RunRecord) -> Vec<String> {
    match record.extra.get("targets") {
        Some(Value::List(items)) => items.iter().map(Value::py_str).collect(),
        Some(Value::Str(text)) => match Value::from_json_str(text) {
            Ok(Value::List(items)) => items.iter().map(Value::py_str).collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

fn selection(value: Option<&Value>) -> Json {
    match value {
        Some(Value::Str(text)) => serde_json::from_str(text).unwrap_or(Json::Null),
        Some(value) => serde_json::to_value(value).unwrap_or(Json::Null),
        None => Json::Null,
    }
}

fn row_selection(row: &Json) -> Json {
    match row.get("selection") {
        Some(Json::String(text)) => serde_json::from_str(text).unwrap_or(Json::Null),
        Some(value) => value.clone(),
        None => Json::Null,
    }
}

pub(crate) async fn record_pipelines_visible(filter: &mut RefFilter, record: &RunRecord) -> bool {
    filter
        .pipeline_snapshots_visible(&selection(record.extra.get("selection")))
        .await
}

fn strip_json_selection(value: &mut Json) {
    if let Json::String(text) = value {
        if let Ok(mut selection) = serde_json::from_str::<Json>(text)
            && meerschaum_connector::sync_metrics::strip_snapshot_proofs(&mut selection)
        {
            *text = selection.to_string();
        }
    } else {
        meerschaum_connector::sync_metrics::strip_snapshot_proofs(value);
    }
}

fn strip_record_proofs(record: &mut RunRecord) {
    record.extra.shift_remove("registration_ref");
    record.extra.shift_remove("registration_id");
    if let Some(value) = record.extra.get_mut("selection") {
        let mut selection = selection(Some(value));
        if meerschaum_connector::sync_metrics::strip_snapshot_proofs(&mut selection) {
            *value = if matches!(value, Value::Str(_)) {
                Value::Str(selection.to_string())
            } else {
                serde_json::from_value(selection).unwrap_or(Value::Null)
            };
        }
    }
}

/// The run and node records `filter`'s caller may read (see the module documentation).
pub async fn visible_runs(filter: &mut RefFilter, mut records: Vec<RunRecord>) -> Vec<RunRecord> {
    if filter.access().is_admin() {
        for record in &mut records {
            strip_record_proofs(record);
        }
        return records;
    }
    filter.prime_run_generations(&records).await;
    let headers: HashSet<_> = records
        .iter()
        .filter(|record| record.pipe_ref.is_none())
        .map(|record| record.run_id.clone())
        .collect();
    let mut hidden_pipelines = HashSet::new();
    for record in records.iter().filter(|record| record.pipe_ref.is_none()) {
        if !record_pipelines_visible(filter, record).await {
            hidden_pipelines.insert(record.run_id.clone());
        }
    }
    // Which pipes of which runs the caller can see.
    let mut seen: HashMap<String, bool> = HashMap::new();
    let mut refs: HashSet<String> = HashSet::new();
    for record in &records {
        if let Some(r) = &record.pipe_ref {
            refs.insert(r.clone());
        } else {
            refs.extend(targets_of(record));
        }
    }
    for r in refs {
        let visible = filter.visible(&r).await;
        seen.insert(r, visible);
    }
    let visible = |r: &str| seen.get(r).copied().unwrap_or(false);
    let mut run_visible: HashMap<String, bool> = HashMap::new();
    let mut has_pipes: HashSet<String> = HashSet::new();
    for record in &records {
        let pipes: Vec<String> = match &record.pipe_ref {
            Some(r) => vec![r.clone()],
            None => targets_of(record),
        };
        if !pipes.is_empty() {
            has_pipes.insert(record.run_id.clone());
        }
        if pipes.iter().any(|r| visible(r)) {
            run_visible.insert(record.run_id.clone(), true);
        }
    }
    records
        .into_iter()
        .filter(|r| {
            headers.contains(&r.run_id)
                && filter.run_generation_current(&r.run_id)
                && !hidden_pipelines.contains(&r.run_id)
                && (run_visible.get(&r.run_id).copied().unwrap_or(false)
                    || !has_pipes.contains(&r.run_id))
        })
        .map(|mut record| {
            match &record.pipe_ref {
                Some(r) if !visible(r) => {
                    record.pipe_ref = Some(HIDDEN_REF.into());
                    record.extra.retain(|k, _| k == "action");
                    record.extra.insert("hidden".into(), Value::Bool(true));
                }
                Some(_) => {}
                None => {
                    let targets = targets_of(&record);
                    if targets.iter().any(|t| !visible(t)) {
                        let kept: Vec<Value> = targets
                            .iter()
                            .filter(|t| visible(t))
                            .map(|t| Value::Str(t.clone()))
                            .collect();
                        let hidden = targets.len() - kept.len();
                        record.extra.insert("targets".into(), Value::List(kept));
                        record
                            .extra
                            .insert("hidden_targets".into(), Value::Int(hidden as i64));
                        // The selection names every target.
                        record.extra.shift_remove("selection");
                        record.extra.shift_remove("message");
                    }
                }
            }
            strip_record_proofs(&mut record);
            record
        })
        .collect()
}

/// Remove storage-only evidence from a public event after its run has been authorized.
pub(crate) fn strip_event_proofs(event: &mut EngineRunEvent) {
    if let Some(selection) = event.fields.get_mut("selection") {
        strip_json_selection(selection);
    }
}

/// `event` as the caller may read it (a hidden pipe's event keeps only its type and order).
pub async fn visible_event(filter: &mut RefFilter, mut event: EngineRunEvent) -> EngineRunEvent {
    strip_event_proofs(&mut event);
    if filter.access().is_admin() {
        return event;
    }
    if let Some(r) = event.pipe_ref.clone()
        && !filter.visible(&r).await
    {
        event.pipe_ref = Some(HIDDEN_REF.into());
        event.fields.clear();
        event
            .fields
            .insert("hidden".into(), serde_json::Value::Bool(true));
    }
    if event.pipe_ref.is_none() {
        if let Some(Json::Array(targets)) = event.fields.get("targets").cloned() {
            let mut kept = Vec::new();
            for target in &targets {
                if let Some(reference) = target.as_str()
                    && filter.visible(reference).await
                {
                    kept.push(target.clone());
                }
            }
            let hidden = targets.len() - kept.len();
            event.fields.insert("targets".into(), Json::Array(kept));
            if hidden > 0 {
                event
                    .fields
                    .insert("hidden_targets".into(), Json::from(hidden));
            }
        }
        if filter.has_hidden() {
            event.fields.shift_remove("message");
            event.fields.shift_remove("error");
            event.fields.shift_remove("selection");
        }
    }
    event
}

/// The columns of an engine state pipe's rows that name pipes.
const REF_COLUMNS: &[&str] = &["pipe_ref", "child_ref", "parent_ref"];

/// The raw rows of the engine state pipe `metric` the caller may read: rows naming a hidden pipe
/// are dropped; a run row loses its hidden `targets` (and its `selection`), and is dropped when
/// every target is hidden.
pub async fn visible_rows(filter: &mut RefFilter, metric: &str, rows: Vec<Json>) -> Vec<Json> {
    if filter.access().is_admin() {
        return rows;
    }
    if metric == "engine_runs" {
        let headers = headers_from_rows(&rows);
        filter.prime_run_generations(&headers).await;
    }
    let mut out = Vec::with_capacity(rows.len());
    let mut visible_event_runs: HashMap<String, bool> = HashMap::new();
    'rows: for mut row in rows {
        for column in REF_COLUMNS {
            if let Some(r) = row.get(*column).and_then(Json::as_str).map(str::to_string)
                && !filter.visible(&r).await
            {
                continue 'rows;
            }
        }
        if metric == "engine_runs"
            && (!filter
                .pipeline_snapshots_visible(&row_selection(&row))
                .await
                || !row
                    .get("run_id")
                    .and_then(Json::as_str)
                    .is_some_and(|id| filter.run_generation_current(id)))
        {
            continue;
        }
        if matches!(metric, "engine_events" | "engine_nodes") {
            let Some(run_id) = row.get("run_id").and_then(Json::as_str).map(str::to_string) else {
                continue;
            };
            let visible = match visible_event_runs.get(&run_id) {
                Some(visible) => *visible,
                None => {
                    let visible = filter.run_visible(&run_id).await;
                    visible_event_runs.insert(run_id.clone(), visible);
                    visible
                }
            };
            if !visible {
                continue;
            }
        }
        if metric == "engine_events" {
            let Some(run_id) = row.get("run_id").and_then(Json::as_str) else {
                continue;
            };
            let payload = match row.get("payload") {
                Some(Json::String(text)) => serde_json::from_str(text).unwrap_or(Json::Null),
                Some(payload) => payload.clone(),
                None => Json::Null,
            };
            let event = EngineRunEvent::from_row(
                run_id,
                0,
                chrono::Utc::now(),
                row.get("pipe_ref")
                    .and_then(Json::as_str)
                    .map(str::to_string),
                row.get("event").and_then(Json::as_str).unwrap_or("event"),
                payload,
            );
            let event = visible_event(filter, event).await;
            if let Some(fields) = row.as_object_mut() {
                fields.insert(
                    "payload".into(),
                    Json::Object(event.fields.into_iter().collect()),
                );
            }
        }
        if metric == "engine_runs" {
            let targets: Vec<String> = match row.get("targets") {
                Some(Json::Array(items)) => items
                    .iter()
                    .filter_map(Json::as_str)
                    .map(str::to_string)
                    .collect(),
                Some(Json::String(text)) => {
                    serde_json::from_str::<Vec<String>>(text).unwrap_or_default()
                }
                _ => Vec::new(),
            };
            let mut kept = Vec::new();
            for t in &targets {
                if filter.visible(t).await {
                    kept.push(Json::String(t.clone()));
                }
            }
            if !targets.is_empty() && kept.is_empty() {
                continue;
            }
            if kept.len() < targets.len()
                && let Some(o) = row.as_object_mut()
            {
                o.insert("targets".into(), Json::Array(kept));
                o.insert("selection".into(), Json::Null);
                o.insert("message".into(), Json::Null);
            }
        }
        if let Some(fields) = row.as_object_mut() {
            fields.remove("registration_ref");
            fields.remove("registration_id");
            if let Some(selection) = fields.get_mut("selection") {
                strip_json_selection(selection);
            }
        }
        out.push(row);
    }
    out
}

/// Refuse (with the first offending ref) state rows naming pipes the caller may not change.
pub async fn check_rows_editable(
    filter: &mut RefFilter,
    rows: &[meerschaum_connector::pipe_ops::Doc],
) -> Result<(), String> {
    if filter.access().is_admin() {
        return Ok(());
    }
    for row in rows {
        for column in ["pipe_ref", "child_ref"] {
            if let Some(r) = row.get(column).map(|v| match v {
                meerschaum_kernel::value::Value::Str(s) => s.clone(),
                other => format!("{other:?}"),
            }) && !r.is_empty()
                && !filter.editable(&r).await.0
            {
                return Err(format!(
                    "Refusing engine state for '{r}': you may not change that pipe."
                ));
            }
        }
        if let Some(targets) = row.get("targets") {
            let targets: Vec<String> = match targets {
                meerschaum_kernel::value::Value::Json(Json::Array(items)) => items
                    .iter()
                    .filter_map(Json::as_str)
                    .map(str::to_string)
                    .collect(),
                meerschaum_kernel::value::Value::Str(text) => {
                    serde_json::from_str(text).unwrap_or_default()
                }
                _ => Vec::new(),
            };
            check_refs_editable(filter, targets.iter().map(String::as_str)).await?;
        }
    }
    Ok(())
}

/// Refuse a list of refs the caller may not change (ledger entries, cursors, states, leases).
pub async fn check_refs_editable<'r, I>(filter: &mut RefFilter, refs: I) -> Result<(), String>
where
    I: IntoIterator<Item = &'r str>,
{
    if filter.access().is_admin() {
        return Ok(());
    }
    for r in refs {
        if !filter.editable(r).await.0 {
            return Err(format!(
                "Refusing engine state for '{r}': you may not change that pipe."
            ));
        }
    }
    Ok(())
}
