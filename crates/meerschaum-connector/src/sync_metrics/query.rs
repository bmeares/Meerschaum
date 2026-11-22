//! Reading history back: the bounds of a [`MetricsQuery`], the sync and engine rows it selects, and
//! the summaries, buckets and per-pipe and per-pipeline aggregates of a [`MetricsResponse`].

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, TimeDelta, Utc};
use meerschaum_core::{Map, Pipe, Value};

use crate::pipe_ops::Doc;
use crate::sync_status::{SyncOutcome, doc_bool, doc_dt, doc_str, doc_u64};
use crate::{ConnectorError, ConnectorResult, DataQuery, DateBound, InstanceConnector};

use super::registration::{RegistrationSnapshot, registration_snapshot};
use super::{
    MAX_NODES, MAX_ROWS, MAX_RUNS, MAX_SUMMARY_GROUPS, METRIC, MetricAggregate, MetricBucket,
    MetricSummary, MetricsQuery, MetricsResponse, PipeMetricSummary, PipelineMetricSummary,
    RETENTION_DAYS, SyncMetric, pipe_key, reference_keys, registration_matches, snapshot_registry,
};

pub(super) fn query_bounds(
    query: &MetricsQuery,
) -> ConnectorResult<(DateTime<Utc>, DateTime<Utc>, usize)> {
    let end = query.end.unwrap_or_else(Utc::now);
    let begin = query
        .begin
        .or_else(|| end.checked_sub_signed(TimeDelta::days(1)))
        .ok_or_else(|| ConnectorError::Invalid("Metrics bounds are out of range.".into()))?;
    if begin >= end
        || end - begin > TimeDelta::days(RETENTION_DAYS)
        || end > Utc::now() + TimeDelta::minutes(5)
    {
        return Err(ConnectorError::Invalid(format!(
            "Metrics need an ordered time window of at most {RETENTION_DAYS} days."
        )));
    }
    Ok((begin, end, query.limit.unwrap_or(1000).clamp(1, MAX_ROWS)))
}

fn reference_key(reference: &str) -> Option<String> {
    let (connector, metric, location) = reference_keys(reference)?;
    Some(pipe_key(connector, metric, location))
}

fn node_reference(doc: &Doc) -> Option<String> {
    doc_str(doc, "registration_ref").or_else(|| doc_str(doc, "pipe_ref"))
}

fn json_field(doc: &Doc, key: &str) -> serde_json::Value {
    match doc.get(key) {
        Some(meerschaum_kernel::value::Value::Json(value)) => value.to_json(),
        _ => doc_str(doc, key)
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default(),
    }
}

fn names_of(
    run: &Doc,
    query: &MetricsQuery,
    current: &HashMap<String, (String, String)>,
) -> Vec<String> {
    let selection = json_field(run, "selection");
    let snapshots = selection
        .get("pipeline_definitions")
        .and_then(serde_json::Value::as_array);
    let mut names = Vec::new();
    for definition in snapshots.into_iter().flatten() {
        if definition
            .get("name")
            .and_then(serde_json::Value::as_str)
            .is_none_or(str::is_empty)
        {
            continue;
        }
        let Some(identity) = definition
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|identity| !identity.is_empty() && !identity.starts_with("legacy:"))
        else {
            continue;
        };
        let Some(registry) = snapshot_registry(definition) else {
            continue;
        };
        // A rename retains the live identity on its exact registry, while the stored
        // snapshot keeps its historical name. An ambiguous duplicate grants no attribution.
        let mut matching = current
            .iter()
            .filter(|(_, (current_id, current_registry))| {
                current_id.as_str() == identity && current_registry.as_str() == registry
            });
        if let Some((name, _)) = matching.next()
            && matching.next().is_none()
        {
            names.push(name.clone());
        }
    }
    // Older run rows only identify a saved Pipeline through the generated run name.
    if names.is_empty()
        && snapshots.is_none_or(Vec::is_empty)
        && query.visible_pipelines.is_none()
        && let (Some(name), Some(run_id)) = (&query.pipeline, doc_str(run, "run_id"))
        && let Some(rest) = run_id.strip_prefix(&format!("{name}-"))
        && let Some((stamp, suffix)) = rest.rsplit_once('-')
        && suffix.len() == 8
        && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
        && chrono::NaiveDateTime::parse_from_str(stamp, "%Y-%m-%dT%H-%M-%S%.6fZ").is_ok()
    {
        names.push(name.clone());
    }
    names.sort();
    names.dedup();
    names
}

async fn engine_docs<C: InstanceConnector + ?Sized>(
    conn: &C,
    metric: &str,
    query: &DataQuery,
) -> ConnectorResult<(Vec<Doc>, bool)> {
    conn.read_engine_metric_rows(metric, query).await
}

async fn read_sync_history<C: InstanceConnector + ?Sized>(
    conn: &C,
    params: &Map,
    registrations: &[Value],
    begin: DateTime<Utc>,
    end: DateTime<Utc>,
    limit: usize,
) -> ConnectorResult<(Vec<SyncMetric>, bool)> {
    let mut history = Vec::new();
    let mut truncated = false;
    let batch = if conn.connector_type() == "valkey" {
        registrations.len()
    } else {
        900
    };
    for identities in registrations.chunks(batch) {
        let mut params = params.clone();
        params.insert("registration_id".into(), Value::List(identities.to_vec()));
        let (rows, capped) = conn
            .read_engine_metric_rows(
                METRIC,
                &DataQuery {
                    begin: Some(DateBound::Int(micros_ceil(begin))),
                    end: Some(DateBound::Int(micros_ceil(end))),
                    params: Some(params),
                    limit: Some((limit + 1) as i64),
                    order: Some("desc".into()),
                    ..Default::default()
                },
            )
            .await?;
        truncated |= capped;
        history.extend(rows.iter().filter_map(SyncMetric::from_doc));
        history.sort_by(|a, b| b.ts.cmp(&a.ts).then_with(|| a.event_id.cmp(&b.event_id)));
        truncated |= history.len() > limit;
        history.truncate(limit + 1);
    }
    Ok((history, truncated))
}

fn micros_ceil(time: DateTime<Utc>) -> i64 {
    time.timestamp_micros().saturating_add(i64::from(
        !time.timestamp_subsec_nanos().is_multiple_of(1000),
    ))
}

async fn read_summary_groups<C: InstanceConnector + ?Sized>(
    conn: &C,
    params: &Map,
    registrations: &[Value],
    begin: DateTime<Utc>,
    end: DateTime<Utc>,
    width: i64,
) -> ConnectorResult<Option<(Vec<MetricAggregate>, bool)>> {
    let mut groups = Vec::new();
    let mut truncated = false;
    for identities in registrations.chunks(900) {
        let remaining = MAX_SUMMARY_GROUPS.saturating_sub(groups.len());
        if remaining == 0 {
            truncated = true;
            break;
        }
        let mut params = params.clone();
        params.insert("registration_id".into(), Value::List(identities.to_vec()));
        let query = DataQuery {
            begin: Some(DateBound::Int(micros_ceil(begin))),
            end: Some(DateBound::Int(micros_ceil(end))),
            params: Some(params),
            ..Default::default()
        };
        let Some((batch, capped)) = conn
            .aggregate_sync_metrics(&query, begin.timestamp_micros(), width, remaining)
            .await?
        else {
            return Ok(None);
        };
        truncated |= capped || batch.len() > remaining;
        groups.extend(batch.into_iter().take(remaining));
    }
    Ok(Some((groups, truncated)))
}

/// Read bounded details and authorized whole-window aggregates independently.
/// Public transports must derive visibility ceilings themselves, never trust request ceilings.
pub async fn read<C: InstanceConnector + ?Sized>(
    conn: &C,
    query: &MetricsQuery,
) -> ConnectorResult<MetricsResponse> {
    let (begin, end, limit) = query_bounds(query)?;
    let mut response = MetricsResponse {
        begin,
        end,
        limit,
        truncated: false,
        summary_complete: true,
        history_truncated: false,
        pipelines_truncated: false,
        buckets: query.buckets.unwrap_or(48).clamp(1, 200),
        retention_days: RETENTION_DAYS,
        summary: MetricSummary::default(),
        series: Vec::new(),
        pipes: Vec::new(),
        pipelines: Vec::new(),
        history: Vec::new(),
    };
    let visible: Option<HashSet<String>> = query.visible_refs.as_ref().map(|refs| {
        refs.iter()
            .filter_map(|reference| reference_key(reference))
            .collect()
    });
    if visible.as_ref().is_some_and(HashSet::is_empty)
        || query.pipeline.as_ref().is_some_and(|name| {
            query
                .visible_pipelines
                .as_ref()
                .is_some_and(|names| !names.contains(name))
        })
    {
        return Ok(response);
    }
    let selected_key = query
        .pipe_ref
        .as_ref()
        .map(|reference| {
            reference_key(reference).ok_or_else(|| {
                ConnectorError::Invalid("Invalid Pipe reference for metrics.".into())
            })
        })
        .transpose()?;
    if selected_key
        .as_ref()
        .is_some_and(|key| visible.as_ref().is_some_and(|keys| !keys.contains(key)))
    {
        return Ok(response);
    }
    let RegistrationSnapshot {
        stamps: current,
        keys: current_keys,
        pipelines: current_pipelines,
    } = registration_snapshot(conn).await?;
    if current.is_empty() {
        return Ok(response);
    }
    let mut params = Map::new();
    if let Some(key) = &selected_key {
        params.insert("pipe_key".into(), Value::Str(key.clone()));
    }
    let allowed_registrations: Vec<Value> = current
        .iter()
        .filter(|(key, _)| {
            visible
                .as_ref()
                .is_none_or(|visible| visible.contains(*key))
        })
        .filter(|(key, _)| {
            selected_key
                .as_ref()
                .is_none_or(|selected| selected == *key)
        })
        .map(|(_, identity)| Value::Str(identity.clone()))
        .collect();
    if allowed_registrations.is_empty() {
        return Ok(response);
    }
    let span = (micros_ceil(end) - begin.timestamp_micros()).max(1);
    let buckets = response.buckets.min(span as usize);
    response.buckets = buckets;
    let width = (span + buckets as i64 - 1) / buckets as i64;
    let window_aggregates = if query.pipeline.is_none() {
        read_summary_groups(conn, &params, &allowed_registrations, begin, end, width).await?
    } else {
        None
    };
    let summary_only = query.summary_only && query.pipeline.is_none();
    let (mut history, capped) =
        if query.pipeline.is_none() && (!summary_only || window_aggregates.is_none()) {
            read_sync_history(conn, &params, &allowed_registrations, begin, end, limit).await?
        } else {
            (Vec::new(), false)
        };
    response.truncated |= capped;
    response.history_truncated |= capped;
    let mut seen_runs = HashSet::new();
    let mut linked_runs: Vec<Value> = history
        .iter()
        .filter_map(|metric| {
            let id = metric.run_id.as_ref()?;
            seen_runs.insert(id.clone()).then(|| Value::Str(id.clone()))
        })
        .collect();
    if current_pipelines.is_empty() {
        response.truncated |= linked_runs.len() > MAX_RUNS;
        linked_runs.truncate(MAX_RUNS);
    }
    // Pipeline identities and global run timings are visible only when the whole run is visible.
    let (runs, runs_capped) =
        if summary_only || (current_pipelines.is_empty() && linked_runs.is_empty()) {
            (Vec::new(), false)
        } else {
            engine_docs(
                conn,
                "engine_runs",
                &DataQuery {
                    begin: Some(DateBound::utc(
                        end.checked_sub_signed(TimeDelta::days(RETENTION_DAYS))
                            .unwrap_or(DateTime::<Utc>::MIN_UTC),
                    )),
                    end: None,
                    params: current_pipelines
                        .is_empty()
                        .then(|| [("run_id".into(), Value::List(linked_runs))].into()),
                    limit: Some(MAX_RUNS as i64),
                    order: Some("desc".into()),
                    ..Default::default()
                },
            )
            .await?
        };
    response.truncated |= runs_capped;
    response.pipelines_truncated |= runs_capped;
    let mut candidate_runs = Vec::new();
    for run in runs {
        if doc_dt(&run, "started").is_none_or(|started| started >= end) {
            continue;
        }
        let names = names_of(&run, query, &current_pipelines);
        let selection = json_field(&run, "selection");
        let snapshots = selection
            .get("pipeline_definitions")
            .and_then(serde_json::Value::as_array);
        if snapshots
            .is_some_and(|snapshots| !snapshots.is_empty() && snapshots.len() != names.len())
            || (query.visible_pipelines.is_some() && snapshots.is_none_or(Vec::is_empty))
        {
            continue;
        }
        if query
            .pipeline
            .as_ref()
            .is_some_and(|name| !names.contains(name))
        {
            continue;
        }
        // A private Pipeline must not label otherwise-visible Pipe metrics or global run totals.
        if query
            .visible_pipelines
            .as_ref()
            .is_some_and(|allowed| names.iter().any(|name| !allowed.contains(name)))
        {
            continue;
        }
        let targets = json_field(&run, "targets");
        let target_refs: Vec<&str> = targets
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .collect();
        if visible.as_ref().is_some_and(|keys| {
            target_refs
                .iter()
                .any(|reference| reference_key(reference).is_none_or(|key| !keys.contains(&key)))
        }) {
            continue;
        }
        candidate_runs.push((run, names));
    }
    let ids: Vec<String> = candidate_runs
        .iter()
        .filter_map(|(run, _)| doc_str(run, "run_id"))
        .collect();
    let mut nodes_by_run: HashMap<String, Vec<Doc>> = HashMap::new();
    if !ids.is_empty() {
        let mut node_params = Map::new();
        node_params.insert(
            "run_id".into(),
            Value::List(ids.iter().cloned().map(Value::Str).collect()),
        );
        let (nodes, nodes_capped) = engine_docs(
            conn,
            "engine_nodes",
            &DataQuery {
                params: Some(node_params),
                limit: Some(MAX_NODES as i64),
                ..Default::default()
            },
        )
        .await?;
        response.truncated |= nodes_capped;
        response.pipelines_truncated |= nodes_capped;
        for node in nodes {
            if let Some(run_id) = doc_str(&node, "run_id") {
                nodes_by_run.entry(run_id).or_default().push(node);
            }
        }
    }
    let mut visible_runs = HashSet::new();
    let mut pipelines: BTreeMap<String, PipelineMetricSummary> = BTreeMap::new();
    for (run, names) in candidate_runs {
        let Some(run_id) = doc_str(&run, "run_id") else {
            continue;
        };
        let nodes = nodes_by_run.remove(&run_id).unwrap_or_default();
        if nodes.len() < doc_u64(&run, "num_nodes") as usize {
            continue;
        }
        if visible.as_ref().is_some_and(|keys| {
            nodes.iter().any(|node| {
                let Some(reference) = node_reference(node) else {
                    return true;
                };
                // A real cross-instance member requires its exact ref in the caller's ceiling.
                let instance = reference.split('|').next().unwrap_or_default();
                if instance != conn.connector_keys() {
                    return query
                        .visible_refs
                        .as_ref()
                        .is_none_or(|refs| !refs.contains(&reference));
                }
                reference_key(&reference).is_none_or(|key| !keys.contains(&key))
            })
        }) {
            continue;
        }
        if nodes.iter().any(|node| {
            let Some(reference) = node_reference(node) else {
                return true;
            };
            // This snapshot contains registrations only on the selected instance.
            // A foreign numeric ID and ACL hash can coincide with a local registration;
            // an exact visible foreign ref does not prove its current generation.
            // A legacy node lacks the persisted registration segment, so it cannot prove
            // continuity after the Pipe was deleted or its privacy changed.
            !registration_matches(
                &reference,
                doc_str(node, "registration_id").as_deref(),
                &conn.connector_keys(),
                &current,
            )
        }) {
            continue;
        }
        if selected_key.as_ref().is_some_and(|selected| {
            !nodes.iter().any(|node| {
                node_reference(node)
                    .and_then(|reference| reference_key(&reference))
                    .as_ref()
                    == Some(selected)
            })
        }) {
            continue;
        }
        visible_runs.insert(run_id.clone());
        let started = doc_dt(&run, "started").unwrap_or(begin);
        let finished = doc_dt(&run, "finished");
        // Pipe outcomes inside the window retain their authorized Pipeline attribution
        // even when the whole run completed outside it. Whole-run timings count only
        // runs completed in the requested window.
        if finished.is_some_and(|finished| finished < begin || finished >= end) {
            continue;
        }
        let mut outcome = SyncOutcome::new(
            doc_bool(&run, "success"),
            "",
            crate::sync_status::SyncSource::Engine,
            None,
        );
        outcome.finished = finished.unwrap_or(started);
        outcome.duration_seconds =
            finished.map(|finished| (finished - started).num_milliseconds().max(0) as f64 / 1000.0);
        outcome.attempts = 0;
        outcome.chunks = 0;
        let mut retries: u64 = 0;
        for node in &nodes {
            outcome.inserted = outcome.inserted.saturating_add(doc_u64(node, "inserted"));
            outcome.updated = outcome.updated.saturating_add(doc_u64(node, "updated"));
            outcome.upserted = outcome.upserted.saturating_add(doc_u64(node, "upserted"));
            outcome.deleted = outcome.deleted.saturating_add(doc_u64(node, "deleted"));
            outcome.chunks = outcome.chunks.saturating_add(doc_u64(node, "num_chunks"));
            outcome.attempts = outcome
                .attempts
                .saturating_add(u32::try_from(doc_u64(node, "attempts")).unwrap_or(u32::MAX));
            retries = retries.saturating_add(doc_u64(node, "attempts").saturating_sub(1));
        }
        let synthetic = Pipe::builder("engine", "run")
            .instance(&conn.connector_keys())
            .build()
            .map_err(|error| ConnectorError::Invalid(error.to_string()))?;
        let metric = SyncMetric::from_outcome(&synthetic, &outcome);
        for name in names {
            if query
                .pipeline
                .as_ref()
                .is_some_and(|selected| selected != &name)
            {
                continue;
            }
            let aggregate =
                pipelines
                    .entry(name.clone())
                    .or_insert_with(|| PipelineMetricSummary {
                        name,
                        ..Default::default()
                    });
            if aggregate.last_started.is_none_or(|last| last < started) {
                aggregate.last_run_id = Some(run_id.clone());
                aggregate.last_started = Some(started);
                aggregate.last_finished = finished;
                aggregate.last_success = finished.map(|_| metric.success);
            }
            if finished.is_some() {
                let previous_retries = aggregate.summary.retries;
                aggregate.summary.add(&metric);
                aggregate.summary.retries = previous_retries.saturating_add(retries);
            } else {
                aggregate.running += 1;
            }
        }
    }
    if query.pipeline.is_some() {
        if visible_runs.is_empty() {
            response.summary_complete = !response.pipelines_truncated;
            return Ok(response);
        }
        params.insert(
            "run_id".into(),
            Value::List(visible_runs.iter().cloned().map(Value::Str).collect()),
        );
    }
    if query.pipeline.is_some() {
        let (rows, capped) =
            read_sync_history(conn, &params, &allowed_registrations, begin, end, limit).await?;
        history = rows;
        response.truncated |= capped;
        response.history_truncated |= capped;
    }
    // Recheck the ceiling before every aggregation, even if a custom backend ignores params.
    history.retain(|metric| {
        metric.ts >= begin
            && metric.ts < end
            && current.get(&metric.pipe_key) == Some(&metric.registration_id)
            && visible
                .as_ref()
                .is_none_or(|keys| keys.contains(&metric.pipe_key))
            && selected_key
                .as_ref()
                .is_none_or(|key| key == &metric.pipe_key)
            && query.pipeline.as_ref().is_none_or(|_| {
                metric
                    .run_id
                    .as_ref()
                    .is_some_and(|run| visible_runs.contains(run))
            })
    });
    history.sort_by(|a, b| b.ts.cmp(&a.ts).then_with(|| a.event_id.cmp(&b.event_id)));
    response.truncated |= history.len() > limit;
    response.history_truncated |= history.len() > limit;
    history.truncate(limit);
    let mut series: BTreeMap<i64, MetricSummary> = BTreeMap::new();
    let mut pipes: BTreeMap<String, PipeMetricSummary> = BTreeMap::new();
    let aggregates = if query.pipeline.is_some() {
        read_summary_groups(conn, &params, &allowed_registrations, begin, end, width).await?
    } else {
        window_aggregates
    };
    if let Some((groups, capped)) = &aggregates {
        response.summary_complete =
            !capped && (query.pipeline.is_none() || !response.pipelines_truncated);
        response.truncated |= capped;
        for group in groups {
            if group.bucket >= buckets
                || current.get(&group.pipe_key) != Some(&group.registration_id)
                || visible
                    .as_ref()
                    .is_some_and(|keys| !keys.contains(&group.pipe_key))
                || selected_key
                    .as_ref()
                    .is_some_and(|key| key != &group.pipe_key)
            {
                continue;
            }
            let Some((connector, metric, location)) = current_keys.get(&group.pipe_key) else {
                continue;
            };
            response.summary.merge(&group.summary);
            series
                .entry(group.bucket as i64)
                .or_default()
                .merge(&group.summary);
            pipes
                .entry(group.pipe_key.clone())
                .or_insert_with(|| PipeMetricSummary {
                    pipe_ref: format!(
                        "{}|{}|{}|{}",
                        conn.connector_keys(),
                        connector,
                        metric,
                        location.as_deref().unwrap_or("None")
                    ),
                    connector_keys: connector.clone(),
                    metric_key: metric.clone(),
                    location_key: location.clone(),
                    summary: MetricSummary::default(),
                })
                .summary
                .merge(&group.summary);
        }
    } else {
        response.summary_complete = !response.history_truncated
            && (query.pipeline.is_none() || !response.pipelines_truncated);
    }
    for metric in &mut history {
        metric.instance_keys = conn.connector_keys();
        metric.pipe_ref = format!(
            "{}|{}|{}|{}",
            metric.instance_keys,
            metric.connector_keys,
            metric.metric_key,
            metric.location_key.as_deref().unwrap_or("None")
        );
        if !metric
            .run_id
            .as_ref()
            .is_some_and(|run| visible_runs.contains(run))
        {
            metric.run_id = None;
        }
        if aggregates.is_some() {
            continue;
        }
        response.summary.add(metric);
        let bucket = (metric.ts.timestamp_micros() - begin.timestamp_micros()) / width;
        series.entry(bucket).or_default().add(metric);
        pipes
            .entry(metric.pipe_key.clone())
            .or_insert_with(|| PipeMetricSummary {
                pipe_ref: metric.pipe_ref.clone(),
                connector_keys: metric.connector_keys.clone(),
                metric_key: metric.metric_key.clone(),
                location_key: metric.location_key.clone(),
                summary: MetricSummary::default(),
            })
            .summary
            .add(metric);
    }
    response.series = (0..buckets)
        .map(|bucket| MetricBucket {
            ts: begin + TimeDelta::microseconds(bucket as i64 * width),
            summary: series.remove(&(bucket as i64)).unwrap_or_default(),
        })
        .collect();
    response.pipes = pipes.into_values().collect();
    response.pipelines = if query.summary_only {
        Vec::new()
    } else {
        pipelines.into_values().collect()
    };
    response.history = if query.summary_only {
        Vec::new()
    } else {
        history
    };
    Ok(response)
}
