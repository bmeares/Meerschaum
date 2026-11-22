//! Storage of sync statuses: the `mrsm:engine_sync_status` pipe on the instance, its rows as
//! [`SyncStatus`] values, and the read-merge-upsert of one pipe's status.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::enforce::{PipeDtypesContext, PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};

use crate::pipe_ops::{Doc, frame_to_docs};
use crate::traits::{ConnectorError, ConnectorResult, DataQuery, InstanceConnector, SyncOptions};

use super::{
    METRIC, SyncOutcome, SyncStatus, parse_iso, registration_from_attributes, rekey,
    settle_instance, settle_pipe,
};

/// The status pipe on `conn`, registered as `instance_keys`
/// ([`InstanceConnector::engine_state_pipe`]).
pub async fn status_pipe<C: InstanceConnector + ?Sized>(
    conn: &C,
    instance_keys: &str,
) -> ConnectorResult<Pipe> {
    conn.engine_state_pipe(instance_keys, METRIC)
        .await?
        .or_else(|| crate::pipe_ops::engine_state_pipe(instance_keys, METRIC))
        .ok_or_else(|| ConnectorError::Invalid(format!("'{METRIC}' is not an engine state pipe.")))
}

fn kv_str(s: &str) -> KValue {
    KValue::Str(s.to_string())
}

fn kv_opt_str(s: Option<&str>) -> KValue {
    s.map(kv_str).unwrap_or(KValue::Null)
}

fn kv_opt_dt(dt: Option<DateTime<Utc>>) -> KValue {
    dt.map(|dt| KValue::Datetime(DateTimeValue::utc(dt.naive_utc())))
        .unwrap_or(KValue::Null)
}

fn kv_int(n: u64) -> KValue {
    KValue::Int(i64::try_from(n).unwrap_or(i64::MAX))
}

/// A status as a row of the status pipe.
pub fn status_doc(s: &SyncStatus) -> Doc {
    [
        ("registration_id", kv_opt_str(s.registration_id.as_deref())),
        ("pipe_ref", kv_str(&s.pipe_ref)),
        ("connector_keys", kv_str(&s.connector_keys)),
        ("metric_key", kv_str(&s.metric_key)),
        ("location_key", kv_opt_str(s.location_key.as_deref())),
        ("instance_keys", kv_str(&s.instance_keys)),
        ("last_attempt", kv_opt_dt(s.last_attempt)),
        ("last_success", kv_opt_dt(s.last_success)),
        ("last_failure", kv_opt_dt(s.last_failure)),
        ("success", KValue::Bool(s.success)),
        ("message", kv_str(&s.message)),
        ("rows_inserted", kv_int(s.rows_inserted)),
        ("rows_updated", kv_int(s.rows_updated)),
        ("rows_upserted", kv_int(s.rows_upserted)),
        (
            "duration_seconds",
            s.duration_seconds
                .map(KValue::Float)
                .unwrap_or(KValue::Null),
        ),
        ("consecutive_failures", kv_int(s.consecutive_failures)),
        ("source", kv_str(&s.source)),
        ("job", kv_opt_str(s.job.as_deref())),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

pub(crate) fn doc_str(doc: &Doc, key: &str) -> Option<String> {
    match doc.get(key)? {
        v if v.is_na() => None,
        KValue::Str(s) => Some(s.clone()),
        other => Some(other.py_str()),
    }
}

pub(crate) fn doc_u64(doc: &Doc, key: &str) -> u64 {
    let n = match doc.get(key) {
        Some(KValue::Int(i)) => *i,
        Some(KValue::Float(f)) if f.is_finite() => *f as i64,
        Some(KValue::Bool(b)) => *b as i64,
        Some(KValue::Str(s)) => s.trim().parse().unwrap_or(0),
        Some(KValue::Decimal(d)) => d.to_f64() as i64,
        _ => 0,
    };
    n.max(0) as u64
}

pub(crate) fn doc_f64(doc: &Doc, key: &str) -> Option<f64> {
    match doc.get(key)? {
        KValue::Float(f) if f.is_finite() => Some(*f),
        KValue::Int(i) => Some(*i as f64),
        KValue::Str(s) => s.trim().parse().ok(),
        KValue::Decimal(d) => Some(d.to_f64()),
        _ => None,
    }
}

pub(crate) fn doc_dt(doc: &Doc, key: &str) -> Option<DateTime<Utc>> {
    match doc.get(key)? {
        KValue::Datetime(d) => Some(DateTime::from_naive_utc_and_offset(d.utc, Utc)),
        KValue::Str(s) => parse_iso(s),
        _ => None,
    }
}

pub(crate) fn doc_bool(doc: &Doc, key: &str) -> bool {
    match doc.get(key) {
        Some(KValue::Bool(b)) => *b,
        Some(KValue::Int(i)) => *i != 0,
        Some(KValue::Float(value)) => value.is_finite() && *value != 0.0,
        Some(KValue::Str(s)) => matches!(s.trim(), "True" | "true" | "1"),
        _ => false,
    }
}

/// A row of the status pipe as a status (`None` without the pipe's keys).
pub fn status_from_doc(doc: &Doc) -> Option<SyncStatus> {
    let connector_keys = doc_str(doc, "connector_keys")?;
    let metric_key = doc_str(doc, "metric_key")?;
    let location_key = doc_str(doc, "location_key").filter(|l| l != "None");
    let instance_keys = doc_str(doc, "instance_keys").unwrap_or_default();
    let pipe_ref = doc_str(doc, "pipe_ref").unwrap_or_else(|| {
        format!(
            "{instance_keys}|{connector_keys}|{metric_key}|{}",
            location_key.as_deref().unwrap_or("None")
        )
    });
    Some(SyncStatus {
        registration_id: doc_str(doc, "registration_id"),
        pipe_ref,
        connector_keys,
        metric_key,
        location_key,
        instance_keys,
        last_attempt: doc_dt(doc, "last_attempt"),
        last_success: doc_dt(doc, "last_success"),
        last_failure: doc_dt(doc, "last_failure"),
        success: doc_bool(doc, "success"),
        message: doc_str(doc, "message").unwrap_or_default(),
        rows_inserted: doc_u64(doc, "rows_inserted"),
        rows_updated: doc_u64(doc, "rows_updated"),
        rows_upserted: doc_u64(doc, "rows_upserted"),
        duration_seconds: doc_f64(doc, "duration_seconds"),
        consecutive_failures: doc_u64(doc, "consecutive_failures"),
        source: doc_str(doc, "source").unwrap_or_default(),
        job: doc_str(doc, "job"),
    })
}

/// The declared dtypes of the status pipe as an enforcement context (no instance query).
pub(crate) fn static_context(pipe: &Pipe) -> PipeDtypesContext {
    let dtypes: IndexMap<String, String> = match pipe.parameters().get("dtypes") {
        Some(Value::Map(m)) => m
            .iter()
            .map(|(k, v)| (k.clone(), meerschaum_core::pyfmt::py_str(v)))
            .collect(),
        _ => IndexMap::new(),
    };
    PipeDtypesContext {
        explicit_dtypes: dtypes.clone(),
        dtypes,
        enforce: true,
        tz_aware: true,
        mixed_numerics: true,
    }
}

/// Of several stored rows for one pipe (written under other labels of the same instance), the
/// latest.
fn latest(statuses: impl IntoIterator<Item = SyncStatus>) -> Option<SyncStatus> {
    statuses.into_iter().max_by_key(|s| s.last_attempt)
}

/// The stored statuses on `conn` (any `params` filter), re-keyed under `instance_keys`: the
/// table belongs to the instance, whatever label wrote a row. `None` for a missing table.
async fn read_rows<C: InstanceConnector + ?Sized>(
    conn: &C,
    instance_keys: &str,
    params: Option<Map>,
) -> ConnectorResult<Vec<SyncStatus>> {
    let pipe = status_pipe(conn, instance_keys).await?;
    let query = DataQuery {
        params,
        ..Default::default()
    };
    let df = match conn.get_pipe_data(&pipe, &query).await? {
        Some(df) => df,
        None => return Ok(Vec::new()),
    };
    Ok(frame_to_docs(&df)
        .iter()
        .filter_map(status_from_doc)
        .map(|s| rekey(s, instance_keys))
        .collect())
}

/// The status of `pipe` on `conn`, after writing what this process holds for it
/// ([`settle_pipe`]; the default of [`InstanceConnector::get_sync_status`]).
pub async fn read_status<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
) -> ConnectorResult<Option<SyncStatus>> {
    settle_pipe(&pipe.pipe_ref()).await;
    stored_status(conn, pipe).await
}

/// The stored status of `pipe` on `conn`, without waiting for this process's writes (what a
/// write merges its outcomes into).
pub async fn stored_status<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
) -> ConnectorResult<Option<SyncStatus>> {
    let mut params = Map::new();
    params.insert(
        "connector_keys".into(),
        Value::Str(pipe.connector_keys().into()),
    );
    params.insert("metric_key".into(), Value::Str(pipe.metric_key().into()));
    let rows = read_rows(conn, pipe.instance_keys(), Some(params)).await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let attributes = conn.get_pipe_attributes(pipe).await?;
    let current = registration_from_attributes(&attributes);
    Ok(latest(rows.into_iter().filter(|s| {
        s.location_key.as_deref() == pipe.location_key()
            && if attributes.is_empty() {
                s.registration_id.is_none()
            } else {
                current.is_some() && s.registration_id == current
            }
    })))
}

/// Every stored status on `conn`, keyed under `instance_keys`, one per pipe, in key order, after
/// writing what this process holds for that instance ([`settle_instance`]; the default of
/// [`InstanceConnector::get_sync_statuses`]).
pub async fn read_statuses<C: InstanceConnector + ?Sized>(
    conn: &C,
    instance_keys: &str,
) -> ConnectorResult<Vec<SyncStatus>> {
    settle_instance(instance_keys).await;
    let rows = read_rows(conn, instance_keys, None).await?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let registrations = conn
        .registered_pipes(&crate::PipesKeysQuery::default())
        .await?;
    let mut current: HashMap<String, Option<String>> = HashMap::new();
    for (identity, entry) in registrations {
        let key = crate::sync_metrics::pipe_key(
            &entry.connector_keys,
            &entry.metric_key,
            entry.location_key.as_deref(),
        );
        let stamp = identity.and_then(|identity| match entry.extra {
            Some(Value::Map(parameters)) => Some(crate::sync_metrics::registration_stamp(
                &identity.to_string(),
                &parameters,
            )),
            Some(Value::Str(text)) => match Value::from_json_str(&text).ok()? {
                Value::Map(parameters) => Some(crate::sync_metrics::registration_stamp(
                    &identity.to_string(),
                    &parameters,
                )),
                _ => None,
            },
            _ => None,
        });
        current.insert(key, stamp);
    }
    let mut by_ref: IndexMap<String, SyncStatus> = IndexMap::new();
    for status in rows {
        let key = crate::sync_metrics::pipe_key(
            &status.connector_keys,
            &status.metric_key,
            status.location_key.as_deref(),
        );
        let visible = match current.get(&key) {
            Some(Some(stamp)) => status.registration_id.as_ref() == Some(stamp),
            Some(None) => false,
            None => status.registration_id.is_none(),
        };
        if !visible {
            continue;
        }
        match by_ref.get(&status.pipe_ref) {
            Some(existing) if existing.last_attempt >= status.last_attempt => {}
            _ => {
                by_ref.insert(status.pipe_ref.clone(), status);
            }
        }
    }
    let mut out: Vec<SyncStatus> = by_ref.into_values().collect();
    out.sort_by(|a, b| a.pipe_ref.cmp(&b.pipe_ref));
    Ok(out)
}

/// Upsert `status` into the status pipe on `conn`.
pub async fn write_status<C: InstanceConnector + ?Sized>(
    conn: &C,
    status: &SyncStatus,
) -> ConnectorResult<()> {
    let pipe = status_pipe(conn, &status.instance_keys).await?;
    let df = pipe_enforce_dtypes(
        PipeInput::Docs(vec![status_doc(status)]),
        &static_context(&pipe),
    )
    .map_err(|e| ConnectorError::Invalid(format!("cannot build the sync status row: {e}")))?;
    let mut extra = Map::new();
    extra.insert("_check_temporary_tables".into(), Value::Bool(false));
    extra.insert("enforce".into(), Value::Bool(false));
    let res = conn
        .sync_pipe(
            &pipe,
            df,
            &SyncOptions {
                check_existing: true,
                extra,
                ..Default::default()
            },
        )
        .await;
    if res.success {
        Ok(())
    } else {
        Err(ConnectorError::Invalid(format!(
            "cannot write the sync status of {}: {}",
            status.pipe_ref, res.message
        )))
    }
}

/// Merge `outcomes` (oldest first) into the stored status of `pipe` on `conn` and store it (the
/// default of [`InstanceConnector::record_sync_status`]): one read, one upsert.
pub async fn merge_and_store<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    outcomes: &[SyncOutcome],
) -> ConnectorResult<Option<SyncStatus>> {
    if outcomes.is_empty() {
        return stored_status(conn, pipe).await;
    }
    let previous = stored_status(conn, pipe).await?;
    let base = match previous {
        // Keep the stored history under this call's keys.
        Some(s) => rekey(s, pipe.instance_keys()),
        None => SyncStatus::for_pipe(pipe),
    };
    let mut status = base;
    for outcome in outcomes {
        let captured = if outcome.registration_captured {
            outcome.registration_id.clone()
        } else {
            crate::sync_metrics::observed_registration(pipe)
        };
        if status.registration_id != captured {
            status = SyncStatus::for_pipe(pipe);
        }
        status.registration_id = captured;
        status = status.apply(outcome);
    }
    let current = conn.get_pipe_attributes(pipe).await?;
    let matches = if current.is_empty() {
        status.registration_id.is_none()
    } else {
        registration_from_attributes(&current)
            .is_some_and(|identity| status.registration_id.as_ref() == Some(&identity))
    };
    if !matches {
        return Ok(None);
    }
    write_status(conn, &status).await?;
    let attributes = conn.get_pipe_attributes(pipe).await?;
    let proven = if attributes.is_empty() {
        status.registration_id.is_none()
    } else {
        registration_from_attributes(&attributes)
            .is_some_and(|current| status.registration_id.as_ref() == Some(&current))
    };
    Ok(proven.then_some(status))
}
