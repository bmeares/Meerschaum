//! The instance-backed engine-state store behind `/engine/*` (§5.8, R2): the Syncing Engine's
//! state pipes kept on the server's own instance, so an API instance's ledger, cursors,
//! definition hashes, run history and leases survive a server restart.
//!
//! The state pipes are the instance's own (`InstanceConnector::engine_state_pipe`: on a SQL
//! instance in `_mrsm_internal`, namespaced by the instance schema). Clients reach them two
//! ways: the engine-pipe primitives (`/engine/pipes/{metric}/*`, what an `APIConnector`
//! routes the engine's `mrsm:engine_*` pipes to), and the per-shape routes (`/engine/ledger`,
//! `/engine/cursors`, `/engine/state`, `/engine/runs`, `/engine/events`), which map their
//! models onto the same rows. Leases use the instance's lease pair, or an in-process table
//! when the instance has none.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use meerschaum_connector::pipe_ops::{Doc, frame_to_docs};
use meerschaum_connector::{ConnectorError, DataQuery, DateBound, InstanceConnector, SyncOptions};
use meerschaum_connector_api::engine_pipes::{self, EnginePipeQuery};
use meerschaum_connector_api::models::{
    EdgeCursor, EngineEvent, EngineRunEvent, EngineState, LedgerEntry, RunRecord,
};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};
use serde_json::Value as Json;

use crate::backends::{EngineBackend, EngineStoreError, MemoryEngine};

/// Engine state on the server's instance (see the module docs).
pub struct InstanceEngine {
    conn: Arc<dyn InstanceConnector>,
    keys: String,
    /// Leases when the instance has no lease pair (`NotImplemented`).
    local_leases: MemoryEngine,
}

impl InstanceEngine {
    /// A store on `conn` (the server's instance, `keys`).
    pub fn new(conn: Arc<dyn InstanceConnector>, keys: &str) -> InstanceEngine {
        InstanceEngine {
            conn,
            keys: keys.to_string(),
            local_leases: MemoryEngine::default(),
        }
    }

    async fn pipe(&self, metric: &str) -> Result<Pipe, EngineStoreError> {
        self.conn
            .engine_state_pipe(&self.keys, metric)
            .await
            .map_err(|e| EngineStoreError::Failed(e.to_string()))?
            .ok_or_else(|| {
                EngineStoreError::Failed(format!("'{metric}' is not an engine state pipe."))
            })
    }

    async fn read(
        &self,
        metric: &str,
        query: DataQuery,
    ) -> Result<Option<Frame>, EngineStoreError> {
        let pipe = self.pipe(metric).await?;
        if !self.conn.pipe_exists(&pipe).await.unwrap_or(false) {
            return Ok(None);
        }
        self.conn
            .get_pipe_data(&pipe, &query)
            .await
            .map_err(|e| EngineStoreError::Failed(e.to_string()))
    }

    async fn read_docs(&self, metric: &str, params: Map) -> Result<Vec<Doc>, EngineStoreError> {
        Ok(self
            .read(
                metric,
                DataQuery {
                    params: Some(params),
                    ..Default::default()
                },
            )
            .await?
            .map(|df| frame_to_docs(&df))
            .unwrap_or_default())
    }

    async fn write(&self, metric: &str, docs: Vec<Doc>) -> SuccessTuple {
        if docs.is_empty() {
            return SuccessTuple::ok("Nothing to write.");
        }
        let pipe = match self.pipe(metric).await {
            Ok(p) => p,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let df = match engine_pipes::engine_frame(metric, docs) {
            Ok(df) => df,
            Err(e) => return SuccessTuple::fail(e),
        };
        let mut extra = Map::new();
        extra.insert("_check_temporary_tables".into(), Value::Bool(false));
        extra.insert("enforce".into(), Value::Bool(false));
        self.conn
            .sync_pipe(
                &pipe,
                df,
                &SyncOptions {
                    check_existing: pipe.upsert(),
                    extra,
                    ..Default::default()
                },
            )
            .await
    }

    async fn clear(
        &self,
        metric: &str,
        params: Option<Map>,
        end: Option<DateBound>,
    ) -> SuccessTuple {
        let pipe = match self.pipe(metric).await {
            Ok(p) => p,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        if !self.conn.pipe_exists(&pipe).await.unwrap_or(false) {
            return SuccessTuple::ok("Nothing to clear.");
        }
        self.conn
            .clear_pipe(
                &pipe,
                &DataQuery {
                    params,
                    end,
                    ..Default::default()
                },
            )
            .await
    }
}

/// A `pipe_ref` (`"{instance}|{ck}|{mk}|{lk}"`) as a pipe (for the instance's lease pair).
fn pipe_from_ref(pipe_ref: &str) -> Option<Pipe> {
    let parts: Vec<&str> = pipe_ref.splitn(4, '|').collect();
    let [instance, ck, mk, lk] = parts.as_slice() else {
        return None;
    };
    let mut builder = Pipe::builder(ck, mk).instance(instance);
    if *lk != "None" && !lk.is_empty() {
        builder = builder.location(Some(lk));
    }
    builder.build().ok()
}

fn bound_query(s: Option<&str>) -> Option<DateBound> {
    s.and_then(DateBound::parse)
}

fn kv_dt(dt: DateTime<Utc>) -> KValue {
    KValue::Datetime(DateTimeValue::utc(dt.naive_utc()))
}

fn kv_opt_dt(dt: Option<DateTime<Utc>>) -> KValue {
    dt.map(kv_dt).unwrap_or(KValue::Null)
}

fn doc_dt(doc: &Doc, key: &str) -> Option<DateTime<Utc>> {
    match doc.get(key)? {
        KValue::Datetime(d) => Some(DateTime::from_naive_utc_and_offset(d.utc, Utc)),
        KValue::Str(s) => meerschaum_kernel::value::parse_datetime_value(s)
            .map(|d| DateTime::from_naive_utc_and_offset(d.utc, Utc)),
        _ => None,
    }
}

/// A cell as JSON (datetimes as ISO strings).
fn kv_json(v: &KValue) -> Json {
    match v {
        v if v.is_na() => Json::Null,
        KValue::Bool(b) => Json::Bool(*b),
        KValue::Int(i) => Json::from(*i),
        KValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        KValue::Str(s) => Json::String(s.clone()),
        KValue::Json(j) => j.clone(),
        KValue::Datetime(d) => Json::String(d.isoformat()),
        other => Json::String(other.py_str()),
    }
}

fn doc_str(doc: &Doc, key: &str) -> Option<String> {
    match kv_json(doc.get(key)?) {
        Json::Null => None,
        Json::String(s) => Some(s),
        other => Some(other.to_string()),
    }
}

fn doc_int(doc: &Doc, key: &str) -> Option<i64> {
    match doc.get(key)? {
        KValue::Int(i) => Some(*i),
        KValue::Float(f) if f.is_finite() => Some(*f as i64),
        KValue::Str(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// A model value as JSON.
fn core_to_json(v: &Value) -> Json {
    serde_json::to_value(v).unwrap_or(Json::Null)
}

/// JSON as a model value.
fn json_to_core(j: Json) -> Value {
    Value::from_json_str(&j.to_string()).unwrap_or(Value::Null)
}

/// An API bound (an ISO timestamp, an int or `null`) as the engine stores it in a `json`
/// column (`{"datetime": ...}` / `{"int": ...}`).
fn bound_to_store(v: &Value) -> KValue {
    match &core_to_json(v) {
        Json::Null => KValue::Null,
        Json::String(s) => KValue::Json(serde_json::json!({"datetime": s})),
        Json::Number(n) if n.is_i64() => KValue::Json(serde_json::json!({"int": n})),
        other => KValue::Json(other.clone()),
    }
}

/// A stored bound as the API model's value.
fn bound_from_store(v: Option<&KValue>) -> Value {
    json_to_core(bound_json_from_store(v))
}

fn bound_json_from_store(v: Option<&KValue>) -> Json {
    let j = match v {
        Some(KValue::Json(j)) => j.clone(),
        Some(KValue::Str(s)) => serde_json::from_str(s).unwrap_or(Json::String(s.clone())),
        Some(other) => kv_json(other),
        None => Json::Null,
    };
    if let Json::Object(m) = &j
        && let Some(inner) = m.get("datetime").or_else(|| m.get("int"))
    {
        return inner.clone();
    }
    j
}

/// The columns of a state pipe (its configured dtypes).
fn columns_of(metric: &str) -> Vec<String> {
    meerschaum_connector::pipe_ops::engine_state_parameters(metric)
        .and_then(|p| p.get("dtypes").map(Value::to_map))
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default()
}

fn str_params(pairs: &[(&str, Value)]) -> Map {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

/// `status` of a run record from the stored `success`.
fn run_status(success: Option<&KValue>) -> &'static str {
    match success {
        Some(KValue::Bool(true)) => "success",
        Some(KValue::Bool(false)) => "failed",
        Some(KValue::Int(i)) if *i != 0 => "success",
        Some(KValue::Int(_)) => "failed",
        _ => "running",
    }
}

/// The record of a stored row: every column but the record's own fields, nulls dropped.
fn record_extra(doc: &Doc, own: &[&str]) -> IndexMap<String, Value> {
    doc.iter()
        .filter(|(k, v)| !own.contains(&k.as_str()) && !v.is_na())
        .map(|(k, v)| {
            let j = if matches!(k.as_str(), "begin" | "end") {
                bound_from_store(Some(v))
            } else {
                json_to_core(kv_json(v))
            };
            (k.clone(), j)
        })
        .collect()
}

#[async_trait]
impl EngineBackend for InstanceEngine {
    async fn append_ledger(&self, entries: Vec<LedgerEntry>) -> SuccessTuple {
        let n = entries.len();
        let docs = entries
            .iter()
            .map(|e| {
                [
                    ("created", kv_dt(e.created)),
                    ("pipe_ref", KValue::Str(e.pipe_ref.clone())),
                    ("seq", KValue::Int(e.seq)),
                    ("begin", bound_to_store(&e.begin)),
                    ("end", bound_to_store(&e.end)),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect()
            })
            .collect();
        let res = self.write("engine_ledger", docs).await;
        if res.success {
            SuccessTuple::ok(format!("Appended {n} ledger entries."))
        } else {
            res
        }
    }

    async fn ledger_after(&self, child_ref: &str, after_seq: Option<i64>) -> Vec<LedgerEntry> {
        let cursors = self
            .read_docs(
                "engine_cursors",
                str_params(&[("child_ref", Value::Str(child_ref.into()))]),
            )
            .await
            .unwrap_or_default();
        let parents: Vec<Value> = cursors
            .iter()
            .filter_map(|d| doc_str(d, "parent_ref"))
            .map(Value::Str)
            .collect();
        if parents.is_empty() {
            return Vec::new();
        }
        let mut entries: Vec<LedgerEntry> = self
            .read_docs(
                "engine_ledger",
                str_params(&[("pipe_ref", Value::List(parents))]),
            )
            .await
            .unwrap_or_default()
            .iter()
            .filter_map(|d| {
                Some(LedgerEntry {
                    pipe_ref: doc_str(d, "pipe_ref")?,
                    seq: doc_int(d, "seq")?,
                    created: doc_dt(d, "created")?,
                    begin: bound_from_store(d.get("begin")),
                    end: bound_from_store(d.get("end")),
                })
            })
            .filter(|e| after_seq.is_none_or(|s| e.seq > s))
            .collect();
        entries.sort_by(|a, b| (&a.pipe_ref, a.seq).cmp(&(&b.pipe_ref, b.seq)));
        entries
    }

    async fn put_cursors(&self, cursors: Vec<EdgeCursor>) -> SuccessTuple {
        let n = cursors.len();
        let docs = cursors
            .iter()
            .map(|c| {
                [
                    ("parent_ref", KValue::Str(c.parent_ref.clone())),
                    ("child_ref", KValue::Str(c.child_ref.clone())),
                    ("seq", KValue::Int(c.seq)),
                    (
                        "parent_watermark_at_cursor",
                        bound_to_store(&c.parent_watermark_at_cursor),
                    ),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect()
            })
            .collect();
        let res = self.write("engine_cursors", docs).await;
        if res.success {
            SuccessTuple::ok(format!("Upserted {n} cursors."))
        } else {
            res
        }
    }

    async fn get_state(&self, pipe_ref: &str) -> Option<EngineState> {
        let docs = self
            .read_docs(
                "engine_state",
                str_params(&[("pipe_ref", Value::Str(pipe_ref.into()))]),
            )
            .await
            .ok()?;
        let doc = docs.first()?;
        Some(EngineState {
            pipe_ref: pipe_ref.to_string(),
            definition_hash: doc_str(doc, "definition_hash"),
            extra: record_extra(doc, &["pipe_ref", "definition_hash"]),
        })
    }

    async fn put_states(&self, states: Vec<EngineState>) -> SuccessTuple {
        let n = states.len();
        let now = Utc::now();
        let docs = states
            .iter()
            .map(|s| {
                [
                    ("pipe_ref", KValue::Str(s.pipe_ref.clone())),
                    (
                        "definition_hash",
                        s.definition_hash
                            .clone()
                            .map(KValue::Str)
                            .unwrap_or(KValue::Null),
                    ),
                    ("updated", kv_dt(now)),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect()
            })
            .collect();
        let res = self.write("engine_state", docs).await;
        if res.success {
            SuccessTuple::ok(format!("Upserted {n} states."))
        } else {
            res
        }
    }

    async fn record_runs(&self, runs: Vec<RunRecord>) -> SuccessTuple {
        let n = runs.len();
        let (run_cols, node_cols) = (columns_of("engine_runs"), columns_of("engine_nodes"));
        let mut run_docs = Vec::new();
        let mut node_docs = Vec::new();
        for r in &runs {
            let mut doc: Doc = Doc::new();
            doc.insert("run_id".into(), KValue::Str(r.run_id.clone()));
            doc.insert("started".into(), kv_dt(r.started));
            doc.insert("finished".into(), kv_opt_dt(r.finished));
            let cols = match &r.pipe_ref {
                None => {
                    let success = match r.status.as_str() {
                        "success" | "succeeded" => KValue::Bool(true),
                        "failed" | "failure" => KValue::Bool(false),
                        _ => KValue::Null,
                    };
                    doc.insert("success".into(), success);
                    &run_cols
                }
                Some(pipe_ref) => {
                    doc.insert("pipe_ref".into(), KValue::Str(pipe_ref.clone()));
                    doc.insert("status".into(), KValue::Str(r.status.clone()));
                    &node_cols
                }
            };
            for (k, v) in &r.extra {
                if cols.contains(k) && !doc.contains_key(k) {
                    let value = if matches!(k.as_str(), "begin" | "end") {
                        bound_to_store(v)
                    } else {
                        meerschaum_kernel::frame::json_to_value(core_to_json(v))
                    };
                    doc.insert(k.clone(), value);
                }
            }
            if r.pipe_ref.is_none() {
                run_docs.push(doc);
            } else {
                node_docs.push(doc);
            }
        }
        for (metric, docs) in [("engine_runs", run_docs), ("engine_nodes", node_docs)] {
            let res = self.write(metric, docs).await;
            if !res.success {
                return res;
            }
        }
        SuccessTuple::ok(format!("Recorded {n} runs."))
    }

    async fn get_runs(&self, run_id: Option<&str>, limit: Option<i64>) -> Vec<RunRecord> {
        let params = match run_id {
            Some(id) => str_params(&[("run_id", Value::Str(id.into()))]),
            None => Map::new(),
        };
        let mut runs: Vec<RunRecord> = self
            .read_docs("engine_runs", params.clone())
            .await
            .unwrap_or_default()
            .iter()
            .filter_map(|d| {
                Some(RunRecord {
                    run_id: doc_str(d, "run_id")?,
                    pipe_ref: None,
                    started: doc_dt(d, "started")?,
                    finished: doc_dt(d, "finished"),
                    status: run_status(d.get("success")).to_string(),
                    extra: record_extra(d, &["run_id", "started", "finished", "success"]),
                })
            })
            .collect();
        runs.sort_by_key(|r| std::cmp::Reverse(r.started));
        if let Some(limit) = limit {
            runs.truncate(usize::try_from(limit.max(0)).unwrap_or(0));
        }
        if run_id.is_some() {
            let nodes = self
                .read_docs("engine_nodes", params)
                .await
                .unwrap_or_default();
            runs.extend(nodes.iter().filter_map(|d| {
                Some(RunRecord {
                    run_id: doc_str(d, "run_id")?,
                    pipe_ref: doc_str(d, "pipe_ref"),
                    started: doc_dt(d, "started")?,
                    finished: doc_dt(d, "finished"),
                    status: doc_str(d, "status").unwrap_or_default(),
                    extra: record_extra(
                        d,
                        &["run_id", "pipe_ref", "started", "finished", "status"],
                    ),
                })
            }));
        }
        runs
    }

    async fn record_events(&self, events: Vec<EngineEvent>) -> SuccessTuple {
        let n = events.len();
        // `seq` orders a run's events: continue each run's sequence.
        let mut next: IndexMap<String, i64> = IndexMap::new();
        let mut docs = Vec::new();
        for e in &events {
            if !next.contains_key(&e.run_id) {
                let last = self
                    .read_docs(
                        "engine_events",
                        str_params(&[("run_id", Value::Str(e.run_id.clone()))]),
                    )
                    .await
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|d| doc_int(d, "seq"))
                    .max()
                    .unwrap_or(0);
                next.insert(e.run_id.clone(), last + 1);
            }
            let seq = next.get_mut(&e.run_id).expect("inserted above");
            let doc: Doc = [
                ("ts", kv_dt(e.created)),
                ("run_id", KValue::Str(e.run_id.clone())),
                ("seq", KValue::Int(*seq)),
                (
                    "pipe_ref",
                    e.pipe_ref.clone().map(KValue::Str).unwrap_or(KValue::Null),
                ),
                ("event", KValue::Str(e.event.clone())),
                ("payload", KValue::Json(core_to_json(&e.payload))),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
            *seq += 1;
            docs.push(doc);
        }
        let res = self.write("engine_events", docs).await;
        if res.success {
            SuccessTuple::ok(format!("Recorded {n} events."))
        } else {
            res
        }
    }

    async fn run_events(
        &self,
        run_id: &str,
        after_seq: Option<i64>,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<EngineRunEvent>, EngineStoreError> {
        let df = self
            .read(
                "engine_events",
                DataQuery {
                    params: Some(str_params(&[("run_id", Value::Str(run_id.to_string()))])),
                    begin: since.map(DateBound::utc),
                    ..Default::default()
                },
            )
            .await?;
        let mut events: Vec<EngineRunEvent> = df
            .map(|df| frame_to_docs(&df))
            .unwrap_or_default()
            .iter()
            .filter_map(crate::engine_stream::event_from_doc)
            .filter(|e| after_seq.is_none_or(|s| e.seq > s))
            .collect();
        events.sort_by_key(|e| e.seq);
        events.dedup_by_key(|e| e.seq);
        Ok(events)
    }

    async fn acquire_lease(&self, pipe_ref: &str, holder: &str, ttl: Duration) -> bool {
        let Some(pipe) = pipe_from_ref(pipe_ref) else {
            return false;
        };
        match self.conn.acquire_pipe_lease(&pipe, holder, ttl).await {
            Ok(b) => b,
            Err(ConnectorError::NotImplemented(_)) => {
                self.local_leases.acquire_lease(pipe_ref, holder, ttl).await
            }
            Err(e) => {
                tracing::warn!("Failed to acquire the lease on {pipe_ref}: {e}");
                false
            }
        }
    }

    async fn release_lease(&self, pipe_ref: &str, holder: &str) -> bool {
        let Some(pipe) = pipe_from_ref(pipe_ref) else {
            return false;
        };
        match self.conn.release_pipe_lease(&pipe, holder).await {
            Ok(b) => b,
            Err(ConnectorError::NotImplemented(_)) => {
                self.local_leases.release_lease(pipe_ref, holder).await
            }
            Err(e) => {
                tracing::warn!("Failed to release the lease on {pipe_ref}: {e}");
                false
            }
        }
    }

    async fn read_pipe(
        &self,
        metric: &str,
        query: &EnginePipeQuery,
    ) -> Result<Option<Frame>, EngineStoreError> {
        self.read(
            metric,
            DataQuery {
                params: query.params_map(),
                select_columns: query.select_columns.clone(),
                begin: bound_query(query.begin.as_deref()),
                end: bound_query(query.end.as_deref()),
                ..Default::default()
            },
        )
        .await
    }

    async fn write_pipe(
        &self,
        metric: &str,
        docs: Vec<Doc>,
    ) -> Result<SuccessTuple, EngineStoreError> {
        Ok(self.write(metric, docs).await)
    }

    async fn clear_pipe(
        &self,
        metric: &str,
        query: &EnginePipeQuery,
    ) -> Result<SuccessTuple, EngineStoreError> {
        Ok(self
            .clear(
                metric,
                query.params_map(),
                bound_query(query.end.as_deref()),
            )
            .await)
    }

    async fn pipe_exists(&self, metric: &str) -> Result<bool, EngineStoreError> {
        let pipe = self.pipe(metric).await?;
        self.conn
            .pipe_exists(&pipe)
            .await
            .map_err(|e| EngineStoreError::Failed(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipe_refs_become_pipes() {
        let p = pipe_from_ref("api:main|sql:x|temp|None").unwrap();
        assert_eq!(
            (p.connector_keys(), p.metric_key(), p.location_key()),
            ("sql:x", "temp", None)
        );
        let p = pipe_from_ref("api:main|sql:x|temp|here").unwrap();
        assert_eq!(p.location_key(), Some("here"));
        assert!(pipe_from_ref("nope").is_none());
    }

    #[test]
    fn bounds_map_both_ways() {
        let s = Value::Str("2024-01-01T00:00:00+00:00".into());
        assert_eq!(bound_from_store(Some(&bound_to_store(&s))), s);
        let i = Value::Int(5);
        assert_eq!(bound_from_store(Some(&bound_to_store(&i))), i);
        assert_eq!(
            bound_from_store(Some(&bound_to_store(&Value::Null))),
            Value::Null
        );
    }
}
