//! Engine state on instances (§5.8, R5, R13): the `mrsm:engine_*` internal pipes.
//!
//! [`InstanceState`] implements [`StateSource`] and [`StateWriter`] over any
//! [`InstanceConnector`]: every row lives in a `temporary`, `static` pipe
//! `Pipe('mrsm', 'engine_<name>', instance=..., target='mrsm_engine_<name>')`, keyed on
//! `pipe_ref` (never `pipe_id`). Writes go through `sync_pipe(check_existing=False,
//! enforce=False, _check_temporary_tables=False)` with rows built in memory, so no dtype
//! inference query precedes them.
//!
//! Where each row lives:
//!
//! | Row | Instance |
//! |---|---|
//! | watermark | read with `get_sync_time` on the pipe's own instance (not persisted) |
//! | definition hash (`engine_state`) | the pipe's own instance |
//! | ledger entries, cursors | the edge's state instance (the child's, §5.1) |
//! | runs, nodes, events | the engine's home instance (`-i`, or the configured default) |
//!
//! **Sequence numbers.** The ledger's per-pipe `seq` is the stored maximum plus one, read under
//! the node's lease (two engine runs never sync the same pipe at once) and an in-process lock.
//! The trait has no atomic counter yet; see the crate README's requests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use meerschaum_connector::pipe_ops::{Doc, frame_to_docs};
use meerschaum_connector::{ConnectorError, DataQuery, DateBound, InstanceConnector, SyncOptions};
use meerschaum_core::success::Bound;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::{PipeDtypesContext, PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};

use crate::events::EventRow;
use crate::ledger::{EdgeCursor, LedgerEntry};
use crate::node::PipeRef;
use crate::plan::Action;
use crate::registry::ConnectorRegistry;
use crate::runs::{EnginePipe, NodeRow, NodeStatus, RunRecord, RunRow, StateRow};
use crate::state::{StateError, StateSource, StateWriter};
use crate::sync::date_bound_to_bound;

/// The internal pipe for one kind of engine state on an instance.
pub fn engine_pipe(instance_keys: &str, kind: EnginePipe) -> Pipe {
    Pipe::builder("mrsm", kind.metric_key())
        .instance(instance_keys)
        .parameters(kind.parameters())
        .kwarg("temporary", true)
        .kwarg("static", true)
        .kwarg("cache", false)
        .build()
        .expect("static engine pipe keys are valid")
}

/// A bound as stored in a `json` column: `{"datetime": "<ISO>"}` or `{"int": n}`. Explicitly
/// typed so that a datetime is never confused with a string.
pub fn bound_to_json(b: &Bound) -> serde_json::Value {
    match b {
        Bound::Datetime(dt) => serde_json::json!({"datetime": dt.to_rfc3339()}),
        Bound::Int(i) => serde_json::json!({"int": i}),
    }
}

/// Parse a stored bound (also accepts a bare ISO string or integer).
pub fn bound_from_json(j: &serde_json::Value) -> Option<Bound> {
    match j {
        serde_json::Value::Object(m) => {
            if let Some(i) = m.get("int").and_then(serde_json::Value::as_i64) {
                return Some(Bound::Int(i));
            }
            m.get("datetime")
                .and_then(serde_json::Value::as_str)
                .and_then(parse_dt)
                .map(Bound::Datetime)
        }
        serde_json::Value::Number(n) => n.as_i64().map(Bound::Int),
        serde_json::Value::String(s) => parse_dt(s).map(Bound::Datetime),
        _ => None,
    }
}

fn parse_dt(s: &str) -> Option<DateTime<Utc>> {
    meerschaum_kernel::value::parse_datetime_value(s)
        .map(|d| DateTime::<Utc>::from_naive_utc_and_offset(d.utc, Utc))
}

fn kv_json(b: Option<&Bound>) -> KValue {
    b.map(|b| KValue::Json(bound_to_json(b)))
        .unwrap_or(KValue::Null)
}

fn kv_dt(dt: DateTime<Utc>) -> KValue {
    KValue::Datetime(DateTimeValue::utc(dt.naive_utc()))
}

fn kv_opt_dt(dt: Option<DateTime<Utc>>) -> KValue {
    dt.map(kv_dt).unwrap_or(KValue::Null)
}

fn kv_str(s: impl Into<String>) -> KValue {
    KValue::Str(s.into())
}

fn kv_opt_str(s: Option<&str>) -> KValue {
    s.map(kv_str).unwrap_or(KValue::Null)
}

fn kv_float(f: Option<f64>) -> KValue {
    f.map(KValue::Float).unwrap_or(KValue::Null)
}

/// Read a doc field as a bound.
fn doc_bound(doc: &Doc, key: &str) -> Option<Bound> {
    match doc.get(key)? {
        KValue::Json(j) => bound_from_json(j),
        KValue::Str(s) => serde_json::from_str(s)
            .ok()
            .and_then(|j: serde_json::Value| bound_from_json(&j))
            .or_else(|| parse_dt(s).map(Bound::Datetime)),
        KValue::Int(i) => Some(Bound::Int(*i)),
        KValue::Datetime(d) => Some(Bound::Datetime(DateTime::from_naive_utc_and_offset(
            d.utc, Utc,
        ))),
        _ => None,
    }
}

fn doc_str(doc: &Doc, key: &str) -> Option<String> {
    doc.get(key).filter(|v| !v.is_na()).map(|v| match v {
        KValue::Str(s) => s.clone(),
        other => other.py_str(),
    })
}

fn doc_int(doc: &Doc, key: &str) -> Option<i64> {
    match doc.get(key)? {
        KValue::Int(i) => Some(*i),
        KValue::Float(f) if f.is_finite() => Some(*f as i64),
        KValue::Bool(b) => Some(*b as i64),
        KValue::Str(s) => s.trim().parse().ok(),
        KValue::Decimal(d) => Some(d.to_f64() as i64),
        _ => None,
    }
}

fn doc_float(doc: &Doc, key: &str) -> Option<f64> {
    match doc.get(key)? {
        KValue::Float(f) if f.is_finite() => Some(*f),
        KValue::Int(i) => Some(*i as f64),
        KValue::Str(s) => s.trim().parse().ok(),
        KValue::Decimal(d) => Some(d.to_f64()),
        _ => None,
    }
}

fn doc_dt(doc: &Doc, key: &str) -> Option<DateTime<Utc>> {
    match doc.get(key)? {
        KValue::Datetime(d) => Some(DateTime::from_naive_utc_and_offset(d.utc, Utc)),
        KValue::Str(s) => parse_dt(s),
        _ => None,
    }
}

fn doc_bool(doc: &Doc, key: &str) -> Option<bool> {
    match doc.get(key)? {
        KValue::Bool(b) => Some(*b),
        KValue::Int(i) => Some(*i != 0),
        KValue::Str(s) => Some(matches!(s.as_str(), "True" | "true" | "1")),
        _ => None,
    }
}

fn parse_enum<T: serde::de::DeserializeOwned>(s: Option<String>) -> Option<T> {
    serde_json::from_value(serde_json::Value::String(s?)).ok()
}

fn str_map(pairs: &[(&str, Value)]) -> Map {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

/// The configured dtypes of an engine pipe, as an enforcement context (no instance query).
fn static_ctx(kind: EnginePipe) -> PipeDtypesContext {
    let dtypes: IndexMap<String, String> = match kind.parameters().get("dtypes") {
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

/// Build a frame for an engine pipe from rows (dtype-enforced in memory).
pub fn engine_frame(kind: EnginePipe, docs: Vec<Doc>) -> Result<Frame, StateError> {
    pipe_enforce_dtypes(PipeInput::Docs(docs), &static_ctx(kind))
        .map_err(|e| StateError::Other(format!("cannot build {} rows: {e}", kind.metric_key())))
}

fn unreachable(instance: &str, e: impl std::fmt::Display) -> StateError {
    StateError::Unreachable {
        instance: instance.to_string(),
        message: e.to_string(),
    }
}

/// Rows of `mrsm:engine_nodes` as documents.
///
/// `started` is the pipe's datetime (and partition) column, so a node that never started is
/// stored with the run's start (`run_started`): a NULL partition key has no partition and
/// defeats the `(started, run_id, pipe_ref)` upsert key. [`node_from_doc`] reads it back as
/// `None` for a node with no attempt and no finish.
pub fn node_doc(row: &NodeRow, run_started: DateTime<Utc>) -> Doc {
    [
        ("run_id", kv_str(&row.run_id)),
        ("pipe_ref", kv_str(row.pipe_ref.as_str())),
        ("action", kv_str(row.action.name())),
        (
            "status",
            kv_str(
                serde_json::to_value(row.status)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_default(),
            ),
        ),
        ("reason", kv_str(&row.reason)),
        ("begin", kv_json(row.begin.as_ref())),
        ("end", kv_json(row.end.as_ref())),
        ("num_chunks", KValue::Int(row.num_chunks as i64)),
        ("attempts", KValue::Int(row.attempts as i64)),
        ("started", kv_dt(row.started.unwrap_or(run_started))),
        ("finished", kv_opt_dt(row.finished)),
        ("duration_seconds", kv_float(row.duration_seconds)),
        ("inserted", KValue::Int(row.inserted as i64)),
        ("updated", KValue::Int(row.updated as i64)),
        ("upserted", KValue::Int(row.upserted as i64)),
        ("deleted", KValue::Int(row.deleted as i64)),
        ("message", kv_opt_str(row.message.as_deref())),
        ("planned_start_seconds", kv_float(row.planned_start_seconds)),
        ("slack_seconds", kv_float(row.slack_seconds)),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

/// A `mrsm:engine_nodes` document as a row.
pub fn node_from_doc(doc: &Doc) -> Option<NodeRow> {
    let pipe_ref = PipeRef::from_string(doc_str(doc, "pipe_ref")?);
    let action: Action = parse_enum(doc_str(doc, "action")).unwrap_or(Action::Skip);
    let mut row = NodeRow::new(
        &doc_str(doc, "run_id")?,
        &pipe_ref,
        action,
        &doc_str(doc, "reason").unwrap_or_default(),
    );
    row.status = parse_enum(doc_str(doc, "status")).unwrap_or(NodeStatus::Pending);
    row.begin = doc_bound(doc, "begin");
    row.end = doc_bound(doc, "end");
    row.num_chunks = doc_int(doc, "num_chunks").unwrap_or(0).max(0) as usize;
    row.attempts = doc_int(doc, "attempts").unwrap_or(0).max(0) as u32;
    row.started = doc_dt(doc, "started");
    row.finished = doc_dt(doc, "finished");
    // (Only when the row was read with those columns: history reads select a subset.)
    if doc.contains_key("attempts") && row.attempts == 0 && row.finished.is_none() {
        // Stored with the run's start (see `node_doc`): the node never started.
        row.started = None;
    }
    row.duration_seconds = doc_float(doc, "duration_seconds");
    row.inserted = doc_int(doc, "inserted").unwrap_or(0).max(0) as u64;
    row.updated = doc_int(doc, "updated").unwrap_or(0).max(0) as u64;
    row.upserted = doc_int(doc, "upserted").unwrap_or(0).max(0) as u64;
    row.deleted = doc_int(doc, "deleted").unwrap_or(0).max(0) as u64;
    row.message = doc_str(doc, "message");
    row.planned_start_seconds = doc_float(doc, "planned_start_seconds");
    row.slack_seconds = doc_float(doc, "slack_seconds");
    Some(row)
}

/// A `mrsm:engine_runs` row as a document.
pub fn run_doc(run: &RunRow) -> Doc {
    [
        ("run_id", kv_str(&run.run_id)),
        ("started", kv_dt(run.started)),
        ("finished", kv_opt_dt(run.finished)),
        (
            "success",
            run.success.map(KValue::Bool).unwrap_or(KValue::Null),
        ),
        ("message", kv_opt_str(run.message.as_deref())),
        (
            "targets",
            KValue::Json(serde_json::Value::Array(
                run.targets
                    .iter()
                    .map(|t| serde_json::Value::String(t.to_string()))
                    .collect(),
            )),
        ),
        ("workers", KValue::Int(run.workers as i64)),
        ("num_nodes", KValue::Int(run.num_nodes as i64)),
        ("resumed_from", kv_opt_str(run.resumed_from.as_deref())),
        ("holder", kv_str(&run.holder)),
        ("selection", kv_opt_str(run.selection.as_deref())),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

/// A `mrsm:engine_runs` document as a row.
pub fn run_from_doc(doc: &Doc) -> Option<RunRow> {
    let targets = match doc.get("targets") {
        Some(KValue::Json(serde_json::Value::Array(a))) => a
            .iter()
            .filter_map(|v| v.as_str().map(PipeRef::from_string))
            .collect(),
        Some(KValue::Str(s)) => serde_json::from_str::<Vec<String>>(s)
            .unwrap_or_default()
            .into_iter()
            .map(PipeRef::from_string)
            .collect(),
        _ => Vec::new(),
    };
    Some(RunRow {
        run_id: doc_str(doc, "run_id")?,
        started: doc_dt(doc, "started")?,
        finished: doc_dt(doc, "finished"),
        success: doc_bool(doc, "success"),
        message: doc_str(doc, "message"),
        targets,
        workers: doc_int(doc, "workers").unwrap_or(0).max(0) as usize,
        num_nodes: doc_int(doc, "num_nodes").unwrap_or(0).max(0) as usize,
        resumed_from: doc_str(doc, "resumed_from"),
        holder: doc_str(doc, "holder").unwrap_or_default(),
        selection: doc_str(doc, "selection"),
    })
}

/// Serializes sequence allocation for one `(instance, pipe_ref)` in this process.
type SeqLock = Arc<tokio::sync::Mutex<()>>;

/// [`StateSource`] + [`StateWriter`] over the instances of a [`ConnectorRegistry`].
pub struct InstanceState {
    registry: Arc<dyn ConnectorRegistry>,
    home: String,
    pipes: RwLock<HashMap<PipeRef, Pipe>>,
    seq_locks: Mutex<HashMap<(String, PipeRef), SeqLock>>,
}

impl std::fmt::Debug for InstanceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstanceState")
            .field("home", &self.home)
            .finish_non_exhaustive()
    }
}

impl InstanceState {
    /// State over `registry`; runs, nodes and events go to `home_instance`.
    pub fn new(registry: Arc<dyn ConnectorRegistry>, home_instance: &str) -> InstanceState {
        InstanceState {
            registry,
            home: home_instance.to_string(),
            pipes: RwLock::new(HashMap::new()),
            seq_locks: Mutex::new(HashMap::new()),
        }
    }

    /// The home instance (runs, nodes, events).
    pub fn home(&self) -> &str {
        &self.home
    }

    /// Remember the pipes of a graph (their columns are needed to read watermarks).
    pub fn register_pipes(&self, pipes: impl IntoIterator<Item = (PipeRef, Pipe)>) {
        let mut map = self.pipes.write().unwrap_or_else(|e| e.into_inner());
        for (r, p) in pipes {
            map.insert(r, p);
        }
    }

    /// The pipe for a node (the registered one when known, else built from its keys).
    pub fn pipe_for(&self, node: &PipeRef) -> Pipe {
        if let Some(p) = self
            .pipes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(node)
        {
            return p.clone();
        }
        let parts = node.parts();
        Pipe::builder(parts.connector_keys, parts.metric_key)
            .location(parts.location_key)
            .instance(parts.instance_keys)
            .build()
            .unwrap_or_else(|_| engine_pipe(parts.instance_keys, EnginePipe::State))
    }

    fn conn(&self, instance: &str) -> Result<Arc<dyn InstanceConnector>, StateError> {
        self.registry
            .instance(instance)
            .map_err(|e| unreachable(instance, e))
    }

    /// Read engine-state rows.
    pub async fn read_rows(
        &self,
        instance: &str,
        kind: EnginePipe,
        params: Map,
        select: Option<&[&str]>,
    ) -> Result<Vec<Doc>, StateError> {
        let conn = self.conn(instance)?;
        let pipe = engine_pipe(instance, kind);
        let q = DataQuery {
            params: Some(params),
            select_columns: select.map(|s| s.iter().map(|c| c.to_string()).collect()),
            ..Default::default()
        };
        match conn.get_pipe_data(&pipe, &q).await {
            Ok(Some(df)) => Ok(frame_to_docs(&df)),
            Ok(None) => Ok(Vec::new()),
            Err(ConnectorError::NotImplemented(m)) => Err(StateError::ReadOnly { instance: m }),
            Err(e) => Err(unreachable(instance, e)),
        }
    }

    /// Write engine-state rows (one `sync_pipe`).
    pub async fn write_rows(
        &self,
        instance: &str,
        kind: EnginePipe,
        docs: Vec<Doc>,
    ) -> Result<(), StateError> {
        if docs.is_empty() {
            return Ok(());
        }
        let conn = self.conn(instance)?;
        let pipe = engine_pipe(instance, kind);
        let df = engine_frame(kind, docs)?;
        let upsert = pipe.upsert();
        let mut extra = Map::new();
        extra.insert("_check_temporary_tables".into(), Value::Bool(false));
        extra.insert("enforce".into(), Value::Bool(false));
        let res = conn
            .sync_pipe(
                &pipe,
                df,
                &SyncOptions {
                    check_existing: upsert,
                    extra,
                    ..Default::default()
                },
            )
            .await;
        if res.success {
            Ok(())
        } else {
            Err(StateError::Other(format!(
                "cannot write {} on '{instance}': {}",
                kind.metric_key(),
                res.message
            )))
        }
    }

    /// Delete engine-state rows matching `params` (and `end` on the datetime axis).
    pub async fn clear_rows(
        &self,
        instance: &str,
        kind: EnginePipe,
        params: Map,
        end: Option<DateTime<Utc>>,
    ) -> Result<(), StateError> {
        let conn = self.conn(instance)?;
        let pipe = engine_pipe(instance, kind);
        if !conn.pipe_exists(&pipe).await.unwrap_or(false) {
            return Ok(());
        }
        let res = conn
            .clear_pipe(
                &pipe,
                &DataQuery {
                    params: Some(params),
                    end: end.map(DateBound::utc),
                    ..Default::default()
                },
            )
            .await;
        if res.success {
            Ok(())
        } else {
            Err(StateError::Other(res.message))
        }
    }

    fn seq_lock(&self, instance: &str, pipe_ref: &PipeRef) -> SeqLock {
        self.seq_locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry((instance.to_string(), pipe_ref.clone()))
            .or_default()
            .clone()
    }

    async fn seqs(&self, instance: &str, parent: &PipeRef) -> Result<Vec<i64>, StateError> {
        let docs = self
            .read_rows(
                instance,
                EnginePipe::Ledger,
                str_map(&[("pipe_ref", Value::Str(parent.to_string()))]),
                Some(&["seq"]),
            )
            .await?;
        Ok(docs.iter().filter_map(|d| doc_int(d, "seq")).collect())
    }

    /// Load a run and its nodes from the home instance.
    pub async fn load_run(&self, run_id: &str) -> Result<Option<RunRecord>, StateError> {
        let params = str_map(&[("run_id", Value::Str(run_id.to_string()))]);
        let runs = self
            .read_rows(&self.home, EnginePipe::Runs, params.clone(), None)
            .await?;
        let Some(run) = runs.iter().find_map(run_from_doc) else {
            return Ok(None);
        };
        let nodes = self
            .read_rows(&self.home, EnginePipe::Nodes, params, None)
            .await?
            .iter()
            .filter_map(node_from_doc)
            .collect();
        Ok(Some(RunRecord { run, nodes }))
    }

    /// The most recent runs on the home instance, newest first.
    pub async fn recent_runs(&self, limit: usize) -> Result<Vec<RunRow>, StateError> {
        let mut runs: Vec<RunRow> = self
            .read_rows(&self.home, EnginePipe::Runs, Map::new(), None)
            .await?
            .iter()
            .filter_map(run_from_doc)
            .collect();
        runs.sort_by_key(|r| std::cmp::Reverse(r.started));
        runs.truncate(limit);
        Ok(runs)
    }

    /// Node rows on the home instance since `since` (oldest first), for duration estimates.
    pub async fn node_history(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<NodeRow>, StateError> {
        let conn = self.conn(&self.home)?;
        let pipe = engine_pipe(&self.home, EnginePipe::Nodes);
        let q = DataQuery {
            params: Some(str_map(&[("status", Value::Str("succeeded".into()))])),
            select_columns: Some(
                [
                    "run_id",
                    "pipe_ref",
                    "action",
                    "status",
                    "duration_seconds",
                    "started",
                ]
                .iter()
                .map(|c| c.to_string())
                .collect(),
            ),
            begin: since.map(DateBound::utc),
            ..Default::default()
        };
        let mut rows: Vec<NodeRow> = match conn.get_pipe_data(&pipe, &q).await {
            Ok(Some(df)) => frame_to_docs(&df)
                .iter()
                .filter_map(node_from_doc)
                .collect(),
            Ok(None) => Vec::new(),
            Err(e) => return Err(unreachable(&self.home, e)),
        };
        rows.sort_by_key(|r| r.started);
        Ok(rows)
    }

    /// Persist events (`mrsm:engine_events`) on the home instance.
    pub async fn write_events(&self, rows: &[EventRow]) -> Result<(), StateError> {
        let docs = rows
            .iter()
            .map(|e| {
                [
                    ("ts", kv_dt(e.ts)),
                    ("run_id", kv_str(&e.run_id)),
                    ("seq", KValue::Int(e.seq as i64)),
                    (
                        "pipe_ref",
                        kv_opt_str(e.pipe_ref.as_ref().map(PipeRef::as_str)),
                    ),
                    ("event", kv_str(&e.event)),
                    (
                        "payload",
                        serde_json::from_str(&e.payload)
                            .map(KValue::Json)
                            .unwrap_or_else(|_| kv_str(&e.payload)),
                    ),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect()
            })
            .collect();
        self.write_rows(&self.home, EnginePipe::Events, docs).await
    }

    /// The events of a run, in order.
    pub async fn load_events(&self, run_id: &str) -> Result<Vec<EventRow>, StateError> {
        let docs = self
            .read_rows(
                &self.home,
                EnginePipe::Events,
                str_map(&[("run_id", Value::Str(run_id.to_string()))]),
                None,
            )
            .await?;
        let mut out: Vec<EventRow> = docs
            .iter()
            .filter_map(|d| {
                Some(EventRow {
                    ts: doc_dt(d, "ts")?,
                    run_id: doc_str(d, "run_id")?,
                    seq: doc_int(d, "seq")?.max(0) as u64,
                    pipe_ref: doc_str(d, "pipe_ref").map(PipeRef::from_string),
                    event: doc_str(d, "event")?,
                    payload: match d.get("payload") {
                        Some(KValue::Json(j)) => j.to_string(),
                        Some(KValue::Str(s)) => s.clone(),
                        _ => String::new(),
                    },
                })
            })
            .collect();
        out.sort_by_key(|e| e.seq);
        Ok(out)
    }

    /// Retention (§5.8): delete runs, nodes and events older than `before` on the home
    /// instance. Ledger entries are not aged out: consumed entries are pruned by compaction
    /// ([`StateWriter::prune_ledger`]), and an unconsumed entry must survive until its children
    /// read it.
    pub async fn prune_history(&self, before: DateTime<Utc>) -> Vec<StateError> {
        let mut errors = Vec::new();
        for kind in [EnginePipe::Runs, EnginePipe::Nodes, EnginePipe::Events] {
            if let Err(e) = self
                .clear_rows(&self.home, kind, Map::new(), Some(before))
                .await
            {
                errors.push(e);
            }
        }
        errors
    }
}

impl StateSource for InstanceState {
    async fn watermark(&self, node: &PipeRef) -> Result<Option<Bound>, StateError> {
        let instance = node.instance_keys().to_string();
        let conn = self.conn(&instance)?;
        let pipe = self.pipe_for(node);
        if pipe.column("datetime").is_none() {
            return Ok(None);
        }
        match conn.get_sync_time(&pipe, None, true).await {
            Ok(t) => Ok(t.as_ref().and_then(date_bound_to_bound)),
            Err(e) => Err(unreachable(&instance, e)),
        }
    }

    async fn definition_hash(&self, node: &PipeRef) -> Result<Option<String>, StateError> {
        let docs = self
            .read_rows(
                node.instance_keys(),
                EnginePipe::State,
                str_map(&[("pipe_ref", Value::Str(node.to_string()))]),
                Some(&["pipe_ref", "definition_hash"]),
            )
            .await?;
        Ok(docs.iter().find_map(|d| doc_str(d, "definition_hash")))
    }

    async fn cursor(
        &self,
        state_instance: &str,
        parent: &PipeRef,
        child: &PipeRef,
    ) -> Result<Option<EdgeCursor>, StateError> {
        let docs = self
            .read_rows(
                state_instance,
                EnginePipe::Cursors,
                str_map(&[
                    ("parent_ref", Value::Str(parent.to_string())),
                    ("child_ref", Value::Str(child.to_string())),
                ]),
                None,
            )
            .await?;
        Ok(docs.iter().find_map(|d| {
            Some(EdgeCursor {
                parent_ref: parent.clone(),
                child_ref: child.clone(),
                seq: doc_int(d, "seq")?,
                parent_watermark_at_cursor: doc_bound(d, "parent_watermark_at_cursor"),
            })
        }))
    }

    async fn ledger_after(
        &self,
        state_instance: &str,
        parent: &PipeRef,
        after_seq: i64,
    ) -> Result<Vec<LedgerEntry>, StateError> {
        let docs = self
            .read_rows(
                state_instance,
                EnginePipe::Ledger,
                str_map(&[("pipe_ref", Value::Str(parent.to_string()))]),
                None,
            )
            .await?;
        let mut out: Vec<LedgerEntry> = docs
            .iter()
            .filter_map(|d| {
                let seq = doc_int(d, "seq")?;
                (seq > after_seq).then(|| LedgerEntry {
                    pipe_ref: parent.clone(),
                    seq,
                    begin: doc_bound(d, "begin"),
                    end: doc_bound(d, "end"),
                    created: doc_dt(d, "created").unwrap_or_else(crate::state::epoch),
                })
            })
            .collect();
        out.sort_by_key(|e| e.seq);
        Ok(out)
    }

    async fn latest_seq(
        &self,
        state_instance: &str,
        parent: &PipeRef,
    ) -> Result<Option<i64>, StateError> {
        Ok(self.seqs(state_instance, parent).await?.into_iter().max())
    }
}

fn cursor_doc(cursor: &EdgeCursor) -> Doc {
    [
        ("parent_ref", kv_str(cursor.parent_ref.as_str())),
        ("child_ref", kv_str(cursor.child_ref.as_str())),
        ("seq", KValue::Int(cursor.seq)),
        (
            "parent_watermark_at_cursor",
            kv_json(cursor.parent_watermark_at_cursor.as_ref()),
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

impl StateWriter for InstanceState {
    async fn put_cursor(&self, state_instance: &str, cursor: EdgeCursor) -> Result<(), StateError> {
        self.write_rows(
            state_instance,
            EnginePipe::Cursors,
            vec![cursor_doc(&cursor)],
        )
        .await
    }

    async fn put_cursors(
        &self,
        state_instance: &str,
        cursors: Vec<EdgeCursor>,
    ) -> Result<(), StateError> {
        // One upsert for all of a node's edges on this instance.
        self.write_rows(
            state_instance,
            EnginePipe::Cursors,
            cursors.iter().map(cursor_doc).collect(),
        )
        .await
    }

    async fn put_definition_hash(&self, node: &PipeRef, hash: &str) -> Result<(), StateError> {
        let row = StateRow {
            pipe_ref: node.clone(),
            definition_hash: hash.to_string(),
            updated: Utc::now(),
        };
        let doc: Doc = [
            ("pipe_ref", kv_str(row.pipe_ref.as_str())),
            ("definition_hash", kv_str(&row.definition_hash)),
            ("updated", kv_dt(row.updated)),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        self.write_rows(node.instance_keys(), EnginePipe::State, vec![doc])
            .await
    }

    async fn append_ledger(
        &self,
        state_instance: &str,
        pipe_ref: &PipeRef,
        spans: Vec<(Option<Bound>, Option<Bound>)>,
    ) -> Result<Vec<LedgerEntry>, StateError> {
        if spans.is_empty() {
            return Ok(Vec::new());
        }
        let lock = self.seq_lock(state_instance, pipe_ref);
        let _guard = lock.lock().await;
        let last = self
            .latest_seq(state_instance, pipe_ref)
            .await?
            .unwrap_or(0);
        let created = Utc::now();
        let entries: Vec<LedgerEntry> = (last + 1..)
            .zip(spans)
            .map(|(seq, (begin, end))| LedgerEntry {
                pipe_ref: pipe_ref.clone(),
                seq,
                begin,
                end,
                created,
            })
            .collect();
        let docs = entries
            .iter()
            .map(|e| {
                [
                    ("created", kv_dt(e.created)),
                    ("pipe_ref", kv_str(e.pipe_ref.as_str())),
                    ("seq", KValue::Int(e.seq)),
                    ("begin", kv_json(e.begin.as_ref())),
                    ("end", kv_json(e.end.as_ref())),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect()
            })
            .collect();
        self.write_rows(state_instance, EnginePipe::Ledger, docs)
            .await?;
        Ok(entries)
    }

    async fn prune_ledger(
        &self,
        state_instance: &str,
        parent: &PipeRef,
        through_seq: i64,
    ) -> Result<usize, StateError> {
        let all = self.seqs(state_instance, parent).await?;
        // The newest entry is never pruned: it carries the pipe's sequence high-water mark, so
        // the next entry continues the sequence instead of restarting below the cursors.
        let newest = all.iter().copied().max();
        let seqs: Vec<i64> = all
            .into_iter()
            .filter(|s| *s <= through_seq && Some(*s) != newest)
            .collect();
        for chunk in seqs.chunks(500) {
            self.clear_rows(
                state_instance,
                EnginePipe::Ledger,
                str_map(&[
                    ("pipe_ref", Value::Str(parent.to_string())),
                    (
                        "seq",
                        Value::List(chunk.iter().map(|s| Value::Int(*s)).collect()),
                    ),
                ]),
                None,
            )
            .await?;
        }
        Ok(seqs.len())
    }

    async fn record_run(&self, run: &RunRecord) -> Result<(), StateError> {
        self.write_rows(&self.home, EnginePipe::Runs, vec![run_doc(&run.run)])
            .await?;
        self.write_rows(
            &self.home,
            EnginePipe::Nodes,
            run.nodes
                .iter()
                .map(|n| node_doc(n, run.run.started))
                .collect(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn bounds_round_trip_as_json() {
        let dt = Bound::Datetime(Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap());
        assert_eq!(bound_from_json(&bound_to_json(&dt)), Some(dt));
        assert_eq!(
            bound_from_json(&bound_to_json(&Bound::Int(-5))),
            Some(Bound::Int(-5))
        );
        assert_eq!(
            bound_from_json(&serde_json::json!("2026-01-02T03:04:05Z")),
            Some(dt)
        );
    }

    #[test]
    fn node_rows_round_trip() {
        let mut row = NodeRow::new(
            "r1",
            &PipeRef::from_string("a|b|c|None"),
            Action::Backfill,
            "x",
        );
        row.status = NodeStatus::UpstreamFailed;
        row.begin = Some(Bound::Int(3));
        row.inserted = 7;
        row.duration_seconds = Some(1.5);
        let back = node_from_doc(&node_doc(&row, Utc::now())).unwrap();
        assert_eq!(back, row);
    }
}
