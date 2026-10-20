//! Pipe-level operations needed by connector implementations and by the trait defaults:
//! value conversions, `Pipe.get_dtypes()`, `Pipe.enforce_dtypes()`, the essentials of
//! `Pipe.sync()` for internal pipes, `Pipe.get_value()` / `get_doc()`, and the generic users,
//! plugins and tokens implementations of `meerschaum/connectors/instance/`.
//!
//! The full `Pipe.sync()` flow belongs to `meerschaum-engine`; these are the pieces connector
//! implementations and the trait defaults need.

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use meerschaum_core::dtypes::is_dtype_special;
use meerschaum_core::users::{Token, User};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::detect::get_special_cols;
use meerschaum_kernel::enforce::{
    PipeDtypesContext, PipeInput, pipe_enforce_dtypes, pipe_get_dtypes, pipe_is_tz_aware,
};
use meerschaum_kernel::query::ParamValue;
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};
use uuid::Uuid;

use crate::models::TokenModel;
use crate::traits::{ConnectorResult, DataQuery, InstanceConnector, PluginRecord, SyncOptions};

/// A document: column name to kernel value.
pub type Doc = IndexMap<String, KValue>;

/// `df.to_dict(orient='records')`.
pub fn frame_to_docs(df: &Frame) -> Vec<Doc> {
    let names = df.names();
    let columns: Vec<Vec<KValue>> = df.columns().iter().map(|c| c.data.values()).collect();
    (0..df.height())
        .map(|i| {
            names
                .iter()
                .zip(columns.iter())
                .map(|(n, vals)| (n.clone(), vals[i].clone()))
                .collect()
        })
        .collect()
}

/// A core (JSON-like) value as a kernel value (containers become JSON values).
pub fn core_to_kernel(v: &Value) -> KValue {
    KValue::from(v)
}

/// A JSON value as a core value.
pub fn json_to_core(j: &serde_json::Value) -> Value {
    serde_json::from_value(j.clone()).unwrap_or(Value::Null)
}

/// A kernel value as a core value: nulls become `Null`, timestamps and other rich scalars their
/// JSON serialization (`json_serialize_value`).
pub fn kernel_to_core(v: &KValue) -> Value {
    v.to_core_value()
        .unwrap_or_else(|| json_to_core(&meerschaum_kernel::serialize::json_serialize_value(v)))
}

/// A core `params` dict as `query_df` params.
pub fn params_to_query(params: &Map) -> IndexMap<String, ParamValue> {
    params
        .iter()
        .map(|(k, v)| {
            let pv = match v {
                Value::List(items) => ParamValue::Many(items.iter().map(core_to_kernel).collect()),
                other => ParamValue::One(core_to_kernel(other)),
            };
            (k.clone(), pv)
        })
        .collect()
}

/// Coerce string params to the column's value type where Python callers pass rich objects
/// (UUIDs, timestamps) and pandas compares objects: a `UUID` param matches a UUID column.
pub fn coerce_params_to_frame(
    params: IndexMap<String, ParamValue>,
    df: &Frame,
) -> IndexMap<String, ParamValue> {
    let coerce = |sample: &KValue, v: KValue| -> KValue {
        match (sample, &v) {
            (KValue::Uuid(_), KValue::Str(s)) => match Uuid::parse_str(s) {
                Ok(u) => KValue::Uuid(u),
                Err(_) => v,
            },
            (KValue::Datetime(_), KValue::Str(s)) if !s.starts_with('_') => {
                match meerschaum_kernel::value::parse_datetime_value(s) {
                    Some(d) => KValue::Datetime(d),
                    None => v,
                }
            }
            _ => v,
        }
    };
    params
        .into_iter()
        .map(|(col, pv)| {
            let sample = df
                .column(&col)
                .and_then(|c| c.data.values().into_iter().find(|v| !v.is_na()));
            let Some(sample) = sample else {
                return (col, pv);
            };
            let pv = match pv {
                ParamValue::One(v) => ParamValue::One(coerce(&sample, v)),
                ParamValue::Many(vs) => {
                    ParamValue::Many(vs.into_iter().map(|v| coerce(&sample, v)).collect())
                }
            };
            (col, pv)
        })
        .collect()
}

/// `pipe.columns` as a string map.
pub fn columns_map(pipe: &Pipe) -> IndexMap<String, String> {
    pipe.columns()
        .iter()
        .map(|(k, v)| (k.clone(), meerschaum_core::pyfmt::py_str(v)))
        .collect()
}

/// `parameters['dtypes']` as a string map (the configured dtypes).
pub fn configured_dtypes(parameters: &Map) -> IndexMap<String, String> {
    match parameters.get("dtypes") {
        Some(Value::Map(m)) => m
            .iter()
            .map(|(k, v)| (k.clone(), meerschaum_core::pyfmt::py_str(v)))
            .collect(),
        _ => IndexMap::new(),
    }
}

/// `Pipe.infer_dtypes()`: the instance's columns types mapped to pandas dtypes (PostgreSQL-style
/// upper-case types through `get_pd_type_from_db_type`, others through `to_pandas_dtype`).
pub async fn infer_dtypes<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
) -> ConnectorResult<IndexMap<String, String>> {
    if !conn.pipe_exists(pipe).await? {
        return Ok(IndexMap::new());
    }
    let columns_types = conn.get_pipe_columns_types(pipe).await?;
    Ok(columns_types
        .into_iter()
        .map(|(c, t)| {
            let pd = if meerschaum_core::pyfmt::py_isupper(&t) {
                meerschaum_core::dtypes::get_pd_type_from_db_type(&t, true)
            } else {
                meerschaum_kernel::dtypes::to_pandas_dtype(&t)
            };
            (c, pd)
        })
        .collect())
}

/// `Pipe.get_dtypes(infer=...)` for the given parameters.
pub async fn get_dtypes<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    parameters: &Map,
    infer: bool,
) -> ConnectorResult<IndexMap<String, String>> {
    let remote = if infer {
        infer_dtypes(conn, pipe).await?
    } else {
        IndexMap::new()
    };
    Ok(pipe_get_dtypes(
        &configured_dtypes(parameters),
        &remote,
        &columns_map(pipe),
        bool_param(parameters, "autoincrement", false),
    ))
}

/// A boolean parameter with Python truthiness.
pub fn bool_param(parameters: &Map, key: &str, default: bool) -> bool {
    parameters.get(key).map(|v| v.truthy()).unwrap_or(default)
}

/// The context `Pipe.enforce_dtypes()` needs.
pub async fn dtypes_context<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    parameters: &Map,
) -> ConnectorResult<PipeDtypesContext> {
    let dtypes = get_dtypes(conn, pipe, parameters, true).await?;
    let explicit = get_dtypes(conn, pipe, parameters, false).await?;
    let tz_aware = pipe_is_tz_aware(
        &columns_map(pipe),
        &dtypes,
        bool_param(parameters, "autotime", false),
        "ts",
    );
    Ok(PipeDtypesContext {
        dtypes,
        explicit_dtypes: explicit,
        enforce: bool_param(parameters, "enforce", true),
        tz_aware,
        mixed_numerics: bool_param(parameters, "mixed_numerics", true),
    })
}

/// `Pipe._persist_new_special_columns(df, dtypes)`: json / uuid / numeric / bytes / geometry
/// columns of `df` that `dtypes` (`Pipe.get_dtypes()`) does not already hold as special dtypes
/// are merged into the pipe's `dtypes` parameter, persisted with `edit_pipe(patch=False)` when
/// `registered` and not `temporary` (a failure only warns), and recorded in `extra` as
/// `new_special_cols` (what `sync_pipe` reads as `pipe._get_cached_value('new_special_cols')`).
///
/// Returns the new special columns.
pub async fn persist_new_special_columns<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &mut Pipe,
    df: &Frame,
    dtypes: &IndexMap<String, String>,
    registered: bool,
    extra: &mut Map,
) -> IndexMap<String, String> {
    let new_special: IndexMap<String, String> = get_special_cols(df)
        .into_iter()
        .filter(|(c, _)| !dtypes.get(c).is_some_and(|t| is_dtype_special(t)))
        .collect();
    if new_special.is_empty() {
        return new_special;
    }
    let mut merged = pipe
        .param("dtypes")
        .and_then(|d| d.as_map().cloned())
        .unwrap_or_default();
    for (c, t) in &new_special {
        merged.insert(c.clone(), Value::Str(t.clone()));
    }
    let mut patch = Map::new();
    patch.insert("dtypes".into(), Value::Map(merged));
    pipe.update_parameters(patch);
    if registered && !pipe.temporary {
        let res = conn.edit_pipe(pipe, false).await;
        if !res.success {
            tracing::warn!(
                "Failed to capture new special columns for {pipe}:\n{}",
                res.message
            );
        }
    }
    extra.insert(
        "new_special_cols".into(),
        Value::Map(
            new_special
                .iter()
                .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
                .collect(),
        ),
    );
    new_special
}

/// The essentials of `Pipe.sync(docs, check_existing=...)` for internal pipes: enforce the
/// pipe's dtypes on the documents, then `instance.sync_pipe()`.
pub async fn sync_docs<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    docs: Vec<Doc>,
    check_existing: bool,
) -> SuccessTuple {
    let ctx = match dtypes_context(conn, pipe, pipe.parameters()).await {
        Ok(c) => c,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let df = match pipe_enforce_dtypes(PipeInput::Docs(docs), &ctx) {
        Ok(df) => df,
        Err(e) => return SuccessTuple::fail(format!("Failed to enforce dtypes:\n{e}")),
    };
    conn.sync_pipe(
        pipe,
        df,
        &SyncOptions {
            check_existing,
            ..Default::default()
        },
    )
    .await
}

/// Compare two kernel values for sorting (`sort_values`, nulls last).
pub fn compare_values(a: &KValue, b: &KValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a.is_na(), b.is_na()) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Greater,
        (false, true) => return Ordering::Less,
        _ => {}
    }
    match (a, b) {
        (KValue::Datetime(x), KValue::Datetime(y)) => x.utc.cmp(&y.utc),
        (KValue::Int(x), KValue::Int(y)) => x.cmp(y),
        (KValue::Int(x), KValue::Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (KValue::Float(x), KValue::Int(y)) => {
            x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal)
        }
        (KValue::Float(x), KValue::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (KValue::Bool(x), KValue::Bool(y)) => x.cmp(y),
        (KValue::Date(x), KValue::Date(y)) => x.cmp(y),
        (KValue::Decimal(x), KValue::Decimal(y)) => x
            .to_f64()
            .partial_cmp(&y.to_f64())
            .unwrap_or(Ordering::Equal),
        _ => a.py_str().cmp(&b.py_str()),
    }
}

/// `df.sort_values(by=cols, ascending=...)` with a stable sort, then `reset_index(drop=True)`.
pub fn sort_frame(df: &Frame, cols: &[String], ascending: bool) -> Frame {
    let keys: Vec<Vec<KValue>> = cols
        .iter()
        .filter_map(|c| df.column(c).map(|col| col.data.values()))
        .collect();
    if keys.is_empty() {
        return df.clone();
    }
    let mut perm: Vec<usize> = (0..df.height()).collect();
    perm.sort_by(|&i, &j| {
        for k in &keys {
            let (a, b) = (&k[i], &k[j]);
            let mut ord = compare_values(a, b);
            if !ascending && !a.is_na() && !b.is_na() {
                ord = ord.reverse();
            }
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
    df.take(&perm)
}

/// The post-processing `Pipe.get_data()` applies to an instance's frame: sort by the datetime
/// axis then the other index columns (`order`), then `limit`.
pub fn pipe_postprocess(pipe: &Pipe, df: Frame, query: &DataQuery) -> Frame {
    let columns = columns_map(pipe);
    let dt_col = columns.get("datetime").cloned();
    let mut indices: Vec<String> = Vec::new();
    if let Some(dt) = dt_col.as_ref().filter(|c| df.contains(c)) {
        indices.push(dt.clone());
    }
    for (role, col) in &columns {
        if role != "datetime" && df.contains(col) {
            indices.push(col.clone());
        }
    }
    let order = query.order.clone().unwrap_or_else(|| "asc".into());
    let mut out = sort_frame(&df, &indices, order.to_lowercase() == "asc");
    if let Some(limit) = query.limit {
        let limit = limit.max(0) as usize;
        if out.height() > limit {
            out = out.take(&(0..limit).collect::<Vec<_>>());
        }
    }
    out
}

/// `Pipe.get_data(...)`: the instance's frame with `Pipe`-level sorting and `limit`.
pub async fn get_data<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    query: &DataQuery,
) -> ConnectorResult<Option<Frame>> {
    Ok(conn
        .get_pipe_data(pipe, query)
        .await?
        .map(|df| pipe_postprocess(pipe, df, query)))
}

/// `Pipe.get_data(...)` as documents.
pub async fn get_docs<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    query: &DataQuery,
) -> ConnectorResult<Vec<Doc>> {
    Ok(get_data(conn, pipe, query)
        .await?
        .map(|df| frame_to_docs(&df))
        .unwrap_or_default())
}

/// `Pipe.get_value(column, params)`: the first value of `column`, or `None`.
pub async fn get_value<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    column: &str,
    params: &Map,
) -> ConnectorResult<Option<Value>> {
    let docs = get_docs(
        conn,
        pipe,
        &DataQuery {
            select_columns: Some(vec![column.to_string()]),
            params: Some(params.clone()),
            limit: Some(1),
            ..Default::default()
        },
    )
    .await?;
    Ok(docs
        .first()
        .and_then(|d| d.get(column))
        .filter(|v| !v.is_na())
        .map(kernel_to_core))
}

/// `Pipe.get_doc(params)`: the first row, or `None`.
pub async fn get_doc<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    params: &Map,
) -> ConnectorResult<Option<Doc>> {
    let docs = get_docs(
        conn,
        pipe,
        &DataQuery {
            params: Some(params.clone()),
            limit: Some(1),
            ..Default::default()
        },
    )
    .await?;
    Ok(docs.into_iter().next())
}

fn map_of(pairs: &[(&str, Value)]) -> Map {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn str_map(pairs: &[(&str, &str)]) -> Value {
    Value::Map(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Str(v.to_string())))
            .collect(),
    )
}

fn build_internal_pipe(instance_keys: &str, metric: &str, kwargs: Map) -> Pipe {
    let mut builder = Pipe::builder("mrsm", metric).instance(instance_keys);
    for (k, v) in kwargs {
        builder = builder.kwarg(&k, v);
    }
    builder.build().unwrap_or_else(|_| {
        // The keys are static and valid; fall back to a bare pipe rather than panic.
        Pipe::builder("mrsm", metric)
            .instance(instance_keys)
            .kwarg("temporary", true)
            .build()
            .expect("static internal pipe keys are valid")
    })
}

/// The parameters of the Syncing Engine's internal state pipe `mrsm:<metric_key>` (§5.8):
/// `engine_state`, `engine_ledger`, `engine_cursors`, `engine_runs`, `engine_nodes` and
/// `engine_events` (`None` for any other metric). The engine builds its state pipes from these;
/// instance connectors use them to forget a dropped pipe's state ([`forget_engine_state`]).
pub fn engine_state_parameters(metric_key: &str) -> Option<Map> {
    let json = match metric_key {
        "engine_state" => {
            r#"{"columns": {"primary": "pipe_ref"},
                    "dtypes": {"pipe_ref": "string", "definition_hash": "string", "updated": "datetime"},
                    "upsert": true}"#
        }
        "engine_ledger" => {
            r#"{"columns": {"datetime": "created", "pipe_ref": "pipe_ref", "seq": "seq"},
                    "dtypes": {"created": "datetime", "pipe_ref": "string", "seq": "int",
                               "begin": "json", "end": "json"}}"#
        }
        "engine_cursors" => {
            r#"{"columns": {"parent_ref": "parent_ref", "child_ref": "child_ref"},
                    "dtypes": {"parent_ref": "string", "child_ref": "string", "seq": "int",
                               "parent_watermark_at_cursor": "json"},
                    "upsert": true}"#
        }
        "engine_runs" => {
            r#"{"columns": {"datetime": "started", "primary": "run_id"},
                    "dtypes": {"run_id": "string", "started": "datetime", "finished": "datetime",
                               "success": "bool", "message": "string", "targets": "json",
                               "workers": "int", "num_nodes": "int", "resumed_from": "string",
                               "holder": "string", "selection": "string"},
                    "upsert": true}"#
        }
        "engine_nodes" => {
            r#"{"columns": {"datetime": "started", "run_id": "run_id", "pipe_ref": "pipe_ref"},
                    "dtypes": {"run_id": "string", "pipe_ref": "string", "action": "string",
                               "status": "string", "reason": "string", "begin": "json",
                               "end": "json", "num_chunks": "int", "attempts": "int",
                               "started": "datetime", "finished": "datetime",
                               "duration_seconds": "float", "inserted": "int", "updated": "int",
                               "upserted": "int", "deleted": "int", "message": "string",
                               "planned_start_seconds": "float", "slack_seconds": "float"},
                    "upsert": true}"#
        }
        "engine_events" => {
            r#"{"columns": {"datetime": "ts", "run_id": "run_id", "seq": "seq"},
                    "dtypes": {"ts": "datetime", "run_id": "string", "seq": "int",
                               "pipe_ref": "string", "event": "string", "payload": "json"}}"#
        }
        _ => return None,
    };
    let mut params = Value::from_json_str(json)
        .expect("valid engine pipe parameters")
        .to_map();
    params.insert("target".into(), Value::Str(format!("mrsm_{metric_key}")));
    params.insert("static".into(), Value::Bool(true));
    Some(params)
}

/// The engine state pipe `mrsm:<metric_key>` on `instance_keys` (see
/// [`engine_state_parameters`]).
pub fn engine_state_pipe(instance_keys: &str, metric_key: &str) -> Option<Pipe> {
    let parameters = engine_state_parameters(metric_key)?;
    Pipe::builder("mrsm", metric_key)
        .instance(instance_keys)
        .parameters(parameters)
        .kwarg("temporary", true)
        .kwarg("static", true)
        .kwarg("cache", false)
        .build()
        .ok()
}

/// Forget the Syncing Engine's state for `pipe` on its instance `conn`: the ledger entries it
/// produced, the edge cursors in which it is the parent or the child, and its definition hash.
/// Called when a pipe is dropped, deleted or fully cleared, so a pipe recreated under the same
/// keys starts fresh, as it does in v4 (which has no engine state). Run history is kept.
/// Engine tables that do not exist are skipped (an instance the engine never ran on pays only
/// the existence checks). Failures are reported, never raised.
pub async fn forget_engine_state<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
) -> SuccessTuple {
    if pipe.connector_keys() == "mrsm" {
        return SuccessTuple::success();
    }
    let pipe_ref = pipe.pipe_ref();
    let mut errors = Vec::new();
    for (metric, column) in [
        ("engine_ledger", "pipe_ref"),
        ("engine_cursors", "parent_ref"),
        ("engine_cursors", "child_ref"),
        ("engine_state", "pipe_ref"),
    ] {
        let Ok(Some(state_pipe)) = conn.engine_state_pipe(pipe.instance_keys(), metric).await
        else {
            continue;
        };
        if !conn.pipe_exists(&state_pipe).await.unwrap_or(false) {
            continue;
        }
        let mut params = Map::new();
        params.insert(column.into(), Value::Str(pipe_ref.clone()));
        let query = DataQuery {
            params: Some(params),
            ..Default::default()
        };
        let res = conn.clear_pipe(&state_pipe, &query).await;
        if !res.success {
            errors.push(format!("{metric} ({column}): {}", res.message));
        }
    }
    if errors.is_empty() {
        SuccessTuple::success()
    } else {
        SuccessTuple::fail(format!(
            "Could not forget the engine state of {pipe}:\n{}",
            errors.join("\n")
        ))
    }
}

/// The separator of a remote-state marker key (see [`remote_state_key`]).
pub const REMOTE_STATE_SEPARATOR: &str = "|remote|";

/// The `definition_hash` value of a remote-state marker row.
pub const REMOTE_STATE_HASH_PREFIX: &str = "remote:";

/// The key of a *remote-state marker*: a row of the home instance's `engine_state` pipe that
/// records that engine state of the pipe `pipe_ref` (its ledger entries and the edge cursors in
/// which it is the parent) was written onto `state_instance`, another instance (a cross-instance
/// child's, §5.1). A removed pipe's home instance reads the markers to find that state
/// ([`forget_remote_engine_state`], GAP-002). A marker never equals a pipe's own key, so
/// readers of the definition hash ignore it; it reuses `engine_state` so no table, column or
/// route is added.
pub fn remote_state_key(pipe_ref: &str, state_instance: &str) -> String {
    format!("{pipe_ref}{REMOTE_STATE_SEPARATOR}{state_instance}")
}

/// The instances other than the pipe's own that hold engine state of `pipe`, from the markers on
/// its home instance (`conn`). Empty when the instance has no engine state or no markers.
pub async fn remote_state_instances<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
) -> Vec<String> {
    let Ok(Some(state_pipe)) = conn
        .engine_state_pipe(pipe.instance_keys(), "engine_state")
        .await
    else {
        return Vec::new();
    };
    if !conn.pipe_exists(&state_pipe).await.unwrap_or(false) {
        return Vec::new();
    }
    let Ok(Some(frame)) = conn.get_pipe_data(&state_pipe, &DataQuery::default()).await else {
        return Vec::new();
    };
    let prefix = format!("{}{REMOTE_STATE_SEPARATOR}", pipe.pipe_ref());
    let mut out: Vec<String> = Vec::new();
    for doc in frame_to_docs(&frame) {
        let Some(KValue::Str(key)) = doc.get("pipe_ref") else {
            continue;
        };
        if let Some(instance) = key.strip_prefix(&prefix)
            && !instance.is_empty()
            && !out.iter().any(|i| i == instance)
        {
            out.push(instance.to_string());
        }
    }
    out
}

/// Forget the Syncing Engine's state of `pipe` that lives on **other instances** (GAP-002): its
/// ledger entries and the edge cursors in which it is the parent, written onto the instances of
/// its cross-instance children. The instances come from the remote-state markers on the pipe's
/// home instance `conn` ([`remote_state_instances`]); `provider` reaches them. A marker is
/// removed once its instance was cleaned. An instance that is not configured here or cannot be
/// reached is reported in the result (the stale rows would make a pipe recreated under the same
/// keys resume from a cursor of the old one) and its marker is kept, so a later removal retries.
/// The pipe's state on its own instance is [`forget_engine_state`]'s.
pub async fn forget_remote_engine_state<C: InstanceConnector + ?Sized>(
    conn: &C,
    provider: &dyn crate::template::InstanceProvider,
    pipe: &Pipe,
) -> SuccessTuple {
    if pipe.connector_keys() == "mrsm" {
        return SuccessTuple::success();
    }
    let instances = remote_state_instances(conn, pipe).await;
    if instances.is_empty() {
        return SuccessTuple::success();
    }
    let pipe_ref = pipe.pipe_ref();
    let mut errors = Vec::new();
    for instance in instances {
        let Some(remote) = provider.instance(&instance) else {
            errors.push(format!(
                "{instance}: not configured on this host, so its engine state of {pipe} was kept"
            ));
            continue;
        };
        let mut failed = false;
        for (metric, column) in [
            ("engine_ledger", "pipe_ref"),
            ("engine_cursors", "parent_ref"),
        ] {
            let Ok(Some(state_pipe)) = remote.engine_state_pipe(&instance, metric).await else {
                continue;
            };
            match remote.pipe_exists(&state_pipe).await {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    errors.push(format!("{instance}: {e}"));
                    failed = true;
                    break;
                }
            }
            let mut params = Map::new();
            params.insert(column.into(), Value::Str(pipe_ref.clone()));
            let res = remote
                .clear_pipe(
                    &state_pipe,
                    &DataQuery {
                        params: Some(params),
                        ..Default::default()
                    },
                )
                .await;
            if !res.success {
                errors.push(format!("{instance}: {metric} ({column}): {}", res.message));
                failed = true;
            }
        }
        if failed {
            continue;
        }
        // The instance is clean: drop its marker.
        if let Ok(Some(state_pipe)) = conn
            .engine_state_pipe(pipe.instance_keys(), "engine_state")
            .await
        {
            let mut params = Map::new();
            params.insert(
                "pipe_ref".into(),
                Value::Str(remote_state_key(&pipe_ref, &instance)),
            );
            let res = conn
                .clear_pipe(
                    &state_pipe,
                    &DataQuery {
                        params: Some(params),
                        ..Default::default()
                    },
                )
                .await;
            if !res.success {
                errors.push(format!("{instance}: marker: {}", res.message));
            }
        }
    }
    if errors.is_empty() {
        SuccessTuple::success()
    } else {
        SuccessTuple::fail(format!(
            "Could not forget the engine state of {pipe} on other instances:\n{}",
            errors.join("\n")
        ))
    }
}

/// Whether a `clear_pipe` query removes every row (no bounds, no filter): a full clear forgets
/// the pipe's engine state like a drop does.
pub fn is_full_clear(query: &DataQuery) -> bool {
    query.begin.is_none() && query.end.is_none() && query.params.as_ref().is_none_or(Map::is_empty)
}

/// `InstanceConnector.get_users_pipe()`.
pub fn users_pipe(instance_keys: &str) -> Pipe {
    build_internal_pipe(
        instance_keys,
        "users",
        map_of(&[
            ("target", Value::Str("mrsm_users".into())),
            ("temporary", Value::Bool(true)),
            ("cache", Value::Bool(true)),
            ("static", Value::Bool(true)),
            ("null_indices", Value::Bool(false)),
            ("columns", str_map(&[("primary", "user_id")])),
            (
                "dtypes",
                str_map(&[
                    ("user_id", "uuid"),
                    ("username", "string"),
                    ("password_hash", "string"),
                    ("email", "string"),
                    ("user_type", "string"),
                    ("attributes", "json"),
                ]),
            ),
            ("indices", str_map(&[("unique", "username")])),
        ]),
    )
}

/// `InstanceConnector.get_plugins_pipe()` (`user_id_dtype` from the users pipe).
pub fn plugins_pipe(instance_keys: &str, user_id_dtype: &str) -> Pipe {
    build_internal_pipe(
        instance_keys,
        "plugins",
        map_of(&[
            ("target", Value::Str("mrsm_plugins".into())),
            ("temporary", Value::Bool(true)),
            ("cache", Value::Bool(true)),
            ("static", Value::Bool(true)),
            ("null_indices", Value::Bool(false)),
            (
                "columns",
                str_map(&[("primary", "plugin_name"), ("user_id", "user_id")]),
            ),
            (
                "dtypes",
                str_map(&[
                    ("plugin_name", "string"),
                    ("user_id", user_id_dtype),
                    ("attributes", "json"),
                    ("version", "string"),
                ]),
            ),
        ]),
    )
}

/// `InstanceConnector.get_tokens_pipe()` (`user_id_dtype` from the users pipe).
pub fn tokens_pipe(instance_keys: &str, user_id_dtype: &str) -> Pipe {
    build_internal_pipe(
        instance_keys,
        "tokens",
        map_of(&[
            ("target", Value::Str("mrsm_tokens".into())),
            ("temporary", Value::Bool(true)),
            ("cache", Value::Bool(true)),
            ("static", Value::Bool(true)),
            ("autotime", Value::Bool(true)),
            ("null_indices", Value::Bool(false)),
            (
                "columns",
                str_map(&[("datetime", "creation"), ("primary", "id")]),
            ),
            (
                "indices",
                str_map(&[("unique", "label"), ("user_id", "user_id")]),
            ),
            (
                "dtypes",
                str_map(&[
                    ("id", "uuid"),
                    ("creation", "datetime"),
                    ("expiration", "datetime"),
                    ("is_valid", "bool"),
                    ("label", "string"),
                    ("user_id", user_id_dtype),
                    ("scopes", "json"),
                    ("secret_hash", "string"),
                ]),
            ),
        ]),
    )
}

/// A UTC timestamp as a kernel value.
pub fn utc_value(dt: DateTime<Utc>) -> KValue {
    KValue::Datetime(DateTimeValue::utc(dt.naive_utc()))
}

/// A UUID-or-string user ID as a kernel value.
fn user_id_value(v: &Value) -> KValue {
    match v {
        Value::Str(s) => match Uuid::parse_str(s) {
            Ok(u) => KValue::Uuid(u),
            Err(_) => KValue::Str(s.clone()),
        },
        other => core_to_kernel(other),
    }
}

/// `user.user_id if user.user_id is not None else self.get_user_id(user)`.
pub async fn resolve_user_id<C: InstanceConnector + ?Sized>(
    conn: &C,
    user: &User,
) -> ConnectorResult<Option<Value>> {
    match &user.user_id {
        Some(id) if !id.is_null() => Ok(Some(id.clone())),
        _ => conn.get_user_id(user).await,
    }
}

/// `get_data([field], params={'user_id': user_id})[field][0]`.
pub async fn user_field<C: InstanceConnector + ?Sized>(
    conn: &C,
    user: &User,
    field: &str,
) -> ConnectorResult<Option<Value>> {
    let user_id = resolve_user_id(conn, user).await?;
    let pipe = conn.get_users_pipe();
    let mut params = Map::new();
    params.insert("user_id".into(), user_id.unwrap_or(Value::Null));
    get_value(conn, &pipe, field, &params).await
}

/// `InstanceConnector.register_user()`.
pub async fn default_register_user<C: InstanceConnector + ?Sized>(
    conn: &C,
    user: &mut User,
) -> SuccessTuple {
    let pipe = conn.get_users_pipe();
    let user_id = Uuid::new_v4();
    user.user_id = Some(Value::Str(user_id.to_string()));
    let password_hash = match user.get_password_hash() {
        Ok(h) => h,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let attributes = Value::Map(user.attributes.clone().unwrap_or_default());
    let doc: Doc = [
        ("user_id".to_string(), KValue::Uuid(user_id)),
        ("username".to_string(), KValue::Str(user.username.clone())),
        (
            "email".to_string(),
            user.email.clone().map(KValue::Str).unwrap_or(KValue::Null),
        ),
        ("password_hash".to_string(), KValue::Str(password_hash)),
        (
            "user_type".to_string(),
            user.user_type
                .clone()
                .map(KValue::Str)
                .unwrap_or(KValue::Null),
        ),
        ("attributes".to_string(), core_to_kernel(&attributes)),
    ]
    .into_iter()
    .collect();
    let res = sync_docs(conn, &pipe, vec![doc], false).await;
    if !res.success {
        return SuccessTuple::fail(format!(
            "Failed to register user '{}':\n{}",
            user.username, res.message
        ));
    }
    SuccessTuple::success()
}

/// `InstanceConnector.edit_user()`.
pub async fn default_edit_user<C: InstanceConnector + ?Sized>(
    conn: &C,
    user: &User,
) -> SuccessTuple {
    let pipe = conn.get_users_pipe();
    let user_id = match resolve_user_id(conn, user).await {
        Ok(id) => id.unwrap_or(Value::Null),
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let mut doc: Doc = Doc::new();
    doc.insert("user_id".into(), user_id_value(&user_id));
    if let Some(email) = user.email.as_ref().filter(|e| !e.is_empty()) {
        doc.insert("email".into(), KValue::Str(email.clone()));
    }
    if let Some(h) = user.password_hash.as_ref().filter(|h| !h.is_empty()) {
        doc.insert("password_hash".into(), KValue::Str(h.clone()));
    } else if !user.password.is_empty() {
        let mut u = user.clone();
        if let Ok(h) = u.get_password_hash() {
            doc.insert("password_hash".into(), KValue::Str(h));
        }
    }
    if let Some(t) = user.user_type.as_ref().filter(|t| !t.is_empty()) {
        doc.insert("user_type".into(), KValue::Str(t.clone()));
    }
    if let Some(attrs) = user.attributes.as_ref().filter(|a| !a.is_empty()) {
        doc.insert(
            "attributes".into(),
            core_to_kernel(&Value::Map(attrs.clone())),
        );
    }
    let res = sync_docs(conn, &pipe, vec![doc], true).await;
    if !res.success {
        return SuccessTuple::fail(format!(
            "Failed to edit user '{}':\n{}",
            user.username, res.message
        ));
    }
    SuccessTuple::success()
}

/// `plugins_pipe.get_value(field, {'plugin_name': plugin.name})`.
pub async fn plugin_field<C: InstanceConnector + ?Sized>(
    conn: &C,
    plugin: &PluginRecord,
    field: &str,
) -> ConnectorResult<Option<Value>> {
    let pipe = conn.get_plugins_pipe();
    let mut params = Map::new();
    params.insert("plugin_name".into(), Value::Str(plugin.name.clone()));
    get_value(conn, &pipe, field, &params).await
}

/// `InstanceConnector.register_plugin()`.
pub async fn default_register_plugin<C: InstanceConnector + ?Sized>(
    conn: &C,
    plugin: &PluginRecord,
) -> SuccessTuple {
    match conn.get_plugin_user_id(plugin).await {
        Ok(Some(user_id)) => {
            let username = conn
                .get_username(&user_id)
                .await
                .ok()
                .flatten()
                .map(|v| meerschaum_core::pyfmt::py_str(&v))
                .unwrap_or_else(|| "None".into());
            return SuccessTuple::fail(format!("{plugin} is already registered to '{username}'."));
        }
        Ok(None) => {}
        Err(e) => return SuccessTuple::fail(e.to_string()),
    }
    let pipe = conn.get_plugins_pipe();
    let doc: Doc = [
        ("plugin_name".to_string(), KValue::Str(plugin.name.clone())),
        (
            "version".to_string(),
            plugin
                .version
                .clone()
                .map(KValue::Str)
                .unwrap_or(KValue::Null),
        ),
        (
            "attributes".to_string(),
            core_to_kernel(&Value::Map(plugin.attributes.clone())),
        ),
        (
            "user_id".to_string(),
            plugin
                .user_id
                .as_ref()
                .map(user_id_value)
                .unwrap_or(KValue::Null),
        ),
    ]
    .into_iter()
    .collect();
    let res = sync_docs(conn, &pipe, vec![doc], false).await;
    if !res.success {
        return SuccessTuple::fail(format!("Failed to register {plugin}:\n{}", res.message));
    }
    SuccessTuple::success()
}

/// `tokens_pipe.get_value(field, params={'id': token_id})`.
pub async fn token_field<C: InstanceConnector + ?Sized>(
    conn: &C,
    token_id: &Uuid,
    field: &str,
) -> ConnectorResult<Option<Value>> {
    let pipe = conn.get_tokens_pipe();
    let mut params = Map::new();
    params.insert("id".into(), Value::Str(token_id.to_string()));
    get_value(conn, &pipe, field, &params).await
}

/// `InstanceConnector.register_token()`.
pub async fn default_register_token<C: InstanceConnector + ?Sized>(
    conn: &C,
    token: &mut Token,
) -> SuccessTuple {
    let (token_id, secret) = match token.generate_credentials() {
        Ok(c) => c,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let user_id = match &token.user {
        Some(user) => match conn.get_user_id(user).await {
            Ok(id) => id,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        },
        None => None,
    };
    let Some(user_id) = user_id else {
        return SuccessTuple::fail("Cannot register a token without a user.");
    };
    let secret_hash = match Token::hash_secret(&secret) {
        Ok(h) => h,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let now = Utc::now();
    token.creation = Some(now);
    let doc: Doc = [
        ("id".to_string(), KValue::Uuid(token_id)),
        ("user_id".to_string(), user_id_value(&user_id)),
        ("creation".to_string(), utc_value(now)),
        (
            "expiration".to_string(),
            token.expiration.map(utc_value).unwrap_or(KValue::Null),
        ),
        (
            "label".to_string(),
            token.label.clone().map(KValue::Str).unwrap_or(KValue::Null),
        ),
        ("is_valid".to_string(), KValue::Bool(token.is_valid)),
        (
            "scopes".to_string(),
            KValue::Json(serde_json::Value::Array(
                token
                    .scopes
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            )),
        ),
        ("secret_hash".to_string(), KValue::Str(secret_hash)),
    ]
    .into_iter()
    .collect();
    let pipe = conn.get_tokens_pipe();
    let res = sync_docs(conn, &pipe, vec![doc], false).await;
    if !res.success {
        return SuccessTuple::fail(format!("Failed to register token:\n{}", res.message));
    }
    SuccessTuple::success()
}

/// `InstanceConnector.edit_token()` / `invalidate_token()`.
pub async fn default_edit_token<C: InstanceConnector + ?Sized>(
    conn: &C,
    token: &Token,
    invalidate: bool,
) -> SuccessTuple {
    let Some(token_id) = token.id else {
        return SuccessTuple::fail("Token ID is not set.");
    };
    match conn.token_exists(&token_id).await {
        Ok(true) => {}
        Ok(false) => return SuccessTuple::fail(format!("Token {token_id} does not exist.")),
        Err(e) => return SuccessTuple::fail(e.to_string()),
    }
    let creation = match token.creation {
        Some(c) => Some(c),
        None => conn
            .get_token_model(&token_id)
            .await
            .ok()
            .flatten()
            .and_then(|m| m.creation),
    };
    let mut doc: Doc = Doc::new();
    doc.insert("id".into(), KValue::Uuid(token_id));
    doc.insert(
        "creation".into(),
        creation.map(utc_value).unwrap_or(KValue::Null),
    );
    if invalidate {
        doc.insert("is_valid".into(), KValue::Bool(false));
    } else {
        doc.insert(
            "expiration".into(),
            token.expiration.map(utc_value).unwrap_or(KValue::Null),
        );
        doc.insert(
            "label".into(),
            token.label.clone().map(KValue::Str).unwrap_or(KValue::Null),
        );
        doc.insert("is_valid".into(), KValue::Bool(token.is_valid));
        doc.insert(
            "scopes".into(),
            KValue::Json(serde_json::Value::Array(
                token
                    .scopes
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            )),
        );
    }
    let pipe = conn.get_tokens_pipe();
    let res = sync_docs(conn, &pipe, vec![doc], true).await;
    if !res.success {
        let verb = if invalidate { "invalidate" } else { "edit" };
        return SuccessTuple::fail(format!(
            "Failed to {verb} token '{token_id}':\n{}",
            res.message
        ));
    }
    SuccessTuple::success()
}

/// `InstanceConnector.delete_token()`.
pub async fn default_delete_token<C: InstanceConnector + ?Sized>(
    conn: &C,
    token: &Token,
) -> SuccessTuple {
    let Some(token_id) = token.id else {
        return SuccessTuple::fail("Token ID is not set.");
    };
    match conn.token_exists(&token_id).await {
        Ok(true) => {}
        Ok(false) => return SuccessTuple::fail(format!("Token {token_id} does not exist.")),
        Err(e) => return SuccessTuple::fail(e.to_string()),
    }
    let pipe = conn.get_tokens_pipe();
    let mut params = Map::new();
    params.insert("id".into(), Value::Str(token_id.to_string()));
    let res = conn
        .clear_pipe(
            &pipe,
            &DataQuery {
                params: Some(params),
                ..Default::default()
            },
        )
        .await;
    if !res.success {
        return SuccessTuple::fail(format!(
            "Failed to delete token '{token_id}':\n{}",
            res.message
        ));
    }
    SuccessTuple::success()
}

/// A token document as a [`TokenModel`].
pub fn token_model_from_doc(doc: &Doc) -> TokenModel {
    let get = |k: &str| doc.get(k).filter(|v| !v.is_na());
    let dt = |k: &str| match get(k) {
        Some(KValue::Datetime(d)) => Some(DateTime::<Utc>::from_naive_utc_and_offset(d.utc, Utc)),
        Some(KValue::Str(s)) => meerschaum_kernel::value::parse_datetime_value(s)
            .map(|d| DateTime::<Utc>::from_naive_utc_and_offset(d.utc, Utc)),
        _ => None,
    };
    TokenModel {
        id: match get("id") {
            Some(KValue::Uuid(u)) => Some(*u),
            Some(KValue::Str(s)) => Uuid::parse_str(s).ok(),
            _ => None,
        },
        creation: dt("creation"),
        expiration: dt("expiration"),
        label: get("label").map(|v| v.py_str()),
        user_id: get("user_id").map(kernel_to_core),
        secret_hash: get("secret_hash").map(|v| v.py_str()),
        scopes: match get("scopes").map(kernel_to_core) {
            Some(Value::List(items)) => items.iter().map(meerschaum_core::pyfmt::py_str).collect(),
            Some(Value::Str(s)) => match Value::from_json_str(&s) {
                Ok(Value::List(items)) => {
                    items.iter().map(meerschaum_core::pyfmt::py_str).collect()
                }
                _ => Vec::new(),
            },
            _ => crate::models::all_scopes(),
        },
        is_valid: match get("is_valid") {
            Some(KValue::Bool(b)) => *b,
            Some(v) => v.py_str() == "True",
            None => true,
        },
    }
}

/// `InstanceConnector.get_tokens()`.
pub async fn default_get_tokens<C: InstanceConnector + ?Sized>(
    conn: &C,
    user: Option<&User>,
    labels: Option<&[String]>,
    ids: Option<&[Uuid]>,
) -> ConnectorResult<Vec<TokenModel>> {
    let pipe = conn.get_tokens_pipe();
    let user_id = match user {
        Some(u) => conn.get_user_id(u).await?,
        None => None,
    };
    let user_type = match user {
        Some(u) => conn.get_user_type(u).await?,
        None => None,
    };
    let mut params = Map::new();
    if let Some(uid) = user_id {
        if user_type.as_deref() == Some("admin") {
            params.insert("user_id".into(), Value::List(vec![uid, Value::Null]));
        } else {
            params.insert("user_id".into(), uid);
        }
    }
    if let Some(labels) = labels.filter(|l| !l.is_empty()) {
        params.insert(
            "label".into(),
            Value::List(labels.iter().map(|l| Value::Str(l.clone())).collect()),
        );
    }
    if let Some(ids) = ids.filter(|i| !i.is_empty()) {
        params.insert(
            "id".into(),
            Value::List(ids.iter().map(|i| Value::Str(i.to_string())).collect()),
        );
    }
    let docs = get_docs(
        conn,
        &pipe,
        &DataQuery {
            params: Some(params),
            ..Default::default()
        },
    )
    .await?;
    Ok(docs.iter().rev().map(token_model_from_doc).collect())
}

/// `InstanceConnector.get_token_model()`.
pub async fn default_get_token_model<C: InstanceConnector + ?Sized>(
    conn: &C,
    token_id: &Uuid,
) -> ConnectorResult<Option<TokenModel>> {
    let pipe = conn.get_tokens_pipe();
    let mut params = Map::new();
    params.insert("id".into(), Value::Str(token_id.to_string()));
    Ok(get_doc(conn, &pipe, &params)
        .await?
        .map(|d| token_model_from_doc(&d)))
}
