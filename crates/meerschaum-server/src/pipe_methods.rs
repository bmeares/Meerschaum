//! The `Pipe` methods the routes call (`meerschaum/core/Pipe/_data.py`, `_sync.py`), written over
//! `Arc<dyn InstanceConnector>`: `parse_date_bounds`, `get_sync_time`, `get_chunk_bounds`,
//! `get_rowcount`, and `sync` for the JSON payloads `POST .../data` accepts.

use std::sync::Arc;

use chrono::TimeDelta;
use indexmap::IndexMap;
use meerschaum_connector::replace::WindowBound;
use meerschaum_connector::{DataQuery, DateBound, InstanceConnector, SyncOptions, pipe_ops};
use meerschaum_core::pipe::PipeDefaults;
use meerschaum_core::pipe::chunk::{ChunkInterval, ChunkIntervalArg};
use meerschaum_core::{Map, Pipe, SuccessTuple};
use meerschaum_kernel::cast::{RoundTo, coerce_datetime, datetime_to_int, round_time};
use meerschaum_kernel::enforce::{PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::frame::json_to_value;
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};
use serde_json::Value as Json;

use crate::error::ApiError;

/// The datetime column and its dtype (`pipe.get_dtypes().get(dt_col, 'datetime')`).
pub async fn dt_info(conn: &Arc<dyn InstanceConnector>, pipe: &Pipe) -> (Option<String>, String) {
    let dt_col = pipe.column("datetime");
    let dtypes = pipe_ops::get_dtypes(conn.as_ref(), pipe, pipe.parameters(), true)
        .await
        .unwrap_or_default();
    let typ = dt_col
        .as_ref()
        .and_then(|c| dtypes.get(c).cloned())
        .unwrap_or_else(|| "datetime".into());
    (dt_col, typ)
}

fn is_int_dtype(typ: &str) -> bool {
    meerschaum_core::dtypes::are_dtypes_equal(typ, "int")
}

/// `coerce_timezone(dt, strip_utc='utc' not in dt_typ.lower())` (`'datetime'` is
/// `datetime64[ns, UTC]`).
fn coerce_for(dt: &DateTimeValue, dt_typ: &str) -> DateTimeValue {
    let typ = if dt_typ == "datetime" {
        "datetime64[ns, UTC]"
    } else {
        dt_typ
    };
    coerce_datetime(dt, !typ.to_lowercase().contains("utc"))
}

/// `Pipe.parse_date_bounds(value)` for a datetime value.
fn coerce_datetime_bound(
    pipe: &Pipe,
    dt: DateTimeValue,
    dt_typ: &str,
    defaults: &PipeDefaults,
) -> Result<DateBound, ApiError> {
    if is_int_dtype(dt_typ) {
        if pipe.param("precision").is_some_and(|p| p.truthy()) {
            let unit = pipe
                .precision(defaults)
                .map_err(|e| ApiError::exception("Invalid precision", e))?
                .unit;
            return datetime_to_int(&KValue::Datetime(dt), unit.name())
                .map(DateBound::Int)
                .map_err(|e| ApiError::exception("Failed to convert a bound", e));
        }
        // PARITY: `error(...)` raises `ValueError`, a 500 (`core/Pipe/_data.py:1044`).
        return Err(ApiError::exception(
            "parse_date_bounds",
            format!(
                "Cannot use datetime bound '{}' on the non-epoch integer axis of {pipe}.",
                dt.isoformat()
            ),
        ));
    }
    Ok(DateBound::Datetime(coerce_for(&dt, dt_typ)))
}

/// `Pipe.parse_date_bounds(raw)` for a query-string bound (`Union[str, int, None]`): integers
/// pass through, strings are parsed as timestamps and coerced to the axis' timezone.
/// Unparseable strings become `None` (Python warns).
pub async fn parse_date_bound(
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    raw: Option<&str>,
    defaults: &PipeDefaults,
) -> Result<Option<DateBound>, ApiError> {
    let Some(raw) = raw else { return Ok(None) };
    if raw.is_empty() {
        return Ok(None);
    }
    if meerschaum_core::pyfmt::is_int(raw) {
        return Ok(meerschaum_core::pyfmt::parse_py_int(raw).map(DateBound::Int));
    }
    let Some(dt) = meerschaum_kernel::value::parse_datetime_value(raw.trim()) else {
        tracing::warn!("Could not parse '{raw}' as datetime.");
        return Ok(None);
    };
    let (_, dt_typ) = dt_info(conn, pipe).await;
    coerce_datetime_bound(pipe, dt, &dt_typ, defaults).map(Some)
}

/// `Pipe.parse_date_bounds(bound)` for an instance's bound.
pub async fn coerce_bound(
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    bound: Option<DateBound>,
    defaults: &PipeDefaults,
) -> Result<Option<DateBound>, ApiError> {
    match bound {
        Some(DateBound::Datetime(dt)) => {
            let (_, dt_typ) = dt_info(conn, pipe).await;
            coerce_datetime_bound(pipe, dt, &dt_typ, defaults).map(Some)
        }
        other => Ok(other),
    }
}

/// `Pipe.get_sync_time(params, newest, round_down)`.
pub async fn get_sync_time(
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    params: Option<&Map>,
    newest: bool,
    round_down: bool,
    defaults: &PipeDefaults,
) -> Result<Option<DateBound>, ApiError> {
    if pipe.column("datetime").is_none() {
        return Ok(None);
    }
    let sync_time = conn
        .get_sync_time(pipe, params, newest)
        .await
        .map_err(|e| ApiError::exception("get_sync_time", e))?;
    let sync_time = match sync_time {
        Some(DateBound::Datetime(dt)) if round_down => Some(DateBound::Datetime(round_time(
            &dt,
            60_000_000,
            RoundTo::Down,
        ))),
        other => other,
    };
    coerce_bound(conn, pipe, sync_time, defaults).await
}

/// `Pipe.get_rowcount(begin, end, params, remote, estimate)` (`0` when unknown). With
/// `estimate`, the database's row estimate where it keeps one (catalog statistics, no scan),
/// else the exact count (`InstanceConnector::get_pipe_rowcount_estimate`). A count that fails
/// warns and is `0` ([`try_get_rowcount`] reports it).
pub async fn get_rowcount(
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    begin: Option<DateBound>,
    end: Option<DateBound>,
    params: Option<Map>,
    remote: bool,
    estimate: bool,
) -> i64 {
    try_get_rowcount(conn, pipe, begin, end, params, remote, estimate)
        .await
        .unwrap_or(0)
}

/// [`get_rowcount`], with a count the instance fails to read as a `500` (rc15 Q17: the
/// `GET .../rowcount` route, so `api:` clients raise instead of counting an outage as `0`). A
/// pipe without a table counts `0`.
pub async fn try_get_rowcount(
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    begin: Option<DateBound>,
    end: Option<DateBound>,
    params: Option<Map>,
    remote: bool,
    estimate: bool,
) -> Result<i64, ApiError> {
    let query = DataQuery {
        begin,
        end,
        params,
        remote,
        ..Default::default()
    };
    let count = if estimate {
        conn.get_pipe_rowcount_estimate(pipe, &query).await
    } else {
        conn.get_pipe_rowcount(pipe, &query).await
    };
    count
        .map(|n| n.unwrap_or(0))
        .map_err(|e| ApiError::exception(&format!("Failed to get a rowcount for {pipe}"), e))
}

fn add_interval(b: &DateBound, interval: &ChunkInterval) -> DateBound {
    match (b, interval) {
        (DateBound::Int(i), ChunkInterval::Int(n)) => DateBound::Int(i + n),
        (DateBound::Int(i), ChunkInterval::Delta(d)) => DateBound::Int(i + d.num_minutes()),
        (DateBound::Datetime(dt), ChunkInterval::Delta(d)) => DateBound::Datetime(DateTimeValue {
            utc: dt.utc + *d,
            tz: dt.tz.clone(),
        }),
        (DateBound::Datetime(dt), ChunkInterval::Int(n)) => DateBound::Datetime(DateTimeValue {
            utc: dt.utc + TimeDelta::minutes(*n),
            tz: dt.tz.clone(),
        }),
    }
}

fn bound_lt(a: &DateBound, b: &DateBound) -> bool {
    match (a, b) {
        (DateBound::Int(x), DateBound::Int(y)) => x < y,
        (DateBound::Datetime(x), DateBound::Datetime(y)) => x.utc < y.utc,
        _ => false,
    }
}

/// A chunk's `(begin, end)`.
pub type ChunkBounds = (Option<DateBound>, Option<DateBound>);

/// `Pipe.get_chunk_bounds(begin, end, bounded, chunk_interval)` (`align=False`).
pub async fn get_chunk_bounds(
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    begin: Option<DateBound>,
    end: Option<DateBound>,
    bounded: bool,
    chunk_interval: Option<ChunkIntervalArg>,
    defaults: &PipeDefaults,
) -> Result<Vec<ChunkBounds>, ApiError> {
    let include_less_than_begin = !bounded && begin.is_none();
    let include_greater_than_end = !bounded && end.is_none();
    let mut begin = match begin {
        Some(b) => Some(b),
        None => get_sync_time(conn, pipe, None, false, false, defaults).await?,
    };
    let mut consolidate_end_chunk = false;
    let mut end = match end {
        Some(e) => Some(e),
        None => {
            let e = get_sync_time(conn, pipe, None, true, false, defaults).await?;
            consolidate_end_chunk = e.is_some();
            e.map(|e| match e {
                DateBound::Datetime(dt) => DateBound::Datetime(DateTimeValue {
                    utc: dt.utc + TimeDelta::minutes(1),
                    tz: dt.tz,
                }),
                DateBound::Int(i) => DateBound::Int(i + 1),
            })
        }
    };
    if begin.is_none() && end.is_none() {
        return Ok(vec![(None, None)]);
    }
    begin = coerce_bound(conn, pipe, begin, defaults).await?;
    end = coerce_bound(conn, pipe, end, defaults).await?;

    if let (Some(b), Some(e)) = (&begin, &end)
        && !bound_lt(b, e)
    {
        return Ok(if bounded {
            vec![(Some(b.clone()), Some(b.clone()))]
        } else {
            vec![(Some(b.clone()), None)]
        });
    }

    let interval = pipe
        .get_chunk_interval(chunk_interval, defaults)
        .map_err(|e| ApiError::exception("get_chunk_interval", e))?;

    let (Some(b), Some(e)) = (begin.clone(), end.clone()) else {
        // PARITY: Python compares `None < end` here and raises `TypeError` (a 500).
        return Err(ApiError::exception(
            "get_chunk_bounds",
            "'<' not supported between instances of 'NoneType' and a bound",
        ));
    };
    let mut chunks: Vec<(DateBound, DateBound)> = Vec::new();
    let mut cursor = b.clone();
    while bound_lt(&cursor, &e) {
        let next = add_interval(&cursor, &interval);
        chunks.push((cursor, next.clone()));
        cursor = next;
        if chunks.len() >= 1_000_000 {
            return Err(ApiError::exception("get_chunk_bounds", "Too many chunks."));
        }
    }
    if chunks.len() > 1 && consolidate_end_chunk {
        let last = chunks.pop().expect("len > 1");
        let second_last = chunks.pop().expect("len > 1");
        chunks.push((second_last.0, last.1));
    }
    if chunks.is_empty() {
        chunks.push((b.clone(), e.clone()));
    }
    if let Some(last) = chunks.last_mut()
        && bound_lt(&e, &last.1)
    {
        last.1 = e.clone();
    }
    if chunks.last().is_some_and(|(x, y)| x == y) {
        chunks.pop();
    }
    let mut out: Vec<ChunkBounds> = chunks
        .into_iter()
        .map(|(x, y)| (Some(x), Some(y)))
        .collect();
    if include_less_than_begin {
        out.insert(0, (None, Some(b)));
    }
    if include_greater_than_end {
        out.push((Some(e), None));
    }
    Ok(out)
}

/// The data `POST .../data` received, after JSON / simple-lines parsing.
#[derive(Debug, Clone)]
pub enum SyncPayload {
    /// `json.loads(body)`.
    Json(Json),
    /// Lines of `string_to_dict` documents.
    Docs(Vec<Map>),
}

fn json_doc(o: &serde_json::Map<String, Json>) -> IndexMap<String, KValue> {
    o.iter()
        .map(|(k, v)| (k.clone(), json_to_value(v.clone())))
        .collect()
}

/// Convert a payload into the frame input of `Pipe.enforce_dtypes`: a list of documents, a
/// dict of lists, or a dict of `{index: value}` dicts (`to_json(orient='columns')`).
pub fn payload_input(payload: &SyncPayload) -> Option<PipeInput> {
    match payload {
        SyncPayload::Docs(docs) => Some(PipeInput::Docs(
            docs.iter()
                .map(|d| {
                    d.iter()
                        .map(|(k, v)| (k.clone(), KValue::from(v)))
                        .collect()
                })
                .collect(),
        )),
        SyncPayload::Json(Json::Array(items)) => Some(PipeInput::Docs(
            items
                .iter()
                .filter_map(|i| i.as_object().map(json_doc))
                .collect(),
        )),
        SyncPayload::Json(Json::Object(o)) => {
            if o.values().all(Json::is_object) {
                // `pd.DataFrame({col: {index: value}})`: align on the union of the indices.
                let mut index: Vec<String> = Vec::new();
                for v in o.values() {
                    for k in v.as_object().into_iter().flat_map(|m| m.keys()) {
                        if !index.contains(k) {
                            index.push(k.clone());
                        }
                    }
                }
                let cols = o
                    .iter()
                    .map(|(c, v)| {
                        let m = v.as_object();
                        let vals = index
                            .iter()
                            .map(|i| {
                                m.and_then(|m| m.get(i))
                                    .cloned()
                                    .map(json_to_value)
                                    .unwrap_or(KValue::Null)
                            })
                            .collect();
                        (c.clone(), vals)
                    })
                    .collect();
                Some(PipeInput::Columns(cols))
            } else if o.values().all(Json::is_array) {
                Some(PipeInput::Columns(
                    o.iter()
                        .map(|(c, v)| {
                            (
                                c.clone(),
                                v.as_array()
                                    .map(|a| a.iter().cloned().map(json_to_value).collect())
                                    .unwrap_or_default(),
                            )
                        })
                        .collect(),
                ))
            } else {
                Some(PipeInput::Docs(vec![json_doc(o)]))
            }
        }
        SyncPayload::Json(_) => None,
    }
}

/// Keyword arguments of `Pipe.sync()` the route forwards.
#[derive(Debug, Clone)]
pub struct SyncKwargs {
    /// `check_existing`.
    pub check_existing: bool,
    /// `force`: retry until success (`retries` = 10, `min_seconds` = 1).
    pub force: bool,
    /// `workers`.
    pub workers: Option<i64>,
    /// `replace` (rc15): the rows replace the pipe's rows of `[begin, end)` matching `params`
    /// ([`SyncOptions::replace`]).
    pub replace: bool,
    /// The replaced window's `begin`: not given (the rows' earliest datetime), unbounded, or a
    /// bound ([`meerschaum_connector::replace::resolve_window`], the rule `pipe.sync` uses).
    pub begin: WindowBound,
    /// The replaced window's `end` (not given: just after the rows' latest datetime).
    pub end: WindowBound,
    /// The replaced window's `params`.
    pub params: Option<Map>,
}

/// `Pipe.sync(df, ...)` for a received payload: enforce the pipe's dtypes, add the `autotime`
/// column, then `instance.sync_pipe()` (retrying with `force`).
pub async fn sync(
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    payload: &SyncPayload,
    kw: &SyncKwargs,
    defaults: &PipeDefaults,
) -> SuccessTuple {
    let Some(input) = payload_input(payload) else {
        return SuccessTuple::fail(format!("Cannot sync the given data to {pipe}."));
    };
    let mut pipe = pipe.clone();
    let pipe = &mut pipe;
    // `if not p.temporary and p.id is None: p.register()`: 'already' is not a failure.
    if !pipe.temporary {
        match conn.get_pipe_id(pipe).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                let reg = conn.register_pipe(pipe).await;
                if !reg.success && !reg.message.contains("already") {
                    return reg;
                }
            }
            Err(e) => return SuccessTuple::fail(e.to_string()),
        }
    }
    // `p.validate_indices()`.
    let valid = pipe.validate_indices();
    if !valid.success {
        return valid;
    }
    let ctx = match pipe_ops::dtypes_context(conn.as_ref(), pipe, pipe.parameters()).await {
        Ok(c) => c,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let mut df = match pipe_enforce_dtypes(input, &ctx) {
        Ok(df) => df,
        Err(e) => return SuccessTuple::fail(format!("Failed to enforce dtypes:\n{e}")),
    };
    let mut check_existing = kw.check_existing;
    if pipe.autotime() {
        let dt_col = pipe.column("datetime");
        let ts_col = dt_col
            .clone()
            .unwrap_or_else(|| defaults.autotime_column.clone());
        if !df.contains(&ts_col) {
            let now = chrono::Utc::now().naive_utc();
            let ts_typ = ctx
                .dtypes
                .get(&ts_col)
                .cloned()
                .unwrap_or_else(|| "datetime".into());
            let value = if is_int_dtype(&ts_typ) {
                let unit = pipe
                    .precision(defaults)
                    .map(|p| p.unit.name())
                    .unwrap_or("second");
                datetime_to_int(&KValue::Datetime(DateTimeValue::utc(now)), unit)
                    .map(KValue::Int)
                    .unwrap_or(KValue::Null)
            } else {
                KValue::Datetime(DateTimeValue::utc(now))
            };
            let n = df.height();
            let col = meerschaum_kernel::frame::Column::object(ts_col.clone(), vec![value; n]);
            if let Err(e) = df.set_column(col) {
                return SuccessTuple::fail(e.to_string());
            }
            check_existing = dt_col.is_some();
        }
    }
    let mut extra = Map::new();
    // `_persist_new_special_columns`.
    let registered = !pipe.temporary;
    pipe_ops::persist_new_special_columns(
        conn.as_ref(),
        pipe,
        &df,
        &ctx.dtypes,
        registered,
        &mut extra,
    )
    .await;
    if let Some(w) = kw.workers {
        extra.insert("workers".into(), meerschaum_core::Value::Int(w));
    }
    // A replaced window that was not given is the posted rows' own datetime range, as for
    // `pipe.sync(df, replace=True)`: a replace never clears the whole table by omission.
    let (begin, end) = if kw.replace {
        match meerschaum_connector::replace::resolve_window(
            pipe,
            &df,
            kw.begin.clone(),
            kw.end.clone(),
        ) {
            Ok(window) => window,
            Err(e) => return SuccessTuple::fail(e),
        }
    } else {
        (None, None)
    };
    let opts = SyncOptions {
        check_existing,
        extra,
        replace: kw.replace,
        begin,
        end,
        params: kw.params.clone(),
        ..Default::default()
    };
    let retries = 10;
    let mut attempt = 1;
    loop {
        let res = conn.sync_pipe(pipe, df.clone(), &opts).await;
        attempt += 1;
        if res.success || !kw.force || attempt > retries {
            if !res.success && kw.force {
                tracing::warn!("Unable to sync {pipe} within {retries} attempts!");
            }
            // Apply a compression policy if the pipe is configured for compression.
            if res.success && pipe.param("compress").is_some_and(|v| v.truthy()) {
                let compressed = conn.apply_compression_policy(pipe).await;
                if !compressed.success {
                    tracing::debug!(
                        "Could not apply compression policy to {pipe}:\n{}",
                        compressed.message
                    );
                }
            }
            return res;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn orient_columns_payload() {
        let p = SyncPayload::Json(json!({"a": {"0": 1, "1": 2}, "b": {"1": "x"}}));
        match payload_input(&p).unwrap() {
            PipeInput::Columns(cols) => {
                assert_eq!(cols["a"], vec![KValue::Int(1), KValue::Int(2)]);
                assert_eq!(cols["b"], vec![KValue::Null, KValue::Str("x".into())]);
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(payload_input(&SyncPayload::Json(json!(1))).is_none());
    }
}
