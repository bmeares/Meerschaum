//! The built-in MCP tool handlers (`meerschaum/mcp/_tools.py`).
//!
//! Handlers return a [`Value`]; the dispatcher renders it as the `content` text block
//! (`json.dumps(result, indent=2)`) and, when the tool declares an output schema, as
//! `structuredContent`. A failure is a [`ToolError`], reported to the model as
//! `"{ExceptionType}: {message}"` like v4's `except Exception as e` handler.

use std::sync::Arc;

use chrono::TimeDelta;
use indexmap::IndexMap;
use meerschaum_connector::{DataQuery, DateBound, InstanceConnector, PipesKeysQuery};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::serialize::GeometryFormat;
use meerschaum_kernel::value::DateTimeValue;

use super::protocol::{decode_cursor, encode_cursor, map_of, paginate, s};
use super::registry::builtin;
use super::security;
use super::{Caller, McpServer, SqlReadError};
use crate::error::ApiError;
use crate::pipe_methods::{self, SyncKwargs, SyncPayload};
use crate::serialize::{DataJsonOptions, DateUnit, data_to_json};
use crate::state::{AppState, hydrate, normalize_location, pipe_attributes};

/// A failed tool call: the Python exception type and its message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolError {
    /// `PermissionError`, `ValueError`, `TypeError`, ...
    pub kind: &'static str,
    /// `str(e)`.
    pub message: String,
}

impl ToolError {
    /// `PermissionError(message)`.
    pub fn permission(message: impl Into<String>) -> ToolError {
        ToolError {
            kind: "PermissionError",
            message: message.into(),
        }
    }

    /// `ValueError(message)`.
    pub fn value(message: impl Into<String>) -> ToolError {
        ToolError {
            kind: "ValueError",
            message: message.into(),
        }
    }

    /// `TypeError(message)`: reported as invalid arguments.
    pub fn type_error(message: impl Into<String>) -> ToolError {
        ToolError {
            kind: "TypeError",
            message: message.into(),
        }
    }

    /// Any other failure.
    pub fn other(message: impl Into<String>) -> ToolError {
        ToolError {
            kind: "Exception",
            message: message.into(),
        }
    }
}

fn api_error_str(e: &ApiError) -> String {
    let detail = match &e.detail {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => "Internal Server Error".into(),
    };
    // `str(APIPermissionError(msg))` is the message; `str(HTTPException)` is
    // `"{status_code}: {detail}"`.
    if e.status.as_u16() == 403 {
        detail
    } else {
        format!("{}: {detail}", e.status.as_u16())
    }
}

/// The context of a tool call.
pub struct ToolCx<'a> {
    /// The MCP server.
    pub server: &'a McpServer,
    /// The API state (instance connectors, config, actions).
    pub state: &'a AppState,
    /// The caller.
    pub caller: &'a Caller,
}

/// `args.get(key) or None`.
fn truthy_arg<'a>(args: &'a Map, key: &str) -> Option<&'a Value> {
    args.get(key).filter(|v| v.truthy())
}

fn str_arg(args: &Map, key: &str) -> Option<String> {
    truthy_arg(args, key).map(Value::py_str)
}

fn map_arg(args: &Map, key: &str) -> Option<Map> {
    truthy_arg(args, key).and_then(|v| v.as_map().cloned())
}

fn list_arg(args: &Map, key: &str) -> Vec<String> {
    match truthy_arg(args, key) {
        Some(Value::List(items)) => items.iter().map(Value::py_str).collect(),
        Some(other) => vec![other.py_str()],
        None => Vec::new(),
    }
}

/// `pipe.target`.
pub fn target(pipe: &Pipe) -> String {
    pipe.target(None).unwrap_or_else(|_| pipe.target_legacy())
}

fn is_protected(t: &str) -> bool {
    builtin().protected_tables.iter().any(|p| p == t)
}

/// The instance connector for `instance_keys`: the API's instance permissions over HTTP
/// (`check_instance_keys`), any resolvable instance over stdio.
pub fn connector(
    cx: &ToolCx<'_>,
    instance_keys: Option<&str>,
) -> Result<Arc<dyn InstanceConnector>, ToolError> {
    if cx.caller.api {
        return cx
            .state
            .get_api_connector(instance_keys)
            .map_err(|e| ToolError::permission(api_error_str(&e)));
    }
    let default = cx.state.config.instance_keys.as_str();
    match instance_keys {
        None => Ok(cx.state.instance()),
        Some(k) if k == default || k == cx.state.instance().connector_keys() => {
            Ok(cx.state.instance())
        }
        Some(k) => cx
            .state
            .resolver
            .resolve(k)
            .ok_or_else(|| ToolError::value(format!("Invalid instance keys '{k}'."))),
    }
}

fn build_pipe(
    cx: &ToolCx<'_>,
    conn: &Arc<dyn InstanceConnector>,
    connector_keys: &str,
    metric_key: &str,
    location_key: Option<&str>,
    parameters: Option<Map>,
) -> Result<Pipe, ToolError> {
    let mut b = Pipe::builder(connector_keys, metric_key)
        .location(location_key)
        .instance(&conn.connector_keys())
        .defaults(cx.state.config.pipe_defaults.clone());
    if let Some(p) = parameters {
        b = b.parameters(p);
    }
    b.build().map_err(|e| ToolError::value(e.to_string()))
}

fn location_of(args: &Map) -> Option<String> {
    match args.get("location_key") {
        None | Some(Value::Null) => None,
        Some(Value::Str(s)) if s == "None" || s.is_empty() || s == "null" => None,
        Some(other) => Some(other.py_str()),
    }
}

/// `_resolve_pipe(args)`: the addressed pipe with its registered parameters, refusing
/// protected targets.
pub async fn resolve_pipe(
    cx: &ToolCx<'_>,
    args: &Map,
) -> Result<(Arc<dyn InstanceConnector>, Pipe), ToolError> {
    let lk = location_of(args);
    let ik = str_arg(args, "instance_keys");
    let conn = connector(cx, ik.as_deref())?;
    let ck = args
        .get("connector_keys")
        .map(Value::py_str)
        .unwrap_or_default();
    let mk = args
        .get("metric_key")
        .map(Value::py_str)
        .unwrap_or_default();
    let mut pipe = build_pipe(cx, &conn, &ck, &mk, lk.as_deref(), None)?;
    hydrate(&conn, &mut pipe)
        .await
        .map_err(|e| ToolError::other(api_error_str(&e)))?;
    check_protected_target(&pipe)?;
    Ok((conn, pipe))
}

fn check_protected_target(pipe: &Pipe) -> Result<(), ToolError> {
    let t = target(pipe);
    if is_protected(&t) {
        return Err(ToolError::permission(format!(
            "Refusing to access protected table '{t}'."
        )));
    }
    Ok(())
}

/// `json_serialize_value` of a sync time or bound.
pub fn bound_value(b: &Option<DateBound>) -> Value {
    match b {
        None => Value::Null,
        Some(DateBound::Int(i)) => Value::Int(*i),
        // `dt.isoformat() + ('Z' if naive else '')`.
        Some(DateBound::Datetime(dt)) => {
            let iso = dt.isoformat();
            if dt.tz.is_none() {
                s(format!("{iso}Z"))
            } else {
                s(iso)
            }
        }
    }
}

/// `_pipe_summary(pipe)`.
pub async fn pipe_summary(conn: &Arc<dyn InstanceConnector>, pipe: &Pipe) -> Value {
    let dtypes =
        meerschaum_connector::pipe_ops::get_dtypes(conn.as_ref(), pipe, pipe.parameters(), true)
            .await
            .unwrap_or_default();
    let tags = match pipe.tags() {
        Value::Null => Value::List(Vec::new()),
        t => t,
    };
    Value::Map(map_of([
        ("connector_keys", s(pipe.connector_keys())),
        ("metric_key", s(pipe.metric_key())),
        (
            "location_key",
            pipe.location_key().map(s).unwrap_or_default(),
        ),
        ("instance_keys", s(pipe.instance_keys())),
        ("tags", tags),
        ("columns", Value::Map(pipe.columns())),
        (
            "dtypes",
            Value::Map(
                dtypes
                    .into_iter()
                    .map(|(k, v)| (k, Value::Str(v)))
                    .collect(),
            ),
        ),
    ]))
}

/// `mrsm.get_pipes(as_list=True, ...)` on a connector: the pipes in the pipes dictionary's
/// order (grouped by connector keys, then metric key).
pub async fn get_pipes(
    cx: &ToolCx<'_>,
    conn: &Arc<dyn InstanceConnector>,
    query: &PipesKeysQuery,
) -> Result<Vec<Pipe>, ToolError> {
    let keys = conn
        .fetch_pipes_keys(query)
        .await
        .map_err(|e| ToolError::other(e.to_string()))?;
    type Grouped = IndexMap<String, IndexMap<String, Vec<Pipe>>>;
    let mut grouped: Grouped = IndexMap::new();
    for entry in keys.entries() {
        let lk = normalize_location(entry.location_key.as_deref());
        let params = match &entry.extra {
            Some(Value::Map(m)) => Some(m.clone()),
            Some(Value::Str(st)) => match Value::from_json_str(st) {
                Ok(Value::Map(m)) => Some(m),
                _ => None,
            },
            _ => None,
        };
        let has_params = params.is_some();
        let mut pipe = build_pipe(
            cx,
            conn,
            &entry.connector_keys,
            &entry.metric_key,
            lk.as_deref(),
            params,
        )?;
        if !has_params {
            hydrate(conn, &mut pipe)
                .await
                .map_err(|e| ToolError::other(api_error_str(&e)))?;
        }
        grouped
            .entry(entry.connector_keys.clone())
            .or_default()
            .entry(entry.metric_key.clone())
            .or_default()
            .push(pipe);
    }
    Ok(grouped
        .into_values()
        .flat_map(|m| m.into_values().flatten())
        .collect())
}

fn parse_location_filter(l: &str) -> Option<String> {
    normalize_location(Some(l))
}

/// `_clamp_limit(value)`: `int(value)` within `[1, MAX_ROW_LIMIT]`, else the default.
fn clamp_limit(v: Option<&Value>) -> i64 {
    let b = builtin();
    let n = match v {
        None => return b.default_row_limit,
        Some(Value::Int(i)) => *i,
        Some(Value::Bool(x)) => i64::from(*x),
        Some(Value::Float(f)) if f.is_finite() => f.trunc() as i64,
        Some(Value::Str(st)) => match st.trim().replace('_', "").parse::<i64>() {
            Ok(i) => i,
            Err(_) => return b.default_row_limit,
        },
        _ => return b.default_row_limit,
    };
    n.clamp(1, b.max_row_limit)
}

fn tuple_value(t: &SuccessTuple) -> Value {
    Value::Map(map_of([
        ("success", Value::Bool(t.success)),
        ("message", s(t.message.clone())),
    ]))
}

async fn bound_arg(
    cx: &ToolCx<'_>,
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    args: &Map,
    key: &str,
) -> Result<Option<DateBound>, ToolError> {
    let Some(v) = truthy_arg(args, key) else {
        return Ok(None);
    };
    let raw = v.py_str();
    pipe_methods::parse_date_bound(conn, pipe, Some(&raw), &cx.state.config.pipe_defaults)
        .await
        .map_err(|e| ToolError::other(api_error_str(&e)))
}

/// Pipe data as JSON records (`to_json(df, orient='records')` with v4's defaults).
fn frame_records(df: Option<meerschaum_kernel::Frame>) -> Vec<Value> {
    let Some(df) = df else {
        return Vec::new();
    };
    let opts = DataJsonOptions {
        epoch: false,
        date_unit: DateUnit::Us,
        double_precision: 15,
        geometry_format: GeometryFormat::GeoJson,
    };
    match Value::from_json_str(&data_to_json(&df, &opts)) {
        Ok(Value::List(rows)) => rows,
        _ => Vec::new(),
    }
}

fn page_of(records: Vec<Value>, offset: usize, limit: usize) -> Value {
    let end = (offset + limit).min(records.len());
    let start = offset.min(end);
    let window: Vec<Value> = records[start..end].to_vec();
    let has_more = records.len() > offset + limit;
    let count = window.len();
    Value::Map(map_of([
        ("rows", Value::List(window)),
        (
            "count",
            Value::Int(i64::try_from(count).unwrap_or(i64::MAX)),
        ),
        (
            "next_cursor",
            if has_more {
                s(encode_cursor(offset + count))
            } else {
                Value::Null
            },
        ),
    ]))
}

/// Dispatch a built-in tool.
pub async fn call(name: &str, args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    match name {
        "list_pipes" => list_pipes(args, cx).await,
        "get_pipe_attributes" => {
            let (conn, pipe) = resolve_pipe(cx, args).await?;
            let attrs = pipe_attributes(&conn, &pipe)
                .await
                .map_err(|e| ToolError::other(api_error_str(&e)))?;
            Ok(Value::Map(map_of([("attributes", Value::Map(attrs))])))
        }
        "get_pipe_stats" => get_pipe_stats(args, cx).await,
        "get_pipe_data" => get_pipe_data(args, cx).await,
        "register_pipe" => register_pipe(args, cx).await,
        "edit_pipe" => edit_pipe(args, cx).await,
        "sync_pipe" => {
            let (_, pipe) = resolve_pipe(cx, args).await?;
            let res = cx
                .server
                .backend
                .sync_pipe(&pipe, str_arg(args, "begin"), str_arg(args, "end"))
                .await;
            Ok(tuple_value(&res))
        }
        "sync_documents" => {
            let (conn, pipe) = resolve_pipe(cx, args).await?;
            let docs = match args.get("documents") {
                Some(v) if v.truthy() => crate::json::from_core(v),
                _ => serde_json::Value::Array(Vec::new()),
            };
            let res = pipe_methods::sync(
                &conn,
                &pipe,
                &SyncPayload::Json(docs),
                &SyncKwargs {
                    check_existing: true,
                    force: false,
                    workers: None,
                },
                &cx.state.config.pipe_defaults,
            )
            .await;
            Ok(tuple_value(&res))
        }
        "verify_pipe" => {
            let (_, pipe) = resolve_pipe(cx, args).await?;
            let res = cx
                .server
                .backend
                .verify_pipe(&pipe, str_arg(args, "begin"), str_arg(args, "end"))
                .await;
            Ok(tuple_value(&res))
        }
        "deduplicate_pipe" | "clear_pipe" => {
            let (conn, pipe) = resolve_pipe(cx, args).await?;
            let query = DataQuery {
                begin: bound_arg(cx, &conn, &pipe, args, "begin").await?,
                end: bound_arg(cx, &conn, &pipe, args, "end").await?,
                params: map_arg(args, "params"),
                ..Default::default()
            };
            let res = if name == "clear_pipe" {
                conn.clear_pipe(&pipe, &query).await
            } else {
                conn.deduplicate_pipe(&pipe, &query).await
            };
            Ok(tuple_value(&res))
        }
        "drop_pipe" => {
            let (conn, pipe) = resolve_pipe(cx, args).await?;
            Ok(tuple_value(&conn.drop_pipe(&pipe).await))
        }
        "delete_pipe" => {
            let (conn, pipe) = resolve_pipe(cx, args).await?;
            Ok(tuple_value(&conn.delete_pipe(&pipe).await))
        }
        "read_sql" => read_sql(args, cx).await,
        "execute_action" => execute_action(args, cx).await,
        other => Err(ToolError::other(format!("Unknown tool: '{other}'."))),
    }
}

async fn list_pipes(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let ik = str_arg(args, "instance_keys");
    let conn = connector(cx, ik.as_deref())?;
    let query = PipesKeysQuery {
        connector_keys: list_arg(args, "connector_keys"),
        metric_keys: list_arg(args, "metric_keys"),
        location_keys: list_arg(args, "location_keys")
            .iter()
            .map(|l| parse_location_filter(l))
            .collect(),
        tags: list_arg(args, "tags"),
        params: None,
    };
    let pipes: Vec<Pipe> = get_pipes(cx, &conn, &query)
        .await?
        .into_iter()
        .filter(|p| !is_protected(&target(p)))
        .collect();
    let b = builtin();
    let (page, next) = paginate(
        &pipes,
        args.get("cursor"),
        b.default_page_size,
        b.max_cursor_offset,
    )
    .map_err(ToolError::value)?;
    let mut summaries = Vec::with_capacity(page.len());
    for p in &page {
        summaries.push(pipe_summary(&conn, p).await);
    }
    Ok(Value::Map(map_of([
        ("pipes", Value::List(summaries)),
        ("count", Value::Int(page.len() as i64)),
        ("total", Value::Int(pipes.len() as i64)),
        ("next_cursor", next.map(Value::Str).unwrap_or_default()),
    ])))
}

/// `pipe.get_bound_time()` (`core/Pipe/_verify.py`).
async fn get_bound_time(
    cx: &ToolCx<'_>,
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
) -> Result<Value, ToolError> {
    const SUFFIXES: &[&str] = &["minutes", "hours", "days", "weeks", "years", "seconds"];
    let verify = pipe
        .parameters()
        .get("verify")
        .and_then(Value::as_map)
        .cloned()
        .unwrap_or_default();
    // PARITY: the first matching key in the dict's order wins, not the documented priority
    // (`_verify.py:575`).
    let found = verify.iter().find_map(|(k, v)| {
        let suffix = k.strip_prefix("bound_")?;
        SUFFIXES
            .contains(&suffix)
            .then(|| (suffix.to_string(), v.clone()))
    });
    let Some((suffix, value)) = found else {
        return Ok(Value::Null);
    };
    if value.is_null() {
        return Ok(Value::Null);
    }
    let Some(dt_col) = pipe.column("datetime") else {
        // The interval is the raw value, but there is no sync time without a datetime column.
        return Ok(Value::Null);
    };
    let defaults = &cx.state.config.pipe_defaults;
    let dtypes =
        meerschaum_connector::pipe_ops::get_dtypes(conn.as_ref(), pipe, pipe.parameters(), true)
            .await
            .unwrap_or_default();
    let dt_typ = dtypes
        .get(&dt_col)
        .cloned()
        .unwrap_or_else(|| "datetime".into());
    enum Interval {
        Int(i64),
        Delta(TimeDelta),
    }
    let interval = if dt_typ.to_lowercase().contains("int") {
        let n = match &value {
            Value::Int(i) => *i,
            Value::Float(f) => f.trunc() as i64,
            Value::Bool(b) => i64::from(*b),
            Value::Str(st) => st.trim().parse::<i64>().map_err(|_| {
                ToolError::value(format!("invalid literal for int() with base 10: '{st}'"))
            })?,
            other => {
                return Err(ToolError::type_error(format!(
                    "int() argument must be a string, a bytes-like object or a real number, \
                     not '{}'",
                    other.type_name()
                )));
            }
        };
        Interval::Int(n)
    } else {
        if suffix == "years" {
            // PARITY: `timedelta(years=...)` raises (`_verify.py:596`).
            return Err(ToolError::type_error(
                "'years' is an invalid keyword argument for __new__()",
            ));
        }
        let amount = match &value {
            Value::Int(i) => *i as f64,
            Value::Float(f) => *f,
            Value::Bool(b) => f64::from(u8::from(*b)),
            other => {
                return Err(ToolError::type_error(format!(
                    "unsupported type for timedelta {suffix} component: {}",
                    other.type_name()
                )));
            }
        };
        let seconds_per = match suffix.as_str() {
            "minutes" => 60.0,
            "hours" => 3600.0,
            "days" => 86400.0,
            "weeks" => 604_800.0,
            _ => 1.0,
        };
        let micros = (amount * seconds_per * 1e6).round() as i64;
        Interval::Delta(TimeDelta::microseconds(micros))
    };
    let sync_time = pipe_methods::get_sync_time(conn, pipe, None, true, false, defaults)
        .await
        .map_err(|e| ToolError::other(api_error_str(&e)))?;
    let Some(sync_time) = sync_time else {
        return Ok(Value::Null);
    };
    let oldest = pipe_methods::get_sync_time(conn, pipe, None, false, false, defaults)
        .await
        .map_err(|e| ToolError::other(api_error_str(&e)))?;
    let max_days = meerschaum_core::static_config::static_config()
        .pipes
        .max_bound_time_days;
    let bound = match (&sync_time, &interval) {
        (DateBound::Int(i), Interval::Int(n)) => DateBound::Int(i - n),
        (DateBound::Datetime(dt), Interval::Delta(d)) => DateBound::Datetime(DateTimeValue {
            utc: dt.utc - *d,
            tz: dt.tz.clone(),
        }),
        _ => {
            return Err(ToolError::type_error(
                "unsupported operand type(s) for -: the sync time and the bound interval",
            ));
        }
    };
    let (greater, extreme) = match (&bound, &oldest, &sync_time) {
        (DateBound::Int(b), Some(DateBound::Int(o)), _) => (b > o, false),
        (DateBound::Datetime(b), Some(DateBound::Datetime(o)), DateBound::Datetime(st)) => {
            (b.utc > o.utc, (st.utc - o.utc) >= TimeDelta::days(max_days))
        }
        _ => (false, false),
    };
    Ok(if greater || extreme {
        bound_value(&Some(bound))
    } else {
        Value::Null
    })
}

async fn get_pipe_stats(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let (conn, pipe) = resolve_pipe(cx, args).await?;
    let params = map_arg(args, "params");
    let exists = conn
        .pipe_exists(&pipe)
        .await
        .map_err(|e| ToolError::other(e.to_string()))?;
    let mut stats = map_of([
        ("exists", Value::Bool(exists)),
        ("target", s(target(&pipe))),
        ("rowcount", Value::Null),
        ("newest_sync_time", Value::Null),
        ("oldest_sync_time", Value::Null),
        ("bound_time", Value::Null),
        ("columns_types", Value::Null),
    ]);
    if !exists {
        return Ok(Value::Map(stats));
    }
    let defaults = &cx.state.config.pipe_defaults;
    let begin = bound_arg(cx, &conn, &pipe, args, "begin").await?;
    let end = bound_arg(cx, &conn, &pipe, args, "end").await?;
    let rowcount =
        pipe_methods::get_rowcount(&conn, &pipe, begin, end, params.clone(), false).await;
    stats.insert("rowcount".into(), Value::Int(rowcount));
    for (key, newest) in [("newest_sync_time", true), ("oldest_sync_time", false)] {
        let st =
            pipe_methods::get_sync_time(&conn, &pipe, params.as_ref(), newest, false, defaults)
                .await
                .map_err(|e| ToolError::other(api_error_str(&e)))?;
        stats.insert(key.into(), bound_value(&st));
    }
    stats.insert("bound_time".into(), get_bound_time(cx, &conn, &pipe).await?);
    let types = conn
        .get_pipe_columns_types(&pipe)
        .await
        .map_err(|e| ToolError::other(e.to_string()))?;
    stats.insert(
        "columns_types".into(),
        Value::Map(types.into_iter().map(|(k, v)| (k, Value::Str(v))).collect()),
    );
    Ok(Value::Map(stats))
}

async fn get_pipe_data(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let (conn, pipe) = resolve_pipe(cx, args).await?;
    let limit = usize::try_from(clamp_limit(args.get("limit"))).unwrap_or(1);
    let offset =
        decode_cursor(args.get("cursor"), builtin().max_cursor_offset).map_err(ToolError::value)?;
    let params = map_arg(args, "params");
    let fetch_limit = i64::try_from(offset + limit + 1).unwrap_or(i64::MAX);
    let backtrack = args.get("backtrack_minutes").filter(|v| !v.is_null());
    let df = if let Some(minutes) = backtrack {
        let minutes = minutes
            .as_i64()
            .or_else(|| minutes.as_f64().map(|f| f.trunc() as i64))
            .or_else(|| minutes.as_str().and_then(|st| st.trim().parse().ok()))
            .ok_or_else(|| {
                ToolError::value(format!(
                    "invalid literal for int() with base 10: '{}'",
                    minutes.py_str()
                ))
            })?;
        conn.get_backtrack_data(
            &pipe,
            minutes,
            &DataQuery {
                params,
                limit: Some(fetch_limit),
                // PARITY: v4's instance `get_backtrack_data` reads newest-first when limited.
                order: Some("desc".into()),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| ToolError::other(e.to_string()))?
    } else {
        let list = |k: &str| {
            let l = list_arg(args, k);
            (!l.is_empty()).then_some(l)
        };
        let order = match args.get("order") {
            None => Some("asc".to_string()),
            Some(Value::Null) => None,
            Some(v) => Some(v.py_str()),
        };
        let query = DataQuery {
            select_columns: list("select_columns"),
            omit_columns: list("omit_columns"),
            begin: bound_arg(cx, &conn, &pipe, args, "begin").await?,
            end: bound_arg(cx, &conn, &pipe, args, "end").await?,
            params,
            limit: Some(fetch_limit),
            order,
            ..Default::default()
        };
        meerschaum_connector::pipe_ops::get_data(conn.as_ref(), &pipe, &query)
            .await
            .map_err(|e| ToolError::other(e.to_string()))?
    };
    Ok(page_of(frame_records(df), offset, limit))
}

async fn register_pipe(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let lk = location_of(args);
    let ik = str_arg(args, "instance_keys");
    let conn = connector(cx, ik.as_deref())?;
    let ck = args
        .get("connector_keys")
        .map(Value::py_str)
        .unwrap_or_default();
    let mk = args
        .get("metric_key")
        .map(Value::py_str)
        .unwrap_or_default();
    // `mrsm.Pipe(..., columns=, dtypes=, tags=, parameters=)`: `columns`, `tags` and `dtypes`
    // override the same keys of `parameters`, in that order.
    let mut parameters = map_arg(args, "parameters").unwrap_or_default();
    if let Some(columns) = truthy_arg(args, "columns") {
        parameters.insert("columns".into(), columns.clone());
    }
    if let Some(tags) = truthy_arg(args, "tags") {
        parameters.insert("tags".into(), tags.clone());
    }
    if let Some(dtypes) = truthy_arg(args, "dtypes") {
        parameters.insert("dtypes".into(), dtypes.clone());
    }
    let mut pipe = build_pipe(cx, &conn, &ck, &mk, lk.as_deref(), Some(parameters))?;
    // Checked after construction because `target` may be set in `parameters`.
    check_protected_target(&pipe)?;
    // `Pipe.register()`: an empty parameters dict registers the default index columns.
    if pipe.parameters().is_empty() {
        let cols = pipe.columns();
        let cols = if cols.is_empty() {
            map_of([("datetime", Value::Null), ("id", Value::Null)])
        } else {
            cols
        };
        pipe.set_parameters(map_of([("columns", Value::Map(cols))]));
    }
    let res = conn.register_pipe(&pipe).await;
    if cx.caller.api {
        let _ = cx.state.pipes(ik.as_deref(), true).await;
    }
    Ok(tuple_value(&res))
}

async fn edit_pipe(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let parameters = map_arg(args, "parameters").unwrap_or_default();
    // Don't let an edit re-point a pipe at a protected table.
    for key in ["target", "target_name", "target_table"] {
        let t = parameters.get(key).map(Value::py_str).unwrap_or_default();
        if is_protected(&t) {
            return Err(ToolError::permission(format!(
                "Refusing to target protected table '{t}'."
            )));
        }
    }
    let (conn, mut pipe) = resolve_pipe(cx, args).await?;
    if args.get("replace").is_some_and(Value::truthy) {
        pipe.set_parameters(parameters);
    } else {
        // `pipe.update_parameters(parameters, persist=True)`: deep-merge, then `edit()`.
        let merged = meerschaum_core::value::apply_patch_maps(pipe.parameters(), &parameters);
        pipe.set_parameters(merged);
    }
    let res = conn.edit_pipe(&pipe, false).await;
    if cx.caller.api {
        let ik = str_arg(args, "instance_keys");
        let _ = cx.state.pipes(ik.as_deref(), true).await;
    }
    Ok(tuple_value(&res))
}

async fn read_sql(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let query = args.get("query").map(Value::py_str).unwrap_or_default();
    security::is_read_only_query(&query).map_err(ToolError::value)?;
    // Protected instance tables are off-limits here too, not just to the pipe tools.
    let stripped = security::strip_sql_noise(&query).unwrap_or_else(|_| query.clone());
    let scan = if stripped.is_empty() {
        &query
    } else {
        &stripped
    };
    for word in security::words(scan) {
        if is_protected(&word.to_lowercase()) {
            return Err(ToolError::permission(format!(
                "Refusing to query protected table '{word}'."
            )));
        }
    }
    let ck = args
        .get("connector_keys")
        .map(Value::py_str)
        .unwrap_or_default();
    let limit = usize::try_from(clamp_limit(args.get("limit"))).unwrap_or(1);
    let offset =
        decode_cursor(args.get("cursor"), builtin().max_cursor_offset).map_err(ToolError::value)?;
    match cx
        .server
        .backend
        .read_sql(&ck, &query, offset + limit + 1)
        .await
    {
        Ok(rows) => Ok(page_of(rows, offset, limit)),
        Err(SqlReadError::Unsupported) => Err(ToolError::value(format!(
            "Connector '{ck}' does not support SQL reads."
        ))),
        Err(SqlReadError::Failed) => Err(ToolError::value(
            "The query could not be executed. Check it for syntax errors, a missing table, or \
             insufficient database privileges.",
        )),
    }
}

async fn execute_action(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let fail = |m: String| Ok(tuple_value(&SuccessTuple::fail(m)));
    let action = args.get("action").map(Value::py_str).unwrap_or_default();
    let parts: Vec<String> = action.split_whitespace().map(String::from).collect();
    let Some(action_name) = parts.first().cloned() else {
        return fail("No action was given.".into());
    };
    if !cx.state.actions.list().contains(&action_name) {
        return fail(format!(
            "Unknown action '{action_name}'. \
             Read the `mrsm://actions` resource for the available actions."
        ));
    }
    if let Err(m) = security::is_action_permitted(&cx.server.config, &action_name) {
        return fail(m);
    }
    // Same check as the REST `/actions` route.
    if cx.caller.api {
        let allowed =
            crate::auth::is_user_allowed_to_execute(cx.state, cx.caller.principal.as_ref()).await;
        if !allowed.success {
            return Ok(tuple_value(&allowed));
        }
    }
    let mut kwargs = map_arg(args, "kwargs").unwrap_or_default();
    if parts.len() > 1 && !kwargs.contains_key("action") {
        kwargs.insert(
            "action".into(),
            Value::List(parts[1..].iter().map(|p| s(p.clone())).collect()),
        );
    }
    // A denylisted action must not be reachable as another action's subaction.
    let mut chain: Vec<String> = parts[1..].to_vec();
    match kwargs.get("action") {
        Some(Value::List(items)) => chain.extend(items.iter().map(Value::py_str)),
        Some(v) if v.truthy() => chain.push(v.py_str()),
        _ => {}
    }
    if let Err(m) = security::check_action_chain(&cx.server.config, &chain) {
        return fail(m);
    }
    // Pin the action to the API's own instance, as the REST route does.
    if cx.caller.api {
        let ik = kwargs
            .get("mrsm_instance")
            .filter(|v| !v.is_null())
            .map(Value::py_str);
        if let Err(e) = cx.state.get_api_connector(ik.as_deref()) {
            return fail(api_error_str(&e));
        }
        let pinned = match ik.filter(|k| !k.is_empty()) {
            Some(k) => k,
            None => cx.state.instance().connector_keys(),
        };
        kwargs.insert("mrsm_instance".into(), s(pinned));
    }
    let res = cx.state.actions.execute(&action_name, kwargs).await;
    Ok(tuple_value(&res))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits() {
        assert_eq!(clamp_limit(None), 200);
        assert_eq!(clamp_limit(Some(&Value::Int(0))), 1);
        assert_eq!(clamp_limit(Some(&Value::Int(1_000_000))), 10_000);
        assert_eq!(clamp_limit(Some(&Value::Str("5".into()))), 5);
        assert_eq!(clamp_limit(Some(&Value::Str("x".into()))), 200);
        assert_eq!(clamp_limit(Some(&Value::Float(2.9))), 2);
    }

    #[test]
    fn pages() {
        let rows: Vec<Value> = (0..3).map(Value::Int).collect();
        let p = page_of(rows.clone(), 0, 2);
        assert_eq!(p.get("count"), Some(&Value::Int(2)));
        assert_eq!(p.get("next_cursor"), Some(&s(encode_cursor(2))));
        let p = page_of(rows, 2, 2);
        assert_eq!(p.get("next_cursor"), Some(&Value::Null));
    }
}
