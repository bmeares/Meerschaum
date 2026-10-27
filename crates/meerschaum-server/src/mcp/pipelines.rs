//! The MCP tools v5 adds ([`super::v5`]): saved Pipelines, runs, jobs and connectors. Each
//! does what its HTTP route does, with the same scopes (the registry's) and, over HTTP, the
//! same permission checks (instances, job visibility, who may run Pipelines). Over stdio the
//! caller is the server's own user (local trust).

use std::sync::Arc;

use meerschaum_connector::InstanceConnector;
use meerschaum_core::{Map, Value};
use serde_json::{Value as Json, json};

use super::protocol::map_of;
use super::tools::{ToolCx, ToolError, connector};
use crate::backends::{PlanOptions, RunStatusFilter, RunsFilter};
use crate::error::ApiError;
use crate::routes::pipelines as http;

/// The runs `list_runs` returns without `limit` (MCP results are read by a model: keep them
/// short).
pub const DEFAULT_RUNS: usize = 20;
/// The most runs one `list_runs` call returns.
pub const MAX_RUNS: usize = 200;
/// The log lines `get_job_logs` returns without `lines`.
pub const DEFAULT_LOG_LINES: usize = 200;
/// The most log lines one `get_job_logs` call returns.
pub const MAX_LOG_LINES: usize = 5000;

fn json_value(j: Json) -> Value {
    serde_json::from_value(j).unwrap_or(Value::Null)
}

/// An HTTP-style error as the tool error the dispatcher reports.
fn api(e: ApiError) -> ToolError {
    let detail = match &e.detail {
        Some(Json::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => e
            .internal
            .as_deref()
            .cloned()
            .unwrap_or_else(|| "Internal Server Error".into()),
    };
    match e.status.as_u16() {
        401 | 403 => ToolError::permission(detail),
        400 | 404 | 409 | 422 => ToolError::value(detail),
        _ => ToolError::other(detail),
    }
}

fn str_arg(args: &Map, key: &str) -> Option<String> {
    match args.get(key) {
        None | Some(Value::Null) => None,
        Some(Value::Str(s)) if s.trim().is_empty() => None,
        Some(v) => Some(v.py_str()),
    }
}

fn required_str(args: &Map, key: &str) -> Result<String, ToolError> {
    str_arg(args, key).ok_or_else(|| ToolError::value(format!("`{key}` is required.")))
}

fn limit_arg(args: &Map, key: &str, default: usize, max: usize) -> Result<usize, ToolError> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Int(n)) if *n >= 1 => Ok(usize::try_from(*n).unwrap_or(max).min(max)),
        Some(Value::Str(s)) => match s.trim().parse::<usize>() {
            Ok(n) if n >= 1 => Ok(n.min(max)),
            _ => Err(ToolError::type_error(format!(
                "`{key}` must be a positive whole number."
            ))),
        },
        Some(_) => Err(ToolError::type_error(format!(
            "`{key}` must be a positive whole number."
        ))),
    }
}

/// The `begin` / `end` arguments (strings or integers).
fn bounds(args: &Map) -> Result<PlanOptions, ToolError> {
    let body = json!({
        "begin": crate::json::from_core(args.get("begin").unwrap_or(&Value::Null)),
        "end": crate::json::from_core(args.get("end").unwrap_or(&Value::Null)),
    });
    http::bounds_of(&body).map_err(api)
}

/// The registry instance of a Pipeline tool call (the server's instance by default).
fn registry(cx: &ToolCx<'_>, args: &Map) -> Result<Arc<dyn InstanceConnector>, ToolError> {
    connector(cx, str_arg(args, "instance_keys").as_deref())
}

/// The caller's principal over HTTP; `None` (the server's own user) over stdio.
fn principal<'a>(cx: &'a ToolCx<'_>) -> Option<&'a crate::auth::Principal> {
    if cx.caller.api {
        cx.caller.principal.as_ref()
    } else {
        None
    }
}

/// `list_pipelines`.
pub async fn list_pipelines(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let conn = registry(cx, args)?;
    let definitions = http::list_definitions(cx.state, &conn).await.map_err(api)?;
    let mut pipelines = Vec::with_capacity(definitions.len());
    for definition in definitions {
        let name = definition["name"].as_str().unwrap_or_default().to_string();
        let members = http::targets_of(&definition);
        let last_run = http::last_run(cx.state, &conn, &name).await;
        pipelines.push(json!({
            "name": name,
            "registry": conn.connector_keys(),
            "description": definition.get("description").cloned().unwrap_or(Json::Null),
            "members": members,
            "parents": definition.get("parents").cloned().unwrap_or(Json::Null),
            "children": definition.get("children").cloned().unwrap_or(Json::Null),
            "last_run": last_run,
        }));
    }
    let count = pipelines.len();
    Ok(Value::Map(map_of([
        ("pipelines", json_value(json!(pipelines))),
        ("count", Value::Int(count as i64)),
    ])))
}

/// `plan_pipeline`.
pub async fn plan_pipeline(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let name = required_str(args, "name")?;
    let options = bounds(args)?;
    let conn = registry(cx, args)?;
    http::require_definition_on(cx.state, &conn, &name)
        .await
        .map_err(api)?;
    let planned = cx
        .state
        .pipelines
        .plan(&name, &conn.connector_keys(), &options)
        .await
        .map_err(|e| api(http::plan_error(e)))?;
    let mut planned = planned;
    if let Some(o) = planned.as_object_mut() {
        o.entry("warnings").or_insert(json!([]));
    }
    Ok(json_value(planned))
}

/// `sync_pipeline`.
pub async fn sync_pipeline(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let name = required_str(args, "name")?;
    let options = bounds(args)?;
    let conn = registry(cx, args)?;
    let definition = http::require_definition_on(cx.state, &conn, &name)
        .await
        .map_err(api)?;
    let started = http::start_sync(
        cx.state,
        principal(cx),
        &conn,
        &name,
        &definition,
        &options,
        None,
    )
    .await
    .map_err(api)?;
    let mut out = map_of([
        ("success", Value::Bool(true)),
        (
            "message",
            Value::Str(format!(
                "Started Pipeline '{name}' (run '{}') as job '{}'.",
                started.run_id, started.job
            )),
        ),
    ]);
    for (key, value) in started.fields {
        out.insert(key, json_value(value));
    }
    Ok(Value::Map(out))
}

/// The engine store that keeps the runs of `instance_keys` (the server's by default).
fn store(
    cx: &ToolCx<'_>,
    args: &Map,
) -> Result<Arc<dyn crate::backends::EngineBackend>, ToolError> {
    let conn = registry(cx, args)?;
    Ok(http::store_of(cx.state, &conn))
}

/// `list_runs`.
pub async fn list_runs(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let status = match str_arg(args, "status") {
        None => None,
        Some(s) => Some(RunStatusFilter::parse(&s).ok_or_else(|| {
            ToolError::value(format!(
                "`status` must be 'success', 'failed' or 'running', got '{s}'."
            ))
        })?),
    };
    let since = match str_arg(args, "since") {
        None => None,
        Some(s) => {
            let value =
                meerschaum_kernel::value::parse_datetime_value(s.trim()).ok_or_else(|| {
                    ToolError::value(format!("`since` must be an ISO 8601 datetime, got '{s}'."))
                })?;
            Some(chrono::DateTime::from_naive_utc_and_offset(
                value.utc,
                chrono::Utc,
            ))
        }
    };
    let filter = RunsFilter {
        run_id: None,
        pipeline: str_arg(args, "pipeline"),
        pipe_ref: str_arg(args, "pipe"),
        status,
        since,
        limit: Some(limit_arg(args, "limit", DEFAULT_RUNS, MAX_RUNS)?),
    };
    let runs = store(cx, args)?
        .get_runs(&filter)
        .await
        .map_err(|e| api(crate::engine_stream::api_error(e)))?;
    let count = runs.len();
    Ok(Value::Map(map_of([
        (
            "runs",
            json_value(serde_json::to_value(runs).unwrap_or(Json::Null)),
        ),
        ("count", Value::Int(count as i64)),
    ])))
}

/// `get_run`.
pub async fn get_run(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let run_id = required_str(args, "run_id")?;
    let store = store(cx, args)?;
    let Some((run, nodes)) = crate::routes::engine::run_detail(store.as_ref(), &run_id)
        .await
        .map_err(|e| api(crate::engine_stream::api_error(e)))?
    else {
        return Err(ToolError::value(format!("Run '{run_id}' was not found.")));
    };
    Ok(Value::Map(map_of([
        (
            "run",
            json_value(serde_json::to_value(run).unwrap_or(Json::Null)),
        ),
        (
            "nodes",
            json_value(serde_json::to_value(nodes).unwrap_or(Json::Null)),
        ),
    ])))
}

/// Whether the caller may see job `name` (`meta`): over HTTP the jobs routes' rule (an
/// execution job is its creator's and the administrators'); over stdio every job.
async fn may_see(cx: &ToolCx<'_>, name: &str, meta: Option<&Json>) -> bool {
    if !cx.caller.api {
        return true;
    }
    let principal = cx.caller.principal.as_ref();
    let admin = crate::auth::is_admin(cx.state, principal).await;
    crate::routes::execute::job_visible_to(
        admin,
        crate::routes::execute::owner_label(principal).as_deref(),
        name,
        meta,
    )
}

/// `list_jobs`.
pub async fn list_jobs(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let status = str_arg(args, "status").map(|s| s.to_lowercase());
    let mut jobs = Vec::new();
    for (name, mut meta) in cx.state.jobs.list().await {
        if !may_see(cx, &name, Some(&meta)).await {
            continue;
        }
        if status
            .as_deref()
            .is_some_and(|s| meta["status"].as_str() != Some(s))
        {
            continue;
        }
        crate::redact::redact_job_meta(&mut meta);
        let result = match meta.get("result") {
            Some(Json::Array(items)) if items.len() >= 2 => {
                json!({"success": items[0], "message": items[1]})
            }
            Some(other) => other.clone(),
            None => Json::Null,
        };
        let mut job = json!({
            "name": name,
            "status": meta.get("status").cloned().unwrap_or(Json::Null),
            "sysargs": meta.get("sysargs").cloned().unwrap_or(Json::Null),
            "restart": meta.get("restart").cloned().unwrap_or(Json::Null),
            "result": result,
        });
        if let Some(health) = meta.get("health")
            && let Some(o) = job.as_object_mut()
        {
            o.insert("health".into(), health.clone());
        }
        jobs.push(job);
    }
    let count = jobs.len();
    Ok(Value::Map(map_of([
        ("jobs", json_value(json!(jobs))),
        ("count", Value::Int(count as i64)),
    ])))
}

/// `get_job_logs`.
pub async fn get_job_logs(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let name = required_str(args, "name")?;
    let lines = limit_arg(args, "lines", DEFAULT_LOG_LINES, MAX_LOG_LINES)?;
    let missing = || ToolError::value(format!("Job '{name}' does not exist."));
    let meta = cx.state.jobs.get(&name).await.ok_or_else(missing)?;
    if !may_see(cx, &name, Some(&meta)).await {
        return Err(missing());
    }
    let logs = cx.state.jobs.logs(&name).await.unwrap_or_default();
    let all: Vec<&str> = logs.lines().collect();
    let start = all.len().saturating_sub(lines);
    let tail = all[start..].join("\n");
    Ok(Value::Map(map_of([
        ("name", Value::Str(name)),
        ("logs", Value::Str(tail)),
        ("lines", Value::Int((all.len() - start) as i64)),
        ("truncated", Value::Bool(start > 0)),
    ])))
}

/// `list_connectors`.
pub async fn list_connectors(args: &Map, cx: &ToolCx<'_>) -> Result<Value, ToolError> {
    let wanted = str_arg(args, "type");
    let mut connectors = Vec::new();
    for (typ, labels) in &cx.state.config.connectors {
        if wanted.as_ref().is_some_and(|w| w != typ) {
            continue;
        }
        let Some(labels) = labels.as_map() else {
            continue;
        };
        for label in labels.keys() {
            if label == "default" {
                continue;
            }
            connectors.push(json!({
                "keys": format!("{typ}:{label}"),
                "type": typ,
                "label": label,
            }));
        }
    }
    connectors.sort_by(|a, b| a["keys"].as_str().cmp(&b["keys"].as_str()));
    let count = connectors.len();
    Ok(Value::Map(map_of([
        ("connectors", json_value(json!(connectors))),
        ("count", Value::Int(count as i64)),
    ])))
}
