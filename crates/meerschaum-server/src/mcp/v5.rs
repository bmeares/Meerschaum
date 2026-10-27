//! What v5 adds to v4's MCP registry (`registry.json`, generated from 4.1.4 and never edited):
//! the Pipelines, runs, jobs and connectors tools, `parents` / `children` on `sync_pipe`, bounds
//! typed as strings or integers, and a primer that mentions Pipelines, runs and jobs.
//!
//! [`super::registry::tools`] and [`super::registry::primer`] are v4's entries with these
//! changes applied; the MCP contract test replays v4's recorded exchanges against them by
//! mapping each served entry back to v4's (`tests/mcp_contract.rs`).

use meerschaum_core::Value;
use serde_json::json;

use super::registry::{Source, Tool};

/// Appended to v4's primer (the `initialize` instructions and the `mrsm://docs` data model).
pub const PRIMER_ADDENDUM: &str = "\n\n**Pipelines** are saved, named groups of pipes that sync together in dependency order \
(parents before children). `list_pipelines` lists them with their members and latest run; \
`plan_pipeline` shows what a run would do without moving any data; `sync_pipeline` starts a run \
in the background and returns its `run_id` and the background `job`. `sync_pipe` with `parents` \
or `children` also syncs a pipe's upstream or downstream pipes in order.\n\n\
**Runs** are the history of Pipeline and dependency syncs: `list_runs` filters them by \
`pipeline`, `pipe`, `status` and `since`, and `get_run` shows one run with each pipe's outcome. \
**Jobs** are background processes (started with `mrsm start job`, schedules, or `sync_pipeline`): \
`list_jobs` shows them and `get_job_logs` reads a job's recent output. `list_connectors` names \
the configured connectors.";

/// The names of the tools v5 adds.
pub const NEW_TOOLS: &[&str] = &[
    "list_pipelines",
    "plan_pipeline",
    "sync_pipeline",
    "list_runs",
    "get_run",
    "list_jobs",
    "get_job_logs",
    "list_connectors",
];

/// The route group of the HTTP equivalent of a tool v5 adds: over HTTP the tool is hidden (and
/// refused) when the server does not serve that group (`api:permissions:routes`).
pub fn route_group(tool: &str) -> Option<&'static str> {
    match tool {
        "list_pipelines" | "plan_pipeline" | "sync_pipeline" => Some("pipelines"),
        "list_runs" | "get_run" => Some("engine"),
        "list_jobs" | "get_job_logs" => Some("jobs"),
        "list_connectors" => Some("connectors"),
        _ => None,
    }
}

/// A datetime-axis bound as v5 types it: an ISO 8601 string or an integer.
fn bound_schema(lower: bool) -> serde_json::Value {
    if lower {
        json!({
            "type": ["string", "integer"],
            "description": "Inclusive lower bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no lower bound."
        })
    } else {
        json!({
            "type": ["string", "integer"],
            "description": "Exclusive upper bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no upper bound."
        })
    }
}

fn depth_schema(parents: bool) -> serde_json::Value {
    if parents {
        json!({
            "type": ["boolean", "integer"],
            "description": "Also sync the pipe's upstream pipes first, in dependency order: true for every generation, or a number of generations."
        })
    } else {
        json!({
            "type": ["boolean", "integer"],
            "description": "Also sync the pipe's downstream pipes after it, in dependency order: true for every generation, or a number of generations."
        })
    }
}

fn to_value(j: serde_json::Value) -> Value {
    serde_json::from_value(j).unwrap_or(Value::Null)
}

/// Apply v5's changes to one of v4's tools.
pub fn patch(tool: &mut Tool) {
    let Value::Map(schema) = &mut tool.input_schema else {
        return;
    };
    let Some(Value::Map(properties)) = schema.get_mut("properties") else {
        return;
    };
    for (key, lower) in [("begin", true), ("end", false)] {
        if properties.contains_key(key) {
            properties.insert(key.into(), to_value(bound_schema(lower)));
        }
    }
    if tool.name == "sync_pipe" {
        properties.insert("parents".into(), to_value(depth_schema(true)));
        properties.insert("children".into(), to_value(depth_schema(false)));
        tool.description.push_str(
            " Pass `parents` or `children` to also sync the pipe's upstream or downstream pipes \
             in dependency order.",
        );
    }
}

const INSTANCE_KEYS: &str =
    "Where the Pipeline (or run) is kept, e.g. 'sql:main'. Omit for the server's default instance.";

fn tool(
    name: &str,
    title: &str,
    description: &str,
    scopes: &[&str],
    read_only: bool,
    input: serde_json::Value,
    output: serde_json::Value,
) -> Tool {
    Tool {
        name: name.into(),
        description: description.into(),
        title: Some(title.into()),
        scopes: scopes.iter().map(|s| s.to_string()).collect(),
        read_only,
        destructive: false,
        idempotent: read_only,
        open_world: false,
        input_schema: to_value(input),
        output_schema: Some(to_value(output)),
        source: Source::Builtin,
    }
}

/// The tools v5 adds, in [`NEW_TOOLS`] order.
pub fn tools() -> Vec<Tool> {
    let name = json!({"type": "string", "description": "The Pipeline's name, e.g. 'daily'."});
    let instance = json!({"type": "string", "description": INSTANCE_KEYS});
    let object_list = json!({"type": "array", "items": {"type": "object"}});
    vec![
        tool(
            "list_pipelines",
            "List Pipelines",
            "List the saved Pipelines on an instance: named groups of pipes that sync together \
             in dependency order, with their members and latest run. Use `plan_pipeline` to see \
             what a run would do and `sync_pipeline` to start one.",
            &["pipes:read"],
            true,
            json!({"type": "object", "properties": {"instance_keys": instance}}),
            json!({"type": "object", "required": ["pipelines"], "properties": {
                "pipelines": object_list,
                "count": {"type": "integer"},
            }}),
        ),
        tool(
            "plan_pipeline",
            "Plan a Pipeline run",
            "Show what a run of a saved Pipeline would do, without moving any data: each pipe's \
             action (sync, skip, backfill, rebuild, ...), why, and its window. Nothing is \
             registered or written.",
            &["pipes:read"],
            true,
            json!({"type": "object", "required": ["name"], "properties": {
                "name": name,
                "instance_keys": instance,
                "begin": bound_schema(true),
                "end": bound_schema(false),
            }}),
            json!({"type": "object", "required": ["plan"], "properties": {
                "plan": {"type": "object"},
                "text": {"type": "string"},
                "warnings": {"type": "array", "items": {"type": "string"}},
            }}),
        ),
        tool(
            "sync_pipeline",
            "Sync a Pipeline",
            "Start a run of a saved Pipeline in the background: its pipes sync in dependency \
             order. Returns the `run_id` (follow it with `get_run`) and the background `job` \
             (read its output with `get_job_logs`).",
            &["pipes:write", "jobs:execute"],
            false,
            json!({"type": "object", "required": ["name"], "properties": {
                "name": name,
                "instance_keys": instance,
                "begin": bound_schema(true),
                "end": bound_schema(false),
            }}),
            json!({"type": "object", "required": ["success", "message"], "properties": {
                "success": {"type": "boolean"},
                "message": {"type": "string"},
                "run_id": {"type": ["string", "null"]},
                "job": {"type": ["string", "null"]},
                "stream": {"type": ["string", "null"]},
            }}),
        ),
        tool(
            "list_runs",
            "List runs",
            "List the runs of Pipelines and dependency syncs, newest first, with their status \
             and timing. Filter by `pipeline`, `pipe` (a pipe reference \
             'instance|connector|metric|location'), `status` and `since`.",
            &["pipes:read"],
            true,
            json!({"type": "object", "properties": {
                "instance_keys": instance,
                "pipeline": {"type": "string", "description": "Only runs of this saved Pipeline."},
                "pipe": {"type": "string", "description": "Only runs that synced this pipe, e.g. 'sql:main|sql:main|weather|None'."},
                "status": {"type": "string", "enum": ["success", "failed", "running"], "description": "Only runs with this status."},
                "since": {"type": "string", "description": "Only runs started at or after this ISO 8601 datetime."},
                "limit": {"type": "integer", "description": "At most this many runs (default 20)."},
            }}),
            json!({"type": "object", "required": ["runs"], "properties": {
                "runs": object_list,
                "count": {"type": "integer"},
            }}),
        ),
        tool(
            "get_run",
            "Get a run",
            "Return one run with each pipe's outcome: its action, status, window, rows inserted \
             and updated, timing and message.",
            &["pipes:read"],
            true,
            json!({"type": "object", "required": ["run_id"], "properties": {
                "run_id": {"type": "string", "description": "The run's ID, from `list_runs` or `sync_pipeline`."},
                "instance_keys": instance,
            }}),
            json!({"type": "object", "required": ["run"], "properties": {
                "run": {"type": "object"},
                "nodes": object_list,
            }}),
        ),
        tool(
            "list_jobs",
            "List jobs",
            "List the background jobs on the server with their status (running, paused, \
             stopped), command and last result. Credentials in their environment are masked.",
            &["jobs:read"],
            true,
            json!({"type": "object", "properties": {
                "status": {"type": "string", "enum": ["running", "paused", "stopped"], "description": "Only jobs with this status."},
            }}),
            json!({"type": "object", "required": ["jobs"], "properties": {
                "jobs": object_list,
                "count": {"type": "integer"},
            }}),
        ),
        tool(
            "get_job_logs",
            "Get job logs",
            "Return the most recent lines of a background job's output.",
            &["jobs:read", "logs:read"],
            true,
            json!({"type": "object", "required": ["name"], "properties": {
                "name": {"type": "string", "description": "The job's name, from `list_jobs` or `sync_pipeline`."},
                "lines": {"type": "integer", "description": "How many of the last lines to return (default 200, at most 5000)."},
            }}),
            json!({"type": "object", "required": ["name", "logs"], "properties": {
                "name": {"type": "string"},
                "logs": {"type": "string"},
                "lines": {"type": "integer"},
                "truncated": {"type": "boolean"},
            }}),
        ),
        tool(
            "list_connectors",
            "List connectors",
            "List the connectors configured on this server (type and label, e.g. 'sql:main'). \
             Credentials are never included.",
            &["connectors:read"],
            true,
            json!({"type": "object", "properties": {
                "type": {"type": "string", "description": "Only connectors of this type, e.g. 'sql'."},
            }}),
            json!({"type": "object", "required": ["connectors"], "properties": {
                "connectors": object_list,
                "count": {"type": "integer"},
            }}),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_tools_are_complete() {
        let tools = tools();
        assert_eq!(
            tools.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
            NEW_TOOLS
        );
        for t in &tools {
            assert!(route_group(&t.name).is_some(), "{}", t.name);
            assert!(!t.scopes.is_empty(), "{}", t.name);
            assert!(t.output_schema.is_some(), "{}", t.name);
        }
    }

    #[test]
    fn v4_tools_get_typed_bounds_and_graph_syncs() {
        let mut sync = super::super::registry::builtin()
            .tools
            .iter()
            .find(|t| t.name == "sync_pipe")
            .cloned()
            .unwrap();
        patch(&mut sync);
        let props = sync.input_schema.get("properties").unwrap();
        assert_eq!(
            props.get("begin").unwrap().get("type"),
            Some(&to_value(json!(["string", "integer"])))
        );
        assert!(props.get("parents").is_some() && props.get("children").is_some());
        assert!(sync.description.contains("`parents`"));
    }
}
