//! The built-in MCP resources (`meerschaum/mcp/_resources.py`): documents a client fetches once
//! and cites (the pipes, one pipe's attributes, the actions, the connectors, the reference).

use meerschaum_connector::PipesKeysQuery;
use meerschaum_core::pyfmt::{JsonStyle, json_dumps};
use meerschaum_core::{Map, Value};

use super::docs;
use super::protocol::{map_of, s};
use super::security::is_action_permitted;
use super::tools::{ToolCx, ToolError, get_pipes, pipe_summary, resolve_pipe};
use crate::state::pipe_attributes;

/// `json.dumps(x, indent=2, default=str)` (`ensure_ascii=True`).
fn dumps_indent(v: &Value) -> String {
    json_dumps(v, JsonStyle::Indent(2), true)
}

/// Read a built-in resource by name with its template variables.
pub async fn read(name: &str, vars: &Map, cx: &ToolCx<'_>) -> Result<String, ToolError> {
    match name {
        "meerschaum_pipes" => {
            // PARITY: v4 reads `mrsm.get_pipes()`, the configured default instance, which a
            // default `start api` serves (`api_instance: MRSM{meerschaum:instance}`); this reads
            // the server's own instance. Protected targets are not filtered here, as in v4.
            let conn = cx.state.instance();
            let pipes = get_pipes(cx, &conn, &PipesKeysQuery::default()).await?;
            let mut out = Vec::with_capacity(pipes.len());
            // Internal `mrsm` pipes (saved Pipelines' registrations) are never listed.
            for p in pipes.iter().filter(|p| p.connector_keys() != "mrsm") {
                out.push(pipe_summary(&conn, p).await);
            }
            Ok(dumps_indent(&Value::List(out)))
        }
        "meerschaum_pipe" => {
            let args = map_of([
                (
                    "connector_keys",
                    vars.get("connector_keys").cloned().unwrap_or_default(),
                ),
                (
                    "metric_key",
                    vars.get("metric_key").cloned().unwrap_or_default(),
                ),
                (
                    "location_key",
                    vars.get("location_key").cloned().unwrap_or_default(),
                ),
            ]);
            let (conn, pipe) = resolve_pipe(cx, &args).await?;
            let attrs = pipe_attributes(&conn, &pipe)
                .await
                .map_err(|e| ToolError::other(format!("{:?}", e.detail)))?;
            Ok(dumps_indent(&Value::Map(attrs)))
        }
        "meerschaum_actions" => {
            let mut names = cx.state.actions.list();
            names.sort();
            let actions = names
                .into_iter()
                .map(|n| {
                    let permitted = is_action_permitted(&cx.server.config, &n).is_ok();
                    Value::Map(map_of([
                        ("name", s(n)),
                        ("permitted", Value::Bool(permitted)),
                    ]))
                })
                .collect();
            Ok(dumps_indent(&Value::Map(map_of([(
                "actions",
                Value::List(actions),
            )]))))
        }
        "meerschaum_connectors" => {
            let mut keys = Vec::new();
            for (typ, labels) in &cx.state.config.connectors {
                let Some(labels) = labels.as_map() else {
                    continue;
                };
                for label in labels.keys() {
                    if label == "default" {
                        continue;
                    }
                    keys.push(format!("{typ}:{label}"));
                }
            }
            keys.sort();
            Ok(dumps_indent(&Value::Map(map_of([(
                "connectors",
                Value::List(keys.into_iter().map(Value::Str).collect()),
            )]))))
        }
        "meerschaum_docs" => Ok(docs::render_reference(cx).await),
        other => Err(ToolError::other(format!("Unknown resource: '{other}'."))),
    }
}
