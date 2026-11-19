//! Fixed native consumer of a sealed Main-selected extension operation.

use meerschaum_server::executor::{extensions, plugin_handlers, plugins};
use std::collections::BTreeSet;

pub(crate) fn run(runtime: &mut crate::dispatch::Runtime<'_>) -> Result<Vec<u8>, String> {
    if std::env::var(meerschaum_core::team_scope::api_execution::PROFILE_VAR).as_deref() != Ok("1")
        || std::env::var_os(meerschaum_server::executor::environment::METADATA_VAR).is_some()
    {
        return Err("Extension invocation requires its private Api execution profile.".into());
    }
    meerschaum_core::team_scope::sandbox::current()
        .ok_or("Extension invocation requires a confined native runtime.")?
        .require_sockets_verdict()
        .map_err(
            |_| "Extension invocation requires mandatory filesystem/socket/signal isolation.",
        )?;
    let value = runtime
        .config()
        .get(&[extensions::CONFIG_KEY])
        .ok_or("No Main-selected extension operation is available.")?;
    let encoded = value.to_json_string();
    if encoded.len() > extensions::MAX_DESCRIPTOR_BYTES {
        return Err("The extension operation exceeds its byte bound.".into());
    }
    let operation: extensions::Operation =
        serde_json::from_str(&encoded).map_err(|_| "Invalid extension operation.")?;
    operation.validated().map_err(|error| error.detail_text())?;
    if operation.deadline <= chrono::Utc::now().timestamp() {
        return Err("The extension operation expired.".into());
    }
    let grants: BTreeSet<String> = serde_json::from_str(
        &std::env::var("MRSM_EXECUTOR_PLUGINS")
            .map_err(|_| "No host-selected extension source grants.")?,
    )
    .map_err(|_| "Invalid host-selected extension source grants.")?;
    if grants != operation.catalog.plugins.keys().cloned().collect() {
        return Err("The extension source ceiling changed.".into());
    }
    let actual = runtime
        .config()
        .get(&[plugins::CONFIG_KEY])
        .ok_or("The extension source catalog is unavailable.")?;
    let actual: plugins::Catalog = serde_json::from_str(&actual.to_json_string())
        .map_err(|_| "Invalid extension source catalog.")?;
    if actual != operation.source_catalog() {
        return Err("The exact extension source view differs from its admitted source set.".into());
    }
    let session = crate::native::session(runtime);
    let connectors = runtime
        .config()
        .get(&["meerschaum", "connectors"])
        .map(|value| value.to_map())
        .unwrap_or_default();
    let callback = runtime
        .config()
        .get(&["meerschaum", "executor"])
        .map(|value| value.py_str())
        .ok_or("The private API callback transport is unavailable.")?;
    for (typ, labels) in connectors {
        for (label, attributes) in labels.to_map() {
            if attributes.is_null() {
                continue;
            }
            let keys = format!("{typ}:{label}");
            if keys == callback && typ == "api" {
                continue;
            }
            let attributes = attributes
                .as_map()
                .ok_or("The private API connector view is invalid.")?;
            if !matches!(typ.as_str(), "sql" | "api" | "valkey")
                || meerschaum_core::team_scope::api_execution::Alias::from_attributes(
                    attributes, &keys,
                )?
                .is_none()
            {
                let label = keys
                    .chars()
                    .filter(|character| !character.is_control())
                    .take(128)
                    .collect::<String>();
                return Err(format!(
                    "Native extension host services require API aliases for every logical instance (key={label}, callback_match={}).",
                    keys == callback
                ));
            }
        }
    }
    let handler = operation.handler().map_err(|error| error.detail_text())?;
    let principal = operation
        .caller
        .as_ref()
        .map(|caller| {
            serde_json::from_value::<meerschaum_plugin::api::Principal>(
                caller
                    .principal_json()
                    .map_err(|error| error.detail_text())?,
            )
            .map_err(|_| "Invalid exact native caller projection.".to_string())
        })
        .transpose()?;
    let caller_scopes: Vec<String> = operation
        .caller
        .as_ref()
        .map(|caller| caller.scopes.iter().cloned().collect())
        .unwrap_or_else(|| handler.scopes.iter().cloned().collect());
    if let (
        plugin_handlers::Selector::McpTool { name },
        plugin_handlers::Request::Mcp { arguments },
    ) = (&handler.selector, &operation.invocation.request)
    {
        let entry = operation.catalog.plugins[&operation.invocation.plugin]
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.mcp_tools.iter().find(|entry| entry.name == *name))
            .ok_or("The declared MCP tool is unavailable.")?;
        meerschaum_plugin::mcp::validate_arguments(&entry.input_schema, arguments)
            .map_err(|_| "The MCP tool arguments do not match their declared schema.")?;
    }
    let kind = operation.catalog.plugins[&operation.invocation.plugin].kind;
    let bytes = if matches!(
        kind,
        plugins::SourceKind::PythonModule | plugins::SourceKind::PythonPackage
    ) {
        let pool = session.pool(runtime.config());
        let reply = session
            .block_on(pool.call(
                "invoke_extension",
                meerschaum_worker::protocol::Msg::new().set("operation_json", encoded),
                Vec::new(),
                meerschaum_worker::CallOptions {
                    timeout: Some(std::time::Duration::from_secs(20)),
                    ..Default::default()
                },
                None,
            ))
            .map_err(|_| "The exact confined extension handler failed.")?;
        if !reply.frames.is_empty() || !reply.chunks.is_empty() {
            return Err("Extension handlers cannot return worker data frames.".into());
        }
        reply
            .value
            .as_str()
            .ok_or("Invalid confined extension response.")?
            .as_bytes()
            .to_vec()
    } else {
        let native = session
            .native_plugins(runtime.config())
            .map_err(|_| "The exact native extension source is unavailable.")?;
        let plugin = native
            .plugin(&operation.invocation.plugin)
            .map_err(|_| "The exact native extension source is unavailable.")?;
        require_native_declaration(&operation, native.manifest())?;
        let reply = crate::native_plugins::with_command_host(
            runtime,
            &native,
            None,
            |host| -> Result<_, String> {
                let host = Some(host as &dyn meerschaum_plugin::HostServices);
                Ok(match (&handler.selector, &operation.invocation.request) {
                (
                    plugin_handlers::Selector::Api {
                        method,
                        path: template,
                    },
                    plugin_handlers::Request::Api {
                        path,
                        query,
                        headers,
                        body,
                        ..
                    },
                ) => {
                    let route = operation.catalog.plugins[&operation.invocation.plugin]
                        .metadata
                        .as_ref()
                        .and_then(|metadata| {
                            metadata
                                .api_routes
                                .iter()
                                .find(|route| route.method == *method && route.path == *template)
                        })
                        .ok_or("The declared native route is unavailable.")?;
                    let mut query_params = std::collections::BTreeMap::<String, Vec<String>>::new();
                    for (key, value) in query {
                        query_params
                            .entry(key.clone())
                            .or_default()
                            .push(value.clone());
                    }
                    let response = plugin
                        .api_route_with_host(&meerschaum_plugin::api::RouteRequest {
                            name: route.name.clone(),
                            method: method.clone(),
                            path: path.clone(),
                            route_path: template.clone(),
                            path_params: meerschaum_server::native::path_params(template, path),
                            query_params,
                            headers: headers
                                .iter()
                                .map(|(key, value)| (key.clone(), vec![value.clone()]))
                                .collect(),
                            body: body.as_bytes().to_vec(),
                            principal: principal.clone(),
                            scopes: caller_scopes.clone(),
                            ..Default::default()
                        },host)
                        .map_err(|_| "The confined native API handler failed.")?;
                    plugin_handlers::Reply::Api {
                        status: response.status,
                        body: String::from_utf8(response.body)
                            .map_err(|_| "The extension API response is not UTF-8.")?,
                    }
                }
                (
                    plugin_handlers::Selector::McpTool { name },
                    plugin_handlers::Request::Mcp { arguments },
                ) => {
                    let reply = plugin
                        .mcp_tool_with_host(&meerschaum_plugin::mcp::ToolCall {
                            name: name.clone(),
                            arguments: arguments
                                .as_object()
                                .ok_or("Invalid MCP tool arguments.")?
                                .clone(),
                            principal: principal.clone(),
                            scopes: caller_scopes.clone(),
                        },host)
                        .map_err(|_| "The confined native MCP tool failed.")?;
                    plugin_handlers::Reply::Json { value: reply }
                }
                (
                    plugin_handlers::Selector::McpResource { name },
                    plugin_handlers::Request::Mcp { arguments },
                ) => {
                    let reply = plugin
                        .mcp_resource_with_host(&meerschaum_plugin::mcp::ResourceRead {
                            name: name.clone(),
                            uri: arguments["uri"]
                                .as_str()
                                .ok_or("Invalid MCP resource URI.")?
                                .into(),
                            variables: arguments["variables"]
                                .as_object()
                                .ok_or("Invalid MCP resource variables.")?
                                .clone(),
                            principal: principal.clone(),
                            scopes: caller_scopes.clone(),
                        },host)
                        .map_err(|_| "The confined native MCP resource failed.")?;
                    plugin_handlers::Reply::Json {
                        value: reply.to_json(),
                    }
                }
                (
                    plugin_handlers::Selector::McpPrompt { name },
                    plugin_handlers::Request::Mcp { arguments },
                ) => {
                    let (description, messages) = plugin
                        .mcp_prompt_with_host(&meerschaum_plugin::mcp::PromptGet {
                            name: name.clone(),
                            arguments: arguments
                                .as_object()
                                .ok_or("Invalid MCP prompt arguments.")?
                                .clone(),
                            principal: principal.clone(),
                            scopes: caller_scopes.clone(),
                        },host)
                        .map_err(|_| "The confined native MCP prompt failed.")?;
                    plugin_handlers::Reply::Json {
                        value: serde_json::json!({"description":description,"messages":messages}),
                    }
                }
                (plugin_handlers::Selector::Page {path,renderer:plugins::PageRenderer::Html},plugin_handlers::Request::Page {query})=>{
                    let function=native.manifest().get("plugins").and_then(|plugins|plugins.get(&operation.invocation.plugin)).and_then(|plugin|plugin.get("web_pages")).and_then(serde_json::Value::as_array)
                        .and_then(|pages|pages.iter().find(|page|page.get("path").and_then(serde_json::Value::as_str).is_some_and(|declared|declared.trim_matches('/').strip_prefix("dash/").unwrap_or(declared.trim_matches('/'))==path)))
                        .and_then(|page|page.get("function")).and_then(serde_json::Value::as_str);
                    let query=meerschaum_connector_api::wire::encode_query(&query.iter().map(|(key,value)|(key.as_str(),value.clone().into())).collect::<Vec<_>>());
                    let response=plugin.call_json_with_host("page",serde_json::json!({"path":path,"function":function,"query":query}),host).map_err(|_|"The confined native page handler failed.")?;
                    let fragment=response.get("html").and_then(serde_json::Value::as_str).ok_or("The native page returned no HTML fragment.")?.to_string();
                    let status=response.get("status").and_then(serde_json::Value::as_u64).map(|status|u16::try_from(status).map_err(|_|"Invalid native page status.")).transpose()?.unwrap_or(200);
                    plugin_handlers::Reply::Page {fragment,status}
                }
                _ => return Err(
                    "This native extension handler kind is not available in the private adapter."
                        .into(),
                ),
            })
            },
        )?;
        serde_json::to_vec(&reply).map_err(|_| "Invalid confined extension response.")?
    };
    operation
        .validate_reply(&bytes)
        .map_err(|error| error.detail_text())?;
    Ok(bytes)
}

fn require_native_declaration(
    operation: &extensions::Operation,
    manifest: &serde_json::Value,
) -> Result<(), String> {
    let handler = operation.handler().map_err(|error| error.detail_text())?;
    let actual = manifest
        .get("plugins")
        .and_then(|plugins| plugins.get(&operation.invocation.plugin))
        .ok_or("The actual native source declaration is unavailable.")?;
    let metadata = operation.catalog.plugins[&operation.invocation.plugin]
        .metadata
        .as_ref()
        .ok_or("The admitted native source declaration is unavailable.")?;
    let changed =
        || "The native handler declaration changed after credential-free discovery.".to_string();
    match handler.selector {
        plugin_handlers::Selector::Api { method, path } => {
            let expected = metadata
                .api_routes
                .iter()
                .find(|route| route.method == method && route.path == path)
                .ok_or_else(changed)?;
            let matches: Vec<_> = actual
                .get("api_routes")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter(|entry| {
                    entry
                        .get("method")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("GET")
                        .eq_ignore_ascii_case(&method)
                        && entry.get("path").and_then(serde_json::Value::as_str)
                            == Some(path.as_str())
                })
                .collect();
            if matches.len() != 1 {
                return Err(changed());
            }
            let current = matches[0];
            let scopes: Vec<String> = current
                .get("scopes")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()
                .map_err(|_| changed())?
                .unwrap_or_default();
            if current.get("name").and_then(serde_json::Value::as_str)
                != Some(expected.name.as_str())
                || scopes.iter().cloned().collect::<BTreeSet<_>>() != handler.scopes
            {
                return Err(changed());
            }
        }
        plugin_handlers::Selector::McpTool { name }
        | plugin_handlers::Selector::McpResource { name }
        | plugin_handlers::Selector::McpPrompt { name } => {
            let (key, entries) = match &operation
                .handler()
                .map_err(|error| error.detail_text())?
                .selector
            {
                plugin_handlers::Selector::McpTool { .. } => ("mcp_tools", &metadata.mcp_tools),
                plugin_handlers::Selector::McpResource { .. } => {
                    ("mcp_resources", &metadata.mcp_resources)
                }
                _ => ("mcp_prompts", &metadata.mcp_prompts),
            };
            let expected = entries
                .iter()
                .find(|entry| entry.name == name)
                .ok_or_else(changed)?;
            let matches: Vec<_> = actual
                .get(key)
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter(|entry| {
                    entry.get("name").and_then(serde_json::Value::as_str) == Some(name.as_str())
                })
                .collect();
            if matches.len() != 1 || &crate::executor_plugin_producer::mcp(matches[0])? != expected
            {
                return Err(changed());
            }
        }
        plugin_handlers::Selector::Page { path, .. } => {
            let matches = actual
                .get("web_pages")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter(|entry| {
                    entry
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|value| {
                            value
                                .trim_matches('/')
                                .strip_prefix("dash/")
                                .unwrap_or(value.trim_matches('/'))
                                == path
                        })
                })
                .count();
            if matches != 1 {
                return Err(changed());
            }
        }
        _ => return Err(changed()),
    }
    Ok(())
}
