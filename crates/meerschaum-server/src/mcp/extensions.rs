//! Per-request MCP adapter over exact confined extension declarations.

use super::{
    Caller, McpServer, NativeMcp, NativeResource, NativeResourceContents,
    registry::{Prompt, Resource, Source, Tool},
};
use crate::{
    AppState,
    error::ApiError,
    executor::{context, plugin_caller::RequestSource, plugin_handlers, plugins},
};
use async_trait::async_trait;
use meerschaum_core::{Map, Value};
use serde_json::Value as Json;
use std::sync::Arc;

struct Adapter {
    state: AppState,
    original: Arc<RequestSource>,
    catalog: Arc<plugins::Validated>,
}

fn core(value: Json) -> Result<Value, String> {
    Value::from_json_str(&value.to_string()).map_err(|_| "Invalid confined MCP result.".into())
}

impl Adapter {
    fn entries(&self, key: &str) -> Vec<Json> {
        let mut result = Vec::new();
        let mut names = std::collections::BTreeSet::new();
        let mut duplicates = std::collections::BTreeSet::new();
        for source in self.catalog.catalog().plugins.values() {
            let Some(metadata) = &source.metadata else {
                continue;
            };
            let Some(registry) = &metadata.registry else {
                continue;
            };
            let entries = match key {
                "mcp_tool" => &metadata.mcp_tools,
                "mcp_resource" => &metadata.mcp_resources,
                _ => &metadata.mcp_prompts,
            };
            for entry in entries {
                let declared = registry.handlers().any(|handler| match &handler.selector {
                    plugin_handlers::Selector::McpTool { name } => {
                        key == "mcp_tool" && name == &entry.name
                    }
                    plugin_handlers::Selector::McpResource { name } => {
                        key == "mcp_resource" && name == &entry.name
                    }
                    plugin_handlers::Selector::McpPrompt { name } => {
                        key == "mcp_prompt" && name == &entry.name
                    }
                    _ => false,
                });
                if !declared {
                    continue;
                }
                if !names.insert(entry.name.clone()) {
                    duplicates.insert(entry.name.clone());
                }
                let Ok(mut value) = serde_json::to_value(entry) else {
                    continue;
                };
                // Permission to inspect/use the exact plugin source is retained with the
                // handler's declared scopes. Unknown classifications never add entries.
                let mut scopes = entry.scopes.clone();
                scopes.push("plugins:read".into());
                scopes.sort();
                scopes.dedup();
                value["scopes"] = serde_json::json!(scopes);
                if let Some(annotations) =
                    value.get("annotations").and_then(Json::as_object).cloned()
                {
                    for (field, key) in [
                        ("read_only", "readOnlyHint"),
                        ("destructive", "destructiveHint"),
                        ("idempotent", "idempotentHint"),
                        ("open_world", "openWorldHint"),
                    ] {
                        if let Some(observed) = annotations.get(key).and_then(Json::as_bool) {
                            value[field] = observed.into();
                        }
                    }
                }
                result.push(value);
            }
        }
        result.retain(|entry| {
            entry["name"]
                .as_str()
                .is_some_and(|name| !duplicates.contains(name))
        });
        result
    }

    async fn call(&self, kind: &str, name: &str, arguments: Json) -> Result<Json, String> {
        let mut selected = None;
        for (plugin, source) in &self.catalog.catalog().plugins {
            let Some(registry) = source
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.registry.as_ref())
            else {
                continue;
            };
            for handler in registry.handlers() {
                let matches = match &handler.selector {
                    plugin_handlers::Selector::McpTool { name: declared } => {
                        kind == "mcp_tool" && declared == name
                    }
                    plugin_handlers::Selector::McpResource { name: declared } => {
                        kind == "mcp_resource" && declared == name
                    }
                    plugin_handlers::Selector::McpPrompt { name: declared } => {
                        kind == "mcp_prompt" && declared == name
                    }
                    _ => false,
                };
                if matches {
                    if selected.is_some() {
                        return Err("Multiple admitted plugins declare this MCP name.".into());
                    }
                    selected = Some((plugin.clone(), handler.id.clone()));
                }
            }
        }
        let (plugin, handler_id) =
            selected.ok_or("The exact declared MCP handler is unavailable.")?;
        let invocation = plugin_handlers::Invocation {
            catalog_sha256: self
                .catalog
                .source_digest()
                .map_err(|_| "Invalid MCP source identity.")?,
            plugin,
            handler_id,
            request: plugin_handlers::Request::Mcp { arguments },
        };
        match crate::routes::extensions::execute_source(&self.state, &self.original, invocation)
            .await
            .map_err(|error| error.detail_text())?
        {
            plugin_handlers::Reply::Json { value } => Ok(value),
            _ => Err("The MCP handler returned another result kind.".into()),
        }
    }
}

#[async_trait]
impl NativeMcp for Adapter {
    fn tools(&self) -> Vec<Tool> {
        self.entries("mcp_tool")
            .into_iter()
            .filter_map(|entry| Tool::from_value(&core(entry).ok()?, Source::Native))
            .collect()
    }
    fn resources(&self) -> Vec<Resource> {
        self.entries("mcp_resource")
            .into_iter()
            .filter_map(|entry| Resource::from_value(&core(entry).ok()?, Source::Native))
            .collect()
    }
    fn prompts(&self) -> Vec<Prompt> {
        self.entries("mcp_prompt")
            .into_iter()
            .filter_map(|entry| Prompt::from_value(&core(entry).ok()?, Source::Native))
            .collect()
    }
    async fn call_tool(
        &self,
        name: &str,
        arguments: &Map,
        _caller: &Caller,
    ) -> Result<Value, String> {
        core(
            self.call(
                "mcp_tool",
                name,
                serde_json::from_str(&Value::Map(arguments.clone()).to_json_string())
                    .map_err(|_| "Invalid MCP arguments.")?,
            )
            .await?,
        )
    }
    async fn read_resource(
        &self,
        name: &str,
        uri: &str,
        variables: &Map,
        _caller: &Caller,
    ) -> Result<NativeResource, String> {
        let value = self
            .call(
                "mcp_resource",
                name,
                serde_json::json!({"uri":uri,"variables":serde_json::from_str::<Json>(&Value::Map(variables.clone()).to_json_string()).map_err(|_|"Invalid MCP variables.")?}),
            )
            .await?;
        let mime_type = value
            .get("mime_type")
            .and_then(Json::as_str)
            .map(str::to_string);
        let contents = if let Some(text) = value.get("text").and_then(Json::as_str) {
            NativeResourceContents::Text(text.into())
        } else if let Some(blob) = value.get("blob_base64").and_then(Json::as_str) {
            NativeResourceContents::Blob(blob.into())
        } else {
            return Err("The confined resource returned invalid typed contents.".into());
        };
        Ok(NativeResource {
            contents,
            mime_type,
        })
    }
    async fn get_prompt(
        &self,
        name: &str,
        arguments: &Map,
        _caller: &Caller,
    ) -> Result<(Option<String>, Vec<Value>), String> {
        let value = self
            .call(
                "mcp_prompt",
                name,
                serde_json::from_str(&Value::Map(arguments.clone()).to_json_string())
                    .map_err(|_| "Invalid MCP prompt arguments.")?,
            )
            .await?;
        let description = value
            .get("description")
            .and_then(Json::as_str)
            .map(str::to_string);
        let messages = value
            .as_array()
            .or_else(|| value.get("messages").and_then(Json::as_array))
            .ok_or("The confined prompt returned no messages.")?;
        Ok((
            description,
            messages
                .iter()
                .cloned()
                .map(core)
                .collect::<Result<_, _>>()?,
        ))
    }
}

/// Replace only this confined request's plugin adapters, retaining the ordinary built-ins.
/// No global registry, Main Python source import or other caller's cache is changed.
pub(super) async fn scoped(
    state: &AppState,
    server: Arc<McpServer>,
    mut caller: Caller,
    original: Option<Arc<RequestSource>>,
) -> Result<(Arc<McpServer>, Caller, Option<Arc<plugins::Validated>>), ApiError> {
    if !caller.plugins_hidden {
        return Ok((server, caller, None));
    }
    let original = original.ok_or_else(ApiError::invalid_credentials)?;
    let access = original.refresh(state, &[]).await?;
    if !access.has("plugins:read") {
        return Ok((server, caller, None));
    }
    let catalog = context::scope(Some(access), state.actions.extension_catalog())
        .await
        .map_err(ApiError::unavailable)?;
    let Some(catalog) = catalog else {
        return Ok((server, caller, None));
    };
    let catalog = Arc::new(catalog);
    let current = original.refresh(state, &["plugins:read"]).await?;
    crate::routes::extensions::confirm_catalog_source(&catalog, &current)?;
    let scoped = McpServer {
        config: server.config.clone(),
        backend: server.backend.clone(),
        federation: None,
        native: Some(Arc::new(Adapter {
            state: state.clone(),
            original,
            catalog: catalog.clone(),
        })),
    };
    caller.plugins_hidden = false;
    Ok((Arc::new(scoped), caller, Some(catalog)))
}
