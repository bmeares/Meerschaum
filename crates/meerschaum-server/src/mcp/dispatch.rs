//! Transport-independent JSON-RPC dispatch (`meerschaum/mcp/_dispatch.py`).
//!
//! Both transports call [`McpServer::handle_payload`]; only the [`Caller`] differs. Built-in
//! entries run here; native plugin entries are called through [`super::NativeMcp`]; entries from
//! the Python API host are forwarded to it ([`super::federation`]).

use meerschaum_core::pyfmt::{JsonStyle, json_dumps};
use meerschaum_core::{Map, Value};

use super::protocol::{
    INTERNAL_ERROR, INVALID_PARAMS, INVALID_REQUEST, METHOD_NOT_FOUND, error, has_scope, map_of,
    paginate, result, s, tool_error,
};
use super::registry::{Prompt, Resource, Source, Tool, builtin, primer};
use super::tools::{self, ToolCx};
use super::{Caller, McpServer, prompts, resources};
use crate::state::AppState;

/// Every tool: the built-ins (v4's with v5's changes, then v5's), the native plugins', then the
/// host's (an entry never shadows an earlier one of the same name).
pub async fn all_tools(server: &McpServer) -> Vec<Tool> {
    let mut out = super::registry::tools().to_vec();
    if let Some(native) = &server.native {
        for t in native.tools() {
            if !out.iter().any(|b| b.name == t.name) {
                out.push(t);
            }
        }
    }
    if let Some(f) = &server.federation {
        let reg = f.registry().await;
        for t in &reg.tools {
            if !out.iter().any(|b| b.name == t.name) {
                out.push(t.clone());
            }
        }
    }
    out
}

/// Every resource and template: the built-ins, the native plugins', then the host's.
pub async fn all_resources(server: &McpServer) -> Vec<Resource> {
    let mut out = builtin().resources.clone();
    if let Some(native) = &server.native {
        out.extend(native.resources());
    }
    if let Some(f) = &server.federation {
        out.extend(f.registry().await.resources.iter().cloned());
    }
    out
}

/// Every prompt: the built-ins, the native plugins', then the host's.
pub async fn all_prompts(server: &McpServer) -> Vec<Prompt> {
    let mut out = builtin().prompts.clone();
    if let Some(native) = &server.native {
        for p in native.prompts() {
            if !out.iter().any(|b| b.name == p.name) {
                out.push(p);
            }
        }
    }
    if let Some(f) = &server.federation {
        let reg = f.registry().await;
        for p in &reg.prompts {
            if !out.iter().any(|b| b.name == p.name) {
                out.push(p.clone());
            }
        }
    }
    out
}

/// Whether a tool's HTTP route group is served (always over stdio, and for v4's tools).
fn tool_group_served(state: &AppState, caller: &Caller, tool: &Tool) -> bool {
    if !caller.api || tool.source != Source::Builtin {
        return true;
    }
    super::v5::route_group(&tool.name).is_none_or(|g| state.config.route_group_is_allowed(g))
}

impl McpServer {
    /// `get_visible_tools(current_scopes)`: the tools the caller may call (hidden rather than
    /// advertised and refused). Over HTTP, a tool v5 adds is also hidden when the server does not
    /// serve the route group of its HTTP equivalent.
    pub async fn visible_tools(&self, state: &AppState, caller: &Caller) -> Vec<Tool> {
        all_tools(self)
            .await
            .into_iter()
            .filter(|t| {
                has_scope(&caller.scopes, &t.scopes)
                    && (t.read_only || !self.config.read_only)
                    && tool_group_served(state, caller, t)
            })
            .collect()
    }

    /// `get_visible_resources(current_scopes)`.
    pub async fn visible_resources(&self, caller: &Caller) -> Vec<Resource> {
        all_resources(self)
            .await
            .into_iter()
            .filter(|r| has_scope(&caller.scopes, &r.scopes))
            .collect()
    }

    /// `get_visible_prompts(current_scopes)`: a prompt that would drive write tools is hidden on
    /// a read-only server.
    pub async fn visible_prompts(&self, caller: &Caller) -> Vec<Prompt> {
        let read_only = self.config.read_only;
        all_prompts(self)
            .await
            .into_iter()
            .filter(|p| {
                has_scope(&caller.scopes, &p.scopes)
                    && !(read_only
                        && p.scopes.iter().any(|sc| {
                            [":write", ":delete", ":drop", ":execute"]
                                .iter()
                                .any(|suffix| sc.ends_with(suffix))
                        }))
            })
            .collect()
    }

    /// The capabilities advertised by `initialize` and `GET /mcp`.
    pub fn capabilities() -> Value {
        let list_changed = || Value::Map(map_of([("listChanged", Value::Bool(false))]));
        Value::Map(map_of([
            ("tools", list_changed()),
            (
                "resources",
                Value::Map(map_of([
                    ("listChanged", Value::Bool(false)),
                    ("subscribe", Value::Bool(false)),
                ])),
            ),
            ("prompts", list_changed()),
        ]))
    }

    /// `serverInfo`.
    pub fn server_info(state: &AppState) -> Value {
        Value::Map(map_of([
            ("name", s(builtin().server_name.clone())),
            ("title", s("Meerschaum")),
            ("version", s(state.config.version.clone())),
        ]))
    }

    fn initialize(&self, state: &AppState, params: &Map) -> Value {
        let b = builtin();
        let requested = params
            .get("protocolVersion")
            .map(Value::py_str)
            .unwrap_or_else(|| b.protocol_version.clone());
        let version = if b.supported_protocol_versions.contains(&requested) {
            requested
        } else {
            b.protocol_version.clone()
        };
        Value::Map(map_of([
            ("protocolVersion", s(version)),
            ("capabilities", McpServer::capabilities()),
            ("serverInfo", McpServer::server_info(state)),
            ("instructions", s(primer().to_string())),
        ]))
    }

    /// `handle_payload(payload, current_scopes)`: a single message or a batch. `None` when
    /// nothing is sent back (only notifications).
    pub async fn handle_payload(
        &self,
        state: &AppState,
        payload: Value,
        caller: &Caller,
    ) -> Option<Value> {
        match payload {
            Value::List(messages) => {
                let mut out = Vec::new();
                for m in messages {
                    if let Some(r) = self.handle_message(state, m, caller).await {
                        out.push(r);
                    }
                }
                (!out.is_empty()).then_some(Value::List(out))
            }
            other => self.handle_message(state, other, caller).await,
        }
    }

    /// `handle_message(message, current_scopes)`.
    pub async fn handle_message(
        &self,
        state: &AppState,
        message: Value,
        caller: &Caller,
    ) -> Option<Value> {
        let Value::Map(message) = message else {
            return Some(error(
                Value::Null,
                INVALID_REQUEST,
                "Request must be a JSON object.",
            ));
        };
        let id = message.get("id").cloned().unwrap_or_default();
        let method = message.get("method").map(Value::py_str).unwrap_or_default();
        let params = match message.get("params") {
            Some(p) if p.truthy() => match p {
                Value::Map(m) => m.clone(),
                _ => return Some(error(id, INVALID_PARAMS, "'params' must be an object.")),
            },
            _ => Map::new(),
        };
        // Notifications carry no `id` and get no response.
        if !message.contains_key("id") || method.starts_with("notifications/") {
            return None;
        }
        Some(match method.as_str() {
            "initialize" => result(id, self.initialize(state, &params)),
            "ping" => result(id, Value::new_map()),
            "tools/list" => {
                let mut tools = self.visible_tools(state, caller).await;
                tools.sort_by(|a, b| a.name.cmp(&b.name));
                let b = builtin();
                match paginate(&tools, params.get("cursor"), 100, b.max_cursor_offset) {
                    Err(e) => error(id, INVALID_PARAMS, e),
                    Ok((page, next)) => {
                        let mut r = map_of([(
                            "tools",
                            Value::List(page.iter().map(Tool::to_dict).collect()),
                        )]);
                        if let Some(n) = next {
                            r.insert("nextCursor".into(), s(n));
                        }
                        result(id, Value::Map(r))
                    }
                }
            }
            "tools/call" => self.call_tool(state, &params, caller, id, &message).await,
            "resources/list" | "resources/templates/list" => {
                let templates = method == "resources/templates/list";
                let list: Vec<Value> = self
                    .visible_resources(caller)
                    .await
                    .iter()
                    .filter(|r| r.is_template() == templates)
                    .map(Resource::to_dict)
                    .collect();
                let key = if templates {
                    "resourceTemplates"
                } else {
                    "resources"
                };
                result(id, Value::Map(map_of([(key, Value::List(list))])))
            }
            "resources/read" => {
                self.read_resource(state, &params, caller, id, &message)
                    .await
            }
            "prompts/list" => {
                let mut prompts = self.visible_prompts(caller).await;
                prompts.sort_by(|a, b| a.name.cmp(&b.name));
                result(
                    id,
                    Value::Map(map_of([(
                        "prompts",
                        Value::List(prompts.iter().map(Prompt::to_dict).collect()),
                    )])),
                )
            }
            "prompts/get" => self.get_prompt(&params, caller, id, &message).await,
            other => error(id, METHOD_NOT_FOUND, format!("Method not found: {other}")),
        })
    }

    async fn call_tool(
        &self,
        state: &AppState,
        params: &Map,
        caller: &Caller,
        id: Value,
        message: &Map,
    ) -> Value {
        let name = params.get("name").map(Value::py_str).unwrap_or_default();
        let arguments = match params.get("arguments") {
            Some(a) if a.truthy() => a.clone(),
            _ => Value::new_map(),
        };
        let Some(tool) = all_tools(self).await.into_iter().find(|t| t.name == name) else {
            return tool_error(id, format!("Unknown tool: '{name}'."));
        };
        if !has_scope(&caller.scopes, &tool.scopes) {
            return tool_error(
                id,
                format!(
                    "Missing required scope for '{name}': {}.",
                    tool.scopes.join(", ")
                ),
            );
        }
        if self.config.read_only && !tool.read_only {
            return tool_error(
                id,
                format!(
                    "'{name}' modifies data, and this MCP server is running in read-only mode."
                ),
            );
        }
        if !tool_group_served(state, caller, &tool) {
            return tool_error(
                id,
                format!("'{name}' is not available on this server (its routes are not served)."),
            );
        }
        let Value::Map(arguments) = arguments else {
            return tool_error(id, "Tool arguments must be an object.");
        };
        for required in tool.required() {
            if !arguments.contains_key(&required) {
                return tool_error(id, format!("Missing required argument '{required}'."));
            }
        }
        if tool.source == Source::Host {
            return self.forward(message, caller, id).await;
        }
        let value = if tool.source == Source::Native {
            let Some(native) = &self.native else {
                return tool_error(id, format!("Unknown tool: '{name}'."));
            };
            match native.call_tool(&name, &arguments, caller).await {
                Ok(v) => v,
                Err(e) => return tool_error(id, e),
            }
        } else {
            let cx = ToolCx {
                server: self,
                state,
                caller,
            };
            match tools::call(&name, &arguments, &cx).await {
                Ok(v) => v,
                Err(e) if e.kind == "TypeError" => {
                    return tool_error(
                        id,
                        format!("Invalid arguments for '{name}': {}", e.message),
                    );
                }
                Err(e) => return tool_error(id, format!("{}: {}", e.kind, e.message)),
            }
        };
        // A native tool's text result is sent as is; other results as indented JSON.
        let text = match (&value, tool.source) {
            (Value::Str(text), Source::Native) => text.clone(),
            _ => json_dumps(&value, JsonStyle::Indent(2), true),
        };
        let mut r = map_of([
            (
                "content",
                Value::List(vec![Value::Map(map_of([
                    ("type", s("text")),
                    ("text", s(text)),
                ]))]),
            ),
            ("isError", Value::Bool(false)),
        ]);
        // Only advertise `structuredContent` when the tool declared an output schema.
        if tool.output_schema.is_some() && value.is_map() {
            r.insert("structuredContent".into(), value);
        }
        result(id, Value::Map(r))
    }

    async fn read_resource(
        &self,
        state: &AppState,
        params: &Map,
        caller: &Caller,
        id: Value,
        message: &Map,
    ) -> Value {
        let uri = params.get("uri").filter(|u| u.truthy()).map(Value::py_str);
        let Some(uri) = uri else {
            return error(id, INVALID_PARAMS, "A resource 'uri' is required.");
        };
        for res in self.visible_resources(caller).await {
            let Some(vars) = res.matches(&uri) else {
                continue;
            };
            if res.source == Source::Host {
                return self.forward(message, caller, id).await;
            }
            if res.source == Source::Native {
                return self
                    .read_native_resource(&res, &uri, &vars, caller, id)
                    .await;
            }
            let cx = ToolCx {
                server: self,
                state,
                caller,
            };
            return match resources::read(&res.name, &vars, &cx).await {
                Ok(text) => result(
                    id,
                    Value::Map(map_of([(
                        "contents",
                        Value::List(vec![Value::Map(map_of([
                            ("uri", s(uri)),
                            ("mimeType", s(res.mime_type.clone())),
                            ("text", s(text)),
                        ]))]),
                    )])),
                ),
                Err(e) => error(id, INTERNAL_ERROR, format!("{}: {}", e.kind, e.message)),
            };
        }
        error(id, INVALID_PARAMS, format!("Resource not found: {uri}"))
    }

    async fn get_prompt(&self, params: &Map, caller: &Caller, id: Value, message: &Map) -> Value {
        let name = params.get("name").map(Value::py_str).unwrap_or_default();
        let arguments = match params.get("arguments") {
            Some(Value::Map(m)) if !m.is_empty() => m.clone(),
            _ => Map::new(),
        };
        let Some(prompt) = self
            .visible_prompts(caller)
            .await
            .into_iter()
            .find(|p| p.name == name)
        else {
            return error(id, INVALID_PARAMS, format!("Unknown prompt: '{name}'."));
        };
        // Validate against the declared arguments rather than the handler's signature.
        let declared = prompt.argument_names();
        let mut unknown: Vec<&String> =
            arguments.keys().filter(|k| !declared.contains(k)).collect();
        if !unknown.is_empty() {
            unknown.sort();
            let mut accepted = declared.clone();
            accepted.sort();
            let accepted = if accepted.is_empty() {
                "none".to_string()
            } else {
                accepted.join(", ")
            };
            return error(
                id,
                INVALID_PARAMS,
                format!(
                    "Unknown argument(s) for '{name}': {}. Accepted: {accepted}.",
                    unknown
                        .iter()
                        .map(|u| u.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            );
        }
        let missing: Vec<String> = prompt
            .arguments
            .iter()
            .filter(|a| a.get("required").is_some_and(Value::truthy))
            .filter_map(|a| a.get("name").map(Value::py_str))
            .filter(|n| !arguments.get(n).is_some_and(Value::truthy))
            .collect();
        if !missing.is_empty() {
            return error(
                id,
                INVALID_PARAMS,
                format!(
                    "Missing required argument(s) for '{name}': {}.",
                    missing.join(", ")
                ),
            );
        }
        if prompt.source == Source::Host {
            return self.forward(message, caller, id).await;
        }
        if prompt.source == Source::Native {
            let Some(native) = &self.native else {
                return error(id, INVALID_PARAMS, format!("Unknown prompt: '{name}'."));
            };
            return match native.get_prompt(&name, &arguments, caller).await {
                Ok((description, messages)) => result(
                    id,
                    Value::Map(map_of([
                        (
                            "description",
                            s(description.unwrap_or_else(|| prompt.description.clone())),
                        ),
                        ("messages", Value::List(messages)),
                    ])),
                ),
                Err(e) => error(id, INTERNAL_ERROR, e),
            };
        }
        match prompts::render(&name, &arguments) {
            Some(messages) => result(
                id,
                Value::Map(map_of([
                    ("description", s(prompt.description.clone())),
                    ("messages", messages),
                ])),
            ),
            None => error(id, INVALID_PARAMS, format!("Unknown prompt: '{name}'.")),
        }
    }

    async fn read_native_resource(
        &self,
        res: &Resource,
        uri: &str,
        vars: &Map,
        caller: &Caller,
        id: Value,
    ) -> Value {
        let Some(native) = &self.native else {
            return error(id, INVALID_PARAMS, format!("Resource not found: {uri}"));
        };
        match native.read_resource(&res.name, uri, vars, caller).await {
            Ok(read) => {
                let (key, contents) = match read.contents {
                    super::NativeResourceContents::Text(text) => ("text", text),
                    super::NativeResourceContents::Blob(blob) => ("blob", blob),
                };
                result(
                    id,
                    Value::Map(map_of([(
                        "contents",
                        Value::List(vec![Value::Map(map_of([
                            ("uri", s(uri)),
                            (
                                "mimeType",
                                s(read.mime_type.unwrap_or_else(|| res.mime_type.clone())),
                            ),
                            (key, s(contents)),
                        ]))]),
                    )])),
                )
            }
            Err(e) => error(id, INTERNAL_ERROR, e),
        }
    }

    async fn forward(&self, message: &Map, caller: &Caller, id: Value) -> Value {
        match &self.federation {
            Some(f) => f
                .forward(&Value::Map(message.clone()), caller)
                .await
                .unwrap_or_else(|e| error(id, INTERNAL_ERROR, format!("The API host failed: {e}"))),
            None => error(id, INTERNAL_ERROR, "The API host is not available."),
        }
    }
}
