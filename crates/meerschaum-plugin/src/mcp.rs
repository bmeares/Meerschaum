//! Native MCP tools, resources and prompts: the `mcp_tools`, `mcp_resources` and `mcp_prompts`
//! manifest entries and the `mcp_tool`, `mcp_resource` and `mcp_prompt` call kinds.
//!
//! The server lists the entries from the cached manifest (no code is loaded to list them), hides
//! the ones the caller's scopes do not cover, and calls the plugin with JSON:
//!
//! | Call kind | Context | Reply |
//! |---|---|---|
//! | `mcp_tool` | [`ToolCall`] | any JSON value (the tool's result) |
//! | `mcp_resource` | [`ResourceRead`] | a string, `{"text", "mime_type"?}` or `{"blob_base64", "mime_type"?}` |
//! | `mcp_prompt` | [`PromptGet`] | a string, a list of messages or `{"description"?, "messages"}` |
//!
//! Tool arguments are validated against the tool's `input_schema` before the call.
use crate::{Result, api::Principal, error};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

/// One `mcp_tools` manifest entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    /// The JSON Schema of the arguments (an object schema).
    #[serde(default = "object_schema")]
    pub input_schema: Value,
    /// The JSON Schema of an object result; when set, the result is also `structuredContent`.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub output_schema: Value,
    /// The scopes a caller must hold to see and call the tool.
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// MCP annotations: `readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`.
    /// A tool without `readOnlyHint: true` is hidden by a read-only MCP server.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub annotations: Value,
}

fn object_schema() -> Value {
    json!({"type": "object", "properties": {}})
}

impl ToolSpec {
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            input_schema: object_schema(),
            output_schema: Value::Null,
            scopes: Vec::new(),
            title: None,
            annotations: Value::Null,
        }
    }
    pub fn input_schema(mut self, schema: Value) -> Self {
        self.input_schema = schema;
        self
    }
    pub fn scopes<S: Into<String>>(mut self, scopes: impl IntoIterator<Item = S>) -> Self {
        self.scopes = scopes.into_iter().map(Into::into).collect();
        self
    }
    /// Mark the tool read-only (`readOnlyHint`), so read-only servers still offer it.
    pub fn read_only(mut self) -> Self {
        if !self.annotations.is_object() {
            self.annotations = json!({});
        }
        self.annotations["readOnlyHint"] = Value::Bool(true);
        self
    }
    /// The annotation `key` as a flag.
    pub fn hint(&self, key: &str) -> bool {
        self.annotations
            .get(key)
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
    /// Check `arguments` against [`ToolSpec::input_schema`].
    pub fn validate(&self, arguments: &Value) -> Result<()> {
        validate_arguments(&self.input_schema, arguments)
    }
}

/// One `mcp_resources` manifest entry: a static `uri` or an RFC 6570 `uri_template` whose
/// `{var}` segments match `[^/]+`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceSpec {
    pub name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri_template: Option<String>,
    /// Default `application/json`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

/// A declared prompt argument.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptArgument {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub required: bool,
}

/// One `mcp_prompts` manifest entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default)]
    pub arguments: Vec<PromptArgument>,
    #[serde(default)]
    pub scopes: Vec<String>,
}

/// The MCP entries of a plugin's manifest entry.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct McpEntries {
    pub tools: Vec<ToolSpec>,
    pub resources: Vec<ResourceSpec>,
    pub prompts: Vec<PromptSpec>,
}

/// Read `mcp_tools`, `mcp_resources` and `mcp_prompts` from a plugin's manifest entry.
pub fn entries_of(entry: &Value) -> Result<McpEntries> {
    fn list<T: serde::de::DeserializeOwned>(entry: &Value, key: &str) -> Result<Vec<T>> {
        match entry.get(key) {
            None | Some(Value::Null) => Ok(Vec::new()),
            Some(v) => serde_json::from_value(v.clone()).map_err(|e| error(format!("{key}: {e}"))),
        }
    }
    Ok(McpEntries {
        tools: list(entry, "mcp_tools")?,
        resources: list(entry, "mcp_resources")?,
        prompts: list(entry, "mcp_prompts")?,
    })
}

/// Validate tool arguments against a JSON Schema; the error lists every violation.
pub fn validate_arguments(schema: &Value, arguments: &Value) -> Result<()> {
    let validator = jsonschema::validator_for(schema)
        .map_err(|e| error(format!("invalid input schema: {e}")))?;
    let problems: Vec<String> = validator
        .iter_errors(arguments)
        .map(|e| {
            let at = e.instance_path().to_string();
            if at.is_empty() {
                e.to_string()
            } else {
                format!("{at}: {e}")
            }
        })
        .collect();
    if problems.is_empty() {
        Ok(())
    } else {
        Err(error(problems.join("; ")))
    }
}

/// The context of an `mcp_tool` call.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub name: String,
    /// The arguments, already validated against the input schema.
    #[serde(default)]
    pub arguments: Map<String, Value>,
    /// The caller (`None` over stdio and under `--no-auth`).
    #[serde(default)]
    pub principal: Option<Principal>,
    /// The caller's scopes (`["*"]` over stdio).
    #[serde(default)]
    pub scopes: Vec<String>,
}

impl ToolCall {
    /// Deserialize the arguments into a typed struct.
    pub fn arguments<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_value(Value::Object(self.arguments.clone())).map_err(error)
    }
    /// The argument `name`.
    pub fn argument(&self, name: &str) -> Option<&Value> {
        self.arguments.get(name)
    }
}

/// The context of an `mcp_resource` call.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceRead {
    /// The [`ResourceSpec::name`] the URI matched.
    pub name: String,
    pub uri: String,
    /// The template variables (`{}` for a static URI).
    #[serde(default)]
    pub variables: Map<String, Value>,
    #[serde(default)]
    pub principal: Option<Principal>,
    #[serde(default)]
    pub scopes: Vec<String>,
}

impl ResourceRead {
    /// The template variable `name` as a string.
    pub fn variable(&self, name: &str) -> Option<&str> {
        self.variables.get(name).and_then(Value::as_str)
    }
}

/// What a resource read returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceContents {
    /// Text, with an optional MIME type (default: the resource's).
    Text {
        text: String,
        mime_type: Option<String>,
    },
    /// Binary contents (sent base64-encoded as an MCP `blob`).
    Blob {
        bytes: Vec<u8>,
        mime_type: Option<String>,
    },
}

impl ResourceContents {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text {
            text: text.into(),
            mime_type: None,
        }
    }
    /// Pretty JSON text (`application/json`).
    pub fn json(value: &impl Serialize) -> Self {
        Self::Text {
            text: serde_json::to_string_pretty(value).unwrap_or_default(),
            mime_type: Some("application/json".into()),
        }
    }
    pub fn blob(bytes: impl Into<Vec<u8>>, mime_type: impl Into<String>) -> Self {
        Self::Blob {
            bytes: bytes.into(),
            mime_type: Some(mime_type.into()),
        }
    }
    /// The wire reply.
    pub fn to_json(&self) -> Value {
        match self {
            Self::Text { text, mime_type } => json!({"text": text, "mime_type": mime_type}),
            Self::Blob { bytes, mime_type } => {
                json!({"blob_base64": STANDARD.encode(bytes), "mime_type": mime_type})
            }
        }
    }
    /// Parse a reply (see the module table).
    pub fn from_reply(value: &Value) -> Result<Self> {
        let mime = || {
            value
                .get("mime_type")
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        Ok(match value {
            Value::String(text) => Self::text(text.clone()),
            Value::Object(m) if m.contains_key("blob_base64") => Self::Blob {
                bytes: STANDARD
                    .decode(m["blob_base64"].as_str().unwrap_or_default())
                    .map_err(error)?,
                mime_type: mime(),
            },
            Value::Object(m) if m.get("text").is_some_and(Value::is_string) => Self::Text {
                text: m["text"].as_str().unwrap_or_default().to_owned(),
                mime_type: mime(),
            },
            other => Self::json(other),
        })
    }
}

/// The context of an `mcp_prompt` call.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PromptGet {
    pub name: String,
    /// The arguments (MCP prompt arguments are strings); only declared names reach the plugin.
    #[serde(default)]
    pub arguments: Map<String, Value>,
    #[serde(default)]
    pub principal: Option<Principal>,
    #[serde(default)]
    pub scopes: Vec<String>,
}

impl PromptGet {
    pub fn argument(&self, name: &str) -> Option<&str> {
        self.arguments.get(name).and_then(Value::as_str)
    }
}

/// One prompt message: `{"role": "user" | "assistant", "content": {"type": "text", "text"}}`.
pub fn message(role: &str, text: impl Into<String>) -> Value {
    json!({"role": role, "content": {"type": "text", "text": text.into()}})
}

/// Normalize a prompt reply to `(description, messages)`.
pub fn prompt_reply(value: &Value) -> Result<(Option<String>, Vec<Value>)> {
    let check = |messages: &[Value]| -> Result<Vec<Value>> {
        messages
            .iter()
            .map(|m| match m {
                Value::String(text) => Ok(message("user", text.clone())),
                Value::Object(o) if o.contains_key("role") && o.contains_key("content") => {
                    Ok(m.clone())
                }
                other => Err(error(format!("invalid prompt message: {other}"))),
            })
            .collect()
    };
    match value {
        Value::String(text) => Ok((None, vec![message("user", text.clone())])),
        Value::Array(items) => Ok((None, check(items)?)),
        Value::Object(o) if o.get("messages").is_some_and(Value::is_array) => Ok((
            o.get("description")
                .and_then(Value::as_str)
                .map(str::to_owned),
            check(
                o["messages"]
                    .as_array()
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
            )?,
        )),
        other => Err(error(format!("invalid prompt reply: {other}"))),
    }
}

/// Return types accepted from a tool handler: any `Serialize` value, optionally in a `Result`.
pub trait IntoToolResult {
    fn into_tool_result(self) -> Result<Value>;
}
impl<T: Serialize> IntoToolResult for Result<T> {
    fn into_tool_result(self) -> Result<Value> {
        self.and_then(|v| serde_json::to_value(v).map_err(error))
    }
}
impl IntoToolResult for Value {
    fn into_tool_result(self) -> Result<Value> {
        Ok(self)
    }
}
impl IntoToolResult for String {
    fn into_tool_result(self) -> Result<Value> {
        Ok(Value::String(self))
    }
}

/// Return types accepted from a resource handler.
pub trait IntoResourceContents {
    fn into_resource_contents(self) -> Result<ResourceContents>;
}
impl IntoResourceContents for ResourceContents {
    fn into_resource_contents(self) -> Result<ResourceContents> {
        Ok(self)
    }
}
impl IntoResourceContents for String {
    fn into_resource_contents(self) -> Result<ResourceContents> {
        Ok(ResourceContents::text(self))
    }
}
impl IntoResourceContents for Value {
    fn into_resource_contents(self) -> Result<ResourceContents> {
        ResourceContents::from_reply(&self)
    }
}
impl<T: IntoResourceContents> IntoResourceContents for Result<T> {
    fn into_resource_contents(self) -> Result<ResourceContents> {
        self?.into_resource_contents()
    }
}

/// Return types accepted from a prompt handler: a string (one user message), a list of
/// messages, or `{"description"?, "messages"}` as a JSON `Value`.
pub trait IntoPromptReply {
    fn into_prompt_reply(self) -> Result<Value>;
}
impl IntoPromptReply for String {
    fn into_prompt_reply(self) -> Result<Value> {
        Ok(Value::String(self))
    }
}
impl IntoPromptReply for Value {
    fn into_prompt_reply(self) -> Result<Value> {
        prompt_reply(&self)?;
        Ok(self)
    }
}
impl IntoPromptReply for Vec<Value> {
    fn into_prompt_reply(self) -> Result<Value> {
        Value::Array(self).into_prompt_reply()
    }
}
impl<T: IntoPromptReply> IntoPromptReply for Result<T> {
    fn into_prompt_reply(self) -> Result<Value> {
        self?.into_prompt_reply()
    }
}

impl crate::LoadedPlugin {
    /// Call the tool `call.name` (the arguments must already be validated).
    pub fn mcp_tool(&self, call: &ToolCall) -> Result<Value> {
        self.mcp_tool_with_host(call, None)
    }
    /// Read the resource `read.name`.
    pub fn mcp_resource(&self, read: &ResourceRead) -> Result<ResourceContents> {
        self.mcp_resource_with_host(read, None)
    }
    /// Render the prompt `get.name` as `(description, messages)`.
    pub fn mcp_prompt(&self, get: &PromptGet) -> Result<(Option<String>, Vec<Value>)> {
        self.mcp_prompt_with_host(get, None)
    }
    /// [`mcp_tool`](Self::mcp_tool) with host services for the tool's host calls.
    pub fn mcp_tool_with_host(
        &self,
        call: &ToolCall,
        host: Option<&dyn crate::HostServices>,
    ) -> Result<Value> {
        let context = serde_json::to_value(call).map_err(error)?;
        self.call_json_concurrent_with_host("mcp_tool", context, host)
    }
    /// [`mcp_resource`](Self::mcp_resource) with host services.
    pub fn mcp_resource_with_host(
        &self,
        read: &ResourceRead,
        host: Option<&dyn crate::HostServices>,
    ) -> Result<ResourceContents> {
        let context = serde_json::to_value(read).map_err(error)?;
        ResourceContents::from_reply(&self.call_json_concurrent_with_host(
            "mcp_resource",
            context,
            host,
        )?)
    }
    /// [`mcp_prompt`](Self::mcp_prompt) with host services.
    pub fn mcp_prompt_with_host(
        &self,
        get: &PromptGet,
        host: Option<&dyn crate::HostServices>,
    ) -> Result<(Option<String>, Vec<Value>)> {
        let context = serde_json::to_value(get).map_err(error)?;
        prompt_reply(&self.call_json_concurrent_with_host("mcp_prompt", context, host)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arguments_are_validated_against_the_schema() {
        let tool = ToolSpec::new("t", "d").input_schema(json!({
            "type": "object",
            "properties": {"n": {"type": "integer"}},
            "required": ["n"],
            "additionalProperties": false,
        }));
        assert!(tool.validate(&json!({"n": 1})).is_ok());
        let e = tool
            .validate(&json!({"n": "x", "m": 1}))
            .unwrap_err()
            .to_string();
        assert!(e.contains("/n") && e.contains("m"), "{e}");
        assert!(tool.validate(&json!({})).is_err());
    }

    #[test]
    fn entries_parse_and_replies_normalize() {
        let entry = json!({
            "mcp_tools": [{"name": "t", "description": "d", "input_schema": {"type": "object"},
                "annotations": {"readOnlyHint": true}}],
            "mcp_resources": [{"name": "r", "description": "d", "uri_template": "x://{id}"}],
            "mcp_prompts": [{"name": "p", "arguments": [{"name": "a", "required": true}]}],
        });
        let entries = entries_of(&entry).unwrap();
        assert!(entries.tools[0].hint("readOnlyHint"));
        assert!(entries.prompts[0].arguments[0].required);
        assert_eq!(
            ResourceContents::from_reply(&json!("hi")).unwrap(),
            ResourceContents::text("hi")
        );
        let blob = ResourceContents::blob(vec![1, 2], "image/png");
        assert_eq!(ResourceContents::from_reply(&blob.to_json()).unwrap(), blob);
        let (d, m) = prompt_reply(&json!({"description": "x", "messages": ["hi"]})).unwrap();
        assert_eq!(d.as_deref(), Some("x"));
        assert_eq!(m[0]["content"]["text"], "hi");
        assert!(prompt_reply(&json!(3)).is_err());
    }
}
