//! MCP tools, resources and prompts of native (Rust) plugins.
//!
//! The CLI implements [`NativeMcp`] over its native plugin registry and installs it with
//! [`super::McpServer::with_native`]. Native entries are listed after the built-ins and before the
//! Python host's (a native entry never shadows a built-in; a Python entry of the same name is
//! hidden). They get the same scope filtering, read-only filtering and required-argument
//! checks as every other entry; the implementation validates the arguments against the
//! plugin's input schema and calls the plugin with the caller's principal and scopes.

use async_trait::async_trait;
use meerschaum_core::{Map, Value};

use super::Caller;
use super::registry::{Prompt, Resource, Tool};

/// The contents of a native resource read.
#[derive(Debug, Clone, PartialEq)]
pub enum NativeResourceContents {
    /// Text.
    Text(String),
    /// Base64-encoded binary contents (an MCP `blob`).
    Blob(String),
}

/// A native resource read: the contents and their MIME type (`None`: the resource's).
#[derive(Debug, Clone, PartialEq)]
pub struct NativeResource {
    /// The contents.
    pub contents: NativeResourceContents,
    /// The MIME type.
    pub mime_type: Option<String>,
}

/// The native plugins' MCP entries.
#[async_trait]
pub trait NativeMcp: Send + Sync {
    /// The tools (with [`super::registry::Source::Native`]).
    fn tools(&self) -> Vec<Tool>;
    /// The resources and templates.
    fn resources(&self) -> Vec<Resource>;
    /// The prompts.
    fn prompts(&self) -> Vec<Prompt>;
    /// Call the tool `name`. `Err` is reported as a tool error (`isError`) with its message.
    async fn call_tool(
        &self,
        name: &str,
        arguments: &Map,
        caller: &Caller,
    ) -> Result<Value, String>;
    /// Read the resource `name` at `uri` (with the template's variables).
    async fn read_resource(
        &self,
        name: &str,
        uri: &str,
        variables: &Map,
        caller: &Caller,
    ) -> Result<NativeResource, String>;
    /// Render the prompt `name`: `(description, messages)`.
    async fn get_prompt(
        &self,
        name: &str,
        arguments: &Map,
        caller: &Caller,
    ) -> Result<(Option<String>, Vec<Value>), String>;
}
