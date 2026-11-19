//! Native plugin MCP tool/resource/prompt and web console page adapters.

use super::*;

// ---------------------------------------------------------------------------------------------
// MCP: native tools, resources and prompts (`mcp_tools`, `mcp_resources`, `mcp_prompts`).
// ---------------------------------------------------------------------------------------------

/// The MCP entries of the native plugins, or `None` when no plugin declares any. Invalid entries
/// are reported in `notices` and skipped.
pub fn mcp(
    natives: &Arc<NativePlugins>,
    notices: &mut Vec<String>,
    env: &HostEnv,
    state: &meerschaum_server::AppState,
) -> Option<Arc<dyn meerschaum_server::mcp::NativeMcp>> {
    let mut entries = NativeMcpEntries {
        natives: natives.clone(),
        env: env.clone(),
        state: state.clone(),
        limits: CallLimits::default(),
        tools: Vec::new(),
        resources: Vec::new(),
        prompts: Vec::new(),
    };
    let builtin = meerschaum_server::mcp::registry::builtin();
    for name in natives.names() {
        match meerschaum_plugin::mcp::entries_of(natives.plugin_entry(&name)) {
            Ok(found) => {
                // An entry never shadows a built-in or an earlier plugin's entry: each one hidden
                // is a notice, like a refused route.
                let mut hidden = |kind: &str, entry: &str, by: String| {
                    notices.push(format!(
                        "Native plugin '{name}': MCP {kind} '{entry}' is hidden: {by}."
                    ))
                };
                for tool in found.tools {
                    if builtin.tools.iter().any(|b| b.name == tool.name) {
                        hidden("tool", &tool.name, "a built-in tool has that name".into());
                    } else if let Some((other, _)) =
                        entries.tools.iter().find(|(_, t)| t.name == tool.name)
                    {
                        hidden("tool", &tool.name, format!("plugin '{other}' declares it"));
                    } else {
                        entries.tools.push((name.clone(), tool));
                    }
                }
                for resource in found.resources {
                    let uri = |u: &Option<String>, t: &Option<String>| {
                        u.clone().or_else(|| t.clone()).unwrap_or_default()
                    };
                    let this = uri(&resource.uri, &resource.uri_template);
                    if builtin
                        .resources
                        .iter()
                        .any(|b| uri(&b.uri, &b.uri_template) == this)
                    {
                        hidden("resource", &this, "a built-in resource has that URI".into());
                    } else if let Some((other, _)) = entries.resources.iter().find(|(_, r)| {
                        r.name == resource.name || uri(&r.uri, &r.uri_template) == this
                    }) {
                        hidden(
                            "resource",
                            &this,
                            format!("plugin '{other}' declares that name or URI"),
                        );
                    } else {
                        entries.resources.push((name.clone(), resource));
                    }
                }
                for prompt in found.prompts {
                    if builtin.prompts.iter().any(|b| b.name == prompt.name) {
                        hidden(
                            "prompt",
                            &prompt.name,
                            "a built-in prompt has that name".into(),
                        );
                    } else if let Some((other, _)) =
                        entries.prompts.iter().find(|(_, p)| p.name == prompt.name)
                    {
                        hidden(
                            "prompt",
                            &prompt.name,
                            format!("plugin '{other}' declares it"),
                        );
                    } else {
                        entries.prompts.push((name.clone(), prompt));
                    }
                }
            }
            Err(error) => notices.push(format!(
                "Native plugin '{name}' MCP entries are disabled: {error}"
            )),
        }
    }
    if entries.tools.is_empty() && entries.resources.is_empty() && entries.prompts.is_empty() {
        return None;
    }
    Some(Arc::new(entries))
}

pub(super) struct NativeMcpEntries {
    pub(super) natives: Arc<NativePlugins>,
    /// The entries' host calls, made for the caller.
    pub(super) env: HostEnv,
    pub(super) state: meerschaum_server::AppState,
    pub(super) limits: CallLimits,
    pub(super) tools: Vec<(String, meerschaum_plugin::mcp::ToolSpec)>,
    pub(super) resources: Vec<(String, meerschaum_plugin::mcp::ResourceSpec)>,
    pub(super) prompts: Vec<(String, meerschaum_plugin::mcp::PromptSpec)>,
}

pub(super) fn core_value(json: serde_json::Value) -> Value {
    serde_json::from_value(json).unwrap_or_default()
}

pub(super) fn caller_context(
    caller: &meerschaum_server::mcp::Caller,
) -> (Option<meerschaum_plugin::api::Principal>, Vec<String>) {
    let principal = serde_json::from_value(meerschaum_server::native::principal_json(
        caller.principal.as_ref(),
    ))
    .ok();
    (principal, caller.scope_list())
}

impl NativeMcpEntries {
    /// Run a plugin call off the async runtime, with host calls for `caller` (full trust over
    /// stdio, the caller's scopes over HTTP).
    pub(super) async fn call<T: Send + 'static>(
        &self,
        plugin: &str,
        caller: &meerschaum_server::mcp::Caller,
        f: impl FnOnce(
            &meerschaum_plugin::LoadedPlugin,
            Option<&dyn meerschaum_plugin::HostServices>,
        ) -> meerschaum_plugin::Result<T>
        + Send
        + 'static,
    ) -> Result<T, String> {
        let permit = self.limits.acquire(plugin).await?;
        let natives = self.natives.clone();
        let plugin = plugin.to_string();
        let host = if caller.api {
            PluginHost::api(
                self.env.clone(),
                self.state.clone(),
                caller.scope_list(),
                caller.principal.clone(),
            )
        } else {
            PluginHost::local(self.env.clone())
        };
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(&*natives.plugin(&plugin)?, Some(&host))
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
    }
}

#[async_trait]
impl meerschaum_server::mcp::NativeMcp for NativeMcpEntries {
    fn tools(&self) -> Vec<meerschaum_server::mcp::registry::Tool> {
        use meerschaum_server::mcp::registry::{Source, Tool};
        self.tools
            .iter()
            .filter_map(|(_, t)| {
                let entry = serde_json::json!({
                    "name": t.name,
                    "description": t.description,
                    "title": t.title,
                    "scopes": t.scopes,
                    "read_only": t.hint("readOnlyHint"),
                    "destructive": t.hint("destructiveHint"),
                    "idempotent": t.hint("idempotentHint"),
                    "open_world": t.hint("openWorldHint"),
                    "input_schema": t.input_schema,
                    "output_schema": t.output_schema,
                });
                Tool::from_value(&core_value(entry), Source::Native)
            })
            .collect()
    }

    fn resources(&self) -> Vec<meerschaum_server::mcp::registry::Resource> {
        use meerschaum_server::mcp::registry::{Resource, Source};
        self.resources
            .iter()
            .filter_map(|(_, r)| {
                let entry = core_value(serde_json::to_value(r).unwrap_or_default());
                Resource::from_value(&entry, Source::Native)
            })
            .collect()
    }

    fn prompts(&self) -> Vec<meerschaum_server::mcp::registry::Prompt> {
        use meerschaum_server::mcp::registry::{Prompt, Source};
        self.prompts
            .iter()
            .filter_map(|(_, p)| {
                let entry = core_value(serde_json::to_value(p).unwrap_or_default());
                Prompt::from_value(&entry, Source::Native)
            })
            .collect()
    }

    async fn call_tool(
        &self,
        name: &str,
        arguments: &Map,
        caller: &meerschaum_server::mcp::Caller,
    ) -> Result<Value, String> {
        let Some((plugin, spec)) = self.tools.iter().find(|(_, t)| t.name == name) else {
            return Err(format!("Unknown tool: '{name}'."));
        };
        let arguments = serde_json::to_value(arguments).map_err(|e| e.to_string())?;
        spec.validate(&arguments)
            .map_err(|e| format!("Invalid arguments for '{name}': {e}"))?;
        let (principal, scopes) = caller_context(caller);
        let call = meerschaum_plugin::mcp::ToolCall {
            name: name.to_string(),
            arguments: match arguments {
                serde_json::Value::Object(map) => map,
                _ => Default::default(),
            },
            principal,
            scopes,
        };
        self.call(plugin, caller, move |p, host| {
            p.mcp_tool_with_host(&call, host)
        })
        .await
        .map(core_value)
    }

    async fn read_resource(
        &self,
        name: &str,
        uri: &str,
        variables: &Map,
        caller: &meerschaum_server::mcp::Caller,
    ) -> Result<meerschaum_server::mcp::NativeResource, String> {
        use meerschaum_plugin::mcp::{ResourceContents, ResourceRead};
        use meerschaum_server::mcp::{NativeResource, NativeResourceContents};
        let Some((plugin, _)) = self.resources.iter().find(|(_, r)| r.name == name) else {
            return Err(format!("Resource not found: {uri}"));
        };
        let (principal, scopes) = caller_context(caller);
        let read = ResourceRead {
            name: name.to_string(),
            uri: uri.to_string(),
            variables: match serde_json::to_value(variables) {
                Ok(serde_json::Value::Object(map)) => map,
                _ => Default::default(),
            },
            principal,
            scopes,
        };
        let contents = self
            .call(plugin, caller, move |p, host| {
                p.mcp_resource_with_host(&read, host)
            })
            .await?;
        Ok(match contents {
            ResourceContents::Text { text, mime_type } => NativeResource {
                contents: NativeResourceContents::Text(text),
                mime_type,
            },
            blob @ ResourceContents::Blob { .. } => {
                let wire = blob.to_json();
                NativeResource {
                    contents: NativeResourceContents::Blob(
                        wire["blob_base64"].as_str().unwrap_or_default().to_string(),
                    ),
                    mime_type: wire["mime_type"].as_str().map(str::to_owned),
                }
            }
        })
    }

    async fn get_prompt(
        &self,
        name: &str,
        arguments: &Map,
        caller: &meerschaum_server::mcp::Caller,
    ) -> Result<(Option<String>, Vec<Value>), String> {
        let Some((plugin, _)) = self.prompts.iter().find(|(_, p)| p.name == name) else {
            return Err(format!("Unknown prompt: '{name}'."));
        };
        let (principal, scopes) = caller_context(caller);
        let get = meerschaum_plugin::mcp::PromptGet {
            name: name.to_string(),
            arguments: match serde_json::to_value(arguments) {
                Ok(serde_json::Value::Object(map)) => map,
                _ => Default::default(),
            },
            principal,
            scopes,
        };
        let (description, messages) = self
            .call(plugin, caller, move |p, host| {
                p.mcp_prompt_with_host(&get, host)
            })
            .await?;
        Ok((description, messages.into_iter().map(core_value).collect()))
    }
}

pub(super) struct NativePage {
    pub(super) natives: Arc<NativePlugins>,
    pub(super) plugin: String,
    pub(super) path: String,
    pub(super) function: Option<String>,
    /// The page's host calls, made for its viewer.
    pub(super) env: HostEnv,
    pub(super) state: meerschaum_server::AppState,
    /// What the page's host calls may also do for every viewer (`host_scopes`).
    pub(super) host_scopes: Vec<String>,
}

/// Who a console page's host calls act for, and with which scopes: everything under
/// `--no-auth`; a signed-in viewer gets what their user would get from `/login` (every scope for
/// an administrator, else the user's scopes); a guest, a user who no longer exists, or one
/// whose permissions cannot be read gets none.
pub(super) async fn page_caller(
    state: &meerschaum_server::AppState,
    username: &str,
) -> (Vec<String>, Option<meerschaum_server::auth::Principal>) {
    if state.config.no_auth {
        return (vec!["*".into()], None);
    }
    if username.is_empty() {
        return (Vec::new(), None);
    }
    let conn = state.instance();
    let mut user = meerschaum_core::users::User::new(username, None);
    match conn.get_user_id(&user).await {
        Ok(Some(id)) => user.user_id = Some(id),
        _ => return (Vec::new(), None),
    }
    let Ok(user_type) = conn.get_user_type(&user).await else {
        return (Vec::new(), None);
    };
    user.user_type = user_type;
    let principal = meerschaum_server::auth::Principal::User(user);
    let Ok(scopes) = meerschaum_server::auth::principal_scopes(state, &principal).await else {
        return (Vec::new(), None);
    };
    (scopes, Some(principal))
}

#[async_trait]
impl meerschaum_web::PageHandler for NativePage {
    async fn render(
        &self,
        query: &str,
        session: &serde_json::Value,
    ) -> (String, axum::http::StatusCode) {
        let natives = self.natives.clone();
        let plugin = self.plugin.clone();
        let context = serde_json::json!({
            "path": self.path,
            "function": self.function,
            "query": query,
            "username": session["username"],
        });
        let (scopes, principal) =
            page_caller(&self.state, session["username"].as_str().unwrap_or("")).await;
        let host = PluginHost::api(self.env.clone(), self.state.clone(), scopes, principal)
            .with_host_scopes(&self.host_scopes);
        let reply = tokio::task::spawn_blocking(move || {
            natives
                .plugin(&plugin)?
                .call_json_with_host("page", context, Some(&host))
        })
        .await;
        match reply {
            Ok(Ok(reply)) => {
                let status = reply["status"]
                    .as_u64()
                    .and_then(|s| u16::try_from(s).ok())
                    .and_then(|s| axum::http::StatusCode::from_u16(s).ok())
                    .unwrap_or(axum::http::StatusCode::OK);
                (
                    reply["html"].as_str().unwrap_or_default().to_string(),
                    status,
                )
            }
            Ok(Err(error)) => (
                format!("<p>{}</p>", html_escape(&error.to_string())),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
            Err(error) => (
                format!("<p>{}</p>", html_escape(&error.to_string())),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        }
    }
}

pub(super) fn html_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// The pages native plugins declare in their manifest (`web_pages`), served as fragments for the
/// Dash console.
pub fn pages(
    manifest: &Manifest,
    natives: &Arc<NativePlugins>,
    env: &HostEnv,
    state: &meerschaum_server::AppState,
) -> Vec<meerschaum_web::PluginPage> {
    let mut pages = Vec::new();
    for plugin in manifest
        .plugins
        .values()
        .filter(|p| natives.contains(&p.name))
    {
        for page in &plugin.web_pages {
            let path = page.path.trim_matches('/');
            let path = path.strip_prefix("dash/").unwrap_or(path).to_string();
            // `host_scopes` is a native manifest key (the shared manifest model omits it).
            let declared = natives.plugin_entry(&plugin.name)["web_pages"]
                .as_array()
                .and_then(|entries| entries.iter().find(|entry| entry["path"] == page.path))
                .map(|entry| entry["host_scopes"].clone())
                .unwrap_or_default();
            let mut notices = Vec::new();
            let host_scopes = host_scopes_of(
                &declared,
                &format!("Native plugin '{}' page '{path}'", plugin.name),
                &mut notices,
            );
            for notice in notices {
                tracing_warn(&notice);
            }
            pages.push(meerschaum_web::PluginPage {
                plugin: plugin.name.clone(),
                path: path.clone(),
                page_group: page
                    .page_group
                    .clone()
                    .unwrap_or_else(|| plugin.name.clone()),
                login_required: page.login_required,
                skip_navbar: page.skip_navbar,
                dark_theme: Some(page.dark_theme),
                content: meerschaum_web::PageContent::Handler(Arc::new(NativePage {
                    natives: natives.clone(),
                    plugin: plugin.name.clone(),
                    path,
                    function: page.function.clone(),
                    env: env.clone(),
                    state: state.clone(),
                    host_scopes,
                })),
            });
        }
    }
    pages
}
