//! Meerschaum's Model Context Protocol server (§7.2), the port of `meerschaum/mcp` and
//! `meerschaum/api/routes/_mcp.py`.
//!
//! The tools, resources and prompts are v4's, exported once as data (`registry.json`, written by
//! `tests/fixtures/gen_mcp_registry.py` from 4.1.4) so their names, descriptions, JSON schemas,
//! scopes and annotations cannot drift. The handlers are Rust ([`tools`], [`resources`],
//! [`prompts`]). Two transports share one dispatcher ([`McpServer::handle_payload`]):
//!
//! - **Streamable HTTP** at `/mcp` ([`http`]): the caller's scopes come from the bearer token
//!   (`CurrentScopes`), and each tool needs the same scope as its REST route. Tools the caller
//!   cannot call are omitted from `tools/list`.
//! - **stdio** ([`stdio`], `mrsm start mcp`): newline-delimited JSON-RPC with the scopes `['*']`
//!   and no API permission checks, the CLI's trust model.
//!
//! **Read-only mode** (`api:mcp:read_only`) hides every tool that is not read-only, whatever the
//! token's scopes.
//!
//! **Federation.** Tools, resources and prompts registered by Python plugins
//! (`meerschaum.mcp.tool(...)`) live in the Python API host (§7.3). [`federation::Federation`]
//! fetches the host's registry at startup and on manifest change and merges it into the lists;
//! calls to a plugin tool are forwarded to the host with the caller's scopes.
//!
//! Operations the server cannot perform on its own instance connector (syncing from a source
//! connector, verification, user SQL) go through [`McpBackend`], whose default answers like a
//! server without those capabilities. The CLI wires the real implementation.

pub mod dispatch;
pub mod docs;
pub mod federation;
pub mod http;
pub mod prompts;
pub mod protocol;
pub mod registry;
pub mod resources;
pub mod security;
pub mod stdio;
pub mod tools;

use std::sync::Arc;

use async_trait::async_trait;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};

use crate::auth::{Principal, Scopes};
use crate::state::AppState;

pub use federation::Federation;

/// The default `api:mcp:actions:denylist`: the actions that execute arbitrary code or SQL on the
/// API host (`config/_default.py`).
pub const DEFAULT_ACTION_DENYLIST: &[&str] = &["sh", "os", "python", "stack", "sql", "install"];

/// `api:mcp`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpConfig {
    /// `api:mcp:enabled` (default `true`): serve `/mcp`.
    pub enabled: bool,
    /// `api:mcp:read_only` (default `false`): only expose read-only tools.
    pub read_only: bool,
    /// `api:mcp:actions:denylist`.
    pub action_denylist: Vec<String>,
    /// `api:mcp:actions:allowlist` (empty: every action not denied).
    pub action_allowlist: Vec<String>,
}

impl Default for McpConfig {
    fn default() -> Self {
        McpConfig {
            enabled: true,
            read_only: false,
            action_denylist: DEFAULT_ACTION_DENYLIST
                .iter()
                .map(|s| s.to_string())
                .collect(),
            action_allowlist: Vec::new(),
        }
    }
}

fn str_list(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::List(items)) => items.iter().map(Value::py_str).collect(),
        // `list(... or [])`: a string is a list of its characters in Python, which no config
        // uses; treat a falsy value as empty.
        _ => Vec::new(),
    }
}

impl McpConfig {
    /// Read `api:mcp` from a config map (`{'api': ..., ...}`); missing keys take v4's defaults.
    pub fn from_map(cfg: &Map) -> McpConfig {
        let mut out = McpConfig::default();
        let Some(mcp) = cfg.get("api").and_then(|a| a.get("mcp")) else {
            return out;
        };
        if let Some(v) = mcp.get("enabled") {
            out.enabled = v.truthy();
        }
        if let Some(v) = mcp.get("read_only") {
            out.read_only = v.truthy();
        }
        if let Some(actions) = mcp.get("actions") {
            if let Some(v) = actions.get("denylist") {
                out.action_denylist = str_list(Some(v));
            }
            if let Some(v) = actions.get("allowlist") {
                out.action_allowlist = str_list(Some(v));
            }
        }
        out
    }
}

/// The caller of an MCP request.
#[derive(Debug, Clone)]
pub struct Caller {
    /// The granted scopes (`['*']` grants everything).
    pub scopes: Scopes,
    /// The authenticated principal (`None` under `--no-auth` and over stdio).
    pub principal: Option<Principal>,
    /// Whether the request came over the API (`get_mcp_context()['api']`): tools then apply the
    /// server's instance and action permissions. `false` over stdio (full local trust).
    pub api: bool,
}

impl Caller {
    /// The stdio caller: every scope, no API checks (`STDIO_SCOPES`).
    pub fn stdio() -> Caller {
        Caller {
            scopes: Scopes::List(vec!["*".into()]),
            principal: None,
            api: false,
        }
    }

    /// An API caller.
    pub fn api(scopes: Scopes, principal: Option<Principal>) -> Caller {
        Caller {
            scopes,
            principal,
            api: true,
        }
    }

    /// The scopes as a list (for forwarding to the API host).
    pub fn scope_list(&self) -> Vec<String> {
        self.scopes.to_list()
    }
}

/// Why `read_sql` could not run a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlReadError {
    /// The connector does not exist or cannot read SQL (`hasattr(connector, 'read')`).
    Unsupported,
    /// The database refused the query (`read()` returned `None`).
    Failed,
}

/// What a plugin or custom connector's `register(pipe)` answered: the connector half of v4's
/// `Pipe.register()` (see [`McpBackend::connector_register`]).
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectorRegister {
    /// The connector defines no `register()`, or this server cannot ask it: nothing changes.
    Undefined,
    /// The parameters it returned (`{}` for `None`, also when `register()` raised).
    Parameters(Map),
    /// It returned something that is not a dict (`str()` of it): v4 warns and changes nothing.
    Invalid(String),
}

/// Operations the MCP tools need beyond the instance connector. The CLI implements them with the
/// engine and the SQL crate; [`NoBackend`] is a server without them.
#[async_trait]
pub trait McpBackend: Send + Sync {
    /// `pipe.sync(begin=..., end=...)`: fetch from the pipe's source connector.
    async fn sync_pipe(
        &self,
        pipe: &Pipe,
        begin: Option<String>,
        end: Option<String>,
    ) -> SuccessTuple;

    /// `pipe.verify(begin=..., end=...)`.
    async fn verify_pipe(
        &self,
        pipe: &Pipe,
        begin: Option<String>,
        end: Option<String>,
    ) -> SuccessTuple;

    /// `connector.read(query, chunksize=limit, chunks=1)` as JSON records
    /// (`to_json(df, orient='records')`).
    async fn read_sql(
        &self,
        connector_keys: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Value>, SqlReadError>;

    /// The connector half of `Pipe.register()`: ask the `plugin:` or custom connector of
    /// `pipe` for its `register(pipe)` parameters. Needs a Python worker, so a server without
    /// one keeps this default (nothing is asked and the pipe is registered as given).
    async fn connector_register(&self, pipe: &Pipe) -> ConnectorRegister {
        let _ = pipe;
        ConnectorRegister::Undefined
    }
}

/// No source syncs and no user SQL.
#[derive(Debug, Default, Clone)]
pub struct NoBackend;

const NO_SOURCE_SYNC: &str = "Syncing from a source connector is not available on this server.";

#[async_trait]
impl McpBackend for NoBackend {
    async fn sync_pipe(&self, _: &Pipe, _: Option<String>, _: Option<String>) -> SuccessTuple {
        SuccessTuple::fail(NO_SOURCE_SYNC)
    }
    async fn verify_pipe(&self, _: &Pipe, _: Option<String>, _: Option<String>) -> SuccessTuple {
        SuccessTuple::fail(NO_SOURCE_SYNC)
    }
    async fn read_sql(&self, _: &str, _: &str, _: usize) -> Result<Vec<Value>, SqlReadError> {
        Err(SqlReadError::Unsupported)
    }
}

/// The MCP server: configuration, the backend for non-instance operations, and the optional
/// federation with the Python API host.
pub struct McpServer {
    /// `api:mcp`.
    pub config: McpConfig,
    /// Source syncs, verification and user SQL.
    pub backend: Arc<dyn McpBackend>,
    /// Plugin tools hosted by the Python API host.
    pub federation: Option<Arc<Federation>>,
}

impl std::fmt::Debug for McpServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpServer")
            .field("config", &self.config)
            .field("federation", &self.federation.is_some())
            .finish()
    }
}

impl McpServer {
    /// A server with `config`, no backend and no federation.
    pub fn new(config: McpConfig) -> McpServer {
        McpServer {
            config,
            backend: Arc::new(NoBackend),
            federation: None,
        }
    }

    /// The server for an [`AppState`]: its `api:mcp` config, and a federation with the API host
    /// when the state proxies one.
    pub fn for_state(state: &AppState) -> McpServer {
        let mut server = McpServer::new(state.config.mcp.clone());
        if let Some(host) = state.host.clone() {
            server.federation = Some(Arc::new(Federation::new(host, state.secret().to_vec())));
        }
        server
    }

    /// Use this backend.
    pub fn with_backend(mut self, backend: Arc<dyn McpBackend>) -> McpServer {
        self.backend = backend;
        self
    }

    /// Use (or drop) this federation.
    pub fn with_federation(mut self, federation: Option<Arc<Federation>>) -> McpServer {
        self.federation = federation;
        self
    }
}
