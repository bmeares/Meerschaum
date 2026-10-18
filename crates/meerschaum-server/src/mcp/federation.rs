//! MCP federation with the Python API host (§7.2, §7.3).
//!
//! Tools, resources and prompts registered by Python plugins (`meerschaum.mcp.tool(...)`) live in
//! the host. The server fetches the host's registry (`GET /_mrsm/host/registry`) at startup and on
//! manifest change ([`Federation::refresh`]) and merges it into its lists; a call to a plugin entry
//! is forwarded (`POST /_mrsm/host/mcp`) with the caller's scopes.
//!
//! **Internal requests** carry `X-Mrsm-Host-Token`, a short-lived HS256 JWT signed with the API
//! secret (the same secret file both processes read) whose `mrsm_host` claim is `mcp`; it holds
//! the caller's `sub` and `scopes`. The host refuses internal requests without it, and the
//! server never proxies the `/_mrsm/` prefix from clients ([`INTERNAL_PREFIX`]), so a client can
//! neither reach the internal routes nor claim scopes it was not granted.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use meerschaum_core::pyfmt::{JsonStyle, json_dumps};
use meerschaum_core::{Map, Value};
use tokio::sync::RwLock;

use super::Caller;
use super::protocol::{map_of, s};
use super::registry::{Prompt, Resource, Source, Tool};
use crate::auth::{Principal, jwt_encode};
use crate::proxy::HostProxy;

/// The path prefix of the host's internal routes. The server answers it with `404` instead of
/// proxying it.
pub const INTERNAL_PREFIX: &str = "/_mrsm/";
/// The host's registry route.
pub const REGISTRY_PATH: &str = "/_mrsm/host/registry";
/// The host's MCP route.
pub const MCP_PATH: &str = "/_mrsm/host/mcp";
/// The internal-token header.
pub const TOKEN_HEADER: &str = "x-mrsm-host-token";
/// The `mrsm_host` claim of internal tokens.
pub const TOKEN_CLAIM: &str = "mrsm_host";
/// Core paths a host should not serve itself (the server's own routes win).
pub const SHADOWED_PATHS: &[&str] = &["/mcp", "/login"];

/// How long a failed registry fetch is remembered before retrying.
const RETRY_AFTER: Duration = Duration::from_secs(30);

/// The host's registry.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HostRegistry {
    /// Plugin tools.
    pub tools: Vec<Tool>,
    /// Plugin resources and templates.
    pub resources: Vec<Resource>,
    /// Plugin prompts.
    pub prompts: Vec<Prompt>,
    /// The paths of the host app's own routes (for the shadowing hint).
    pub routes: Vec<String>,
}

impl HostRegistry {
    /// Parse the host's `GET /_mrsm/host/registry` body.
    pub fn from_value(v: &Value) -> HostRegistry {
        let list = |k: &str| {
            v.get(k)
                .and_then(Value::as_list)
                .cloned()
                .unwrap_or_default()
        };
        HostRegistry {
            tools: list("tools")
                .iter()
                .filter_map(|t| Tool::from_value(t, Source::Host))
                .collect(),
            resources: list("resources")
                .iter()
                .filter_map(|r| Resource::from_value(r, Source::Host))
                .collect(),
            prompts: list("prompts")
                .iter()
                .filter_map(|p| Prompt::from_value(p, Source::Host))
                .collect(),
            routes: list("routes").iter().map(Value::py_str).collect(),
        }
    }

    /// The hint logged when the host serves a path the server shadows.
    pub fn shadowing_hints(&self) -> Vec<String> {
        SHADOWED_PATHS
            .iter()
            .filter(|p| self.routes.iter().any(|r| r == *p))
            .map(|p| {
                format!(
                    "The API host registers its own `{p}`, which the server serves itself, so the \
                     plugin's route is unreachable. To restrict core routes, use \
                     `api:permissions:routes:allowlist` instead of replacing them."
                )
            })
            .collect()
    }
}

struct Cached {
    registry: Arc<HostRegistry>,
    fetched: Instant,
    ok: bool,
}

/// The federation with one API host.
pub struct Federation {
    host: Arc<HostProxy>,
    secret: Vec<u8>,
    cache: RwLock<Option<Cached>>,
}

impl std::fmt::Debug for Federation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Federation")
            .field("host", &self.host)
            .finish()
    }
}

/// An internal token for `caller` (or for the server itself when `None`).
pub fn internal_token(secret: &[u8], caller: Option<&Caller>) -> String {
    // `sub` must be a string when present (PyJWT and the server's decoder both reject others),
    // so it is omitted for the server itself and for unauthenticated callers.
    let sub = match caller.and_then(|c| c.principal.as_ref()) {
        Some(Principal::User(u)) => Some(u.username.clone()),
        Some(Principal::Token(t)) => t.id.map(|id| id.to_string()),
        None => None,
    };
    let scopes = caller
        .map(|c| {
            Value::List(
                c.scope_list()
                    .into_iter()
                    .map(Value::Str)
                    .collect::<Vec<_>>(),
            )
        })
        .unwrap_or_else(|| Value::List(Vec::new()));
    let exp = chrono::Utc::now().timestamp() + 300;
    let mut claims = Map::new();
    if let Some(sub) = sub {
        claims.insert("sub".into(), s(sub));
    }
    claims.extend(map_of([
        ("scopes", scopes),
        (TOKEN_CLAIM, s("mcp")),
        ("exp", Value::Int(exp)),
    ]));
    jwt_encode(&claims, secret)
}

impl Federation {
    /// A federation over a host proxy, signing internal tokens with `secret`.
    pub fn new(host: Arc<HostProxy>, secret: Vec<u8>) -> Federation {
        Federation {
            host,
            secret,
            cache: RwLock::new(None),
        }
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        caller: Option<&Caller>,
        body: Option<String>,
    ) -> Result<(StatusCode, bytes::Bytes), String> {
        let mut b = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "localhost")
            .header(TOKEN_HEADER, internal_token(&self.secret, caller));
        if body.is_some() {
            b = b.header("content-type", "application/json");
        }
        let req = b
            .body(body.map(Body::from).unwrap_or_else(Body::empty))
            .map_err(|e| e.to_string())?;
        let resp = self.host.forward(req).await;
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .map_err(|e| e.to_string())?;
        Ok((status, bytes))
    }

    /// Fetch the host's registry now, cache it, and log the shadowing hints.
    pub async fn refresh(&self) -> Result<Arc<HostRegistry>, String> {
        let fetched = self.fetch().await;
        let (registry, ok) = match &fetched {
            Ok(r) => (Arc::new(r.clone()), true),
            Err(e) => {
                tracing::warn!("Failed to fetch the API host's MCP registry: {e}");
                (Arc::new(HostRegistry::default()), false)
            }
        };
        if ok {
            for hint in registry.shadowing_hints() {
                tracing::warn!("{hint}");
            }
        }
        *self.cache.write().await = Some(Cached {
            registry: registry.clone(),
            fetched: Instant::now(),
            ok,
        });
        fetched.map(|_| registry)
    }

    async fn fetch(&self) -> Result<HostRegistry, String> {
        // No plugin needs the host: nothing to federate, and the host must not be started.
        if !self.host.is_active() {
            return Ok(HostRegistry::default());
        }
        let (status, body) = self.request(Method::GET, REGISTRY_PATH, None, None).await?;
        if !status.is_success() {
            return Err(format!(
                "{status}: {}",
                String::from_utf8_lossy(&body)
                    .chars()
                    .take(200)
                    .collect::<String>()
            ));
        }
        let v: Value = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
        Ok(HostRegistry::from_value(&v))
    }

    /// Forget the cached registry (the next use fetches it again).
    pub async fn invalidate(&self) {
        *self.cache.write().await = None;
    }

    /// The cached registry, fetched on first use (a failed fetch is retried after a while).
    pub async fn registry(&self) -> Arc<HostRegistry> {
        {
            let guard = self.cache.read().await;
            if let Some(c) = guard.as_ref()
                && (c.ok || c.fetched.elapsed() < RETRY_AFTER)
            {
                return c.registry.clone();
            }
        }
        match self.refresh().await {
            Ok(r) => r,
            Err(_) => self
                .cache
                .read()
                .await
                .as_ref()
                .map(|c| c.registry.clone())
                .unwrap_or_default(),
        }
    }

    /// Forward a JSON-RPC message to the host's MCP route with the caller's scopes.
    pub async fn forward(&self, message: &Value, caller: &Caller) -> Result<Value, String> {
        let body = json_dumps(message, JsonStyle::Compact, false);
        let (status, bytes) = self
            .request(Method::POST, MCP_PATH, Some(caller), Some(body))
            .await?;
        if !status.is_success() {
            return Err(format!(
                "{status}: {}",
                String::from_utf8_lossy(&bytes)
                    .chars()
                    .take(200)
                    .collect::<String>()
            ));
        }
        serde_json::from_slice(&bytes).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hints_for_shadowed_routes() {
        let r = HostRegistry {
            routes: vec!["/mcp".into(), "/plugin".into()],
            ..Default::default()
        };
        let hints = r.shadowing_hints();
        assert_eq!(hints.len(), 1);
        assert!(hints[0].contains("`/mcp`"));
    }

    #[test]
    fn internal_tokens_carry_scopes() {
        let caller = Caller::api(crate::auth::Scopes::List(vec!["pipes:read".into()]), None);
        let t = internal_token(b"k", Some(&caller));
        let claims = crate::auth::jwt_decode(&t, b"k", chrono::Utc::now()).unwrap();
        assert_eq!(claims["mrsm_host"], "mcp");
        assert_eq!(claims["scopes"][0], "pipes:read");
    }
}
