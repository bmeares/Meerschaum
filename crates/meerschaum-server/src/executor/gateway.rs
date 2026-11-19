//! Authenticated outer listener for a team executor's ordinary Meerschaum API.
//!
//! The listener starts with a public verifier, without opening any instance or loading plugins.
//! Only a signed configuration can construct the inner API. Neither ordinary login credentials
//! nor headers supplied by clients can stand in for the verified request extensions.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{Method, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Semaphore};
use tower::ServiceExt;
use uuid::Uuid;

use super::control::{Claims, ControlError, ReplayCache, Verifier};
use crate::error::ApiError;

/// The signed control configuration endpoint, outside the ordinary public API.
pub const CONFIGURATION_PATH: &str = "/_mrsm/executor/configuration";
/// A signed health probe, including identity and installed configuration revision.
pub const HEALTH_PATH: &str = "/_mrsm/executor/health";
/// Revoke this process until it is restarted with a fresh bootstrap generation.
pub const REVOKE_PATH: &str = "/_mrsm/executor/revoke";
/// Signed terminal lease operations; suffix is a main-owned terminal UUID, never a console id.
pub const TERMINALS_PATH: &str = "/_mrsm/executor/terminals/";
/// Main-owned execution profile updates; suffix is an opaque launch UUID.
pub const CONTEXTS_PATH: &str = "/_mrsm/executor/contexts/";
/// Only this signed, structurally valid context response permits one exact source pull/retry.
pub const SOURCE_CATALOG_MISSING: &str = "execution_source_catalog_missing";
/// Signed metadata and exact Main-API pull capabilities. This endpoint accepts no archives.
pub const PLUGIN_DELIVERY_PATH: &str = "/_mrsm/executor/plugin-delivery";
/// Maximum complete control request, including configuration and plugin archives.
pub const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;
/// Stop long-lived work when the main API can no longer renew authenticated control.
pub const CONTROL_LEASE_SECONDS: u64 = 90;

/// Authenticated caller information attached by the outer listener, never by HTTP headers.
#[derive(Clone, Debug)]
pub struct VerifiedRequest {
    /// Signed caller identity and request binding.
    pub claims: Claims,
    /// Current display name from the signed team configuration.
    pub team_name: String,
}

tokio::task_local! {
    static CALLER: VerifiedRequest;
}

/// The executor gateway's authenticated caller in the current request task.
pub fn current() -> Option<VerifiedRequest> {
    CALLER.try_with(Clone::clone).ok()
}

/// Preserve a verified caller when an accepted operation moves to a background task.
pub async fn scope<F: std::future::Future>(
    caller: Option<VerifiedRequest>,
    operation: F,
) -> F::Output {
    match caller {
        Some(caller) => CALLER.scope(caller, operation).await,
        None => operation.await,
    }
}

/// A complete team-filtered runtime configuration. It never contains a main-API signing key.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    /// Immutable source/metadata catalog. Rotating repository credentials are delivered separately.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_sha256: Option<String>,
    /// The one team in the bootstrap verifier.
    pub team_id: Uuid,
    /// Team display name for executor job metadata and audit messages.
    pub team_name: String,
    /// Credential generation, changed when replacing or revoking the service.
    pub generation: Uuid,
    /// Explicit plugin grants, including plugins with no configuration section.
    pub plugins: std::collections::BTreeSet<String>,
    /// Configuration and connector definitions already filtered by the main API.
    pub config: meerschaum_core::Map,
}

impl Configuration {
    /// Stable revision of the encoded configuration used by the control client.
    pub fn fingerprint(&self) -> Result<String, serde_json::Error> {
        Ok(fingerprint(&serde_json::to_vec(self)?))
    }
}

/// Native runtime construction lives in the CLI, above the server in the dependency graph.
#[async_trait]
pub trait RuntimeFactory: Send + Sync {
    /// Construct an ordinary API using only this team's configuration and private state root.
    /// Failure is intentionally reported without reflecting configuration or connector secrets.
    async fn build(&self, configuration: Configuration) -> Result<Router, RuntimeError>;
    /// Stop team jobs, extension hosts and other long-lived work after generation revocation.
    async fn revoke(&self);
    /// Stage bounded data by pulling exact sources from Main; never load code here.
    async fn deliver_plugins(
        &self,
        _delivery: super::plugin_delivery::Delivery,
    ) -> Result<String, RuntimeError> {
        Err(RuntimeError)
    }
    /// Leased executor sessions; roles without terminal hosting return none.
    fn terminal_sessions(&self) -> Option<Arc<super::terminal::TerminalSessions>> {
        None
    }
    /// Retire exactly one server-generated terminal name on this runtime's private tmux server.
    async fn end_terminal(&self, _name: &str) {}
    /// Check only exact source-catalog presence. Corruption, links and I/O errors are refusals.
    async fn context_sources_available(
        &self,
        _actor: &super::control::Actor,
        _update: &super::environment::Update,
    ) -> Result<bool, RuntimeError> {
        Ok(true)
    }
    /// Install a signed launch profile/credential, using paths generated by the executor.
    async fn update_context(
        &self,
        _actor: &super::control::Actor,
        _update: super::environment::Update,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError)
    }
    /// Sign only a completed native Metadata receipt from a constructor-verified Factory.
    /// Ordinary runtimes/test doubles cannot manufacture this from job properties or JSON.
    async fn metadata_quote(
        &self,
        _actor: &super::control::Actor,
        _configuration_sha256: &str,
        _request: super::factory_attestation::quote::Request,
    ) -> Result<super::factory_attestation::quote::Response, RuntimeError> {
        Err(RuntimeError)
    }
    /// Revoke one previously delivered context, never a path supplied by a remote response.
    async fn revoke_context(
        &self,
        _actor: &super::control::Actor,
        _id: Uuid,
        _revision: u64,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError)
    }
}

/// A sanitized failure to construct a team's runtime, without connector or config values.
#[derive(Debug, thiserror::Error)]
#[error("The executor runtime could not be configured.")]
pub struct RuntimeError;

struct Runtime {
    revision: String,
    team_name: String,
    router: Router,
}

struct Inner {
    verifier: Verifier,
    replays: ReplayCache,
    factory: Arc<dyn RuntimeFactory>,
    runtime: Mutex<Option<Runtime>>,
    revoked: std::sync::atomic::AtomicBool,
    requests: Semaphore,
    lease: std::sync::Mutex<Option<tokio::time::Instant>>,
    watchdog: std::sync::atomic::AtomicBool,
}

/// A bootstrap-only listener. Its inner API is reachable exclusively through this gateway.
#[derive(Clone)]
pub struct Gateway(Arc<Inner>);

impl Gateway {
    /// Validate the public bootstrap before binding any listener.
    pub fn new(verifier: Verifier, factory: Arc<dyn RuntimeFactory>) -> Result<Self, ControlError> {
        verifier.validate()?;
        Ok(Self(Arc::new(Inner {
            verifier,
            replays: ReplayCache::default(),
            factory,
            runtime: Mutex::new(None),
            revoked: false.into(),
            requests: Semaphore::new(64),
            lease: std::sync::Mutex::new(None),
            watchdog: false.into(),
        })))
    }

    /// The sole external router; do not bind the configured inner router separately.
    pub fn router(self) -> Router {
        Router::new().fallback(handle).with_state(self)
    }
}

fn keepalive(gateway: &Gateway) {
    use std::sync::atomic::Ordering;
    *gateway.0.lease.lock().unwrap_or_else(|e| e.into_inner()) = Some(tokio::time::Instant::now());
    if gateway.0.watchdog.swap(true, Ordering::AcqRel) {
        return;
    }
    let weak = Arc::downgrade(&gateway.0);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            let Some(inner) = weak.upgrade() else { return };
            let expired = inner
                .lease
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_some_and(|last| last.elapsed() >= Duration::from_secs(CONTROL_LEASE_SECONDS));
            if expired {
                inner.revoked.store(true, Ordering::Release);
                *inner.runtime.lock().await = None;
                inner.factory.revoke().await;
                return;
            }
        }
    });
}

fn fingerprint(bytes: &[u8]) -> String {
    super::control::body_digest(bytes)
}

async fn handle(State(gateway): State<Gateway>, request: Request) -> Response {
    match dispatch(gateway, request).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn dispatch(gateway: Gateway, request: Request) -> Result<Response, ApiError> {
    use std::sync::atomic::Ordering;
    let inner = &gateway.0;
    let _permit = inner.requests.try_acquire().map_err(|_| {
        ApiError::new(
            429,
            "The executor's request capacity is exhausted; retry shortly.",
        )
    })?;
    if inner.revoked.load(Ordering::Acquire) {
        return Err(ApiError::unavailable(
            "This executor generation has been revoked.",
        ));
    }
    let (mut parts, body) = request.into_parts();
    let token = parts
        .headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .filter(|s| s.len() <= super::control::MAX_TOKEN_BYTES)
        .ok_or_else(ApiError::invalid_credentials)?
        .to_owned();
    let path = parts.uri.path_and_query().map_or("/", |p| p.as_str());
    let head = inner
        .verifier
        .authenticate_head(
            &token,
            parts.method.as_str(),
            path,
            chrono::Utc::now().timestamp(),
        )
        .map_err(|_| ApiError::invalid_credentials())?;
    let body = tokio::time::timeout(Duration::from_secs(15), to_bytes(body, MAX_REQUEST_BYTES))
        .await
        .map_err(|_| ApiError::new(408, "Executor request body timed out."))?
        .map_err(|_| ApiError::new(413, "Executor request body exceeds its size limit."))?;
    let claims = head
        .finish(&body, chrono::Utc::now().timestamp(), &inner.replays)
        .map_err(|error| match error {
            ControlError::Capacity => ApiError::new(429, error.to_string()),
            _ => ApiError::invalid_credentials(),
        })?;
    keepalive(&gateway);

    if parts.uri.path() == REVOKE_PATH && parts.method == Method::POST {
        inner.revoked.store(true, Ordering::Release);
        *inner.runtime.lock().await = None;
        inner.factory.revoke().await;
        return Ok(axum::Json(serde_json::json!({"revoked": true})).into_response());
    }
    if parts.uri.path() == PLUGIN_DELIVERY_PATH && parts.method == Method::POST {
        let delivery: super::plugin_delivery::Delivery = serde_json::from_slice(&body)
            .map_err(|_| ApiError::new(400, "Invalid plugin delivery."))?;
        let expected = delivery
            .validate(
                inner.verifier.team_id,
                inner.verifier.generation,
                &Default::default(),
            )
            .map_err(|_| ApiError::new(403, "The plugin delivery does not match this executor."))?
            .digest()
            .map_err(|_| ApiError::new(400, "Invalid plugin catalog."))?;
        // Staging data may precede configuration. The ordinary API remains unreachable until
        // an independently signed configuration names the resulting immutable catalog digest.
        let digest = inner.factory.deliver_plugins(delivery).await.map_err(|_| {
            ApiError::unavailable("The exact plugin sources could not be delivered.")
        })?;
        if digest != expected {
            return Err(ApiError::unavailable(
                "The delivered plugin catalog did not match.",
            ));
        }
        return Ok(axum::Json(serde_json::json!({"catalog_sha256":digest})).into_response());
    }
    if parts.uri.path() == CONFIGURATION_PATH && parts.method == Method::PUT {
        if fingerprint(&body) != claims.configuration_sha256 {
            return Err(ApiError::new(
                409,
                "The configuration revision does not match.",
            ));
        }
        let configuration: Configuration = serde_json::from_slice(&body)
            .map_err(|_| ApiError::new(400, "Invalid executor configuration."))?;
        if configuration.team_id != inner.verifier.team_id
            || configuration.generation != inner.verifier.generation
        {
            return Err(ApiError::new(
                403,
                "The configuration names another executor.",
            ));
        }
        if configuration
            .catalog_sha256
            .as_ref()
            .is_some_and(|digest| !super::plugins::valid_digest(digest))
        {
            return Err(ApiError::new(400, "Invalid plugin catalog digest."));
        }
        if configuration.team_name.trim().is_empty()
            || configuration.team_name.len() > 256
            || configuration.team_name.chars().any(char::is_control)
        {
            return Err(ApiError::new(400, "Invalid executor team name."));
        }
        if configuration.plugins.len() > 1024
            || configuration.plugins.iter().any(|name| {
                name.is_empty()
                    || name.len() > 128
                    || !name
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
            })
        {
            return Err(ApiError::new(400, "Invalid executor plugin grants."));
        }
        // Construction owns its task. Timing out / disconnecting the HTTP request must not
        // detach a blocking plugin import and later leave an untracked runtime alive.
        let apply_inner = inner.clone();
        let revision = claims.configuration_sha256.clone();
        let mut application = tokio::spawn(async move {
            let team_name = configuration.team_name.clone();
            let mut runtime = apply_inner.runtime.lock().await;
            if apply_inner.revoked.load(Ordering::Acquire) {
                return Err(RuntimeError);
            }
            if runtime.as_ref().is_none_or(|r| r.revision != revision) {
                // Revoke the old configuration before construction: failure cannot leave old grants
                // serving new requests. Requests already admitted retain their original snapshot.
                *runtime = None;
                let built = apply_inner.factory.build(configuration).await;
                if apply_inner.revoked.load(Ordering::Acquire) || built.is_err() {
                    apply_inner.factory.revoke().await;
                    return Err(RuntimeError);
                }
                *runtime = Some(Runtime {
                    revision,
                    team_name,
                    router: built?,
                });
            }
            Ok(())
        });
        match tokio::time::timeout(Duration::from_secs(60), &mut application).await {
            Ok(Ok(Ok(()))) => {}
            Ok(_) => {
                return Err(ApiError::unavailable(
                    "The executor configuration could not be applied.",
                ));
            }
            Err(_) => {
                // The owned task observes this before admitting its runtime, then retires it.
                // A stuck import needs an administrator's restart with a fresh generation.
                inner.revoked.store(true, Ordering::Release);
                let retiring = inner.clone();
                tokio::spawn(async move {
                    *retiring.runtime.lock().await = None;
                    retiring.factory.revoke().await;
                });
                return Err(ApiError::unavailable(
                    "Executor configuration timed out; restart this executor generation.",
                ));
            }
        }
        return Ok(axum::Json(
            serde_json::json!({"configuration_sha256": claims.configuration_sha256}),
        )
        .into_response());
    }
    let runtime = inner.runtime.lock().await;
    if parts.uri.path() == HEALTH_PATH && parts.method == Method::GET {
        return Ok(axum::Json(serde_json::json!({
            "protocol": inner.verifier.protocol,
            "version": inner.verifier.version,
            "team_id": inner.verifier.team_id,
            "generation": inner.verifier.generation,
            "configuration_sha256": runtime.as_ref().map(|r| &r.revision),
            "ready": runtime.is_some(),
        }))
        .into_response());
    }
    let configured = runtime
        .as_ref()
        .filter(|r| r.revision == claims.configuration_sha256)
        .ok_or_else(|| {
            ApiError::unavailable("This executor does not have the authorized configuration.")
        })?;
    if parts.uri.path() == super::factory_attestation::quote::PATH {
        if parts.method != Method::POST || parts.uri.query().is_some() {
            return Err(ApiError::not_found());
        }
        if body.len() > super::factory_attestation::quote::MAX_REQUEST {
            return Err(ApiError::new(
                413,
                "The native Metadata proof request exceeds its bound.",
            ));
        }
        let request: super::factory_attestation::quote::Request = serde_json::from_slice(&body)
            .map_err(|_| ApiError::new(400, "Invalid native Metadata proof request."))?;
        request
            .validate()
            .map_err(|_| ApiError::new(400, "Invalid native Metadata proof request."))?;
        // Retain the configured runtime lock through the native proof. A reconfiguration
        // cannot relabel a completion as belonging to a different signed revision.
        let response = inner
            .factory
            .metadata_quote(&claims.actor, &configured.revision, request)
            .await
            .map_err(|_| {
                ApiError::unavailable("The native Metadata completion proof is unavailable.")
            })?;
        if inner.revoked.load(Ordering::Acquire)
            || claims.expires_at <= chrono::Utc::now().timestamp()
        {
            return Err(ApiError::invalid_credentials());
        }
        let encoded = serde_json::to_vec(&response).map_err(|_| {
            ApiError::unavailable("The native Metadata proof could not be encoded.")
        })?;
        if encoded.len() > super::factory_attestation::quote::MAX_RESPONSE {
            return Err(ApiError::unavailable(
                "The native Metadata proof exceeds its bound.",
            ));
        }
        return Ok(axum::Json(response).into_response());
    }
    if let Some(id) = parts.uri.path().strip_prefix(TERMINALS_PATH) {
        let id = Uuid::parse_str(id).map_err(|_| ApiError::not_found())?;
        let sessions = inner
            .factory
            .terminal_sessions()
            .ok_or_else(ApiError::not_found)?;
        if parts.method == Method::PUT {
            let grant: super::terminal::TerminalGrant = serde_json::from_slice(&body)
                .map_err(|_| ApiError::new(400, "Invalid terminal grant."))?;
            if grant.id != id {
                return Err(ApiError::new(400, "Terminal grant identity mismatch."));
            }
            sessions
                .renew(&claims.actor, grant)
                .map_err(|_| ApiError::new(403, "This terminal grant could not be renewed."))?;
        } else if parts.method == Method::DELETE {
            if let Some(name) = sessions
                .revoke(id, &claims.actor)
                .map_err(|_| ApiError::new(403, "This terminal could not be revoked."))?
            {
                inner.factory.end_terminal(&name).await;
            }
        } else {
            return Err(ApiError::not_found());
        }
        return Ok(axum::Json(serde_json::json!({"success": true})).into_response());
    }
    if let Some(id) = parts.uri.path().strip_prefix(CONTEXTS_PATH) {
        let id = Uuid::parse_str(id).map_err(|_| ApiError::not_found())?;
        if parts.method == Method::PUT {
            let update: super::environment::Update = serde_json::from_slice(&body)
                .map_err(|_| ApiError::new(400, "Invalid execution context."))?;
            if update.id != id {
                return Err(ApiError::new(400, "Execution context identity mismatch."));
            }
            update
                .validate(&claims.actor)
                .map_err(|_| ApiError::new(403, "This execution context was refused."))?;
            if !inner
                .factory
                .context_sources_available(&claims.actor, &update)
                .await
                .map_err(|_| {
                    ApiError::unavailable("The exact execution source catalog is unavailable.")
                })?
            {
                return Err(ApiError::with_detail(
                    409,
                    serde_json::json!({"code":SOURCE_CATALOG_MISSING}),
                ));
            }
            inner
                .factory
                .update_context(&claims.actor, update)
                .await
                .map_err(|_| {
                    ApiError::unavailable(
                        "This executor cannot apply the requested execution profile.",
                    )
                })?;
        } else if parts.method == Method::DELETE {
            let retire: super::environment::Retire = serde_json::from_slice(&body)
                .map_err(|_| ApiError::new(400, "Invalid execution context retirement."))?;
            if retire.revision == 0 {
                return Err(ApiError::new(400, "Invalid execution context revision."));
            }
            inner
                .factory
                .revoke_context(&claims.actor, id, retire.revision)
                .await
                .map_err(|_| ApiError::new(403, "This execution context could not be revoked."))?;
        } else {
            return Err(ApiError::not_found());
        }
        return Ok(axum::Json(serde_json::json!({"success": true})).into_response());
    }
    // Identity, credential issuance and team administration always remain on the main API.
    // Encoded aliases are decoded by the route group classifier too.
    let group = crate::routes::route_group_of(&crate::routes::decoded_path(parts.uri.path()));
    if matches!(group, "login" | "users" | "tokens" | "mcp" | "docs")
        || crate::routes::decoded_path(parts.uri.path()).starts_with("/_mrsm/")
    {
        return Err(ApiError::not_found());
    }
    let router = configured.router.clone();
    let team_name = configured.team_name.clone();
    drop(runtime);
    // The ordinary API is configured without public authentication. Remove all reusable public
    // credentials and untrusted routing headers; verified identity travels only in extensions.
    for name in [
        header::AUTHORIZATION,
        header::COOKIE,
        header::PROXY_AUTHORIZATION,
    ] {
        parts.headers.remove(name);
    }
    for name in ["x-mrsm-team", "x-mrsm-acting-team", "x-mrsm-executor-actor"] {
        parts.headers.remove(name);
    }
    let verified = VerifiedRequest { claims, team_name };
    parts.extensions.insert(verified.clone());
    let response = scope(
        Some(verified),
        router.oneshot(Request::from_parts(parts, Body::from(body))),
    )
    .await
    .unwrap_or_else(|never| match never {});
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::Extension;
    use axum::http::{HeaderMap, StatusCode};
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use crate::executor::control::{Actor, Authority};

    #[derive(Default)]
    struct Factory {
        builds: AtomicUsize,
        contexts: AtomicUsize,
        fail: AtomicBool,
        deliveries: AtomicUsize,
        missing_sources: AtomicBool,
    }

    #[async_trait]
    impl RuntimeFactory for Factory {
        async fn revoke(&self) {}

        async fn context_sources_available(
            &self,
            _: &Actor,
            _: &super::super::environment::Update,
        ) -> Result<bool, RuntimeError> {
            Ok(!self.missing_sources.load(Ordering::SeqCst))
        }

        async fn deliver_plugins(
            &self,
            delivery: super::super::plugin_delivery::Delivery,
        ) -> Result<String, RuntimeError> {
            let catalog = delivery
                .validate(
                    delivery.catalog.team_id,
                    delivery.catalog.generation,
                    &Default::default(),
                )
                .map_err(|_| RuntimeError)?;
            self.deliveries.fetch_add(1, Ordering::SeqCst);
            catalog.digest().map_err(|_| RuntimeError)
        }

        async fn update_context(
            &self,
            actor: &Actor,
            update: super::super::environment::Update,
        ) -> Result<(), RuntimeError> {
            update.validate(actor).map_err(|_| RuntimeError)?;
            self.contexts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn build(&self, _: Configuration) -> Result<Router, RuntimeError> {
            self.builds.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                return Err(RuntimeError);
            }
            Ok(Router::new().fallback(
                |Extension(verified): Extension<VerifiedRequest>, headers: HeaderMap| async move {
                    let claims = verified.claims;
                    assert!(!headers.contains_key(header::AUTHORIZATION));
                    assert!(!headers.contains_key(header::COOKIE));
                    assert!(!headers.contains_key("x-mrsm-acting-team"));
                    axum::Json(
                        serde_json::json!({"owner": claims.actor.owner, "team_id": claims.team_id}),
                    )
                },
            ))
        }
    }

    fn fixture() -> (Authority, Arc<Factory>, Router, Configuration) {
        let team_id = Uuid::new_v4();
        let generation = Uuid::new_v4();
        let (authority, _) = Authority::generate(Uuid::new_v4(), team_id, generation).unwrap();
        let factory = Arc::new(Factory::default());
        let router = Gateway::new(authority.verifier(), factory.clone())
            .unwrap()
            .router();
        let configuration = Configuration {
            catalog_sha256: None,
            team_id,
            team_name: "Example".into(),
            generation,
            plugins: Default::default(),
            config: Default::default(),
        };
        (authority, factory, router, configuration)
    }

    fn signed(
        authority: &Authority,
        method: Method,
        path: &str,
        body: Vec<u8>,
        revision: &str,
    ) -> Request {
        let token = authority
            .issue(
                Actor {
                    owner: "operator".into(),
                    username: Some("operator".into()),
                    permissions: BTreeSet::from(["jobs:write".into()]),
                    administrator: false,
                },
                super::super::control::Request {
                    method: method.as_str(),
                    path,
                    body: &body,
                },
                revision,
                chrono::Utc::now().timestamp(),
            )
            .unwrap();
        Request::builder()
            .method(method)
            .uri(path)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::COOKIE, "main-session=never-forward")
            .header("x-mrsm-acting-team", "another-team")
            .body(Body::from(body))
            .unwrap()
    }

    async fn configure(
        authority: &Authority,
        router: &Router,
        config: &Configuration,
    ) -> StatusCode {
        let body = serde_json::to_vec(config).unwrap();
        let request = signed(
            authority,
            Method::PUT,
            CONFIGURATION_PATH,
            body.clone(),
            &fingerprint(&body),
        );
        router.clone().oneshot(request).await.unwrap().status()
    }

    #[tokio::test]
    async fn plugin_delivery_secrets_are_request_bound_and_do_not_change_the_catalog_configuration_revision()
     {
        let (authority, factory, router, mut config) = fixture();
        config.plugins = BTreeSet::from(["probe".into()]);
        let mut body = serde_json::json!({
            "catalog":{"schema":1,"release":env!("CARGO_PKG_VERSION"),"team_id":config.team_id,"generation":config.generation,
                "plugins":{"probe":{"archive_sha256":"0".repeat(64),"kind":"python_module","metadata":null}}},
            "repository_origin":"https://main.example",
            "credentials":{"probe":{"authorization":"Mrsm-Plugin synthetic-one.signature","expires_at":chrono::Utc::now().timestamp()+30}}
        });
        let delivery: super::super::plugin_delivery::Delivery =
            serde_json::from_value(body.clone()).unwrap();
        config.catalog_sha256 = Some(
            delivery
                .validate(config.team_id, config.generation, &Default::default())
                .unwrap()
                .digest()
                .unwrap(),
        );
        let revision = config.fingerprint().unwrap();
        let request = signed(
            &authority,
            Method::POST,
            PLUGIN_DELIVERY_PATH,
            serde_json::to_vec(&body).unwrap(),
            &revision,
        );
        assert_eq!(
            router.clone().oneshot(request).await.unwrap().status(),
            StatusCode::OK
        );
        assert_eq!(
            factory.builds.load(Ordering::SeqCst),
            0,
            "Data staging must not load a runtime."
        );
        assert_eq!(
            configure(&authority, &router, &config).await,
            StatusCode::OK
        );
        body["credentials"]["probe"]["authorization"] =
            "Mrsm-Plugin synthetic-two.signature".into();
        let request = signed(
            &authority,
            Method::POST,
            PLUGIN_DELIVERY_PATH,
            serde_json::to_vec(&body).unwrap(),
            &revision,
        );
        assert_eq!(
            router.clone().oneshot(request).await.unwrap().status(),
            StatusCode::OK
        );
        assert_eq!(config.fingerprint().unwrap(), revision);
        assert_eq!(
            configure(&authority, &router, &config).await,
            StatusCode::OK
        );
        assert_eq!(factory.builds.load(Ordering::SeqCst), 1);
        assert_eq!(factory.deliveries.load(Ordering::SeqCst), 2);
        let mut request = signed(
            &authority,
            Method::POST,
            PLUGIN_DELIVERY_PATH,
            serde_json::to_vec(&body).unwrap(),
            &revision,
        );
        body["credentials"]["probe"]["authorization"] =
            "Mrsm-Plugin altered-in-transit.signature".into();
        *request.body_mut() = Body::from(body.to_string());
        assert_eq!(
            router.clone().oneshot(request).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        body["catalog"]["team_id"] = Uuid::new_v4().to_string().into();
        let request = signed(
            &authority,
            Method::POST,
            PLUGIN_DELIVERY_PATH,
            serde_json::to_vec(&body).unwrap(),
            &revision,
        );
        assert_eq!(
            router.clone().oneshot(request).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(factory.deliveries.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn unauthenticated_requests_never_read_a_body_or_construct_runtime() {
        let (_, factory, router, _) = fixture();
        let body = Body::from_stream(futures_util::stream::once(async {
            panic!("An unauthenticated request body was polled.");
            #[allow(unreachable_code)]
            Ok::<_, std::io::Error>(bytes::Bytes::new())
        }));
        let response = router
            .oneshot(Request::builder().uri("/execute").body(body).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(factory.builds.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn credential_delivery_is_signed_and_bound_to_the_owner_and_launch() {
        let (authority, factory, router, config) = fixture();
        assert_eq!(
            configure(&authority, &router, &config).await,
            StatusCode::OK
        );
        let id = Uuid::new_v4();
        let path = format!("{CONTEXTS_PATH}{id}");
        let revision = config.fingerprint().unwrap();
        let mut body = serde_json::json!({"id": id, "revision": 1, "owner": "operator", "owner_id": "registered-operator", "profile": "team", "target": {"kind": "terminal"}, "callback_url": "https://main.example:8989", "credential": {"token": "synthetic-short-callback", "expires_at": chrono::Utc::now().timestamp() + 30}});
        for mutation in ["owner", "id", "path", "expired", "callback"] {
            let mut invalid = body.clone();
            match mutation {
                "owner" => invalid["owner"] = "another-user".into(),
                "id" => invalid["id"] = Uuid::new_v4().to_string().into(),
                "path" => invalid["credential_file"] = "/some/private/path".into(),
                "expired" => invalid["credential"]["expires_at"] = 0.into(),
                "callback" => invalid["callback_url"] = "https://main.example/login?token=x".into(),
                _ => unreachable!(),
            }
            let request = signed(
                &authority,
                Method::PUT,
                &path,
                serde_json::to_vec(&invalid).unwrap(),
                &revision,
            );
            assert!(
                router
                    .clone()
                    .oneshot(request)
                    .await
                    .unwrap()
                    .status()
                    .is_client_error(),
                "{mutation}"
            );
        }
        assert_eq!(factory.contexts.load(Ordering::SeqCst), 0);
        let request = signed(
            &authority,
            Method::PUT,
            &path,
            serde_json::to_vec(&body).unwrap(),
            &revision,
        );
        assert_eq!(
            router.clone().oneshot(request).await.unwrap().status(),
            StatusCode::OK
        );
        assert_eq!(factory.contexts.load(Ordering::SeqCst), 1);
        // A signed request cannot be changed into another credential delivery in transit.
        let mut request = signed(
            &authority,
            Method::PUT,
            &path,
            serde_json::to_vec(&body).unwrap(),
            &revision,
        );
        body["credential"]["token"] = "changed-in-transit".into();
        *request.body_mut() = Body::from(body.to_string());
        assert_eq!(
            router.oneshot(request).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(factory.contexts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn only_a_valid_signed_context_can_report_a_missing_exact_source_catalog() {
        let (authority, factory, router, config) = fixture();
        assert_eq!(
            configure(&authority, &router, &config).await,
            StatusCode::OK
        );
        factory.missing_sources.store(true, Ordering::SeqCst);
        let id = Uuid::new_v4();
        let path = format!("{CONTEXTS_PATH}{id}");
        let revision = config.fingerprint().unwrap();
        let mut update = serde_json::json!({"id":id,"revision":1,"owner":"operator","owner_id":"registered-operator",
            "profile":"api","target":{"kind":"job","name":format!("mrsm-{}",id.simple())},
            "callback_url":"https://main.example","discovery_catalog_sha256":"a".repeat(64),
            "credential":{"token":"synthetic-exact-context","expires_at":chrono::Utc::now().timestamp()+30}});
        let response = router
            .clone()
            .oneshot(signed(
                &authority,
                Method::PUT,
                &path,
                serde_json::to_vec(&update).unwrap(),
                &revision,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let bytes = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["detail"]["code"],
            SOURCE_CATALOG_MISSING
        );
        assert_eq!(factory.contexts.load(Ordering::SeqCst), 0);
        update["owner"] = "another-owner".into();
        let refused = router
            .clone()
            .oneshot(signed(
                &authority,
                Method::PUT,
                &path,
                serde_json::to_vec(&update).unwrap(),
                &revision,
            ))
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        update["owner"] = "operator".into();
        factory.missing_sources.store(false, Ordering::SeqCst);
        let applied = router
            .oneshot(signed(
                &authority,
                Method::PUT,
                &path,
                serde_json::to_vec(&update).unwrap(),
                &revision,
            ))
            .await
            .unwrap();
        assert_eq!(applied.status(), StatusCode::OK);
        assert_eq!(factory.contexts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn signed_configuration_is_required_and_actor_is_an_extension() {
        let (authority, factory, router, config) = fixture();
        let revision = config.fingerprint().unwrap();
        let request = signed(
            &authority,
            Method::POST,
            "/execute?debug=false",
            b"{}".to_vec(),
            &revision,
        );
        assert_eq!(
            router.clone().oneshot(request).await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            configure(&authority, &router, &config).await,
            StatusCode::OK
        );
        assert_eq!(
            configure(&authority, &router, &config).await,
            StatusCode::OK
        );
        assert_eq!(factory.builds.load(Ordering::SeqCst), 1);
        let request = signed(
            &authority,
            Method::POST,
            "/execute?debug=false",
            b"{}".to_vec(),
            &revision,
        );
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["owner"], "operator");
        assert_eq!(value["team_id"], config.team_id.to_string());
    }

    #[tokio::test]
    async fn another_team_or_configuration_revision_cannot_initialize_runtime() {
        let (authority, factory, router, mut config) = fixture();
        let request = signed(
            &authority,
            Method::PUT,
            CONFIGURATION_PATH,
            serde_json::to_vec(&config).unwrap(),
            &"0".repeat(64),
        );
        assert_eq!(
            router.clone().oneshot(request).await.unwrap().status(),
            StatusCode::CONFLICT
        );
        config.team_id = Uuid::new_v4();
        assert_eq!(
            configure(&authority, &router, &config).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(factory.builds.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn failed_reconfiguration_disables_the_old_runtime() {
        let (authority, factory, router, mut config) = fixture();
        assert_eq!(
            configure(&authority, &router, &config).await,
            StatusCode::OK
        );
        let old_revision = config.fingerprint().unwrap();
        factory.fail.store(true, Ordering::SeqCst);
        config
            .config
            .insert("new_grants".into(), meerschaum_core::Value::Bool(true));
        assert_eq!(
            configure(&authority, &router, &config).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        for revision in [old_revision, config.fingerprint().unwrap()] {
            let request = signed(&authority, Method::GET, "/jobs", Vec::new(), &revision);
            assert_eq!(
                router.clone().oneshot(request).await.unwrap().status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
        }
    }

    #[tokio::test]
    async fn identity_routes_and_encoded_aliases_are_never_exposed() {
        let (authority, _, router, config) = fixture();
        assert_eq!(
            configure(&authority, &router, &config).await,
            StatusCode::OK
        );
        for path in [
            "/users/admin",
            "/%75sers/admin",
            "/teams",
            "/tokens",
            "/login",
            "/mcp",
            "/_mrsm/private",
        ] {
            let request = signed(
                &authority,
                Method::GET,
                path,
                Vec::new(),
                &config.fingerprint().unwrap(),
            );
            assert_eq!(
                router.clone().oneshot(request).await.unwrap().status(),
                StatusCode::NOT_FOUND,
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn replay_and_revocation_cannot_reenter_the_runtime() {
        let (authority, _, router, config) = fixture();
        assert_eq!(
            configure(&authority, &router, &config).await,
            StatusCode::OK
        );
        let revision = config.fingerprint().unwrap();
        let first = signed(&authority, Method::GET, HEALTH_PATH, Vec::new(), &revision);
        let token = first.headers()[header::AUTHORIZATION].clone();
        assert_eq!(
            router.clone().oneshot(first).await.unwrap().status(),
            StatusCode::OK
        );
        let replay = Request::builder()
            .uri(HEALTH_PATH)
            .header(header::AUTHORIZATION, token)
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            router.clone().oneshot(replay).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let revoke = signed(&authority, Method::POST, REVOKE_PATH, Vec::new(), &revision);
        assert_eq!(
            router.clone().oneshot(revoke).await.unwrap().status(),
            StatusCode::OK
        );
        assert_eq!(
            configure(&authority, &router, &config).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_timed_out_build_is_owned_until_it_finishes_and_is_then_revoked() {
        #[derive(Default)]
        struct SlowFactory {
            entered: tokio::sync::Notify,
            release: tokio::sync::Notify,
            revocations: AtomicUsize,
        }
        #[async_trait]
        impl RuntimeFactory for SlowFactory {
            async fn build(&self, _: Configuration) -> Result<Router, RuntimeError> {
                self.entered.notify_one();
                self.release.notified().await;
                Ok(Router::new())
            }
            async fn revoke(&self) {
                self.revocations.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (authority, _, _, config) = fixture();
        let factory = Arc::new(SlowFactory::default());
        let router = Gateway::new(authority.verifier(), factory.clone())
            .unwrap()
            .router();
        let request = signed(
            &authority,
            Method::PUT,
            CONFIGURATION_PATH,
            serde_json::to_vec(&config).unwrap(),
            &config.fingerprint().unwrap(),
        );
        let application = tokio::spawn(router.clone().oneshot(request));
        factory.entered.notified().await;
        tokio::time::advance(Duration::from_secs(61)).await;
        assert_eq!(
            application.await.unwrap().unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        factory.release.notify_one();
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(factory.revocations.load(Ordering::SeqCst) > 0);
        assert_eq!(
            configure(&authority, &router, &config).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test(start_paused = true)]
    async fn losing_authenticated_control_expires_the_generation_and_stops_serving() {
        let (authority, _, router, config) = fixture();
        assert_eq!(
            configure(&authority, &router, &config).await,
            StatusCode::OK
        );
        // Give the watchdog its first poll before advancing the clock.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(CONTROL_LEASE_SECONDS + 1)).await;
        tokio::task::yield_now().await;
        let request = signed(
            &authority,
            Method::GET,
            HEALTH_PATH,
            Vec::new(),
            &config.fingerprint().unwrap(),
        );
        assert_eq!(
            router.oneshot(request).await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
