//! One exact engine target for original/service authority, references and storage.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{
    AppState,
    access::{self, Access, RefFilter},
    backends::EngineBackend,
    engine_store::InstanceEngine,
    error::ApiError,
    routes::Ctx,
};
use meerschaum_connector::InstanceConnector;
use meerschaum_connector::api_target::NativeApiTarget;
use meerschaum_core::{Map, Value};
use sha2::{Digest, Sha256};

const MAX_SELECTED_ENGINES: usize = 64;

/// Main-only native factory capability. The configured API is an authentication boundary,
/// not a grant of its backing SQL label to the initiating caller.
#[derive(Clone)]
pub struct SelfApiBinding {
    logical: String,
    configured: Arc<dyn InstanceConnector>,
    native: Arc<dyn InstanceConnector>,
    target: NativeApiTarget,
    /// Successful password verification for this exact immutable configured target only.
    /// A changed stored password hash requires a new verification before using the service.
    pub(super) verified_password: Arc<tokio::sync::Mutex<Option<String>>>,
    configured_sha256: String,
    definition_sha256: String,
}

/// No Serialize/Deserialize, environment/property adapter or request-body constructor.
#[derive(Default)]
pub struct SelfApiBindings {
    entries: HashMap<String, SelfApiBinding>,
}
type NativeInstances = HashMap<String, Arc<dyn InstanceConnector>>;
impl SelfApiBindings {
    pub(super) fn binding(&self, logical: &str) -> Option<&SelfApiBinding> {
        self.entries.get(logical)
    }
    pub(crate) fn from_native(
        config: &crate::ServerConfig,
        resolver: &dyn crate::backends::InstanceResolver,
        native: Arc<dyn InstanceConnector>,
    ) -> Result<(Self, NativeInstances), String> {
        let mut result = Self::default();
        let mut instances = HashMap::new();
        let Some(advertised) = config.execution_callback_url.as_deref() else {
            return Ok((result, instances));
        };
        if !config.allow_multiple_instances || native.connector_type() != "sql" {
            return Ok((result, instances));
        }
        let trusted = origin(advertised)?;
        let live = resolver.connectors();
        let connectors = live.as_deref().unwrap_or(&config.connectors);
        let Some(Value::Map(apis)) = connectors.get("api") else {
            return Ok((result, instances));
        };
        for (label, attributes) in apis {
            let logical = format!("api:{label}");
            if !config
                .allowed_instance_keys
                .iter()
                .any(|pattern| crate::config::fnmatch(&logical, pattern))
            {
                continue;
            }
            let Some(configured) = resolver.resolve(&logical) else {
                continue;
            };
            let Some(target) = configured.native_api_target() else {
                continue;
            };
            if origin(&target.origin)? != trusted
                || target.instance.as_deref() != Some(native.connector_keys().as_str())
            {
                continue;
            }
            let Value::Map(attributes) = attributes else {
                return Err("Invalid native self-API definition.".into());
            };
            result.bind_native(
                &logical,
                configured.clone(),
                native.clone(),
                advertised,
                attributes,
            )?;
            instances.insert(logical, configured);
        }
        Ok((result, instances))
    }

    /// Called by the native AppState builder only, with the actual configured API object
    /// and exact native home connector. URL equality alone must not mint this capability.
    pub fn bind_native(
        &mut self,
        logical: &str,
        configured: Arc<dyn InstanceConnector>,
        native: Arc<dyn InstanceConnector>,
        trusted_callback_url: &str,
        configured_attributes: &Map,
    ) -> Result<(), String> {
        let target = configured
            .native_api_target()
            .ok_or("The configured API transport cannot prove a native target.")?;
        let configured_sha256 = configured_digest(configured_attributes)?;
        if self.entries.len() >= MAX_SELECTED_ENGINES
            || !logical.starts_with("api:")
            || configured.connector_keys() != logical
            || configured.connector_type() != "api"
            || native.connector_type() != "sql"
            || target.logical != logical
            || target.instance.as_deref() != Some(native.connector_keys().as_str())
            || origin(&target.origin)? != origin(trusted_callback_url)?
            || !crate::executor::plugins::valid_digest(&target.definition_sha256)
            || self.entries.contains_key(logical)
        {
            return Err("Invalid native self-API engine binding.".into());
        }
        self.entries.insert(
            logical.into(),
            SelfApiBinding {
                logical: logical.into(),
                configured,
                native,
                configured_sha256,
                definition_sha256: target.definition_sha256.clone(),
                target,
                verified_password: Arc::default(),
            },
        );
        Ok(())
    }
}

#[derive(PartialEq, Eq)]
struct Origin {
    scheme: String,
    host: String,
    port: u16,
}

fn origin(value: &str) -> Result<Origin, String> {
    crate::executor::environment::validate_callback_url(value)?;
    let uri: axum::http::Uri = value.parse().map_err(|_| "Invalid self-API origin.")?;
    let scheme = uri.scheme_str().ok_or("Invalid self-API origin.")?;
    let host = uri.host().ok_or("Invalid self-API origin.")?;
    Ok(Origin {
        scheme: scheme.to_ascii_lowercase(),
        host: host.to_ascii_lowercase(),
        port: uri
            .port_u16()
            .unwrap_or(if scheme == "https" { 443 } else { 80 }),
    })
}

fn configured_digest(attributes: &Map) -> Result<String, String> {
    let bytes = serde_json::to_vec(attributes).map_err(|_| "Invalid self-API definition.")?;
    if bytes.len() > 64 * 1024 {
        return Err("The configured self-API definition exceeds its bound.".into());
    }
    let mut digest = Sha256::new();
    digest.update(b"mrsm-native-self-api-config-v1\0");
    digest.update(bytes);
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

impl SelfApiBinding {
    pub(super) fn target(&self) -> &NativeApiTarget {
        &self.target
    }

    /// Live config is not covered by a database writer lock. Recheck it even when raw DB
    /// authorization facts are pinned to that exact held transaction.
    pub(super) fn current(&self, state: &AppState) -> Result<(), ApiError> {
        let refuse = || ApiError::new(409, "The native self-API engine binding changed.");
        if !Arc::ptr_eq(&self.native, &state.instance()) {
            return Err(refuse());
        }
        let selected = state.get_api_connector(Some(&self.logical))?;
        if !Arc::ptr_eq(&selected, &self.configured) {
            return Err(refuse());
        }
        let target = selected.native_api_target().ok_or_else(refuse)?;
        let label = self.logical.strip_prefix("api:").ok_or_else(refuse)?;
        let live = state.connectors();
        let attributes = live
            .get("api")
            .and_then(|api| api.get(label))
            .and_then(|attributes| match attributes {
                Value::Map(attributes) => Some(attributes),
                _ => None,
            })
            .ok_or_else(refuse)?;
        if configured_digest(attributes).map_err(|_| refuse())? != self.configured_sha256
            || target.definition_sha256 != self.definition_sha256
            || target.logical != self.logical
            || target.instance != self.target.instance
            || origin(&target.origin).map_err(|_| refuse())?
                != origin(&self.target.origin).map_err(|_| refuse())?
            || state
                .config
                .execution_callback_url
                .as_ref()
                .is_none_or(|advertised| {
                    origin(advertised).ok() != origin(&self.target.origin).ok()
                })
        {
            return Err(refuse());
        }
        Ok(())
    }
}

struct Cached {
    connector: Arc<dyn InstanceConnector>,
    backend: Arc<dyn EngineBackend>,
}
/// Retain fallback lease tables until AppState drops. Never reset them each request or
/// evict a backend whose lease could still be active. New labels fail at the finite cap.
#[derive(Default)]
pub struct EngineCache {
    entries: Mutex<HashMap<String, Cached>>,
}

#[derive(Clone)]
pub struct SelectedEngine {
    logical: String,
    connector: Arc<dyn InstanceConnector>,
    backend: Arc<dyn EngineBackend>,
    mapping: RefMapping,
    service: Option<SelfApiBinding>,
    authority: Arc<tokio::sync::OnceCell<super::service::ServiceAuthority>>,
    current_service: Arc<Mutex<Option<Access>>>,
    pub(super) runtime_graph: Arc<std::sync::OnceLock<super::runtime::RuntimeGraph>>,
}
#[derive(Clone, Copy)]
enum RefMapping {
    ApiLocal,
    ExactLogical,
}

impl EngineCache {
    pub fn select(
        &self,
        ctx: &Ctx,
        caller: &Access,
        self_api: &SelfApiBindings,
    ) -> Result<SelectedEngine, ApiError> {
        let requested = ctx.instance_keys();
        let logical = requested
            .as_deref()
            .unwrap_or(&ctx.state.config.instance_keys)
            .to_string();
        let home = ctx.state.instance();
        if logical == ctx.state.config.instance_keys || logical == home.connector_keys() {
            // Existing direct API requests keep the existing home backend and API-local
            // client-ref convention, including explicit administrator/no-auth behavior.
            access::guard::check_connector_teams(&ctx.state, caller, &home.connector_keys())?;
            return Ok(SelectedEngine {
                logical,
                connector: home,
                backend: ctx.state.engine.clone(),
                mapping: RefMapping::ApiLocal,
                service: None,
                authority: Arc::default(),
                current_service: Arc::default(),
                runtime_graph: Arc::default(),
            });
        }
        let (kind, label) = meerschaum_core::team_scope::api_execution::split_keys(&logical)
            .map_err(|_| ApiError::new(422, "Invalid selected engine instance."))?;
        if !matches!(kind, "sql" | "api" | "valkey") || label == "default" {
            return Err(ApiError::new(
                501,
                "This logical engine backend is unsupported.",
            ));
        }
        // Check the initiating caller's exact logical label before resolving anything.
        access::guard::check_connector_teams(&ctx.state, caller, &logical)?;
        let configured = ctx.state.get_api_connector(Some(&logical))?;
        if configured.connector_keys() != logical {
            return Err(ApiError::new(409, "The logical engine identity changed."));
        }
        let service = self_api.entries.get(&logical).cloned();
        if kind == "api"
            && let Some(binding) = &service
            && (!Arc::ptr_eq(&binding.configured, &configured)
                || !Arc::ptr_eq(&binding.native, &home)
                || binding.logical != logical)
        {
            return Err(ApiError::new(409, "The native self-API binding changed."));
        }
        // Definition digest + actual configured service credential must be checked
        // by ServiceAuthority before any native index/read/write is admitted.
        let connector = service
            .as_ref()
            .map(|binding| binding.native.clone())
            .unwrap_or(configured.clone());
        let mut cache = self
            .entries
            .lock()
            .map_err(|_| ApiError::unavailable("Selected engine cache is unavailable."))?;
        let backend = if let Some(cached) = cache.get(&logical) {
            if !Arc::ptr_eq(&cached.connector, &connector) {
                return Err(ApiError::new(
                    409,
                    "The selected engine connector changed; reconcile it before retrying.",
                ));
            }
            cached.backend.clone()
        } else {
            if cache.len() >= MAX_SELECTED_ENGINES {
                return Err(ApiError::unavailable(
                    "The selected engine capacity is exhausted.",
                ));
            }
            let backend: Arc<dyn EngineBackend> =
                Arc::new(InstanceEngine::new(connector.clone(), &logical));
            cache.insert(
                logical.clone(),
                Cached {
                    connector: connector.clone(),
                    backend: backend.clone(),
                },
            );
            backend
        };
        let mapping = if kind == "api" {
            RefMapping::ExactLogical
        } else {
            RefMapping::ApiLocal
        };
        Ok(SelectedEngine {
            logical,
            connector,
            backend,
            mapping,
            service,
            authority: Arc::default(),
            current_service: Arc::default(),
            runtime_graph: Arc::default(),
        })
    }
}

impl SelectedEngine {
    /// Translate an actual native committed inference through the same earlier
    /// process-local target proof. This does not admit new SQL or data work.
    pub(crate) async fn native_graph_inference(
        &self,
        state: &AppState,
        headers: &axum::http::HeaderMap,
        binding: &crate::executor::pipe_operation::CallbackBinding,
        target: &meerschaum_connector::mutation::NativeTarget,
        change: &meerschaum_connector::mutation::MetadataInference,
    ) -> Result<meerschaum_connector::mutation::MetadataInference, ApiError> {
        change
            .validate()
            .map_err(|reason| ApiError::new(409, reason))?;
        self.require_native_write()?;
        if !self.is_native_alias()
            || target.physical_reference() != change.pipe.pipe_ref()
            || target.registration() != change.registration.py_str()
        {
            return Err(ApiError::new(
                409,
                "Native inference lost its captured target mapping.",
            ));
        }
        let registry =
            super::registry::NativeRegistry::load(state, &self.logical, self.connector.clone())
                .await?;
        let original = self
            .fresh_headers_inner(state, headers, &["pipes:write"], Some(binding))
            .await?;
        let service = self
            .service_access()
            .ok_or_else(ApiError::invalid_credentials)?;
        service.require(&["pipes:read", "pipes:write"], &[])?;
        let object =
            registry.check(state, &original, &service, target.logical_reference(), true)?;
        if object.physical.pipe_ref() != target.physical_reference()
            || object.logical.registration_id().as_deref() != Some(target.registration())
            || object.physical.raw_parameters() != &change.after
        {
            return Err(ApiError::new(
                409,
                "Committed inference differs from the actual native registry.",
            ));
        }
        let mut logical = change.clone();
        logical.pipe = object.logical.clone();
        Ok(logical)
    }
    /// The graph data route alone may consume this typed native mapping. All ordinary
    /// alias paths retain their refusal until their own guarded native adapter exists.
    pub(crate) async fn native_graph_target(
        &self,
        state: &AppState,
        headers: &axum::http::HeaderMap,
        reference: &str,
    ) -> Result<
        (
            meerschaum_core::Pipe,
            meerschaum_connector::mutation::DataWindow,
        ),
        ApiError,
    > {
        self.require_native_write()?;
        if !self.is_native_alias() {
            return Err(ApiError::unavailable(
                "The graph has no captured native API target.",
            ));
        }
        let initial = access::require_fresh(state, headers, &["pipes:write"]).await?;
        let admitted = state
            .actions
            .pipe_operation_read(&initial)
            .await
            .map_err(|reason| ApiError::new(409, reason))?
            .ok_or_else(|| {
                ApiError::unavailable("The native alias has no canonical graph window.")
            })?;
        self.fresh_headers_inner(state, headers, &["pipes:write"], Some(&admitted.binding))
            .await?;
        let registry =
            super::registry::NativeRegistry::load(state, &self.logical, self.connector.clone())
                .await?;
        let original = self
            .fresh_headers_inner(state, headers, &["pipes:write"], Some(&admitted.binding))
            .await?;
        let service = self
            .service_access()
            .ok_or_else(ApiError::invalid_credentials)?;
        service.require(&["pipes:read", "pipes:write"], &[])?;
        let object = registry
            .check(state, &original, &service, reference, true)?
            .clone();
        let fresh = state
            .actions
            .pipe_operation_read(&original)
            .await
            .map_err(|reason| ApiError::new(409, reason))?
            .ok_or_else(|| {
                ApiError::unavailable("The native alias lost its canonical graph window.")
            })?;
        if fresh.binding != admitted.binding || fresh.issuer != admitted.issuer {
            return Err(ApiError::new(
                409,
                "The native graph source changed during mapping.",
            ));
        }
        let window = fresh.write_window(&object.logical)?;
        let final_source = self
            .fresh_headers_inner(state, headers, &["pipes:write"], Some(&fresh.binding))
            .await?;
        if final_source.principal != fresh.caller.principal
            || final_source.primary_team != fresh.caller.primary_team
        {
            return Err(ApiError::new(
                409,
                "The native graph source changed after registry preparation.",
            ));
        }
        let service = self
            .service_access()
            .ok_or_else(ApiError::invalid_credentials)?;
        registry.check(state, &final_source, &service, reference, true)?;
        let target = meerschaum_connector::mutation::NativeTarget::from_registered(
            &object.logical,
            &object.physical,
        )
        .map_err(|reason| ApiError::new(409, reason))?;
        Ok((
            object.physical,
            window
                .for_native_target(&target)
                .map_err(|reason| ApiError::new(409, reason))?,
        ))
    }
    /// Map one already registered logical destination through this host-owned native binding.
    /// The returned physical Pipe never changes the original actor's logical connector grant.
    pub(crate) async fn native_data_pipe(
        &self,
        state: &AppState,
        headers: &axum::http::HeaderMap,
        reference: &str,
    ) -> Result<meerschaum_core::Pipe, ApiError> {
        self.require_native_write()?;
        if !self.is_native_alias() {
            return Err(ApiError::new(
                501,
                "This API destination has no bound native mutation capability.",
            ));
        }
        let registry =
            super::registry::NativeRegistry::load(state, &self.logical, self.connector.clone())
                .await?;
        let caller = self.fresh_headers(state, headers, super::SCOPES).await?;
        if matches!(&caller.auth, Some(crate::auth::Principal::Execution(identity)) if identity.grant.required.contains("pipes:write"))
        {
            return Err(ApiError::new(
                501,
                "A tracked logical API pipe operation requires native graph-admission mapping before it can write.",
            ));
        }
        let service = self
            .service_access()
            .ok_or_else(ApiError::invalid_credentials)?;
        service.require(&["pipes:read", "pipes:write"], &[])?;
        Ok(registry
            .check(state, &caller, &service, reference, true)?
            .physical
            .clone())
    }

    pub(crate) fn has_password_service(&self) -> bool {
        self.service.as_ref().is_some_and(|binding| {
            matches!(
                binding.target().credential,
                meerschaum_connector::api_target::ServiceCredential::Password { .. }
            )
        })
    }
    pub(crate) fn is_native_alias(&self) -> bool {
        self.service.is_some()
    }
    pub(crate) fn service_access(&self) -> Option<Access> {
        self.current_service
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
    pub(crate) fn unrestricted(&self, caller: &Access) -> bool {
        caller.is_admin() && self.service.is_none()
    }
    pub fn logical(&self) -> &str {
        &self.logical
    }
    pub fn backend(&self) -> Arc<dyn EngineBackend> {
        self.backend.clone()
    }
    pub fn connector(&self) -> Arc<dyn InstanceConnector> {
        self.connector.clone()
    }

    /// Only an injected, still-current native binding can support composite scoped writes.
    /// A configured external API cannot silently fall back to Main or use a preflight as
    /// original-after-lock authorization.
    pub fn require_native_write(&self) -> Result<(), ApiError> {
        if let Some(binding) = &self.service {
            meerschaum_connector::check_protected(
                binding.configured.as_ref(),
                "write through the selected native API binding",
            )
            .map_err(|reason| ApiError::new(403, reason))?;
        }
        if self.connector.connector_type() == "valkey" {
            return Err(ApiError::new(
                501,
                "Scoped Valkey engine mutations require a native conditional authority capability.",
            ));
        }
        if matches!(self.mapping, RefMapping::ExactLogical) && self.service.is_none() {
            return Err(ApiError::new(
                501,
                "Scoped writes through an external API require a bound forwarding capability.",
            ));
        }
        Ok(())
    }

    /// Retain this backend and its exact native registry through history and projection.
    pub fn filter(&self, state: &AppState, caller: &Access) -> RefFilter {
        let filter =
            RefFilter::new(state, caller, &self.logical).with_selected_engine(self.backend.clone());
        let filter = if self.is_native_alias() {
            filter.with_native_registry(
                &self.logical,
                self.connector.clone(),
                self.service_access(),
            )
        } else {
            filter
        };
        match self.mapping {
            RefMapping::ApiLocal => filter.for_engine(),
            RefMapping::ExactLogical => filter,
        }
    }

    /// Reauthorize the exact label after awaits. No selector error adopts the default store.
    /// The native-write caller must additionally recheck ServiceAuthority and each canonical
    /// object in its captured MutationCheck after the native lock, before mutations/commit.
    pub async fn fresh(&self, ctx: &Ctx, scopes: &[&str]) -> Result<Access, ApiError> {
        self.fresh_headers(&ctx.state, &ctx.headers, scopes).await
    }

    pub(crate) async fn fresh_headers(
        &self,
        state: &AppState,
        headers: &axum::http::HeaderMap,
        scopes: &[&str],
    ) -> Result<Access, ApiError> {
        if let Some(runtime) = self.runtime_graph.get() {
            let (_, caller) = runtime.fresh(self, state, headers, scopes).await?;
            return Ok(caller);
        }
        self.fresh_headers_inner(state, headers, scopes, None).await
    }

    pub(super) async fn fresh_headers_inner(
        &self,
        state: &AppState,
        headers: &axum::http::HeaderMap,
        scopes: &[&str],
        graph: Option<&crate::executor::pipe_operation::CallbackBinding>,
    ) -> Result<Access, ApiError> {
        let service = if let Some(binding) = &self.service {
            binding.current(state)?;
            let authority = self
                .authority
                .get_or_try_init(|| super::service::ServiceAuthority::capture(state, binding))
                .await?;
            Some((authority, authority.fresh(state, scopes).await?))
        } else {
            None
        };
        let current = access::require_fresh(state, headers, scopes).await?;
        if self.is_native_alias()
            && matches!(&current.auth, Some(crate::auth::Principal::Execution(identity)) if identity.grant.required.contains("pipes:write")
                && graph.is_none_or(|binding|binding.grant!=identity.grant.id || binding.launch!=identity.grant.launch_id
                    || identity.grant.graph_origin.is_none() || identity.grant.graph_pending))
        {
            return Err(ApiError::new(
                501,
                "A tracked logical API pipe operation requires native graph-admission mapping.",
            ));
        }
        access::guard::check_connector_teams(state, &current, &self.logical)?;
        if let Some(binding) = &self.service {
            binding.current(state)?;
        }
        if let Some((authority, service)) = service {
            authority.finish(state, &service)?;
            *self
                .current_service
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(service);
        }
        super::authority::finish_original(state, headers, &current)?;
        Ok(current)
    }

    /// Metadata projection was computed with these two principals. Refuse a changed
    /// principal after its last metadata await instead of returning that earlier projection.
    pub(crate) async fn finish_read_headers(
        &self,
        state: &AppState,
        headers: &axum::http::HeaderMap,
        before: &Access,
        scopes: &[&str],
    ) -> Result<(), ApiError> {
        let service = self.service_access();
        let after = self.fresh_headers(state, headers, scopes).await?;
        if before.principal != after.principal
            || before.primary_team != after.primary_team
            || service.as_ref().map(|access| &access.principal)
                != self
                    .service_access()
                    .as_ref()
                    .map(|access| &access.principal)
        {
            return Err(ApiError::new(
                409,
                "The selected engine read authority changed during projection; retry.",
            ));
        }
        Ok(())
    }
}
