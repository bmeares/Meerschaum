//! Shared server state: the instance connector(s), the secret key, the backends, and the shared
//! pipes dictionary (`meerschaum/api/__init__.py`: `get_api_connector`, `pipes`, `get_pipe`).
//!
//! The pipe cache keeps the v4 semantics (see `CLAUDE.md`, "API Server Pipe Cache"):
//! `get_pipe(refresh=false)` returns the *shared* handle from the pipes dictionary, so cache
//! entries (columns types) persist across requests until `DELETE .../cache` invalidates them;
//! `get_pipe(refresh=true)` returns a throwaway handle that is never stored.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use indexmap::IndexMap;
use meerschaum_connector::{InstanceConnector, PipesKeysQuery};
use meerschaum_core::{Map, Pipe, Value};

use crate::auth;
use crate::backends::{
    ActionsBackend, EngineBackend, InstanceResolver, JobsBackend, MemoryEngine, NoActions,
    NoInstances, NoJobs,
};
use crate::config::{ServerConfig, fnmatch};
use crate::error::ApiError;
use crate::proxy::HostProxy;
use crate::ratelimit::RateLimiter;

/// A pipe as served by the API, with its per-object caches.
#[derive(Debug)]
pub struct PipeHandle {
    /// The pipe, with its parameters loaded from the instance.
    pub pipe: Pipe,
    created: Instant,
    columns_types: Mutex<Option<(Instant, IndexMap<String, String>)>>,
}

impl PipeHandle {
    fn new(pipe: Pipe) -> PipeHandle {
        PipeHandle {
            pipe,
            created: Instant::now(),
            columns_types: Mutex::new(None),
        }
    }

    /// The cached columns types, if fresher than `max_age_seconds`.
    pub fn cached_columns_types(&self, max_age_seconds: f64) -> Option<IndexMap<String, String>> {
        let guard = self.columns_types.lock().unwrap_or_else(|e| e.into_inner());
        guard
            .as_ref()
            .filter(|(at, _)| at.elapsed().as_secs_f64() < max_age_seconds)
            .map(|(_, t)| t.clone())
    }

    /// Cache the columns types.
    pub fn cache_columns_types(&self, types: IndexMap<String, String>) {
        *self.columns_types.lock().unwrap_or_else(|e| e.into_inner()) =
            Some((Instant::now(), types));
    }

    /// `pipe._invalidate_cache(hard=True)`.
    pub fn invalidate(&self) {
        *self.columns_types.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}

/// `{connector_keys: {metric_key: {location_key: Pipe}}}`.
pub type PipesDict = IndexMap<String, IndexMap<String, IndexMap<Option<String>, Arc<PipeHandle>>>>;

/// The server state (cheap to clone).
#[derive(Clone)]
pub struct AppState(Arc<Inner>);

/// The state behind [`AppState`].
pub struct Inner {
    /// The resolved configuration.
    pub config: Arc<ServerConfig>,
    instance: Arc<dyn InstanceConnector>,
    secret: Vec<u8>,
    /// `/actions` dispatch.
    pub actions: Arc<dyn ActionsBackend>,
    /// `/jobs/*`.
    pub jobs: Arc<dyn JobsBackend>,
    /// `/engine/*`.
    pub engine: Arc<dyn EngineBackend>,
    /// Extra instances.
    pub resolver: Arc<dyn InstanceResolver>,
    /// The extension-host proxy for unmatched routes.
    pub host: Option<Arc<HostProxy>>,
    /// Per-route-group rate limits.
    pub rate_limiter: RateLimiter,
    user_ids: Mutex<HashMap<String, Value>>,
    pipes: tokio::sync::Mutex<HashMap<String, Arc<PipesDict>>>,
    instances: Mutex<HashMap<String, Arc<dyn InstanceConnector>>>,
}

impl Deref for AppState {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        &self.0
    }
}

/// Builds an [`AppState`].
pub struct AppStateBuilder {
    config: ServerConfig,
    instance: Arc<dyn InstanceConnector>,
    secret: Option<Vec<u8>>,
    actions: Arc<dyn ActionsBackend>,
    jobs: Arc<dyn JobsBackend>,
    engine: Arc<dyn EngineBackend>,
    resolver: Arc<dyn InstanceResolver>,
    host: Option<Arc<HostProxy>>,
}

impl AppStateBuilder {
    /// Use this secret instead of the secret key file.
    pub fn secret(mut self, secret: Vec<u8>) -> Self {
        self.secret = Some(secret);
        self
    }
    /// The actions backend.
    pub fn actions(mut self, actions: Arc<dyn ActionsBackend>) -> Self {
        self.actions = actions;
        self
    }
    /// The jobs backend.
    pub fn jobs(mut self, jobs: Arc<dyn JobsBackend>) -> Self {
        self.jobs = jobs;
        self
    }
    /// The engine-state backend.
    pub fn engine(mut self, engine: Arc<dyn EngineBackend>) -> Self {
        self.engine = engine;
        self
    }
    /// The resolver for extra instances.
    pub fn resolver(mut self, resolver: Arc<dyn InstanceResolver>) -> Self {
        self.resolver = resolver;
        self
    }
    /// The extension-host proxy.
    pub fn host(mut self, host: Arc<HostProxy>) -> Self {
        self.host = Some(host);
        self
    }

    /// Build the state, reading (or creating) the secret key file unless a secret was given.
    pub fn build(self) -> std::io::Result<AppState> {
        let secret = match self.secret {
            Some(s) => s,
            None => auth::generate_secret_key(&self.config.secret_key_path)?,
        };
        let rate_limiter = RateLimiter::new(&self.config);
        Ok(AppState(Arc::new(Inner {
            config: Arc::new(self.config),
            instance: self.instance,
            secret,
            actions: self.actions,
            jobs: self.jobs,
            engine: self.engine,
            resolver: self.resolver,
            host: self.host,
            rate_limiter,
            user_ids: Mutex::new(HashMap::new()),
            pipes: tokio::sync::Mutex::new(HashMap::new()),
            instances: Mutex::new(HashMap::new()),
        })))
    }
}

/// Normalize a location key path segment (`'[None]'`, `'None'`, `'null'` mean `None`).
pub fn normalize_location(location_key: Option<&str>) -> Option<String> {
    match location_key {
        None | Some("[None]") | Some("None") | Some("null") => None,
        Some(other) => Some(other.to_string()),
    }
}

impl AppState {
    /// Start building a state for `instance`.
    pub fn builder(config: ServerConfig, instance: Arc<dyn InstanceConnector>) -> AppStateBuilder {
        AppStateBuilder {
            config,
            instance,
            secret: None,
            actions: Arc::new(NoActions),
            jobs: Arc::new(NoJobs),
            engine: Arc::new(MemoryEngine::default()),
            resolver: Arc::new(NoInstances),
            host: None,
        }
    }

    /// The server's own instance connector.
    pub fn instance(&self) -> Arc<dyn InstanceConnector> {
        self.0.instance.clone()
    }

    /// The JWT secret.
    pub fn secret(&self) -> &[u8] {
        &self.0.secret
    }

    /// The process-lifetime username → user ID cache of the user loader.
    pub fn cached_user_id(&self, username: &str) -> Option<Value> {
        self.user_ids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(username)
            .cloned()
    }

    /// Remember a user ID.
    pub fn cache_user_id(&self, username: &str, user_id: Value) {
        self.user_ids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(username.to_string(), user_id);
    }

    /// `get_api_connector(instance_keys)`.
    pub fn get_api_connector(
        &self,
        instance_keys: Option<&str>,
    ) -> Result<Arc<dyn InstanceConnector>, ApiError> {
        let default = self.config.instance_keys.as_str();
        let keys = instance_keys.unwrap_or(default);
        if !self.config.allow_multiple_instances && keys != default {
            return Err(ApiError::new(
                403,
                "This API instance does not allow for accessing additional instances.",
            ));
        }
        if !self
            .config
            .allowed_instance_keys
            .iter()
            .any(|p| fnmatch(keys, p))
        {
            return Err(ApiError::new(
                403,
                format!("Instance keys '{keys}' does not match the allowed instances patterns."),
            ));
        }
        if keys == default || keys == self.0.instance.connector_keys() {
            return Ok(self.instance());
        }
        let mut cache = self.instances.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(c) = cache.get(keys) {
            return Ok(c.clone());
        }
        let conn = self
            .resolver
            .resolve(keys)
            .ok_or_else(|| ApiError::new(422, "Invalid instance keys."))?;
        cache.insert(keys.to_string(), conn.clone());
        Ok(conn)
    }

    fn build_pipe(
        &self,
        conn: &Arc<dyn InstanceConnector>,
        connector_keys: &str,
        metric_key: &str,
        location_key: Option<&str>,
        parameters: Option<Map>,
    ) -> Result<Pipe, ApiError> {
        let mut b = Pipe::builder(connector_keys, metric_key)
            .location(location_key)
            .instance(&conn.connector_keys())
            .defaults(self.config.pipe_defaults.clone());
        if let Some(p) = parameters {
            b = b.parameters(p);
        }
        b.build()
            .map_err(|e| ApiError::exception("Failed to build a pipe", e))
    }

    /// `pipes(instance_keys, refresh)`: the shared pipes dictionary.
    pub async fn pipes(
        &self,
        instance_keys: Option<&str>,
        refresh: bool,
    ) -> Result<Arc<PipesDict>, ApiError> {
        let conn = self.get_api_connector(instance_keys)?;
        let key = conn.connector_keys();
        let mut guard = self.pipes.lock().await;
        if !refresh && let Some(d) = guard.get(&key) {
            return Ok(d.clone());
        }
        let keys = conn
            .fetch_pipes_keys(&PipesKeysQuery::default())
            .await
            .map_err(|e| ApiError::exception("Failed to fetch pipes' keys", e))?;
        let mut dict = PipesDict::new();
        for entry in keys.entries() {
            let lk = normalize_location(entry.location_key.as_deref());
            let params = match &entry.extra {
                Some(Value::Map(m)) => Some(m.clone()),
                Some(Value::Str(s)) => match Value::from_json_str(s) {
                    Ok(Value::Map(m)) => Some(m),
                    _ => None,
                },
                _ => None,
            };
            let mut pipe = self.build_pipe(
                &conn,
                &entry.connector_keys,
                &entry.metric_key,
                lk.as_deref(),
                params.clone(),
            )?;
            if params.is_none() {
                hydrate(&conn, &mut pipe).await?;
            }
            dict.entry(entry.connector_keys.clone())
                .or_default()
                .entry(entry.metric_key.clone())
                .or_default()
                .insert(lk, Arc::new(PipeHandle::new(pipe)));
        }
        let dict = Arc::new(dict);
        guard.insert(key, dict.clone());
        Ok(dict)
    }

    /// `get_pipe(connector_keys, metric_key, location_key, instance_keys, refresh)`.
    pub async fn get_pipe(
        &self,
        connector_keys: &str,
        metric_key: &str,
        location_key: Option<&str>,
        instance_keys: Option<&str>,
        refresh: bool,
    ) -> Result<Arc<PipeHandle>, ApiError> {
        let location_key = normalize_location(location_key);
        let conn = self.get_api_connector(instance_keys)?;
        if let Some((status, detail)) =
            meerschaum_connector_api::routes::refuse_mrsm_connector(connector_keys)
        {
            // PARITY: `api/__init__.py:270`.
            return Err(ApiError::new(status, detail));
        }
        let dict = self.pipes(instance_keys, false).await?;
        if !refresh
            && let Some(h) = dict
                .get(connector_keys)
                .and_then(|m| m.get(metric_key))
                .and_then(|l| l.get(&location_key))
            && h.created.elapsed().as_secs_f64() < self.config.attributes_cache_seconds
        {
            return Ok(h.clone());
        }
        let mut pipe = self.build_pipe(
            &conn,
            connector_keys,
            metric_key,
            location_key.as_deref(),
            None,
        )?;
        hydrate(&conn, &mut pipe).await?;
        Ok(Arc::new(PipeHandle::new(pipe)))
    }

    /// `is_pipe_registered(pipe, pipes(instance_keys, refresh=True))`.
    pub async fn is_registered(
        &self,
        pipe: &Pipe,
        instance_keys: Option<&str>,
    ) -> Result<bool, ApiError> {
        let dict = self.pipes(instance_keys, true).await?;
        let lk = pipe.location_key().map(String::from);
        Ok(dict
            .get(pipe.connector_keys())
            .and_then(|m| m.get(pipe.metric_key()))
            .is_some_and(|l| l.contains_key(&lk)))
    }
}

/// Load a pipe's parameters from the instance (`Pipe.attributes`: the instance's attributes
/// patched with the local ones).
pub async fn hydrate(conn: &Arc<dyn InstanceConnector>, pipe: &mut Pipe) -> Result<(), ApiError> {
    let attrs = conn
        .get_pipe_attributes(pipe)
        .await
        .map_err(|e| ApiError::exception("Failed to load a pipe's attributes", e))?;
    if let Some(params) = parameters_of(&attrs) {
        let merged = meerschaum_core::value::apply_patch_maps(&params, pipe.raw_parameters());
        pipe.set_parameters(merged);
    }
    Ok(())
}

/// The `parameters` of an attributes document (a dict, or a JSON string from some instances).
pub fn parameters_of(attrs: &Map) -> Option<Map> {
    match attrs.get("parameters")? {
        Value::Map(m) => Some(m.clone()),
        Value::Str(s) => match Value::from_json_str(s) {
            Ok(Value::Map(m)) => Some(m),
            _ => None,
        },
        _ => None,
    }
}

/// `pipe.attributes`: `apply_patch_to_config(instance_attributes, local_attributes)`.
pub async fn pipe_attributes(
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
) -> Result<Map, ApiError> {
    let instance_attrs = conn
        .get_pipe_attributes(pipe)
        .await
        .map_err(|e| ApiError::exception("Failed to load a pipe's attributes", e))?;
    let mut local = Map::new();
    local.insert(
        "connector_keys".into(),
        Value::Str(pipe.connector_keys().into()),
    );
    local.insert("metric_key".into(), Value::Str(pipe.metric_key().into()));
    local.insert(
        "location_key".into(),
        pipe.location_key().map(String::from).into(),
    );
    local.insert(
        "parameters".into(),
        Value::Map(pipe.raw_parameters().clone()),
    );
    Ok(meerschaum_core::value::apply_patch_maps(
        &instance_attrs,
        &local,
    ))
}
