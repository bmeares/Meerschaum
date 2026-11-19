//! Native plugin HTTP route adapters and concurrency call limits.

use super::*;

// ---------------------------------------------------------------------------------------------
// Server extensions: native routes (`@api_plugin` equivalent) and console pages (`@web_page`).
// ---------------------------------------------------------------------------------------------

/// The `host_scopes` of each declared route, by plugin and route name.
pub(super) type RouteHostScopes = std::collections::HashMap<(String, String), Vec<String>>;

/// The `host_scopes` a route or page declares, without `*` (each one is a notice).
pub(super) fn host_scopes_of(
    declared: &serde_json::Value,
    what: &str,
    notices: &mut Vec<String>,
) -> Vec<String> {
    let scopes: Vec<String> = declared
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|s| s.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    if scopes.iter().any(|s| s == "*") {
        notices.push(format!(
            "{what}: `host_scopes` may not contain '*' (ignored); name the scopes instead."
        ));
    }
    scopes.into_iter().filter(|s| s != "*").collect()
}

/// The routes the native plugins declare: their manifest's `api_routes` (served by the
/// `api_route` call kind), or, for plugins written before those existed, the routes a plugin
/// lists from its `routes` method when its manifest names an `api_plugins` entry (served by the
/// `route` call kind with the original text-body context).
pub(super) fn declared_routes(
    natives: &NativePlugins,
    notices: &mut Vec<String>,
) -> (
    Vec<meerschaum_server::native::NativeRoute>,
    std::collections::HashSet<String>,
    RouteHostScopes,
) {
    use meerschaum_server::native::NativeRoute;
    let mut routes = Vec::new();
    let mut legacy = std::collections::HashSet::new();
    let mut host_scopes = RouteHostScopes::new();
    for name in natives.names() {
        let entry = natives.plugin_entry(&name);
        match meerschaum_plugin::api::routes_of(entry) {
            Ok(specs) if !specs.is_empty() => {
                for spec in specs {
                    let granted = host_scopes_of(
                        &serde_json::json!(spec.host_scopes),
                        &format!("Native plugin '{name}' route '{}'", spec.name),
                        notices,
                    );
                    if !granted.is_empty() {
                        host_scopes.insert((name.clone(), spec.name.clone()), granted);
                    }
                    routes.push(NativeRoute {
                        plugin: name.clone(),
                        name: spec.name,
                        method: spec.method,
                        path: spec.path,
                        scopes: spec.scopes,
                        public: spec.public,
                    });
                }
                continue;
            }
            Ok(_) => {}
            Err(error) => {
                notices.push(format!(
                    "Native plugin '{name}' routes are disabled: invalid `api_routes`: {error}"
                ));
                continue;
            }
        }
        if !entry["api_plugins"]
            .as_array()
            .is_some_and(|a| !a.is_empty())
        {
            continue;
        }
        let listed = natives
            .plugin(&name)
            .and_then(|plugin| plugin.call_json("routes", serde_json::json!({})));
        let listed = match listed {
            Ok(listed) => listed,
            Err(error) => {
                notices.push(format!(
                    "Native plugin '{name}' routes are disabled: {error}"
                ));
                continue;
            }
        };
        legacy.insert(name.clone());
        for entry in listed.as_array().cloned().unwrap_or_default() {
            let path = entry["path"].as_str().unwrap_or_default().to_string();
            let method = entry["method"].as_str().unwrap_or("GET").to_string();
            routes.push(NativeRoute {
                plugin: name.clone(),
                name: format!("{method} {path}"),
                method,
                // The original routes had no path parameters.
                public: entry["public"].as_bool().unwrap_or(false),
                scopes: Vec::new(),
                path,
            });
        }
    }
    (routes, legacy, host_scopes)
}

/// The router serving the routes native plugins declare (see `declared_routes` and
/// `meerschaum_server::native`). Refused routes (a core route they would shadow, a conflict,
/// bad syntax) are skipped; each is one notice.
pub fn routes(
    state: &meerschaum_server::AppState,
    natives: &Arc<NativePlugins>,
    env: &HostEnv,
) -> (axum::Router, Vec<String>) {
    let mut notices = Vec::new();
    let (routes, legacy, host_scopes) = declared_routes(natives, &mut notices);
    if routes.is_empty() {
        return (axum::Router::new(), notices);
    }
    let handler = Arc::new(RouteHandler {
        natives: natives.clone(),
        legacy,
        host_scopes,
        limits: CallLimits::default(),
        env: env.clone(),
        state: state.clone(),
    });
    let (router, refused) = meerschaum_server::native::native_router(state, routes, handler);
    notices.extend(refused);
    (router, notices)
}

pub(super) struct RouteHandler {
    pub(super) natives: Arc<NativePlugins>,
    /// Plugins served through the original `route` call kind.
    pub(super) legacy: std::collections::HashSet<String>,
    /// What each route's host calls may also do for every caller.
    pub(super) host_scopes: RouteHostScopes,
    pub(super) limits: CallLimits,
    /// The handlers' host calls, made for the caller.
    pub(super) env: HostEnv,
    pub(super) state: meerschaum_server::AppState,
}

/// The most server calls (`api_route`, `mcp_*`) one native plugin runs at once. They run in
/// parallel, each on a blocking thread; up to [`MAX_QUEUED_CALLS`] more wait up to
/// [`CALL_QUEUE_TIMEOUT`] for a slot and any beyond that are answered `503` at once, so a slow or
/// stuck handler holds at most this many threads and buffered route bodies (a route's body is read
/// only once it holds a slot) instead of piling them up.
pub(super) const MAX_CONCURRENT_CALLS: usize = 32;
/// How many server calls may wait for one of a plugin's [`MAX_CONCURRENT_CALLS`] slots.
pub(super) const MAX_QUEUED_CALLS: usize = 16;
/// How long a server call waits for one of its plugin's [`MAX_CONCURRENT_CALLS`] slots.
pub(super) const CALL_QUEUE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// One plugin's call slots and the number of calls waiting for one.
#[derive(Clone)]
pub(super) struct PluginSlots {
    pub(super) semaphore: Arc<tokio::sync::Semaphore>,
    pub(super) waiting: Arc<std::sync::atomic::AtomicUsize>,
}

/// Leaves a plugin's wait queue when dropped (including when the waiting request is cancelled).
pub(super) struct Waiting(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// One semaphore of [`MAX_CONCURRENT_CALLS`] slots per native plugin, with a wait queue of at most
/// [`MAX_QUEUED_CALLS`].
#[derive(Clone)]
pub(super) struct CallLimits {
    pub(super) plugins: Arc<std::sync::Mutex<std::collections::HashMap<String, PluginSlots>>>,
    pub(super) slots: usize,
    pub(super) queue: usize,
    pub(super) timeout: std::time::Duration,
}

impl Default for CallLimits {
    fn default() -> Self {
        Self::new(MAX_CONCURRENT_CALLS, MAX_QUEUED_CALLS, CALL_QUEUE_TIMEOUT)
    }
}

impl CallLimits {
    pub(super) fn new(slots: usize, queue: usize, timeout: std::time::Duration) -> Self {
        Self {
            plugins: Default::default(),
            slots,
            queue,
            timeout,
        }
    }

    /// A slot of `plugin`'s calls, or the message to answer when the queue is full or no slot
    /// frees up in time.
    pub(super) async fn acquire(
        &self,
        plugin: &str,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, String> {
        use std::sync::atomic::Ordering;
        let slots = self
            .plugins
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(plugin.to_string())
            .or_insert_with(|| PluginSlots {
                semaphore: Arc::new(tokio::sync::Semaphore::new(self.slots)),
                waiting: Default::default(),
            })
            .clone();
        let busy = || {
            format!(
                "Native plugin '{plugin}' is busy ({} calls in progress).",
                self.slots
            )
        };
        if let Ok(permit) = slots.semaphore.clone().try_acquire_owned() {
            return Ok(permit);
        }
        let queued = slots.waiting.fetch_add(1, Ordering::SeqCst);
        let _waiting = Waiting(slots.waiting.clone());
        if queued >= self.queue {
            return Err(busy());
        }
        match tokio::time::timeout(self.timeout, slots.semaphore.acquire_owned()).await {
            Ok(Ok(permit)) => Ok(permit),
            _ => Err(busy()),
        }
    }
}

pub(super) fn busy_response(message: String) -> meerschaum_server::native::NativeRouteResponse {
    let busy =
        meerschaum_plugin::api::RouteResponse::error(503, message).header("retry-after", "1");
    meerschaum_server::native::NativeRouteResponse {
        status: busy.status,
        headers: busy.headers,
        body: busy.body,
    }
}

/// The reply of an original `route` call, parsed as before `api_route` existed: an invalid or
/// missing `status` is `200`, `content_type` defaults to `text/plain; charset=utf-8` and `body`
/// is text.
pub(super) fn legacy_route_response(
    reply: &serde_json::Value,
) -> meerschaum_plugin::api::RouteResponse {
    let status = reply["status"]
        .as_u64()
        .and_then(|s| u16::try_from(s).ok())
        .and_then(|s| axum::http::StatusCode::from_u16(s).ok())
        .map_or(200, |s| s.as_u16());
    let content_type = reply["content_type"]
        .as_str()
        .unwrap_or("text/plain; charset=utf-8");
    meerschaum_plugin::api::RouteResponse::bytes(
        content_type,
        reply["body"]
            .as_str()
            .unwrap_or_default()
            .as_bytes()
            .to_vec(),
    )
    .with_status(status)
}

#[async_trait]
impl meerschaum_server::native::NativeRouteHandler for RouteHandler {
    async fn admit(
        &self,
        route: &meerschaum_server::native::NativeRoute,
    ) -> Result<
        meerschaum_server::native::NativeAdmission,
        meerschaum_server::native::NativeRouteResponse,
    > {
        self.limits
            .acquire(&route.plugin)
            .await
            .map(meerschaum_server::native::NativeAdmission::new)
            .map_err(busy_response)
    }

    async fn handle(
        &self,
        route: &meerschaum_server::native::NativeRoute,
        request: meerschaum_server::native::NativeRouteRequest,
    ) -> Result<meerschaum_server::native::NativeRouteResponse, String> {
        use meerschaum_plugin::api::RouteRequest;
        let natives = self.natives.clone();
        let plugin = route.plugin.clone();
        let legacy = self.legacy.contains(&route.plugin);
        let name = route.name.clone();
        let route_path = route.path.clone();
        // The body was read under this admission (see `admit`); a request that reaches `handle`
        // without one (a direct call) takes its slot now.
        let mut request = request;
        let admission = if request.admission.is_held() {
            std::mem::take(&mut request.admission)
        } else {
            match self.limits.acquire(&route.plugin).await {
                Ok(permit) => meerschaum_server::native::NativeAdmission::new(permit),
                Err(message) => return Ok(busy_response(message)),
            }
        };
        let host = PluginHost::api(
            self.env.clone(),
            self.state.clone(),
            request.scopes.clone(),
            request.caller.take(),
        )
        .with_host_scopes(
            self.host_scopes
                .get(&(route.plugin.clone(), route.name.clone()))
                .map_or(&[], Vec::as_slice),
        );
        let reply = tokio::task::spawn_blocking(move || -> meerschaum_plugin::Result<_> {
            // The slot is held until the plugin returns, even when the client has gone.
            let _admission = admission;
            let plugin = natives.plugin(&plugin)?;
            let host = Some(&host as &dyn meerschaum_plugin::HostServices);
            if legacy {
                let context = serde_json::json!({
                    "method": request.method,
                    "path": request.path,
                    "query": request.query,
                    "body": String::from_utf8_lossy(&request.body),
                    "user": request.principal.get("username").cloned().unwrap_or_default(),
                });
                // Serialized with the plugin's other calls, as the original adapter was.
                return Ok(legacy_route_response(
                    &plugin.call_json_with_host("route", context, host)?,
                ));
            }
            let principal = serde_json::from_value(request.principal).unwrap_or_default();
            plugin.api_route_with_host(
                &RouteRequest {
                    name,
                    method: request.method,
                    path: request.path,
                    route_path,
                    path_params: request.path_params,
                    query: request.query,
                    query_params: request.query_params,
                    headers: request.headers,
                    body: request.body,
                    principal,
                    scopes: request.scopes,
                },
                host,
            )
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        Ok(meerschaum_server::native::NativeRouteResponse {
            status: reply.status,
            headers: reply.headers,
            body: reply.body,
        })
    }
}
