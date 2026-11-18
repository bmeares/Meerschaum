//! Typed extension endpoints and the adapter for existing plugin API paths.

use crate::{
    AppState,
    error::ApiError,
    executor::{context, extensions, plugin_caller, plugin_handlers, plugins},
};
use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use std::ops::ControlFlow;

const INPUT_LIMIT: usize = 1024 * 1024;

pub fn mount(app: Router<AppState>) -> Router<AppState> {
    app.route("/plugins/extensions", get(index))
        .route("/plugins/extensions/invoke", post(invoke))
        .route("/plugins/extensions/policy", get(policy))
}

async fn source(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<plugin_caller::RequestSource, ApiError> {
    plugin_caller::RequestSource::resolve(state, headers, &["plugins:read"]).await
}

pub(crate) async fn catalog(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<plugins::Validated, ApiError> {
    let original = source(state, headers).await?;
    catalog_source(state, &original).await
}

/// Retain the original request ceiling through metadata discovery and later admission.
pub(crate) async fn catalog_source(
    state: &AppState,
    original: &plugin_caller::RequestSource,
) -> Result<plugins::Validated, ApiError> {
    let caller = original.refresh(state, &["plugins:read"]).await?;
    let catalog = context::scope(Some(caller), state.actions.extension_catalog())
        .await
        .map_err(ApiError::unavailable)?
        .ok_or_else(|| ApiError::new(503, "No confined extension source view is available."))?;
    let current = original.refresh(state, &["plugins:read"]).await?;
    confirm_catalog_source(&catalog, &current)?;
    Ok(catalog)
}

/// Project the exact attested source view after the final original-actor refresh. No I/O,
/// cache lookup or source import may follow this proof before publishing the catalog.
pub(crate) fn confirm_catalog_source(
    catalog: &plugins::Validated,
    current: &crate::access::Access,
) -> Result<(), ApiError> {
    use crate::executor::plugin_host::Placement;
    let refused = || ApiError::new(403, "The exact extension source grants were revoked.");
    current.require(&["plugins:read"], &[])?;
    let team_id = match &catalog.catalog().placement {
        Placement::Remote => Some(catalog.catalog().team_id),
        Placement::Host(host) => host.team_id,
    };
    let team = team_id
        .map(|id| {
            current
                .team_records
                .iter()
                .find(|team| {
                    team.id
                        .as_deref()
                        .and_then(|id| uuid::Uuid::parse_str(id).ok())
                        == Some(id)
                })
                .ok_or_else(refused)
        })
        .transpose()?;
    match (&catalog.catalog().placement, team) {
        (Placement::Remote, Some(team))
            if team.is_isolated()
                && team
                    .execution()
                    .is_ok_and(|settings| settings.mode.is_remote()) => {}
        (Placement::Host(_), Some(team))
            if team
                .execution()
                .is_ok_and(|settings| settings.mode == meerschaum_core::execution::Mode::Host) => {}
        (Placement::Host(host), None) if host.team_id.is_none() => {}
        _ => return Err(refused()),
    }
    let name = team.map(|team| team.name.as_str());
    let ceiling = crate::executor::source_ceiling::Ceiling::from_catalog(catalog.catalog())
        .map_err(|_| refused())?;
    if !ceiling.within_parent(current)
        || !ceiling.current(current, team)
        || !current.principal.permits_team(name)
        || current
            .principal
            .team_boundary
            .as_deref()
            .is_some_and(|boundary| !meerschaum_core::access::same_team(Some(boundary), name))
        || (team.is_some() && !current.full_admin() && !current.in_team(name))
    {
        return Err(refused());
    }
    Ok(())
}

/// Pure constructor-selected policy for the console's unknown page adapter. Missing
/// plugin permissions do not turn a confined caller into a trusted legacy caller.
async fn policy(State(state): State<AppState>, headers: HeaderMap) -> Result<Response, ApiError> {
    let confined = if state.config.no_auth {
        false
    } else {
        let original = plugin_caller::RequestSource::resolve(&state, &headers, &[]).await?;
        requires_confined(&state, &original.access())
    };
    let mut response = axum::Json(serde_json::json!({"confined":confined})).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    Ok(response)
}

/// The native page adapter must retain a verified console session through discovery.
/// This Rust-only selector cannot be supplied by an Invocation, header or environment.
pub async fn console_catalog(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<plugins::Validated, ApiError> {
    let original = source(state, headers).await?;
    if !original.console_session() {
        return Err(ApiError::invalid_credentials());
    }
    catalog_source(state, &original).await
}

/// Run one declared page/callback with the same verified console session, credential and
/// original scope ceiling before and after every awaited discovery/invocation step.
pub async fn execute_console(
    state: &AppState,
    headers: &HeaderMap,
    invocation: plugin_handlers::Invocation,
) -> Result<plugin_handlers::Reply, ApiError> {
    let original = source(state, headers).await?;
    if !original.console_session() {
        return Err(ApiError::invalid_credentials());
    }
    execute_source(state, &original, invocation).await
}

async fn index(State(state): State<AppState>, headers: HeaderMap) -> Result<Response, ApiError> {
    let catalog = catalog(&state, &headers).await?;
    let value = serde_json::to_value(catalog.catalog())
        .map_err(|_| ApiError::new(500, "Invalid extension catalog."))?;
    Ok(axum::Json(value).into_response())
}

async fn invoke(State(state): State<AppState>, request: Request) -> Result<Response, ApiError> {
    let (parts, body) = request.into_parts();
    let original = source(&state, &parts.headers).await?;
    let bytes = to_bytes(body, INPUT_LIMIT)
        .await
        .map_err(|_| ApiError::new(413, "The extension request exceeds its byte bound."))?;
    let invocation: plugin_handlers::Invocation = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::new(422, "Invalid typed extension invocation."))?;
    let reply = execute_source(&state, &original, invocation).await?;
    Ok(axum::Json(
        serde_json::to_value(reply).map_err(|_| ApiError::new(500, "Invalid extension reply."))?,
    )
    .into_response())
}

pub async fn execute(
    state: &AppState,
    headers: &HeaderMap,
    invocation: plugin_handlers::Invocation,
) -> Result<plugin_handlers::Reply, ApiError> {
    let original = source(state, headers).await?;
    execute_source(state, &original, invocation).await
}

/// Main-only adapter for a request already captured before body/catalog waits.
pub(crate) async fn execute_source(
    state: &AppState,
    original: &plugin_caller::RequestSource,
    invocation: plugin_handlers::Invocation,
) -> Result<plugin_handlers::Reply, ApiError> {
    let catalog = catalog_source(state, original).await?;
    let mut operation = extensions::Operation {
        caller: None,
        deadline: chrono::Utc::now().timestamp() + 60,
        catalog: catalog.catalog().clone(),
        invocation: invocation.clone(),
    };
    operation.validated()?;
    let required = operation.required()?;
    let scopes: Vec<_> = required.iter().map(String::as_str).collect();
    let caller = original.refresh(state, &scopes).await?;
    operation.caller = Some(plugin_caller::Caller::capture(
        &caller,
        original.console_session(),
    )?);
    let team = operation
        .team_id()
        .map(|id| {
            caller
                .team_records
                .iter()
                .find(|team| {
                    team.id
                        .as_deref()
                        .and_then(|id| uuid::Uuid::parse_str(id).ok())
                        == Some(id)
                })
                .ok_or_else(|| ApiError::new(403, "The extension source Team is unavailable."))
        })
        .transpose()?;
    operation.authorize_placement(&caller, team)?;
    let reply = context::extension_console_scope(
        original.console_session(),
        context::scope(Some(caller), state.actions.extension_invoke(invocation)),
    )
    .await
    .map_err(ApiError::unavailable)?;
    // A lost role, deleted user, expired JWT or narrowed parent during invocation cannot
    // publish a previously admitted result. The backend also checks its stored source.
    let caller = original.refresh(state, &scopes).await?;
    let team = operation
        .team_id()
        .map(|id| {
            caller
                .team_records
                .iter()
                .find(|team| {
                    team.id
                        .as_deref()
                        .and_then(|id| uuid::Uuid::parse_str(id).ok())
                        == Some(id)
                })
                .ok_or_else(|| ApiError::new(403, "The extension source Team is unavailable."))
        })
        .transpose()?;
    operation.authorize_placement(&caller, team)?;
    reply
        .validate(&operation.handler()?.selector)
        .map_err(|_| ApiError::new(502, "Invalid typed extension response."))?;
    Ok(reply)
}

/// Main's trusted tracked bridge selects this behavior; no request/property can opt into or
/// out of it. Narrowed admin tokens and admin-origin callbacks remain effective nonadmins.
pub fn requires_confined(state: &AppState, caller: &crate::access::Access) -> bool {
    !state.config.no_auth
        && !caller.no_auth
        && caller.auth.is_some()
        && !caller.full_admin()
        && (caller.principal.is_isolated()
            || matches!(
                state.actions.api_execution(),
                crate::backends::ApiExecution::Tracked { .. }
            ))
}
fn credential_attempt(state: &AppState, headers: &HeaderMap) -> bool {
    headers.contains_key(header::AUTHORIZATION)
        // Without a console provider, a browser's old session-cookie name cannot
        // authenticate here. Leave public legacy routing intact; the legacy handler
        // still removes that cookie before forwarding ordinary cookies to a plugin.
        || (state.engine_streams.browser_auth().is_some()
            && headers.get_all(header::COOKIE).iter().any(|value| {
            value.to_str().is_ok_and(|cookie| {
                cookie.split(';').any(|part| {
                    part.split_once('=')
                        .is_some_and(|(name, _)| name.trim() == "mrsm-session-id")
                })
            })
            }))
}

/// Preserve declared legacy fragment URLs without taking over Main's ordinary pages.
/// Only a verified confined caller selects this adapter; the destination is fixed
/// Core routing, never a URL returned by a source or a request body.
async fn page_fallback(state: &AppState, request: Request) -> ControlFlow<Response, Request> {
    let path = super::decoded_path(request.uri().path()).into_owned();
    let Some(page) = path.strip_prefix("/_pages/") else {
        return ControlFlow::Continue(request);
    };
    if state.config.no_auth {
        return ControlFlow::Continue(request);
    }
    let original = match plugin_caller::RequestSource::resolve(state, request.headers(), &[]).await
    {
        Ok(source) => source,
        Err(error) if credential_attempt(state, request.headers()) => {
            return ControlFlow::Break(error.into_response());
        }
        Err(_) => return ControlFlow::Continue(request),
    };
    if !requires_confined(state, &original.access()) {
        return ControlFlow::Continue(request);
    }
    if state.config.no_dash || !state.config.route_group_is_allowed("plugins") {
        return ControlFlow::Break(ApiError::not_found().into_response());
    }
    if !matches!(request.method().as_str(), "GET" | "HEAD") {
        return ControlFlow::Break(ApiError::method_not_allowed().into_response());
    }
    if page.len() > 512
        || !page.split('/').all(plugins::valid_name)
        || request
            .uri()
            .query()
            .is_some_and(|query| query.len() > 8192)
    {
        return ControlFlow::Break(ApiError::not_found().into_response());
    }
    let catalog = match catalog_source(state, &original).await {
        Ok(catalog) => catalog,
        Err(error) => return ControlFlow::Break(error.into_response()),
    };
    let count = catalog.catalog().plugins.values().filter_map(|source| {
        source.metadata.as_ref()?.registry.as_ref()
    }).flat_map(|registry| registry.handlers()).filter(|handler| {
        matches!(&handler.selector, plugin_handlers::Selector::Page{path,..} if path == page)
    }).count();
    if count != 1 {
        return ControlFlow::Break(ApiError::not_found().into_response());
    }
    // catalog_source just projected the same original source after its final refresh.
    // Keep redirect construction pure so no later wait invalidates that proof.
    let mut location = format!("/plugins/extensions/pages/{page}");
    if let Some(query) = request.uri().query() {
        location.push('?');
        location.push_str(query);
    }
    let Ok(location) = axum::http::HeaderValue::from_str(&location) else {
        return ControlFlow::Break(ApiError::new(422, "Invalid page request URI.").into_response());
    };
    let mut response = StatusCode::TEMPORARY_REDIRECT.into_response();
    response.headers_mut().insert(header::LOCATION, location);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    ControlFlow::Break(response)
}

/// Preserve exact declared API URLs in the caller's confined source view. Full administrators
/// and no-auth servers retain their deliberately trusted legacy handlers; a confined caller
/// never falls through to one after missing metadata, authorization or invocation failure.
pub async fn api_fallback(state: AppState, request: Request) -> ControlFlow<Response, Request> {
    let request = page_fallback(&state, request).await?;
    let path = super::decoded_path(request.uri().path());
    // Core console, assets and disabled route groups keep their own native guards.
    // Only plugin-owned API paths belong to this adapter.
    if crate::web::is_console_path(request.uri().path())
        || path == "/static"
        || path.starts_with("/static/")
        || super::route_group_of(request.uri().path()) != "other"
        || !super::is_proxyable(&state.config, request.uri().path())
    {
        return ControlFlow::Continue(request);
    }
    if state.config.no_auth {
        return ControlFlow::Continue(request);
    }
    let original = match plugin_caller::RequestSource::resolve(&state, request.headers(), &[]).await
    {
        Ok(source) => source,
        Err(error) if credential_attempt(&state, request.headers()) => {
            return ControlFlow::Break(error.into_response());
        }
        Err(_) => return ControlFlow::Continue(request),
    };
    if !requires_confined(&state, &original.access()) {
        return ControlFlow::Continue(request);
    }
    let catalog = match catalog_source(&state, &original).await {
        Ok(catalog) => catalog,
        Err(error) => return ControlFlow::Break(error.into_response()),
    };
    let path = request.uri().path();
    let head = request.method() == axum::http::Method::HEAD;
    let requested_method = request.method().as_str();
    let mut method = requested_method;
    let mut matching = Vec::new();
    // Match an explicitly declared HEAD first, then GET exactly as native routes do.
    // Every other method stays exact; normalization is shared by fallback/matched routes.
    for candidate in std::iter::once(requested_method).chain(head.then_some("GET")) {
        for (name, source) in &catalog.catalog().plugins {
            if let Some(registry) = source
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.registry.as_ref())
            {
                for handler in registry.handlers() {
                    let plugin_handlers::Selector::Api {
                        method: declared,
                        path: template,
                    } = &handler.selector
                    else {
                        continue;
                    };
                    let parameters = template
                        .chars()
                        .filter(|character| *character == '{')
                        .count();
                    if declared == candidate
                        && (if parameters == 0 {
                            template == path
                        } else {
                            crate::native::path_params(template, path).len() == parameters
                        })
                    {
                        matching.push((name.clone(), handler.id.clone()));
                    }
                }
            }
        }
        if !matching.is_empty() {
            method = candidate;
            break;
        }
    }
    let method = method.to_string();
    if matching.is_empty() {
        return ControlFlow::Break(ApiError::not_found().into_response());
    }
    if matching.len() != 1 {
        return ControlFlow::Break(
            ApiError::new(409, "Multiple admitted extensions declare this API path.")
                .into_response(),
        );
    }
    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, INPUT_LIMIT).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return ControlFlow::Break(
                ApiError::new(413, "The extension request exceeds its byte bound.").into_response(),
            );
        }
    };
    let body = match String::from_utf8(bytes.to_vec()) {
        Ok(body) => body,
        Err(_) => {
            return ControlFlow::Break(
                ApiError::new(422, "This extension API request must be UTF-8.").into_response(),
            );
        }
    };
    let mut headers = std::collections::BTreeMap::new();
    for name in ["accept", "content-type"] {
        if let Some(value) = parts
            .headers
            .get(name)
            .and_then(|value| value.to_str().ok())
        {
            headers.insert(name.into(), value.into());
        }
    }
    let (plugin, handler_id) = matching.remove(0);
    let invocation = plugin_handlers::Invocation {
        catalog_sha256: match catalog.source_digest() {
            Ok(digest) => digest,
            Err(_) => {
                return ControlFlow::Break(
                    ApiError::new(500, "Invalid extension source identity.").into_response(),
                );
            }
        },
        plugin,
        handler_id,
        request: plugin_handlers::Request::Api {
            method,
            path: parts.uri.path().into(),
            query: meerschaum_connector_api::wire::parse_query(
                parts.uri.query().unwrap_or_default(),
            ),
            headers,
            body,
        },
    };
    ControlFlow::Break(match execute_source(&state, &original, invocation).await {
        Ok(plugin_handlers::Reply::Api { status, body }) => {
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
            let mut response = if head {
                (status, axum::body::Body::empty()).into_response()
            } else {
                (status, body).into_response()
            };
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
            response.headers_mut().insert(
                header::X_CONTENT_TYPE_OPTIONS,
                axum::http::HeaderValue::from_static("nosniff"),
            );
            response
        }
        Ok(_) => ApiError::new(502, "The extension returned another handler kind.").into_response(),
        Err(error) => error.into_response(),
    })
}
