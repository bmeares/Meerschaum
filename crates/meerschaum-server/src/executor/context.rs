//! The main API's authenticated request identity, available to injected execution backends.
//!
//! This context does not replace route authorization. Routes still require scopes and object
//! access before invoking their backends; backends use it to sign the accepted operation.

use super::control::Actor;
use crate::access::Access;
use std::sync::Arc;

tokio::task_local! {
    static CALLER: Option<Arc<Access>>;
    static PIPE_OPERATION: Option<Arc<super::pipe_operation::GraphOperation>>;
    static ACTION_REQUEST: bool;
    static STORED_REQUEST: Arc<StoredRequest>;
    static ACTION_SOURCE: Arc<super::plugin_caller::RequestSource>;
    static REGISTRATION_SOURCE: Option<String>;
    static CONNECTOR_REGISTRATION: Option<Arc<super::registration::Registration>>;
    static EXTENSION: Option<Arc<super::extensions::Operation>>;
    static EXTENSION_DISCOVERY: bool;
    static EXTENSION_CONSOLE_SESSION: bool;
    static REQUEST_HEADERS: axum::http::HeaderMap;
}

/// Set only by Main after its installed browser adapter verifies the same current user.
/// No public body, header or environment value selects this data projection.
pub fn extension_console_session() -> bool {
    EXTENSION_CONSOLE_SESSION
        .try_with(|value| *value)
        .unwrap_or(false)
}

pub async fn extension_console_scope<F: std::future::Future>(
    verified: bool,
    future: F,
) -> F::Output {
    EXTENSION_CONSOLE_SESSION.scope(verified, future).await
}

/// Original HTTP credential for native MCP mutation waits; never serialized into a job.
pub(crate) fn request_headers() -> Option<axum::http::HeaderMap> {
    REQUEST_HEADERS.try_with(Clone::clone).ok()
}

pub(crate) async fn request_headers_scope<F: std::future::Future>(
    headers: axum::http::HeaderMap,
    future: F,
) -> F::Output {
    REQUEST_HEADERS.scope(headers, future).await
}

pub fn extension_discovery() -> bool {
    EXTENSION_DISCOVERY
        .try_with(|value| *value)
        .unwrap_or(false)
}
pub async fn extension_discovery_scope<F: std::future::Future>(future: F) -> F::Output {
    EXTENSION_DISCOVERY.scope(true, future).await
}

/// Main-only admitted extension descriptor, never decoded from public job properties.
pub fn extension() -> Option<Arc<super::extensions::Operation>> {
    EXTENSION.try_with(Clone::clone).ok().flatten()
}

pub async fn extension_scope<F: std::future::Future>(
    operation: Arc<super::extensions::Operation>,
    future: F,
) -> F::Output {
    EXTENSION.scope(Some(operation), future).await
}

/// Main-only accepted fixed hook descriptor, outside public job properties.
pub fn connector_registration() -> Option<Arc<super::registration::Registration>> {
    CONNECTOR_REGISTRATION.try_with(Clone::clone).ok().flatten()
}

/// Preserve the exact descriptor only while Main reserves its canonical private job.
pub async fn connector_registration_scope<F: std::future::Future>(
    registration: Arc<super::registration::Registration>,
    future: F,
) -> F::Output {
    CONNECTOR_REGISTRATION
        .scope(Some(registration), future)
        .await
}

/// Exact Main-accepted source connector while preparing a typed registration hook.
pub fn registration_source() -> Option<String> {
    REGISTRATION_SOURCE.try_with(Clone::clone).ok().flatten()
}

/// Bind source-only discovery to the originating registration permission.
pub async fn registration_scope<F: std::future::Future>(source: String, future: F) -> F::Output {
    REGISTRATION_SOURCE.scope(Some(source), future).await
}

/// Main-owned action endpoint admission, independent of the command's data-only spelling.
pub fn action_request() -> bool {
    ACTION_REQUEST.try_with(|value| *value).unwrap_or(false)
}

/// Preserve the originating action scope when Main creates an internal tracked execution.
pub async fn action_request_scope<F: std::future::Future>(future: F) -> F::Output {
    ACTION_REQUEST.scope(true, future).await
}

/// Original HTTP context for one public stored-job request. Credentials and public properties
/// never enter the durable intent. Only graph admission opens the request's credential source.
pub(crate) enum JobRequestKind {
    Create { words: Vec<String>, plain: bool },
    Start,
}
pub(crate) struct StoredRequest {
    name: String,
    kind: JobRequestKind,
    headers: axum::http::HeaderMap,
    initial: Access,
    source: tokio::sync::OnceCell<super::plugin_caller::RequestSource>,
}

pub(crate) async fn stored_request_scope<F: std::future::Future>(
    name: &str,
    kind: JobRequestKind,
    headers: &axum::http::HeaderMap,
    initial: &Access,
    future: F,
) -> F::Output {
    STORED_REQUEST
        .scope(
            Arc::new(StoredRequest {
                name: name.into(),
                kind,
                headers: headers.clone(),
                initial: initial.clone(),
                source: Default::default(),
            }),
            future,
        )
        .await
}

fn stored_request(name: &str) -> Result<Arc<StoredRequest>, crate::error::ApiError> {
    STORED_REQUEST
        .try_with(Clone::clone)
        .ok()
        .filter(|request| request.name == name && !request.initial.no_auth)
        .ok_or_else(|| {
            crate::error::ApiError::new(
                403,
                "This graph requires its original stored-job HTTP request.",
            )
        })
}

/// A stored graph only accepts the exact argv originally submitted to the public job route.
/// Internal jobs, scheduled properties, and a legacy cleaner that changed argv have no capture.
pub fn stored_create_words(
    name: &str,
    words: &[String],
) -> Result<Vec<String>, crate::error::ApiError> {
    match &stored_request(name)?.kind {
        JobRequestKind::Create {
            words: original,
            plain: true,
        } if original == words => Ok(original.clone()),
        _ => Err(crate::error::ApiError::new(
            403,
            "This job's original arguments or properties require a separate graph capture.",
        )),
    }
}

pub async fn refresh_stored_request(
    state: &crate::AppState,
    name: &str,
    create: bool,
) -> Result<Arc<Access>, crate::error::ApiError> {
    let request = stored_request(name)?;
    let scopes: &[&str] = match (&request.kind, create) {
        (JobRequestKind::Create { .. }, true) => &["jobs:execute", "jobs:write", "pipes:write"],
        (JobRequestKind::Start, false) => &["jobs:execute"],
        _ => {
            return Err(crate::error::ApiError::new(
                403,
                "The graph launch crossed a different job request.",
            ));
        }
    };
    let source = request
        .source
        .get_or_try_init(|| {
            super::plugin_caller::RequestSource::resolve(state, &request.headers, scopes)
        })
        .await?;
    check_same_action_access(&request.initial, &source.access())?;
    let fresh = source.refresh(state, scopes).await?;
    check_same_action_access(&request.initial, &fresh)?;
    Ok(fresh)
}

/// Only the authenticated Main action route establishes the credential refresh handle.
pub(crate) async fn direct_action_scope<F: std::future::Future>(
    state: &crate::AppState,
    headers: &axum::http::HeaderMap,
    caller: &Access,
    future: F,
) -> Result<F::Output, crate::error::ApiError> {
    let source = super::plugin_caller::RequestSource::resolve(
        state,
        headers,
        &["actions:execute", "pipes:write"],
    )
    .await?;
    check_same_action_access(caller, &source.access())?;
    Ok(ACTION_SOURCE.scope(Arc::new(source), future).await)
}

pub(crate) fn check_same_action_access(
    original: &Access,
    current: &Access,
) -> Result<(), crate::error::ApiError> {
    super::plugin_caller::Caller::capture(original, false)?.matches(current)?;
    if original.principal != current.principal
        || original.primary_team != current.primary_team
        || original.team_records != current.team_records
        || original.credential != current.credential
        || original.attributes != current.attributes
        || original.no_auth
        || current.no_auth
    {
        return Err(crate::error::ApiError::new(
            403,
            "The original action actor, Team or scope ceiling changed.",
        ));
    }
    Ok(())
}

/// Refresh the very credential that submitted this action after catalog and object waits.
pub async fn refresh_direct_action(
    state: &crate::AppState,
    initial: &Access,
) -> Result<Arc<Access>, crate::error::ApiError> {
    let source = ACTION_SOURCE.try_with(Clone::clone).map_err(|_| {
        crate::error::ApiError::new(403, "The direct action lost its originating request.")
    })?;
    check_same_action_access(initial, &source.access())?;
    let fresh = source
        .refresh(state, &["actions:execute", "pipes:write"])
        .await?;
    check_same_action_access(initial, &fresh)?;
    Ok(fresh)
}

tokio::task_local! {
    static PIPELINE_REQUEST: Option<Arc<super::pipe_operation::PipelineRunRequest>>;
}

/// Only the public Pipeline sync route supplies a request, scoped to its exact reserved job.
pub fn pipeline_request(
    name: &str,
    words: &[String],
) -> Option<Arc<super::pipe_operation::PipelineRunRequest>> {
    PIPELINE_REQUEST
        .try_with(Clone::clone)
        .ok()
        .flatten()
        .filter(|request| request.matches(name, words))
}

pub(crate) async fn pipeline_request_scope<F: std::future::Future>(
    request: Option<Arc<super::pipe_operation::PipelineRunRequest>>,
    future: F,
) -> F::Output {
    PIPELINE_REQUEST.scope(request, future).await
}

/// Main-owned typed endpoint admission, never selected by a public job property.
pub fn pipe_operation() -> Option<Arc<super::pipe_operation::GraphOperation>> {
    PIPE_OPERATION.try_with(Clone::clone).ok().flatten()
}

/// Bind only the exact accepted built-in endpoint operation while reserving its private job.
pub async fn pipe_operation_scope<F: std::future::Future>(
    operation: Arc<super::pipe_operation::PipeOperation>,
    future: F,
) -> F::Output {
    PIPE_OPERATION
        .scope(Some(Arc::new((*operation).clone().into())), future)
        .await
}

/// Preserve Main's exact generic graph capture, never public job properties.
pub async fn graph_operation_scope<F: std::future::Future>(
    operation: Arc<super::pipe_operation::GraphOperation>,
    future: F,
) -> F::Output {
    PIPE_OPERATION.scope(Some(operation), future).await
}

/// Authenticated request identity, or no public caller for local maintenance.
pub fn current() -> Option<Arc<Access>> {
    CALLER.try_with(Clone::clone).ok().flatten()
}

/// Preserve the accepted caller across a request or an explicitly spawned operation.
pub async fn scope<F: std::future::Future>(caller: Option<Arc<Access>>, operation: F) -> F::Output {
    CALLER.scope(caller, operation).await
}

/// Sign only the effective permissions the main API resolved, including token narrowing.
pub fn actor(access: &Access) -> Actor {
    Actor {
        owner: access
            .username()
            .map(str::to_string)
            .or_else(|| crate::routes::execute::owner_label(access.auth.as_ref()))
            .unwrap_or_else(|| "server".into()),
        username: access.username().map(str::to_string),
        permissions: access.principal.permissions.clone(),
        administrator: access.full_admin(),
    }
}
