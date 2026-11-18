//! Retain the original HTTP caller through native metadata and destructive-write waits.

use super::*;
use meerschaum_connector::mutation::{MutationCheck, scope};
use std::future::Future;

pub(crate) enum Operation {
    Register,
    Edit(Map),
    Delete,
    Drop,
    Clear,
    Deduplicate,
    Maintenance { schema: bool },
}

struct Authority {
    state: AppState,
    source: Source,
    conn: Arc<dyn InstanceConnector>,
    pipe: Pipe,
    original: Access,
    registration: Option<Value>,
    parameters: Option<Map>,
    operation: Operation,
}

enum Source {
    Headers(axum::http::HeaderMap),
    Callback {
        grant: uuid::Uuid,
        launch: uuid::Uuid,
        headers: Option<axum::http::HeaderMap>,
    },
}

impl Authority {
    async fn fresh(&self, permissions: &[&str]) -> Result<Access, ApiError> {
        match &self.source {
            Source::Headers(headers) => {
                access::require_fresh(&self.state, headers, permissions).await
            }
            Source::Callback {
                grant,
                launch,
                headers,
            } => {
                if let Some(headers) = headers {
                    access::require_fresh(&self.state, headers, permissions).await?;
                }
                let identity =
                    crate::executor::callbacks::load_identity(&self.state, *grant, *launch).await?;
                let caller = crate::executor::admission::registration_access(
                    &self.state,
                    &identity,
                    self.pipe.connector_keys(),
                    self.pipe.instance_keys(),
                )?;
                caller.require(permissions, &[])?;
                Ok(caller)
            }
        }
    }

    async fn check_current(&self) -> Result<(), ApiError> {
        let permissions = match self.operation {
            Operation::Register => &["pipes:register"][..],
            Operation::Edit(_) | Operation::Deduplicate | Operation::Maintenance { .. } => {
                PIPES_WRITE
            }
            Operation::Delete | Operation::Clear => PIPES_DELETE,
            Operation::Drop => PIPES_DROP,
        };
        // Bypass the request's cached JWT and role resolution on every native check.
        let caller = self.fresh(permissions).await?;
        let graph_source = match &self.source {
            Source::Headers(_) => caller.clone(),
            Source::Callback { grant, launch, .. } => {
                crate::executor::callbacks::load_identity(&self.state, *grant, *launch)
                    .await?
                    .caller()
            }
        };
        if self
            .state
            .actions
            .pipe_operation_read(&graph_source)
            .await
            .map_err(|reason| ApiError::new(409, reason))?
            .is_some()
        {
            return Err(ApiError::new(
                501,
                "This captured graph requires an exact native schema/maintenance instruction. Public metadata or destructive mutation cannot bypass its issued node window.",
            ));
        }
        access::guard::check_connector_teams(&self.state, &caller, self.pipe.instance_keys())?;
        let attributes = self
            .conn
            .get_pipe_attributes(&self.pipe)
            .await
            .map_err(|e| ApiError::exception("Failed to refresh pipe mutation authority", e))?;
        if attributes.get("pipe_id") != self.registration.as_ref()
            || crate::state::parameters_of(&attributes) != self.parameters
        {
            return Err(ApiError::new(
                409,
                "The pipe's registration or parameters changed during preparation; retry the request.",
            ));
        }
        let mut canonical = self.pipe.clone();
        match &self.parameters {
            None => {
                if !matches!(self.operation, Operation::Register) || !attributes.is_empty() {
                    return Err(ApiError::new(409, "The pipe registration is unavailable."));
                }
                if !self.state.config.registration_pipes {
                    return Err(ApiError::new(403, "Pipe registration is disabled."));
                }
                if caller.principal != self.original.principal
                    || caller.primary_team != self.original.primary_team
                {
                    return Err(ApiError::new(
                        409,
                        "Your pipe registration authority changed; retry the request.",
                    ));
                }
                access::guard::check_source_connector(
                    &self.state,
                    &caller,
                    canonical.connector_keys(),
                )?;
                access::guard::check_parameters(
                    &self.state,
                    &caller,
                    &self.conn,
                    &canonical,
                    None,
                    canonical.raw_parameters(),
                )
                .await?;
            }
            Some(parameters) => {
                if matches!(self.operation, Operation::Register) {
                    return Err(ApiError::new(409, "The pipe is already registered."));
                }
                canonical.set_parameters(parameters.clone());
                if !caller.can_see_pipe(&canonical) {
                    return Err(access::hidden_pipe(&canonical));
                }
                if !caller.can_edit(canonical.raw_parameters()) {
                    return Err(access::not_editable(&canonical));
                }
                match self.operation {
                    Operation::Edit(_) => {
                        let merged = access::guard::merge_edit(
                            &caller,
                            parameters,
                            self.pipe.raw_parameters().clone(),
                        )?;
                        if &merged != self.pipe.raw_parameters() {
                            return Err(ApiError::new(409, "Pipe edit authority changed."));
                        }
                        access::guard::check_parameters(
                            &self.state,
                            &caller,
                            &self.conn,
                            &canonical,
                            Some(parameters),
                            self.pipe.raw_parameters(),
                        )
                        .await?;
                        if let Some(error) = access::guard::parameters_lock_conflict(
                            &canonical,
                            parameters,
                            self.pipe.raw_parameters(),
                        ) {
                            return Err(error);
                        }
                    }
                    Operation::Delete
                    | Operation::Drop
                    | Operation::Clear
                    | Operation::Deduplicate => {
                        let lock = match self.operation {
                            Operation::Delete => LockOp::Delete,
                            Operation::Drop => LockOp::Drop,
                            Operation::Deduplicate => LockOp::Deduplicate,
                            _ => LockOp::Clear,
                        };
                        if let Some(error) = access::guard::lock_conflict(&canonical, lock) {
                            return Err(error);
                        }
                    }
                    Operation::Maintenance { schema: true } => {
                        if let Some(error) =
                            access::guard::lock_conflict(&canonical, LockOp::Schema)
                        {
                            return Err(error);
                        }
                    }
                    Operation::Maintenance { schema: false } => {}
                    Operation::Register => unreachable!(),
                }
            }
        }
        let dict = self
            .state
            .pipes(Some(self.pipe.instance_keys()), true)
            .await?;
        // The preceding metadata/template reads may await. Re-resolve the original source
        // again before checking the final target; the driver calls us after taking its lock.
        let caller = self.fresh(permissions).await?;
        access::guard::check_connector_teams(&self.state, &caller, self.pipe.instance_keys())?;
        if self.parameters.is_some() {
            if !caller.can_see_pipe(&canonical) {
                return Err(access::hidden_pipe(&canonical));
            }
            if !caller.can_edit(canonical.raw_parameters()) {
                return Err(access::not_editable(&canonical));
            }
            if let Operation::Edit(_) = &self.operation {
                let merged = access::guard::merge_edit(
                    &caller,
                    canonical.raw_parameters(),
                    self.pipe.raw_parameters().clone(),
                )?;
                if &merged != self.pipe.raw_parameters() {
                    return Err(ApiError::new(409, "Pipe edit authority changed."));
                }
            }
        } else if caller.principal != self.original.principal
            || caller.primary_team != self.original.primary_team
        {
            return Err(ApiError::new(409, "Pipe registration authority changed."));
        }
        access::guard::check_source_connector(&self.state, &caller, self.pipe.connector_keys())?;
        let index = access::index_from_dict(self.pipe.instance_keys(), &dict, self.conn.as_ref());
        access::guard::check_pipe_target(
            self.conn.as_ref(),
            &canonical,
            &index.hidden_targets(&caller),
        )?;
        access::guard::check_pipe_target(
            self.conn.as_ref(),
            &self.pipe,
            &index.hidden_targets(&caller),
        )?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl MutationCheck for Authority {
    async fn check(&self) -> Result<(), String> {
        self.check_current()
            .await
            .map_err(|error| error.detail_text())
    }
}

pub(super) async fn run<F: Future<Output = SuccessTuple>>(
    ctx: &Ctx,
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    caller: &Access,
    operation: Operation,
    future: F,
) -> Result<SuccessTuple, ApiError> {
    Box::pin(run_scoped(
        &ctx.state,
        conn,
        pipe,
        caller,
        operation,
        Source::Headers(ctx.headers.clone()),
        future,
    ))
    .await
}

/// Apply the same native mutation admission to an MCP HTTP request. The original
/// presented credential remains private and is revalidated after native lock waits.
pub(crate) async fn run_headers<F: Future<Output = SuccessTuple>>(
    state: &AppState,
    headers: axum::http::HeaderMap,
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    caller: &Access,
    operation: Operation,
    future: F,
) -> Result<SuccessTuple, ApiError> {
    Box::pin(run_scoped(
        state,
        conn,
        pipe,
        caller,
        operation,
        Source::Headers(headers),
        future,
    ))
    .await
}

/// MCP retains a private original-source proof across a confined registration hook and
/// native database waits. No callback bearer is minted or exposed by this helper.
pub(crate) async fn run_registration<F: Future<Output = SuccessTuple>>(
    state: &AppState,
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    original: &Access,
    grant: uuid::Uuid,
    launch: uuid::Uuid,
    future: F,
) -> Result<SuccessTuple, ApiError> {
    Box::pin(run_scoped(
        state,
        conn,
        pipe,
        original,
        Operation::Register,
        Source::Callback {
            grant,
            launch,
            headers: crate::executor::context::request_headers(),
        },
        future,
    ))
    .await
}

async fn run_scoped<F: Future<Output = SuccessTuple>>(
    state: &AppState,
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    caller: &Access,
    operation: Operation,
    source: Source,
    future: F,
) -> Result<SuccessTuple, ApiError> {
    let expected_id = pipe.registration_id();
    let attributes = conn
        .get_pipe_attributes(pipe)
        .await
        .map_err(|e| ApiError::exception("Failed to capture pipe mutation authority", e))?;
    let parameters = crate::state::parameters_of(&attributes);
    let expected_parameters = match &operation {
        Operation::Register => None,
        Operation::Edit(old) => Some(old),
        _ => Some(pipe.raw_parameters()),
    };
    if parameters.as_ref() != expected_parameters
        || expected_id.is_some_and(|expected| {
            attributes.get("pipe_id").map(Value::py_str).as_ref() != Some(&expected)
        })
    {
        return Err(ApiError::new(
            409,
            "The pipe changed during mutation preparation; retry the request.",
        ));
    }
    let authority = Arc::new(Authority {
        state: state.clone(),
        source,
        conn: conn.clone(),
        pipe: pipe.clone(),
        original: caller.clone(),
        registration: attributes.get("pipe_id").cloned(),
        parameters,
        operation,
    });
    authority.check_current().await?;
    Ok(scope(authority, future).await)
}
