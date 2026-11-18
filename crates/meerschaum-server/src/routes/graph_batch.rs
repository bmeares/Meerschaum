//! Main-only delivery bookkeeping; peers cannot report native COMMIT or source EOF.

use super::{Ctx, body_bytes};
use crate::{
    AppState, ServerConfig,
    access::Access,
    auth::Principal,
    envelope,
    error::{ApiError, ApiResult},
    executor::{pipe_operation::CallbackBinding, plugin_caller::RequestSource},
};
use axum::{Router, body::Body, routing::post};
use meerschaum_connector_api::graph_batch::{
    BatchReply, BatchRequest, GRAPH_BATCH_PATH, GraphBinding, MAX_BATCH_REQUEST,
};

pub fn routes(app: Router<AppState>, _: &ServerConfig) -> Router<AppState> {
    app.route(GRAPH_BATCH_PATH, post(batch))
}

fn known_no_graph(caller: &Access) -> bool {
    matches!(caller.auth.as_ref(), Some(Principal::Execution(identity))
        if identity.grant.graph_tracking == 1
            && !identity.grant.graph_pending
            && identity.grant.graph_origin.is_none())
}

fn same_binding(wire: &GraphBinding, native: &CallbackBinding) -> bool {
    wire.grant == native.grant
        && wire.launch == native.launch
        && wire.revision == native.context_revision
}

fn confirm_reply(
    reply: &BatchReply,
    request: &BatchRequest,
    binding: &CallbackBinding,
    issuer: &CallbackBinding,
) -> Result<(), ApiError> {
    reply
        .validate(request)
        .map_err(|_| ApiError::unavailable("Invalid canonical native batch response."))?;
    if !same_binding(&reply.consumer, binding) || !same_binding(&reply.issuer, issuer) {
        return Err(ApiError::unavailable(
            "The native batch belongs to a different canonical execution.",
        ));
    }
    Ok(())
}

async fn batch(ctx: Ctx, body: Body) -> ApiResult {
    let original = RequestSource::resolve(&ctx.state, &ctx.headers, &[]).await?;
    let initial = original.access();
    if !matches!(initial.auth.as_ref(), Some(Principal::Execution(_))) {
        return Err(ApiError::new(
            403,
            "Native batch delivery requires the originating execution callback.",
        ));
    }
    let request: BatchRequest =
        serde_json::from_slice(&body_bytes(body, Some(MAX_BATCH_REQUEST)).await?)
            .map_err(|_| ApiError::new(400, "Invalid native batch request."))?;
    request
        .validate()
        .map_err(|_| ApiError::new(400, "Invalid native batch selector."))?;
    let caller = original.refresh(&ctx.state, &[]).await?;
    let operation = ctx
        .state
        .actions
        .pipe_operation_read(&caller)
        .await
        .map_err(ApiError::unavailable)?;
    let admitted = original.refresh(&ctx.state, &[]).await?;
    let reply = if let Some(mut operation) = operation {
        operation.confirm_request(&ctx.state, &initial, &admitted)?;
        // Derive the selected writable member/window on Main before asking its
        // durable provider. Requests supply no issuer, registration, range or counts.
        operation.native_batch_context(request.reference())?;
        let reply = ctx
            .state
            .actions
            .graph_batch(&operation.caller, &request)
            .await
            .map_err(ApiError::unavailable)?
            .ok_or_else(|| {
                ApiError::unavailable("The captured graph has no native batch provider.")
            })?;
        let current = original.refresh(&ctx.state, &[]).await?;
        // No awaits after the last refresh of the SAME original source/ceiling.
        operation.confirm_request(&ctx.state, &initial, &current)?;
        confirm_reply(&reply, &request, &operation.binding, &operation.issuer)?;
        Some(reply)
    } else {
        if !known_no_graph(&initial) || !known_no_graph(&admitted) {
            return Err(ApiError::unavailable(
                "A missing batch provider cannot declassify this captured or unknown graph.",
            ));
        }
        None
    };
    Ok(envelope::ok(
        "Main native batch delivery state.",
        serde_json::json!({"batch":reply})
            .as_object()
            .unwrap()
            .clone(),
    ))
}

/// Original execution and wire selector, kept on Main through body/metadata waits.
pub(super) struct WriteRequest {
    original: RequestSource,
    selector: meerschaum_connector_api::graph_batch::BatchSelector,
}

const WRITE_SCOPES: &[&str] = &["pipes:write"];

impl WriteRequest {
    pub(super) async fn capture(ctx: &Ctx) -> Result<Option<Self>, ApiError> {
        use meerschaum_connector_api::graph_batch::{BATCH_HEADER, BatchSelector};
        let mut headers = ctx.headers.get_all(BATCH_HEADER).iter();
        let Some(header) = headers.next() else {
            return Ok(None);
        };
        if headers.next().is_some() || header.as_bytes().len() > MAX_BATCH_REQUEST {
            return Err(ApiError::new(400, "Invalid native batch selector header."));
        }
        let selector: BatchSelector = serde_json::from_slice(header.as_bytes())
            .map_err(|_| ApiError::new(400, "Invalid native batch selector header."))?;
        selector
            .validate()
            .map_err(|_| ApiError::new(400, "Invalid native batch selector."))?;
        let original = RequestSource::resolve(&ctx.state, &ctx.headers, WRITE_SCOPES).await?;
        if !matches!(
            original.access().auth.as_ref(),
            Some(Principal::Execution(_))
        ) {
            return Err(ApiError::new(
                403,
                "A native batch belongs to its originating execution.",
            ));
        }
        Ok(Some(Self { original, selector }))
    }

    async fn operation(
        &self,
        state: &AppState,
    ) -> Result<crate::executor::pipe_operation::ReadAccess, ApiError> {
        let caller = self.original.refresh(state, WRITE_SCOPES).await?;
        let mut operation = state
            .actions
            .pipe_operation_read(&caller)
            .await
            .map_err(ApiError::unavailable)?
            .ok_or_else(|| ApiError::new(409, "The captured write operation is unavailable."))?;
        let fresh = self.original.refresh(state, WRITE_SCOPES).await?;
        operation.confirm_request(state, &self.original.access(), &fresh)?;
        Ok(operation)
    }

    pub(super) async fn claim(
        self,
        ctx: &Ctx,
        reference: &str,
        kind: &str,
        raw: &[u8],
    ) -> Result<std::sync::Arc<WriteBatch>, ApiError> {
        use meerschaum_connector::mutation::{BatchClaimOutcome, native_request_digest};
        let digest = native_request_digest(reference, kind, ctx.query.pairs(), raw)
            .map_err(|reason| ApiError::new(400, reason))?;
        let mut operation = self.operation(&ctx.state).await?;
        let expected = operation.native_batch_context(reference)?;
        let outcome = ctx
            .state
            .actions
            .claim_pipe_batch(&operation.caller, reference, &self.selector, &digest)
            .await
            .map_err(|reason| ApiError::new(409, reason))?;
        let fresh = self.original.refresh(&ctx.state, WRITE_SCOPES).await?;
        operation.confirm_request(&ctx.state, &self.original.access(), &fresh)?;
        let native = match outcome {
            BatchClaimOutcome::Native(native)
                if native.reference() == reference && native.request_sha256() == digest =>
            {
                native
            }
            BatchClaimOutcome::Native(_) => {
                return Err(ApiError::unavailable("Invalid native batch claim."));
            }
            BatchClaimOutcome::AlreadyCommitted(_) => {
                return Err(ApiError::new(
                    409,
                    "The original batch committed. Its result must be reconciled without repeating the write.",
                ));
            }
            BatchClaimOutcome::Unknown => {
                return Err(ApiError::new(409, "The original batch is unresolved."));
            }
        };
        Ok(std::sync::Arc::new(WriteBatch {
            request: self,
            native,
            expected,
        }))
    }
}

/// The typed claim comes exclusively from the canonical Main journal.
pub(super) struct WriteBatch {
    request: WriteRequest,
    pub(super) native: meerschaum_connector::mutation::NativeBatch,
    expected: crate::executor::pipe_operation::commits::batches::Context,
}

impl WriteBatch {
    async fn current(
        &self,
        state: &AppState,
    ) -> Result<crate::executor::pipe_operation::ReadAccess, ApiError> {
        let read = self.request.operation(state).await?;
        if read.native_batch_context(self.native.reference())? != self.expected {
            return Err(ApiError::new(409, "The canonical batch window changed."));
        }
        Ok(read)
    }

    pub(super) async fn confirm(&self, state: &AppState) -> Result<(), ApiError> {
        let mut read = self.current(state).await?;
        state
            .actions
            .confirm_pipe_batch(&read.caller, &self.native)
            .await
            .map_err(|reason| ApiError::new(409, reason))?;
        let fresh = self.request.original.refresh(state, WRITE_SCOPES).await?;
        read.confirm_request(state, &self.request.original.access(), &fresh)
    }

    pub(super) async fn receipt(
        &self,
        state: &AppState,
    ) -> Result<meerschaum_connector_api::graph_batch::BatchReceipt, ApiError> {
        let mut read = self.current(state).await?;
        let received = state
            .actions
            .accepted_pipe_batch(&read.caller, &self.native)
            .await
            .map_err(|reason| ApiError::new(409, reason))?;
        let fresh = self.request.original.refresh(state, WRITE_SCOPES).await?;
        read.confirm_request(state, &self.request.original.access(), &fresh)?;
        let selected = &self.request.selector;
        if received.stream() != selected.stream
            || received.ordinal() != selected.ordinal
            || received.batch() != selected.batch
        {
            return Err(ApiError::unavailable(
                "The accepted receipt belongs to another batch.",
            ));
        }
        Ok(meerschaum_connector_api::graph_batch::BatchReceipt {
            selector: selected.clone(),
            sha256: received.sha256().into(),
        })
    }
}
