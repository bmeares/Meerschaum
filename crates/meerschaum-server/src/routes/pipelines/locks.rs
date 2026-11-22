//! The metadata leases of Pipeline edits, and a Pipeline's access and locks.

use super::*;

async fn lease(ctx: Ctx, body: Body, release: bool) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    let request: meerschaum_connector_api::models::LeaseRequest =
        serde_json::from_slice(&body_bytes(body, ctx.state.config.limits.body).await?)
            .map_err(|e| ApiError::new(400, format!("Invalid lease request: {e}")))?;
    let parts: Vec<_> = request.pipe_ref.splitn(4, '|').collect();
    if parts.len() != 4
        || parts[1] != "mrsm"
        || !["pipeline", "pipeline_registry"].contains(&parts[2])
    {
        return Err(ApiError::new(
            400,
            "Only Pipeline metadata leases are accepted.",
        ));
    }
    let conn = connector(&ctx)?;
    let location = if parts[3] == "None" {
        None
    } else {
        Some(parts[3])
    };
    if let Some(name) = location {
        validate_name(name)?;
        // The lease of a Pipeline's edit: a Pipeline the caller may change.
        if parts[2] == "pipeline"
            && let Some(parameters) = registration_parameters(&ctx.state, &conn, name).await?
            && !caller.can_edit(&parameters)
        {
            return Err(ApiError::new(
                if caller.can_see(&parameters) {
                    403
                } else {
                    404
                },
                format!("You cannot change Pipeline '{name}'."),
            ));
        }
    }
    let pipe = Pipe::builder("mrsm", parts[2])
        .location(location)
        .instance(&conn.connector_keys())
        .build()
        .map_err(|e| ApiError::new(400, e.to_string()))?;
    let success = if release {
        conn.release_pipe_lease(&pipe, &request.holder).await
    } else {
        conn.acquire_pipe_lease(
            &pipe,
            &request.holder,
            Duration::from_millis(request.ttl_ms),
        )
        .await
    }
    .map_err(|e| failed("Pipeline metadata lease", e))?;
    match (success, release) {
        (true, false) => Ok(envelope::ok(
            format!("Acquired the lease on '{}'.", request.pipe_ref),
            JsonMap::new(),
        )),
        (true, true) => Ok(envelope::ok(
            format!("Released the lease on '{}'.", request.pipe_ref),
            JsonMap::new(),
        )),
        (false, false) => Err(ApiError::new(
            409,
            format!(
                "Could not acquire the lease on '{}': another process holds it.",
                request.pipe_ref
            ),
        )),
        (false, true) => Err(ApiError::new(
            409,
            format!(
                "Could not release the lease on '{}': '{}' does not hold it.",
                request.pipe_ref, request.holder
            ),
        )),
    }
}
pub(super) async fn acquire_lease(ctx: Ctx, body: Body) -> ApiResult {
    lease(ctx, body, false).await
}
pub(super) async fn release_lease(ctx: Ctx, body: Body) -> ApiResult {
    lease(ctx, body, true).await
}

/// The registration pipe of the Pipeline `name` that `caller` may see, with its parameters.
async fn visible_pipeline_pipe(
    ctx: &Ctx,
    caller: &Access,
    name: &str,
) -> Result<(Arc<dyn InstanceConnector>, Pipe, Map), ApiError> {
    let conn = connector(ctx)?;
    let (_, parameters) = visible_registration(&ctx.state, caller, &conn, name).await?;
    let mut pipe = registration_pipe(&ctx.state, &conn, name)?;
    pipe.set_parameters(parameters.clone());
    Ok((conn, pipe, parameters))
}

pub(super) async fn get_access(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let (_, _, parameters) = visible_pipeline_pipe(&ctx, &caller, &name).await?;
    Ok(envelope::ok(
        format!("The access of Pipeline '{name}'."),
        super::pipe_access::access_fields(&caller, &parameters),
    ))
}

pub(super) async fn patch_access(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    let body = optional_body(body, ctx.state.config.limits.body).await?;
    let body = if body.is_null() { json!({}) } else { body };
    let (conn, pipe, parameters) = visible_pipeline_pipe(&ctx, &caller, &name).await?;
    let new = super::pipe_access::changed_access(&caller, &parameters, &body)?;
    let res = conn.set_pipe_access(&pipe, &new).await;
    super::pipe_access::forget(&ctx.state, &conn).await;
    if !res.success {
        return Err(super::pipe_access::change_failed(&res));
    }
    let parameters = super::pipe_access::registered(&conn, &pipe).await?;
    Ok(envelope::ok(
        res.message,
        super::pipe_access::access_fields(&caller, &parameters),
    ))
}

async fn lock_or_unlock(ctx: Ctx, name: String, body: Body, lock: bool) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    let body = optional_body(body, ctx.state.config.limits.body).await?;
    let requested = super::pipe_access::requested_locks(&body)?;
    let (conn, pipe, parameters) = visible_pipeline_pipe(&ctx, &caller, &name).await?;
    super::pipe_access::check_may_lock(&caller, &pipe, &parameters, lock)?;
    let res = if lock {
        conn.lock_pipe(&pipe, &requested).await
    } else {
        conn.unlock_pipe(&pipe, &requested).await
    };
    super::pipe_access::forget(&ctx.state, &conn).await;
    if !res.success {
        return Err(super::pipe_access::change_failed(&res));
    }
    let parameters = super::pipe_access::registered(&conn, &pipe).await?;
    Ok(envelope::ok(
        res.message,
        super::pipe_access::access_fields(&caller, &parameters),
    ))
}

pub(super) async fn lock(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    lock_or_unlock(ctx, name, body, true).await
}

pub(super) async fn unlock(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    lock_or_unlock(ctx, name, body, false).await
}
