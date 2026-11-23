//! Register, edit and delete a pipe (`/register`, `/edit`, `/delete`).

use super::*;

/// `POST .../register`. Registering needs `pipes:register` (`pipes:write` still works for
/// credentials made before 5.0.0rc15). The new pipe's owner is the caller (an administrator may name
/// another) and its team defaults to the caller's primary team; its SQL definition passes the
/// SQL guard ([`crate::sql_guard`]); a connector reserved for other teams is refused.
pub(super) async fn register_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    let ik = ctx.instance_keys();
    let body = parse_json_body(&body_bytes(body, ctx.state.config.limits.body).await?)?;
    // Genuine legacy credentials are expanded during access resolution. Rechecking a raw
    // `pipes:write` alias here would restore registration that a modern role removed.
    let access = access::require_fresh(&ctx.state, &ctx.headers, &["pipes:register"]).await?;
    let parameters = optional_dict_body(body)?;
    if !ctx.state.config.registration_pipes {
        return Ok(tuple(
            false,
            "The administrator for this server has not allowed pipe registration.\n\n\
             Please contact the system administrator, or if you are running this server, \
             open the configuration file with `edit config api` and search for 'permissions'. \
             Under the keys `api:permissions:registration`, \
             you can toggle various registration types.",
        ));
    }
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    if ctx.state.is_registered(&h.pipe, ik.as_deref()).await? {
        return Err(ApiError::new(
            409,
            format!("{} already registered.", h.pipe),
        ));
    }
    access::guard::check_source_connector(&ctx.state, &access, &ck)?;
    let mut pipe = h.pipe.clone();
    let requested = parameters
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| pipe.raw_parameters().clone());
    let stamped = access::guard::stamp_new_pipe(&access, &requested)?;
    access::guard::check_parameters(&ctx.state, &access, &conn, &pipe, None, &stamped).await?;
    if !stamped.is_empty() {
        pipe.set_parameters(stamped);
    }
    let dict = ctx.state.pipes(ik.as_deref(), true).await?;
    let fresh = access::require_fresh(&ctx.state, &ctx.headers, &["pipes:register"]).await?;
    if fresh.principal != access.principal || fresh.primary_team != access.primary_team {
        return Err(ApiError::new(
            409,
            "Your pipe registration permissions changed during preparation; retry the request.",
        ));
    }
    access::guard::check_source_connector(&ctx.state, &fresh, &ck)?;
    let index = access::index_from_dict(&conn.connector_keys(), &dict, conn.as_ref());
    access::guard::check_pipe_target(
        conn.as_ref(),
        &pipe,
        &index.hidden_targets(&ctx.state, &fresh),
    )?;
    let res = mutation::run(
        &ctx,
        &conn,
        &pipe,
        &fresh,
        mutation::Operation::Register,
        conn.register_pipe(&pipe),
    )
    .await?;
    ctx.state.pipes(ik.as_deref(), true).await?;
    forget_access(&ctx);
    Ok(tuple_response(&res))
}

/// `PATCH .../edit`: a pipe the caller may change. `access` and `locks` the body leaves out keep
/// their stored values; changing them needs the rights to (the owner, the team's managers,
/// administrators); a SQL definition passes the SQL guard.
pub(super) async fn edit_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    let ik = ctx.instance_keys();
    let body = parse_json_body(&body_bytes(body, ctx.state.config.limits.body).await?)?;
    let access = authorize(&ctx, PIPES_WRITE).await?;
    let parameters = match body {
        None => return Err(ApiError::missing(&["body"])),
        Some(Json::Object(o)) => to_core(&Json::Object(o)).to_map(),
        Some(other) => {
            return Err(ApiError::validation(vec![ApiError::field_error(
                "dict_type",
                &["body"],
                "Input should be a valid dictionary",
                other,
                None,
            )]));
        }
    };
    let patch = ctx.query.bool("patch", false)?;
    if !ctx.state.config.actions_non_admin {
        return Ok(tuple(
            false,
            "The administrator for this server has not allowed actions.\n\n\
             Please contact the system administrator, or if you are running this server, \
             open the configuration file with `edit config api` and search for 'permissions'. \
             Under the keys `api:permissions:actions`, \
             you can toggle non-admin actions.",
        ));
    }
    let (conn, h) = editable(&ctx, &access, &ck, &mk, &lk, true).await?;
    if !ctx.state.is_registered(&h.pipe, ik.as_deref()).await? {
        return Err(ApiError::new(409, format!("{} is not registered.", h.pipe)));
    }
    let old = h.pipe.raw_parameters().clone();
    let requested = if patch {
        meerschaum_core::value::apply_patch_maps(&old, &parameters)
    } else {
        parameters
    };
    let new = access::guard::merge_edit(
        &access.for_instance(&ctx.state, h.pipe.instance_keys()),
        &old,
        requested,
    )?;
    access::guard::check_parameters(&ctx.state, &access, &conn, &h.pipe, Some(&old), &new).await?;
    if let Some(e) = access::guard::parameters_lock_conflict(&h.pipe, &old, &new) {
        return Ok(tuple(false, e.detail_text()));
    }
    let mut pipe = h.pipe.clone();
    pipe.set_parameters(new);
    let res = mutation::run(
        &ctx,
        &conn,
        &pipe,
        &access,
        mutation::Operation::Edit(old),
        conn.edit_pipe(&pipe, false),
    )
    .await?;
    ctx.state.pipes(ik.as_deref(), true).await?;
    forget_access(&ctx);
    Ok(tuple_response(&res))
}

pub(super) async fn delete_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let ik = ctx.instance_keys();
    let access = authorize(&ctx, PIPES_DELETE).await?;
    let (conn, h) = editable(&ctx, &access, &ck, &mk, &lk, true).await?;
    if !ctx.state.is_registered(&h.pipe, ik.as_deref()).await? {
        return Err(ApiError::new(409, format!("{} is not registered.", h.pipe)));
    }
    if let Some(e) = access::guard::lock_conflict(&h.pipe, LockOp::Delete) {
        return Ok(tuple(false, e.detail_text()));
    }
    let res = mutation::run(
        &ctx,
        &conn,
        &h.pipe,
        &access,
        mutation::Operation::Delete,
        conn.delete_pipe(&h.pipe),
    )
    .await?;
    ctx.state.pipes(ik.as_deref(), true).await?;
    forget_access(&ctx);
    Ok(tuple_response(&res))
}
