//! Drop, clear and cache deletion, and the table maintenance routes (compress, vacuum, analyze,
//! partition).

use super::*;

pub(super) async fn drop_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_DROP).await?;
    let ik = ctx.instance_keys();
    let (conn, h) = editable(&ctx, &access, &ck, &mk, &lk, true).await?;
    if let Some(e) = access::guard::lock_conflict(&h.pipe, LockOp::Drop) {
        return Ok(tuple(false, e.detail_text()));
    }
    let res = mutation::run(
        &ctx,
        &conn,
        &h.pipe,
        &access,
        mutation::Operation::Drop,
        conn.drop_pipe(&h.pipe),
    )
    .await?;
    h.invalidate();
    ctx.state.pipes(ik.as_deref(), true).await?;
    Ok(tuple_response(&res))
}

pub(super) async fn clear_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_DELETE).await?;
    let q = &ctx.query;
    let params = parse_params(q.get("params"))?;
    let ik = ctx.instance_keys();
    let (conn, h) = editable(&ctx, &access, &ck, &mk, &lk, true).await?;
    if let Some(e) = access::guard::lock_conflict(&h.pipe, LockOp::Clear) {
        return Ok(tuple(false, e.detail_text()));
    }
    let defaults = &ctx.state.config.pipe_defaults;
    let begin = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    let query = DataQuery {
        begin,
        end,
        params: Some(params),
        ..Default::default()
    };
    let clear = conn.clear_pipe(&h.pipe, &query);
    let res = mutation::run(
        &ctx,
        &conn,
        &h.pipe,
        &access,
        mutation::Operation::Clear,
        clear,
    )
    .await?;
    ctx.state.pipes(ik.as_deref(), true).await?;
    Ok(tuple_response(&res))
}

pub(super) async fn delete_pipe_cache(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_READ).await?;
    let (_, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    // `pipe._invalidate_cache(hard=True)` on the shared object.
    h.invalidate();
    Ok(tuple_response(&SuccessTuple::success()))
}

async fn maintenance<F, Fut>(
    ctx: Ctx,
    access: Access,
    keys: (String, String, String),
    verb: &str,
    op: F,
) -> ApiResult
where
    F: FnOnce(Arc<dyn InstanceConnector>, Pipe) -> Fut,
    Fut: std::future::Future<Output = SuccessTuple>,
{
    let ik = ctx.instance_keys();
    let (conn, h) = editable(&ctx, &access, &keys.0, &keys.1, &keys.2, true).await?;
    refuse_protected(&h.pipe, verb)?;
    // Repartitioning changes the table (the `drop` lock).
    if verb == "repartition"
        && let Some(e) = access::guard::lock_conflict(&h.pipe, LockOp::Schema)
    {
        return Ok(tuple(false, e.detail_text()));
    }
    let res = mutation::run(
        &ctx,
        &conn,
        &h.pipe,
        &access,
        mutation::Operation::Maintenance {
            schema: verb == "repartition",
        },
        op(conn.clone(), h.pipe.clone()),
    )
    .await?;
    ctx.state.pipes(ik.as_deref(), true).await?;
    Ok(tuple_response(&res))
}

pub(super) async fn compress_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_WRITE).await?;
    maintenance(ctx, access, keys, "compress", |c, p| async move {
        c.compress_pipe(&p).await
    })
    .await
}

pub(super) async fn decompress_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_WRITE).await?;
    let no_policy = ctx.query.bool("no_policy", false)?;
    maintenance(ctx, access, keys, "decompress", move |c, p| async move {
        c.decompress_pipe(&p, no_policy).await
    })
    .await
}

pub(super) async fn vacuum_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_WRITE).await?;
    let full = ctx.query.bool("full", false)?;
    maintenance(ctx, access, keys, "vacuum", move |c, p| async move {
        c.vacuum_pipe(&p, full).await
    })
    .await
}

pub(super) async fn analyze_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_WRITE).await?;
    maintenance(ctx, access, keys, "analyze", |c, p| async move {
        c.analyze_pipe(&p).await
    })
    .await
}

pub(super) async fn partition_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_WRITE).await?;
    let chunk_minutes = ctx.query.opt_int("chunk_minutes")?;
    maintenance(ctx, access, keys, "repartition", move |c, p| async move {
        c.partition_pipe(&p, chunk_minutes).await
    })
    .await
}
