//! A pipe's table shape: column types, indices, index names and size.

use super::*;

pub(super) async fn get_pipe_columns_types(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let max_age = ctx.state.config.columns_types_cache_seconds;
    let types = match h.cached_columns_types(max_age) {
        Some(t) => t,
        None => {
            let t = conn
                .get_pipe_columns_types(&h.pipe)
                .await
                .map_err(|e| ApiError::exception("get_pipe_columns_types", e))?;
            h.cache_columns_types(t.clone());
            t
        }
    };
    let types = if types.is_empty() {
        meerschaum_connector::pipe_ops::get_dtypes(
            conn.as_ref(),
            &h.pipe,
            h.pipe.parameters(),
            true,
        )
        .await
        .map_err(|e| ApiError::exception("get_dtypes", e))?
    } else {
        types
    };
    Ok(ok_json(&serde_json::to_value(types).unwrap_or(Json::Null)))
}

pub(super) async fn get_pipe_columns_indices(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let indices = conn
        .get_pipe_columns_indices(&h.pipe)
        .await
        .map_err(|e| ApiError::exception("get_pipe_columns_indices", e))?;
    Ok(ok_json(
        &serde_json::to_value(indices).unwrap_or(Json::Null),
    ))
}

pub(super) async fn get_pipe_index_names(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let names = match conn.get_pipe_index_names(&h.pipe).await {
        Ok(m) => m,
        // `hasattr(instance_connector, 'get_pipe_index_names')` is false: `{}`.
        Err(meerschaum_connector::ConnectorError::NotImplemented(_)) => Map::new(),
        Err(e) => return Err(ApiError::exception("get_pipe_index_names", e)),
    };
    Ok(ok_json(&from_core(&Value::Map(names))))
}

pub(super) async fn get_pipe_size(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, PIPES_READ).await?;
    let access = original.initial().clone();
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let query = DataQuery::default();
    let window = original.check(&ctx, &h.pipe, &query).await?;
    let size = match conn.get_pipe_size(&h.pipe).await {
        Ok(s) => s,
        Err(meerschaum_connector::ConnectorError::NotImplemented(_)) => None,
        Err(e) => return Err(ApiError::exception("get_pipe_size", e)),
    };
    original.finish(&ctx, &h.pipe, &query, &window).await?;
    Ok(ok_json(&json!(size)))
}
