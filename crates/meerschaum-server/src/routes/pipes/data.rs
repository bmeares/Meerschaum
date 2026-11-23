//! Read a pipe's data (`/data`, `/docs`, `/csv`), its chunk bounds and its row count.

use super::*;

struct DataArgs {
    select_columns: Vec<String>,
    omit_columns: Vec<String>,
    params: Map,
    limit: i64,
    order: String,
}

fn data_args(ctx: &Ctx) -> Result<DataArgs, ApiError> {
    let q = &ctx.query;
    let max = ctx.state.config.max_response_row_limit;
    let limit = q.int("limit", max)?;
    let order = q.str_or("order", "asc");
    if limit > max {
        return Err(ApiError::new(
            413,
            format!("Requested limit {limit} exceeds the maximum response size of {max} rows."),
        ));
    }
    Ok(DataArgs {
        params: parse_params(q.get("params"))?,
        select_columns: parse_select_columns(q.get("select_columns"))?,
        omit_columns: parse_omit_columns(q.get("omit_columns"))?,
        limit,
        order,
    })
}

/// Use the same current registration for access checks, date bounds and the data read.
async fn visible_registration(
    ctx: &Ctx,
    access: &Access,
    ck: &str,
    mk: &str,
    lk: &str,
    missing: &'static str,
) -> Result<(Arc<dyn InstanceConnector>, Arc<PipeHandle>), ApiError> {
    let ik = ctx.instance_keys();
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    let pipe = ctx.state.pipe_from_path(&conn, ck, mk, Some(lk))?;
    let index = visibility_index(ctx, access).await?;
    let registered = ctx
        .state
        .registered_pipe(&conn, &pipe, ik.as_deref())
        .await?;
    check_pipe_visibility(
        &ctx.state,
        access,
        &conn,
        registered.as_ref().map_or(&pipe, |h| &h.pipe),
        index.as_deref(),
    )?;
    let h = registered.ok_or_else(|| ApiError::new(409, missing))?;
    refuse_protected_read(&h.pipe)?;
    Ok((conn, h))
}

pub(super) async fn get_pipe_data(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, PIPES_READ).await?;
    let access = original.initial().clone();
    let q = &ctx.query;
    let date_format = q.str_or("date_format", "iso");
    let date_unit = q.str_or("date_unit", "us");
    let double_precision = q.int("double_precision", 15)?;
    let geometry_format = q.str_or("geometry_format", "wkb_hex");
    let args = data_args(&ctx)?;
    let (conn, h) = visible_registration(
        &ctx,
        &access,
        &ck,
        &mk,
        &lk,
        "Pipe must be registered with the datetime column specified.",
    )
    .await?;
    let defaults = &ctx.state.config.pipe_defaults;
    let begin = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    let query = DataQuery {
        select_columns: nonempty(args.select_columns),
        omit_columns: nonempty(args.omit_columns),
        begin,
        end,
        params: Some(args.params),
        limit: Some(args.limit.min(ctx.state.config.max_response_row_limit)),
        order: Some(args.order),
        ..Default::default()
    };
    let window = original.check(&ctx, &h.pipe, &query).await?;
    let df = meerschaum_connector::pipe_ops::get_data(conn.as_ref(), &h.pipe, &query)
        .await
        .map_err(|e| ApiError::exception("get_pipe_data", e))?;
    let Some(df) = df else {
        return Err(ApiError::new(
            400,
            "Could not fetch data with the given parameters.",
        ));
    };
    let unit = DateUnit::parse(&date_unit).ok_or_else(ApiError::internal)?;
    let geometry_format = match geometry_format.as_str() {
        "geojson" => GeometryFormat::GeoJson,
        "wkt" => GeometryFormat::Wkt,
        _ => GeometryFormat::WkbHex,
    };
    let opts = DataJsonOptions {
        epoch: date_format == "epoch",
        date_unit: unit,
        double_precision: u32::try_from(double_precision.clamp(0, 15)).unwrap_or(15),
        geometry_format,
    };
    let encoded = data_to_json(&df, &opts);
    original.finish(&ctx, &h.pipe, &query, &window).await?;
    Ok(raw_json(StatusCode::OK, encoded))
}

pub(super) async fn get_pipe_docs(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, PIPES_READ).await?;
    let access = original.initial().clone();
    let args = data_args(&ctx)?;
    let q = &ctx.query;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let defaults = &ctx.state.config.pipe_defaults;
    let begin = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    refuse_protected_read(&h.pipe)?;
    let query = DataQuery {
        select_columns: nonempty(args.select_columns),
        omit_columns: nonempty(args.omit_columns),
        begin,
        end,
        params: Some(args.params),
        limit: Some(args.limit.min(ctx.state.config.max_response_row_limit)),
        order: Some(args.order),
        ..Default::default()
    };
    // v4's route calls `pipe.get_docs()`, i.e. the instance's own `get_pipe_docs()`: the raw
    // rows, not the dtype-enforced frame (SQLite keeps its stored datetime text
    // `2024-01-02 03:04:05.000000`, MySQL-family naive datetimes serialize as `...Z`, the
    // PostgreSQL family as `...+00:00`).
    let window = original.check(&ctx, &h.pipe, &query).await?;
    let docs = conn
        .get_pipe_docs(&h.pipe, &query)
        .await
        .map_err(|e| ApiError::exception("get_pipe_docs", e))?;
    let value = Value::List(
        docs.iter()
            .map(|d| Value::Map(d.iter().map(|(k, v)| (k.clone(), doc_value(v))).collect()))
            .collect(),
    );
    let encoded = python_json(&value);
    original.finish(&ctx, &h.pipe, &query, &window).await?;
    Ok(raw_json(StatusCode::OK, encoded))
}

/// A document cell as v4 serializes it: a SQL `NULL` is `null`, also in a float column, where the
/// frame holds `NaN` (v4's rows carry `None`).
// PARITY: a stored float `NaN` also becomes `null` (v4 would write the invalid-JSON `NaN`);
// the frame cannot tell the two apart.
fn doc_value(v: &meerschaum_kernel::Value) -> Value {
    match meerschaum_connector::pipe_ops::kernel_to_core(v) {
        Value::Float(f) if f.is_nan() => Value::Null,
        other => other,
    }
}

fn refuse_protected_read(pipe: &Pipe) -> Result<(), ApiError> {
    let t = target(pipe);
    if crate::web::session_target(&t) {
        return Err(ApiError::new(403, "The session table is internal."));
    }
    if meerschaum_connector_api::routes::PROTECTED_TARGETS.contains(&t.as_str()) {
        return Err(ApiError::new(
            409,
            format!("Cannot retrieve data from protected table '{t}'."),
        ));
    }
    Ok(())
}

pub(super) async fn get_pipe_chunk_bounds(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, &[]).await?;
    if original.graph(&ctx).await?.is_some() {
        return Err(ApiError::unavailable(
            "Delegated chunk grids must come from Main's exact native verification instructions.",
        ));
    }
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let q = &ctx.query;
    let bounded = q.bool("bounded", true)?;
    let chunk_interval_minutes = q.opt_int("chunk_interval_minutes")?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let defaults = &ctx.state.config.pipe_defaults;
    let begin = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    let (_, dt_typ) = pipe_methods::dt_info(&conn, &h.pipe).await;
    let chunk_interval = chunk_interval_minutes.map(|m| {
        if meerschaum_core::dtypes::are_dtypes_equal(&dt_typ, "int") {
            ChunkIntervalArg::Int(m)
        } else {
            ChunkIntervalArg::Delta(TimeDelta::minutes(m))
        }
    });
    let bounds = pipe_methods::get_chunk_bounds(
        &conn,
        &h.pipe,
        begin,
        end,
        bounded,
        chunk_interval,
        defaults,
    )
    .await?;
    let value = Value::List(
        bounds
            .iter()
            .map(|(b, e)| Value::List(vec![bound_json(b), bound_json(e)]))
            .collect(),
    );
    Ok(raw_json(StatusCode::OK, python_json(&value)))
}

pub(super) async fn get_pipe_csv(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, PIPES_READ).await?;
    let access = original.initial().clone();
    let q = &ctx.query;
    let params = parse_params(q.get("params"))?;
    let (conn, h) =
        visible_registration(&ctx, &access, &ck, &mk, &lk, "Pipe must be registered.").await?;
    let defaults = &ctx.state.config.pipe_defaults;
    let mut begin =
        pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let mut end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    if h.pipe.column("datetime").is_some() && original.graph(&ctx).await?.is_none() {
        if begin.is_none() {
            begin =
                pipe_methods::get_sync_time(&conn, &h.pipe, None, false, false, defaults).await?;
        }
        if end.is_none() {
            end = pipe_methods::get_sync_time(&conn, &h.pipe, None, true, false, defaults)
                .await?
                .map(|e| match e {
                    DateBound::Int(i) => DateBound::Int(i + 1),
                    DateBound::Datetime(dt) => {
                        DateBound::Datetime(meerschaum_kernel::value::DateTimeValue {
                            utc: dt.utc + TimeDelta::minutes(1),
                            tz: dt.tz,
                        })
                    }
                });
        }
    }
    let bounds_text = match (&begin, &end) {
        (Some(b), Some(e)) => format!("-{}-{}", b.py_str(), e.py_str()),
        _ => String::new(),
    };
    let filename = format!("{}{bounds_text}.csv", target(&h.pipe));
    let query = DataQuery {
        begin,
        end,
        params: Some(params),
        ..Default::default()
    };
    let window = original.check(&ctx, &h.pipe, &query).await?;
    let df = meerschaum_connector::pipe_ops::get_data(conn.as_ref(), &h.pipe, &query)
        .await
        .map_err(|e| ApiError::exception("get_pipe_data", e))?;
    original.finish(&ctx, &h.pipe, &query, &window).await?;
    let Some(df) = df else {
        // PARITY: `df.to_csv` on `None` raises `AttributeError`, a 500.
        return Err(ApiError::internal());
    };
    let mut resp = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/csv; charset=utf-8")],
        to_csv(&df),
    )
        .into_response();
    if let Ok(v) = HeaderValue::from_str(&format!("attachment; filename={filename}")) {
        resp.headers_mut().insert(header::CONTENT_DISPOSITION, v);
    }
    Ok(resp)
}

/// `GET .../rowcount`: v4's route, plus (new in v5) `estimate=true` for the database's row
/// estimate (exact where there is none or counting is cheap) and `params` as a JSON query string
/// for clients that cannot send a body with a `GET` (the body wins when both are given).
pub(super) async fn get_pipe_rowcount(
    ctx: Ctx,
    Path((ck, mk, lk)): Keys3,
    body: Body,
) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, &[]).await?;
    let body = parse_json_body(&body_bytes(body, ctx.state.config.limits.body).await?)?;
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let params = match optional_dict_body(body)? {
        Some(params) => Some(params),
        None => query_params_dict(&ctx)?,
    };
    let remote = ctx.query.bool("remote", false)?;
    let estimate = ctx.query.bool("estimate", false)?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let defaults = &ctx.state.config.pipe_defaults;
    let q = &ctx.query;
    let begin = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    // A count the instance fails to read is a `500`, not `0`.
    let query = DataQuery {
        begin: begin.clone(),
        end: end.clone(),
        params: params.clone(),
        remote,
        ..Default::default()
    };
    let window = original.check(&ctx, &h.pipe, &query).await?;
    if window.is_some() && estimate {
        return Err(ApiError::unavailable(
            "A full-table estimate cannot represent this Main-issued finite read.",
        ));
    }
    let n = pipe_methods::try_get_rowcount(&conn, &h.pipe, begin, end, params, remote, estimate)
        .await?;
    original.finish(&ctx, &h.pipe, &query, &window).await?;
    Ok(ok_json(&json!(n)))
}

/// `?params=<JSON object>` (an empty value or `null` means none).
fn query_params_dict(ctx: &Ctx) -> Result<Option<Map>, ApiError> {
    let Some(text) = ctx.query.get("params").filter(|t| !t.trim().is_empty()) else {
        return Ok(None);
    };
    match Value::from_json_str(text) {
        Ok(Value::Map(m)) => Ok(Some(m)),
        Ok(Value::Null) => Ok(None),
        _ => Err(ApiError::validation(vec![ApiError::field_error(
            "dict_type",
            &["query", "params"],
            "Input should be a JSON object",
            Json::String(text.to_string()),
            None,
        )])),
    }
}
