//! Numeric performance history. Credentials and object ACLs are refreshed before aggregation.

use axum::{
    Router,
    body::Body,
    http::StatusCode,
    routing::{get, post},
};
use meerschaum_connector::sync_metrics::{BATCH_SIZE, MetricsQuery, SyncMetric};
use meerschaum_core::Pipe;
use serde::Deserialize;
use serde_json::json;

use super::Ctx;
use crate::access::{self, Access, guard};
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::json_response;
use crate::state::AppState;

/// Register the native metrics endpoint in the engine route group.
pub fn routes(app: Router<AppState>, _config: &ServerConfig) -> Router<AppState> {
    app.route("/engine/metrics", get(read).post(record))
        .route("/engine/metrics/initialize", post(initialize))
}

async fn caller(ctx: &Ctx, scopes: &[&str]) -> Result<Access, ApiError> {
    let access = if ctx.state.config.no_auth {
        Access::everything()
    } else {
        let principal = crate::auth::load_user_or_token(&ctx.state, &ctx.headers).await?;
        let granted = crate::auth::granted_scopes(&ctx.state, &ctx.headers, &principal).await?;
        access::access_of_fresh(&ctx.state, principal, &granted).await?
    };
    access.require(scopes, &[])?;
    Ok(access)
}

fn timestamp(ctx: &Ctx, name: &str) -> Result<Option<chrono::DateTime<chrono::Utc>>, ApiError> {
    ctx.query
        .opt_str(name)
        .map(|text| {
            chrono::DateTime::parse_from_rfc3339(&text)
                .map(|value| value.with_timezone(&chrono::Utc))
                .map_err(|_| {
                    ApiError::new(
                        400,
                        format!("'{name}' must be an ISO-8601 timestamp with a timezone."),
                    )
                })
        })
        .transpose()
}

fn count(ctx: &Ctx, name: &str) -> Result<Option<usize>, ApiError> {
    ctx.query
        .opt_str(name)
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| ApiError::new(400, format!("'{name}' must be a positive integer.")))
        })
        .transpose()
}

fn ceiling(ctx: &Ctx, name: &str) -> Result<Option<Vec<String>>, ApiError> {
    ctx.query
        .opt_str(name)
        .map(|text| {
            let values: Vec<String> = serde_json::from_str(&text)
                .map_err(|_| ApiError::new(400, "Invalid metrics visibility filter."))?;
            if values.len() > 10000 {
                return Err(ApiError::new(
                    400,
                    "The metrics visibility filter is too large.",
                ));
            }
            Ok(values)
        })
        .transpose()
}

async fn read(ctx: Ctx) -> ApiResult {
    let caller = caller(&ctx, &["pipes:read"]).await?;
    let instance = ctx.instance_keys();
    let conn = ctx.state.get_api_connector(instance.as_deref())?;
    guard::check_connector_teams(&ctx.state, &caller, &conn.connector_keys())?;
    let mut query = MetricsQuery {
        begin: timestamp(&ctx, "begin")?,
        end: timestamp(&ctx, "end")?,
        pipe_ref: ctx
            .query
            .opt_str("pipe_ref")
            .or_else(|| ctx.query.opt_str("pipe")),
        pipeline: ctx.query.opt_str("pipeline"),
        limit: count(&ctx, "limit")?,
        buckets: count(&ctx, "buckets")?,
        ..Default::default()
    };
    query.validate().map_err(|_| {
        ApiError::new(
            400,
            "Metrics need an ordered time window of at most 30 days.",
        )
    })?;
    // This is a fresh registry read, rather than an access cache shared only by one worker.
    let registry = ctx.state.pipes(instance.as_deref(), true).await?;
    let index = access::index_from_dict(&conn.connector_keys(), &registry, conn.as_ref());
    let hidden = index.hidden_targets(&caller);
    let mut visible_refs = Vec::new();
    let mut visible_pipelines = Vec::new();
    for (connector, metrics) in registry.iter() {
        for (metric, locations) in metrics {
            for (location, handle) in locations {
                if !caller.can_see(handle.pipe.parameters())
                    || !guard::may_use_connector(&ctx.state, &caller, connector)
                {
                    continue;
                }
                if connector == "mrsm" && metric == "pipeline" {
                    if let Some(name) = location {
                        visible_pipelines.push(name.clone());
                    }
                } else if meerschaum_connector::sync_status::is_recorded(&handle.pipe) {
                    if !caller.can_see_pipe(&handle.pipe)
                        || guard::check_pipe_target(conn.as_ref(), &handle.pipe, &hidden).is_err()
                    {
                        continue;
                    }
                    visible_refs.push(handle.pipe.pipe_ref());
                }
            }
        }
    }
    let saved = conn
        .registered_pipes(&meerschaum_connector::PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            ..Default::default()
        })
        .await
        .map_err(|_| ApiError::new(503, "Cannot read saved Pipeline access."))?;
    for (_, entry) in saved {
        let parameters = match &entry.extra {
            Some(meerschaum_core::Value::Map(parameters)) => parameters.clone(),
            Some(meerschaum_core::Value::Str(text)) => meerschaum_core::Value::from_json_str(text)
                .ok()
                .map(|value| value.to_map())
                .unwrap_or_default(),
            _ => meerschaum_core::Map::new(),
        };
        if caller.can_see(&parameters)
            && let Some(name) = &entry.location_key
        {
            visible_pipelines.push(name.clone());
        }
    }
    // Remote ceilings can only narrow the fresh server-derived ceiling.
    if let Some(requested) = ceiling(&ctx, "visible_refs")? {
        let keys: std::collections::HashSet<_> = requested
            .iter()
            .filter_map(|reference| meerschaum_connector::sync_metrics::reference_keys(reference))
            .map(|(connector, metric, location)| {
                meerschaum_connector::sync_metrics::pipe_key(connector, metric, location)
            })
            .collect();
        visible_refs.retain(|reference| {
            meerschaum_connector::sync_metrics::reference_keys(reference).is_some_and(
                |(connector, metric, location)| {
                    keys.contains(&meerschaum_connector::sync_metrics::pipe_key(
                        connector, metric, location,
                    ))
                },
            )
        });
    }
    if let Some(requested) = ceiling(&ctx, "visible_pipelines")? {
        visible_pipelines.retain(|name| requested.contains(name));
    }
    query.visible_refs = Some(visible_refs);
    query.visible_pipelines = Some(visible_pipelines);
    let metrics = conn
        .get_engine_metrics(&query)
        .await
        .map_err(|_| ApiError::new(503, "Cannot read sync performance history."))?;
    Ok(json_response(
        StatusCode::OK,
        &json!({"success": true, "message": "Sync performance history.", "metrics": metrics}),
    ))
}

#[derive(Deserialize)]
struct Batch {
    metrics: Vec<SyncMetric>,
}

async fn record(ctx: Ctx, body: Body) -> ApiResult {
    let caller = caller(&ctx, &["pipes:write"]).await?;
    let bytes = axum::body::to_bytes(body, 1 << 20)
        .await
        .map_err(|_| ApiError::new(413, "The metrics body is too large."))?;
    let mut batch: Batch =
        serde_json::from_slice(&bytes).map_err(|_| ApiError::new(400, "Invalid sync metrics."))?;
    if batch.metrics.is_empty() || batch.metrics.len() > BATCH_SIZE {
        return Err(ApiError::new(
            400,
            "A metrics batch must contain 1 through 128 outcomes.",
        ));
    }
    let instance = ctx.instance_keys();
    let conn = ctx.state.get_api_connector(instance.as_deref())?;
    guard::check_connector_teams(&ctx.state, &caller, &conn.connector_keys())?;
    let registry = ctx.state.pipes(instance.as_deref(), true).await?;
    let current = meerschaum_connector::sync_metrics::registrations(conn.as_ref())
        .await
        .map_err(|_| ApiError::new(503, "Cannot read Pipe registrations."))?;
    for metric in &mut batch.metrics {
        let pipe = Pipe::builder(&metric.connector_keys, &metric.metric_key)
            .location(metric.location_key.as_deref())
            .instance(&conn.connector_keys())
            .build()
            .map_err(|_| ApiError::new(400, "Invalid metric Pipe identity."))?;
        if !meerschaum_connector::sync_status::is_recorded(&pipe) {
            return Err(ApiError::new(
                400,
                "Internal Pipes do not emit user sync metrics.",
            ));
        }
        guard::check_connector_teams(&ctx.state, &caller, pipe.connector_keys())?;
        let registered = registry
            .get(pipe.connector_keys())
            .and_then(|metrics| metrics.get(pipe.metric_key()))
            .and_then(|locations| locations.get(&metric.location_key));
        let Some(registered) = registered else {
            return Err(ApiError::new(404, "Pipe was not found."));
        };
        if !caller.can_see(registered.pipe.parameters()) {
            return Err(ApiError::new(404, "Pipe was not found."));
        }
        if !caller.can_edit(registered.pipe.parameters()) {
            return Err(access::not_editable(&pipe));
        }
        let key = meerschaum_connector::sync_metrics::pipe_key(
            pipe.connector_keys(),
            pipe.metric_key(),
            pipe.location_key(),
        );
        if current.get(&key) != Some(&metric.registration_id) {
            return Err(ApiError::new(
                409,
                "The metric belongs to an earlier Pipe registration or privacy segment.",
            ));
        }
        metric
            .canonicalize(&pipe)
            .map_err(|_| ApiError::new(400, "Invalid sync metric bounds or identity."))?;
    }
    conn.record_sync_metrics(&batch.metrics)
        .await
        .map_err(|_| ApiError::new(503, "Cannot persist sync metrics."))?;
    Ok(json_response(
        StatusCode::OK,
        &json!({"success": true, "message": "Recorded sync performance."}),
    ))
}

async fn initialize(ctx: Ctx) -> ApiResult {
    let caller = caller(&ctx, &["pipes:write"]).await?;
    if !caller.full_admin() {
        return Err(ApiError::new(
            403,
            "Only a full administrator may initialize internal metrics storage.",
        ));
    }
    let instance = ctx.instance_keys();
    let conn = ctx.state.get_api_connector(instance.as_deref())?;
    guard::check_connector_teams(&ctx.state, &caller, &conn.connector_keys())?;
    conn.initialize_engine_metrics()
        .await
        .map_err(|_| ApiError::new(503, "Cannot initialize sync metrics."))?;
    Ok(json_response(
        StatusCode::OK,
        &json!({"success": true, "message": "Sync performance storage is ready."}),
    ))
}
