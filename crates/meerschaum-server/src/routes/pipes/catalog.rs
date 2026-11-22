//! List pipes and their keys, and read one pipe's registration (`/pipes`, `/pipes/keys`,
//! `/id`, `/attributes`, `/exists`) and `/metadata`.

use super::*;

fn json_str_list(raw: &str, key: &str) -> Result<Vec<Value>, ApiError> {
    match Value::from_json_str(raw) {
        Ok(Value::List(items)) => Ok(items),
        Ok(Value::Null) => Ok(Vec::new()),
        Ok(other) => Ok(vec![other]),
        Err(e) => Err(ApiError::exception(&format!("json.loads({key})"), e)),
    }
}

/// `keys` without the pipes `access` cannot see.
async fn visible_keys(ctx: &Ctx, access: &Access, keys: PipesKeys) -> Result<PipesKeys, ApiError> {
    if access.is_admin() {
        return Ok(keys);
    }
    let index = access::pipe_index(&ctx.state, ctx.instance_keys().as_deref()).await?;
    let visible = |e: &meerschaum_connector::PipeKeysEntry| {
        if e.connector_keys == "mrsm" {
            return false;
        }
        let lk = normalize_location(e.location_key.as_deref());
        index
            .get(&e.connector_keys, &e.metric_key, lk.as_deref())
            .is_none_or(|p| access.can_see_indexed(p))
    };
    Ok(match keys {
        PipesKeys::List(v) => PipesKeys::List(v.into_iter().filter(|e| visible(e)).collect()),
        PipesKeys::Dict(m) => PipesKeys::Dict(m.into_iter().filter(|(_, e)| visible(e)).collect()),
    })
}

pub(super) async fn fetch_pipes_keys(ctx: Ctx) -> ApiResult {
    let (access, operation) = authorize_catalog(&ctx).await?;
    let q = &ctx.query;
    let as_dict = q.bool("as_dict", false)?;
    let conn = ctx
        .state
        .get_api_connector(ctx.instance_keys().as_deref())?;
    let strs = |v: Vec<Value>| v.iter().map(Value::py_str).collect::<Vec<_>>();
    let query = PipesKeysQuery {
        connector_keys: strs(json_str_list(
            &q.str_or("connector_keys", "[]"),
            "connector_keys",
        )?),
        metric_keys: strs(json_str_list(
            &q.str_or("metric_keys", "[]"),
            "metric_keys",
        )?),
        location_keys: json_str_list(&q.str_or("location_keys", "[]"), "location_keys")?
            .iter()
            .map(|v| match v {
                Value::Null => None,
                other => Some(other.py_str()),
            })
            .collect(),
        tags: strs(json_str_list(&q.str_or("tags", "[]"), "tags")?),
        params: match Value::from_json_str(&q.str_or("params", "{}")) {
            Ok(Value::Map(m)) => Some(m),
            Ok(_) => None,
            Err(e) => return Err(ApiError::exception("json.loads(params)", e)),
        },
    };
    let keys = conn
        .fetch_pipes_keys(&query)
        .await
        .map_err(|e| ApiError::exception("fetch_pipes_keys", e))?;
    let keys = visible_keys(&ctx, &access, keys).await?;
    let keys = if let Some(operation) = &operation {
        let instance = ctx
            .instance_keys()
            .unwrap_or_else(|| ctx.state.config.instance_keys.clone());
        let allowed = |entry: &meerschaum_connector::PipeKeysEntry| {
            operation.permits(
                &instance,
                &entry.connector_keys,
                &entry.metric_key,
                entry.location_key.as_deref(),
            )
        };
        match keys {
            PipesKeys::List(entries) => {
                PipesKeys::List(entries.into_iter().filter(allowed).collect())
            }
            PipesKeys::Dict(entries) => PipesKeys::Dict(
                entries
                    .into_iter()
                    .filter(|(_, entry)| allowed(entry))
                    .collect(),
            ),
        }
    } else {
        keys
    };
    let out = match keys {
        PipesKeys::Dict(m) => {
            if as_dict {
                Json::Object(
                    m.iter()
                        .map(|(k, v)| (k.to_string(), from_core(&Value::List(v.to_list()))))
                        .collect(),
                )
            } else {
                Json::Array(
                    m.values()
                        .map(|v| from_core(&Value::List(v.to_list().into_iter().take(3).collect())))
                        .collect(),
                )
            }
        }
        PipesKeys::List(l) => {
            if as_dict {
                Json::Object(
                    l.iter()
                        .enumerate()
                        .map(|(i, v)| (i.to_string(), from_core(&Value::List(v.to_list()))))
                        .collect(),
                )
            } else {
                Json::Array(
                    l.iter()
                        .map(|v| from_core(&Value::List(v.to_list().into_iter().take(3).collect())))
                        .collect(),
                )
            }
        }
    };
    Ok(ok_json(&out))
}

pub(super) async fn get_pipes(ctx: Ctx) -> ApiResult {
    let (access, operation) = authorize_catalog(&ctx).await?;
    let q = &ctx.query;
    let ik = q.opt_str("instance_keys").filter(|s| !s.is_empty());
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    // `get_pipes(connector_keys=..., ...)` with the raw strings as one-element filters.
    let one = |k: &str| {
        q.get(k)
            .filter(|s| !s.is_empty())
            .map(|s| vec![s.to_string()])
            .unwrap_or_default()
    };
    let query = PipesKeysQuery {
        connector_keys: one("connector_keys"),
        metric_keys: one("metric_keys"),
        location_keys: one("location_keys")
            .into_iter()
            .map(|l| normalize_location(Some(&l)))
            .collect(),
        ..Default::default()
    };
    let keys = conn
        .fetch_pipes_keys(&query)
        .await
        .map_err(|e| ApiError::exception("fetch_pipes_keys", e))?;
    // PARITY: these pipes are built from `get_pipes()` and their attributes are the local
    // ones: no `pipe_id`, and `instance_keys: null` (`api/routes/_pipes.py:223`).
    let mut out = serde_json::Map::new();
    for entry in keys.entries() {
        if operation.as_ref().is_some_and(|operation| {
            !operation.permits(
                &conn.connector_keys(),
                &entry.connector_keys,
                &entry.metric_key,
                entry.location_key.as_deref(),
            )
        }) {
            continue;
        }
        let lk = normalize_location(entry.location_key.as_deref());
        let params = match &entry.extra {
            Some(Value::Map(m)) => Value::Map(m.clone()),
            Some(Value::Str(s)) => match Value::from_json_str(s) {
                Ok(v @ Value::Map(_)) => v,
                _ => Value::new_map(),
            },
            _ => {
                let h = ctx
                    .state
                    .get_pipe(
                        &entry.connector_keys,
                        &entry.metric_key,
                        lk.as_deref().or(Some("None")),
                        ik.as_deref(),
                        false,
                    )
                    .await?;
                Value::Map(h.pipe.raw_parameters().clone())
            }
        };
        let listed_pipe = Pipe::builder(&entry.connector_keys, &entry.metric_key)
            .location(lk.as_deref())
            .parameters(params.to_map())
            .build()
            .map_err(|e| ApiError::exception("Pipe", e))?;
        if !access.can_see_pipe(&listed_pipe) {
            continue;
        }
        let attrs = json!({
            "connector_keys": entry.connector_keys,
            "metric_key": entry.metric_key,
            "location_key": lk,
            "instance_keys": null,
            "parameters": from_core(&params),
        });
        let metrics = out
            .entry(entry.connector_keys.clone())
            .or_insert_with(|| Json::Object(Default::default()));
        if let Some(m) = metrics.as_object_mut() {
            let locs = m
                .entry(entry.metric_key.clone())
                .or_insert_with(|| Json::Object(Default::default()));
            if let Some(l) = locs.as_object_mut() {
                l.insert(lk.unwrap_or_else(|| "None".into()), attrs);
            }
        }
    }
    Ok(ok_json(&Json::Object(out)))
}

pub(super) async fn get_pipes_by_connector(ctx: Ctx, Path(ck): Path<String>) -> ApiResult {
    let access = authorize(&ctx, PIPES_READ).await?;
    let ik = ctx.instance_keys();
    let dict = ctx.state.pipes(ik.as_deref(), true).await?;
    let not_found = || ApiError::new(404, format!("Connector '{ck}' not found."));
    let Some(metrics) = dict.get(&ck) else {
        return Err(not_found());
    };
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    let mut out = serde_json::Map::new();
    for (mk, locations) in metrics {
        let seen: Vec<_> = locations
            .iter()
            .filter(|(_, h)| access.can_see_pipe(&h.pipe))
            .collect();
        if seen.is_empty() {
            continue;
        }
        out.insert(
            mk.clone(),
            Json::Object(attributes_tree(&conn, seen).await?),
        );
    }
    if out.is_empty() && !metrics.is_empty() {
        return Err(not_found());
    }
    Ok(ok_json(&Json::Object(out)))
}

pub(super) async fn get_pipes_by_connector_and_metric(
    ctx: Ctx,
    Path((ck, mk)): Path<(String, String)>,
) -> ApiResult {
    let access = authorize(&ctx, PIPES_READ).await?;
    let ik = ctx.instance_keys();
    let dict = ctx.state.pipes(ik.as_deref(), true).await?;
    let visible = |locations: &indexmap::IndexMap<Option<String>, Arc<PipeHandle>>| {
        locations.values().any(|h| access.can_see_pipe(&h.pipe))
    };
    let Some(metrics) = dict.get(&ck).filter(|m| m.values().any(visible)) else {
        return Err(ApiError::new(404, format!("Connector '{ck}' not found.")));
    };
    let Some(locations) = metrics.get(&mk).filter(|l| visible(l)) else {
        return Err(ApiError::new(404, format!("Metric '{mk}' not found.")));
    };
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    let seen: Vec<_> = locations
        .iter()
        .filter(|(_, h)| access.can_see_pipe(&h.pipe))
        .collect();
    Ok(ok_json(&Json::Object(attributes_tree(&conn, seen).await?)))
}

pub(super) async fn get_pipe_by_keys(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let ik = ctx.instance_keys();
    let dict = ctx.state.pipes(ik.as_deref(), true).await?;
    let visible = |locations: &indexmap::IndexMap<Option<String>, Arc<PipeHandle>>| {
        locations.values().any(|h| access.can_see_pipe(&h.pipe))
    };
    let Some(metrics) = dict.get(&ck).filter(|m| m.values().any(visible)) else {
        return Err(ApiError::new(404, format!("Connector '{ck}' not found.")));
    };
    let Some(locations) = metrics.get(&mk).filter(|l| visible(l)) else {
        return Err(ApiError::new(404, format!("Metric '{mk}' not found.")));
    };
    let loc = normalize_location(Some(&lk));
    let Some(h) = locations.get(&loc).filter(|h| access.can_see_pipe(&h.pipe)) else {
        return Err(ApiError::new(
            404,
            format!(
                "location_key '{}' not found.",
                loc.as_deref().unwrap_or("None")
            ),
        ));
    };
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    let attrs = pipe_attributes(&conn, &h.pipe).await?;
    Ok(ok_json(&from_core(&Value::Map(attrs))))
}

pub(super) async fn get_pipe_id(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let id = conn
        .get_pipe_id(&h.pipe)
        .await
        .map_err(|e| ApiError::exception("get_pipe_id", e))?;
    match id {
        None => Err(ApiError::new(404, "Pipe is not registered.")),
        Some(id) => Ok(ok_json(&from_core(&id.to_value()))),
    }
}

pub(super) async fn get_pipe_attributes(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, true).await?;
    let attrs = pipe_attributes(&conn, &h.pipe).await?;
    Ok(ok_json(&from_core(&Value::Map(attrs))))
}

pub(super) async fn get_pipe_exists(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    // A catalog query that fails is a `500`, never `false`: a client deciding whether a pipe
    // has rows to read must not take a failed instance for a missing table (v4 clients still
    // read any refusal as `False`).
    let exists = conn
        .pipe_exists_checked(&h.pipe)
        .await
        .map_err(|e| ApiError::exception("pipe_exists", e))?;
    Ok(ok_json(&Json::Bool(exists)))
}

pub(super) async fn create_metadata(ctx: Ctx) -> ApiResult {
    authorize(&ctx, &["actions:execute"]).await?;
    let conn = ctx
        .state
        .get_api_connector(ctx.instance_keys().as_deref())?;
    // `if conn.type not in ('sql', 'api'): return False`.
    if !matches!(conn.connector_type(), "sql" | "api") {
        return Ok(ok_json(&Json::Bool(false)));
    }
    // `get_tables(mrsm_instance=conn)`: creates the instance tables of a SQL instance (an API
    // instance has none of its own).
    // PARITY: v4 only logs a failed `create_all` (`tables/__init__.py: create_tables`) and still
    // answers `true`; a failure here is a 500 carrying the reason.
    match conn.create_instance_tables().await {
        Ok(true) => Ok(ok_json(&Json::Bool(true))),
        Ok(false) => Err(ApiError::new(500, "Failed to create the instance tables.")),
        Err(e) => Err(ApiError::new(500, e.to_string())),
    }
}
