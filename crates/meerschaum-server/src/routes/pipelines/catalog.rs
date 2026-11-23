//! List and read saved Pipelines: their registrations, definitions, members and last runs
//! (`GET /pipelines`, `/pipelines/keys`, `/pipelines/{name}`).

use super::*;

/// The saved Pipelines of the registry `conn` with their registrations' parameters (`access`
/// and `locks` live there), by name (each definition carries its `name`).
pub async fn list_registrations(
    state: &AppState,
    conn: &Arc<dyn InstanceConnector>,
) -> Result<Vec<(Json, Map)>, ApiError> {
    let listing = conn
        .fetch_pipes_keys(&PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            ..Default::default()
        })
        .await
        .map_err(|e| failed("fetch_pipes_keys", e))?;
    let mut pipelines: Vec<(String, Json, Map)> = Vec::new();
    for row in listing.entries() {
        if let Some(name) = &row.location_key {
            let pipe = registration_pipe(state, conn, name)?;
            let attrs = conn
                .get_pipe_attributes(&pipe)
                .await
                .map_err(|e| failed("get_pipe_attributes", e))?;
            let parameters = crate::state::parameters_of(&attrs).unwrap_or_default();
            if let Some(definition) = parameters.get("pipeline").cloned() {
                pipelines.push((name.clone(), from_core(&definition), parameters));
            }
        }
    }
    pipelines.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(pipelines
        .into_iter()
        .map(|(name, mut definition, parameters)| {
            if let Some(o) = definition.as_object_mut() {
                o.entry("name").or_insert(json!(name));
            }
            (definition, parameters)
        })
        .collect())
}

/// The registration parameters of the Pipeline `name` (`None` when it is not registered).
pub async fn registration_parameters(
    state: &AppState,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
) -> Result<Option<Map>, ApiError> {
    let pipe = registration_pipe(state, conn, name)?;
    if conn
        .get_pipe_id(&pipe)
        .await
        .map_err(|e| failed("get_pipe_id", e))?
        .is_none()
    {
        return Ok(None);
    }
    let attrs = conn
        .get_pipe_attributes(&pipe)
        .await
        .map_err(|e| failed("get_pipe_attributes", e))?;
    Ok(Some(
        crate::state::parameters_of(&attrs).unwrap_or_default(),
    ))
}

/// `definition` as `caller` may read it: the members (`targets`) it cannot see left out and
/// counted (`hidden_members`).
pub async fn visible_definition(filter: &mut RefFilter, mut definition: Json) -> Json {
    let targets = targets_of(&definition);
    let mut kept = Vec::with_capacity(targets.len());
    let mut hidden = 0usize;
    for target in targets {
        if filter.visible(&target).await {
            kept.push(Json::String(target));
        } else {
            hidden += 1;
        }
    }
    if let Some(o) = definition.as_object_mut() {
        // The response declares its actual served registry, never a user-supplied alias.
        o.insert("registry".into(), json!(filter.default_instance()));
        if o.contains_key("targets") {
            o.insert("targets".into(), Json::Array(kept));
        }
        if hidden > 0 {
            o.insert("hidden_members".into(), json!(hidden));
        }
    }
    definition
}

/// The registered Pipeline `name` that `caller` may see: its definition and registration
/// parameters (`404` otherwise, as for one that does not exist).
pub async fn visible_registration(
    state: &AppState,
    caller: &Access,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
) -> Result<(Json, Map), ApiError> {
    let missing = || {
        ApiError::new(
            404,
            format!(
                "Pipeline '{name}' is not registered on '{}'.",
                conn.connector_keys()
            ),
        )
    };
    let parameters = registration_parameters(state, conn, name)
        .await?
        .ok_or_else(missing)?;
    if !caller
        .for_instance(state, &conn.connector_keys())
        .can_see(&parameters)
    {
        return Err(missing());
    }
    let definition = parameters
        .get("pipeline")
        .map(from_core)
        .ok_or_else(missing)?;
    Ok((definition, parameters))
}

/// The saved Pipelines of the registry `conn`, by name (each definition carries its `name`).
pub async fn list_definitions(
    state: &AppState,
    conn: &Arc<dyn InstanceConnector>,
) -> Result<Vec<Json>, ApiError> {
    let listing = conn
        .fetch_pipes_keys(&PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            ..Default::default()
        })
        .await
        .map_err(|e| failed("fetch_pipes_keys", e))?;
    let mut pipelines: Vec<(String, Json)> = Vec::new();
    for row in listing.entries() {
        if let Some(name) = &row.location_key {
            let pipe = registration_pipe(state, conn, name)?;
            let attrs = conn
                .get_pipe_attributes(&pipe)
                .await
                .map_err(|e| failed("get_pipe_attributes", e))?;
            if let Some(definition) =
                crate::state::parameters_of(&attrs).and_then(|p| p.get("pipeline").cloned())
            {
                pipelines.push((name.clone(), from_core(&definition)));
            }
        }
    }
    pipelines.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(pipelines
        .into_iter()
        .map(|(name, mut definition)| {
            if let Some(o) = definition.as_object_mut() {
                o.entry("name").or_insert(json!(name));
            }
            definition
        })
        .collect())
}

pub(super) async fn list(ctx: Ctx) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let registry = conn.connector_keys();
    let mut filter = RefFilter::new(&ctx.state, &caller, &registry);
    let pipe_access = caller.for_instance(&ctx.state, &registry);
    let mut pipelines = Vec::new();
    for (definition, parameters) in list_registrations(&ctx.state, &conn).await? {
        if pipe_access.can_see(&parameters) {
            pipelines.push(visible_definition(&mut filter, definition).await);
        }
    }
    let message = format!(
        "Found {} on '{}'.",
        plural(pipelines.len(), "Pipeline", "Pipelines"),
        conn.connector_keys()
    );
    Ok(envelope::ok_with(message, "pipelines", json!(pipelines)))
}

pub(super) async fn keys(ctx: Ctx) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let decode = |key: &str| -> Result<Vec<Value>, ApiError> {
        match Value::from_json_str(&ctx.query.str_or(key, "[]")) {
            Ok(Value::List(items)) => Ok(items),
            Ok(Value::Null) => Ok(Vec::new()),
            _ => Err(ApiError::new(400, format!("Invalid {key} filter."))),
        }
    };
    let query = PipesKeysQuery {
        connector_keys: vec!["mrsm".into()],
        metric_keys: vec!["pipeline".into()],
        location_keys: decode("location_keys")?
            .into_iter()
            .map(|value| {
                if value.is_null() {
                    None
                } else {
                    Some(value.py_str())
                }
            })
            .collect(),
        tags: decode("tags")?.iter().map(Value::py_str).collect(),
        params: ctx
            .query
            .get("params")
            .map(Value::from_json_str)
            .transpose()
            .map_err(|e| ApiError::new(400, format!("Invalid params filter: {e}")))?
            .and_then(|value| value.as_map().cloned()),
    };
    let keys = conn
        .registered_pipes(&query)
        .await
        .map_err(|e| failed("fetch_pipes_keys", e))?;
    let mut rows: Vec<Json> = Vec::new();
    let mut dictionary = serde_json::Map::new();
    let mut as_dict = ctx.query.bool("as_dict", false)?;
    let mut filter = RefFilter::new(&ctx.state, &caller, &conn.connector_keys());
    let pipe_access = caller.for_instance(&ctx.state, &conn.connector_keys());
    for (identity, mut row) in keys {
        let mut parameters = match row.extra {
            Some(Value::Map(parameters)) => parameters,
            Some(Value::Str(text)) => match Value::from_json_str(&text) {
                Ok(Value::Map(parameters)) => parameters,
                _ => continue,
            },
            _ => continue,
        };
        if !pipe_access.can_see(&parameters) {
            continue;
        }
        if let Some(definition) = parameters.get("pipeline") {
            let visible = visible_definition(&mut filter, from_core(definition)).await;
            parameters.insert("pipeline".into(), to_core(&visible));
        }
        row.extra = Some(Value::Map(parameters));
        let value = from_core(&Value::List(row.to_list()));
        rows.push(value.clone());
        if let Some(identity) = identity {
            dictionary.insert(identity.to_string(), value);
        } else {
            as_dict = false;
        }
    }
    Ok(envelope::ok_with(
        format!("Found {}.", plural(rows.len(), "Pipeline", "Pipelines")),
        "keys",
        if as_dict {
            Json::Object(dictionary)
        } else {
            json!(rows)
        },
    ))
}

/// A member reference `instance|connector|metric|location` as JSON, with whether it is
/// registered (`null` when its instance cannot be checked from here).
pub async fn member(state: &AppState, reference: &str) -> Json {
    let parts: Vec<&str> = reference.splitn(4, '|').collect();
    let [instance, ck, mk, lk] = parts.as_slice() else {
        return json!({"pipe_ref": reference, "registered": null});
    };
    let location = crate::state::normalize_location(Some(lk));
    let registered = match state.get_api_connector(Some(instance)) {
        Ok(conn) => match Pipe::builder(ck, mk)
            .location(location.as_deref())
            .instance(instance)
            .build()
        {
            Ok(pipe) => match conn.get_pipe_id(&pipe).await {
                Ok(id) => json!(id.is_some()),
                Err(_) => Json::Null,
            },
            Err(_) => Json::Null,
        },
        Err(_) => Json::Null,
    };
    json!({
        "pipe_ref": reference,
        "connector_keys": ck,
        "metric_key": mk,
        "location_key": location,
        "instance_keys": instance,
        "registered": registered,
    })
}

/// The members (targets) a definition names.
pub fn targets_of(definition: &Json) -> Vec<String> {
    definition
        .get("targets")
        .and_then(Json::as_array)
        .map(|t| {
            t.iter()
                .filter_map(Json::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// The engine store of the registry instance `conn` (the server's own, or that instance's state
/// pipes): where a saved Pipeline's runs are kept.
pub fn store_of(
    state: &AppState,
    conn: &Arc<dyn InstanceConnector>,
) -> Arc<dyn crate::backends::EngineBackend> {
    let keys = conn.connector_keys();
    if crate::engine_stream::own_instance_names(state).contains(&keys) {
        state.engine.clone()
    } else {
        Arc::new(crate::engine_store::InstanceEngine::new(
            conn.clone(),
            &keys,
        ))
    }
}

/// The latest run of the Pipeline `name` on its registry `conn` (`null` if none, or if the
/// history cannot be read: it is informational here).
pub async fn last_run(
    state: &AppState,
    caller: &Access,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
) -> Json {
    let store = store_of(state, conn);
    match store
        .get_runs(&RunsFilter {
            pipeline: Some(name.to_string()),
            limit: Some(1),
            ..Default::default()
        })
        .await
    {
        Ok(runs) => access::runs::visible_runs(
            &mut RefFilter::new(state, caller, &conn.connector_keys()).for_engine(),
            runs,
        )
        .await
        .into_iter()
        .find(|r| r.pipe_ref.is_none())
        .map(|r| serde_json::to_value(r).unwrap_or(Json::Null))
        .unwrap_or(Json::Null),
        Err(e) => {
            tracing::debug!("Cannot read the runs of Pipeline '{name}': {e}");
            Json::Null
        }
    }
}

/// `GET /pipelines/{name}`: the definition, its members and the latest run.
pub(super) async fn detail(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let (definition, _) = visible_registration(&ctx.state, &caller, &conn, &name).await?;
    let registry = conn.connector_keys();
    let mut filter = RefFilter::new(&ctx.state, &caller, &registry);
    let mut members = Vec::new();
    let mut hidden = 0usize;
    for reference in targets_of(&definition) {
        if filter.visible(&reference).await {
            members.push(member(&ctx.state, &reference).await);
        } else {
            hidden += 1;
        }
    }
    let definition = visible_definition(&mut filter, definition).await;
    let last_run = last_run(&ctx.state, &caller, &conn, &name).await;
    let mut fields = JsonMap::new();
    let message = format!(
        "Pipeline '{name}' on '{}' has {}{}.",
        conn.connector_keys(),
        plural(members.len() + hidden, "member", "members"),
        if hidden > 0 {
            format!(" ({})", plural(hidden, "hidden pipe", "hidden pipes"))
        } else {
            String::new()
        }
    );
    fields.insert("pipeline".into(), definition);
    fields.insert("members".into(), json!(members));
    fields.insert("hidden_members".into(), json!(hidden));
    fields.insert("last_run".into(), last_run);
    Ok(envelope::ok(message, fields))
}
