//! Plan a Pipeline run (`POST /pipelines/{name}/plan`) and anonymize the nodes the caller
//! cannot see.

use super::*;

/// A plan as `caller` may read it: the nodes of pipes it cannot see anonymized (`pipe_ref` and
/// details dropped, `hidden: true`), the edges to them dropped, `hidden_nodes` counted, and the
/// rendered `text` (which names every node) left out when any node is hidden.
pub async fn visible_plan(filter: &mut RefFilter, mut planned: Json) -> Json {
    let mut hidden: Vec<String> = Vec::new();
    if let Some(nodes) = planned
        .get_mut("plan")
        .and_then(|p| p.get_mut("nodes"))
        .and_then(Json::as_array_mut)
    {
        for node in nodes.iter_mut() {
            let Some(pipe_ref) = node
                .get("pipe_ref")
                .and_then(Json::as_str)
                .map(str::to_string)
            else {
                continue;
            };
            if filter.visible(&pipe_ref).await {
                continue;
            }
            hidden.push(pipe_ref);
            *node = json!({
                "pipe_ref": null,
                "hidden": true,
                "action": node.get("action").cloned().unwrap_or(Json::Null),
            });
        }
        for node in nodes.iter_mut() {
            if let Some(consumes) = node.get_mut("consumes").and_then(Json::as_array_mut) {
                consumes.retain(|c| {
                    !c.get("parent")
                        .and_then(Json::as_str)
                        .is_some_and(|p| hidden.iter().any(|h| h == p))
                });
            }
        }
    }
    if hidden.is_empty() {
        return planned;
    }
    if let Some(edges) = planned
        .get_mut("plan")
        .and_then(|p| p.get_mut("edges"))
        .and_then(Json::as_array_mut)
    {
        edges.retain(|edge| {
            !edge.as_array().is_some_and(|pair| {
                pair.iter()
                    .filter_map(Json::as_str)
                    .any(|r| hidden.iter().any(|h| h == r))
            })
        });
    }
    // Warnings that name a hidden pipe go too (they name it by its keys).
    let names_hidden = |w: &Json| {
        w.as_str().is_some_and(|text| {
            hidden.iter().any(|h| {
                access::split_ref(h)
                    .is_some_and(|(ck, mk, _)| text.contains(ck) && text.contains(mk))
            })
        })
    };
    if let Some(nodes) = planned
        .get_mut("plan")
        .and_then(|p| p.get_mut("nodes"))
        .and_then(Json::as_array_mut)
    {
        for node in nodes {
            if node.get("reason").is_some_and(names_hidden)
                && let Some(fields) = node.as_object_mut()
            {
                fields.remove("reason");
            }
            if let Some(warnings) = node.get_mut("warnings").and_then(Json::as_array_mut) {
                warnings.retain(|warning| !names_hidden(warning));
            }
        }
    }
    for warnings in [planned.get_mut("warnings")].into_iter().flatten() {
        if let Some(list) = warnings.as_array_mut() {
            list.retain(|w| !names_hidden(w));
        }
    }
    if let Some(list) = planned
        .get_mut("plan")
        .and_then(|p| p.get_mut("warnings"))
        .and_then(Json::as_array_mut)
    {
        list.retain(|w| !names_hidden(w));
    }
    if let Some(o) = planned.as_object_mut() {
        o.remove("text");
        o.insert("hidden_nodes".into(), json!(hidden.len()));
    }
    planned
}

/// The `begin` / `end` of a plan or sync body (strings or integers), `400` otherwise.
pub fn bounds_of(body: &Json) -> Result<PlanOptions, ApiError> {
    let bound = |key: &str| -> Result<Option<String>, ApiError> {
        match body.get(key) {
            None | Some(Json::Null) => Ok(None),
            Some(Json::String(s)) if s.trim().is_empty() => Ok(None),
            Some(Json::String(s)) => Ok(Some(s.trim().to_string())),
            Some(Json::Number(n)) if n.is_i64() => Ok(Some(n.to_string())),
            Some(other) => Err(ApiError::new(
                400,
                format!("`{key}` must be an ISO 8601 datetime or an integer, got {other}."),
            )),
        }
    };
    Ok(PlanOptions {
        begin: bound("begin")?,
        end: bound("end")?,
    })
}

/// `POST /pipelines/{name}/plan`: what a run would do, read-only.
pub(super) async fn plan(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let body = optional_body(body, ctx.state.config.limits.body).await?;
    let options = bounds_of(&body)?;
    let conn = connector(&ctx)?;
    visible_registration(&ctx.state, &caller, &conn, &name).await?;
    let planned = ctx
        .state
        .pipelines
        .plan(&name, &conn.connector_keys(), &options)
        .await
        .map_err(plan_error)?;
    let registry = conn.connector_keys();
    let planned = visible_plan(&mut RefFilter::new(&ctx.state, &caller, &registry), planned).await;
    let mut fields = planned.as_object().cloned().unwrap_or_default();
    let nodes = fields
        .get("plan")
        .and_then(|p| p.get("nodes"))
        .and_then(Json::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    fields.entry("warnings").or_insert(json!([]));
    Ok(envelope::ok(
        format!(
            "Planned Pipeline '{name}': {}.",
            plural(nodes, "pipe", "pipes")
        ),
        fields,
    ))
}

/// A plan failure as a response.
pub fn plan_error(e: PlanError) -> ApiError {
    match e {
        PlanError::Unavailable => ApiError::new(501, e.to_string()),
        PlanError::MetadataUnavailable(message) => ApiError::unavailable(message),
        PlanError::Invalid(m) => ApiError::new(400, m),
        PlanError::Failed(m) => ApiError::new(500, "Failed to plan the Pipeline.").with_internal(m),
    }
}
