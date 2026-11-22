//! Register, edit, rename and delete a saved Pipeline, and read its ID and attributes.

use super::*;

/// Refuse members of `definition` that `caller` cannot see (other than those in `allowed`).
async fn check_new_members(
    filter: &mut RefFilter,
    definition: Option<&Value>,
    allowed: &[String],
) -> Result<(), ApiError> {
    let targets = definition
        .map(|d| targets_of(&from_core(d)))
        .unwrap_or_default();
    for target in targets {
        if !allowed.contains(&target) && !filter.visible(&target).await {
            return Err(ApiError::new(
                403,
                format!("Refusing a Pipeline member you cannot see ('{target}')."),
            ));
        }
    }
    Ok(())
}

pub(super) async fn register(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller = access::require_fresh(&ctx.state, &ctx.headers, &["pipes:register"]).await?;
    if !ctx.state.config.registration_pipes {
        return Err(ApiError::new(403, "Pipe registration is disabled."));
    }
    let conn = connector(&ctx)?;
    access::guard::check_connector_teams(&ctx.state, &caller, &conn.connector_keys())?;
    let mut pipe = metadata(&ctx, &conn, &name)?;
    let mut requested = parameters(body, &name, ctx.state.config.limits.body).await?;
    let registry = conn.connector_keys();
    canonicalize_client_members(&ctx, &registry, &mut requested);
    if let Some(Value::Map(definition)) = requested.get_mut("pipeline") {
        definition.insert("id".into(), Value::Str(uuid::Uuid::new_v4().to_string()));
        definition.insert("name".into(), Value::Str(name.clone()));
    }
    check_new_members(
        &mut RefFilter::new(&ctx.state, &caller, &registry),
        requested.get("pipeline"),
        &[],
    )
    .await?;
    pipe.set_parameters(access::guard::stamp_new_pipe(&caller, &requested)?);
    if conn
        .get_pipe_id(&pipe)
        .await
        .map_err(|e| failed("get_pipe_id", e))?
        .is_some()
    {
        return Err(ApiError::new(
            409,
            format!("Pipeline '{name}' is already registered."),
        ));
    }
    let res = conn.register_pipe(&pipe).await;
    ctx.state.access_cache().forget_pipes();
    envelope::tuple(&res, 500)
}

pub(super) async fn edit(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller = access::require_fresh(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    let conn = connector(&ctx)?;
    access::guard::check_connector_teams(&ctx.state, &caller, &conn.connector_keys())?;
    let mut pipe = metadata(&ctx, &conn, &name)?;
    let Some(old) = registration_parameters(&ctx.state, &conn, &name).await? else {
        return Err(ApiError::new(
            404,
            format!("Pipeline '{name}' is not registered."),
        ));
    };
    if !caller.can_see(&old) {
        return Err(ApiError::new(
            404,
            format!("Pipeline '{name}' is not registered."),
        ));
    }
    if !caller.can_edit(&old) {
        return Err(ApiError::new(
            403,
            format!(
                "You cannot change Pipeline '{name}': only its owner, its team's managers and administrators may."
            ),
        ));
    }
    let patch = ctx.query.bool("patch", false)?;
    let mut requested = parameters(body, &name, ctx.state.config.limits.body).await?;
    canonicalize_client_members(&ctx, &conn.connector_keys(), &mut requested);
    let requested = if patch {
        meerschaum_core::value::apply_patch_maps(&old, &requested)
    } else {
        requested
    };
    let mut new = access::guard::merge_edit(&caller, &old, requested)?;
    if let Some(Value::Map(definition)) = new.get_mut("pipeline") {
        let identity = old
            .get("pipeline")
            .and_then(|pipeline| pipeline.get("id"))
            .and_then(Value::as_str)
            .filter(|identity| !identity.is_empty() && !identity.starts_with("legacy:"));
        if let Some(identity) = identity {
            if definition
                .get("id")
                .is_some_and(|requested| requested.as_str() != Some(identity))
            {
                return Err(ApiError::new(
                    409,
                    "A saved Pipeline identity cannot be replaced.",
                ));
            }
            definition.insert("id".into(), Value::Str(identity.to_string()));
        } else {
            definition.insert("id".into(), Value::Str(uuid::Uuid::new_v4().to_string()));
        }
    }
    // Members the caller cannot see stay (it could not have meant to drop them); a new member
    // must be one it can see.
    let registry = conn.connector_keys();
    let mut filter = RefFilter::new(&ctx.state, &caller, &registry);
    let old_targets = old
        .get("pipeline")
        .map(|d| targets_of(&from_core(d)))
        .unwrap_or_default();
    let mut hidden_old = Vec::new();
    for target in &old_targets {
        if !filter.visible(target).await {
            hidden_old.push(target.clone());
        }
    }
    check_new_members(&mut filter, new.get("pipeline"), &old_targets).await?;
    if !hidden_old.is_empty()
        && let Some(Value::Map(definition)) = new.get_mut("pipeline")
    {
        let mut targets: Vec<Value> = match definition.get("targets") {
            Some(Value::List(items)) => items.clone(),
            _ => Vec::new(),
        };
        for target in hidden_old {
            if !targets.iter().any(|t| t.py_str() == target) {
                targets.push(Value::Str(target));
            }
        }
        definition.insert("targets".into(), Value::List(targets));
    }
    pipe.set_parameters(new);
    let res = conn.edit_pipe(&pipe, false).await;
    ctx.state.access_cache().forget_pipes();
    envelope::tuple(&res, 500)
}

#[derive(serde::Deserialize)]
struct RenameBody {
    new_name: String,
    #[serde(default)]
    expected_revision: Option<i64>,
    #[serde(default)]
    expected_id: Option<String>,
}

/// Transfer only a live authorized registration; old UUIDs cannot be supplied for creation.
pub(super) async fn rename(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller = access::require_fresh(
        &ctx.state,
        &ctx.headers,
        &[
            "pipes:read",
            "pipes:write",
            "pipes:register",
            "pipes:delete",
        ],
    )
    .await?;
    validate_name(&name)?;
    if !ctx.state.config.registration_pipes {
        return Err(ApiError::new(403, "Pipe registration is disabled."));
    }
    let request: RenameBody = serde_json::from_slice(
        &body_bytes(body, ctx.state.config.limits.body).await?,
    )
    .map_err(|_| {
        ApiError::new(
            400,
            "Expected a destination name and optional source identity/revision.",
        )
    })?;
    validate_name(&request.new_name)?;
    let conn = connector(&ctx)?;
    access::guard::check_connector_teams(&ctx.state, &caller, &conn.connector_keys())?;
    let source = metadata(&ctx, &conn, &name)?;
    let expected = conn
        .get_pipe_attributes(&source)
        .await
        .map_err(|_| ApiError::new(503, "Cannot read Pipeline registration."))?;
    let parameters = crate::state::parameters_of(&expected)
        .filter(|parameters| caller.can_see(parameters))
        .ok_or_else(|| ApiError::new(404, "Pipeline was not found."))?;
    if !caller.can_edit(&parameters) {
        return Err(ApiError::new(403, "You cannot rename this Pipeline."));
    }
    for operation in [
        meerschaum_core::access::LockOp::EditParameters,
        meerschaum_core::access::LockOp::DeleteAndDrop,
    ] {
        if let Some(refusal) =
            meerschaum_connector::locks::refusal_of(&source, Some(&parameters), operation)
        {
            return Err(ApiError::new(403, refusal.message));
        }
    }
    let Some(Value::Map(current)) = parameters.get("pipeline") else {
        return Err(ApiError::new(409, "Pipeline definition is unavailable."));
    };
    let old_id = current.get("id").and_then(Value::as_str);
    let established =
        old_id.filter(|identity| !identity.is_empty() && !identity.starts_with("legacy:"));
    if request.expected_id.as_deref().is_some_and(|identity| {
        established.is_some_and(|current| identity != current)
            || (established.is_none()
                && !identity.starts_with("legacy:")
                && old_id != Some(identity))
    }) {
        return Err(ApiError::new(
            409,
            "Pipeline identity changed; reload before renaming.",
        ));
    }
    let revision = current.get("revision").and_then(Value::as_i64).unwrap_or(0);
    if request
        .expected_revision
        .is_some_and(|expected| expected != revision)
    {
        return Err(ApiError::new(
            409,
            "Pipeline revision changed; reload before renaming.",
        ));
    }
    let (jobs, hidden) = referencing_jobs(&ctx, &conn, &name, &caller).await?;
    if !jobs.is_empty() || hidden > 0 {
        return Err(ApiError::new(
            409,
            "Remove or reassign referencing scheduled Jobs before renaming.",
        ));
    }
    if request.new_name == name {
        let visible = visible_definition(
            &mut RefFilter::new(&ctx.state, &caller, &conn.connector_keys()),
            from_core(&Value::Map(current.clone())),
        )
        .await;
        return Ok(envelope::ok_with(
            "Pipeline name is unchanged.",
            "pipeline",
            visible,
        ));
    }
    let mut updated = parameters.clone();
    let Some(Value::Map(definition)) = updated.get_mut("pipeline") else {
        unreachable!("checked");
    };
    definition.insert("name".into(), Value::Str(request.new_name.clone()));
    definition.insert("registry".into(), Value::Str(conn.connector_keys()));
    definition.insert(
        "revision".into(),
        Value::Int(
            revision
                .checked_add(1)
                .ok_or_else(|| ApiError::new(409, "Pipeline revision is out of range."))?,
        ),
    );
    definition.insert(
        "id".into(),
        Value::Str(
            established
                .map(str::to_string)
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        ),
    );
    if !conn
        .move_pipeline_registration(&source, &request.new_name, &expected, &updated)
        .await
        .map_err(|_| ApiError::new(503, "Cannot atomically rename this Pipeline."))?
    {
        return Err(ApiError::new(
            409,
            "Pipeline source changed or destination already exists.",
        ));
    }
    ctx.state.access_cache().forget_pipes();
    let definition = updated.get("pipeline").map(from_core).unwrap_or(Json::Null);
    let visible = visible_definition(
        &mut RefFilter::new(&ctx.state, &caller, &conn.connector_keys()),
        definition,
    )
    .await;
    Ok(envelope::ok_with(
        "Renamed the live Pipeline registration.",
        "pipeline",
        visible,
    ))
}

/// The scheduled jobs that run Pipeline `name`: the ones `caller` may see (sorted), and how many
/// others there are (a job of another team is only counted).
async fn referencing_jobs(
    ctx: &Ctx,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
    caller: &Access,
) -> Result<(Vec<String>, usize), ApiError> {
    let mut found = Vec::new();
    let mut hidden = 0;
    // Only the commands and owners: a job's status would cost a `systemctl` query per job.
    for (job_name, meta) in ctx
        .state
        .jobs
        .try_list_definitions()
        .await
        .map_err(ApiError::unavailable)?
    {
        let Some(args) = meta.get("sysargs").and_then(Json::as_array) else {
            continue;
        };
        let args: Vec<_> = args.iter().filter_map(Json::as_str).collect();
        if !args
            .iter()
            .any(|arg| ["--schedule", "-s", "--loop"].contains(arg))
        {
            continue;
        }
        for step in args.split(|arg| ["+", ":"].contains(arg)) {
            if step.len() < 3 || step[0] != "sync" || !["pipeline", "pipelines"].contains(&step[1])
            {
                continue;
            }
            let registry = step
                .windows(2)
                .find(|pair| ["-i", "--instance", "--mrsm-instance"].contains(&pair[0]))
                .map(|pair| pair[1]);
            let targets = step[2..].iter().take_while(|arg| !arg.starts_with('-'));
            if targets.into_iter().any(|target| *target == name)
                && registry.unwrap_or(&ctx.state.config.instance_keys) == conn.connector_keys()
            {
                if super::jobs::can_see(caller, &job_name, Some(&meta)) {
                    found.push(job_name.clone());
                } else {
                    hidden += 1;
                }
                break;
            }
        }
    }
    found.sort();
    found.dedup();
    Ok((found, hidden))
}

/// `a, b and 2 hidden jobs` (the jobs the caller may not see are only counted).
fn jobs_label(visible: &[String], hidden: usize) -> String {
    let mut parts: Vec<String> = visible.to_vec();
    if hidden > 0 {
        parts.push(format!(
            "{hidden} hidden job{}",
            if hidden == 1 { "" } else { "s" }
        ));
    }
    match parts.split_last() {
        Some((last, rest)) if !rest.is_empty() => format!("{} and {last}", rest.join(", ")),
        Some((last, _)) => last.clone(),
        None => String::new(),
    }
}

pub(super) async fn delete(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:delete"]).await?;
    let conn = connector(&ctx)?;
    let pipe = metadata(&ctx, &conn, &name)?;
    let parameters = registration_parameters(&ctx.state, &conn, &name)
        .await?
        .filter(|p| caller.can_see(p))
        .ok_or_else(|| ApiError::new(404, format!("Pipeline '{name}' is not registered.")))?;
    if !caller.can_edit(&parameters) {
        return Err(ApiError::new(
            403,
            format!(
                "You cannot delete Pipeline '{name}': only its owner, its team's managers and administrators may."
            ),
        ));
    }
    let (jobs, hidden_jobs) = referencing_jobs(&ctx, &conn, &name, &caller).await?;
    if !jobs.is_empty() || hidden_jobs > 0 {
        return Err(ApiError::new(
            409,
            format!(
                "Remove or reassign the scheduled jobs that run this Pipeline first: {}.",
                jobs_label(&jobs, hidden_jobs)
            ),
        ));
    }
    envelope::tuple(&conn.delete_pipe(&pipe).await, 500)
}

pub(super) async fn id(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let pipe = metadata(&ctx, &conn, &name)?;
    visible_registration(&ctx.state, &caller, &conn, &name).await?;
    let id = conn
        .get_pipe_id(&pipe)
        .await
        .map_err(|e| failed("get_pipe_id", e))?
        .ok_or_else(|| ApiError::new(404, format!("Pipeline '{name}' is not registered.")))?;
    Ok(envelope::ok_with(
        format!("Pipeline '{name}' is registered."),
        "id",
        from_core(&id.to_value()),
    ))
}

pub(super) async fn attributes(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let pipe = metadata(&ctx, &conn, &name)?;
    let (definition, _) = visible_registration(&ctx.state, &caller, &conn, &name).await?;
    let mut attrs = crate::state::pipe_attributes(&conn, &pipe).await?;
    // Members follow the caller's access; the declared registry is authoritative for admin
    // transport too, so legacy user-supplied aliases never rebind a foreign member.
    let registry = conn.connector_keys();
    let visible = visible_definition(
        &mut RefFilter::new(&ctx.state, &caller, &registry),
        definition,
    )
    .await;
    if let Some(Value::Map(parameters)) = attrs.get_mut("parameters") {
        parameters.insert("pipeline".into(), crate::json::to_core(&visible));
    }
    // The jobs the caller may see by name; the others only counted, as one last entry (so a
    // client that refuses to rename a Pipeline some job runs still refuses) and in
    // `pipeline_hidden_referencing_jobs`.
    let (mut jobs, hidden_jobs) = referencing_jobs(&ctx, &conn, &name, &caller).await?;
    if hidden_jobs > 0 {
        jobs.push(jobs_label(&[], hidden_jobs));
    }
    attrs.insert(
        "pipeline_referencing_jobs".into(),
        Value::List(jobs.into_iter().map(Value::Str).collect()),
    );
    attrs.insert(
        "pipeline_hidden_referencing_jobs".into(),
        Value::Int(hidden_jobs as i64),
    );
    Ok(envelope::ok_with(
        format!("The attributes of Pipeline '{name}'."),
        "attributes",
        from_core(&Value::Map(attrs)),
    ))
}
