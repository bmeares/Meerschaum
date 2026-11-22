//! Start a Pipeline run as a background job (`POST /pipelines/{name}/sync`) once the caller may
//! edit every pipe it would sync.

use super::*;

/// A new run ID for a Pipeline run started over HTTP: when and how it started
/// (`2026-10-27T05-00-00Z-api-1a2b3c4d5e6f`).
pub fn new_run_id() -> String {
    format!(
        "{}-api-{}",
        chrono::Utc::now().format("%Y-%m-%dT%H-%M-%SZ"),
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

/// Whether a caller may run Pipelines: administrators always; others when the server lets
/// non-administrators run actions (`api:permissions:actions:non_admin`, off under `--secure`)
/// and every instance the run touches is one they may use.
async fn may_run(
    state: &AppState,
    principal: Option<&Principal>,
    instances: &[String],
) -> Result<(), ApiError> {
    if is_admin(state, principal).await {
        return Ok(());
    }
    if !state.config.actions_non_admin {
        return Err(ApiError::new(
            403,
            "The administrator for this server has not allowed users to run Pipelines.",
        ));
    }
    for keys in instances {
        state.get_api_connector(Some(keys)).map_err(|e| {
            ApiError::new(
                403,
                format!(
                    "Cannot run this Pipeline: its pipes on '{keys}' are not available to you ({}).",
                    match &e.detail {
                        Some(Json::String(s)) => s.clone(),
                        _ => "not allowed".into(),
                    }
                ),
            )
        })?;
    }
    Ok(())
}

/// Refuse a run of the Pipeline `name` that would sync a pipe `caller` may not change (a pipe
/// locked against syncs is skipped by the engine and needs no rights): the plan's nodes, or the
/// members when this server cannot plan.
pub async fn check_members_editable(
    state: &AppState,
    caller: &Access,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
    definition: &Json,
    options: &PlanOptions,
) -> Result<(), ApiError> {
    if caller.no_auth || caller.is_admin() {
        return Ok(());
    }
    let registry = conn.connector_keys();
    let refs: Vec<String> = match state.pipelines.plan(name, &registry, options).await {
        Ok(planned) => planned
            .get("plan")
            .and_then(|p| p.get("nodes"))
            .and_then(Json::as_array)
            .map(|nodes| {
                nodes
                    .iter()
                    .filter_map(|n| n.get("pipe_ref").and_then(Json::as_str))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        // Without a plan (this server cannot plan, or the plan fails, as the run then will):
        // the members.
        Err(_) => targets_of(definition),
    };
    let mut filter = RefFilter::new(state, caller, &registry);
    let mut refused = 0usize;
    for pipe_ref in &refs {
        let (editable, sync_locked) = filter.editable(pipe_ref).await;
        if !editable && !sync_locked {
            refused += 1;
        }
    }
    if refused > 0 {
        return Err(ApiError::new(
            403,
            format!(
                "You cannot run Pipeline '{name}': you may not change {} it would sync.",
                plural(refused, "pipe", "of the pipes")
            ),
        ));
    }
    Ok(())
}

/// What [`start_sync`] started.
#[derive(Debug, Clone)]
pub struct StartedSync {
    /// The run's ID.
    pub run_id: String,
    /// The background job that runs it.
    pub job: String,
    /// The fields of the response (`run_id`, `job`, `stream`, `log_stream`, `logs`).
    pub fields: JsonMap<String, Json>,
    /// The job's start message.
    pub message: String,
}

async fn launch_reserved_run(
    state: AppState,
    principal: Option<Principal>,
    conn: Arc<dyn InstanceConnector>,
    run_id: String,
    job: String,
    sysargs: Vec<String>,
    metadata: Json,
) -> Result<meerschaum_core::SuccessTuple, ApiError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let unavailable = || ApiError::new(503, "Timed out reserving the Pipeline run.");
    let store = store_of(&state, &conn);
    let resource = format!("{}|mrsm|pipeline_launch|{run_id}", conn.connector_keys());
    let holder = uuid::Uuid::new_v4().to_string();
    if !tokio::time::timeout_at(
        deadline,
        store.acquire_lease(&resource, &holder, Duration::from_secs(65)),
    )
    .await
    .map_err(|_| unavailable())?
    {
        return Err(ApiError::new(
            409,
            "The Pipeline run ID is already reserved.",
        ));
    }
    let result = tokio::time::timeout_at(deadline, async {
        let existing = store
            .get_runs(&crate::backends::RunsFilter {
                run_id: Some(run_id),
                limit: Some(1),
                ..Default::default()
            })
            .await
            .map_err(crate::engine_stream::api_error)?;
        if !existing.is_empty() || state.jobs.exists(&job).await {
            return Err(ApiError::new(409, "The Pipeline run ID already exists."));
        }
        super::execute::start_execution(&state, principal.as_ref(), &job, &sysargs, metadata).await
    })
    .await
    .map_err(|_| unavailable());
    let _ = tokio::time::timeout(
        Duration::from_secs(3),
        store.release_lease(&resource, &holder),
    )
    .await;
    result?
}

/// Start a run of the saved Pipeline `name` (on the registry `conn`) as a background job, the
/// way the console does (`sync pipelines NAME -i REGISTRY --run-id RUN_ID`, a temporary
/// execution owned by the caller). `POST /pipelines/{name}/sync` and the MCP `sync_pipeline`
/// tool share it.
pub async fn start_sync(
    state: &AppState,
    principal: Option<&Principal>,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
    definition: &Json,
    options: &PlanOptions,
    run_id: Option<String>,
) -> Result<StartedSync, ApiError> {
    // REST and the HTTP MCP transport install the original request headers. Stdio
    // and in-process callers keep their local execution context.
    let original = match crate::executor::context::request_headers() {
        Some(headers) if !state.config.no_auth => {
            let initial = crate::executor::context::current().ok_or_else(|| {
                ApiError::new(403, "The original Pipeline HTTP caller is unavailable.")
            })?;
            if initial.full_admin() {
                None
            } else {
                let source = crate::executor::plugin_caller::RequestSource::resolve(
                    state,
                    &headers,
                    &["pipes:write", "jobs:execute"],
                )
                .await?;
                crate::executor::context::check_same_action_access(&initial, &source.access())?;
                Some(Arc::new(source))
            }
        }
        _ => None,
    };
    let registry = conn.connector_keys();
    let mut instances = vec![registry.clone()];
    for reference in targets_of(definition) {
        if let Some(instance) = reference.split('|').next()
            && !instances.iter().any(|i| i == instance)
        {
            instances.push(instance.to_string());
        }
    }
    may_run(state, principal, &instances).await?;
    let caller = match &original {
        Some(source) => source.access(),
        None => Arc::new(super::jobs::access_of_principal(state, principal).await?),
    };
    check_members_editable(state, &caller, conn, name, definition, options).await?;
    let run_id = match run_id {
        Some(id) if crate::engine_stream::valid_run_id(&id) => id,
        Some(_) => {
            return Err(ApiError::new(
                400,
                "`run_id` must be up to 200 letters, digits, '-', '_', '.' or ':', starting with a letter or digit.",
            ));
        }
        None => new_run_id(),
    };
    let mut sysargs: Vec<String> = [
        "sync",
        "pipelines",
        name,
        "-i",
        &registry,
        "--run-id",
        &run_id,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    for (flag, value) in [("--begin", &options.begin), ("--end", &options.end)] {
        if let Some(value) = value {
            sysargs.push(flag.into());
            sysargs.push(value.clone());
        }
    }
    // Job metadata is the durable pre-header reservation. Its deterministic name plus
    // the launch lease keeps two requests/workers from accepting the same run ID.
    use sha2::Digest;
    let digest = sha2::Sha256::digest(format!("{registry}|{run_id}"));
    let suffix: String = digest
        .iter()
        .take(12)
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let job = format!("execute-{suffix}");
    let metadata = json!({
        "origin": "api_pipeline_sync",
        "run_id": run_id,
        "pipeline": {
            "name": name,
            "registry": registry,
            "id": definition.get("id"),
            "revision": definition.get("revision"),
        },
    });
    // Once accepted, finish the bounded launch even if its HTTP request is cancelled.
    // No provisional running history is written: SSE waits for the runner's trusted header.
    let request = original.map(|source| {
        Arc::new(crate::executor::pipe_operation::PipelineRunRequest {
            source,
            job: job.clone(),
            words: sysargs.clone(),
            registry: registry.clone(),
            name: name.into(),
            definition: definition.clone(),
            run_id: run_id.clone(),
            begin: options.begin.clone(),
            end: options.end.clone(),
        })
    });
    let started = tokio::spawn(crate::executor::context::scope(
        crate::executor::context::current(),
        crate::executor::gateway::scope(
            crate::executor::gateway::current(),
            crate::executor::context::pipeline_request_scope(
                request,
                launch_reserved_run(
                    state.clone(),
                    principal.cloned(),
                    conn.clone(),
                    run_id.clone(),
                    job.clone(),
                    sysargs,
                    metadata,
                ),
            ),
        ),
    ))
    .await
    .map_err(|_| ApiError::new(503, "The Pipeline launch was interrupted."))??;
    if !started.success {
        return Err(ApiError::new(500, started.message));
    }
    let mut fields = JsonMap::new();
    fields.insert("run_id".into(), json!(run_id));
    fields.insert("job".into(), json!(job));
    // The run's events (Server-Sent Events), when the `engine` group is served.
    let stream = if state.config.route_group_is_allowed("engine") {
        let wait = RUN_STREAM_WAIT_SECONDS.to_string();
        let own = crate::engine_stream::own_instance_names(state).contains(&registry);
        let mut query = vec![("wait", wait.as_str())];
        if !own {
            query.push(("instance_keys", registry.as_str()));
        }
        json!(format!(
            "/engine/runs/{}/events/stream?{}",
            crate::json::quote_segment(&run_id),
            crate::json::query_string(&query)
        ))
    } else {
        Json::Null
    };
    fields.insert("stream".into(), stream);
    let logs = super::execute::log_routes(state, &job);
    fields.insert(
        "log_stream".into(),
        logs.get("stream").cloned().unwrap_or(Json::Null),
    );
    fields.insert(
        "logs".into(),
        logs.get("logs").cloned().unwrap_or(Json::Null),
    );
    Ok(StartedSync {
        run_id,
        job,
        fields,
        message: started.message,
    })
}

/// `POST /pipelines/{name}/sync`: start a run in the background.
pub(super) async fn sync(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller =
        access::require(&ctx.state, &ctx.headers, &["pipes:write", "jobs:execute"]).await?;
    let principal = caller.auth.clone();
    let body = optional_body(body, ctx.state.config.limits.body).await?;
    let options = bounds_of(&body)?;
    let run_id = match body.get("run_id") {
        None | Some(Json::Null) => None,
        Some(Json::String(s)) => Some(s.clone()),
        Some(_) => return Err(ApiError::new(400, "`run_id` must be a string.")),
    };
    let conn = connector(&ctx)?;
    let (definition, _) = visible_registration(&ctx.state, &caller, &conn, &name).await?;
    let started = crate::executor::context::request_headers_scope(
        ctx.headers,
        crate::executor::context::scope(
            Some(Arc::new(caller)),
            start_sync(
                &ctx.state,
                principal.as_ref(),
                &conn,
                &name,
                &definition,
                &options,
                run_id,
            ),
        ),
    )
    .await?;
    Ok(envelope::ok(
        format!(
            "Started Pipeline '{name}' (run '{}') as job '{}'.",
            started.run_id, started.job
        ),
        started.fields,
    ))
}
