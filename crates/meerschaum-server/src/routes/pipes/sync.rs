//! Sync time and `POST .../data`: parse the body, then sync it under a data admission that
//! keeps the request's credential and rechecks the destination before every write (also used by MCP
//! document writes and replacements).

use super::*;

pub(super) async fn get_sync_time(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, &[]).await?;
    let body = parse_json_body(&body_bytes(body, ctx.state.config.limits.body).await?)?;
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let params = optional_dict_body(body)?;
    let newest = ctx.query.bool("newest", true)?;
    let _remote = ctx.query.bool("remote", false)?;
    let round_down = ctx.query.bool("round_down", true)?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let query = DataQuery {
        params: params.clone(),
        ..Default::default()
    };
    let window = original.check(&ctx, &h.pipe, &query).await?;
    let st = pipe_methods::get_sync_time(
        &conn,
        &h.pipe,
        params.as_ref(),
        newest,
        round_down,
        &ctx.state.config.pipe_defaults,
    )
    .await?;
    original.finish(&ctx, &h.pipe, &query, &window).await?;
    Ok(ok_json(&from_core(&bound_json(&st))))
}

/// Parse the body of `POST .../data` like the Python route: JSON, else UTF-8 text of
/// `string_to_dict` lines.
pub(super) fn parse_sync_body(body: &[u8]) -> Result<Option<SyncPayload>, ApiError> {
    let data = match serde_json::from_slice::<Json>(body) {
        Ok(j) => {
            let empty = match &j {
                Json::Null => true,
                Json::Bool(b) => !b,
                Json::Number(n) => n.as_f64() == Some(0.0),
                Json::String(s) => s.is_empty(),
                Json::Array(a) => a.is_empty(),
                Json::Object(o) => o.is_empty(),
            };
            if empty {
                return Ok(None);
            }
            match j {
                Json::String(s) => SyncText(s),
                other => return Ok(Some(SyncPayload::Json(other))),
            }
        }
        Err(_) => SyncText(String::from_utf8_lossy(body).into_owned()),
    };
    let SyncText(text) = data;
    if text.is_empty() {
        return Ok(None);
    }
    // PARITY: `not data.lstrip()[0] not in ('{', '[')` is always true for a non-blank string,
    // so every string is parsed as simple-dict lines (`api/routes/_pipes.py:411`).
    if text.trim().is_empty() {
        // PARITY: `HTTPException(status=400, ...)` is a `TypeError`, a 500.
        return Err(ApiError::internal());
    }
    let mut docs = Vec::new();
    for line in meerschaum_core::pyfmt::py_splitlines(&text, false) {
        match meerschaum_core::pyfmt::string_to_dict(line) {
            Ok(d) => docs.push(d),
            Err(_) => return Err(ApiError::internal()),
        }
    }
    if docs.is_empty() {
        return Err(ApiError::internal());
    }
    Ok(Some(SyncPayload::Docs(docs)))
}

struct SyncText(String);

/// Data POST admission retains the request's credential, never an ambient task identity.
/// Native preparation can await metadata or retries; each subsequent write rechecks the
/// original credential and the stored pipe instead of the earlier route snapshot.
struct DataAdmission {
    batch: Option<Arc<super::graph_batch::WriteBatch>>,
    state: AppState,
    headers: axum::http::HeaderMap,
    columns: Option<Value>,
    replace: bool,
    registration: std::sync::Mutex<Option<String>>,
    reference: String,
    logical_reference: String,
    native_alias: Option<super::engine::selection::SelectedEngine>,
    innodb_user_service: bool,
    direct_administrator: bool,
    graph_target: std::sync::Mutex<
        Option<(
            crate::executor::pipe_operation::CallbackBinding,
            meerschaum_connector::mutation::NativeTarget,
        )>,
    >,
    parameters: std::sync::Mutex<Option<Map>>,
    inference_failure: std::sync::Mutex<Option<String>>,
    snapshot_safe: std::sync::atomic::AtomicBool,
}

/// Native MCP document writes retain the same original credential and inference authority
/// as the data POST route, including after database lock waits.
pub(crate) fn document_admission(
    state: &AppState,
    headers: axum::http::HeaderMap,
    pipe: &Pipe,
) -> Arc<dyn pipe_methods::SyncAdmission> {
    captured_data_admission(state, headers, pipe, false, None)
}
pub(in crate::routes) fn replacement_admission(
    state: &AppState,
    headers: axum::http::HeaderMap,
    pipe: &Pipe,
    batch: Option<Arc<super::graph_batch::WriteBatch>>,
) -> Arc<dyn pipe_methods::SyncAdmission> {
    captured_data_admission(state, headers, pipe, true, batch)
}
fn captured_data_admission(
    state: &AppState,
    headers: axum::http::HeaderMap,
    pipe: &Pipe,
    replace: bool,
    batch: Option<Arc<super::graph_batch::WriteBatch>>,
) -> Arc<dyn pipe_methods::SyncAdmission> {
    Arc::new(DataAdmission {
        batch,
        state: state.clone(),
        headers,
        columns: None,
        replace,
        registration: Default::default(),
        reference: pipe.pipe_ref(),
        logical_reference: pipe.pipe_ref(),
        native_alias: None,
        innodb_user_service: false,
        direct_administrator: false,
        graph_target: Default::default(),
        parameters: Default::default(),
        inference_failure: Default::default(),
        snapshot_safe: Default::default(),
    })
}

/// Shared synchronous data-destination guards. The caller and metadata must have been
/// refreshed after the last preparation await; this helper performs no canonical reads.
pub(crate) fn check_data_destination(
    caller: &access::InstanceAccess<'_>,
    conn: &dyn InstanceConnector,
    pipe: &Pipe,
    hidden: &crate::sql_guard::HiddenTables,
) -> Result<(), ApiError> {
    if !caller.can_see_pipe(pipe) {
        return Err(access::hidden_pipe(pipe));
    }
    if !caller.can_edit(pipe.raw_parameters()) {
        return Err(access::not_editable(pipe));
    }
    access::guard::check_pipe_target(conn, pipe, hidden)?;
    let table = target(pipe);
    if meerschaum_connector_api::routes::PROTECTED_TARGETS.contains(&table.as_str()) {
        return Err(ApiError::new(
            409,
            format!("Cannot sync data to protected table '{table}'."),
        ));
    }
    Ok(())
}

impl DataAdmission {
    fn check_administrator(&self, caller: &Access) -> Result<(), ApiError> {
        if self.direct_administrator && !mutation::direct_administrator(caller) {
            return Err(ApiError::new(
                403,
                "The original administrator authority changed.",
            ));
        }
        Ok(())
    }

    async fn canonical(
        &self,
        conn: &Arc<dyn InstanceConnector>,
        requested: &Pipe,
    ) -> Result<Pipe, ApiError> {
        if let Some(reason) = self
            .inference_failure
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
        {
            return Err(ApiError::new(409, reason));
        }
        if let Some(alias) = &self.native_alias {
            if !Arc::ptr_eq(conn, &alias.connector()) || requested.pipe_ref() != self.reference {
                return Err(ApiError::new(
                    409,
                    "The selected native data target changed.",
                ));
            }
            let source = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
            let graph = self
                .state
                .actions
                .pipe_operation_read(&source)
                .await
                .map_err(|reason| ApiError::new(409, reason))?
                .is_some();
            let pipe = if graph {
                alias
                    .native_graph_target(&self.state, &self.headers, &self.logical_reference)
                    .await?
                    .0
            } else {
                alias
                    .native_data_pipe(&self.state, &self.headers, &self.logical_reference)
                    .await?
            };
            let registration = pipe.registration_id();
            let parameters = pipe.raw_parameters().clone();
            {
                let mut expected = self
                    .registration
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if expected.is_some() && *expected != registration {
                    return Err(ApiError::new(409, "The logical data registration changed."));
                }
                *expected = registration;
            }
            {
                let mut expected = self
                    .parameters
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if expected
                    .as_ref()
                    .is_some_and(|accepted| accepted != &parameters)
                {
                    return Err(ApiError::new(
                        409,
                        "The logical data definition changed during preparation.",
                    ));
                }
                *expected = Some(parameters);
            }
            return Ok(pipe);
        }
        if conn.connector_type() == "api" {
            return Err(ApiError::new(
                501,
                "Scoped API data writes require a bound native or forwarding mutation capability.",
            ));
        }
        self.state
            .get_api_connector(Some(requested.instance_keys()))?;
        let source = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
        if matches!(&source.auth, Some(crate::auth::Principal::Execution(identity)) if identity.grant.required.contains("pipes:write"))
        {
            // A typed operation must not use a peer-edited definition before the native
            // inference receipt has advanced its exact canonical snapshot.
            self.state
                .actions
                .pipe_operation_read(&source)
                .await
                .map_err(|reason| ApiError::new(409, reason))?;
        }
        let attributes = conn
            .get_pipe_attributes(requested)
            .await
            .map_err(|e| ApiError::exception("Failed to load a pipe's attributes", e))?;
        let registered = !attributes.is_empty();
        let stored_parameters = crate::state::parameters_of(&attributes);
        // The hidden-target index and source permissions may have changed while metadata
        // was prepared. Refresh both; a failed read is not default public access.
        let dict = self
            .state
            .pipes(Some(requested.instance_keys()), true)
            .await?;
        let caller = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
        self.check_administrator(&caller)?;
        access::guard::check_connector_teams(&self.state, &caller, requested.instance_keys())?;
        let mut pipe = requested.clone();
        pipe.temporary = false;
        pipe.set_parameters(if registered {
            stored_parameters.clone().ok_or_else(|| {
                ApiError::new(503, "The pipe's registered parameters are unavailable.")
            })?
        } else {
            // The data route accepts columns, not caller-selected access or SQL definitions.
            Map::new()
        });
        if pipe.columns().is_empty()
            && let Some(columns) = &self.columns
        {
            pipe.set_columns(columns.clone())
                .map_err(|e| ApiError::exception("pipe.columns", e))?;
        }
        if !registered {
            caller.require(&["pipes:register"], &[])?;
            if !self.state.config.registration_pipes {
                return Err(ApiError::new(
                    403,
                    "The server has not allowed pipe registration.",
                ));
            }
            access::guard::check_source_connector(&self.state, &caller, pipe.connector_keys())?;
            let stamped = access::guard::stamp_new_pipe(&caller, pipe.raw_parameters())?;
            access::guard::check_parameters(&self.state, &caller, conn, &pipe, None, &stamped)
                .await?;
            pipe.set_parameters(stamped);
        }
        // Resolve authority again after parameter/template checks, which can themselves await.
        let caller = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
        self.check_administrator(&caller)?;
        access::guard::check_connector_teams(&self.state, &caller, pipe.instance_keys())?;
        if !registered {
            caller.require(&["pipes:register"], &[])?;
            access::guard::check_source_connector(&self.state, &caller, pipe.connector_keys())?;
            // No authority from the first read may set the new registration's identity.
            let mut parameters = pipe.raw_parameters().clone();
            parameters.shift_remove("access");
            pipe.set_parameters(access::guard::stamp_new_pipe(&caller, &parameters)?);
            let team = access::rules::PipeAccess::from_parameters(pipe.raw_parameters()).team;
            access::guard::check_pipe_connector(
                &self.state,
                &caller,
                pipe.connector_keys(),
                team.as_deref(),
            )?;
        }
        let index = access::index_from_dict(pipe.instance_keys(), &dict, conn.as_ref());
        let access = caller.for_instance(&self.state, pipe.instance_keys());
        check_data_destination(
            &access,
            conn.as_ref(),
            &pipe,
            &index.hidden_targets_for(&access),
        )?;
        // Check metadata drift only after verifying the current caller can see the target.
        if registered {
            let mut expected = self
                .parameters
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if expected.is_some() && *expected != stored_parameters {
                return Err(ApiError::new(
                    409,
                    "The pipe definition changed during this data request; retry with fresh metadata.",
                ));
            }
            *expected = stored_parameters.clone();
        }
        let registration = attributes
            .get("pipe_id")
            .and_then(|identity| match identity {
                Value::Int(_) | Value::Str(_) => Some(identity.py_str()),
                _ => None,
            });
        {
            let mut expected = self.registration.lock().unwrap_or_else(|e| e.into_inner());
            if expected.is_some() && *expected != registration {
                return Err(ApiError::new(
                    409,
                    "The pipe's registration changed during the sync; retry the request.",
                ));
            }
            if registered {
                *expected = registration;
            }
        }
        Ok(pipe)
    }
}

#[async_trait::async_trait]
impl pipe_methods::SyncAdmission for DataAdmission {
    fn direct_administrator(&self) -> bool {
        self.direct_administrator
    }

    fn native_batch(&self) -> Option<meerschaum_connector::mutation::NativeBatch> {
        self.batch.as_ref().map(|batch| batch.native.clone())
    }
    async fn begin(&self, conn: &Arc<dyn InstanceConnector>, pipe: &Pipe) -> Result<(), ApiError> {
        self.canonical(conn, pipe).await?;
        if let Some(batch) = &self.batch {
            return batch.confirm(&self.state).await;
        }
        let source = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
        if self
            .state
            .actions
            .pipe_operation_read(&source)
            .await
            .map_err(|reason| ApiError::new(409, reason))?
            .is_some()
        {
            return Err(ApiError::new(
                409,
                "A captured graph mutation requires its Main-issued native batch claim.",
            ));
        }
        Ok(())
    }
    async fn write_window(
        &self,
        conn: &Arc<dyn InstanceConnector>,
        pipe: &Pipe,
    ) -> Result<Option<meerschaum_connector::mutation::DataWindow>, ApiError> {
        let canonical = self.canonical(conn, pipe).await?;
        let source = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
        let Some(read) = self
            .state
            .actions
            .pipe_operation_read(&source)
            .await
            .map_err(|reason| ApiError::new(409, reason))?
        else {
            return Ok(None);
        };
        // Only a captured graph target can opt direct SQL into a held auth
        // snapshot, and it cannot target that snapshot's own auth/registration
        // storage through a logical or physical alias. The SQL native adapter
        // independently repeats this before binding the writer.
        access::guard::check_pipe_target(
            conn.as_ref(),
            &canonical,
            &crate::sql_guard::HiddenTables::default(),
        )?;
        self.snapshot_safe
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(alias) = &self.native_alias {
            if !Arc::ptr_eq(conn, &alias.connector()) {
                return Err(ApiError::new(409, "The native graph connector changed."));
            }
            let (physical, window) = alias
                .native_graph_target(&self.state, &self.headers, &self.logical_reference)
                .await?;
            if physical.pipe_ref() != canonical.pipe_ref()
                || physical.raw_parameters() != canonical.raw_parameters()
            {
                return Err(ApiError::new(
                    409,
                    "The native graph target changed after preparation.",
                ));
            }
            let target = window
                .native_target()
                .cloned()
                .ok_or_else(|| ApiError::new(409, "The native graph mapping disappeared."))?;
            let mut retained = self
                .graph_target
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if retained
                .as_ref()
                .is_some_and(|(binding, old)| binding != &read.binding || old != &target)
            {
                return Err(ApiError::new(
                    409,
                    "The retained native graph mapping changed.",
                ));
            }
            *retained = Some((read.binding, target));
            Ok(Some(window))
        } else {
            Ok(Some(read.write_window(&canonical)?))
        }
    }
    async fn prepare_data_commit(
        &self,
        conn: &Arc<dyn InstanceConnector>,
        pipe: &Pipe,
        effect: &meerschaum_connector::mutation::NativeDataEffect,
    ) -> Result<Option<Arc<dyn meerschaum_connector::mutation::PendingDataCommit>>, ApiError> {
        let window = self
            .write_window(conn, pipe)
            .await?
            .ok_or_else(|| ApiError::new(409, "The target commit lost its Main-issued window."))?;
        if &window != effect.window()
            || self.reference != effect.physical_reference()
            || self.logical_reference != effect.logical_reference()
        {
            return Err(ApiError::new(
                409,
                "The native target effect differs from this exact data request.",
            ));
        }
        let source = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
        self.state
            .actions
            .prepare_pipe_commit(&source, effect.clone())
            .await
            .map(Some)
            .map_err(|reason| ApiError::new(409, reason))
    }
    fn lock_snapshot_reads(&self) -> bool {
        self.native_alias.is_some()
            || self
                .snapshot_safe
                .load(std::sync::atomic::Ordering::Acquire)
    }
    fn same_store_innodb_reads(&self, conn: &Arc<dyn InstanceConnector>) -> bool {
        self.innodb_user_service
            && self.batch.is_some()
            && !self.state.config.no_auth
            && Arc::ptr_eq(conn, &self.state.instance())
    }
    fn tracks_metadata_inference(&self) -> bool {
        true
    }

    async fn metadata_inferred(
        &self,
        conn: &Arc<dyn InstanceConnector>,
        change: meerschaum_connector::mutation::MetadataInference,
    ) -> Result<(), ApiError> {
        let result = async {
            change
                .validate()
                .map_err(|reason| ApiError::new(409, reason))?;
            if change.pipe.pipe_ref() != self.reference
                || self
                    .registration
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .as_deref()
                    != Some(change.registration.py_str().as_str())
                || self
                    .parameters
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .as_ref()
                    != Some(&change.before)
            {
                return Err(ApiError::new(
                    409,
                    "Native inference lost the accepted data request's registration or parameters.",
                ));
            }
            let attributes = conn.get_pipe_attributes(&change.pipe).await.map_err(|_| {
                ApiError::unavailable("Could not confirm committed native inference.")
            })?;
            if attributes.get("pipe_id") != Some(&change.registration)
                || crate::state::parameters_of(&attributes).as_ref() != Some(&change.after)
            {
                return Err(ApiError::new(
                    409,
                    "The registered metadata differs from committed native inference.",
                ));
            }
            if let Some(alias) = &self.native_alias {
                let retained = self
                    .graph_target
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone();
                let pipe = if let Some((binding, target)) = retained {
                    let logical = alias
                        .native_graph_inference(
                            &self.state,
                            &self.headers,
                            &binding,
                            &target,
                            &change,
                        )
                        .await?;
                    let source =
                        access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
                    if !self
                        .state
                        .actions
                        .accept_pipe_inference(&source, logical)
                        .await
                        .map_err(|reason| ApiError::new(409, reason))?
                    {
                        return Err(ApiError::new(
                            409,
                            "The native graph inference was not accepted by its canonical issuer.",
                        ));
                    }
                    alias
                        .native_graph_target(&self.state, &self.headers, &self.logical_reference)
                        .await?
                        .0
                } else {
                    alias
                        .native_data_pipe(&self.state, &self.headers, &self.logical_reference)
                        .await?
                };
                if pipe.registration_id().as_deref() != Some(change.registration.py_str().as_str())
                    || pipe.raw_parameters() != &change.after
                {
                    return Err(ApiError::new(
                        409,
                        "The logical native inference registration changed.",
                    ));
                }
                let mut expected = self
                    .parameters
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if expected.as_ref() != Some(&change.before) {
                    return Err(ApiError::new(
                        409,
                        "Concurrent logical native inference changed this request.",
                    ));
                }
                *expected = Some(change.after);
                return Ok(());
            }
            let caller = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
            access::guard::check_parameters(
                &self.state,
                &caller,
                conn,
                &change.pipe,
                Some(&change.before),
                &change.after,
            )
            .await?;
            self.state
                .actions
                .accept_pipe_inference(&caller, change.clone())
                .await
                .map_err(|reason| ApiError::new(409, reason))?;
            access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
            let mut expected = self
                .parameters
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if expected.as_ref() != Some(&change.before) {
                return Err(ApiError::new(
                    409,
                    "Concurrent native inference changed this data request.",
                ));
            }
            *expected = Some(change.after);
            Ok(())
        }
        .await;
        if let Err(error) = &result {
            let reason = format!(
                "Native metadata inference committed before this data request was refused: {}. Its committed metadata was not rolled back. Start a fresh operation after resolving its original authorization or metadata conflict.",
                meerschaum_core::debug::redact_text(&error.detail_text())
                    .chars()
                    .take(512)
                    .collect::<String>()
            );
            *self
                .inference_failure
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(reason.clone());
            return Err(ApiError::new(409, reason));
        }
        result
    }

    async fn prepare(
        &self,
        conn: &Arc<dyn InstanceConnector>,
        pipe: &Pipe,
    ) -> Result<Pipe, ApiError> {
        let pipe = self.canonical(conn, pipe).await?;
        if let Some(error) = access::guard::lock_conflict(
            &pipe,
            if self.replace {
                LockOp::Replace
            } else {
                LockOp::Sync
            },
        ) {
            return Err(error);
        }
        Ok(pipe)
    }
}

pub(super) async fn sync_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    let batch_request = super::graph_batch::WriteRequest::capture(&ctx).await?;
    let caller = authorize(&ctx, PIPES_WRITE).await?;
    let q = &ctx.query;
    let check_existing = q.bool("check_existing", true)?;
    let blocking = q.bool("blocking", true)?;
    if batch_request.is_some() && !blocking {
        return Err(ApiError::new(
            400,
            "A native batch requires a synchronous write result.",
        ));
    }
    let force = q.bool("force", false)?;
    let workers = q.opt_int("workers")?;
    let columns = q.opt_str("columns");
    let special_dtypes =
        pipe_methods::special_input_hints(q.opt_str("new_special_cols").as_deref())?;
    // `replace=true`: the rows replace the window `[begin, end)` matching `params`; with no
    // rows the window is cleared. A bound that is not sent is the posted rows' own range (as for
    // `pipe.sync(df, replace=True)`); `None` (or empty) is no bound on that side.
    let replace = q.bool("replace", false)?;
    let window_bound = |key: &str| -> Result<WindowBound, ApiError> {
        match q.opt_str(key) {
            None => Ok(WindowBound::FromRows),
            Some(v) if v.is_empty() || v == "None" => Ok(WindowBound::Unbounded),
            Some(v) => DateBound::parse(&v)
                .map(WindowBound::At)
                .ok_or_else(|| ApiError::new(400, format!("Invalid `{key}`: '{v}'."))),
        }
    };
    let (window_begin, window_end) = (window_bound("begin")?, window_bound("end")?);
    let window_params = match q.opt_str("params").filter(|p| !p.is_empty()) {
        Some(p) => Some(
            Value::from_json_str(&p)
                .map_err(|e| ApiError::exception("json.loads(params)", e))?
                .to_map(),
        ),
        None => None,
    };
    let limit = ctx.state.config.limits.data;
    let limit = if batch_request.is_some() {
        let maximum = meerschaum_connector::mutation::MAX_NATIVE_BATCH_BODY;
        Some(limit.map_or(maximum, |limit| limit.min(maximum)))
    } else {
        limit
    };
    let body = body_bytes(body, limit).await?;
    let (payload, no_rows) = match parse_sync_body(&body)? {
        Some(payload) => (payload, false),
        None if replace => (SyncPayload::Json(Json::Array(Vec::new())), true),
        None if batch_request.is_some() => {
            return Err(ApiError::new(
                400,
                "A native data batch requires a target operation.",
            ));
        }
        None => return Ok(tuple(true, "No data to sync.")),
    };
    let selected = ctx.selected_engine(&caller)?;
    let graph = ctx
        .state
        .actions
        .pipe_operation_read(&caller)
        .await
        .map_err(|reason| ApiError::new(409, reason))?
        .is_some();
    if graph && batch_request.is_none() {
        return Err(ApiError::new(
            409,
            "A captured graph data write requires its Main-issued batch selector.",
        ));
    }
    if (selected.connector().connector_type() == "api" || selected.is_native_alias())
        && !(graph && selected.is_native_alias())
    {
        selected.require_native_write()?;
        selected.fresh(&ctx, PIPES_WRITE).await?;
    }
    let (configured, h) = handle(&ctx, &ck, &mk, &lk, true).await?;
    let logical_reference = h.pipe.pipe_ref();
    let (conn, requested, native_alias) = if configured.connector_type() == "api" {
        selected.require_native_write()?;
        let physical = if graph {
            selected
                .native_graph_target(&ctx.state, &ctx.headers, &logical_reference)
                .await?
                .0
        } else {
            selected
                .native_data_pipe(&ctx.state, &ctx.headers, &logical_reference)
                .await?
        };
        (selected.connector(), physical, Some(selected))
    } else {
        (configured, h.pipe.clone(), None)
    };
    let batch = match batch_request {
        Some(request) => Some(
            request
                .claim(&ctx, &logical_reference, "data", &body)
                .await?,
        ),
        None => None,
    };
    let admission = Arc::new(DataAdmission {
        batch: batch.clone(),
        state: ctx.state.clone(),
        headers: ctx.headers.clone(),
        columns: columns
            .map(|c| {
                Value::from_json_str(&c).map_err(|e| ApiError::exception("json.loads(columns)", e))
            })
            .transpose()?,
        replace,
        registration: Default::default(),
        reference: requested.pipe_ref(),
        logical_reference,
        innodb_user_service: native_alias
            .as_ref()
            .is_some_and(|alias| alias.has_password_service())
            && matches!(&caller.auth,
                Some(crate::auth::Principal::Execution(identity)) if identity.is_user_bound()),
        direct_administrator: !graph
            && batch.is_none()
            && native_alias.is_none()
            && mutation::direct_administrator(&caller),
        native_alias,
        graph_target: Default::default(),
        parameters: Default::default(),
        inference_failure: Default::default(),
        snapshot_safe: Default::default(),
    });
    let pipe = admission.canonical(&conn, &requested).await?;
    if let Some(e) = access::guard::lock_conflict(
        &pipe,
        if replace {
            LockOp::Replace
        } else {
            LockOp::Sync
        },
    ) {
        return Ok(tuple(false, e.detail_text()));
    }
    // Clearing a window without rows needs the window: `replace=true` alone with an empty body
    // must not clear the whole table (a pipe without a datetime column has only that window).
    if no_rows
        && window_begin == WindowBound::FromRows
        && window_end == WindowBound::FromRows
        && pipe.column("datetime").is_some()
    {
        return Err(ApiError::new(
            400,
            meerschaum_connector::replace::no_rows_window(&pipe),
        ));
    }
    let kw = SyncKwargs {
        special_dtypes,
        check_existing,
        force,
        workers,
        replace,
        begin: window_begin,
        end: window_end,
        params: window_params,
    };
    let defaults = ctx.state.config.pipe_defaults.clone();
    // The pipe's sync status records this sync (`routes::sync_status`).
    let (started, status_headers) = (std::time::Instant::now(), ctx.headers.clone());
    let registration = meerschaum_connector::sync_metrics::observed_registration(&pipe);
    if !blocking {
        let msg = format!("Spawned asyncronous sync for {pipe}.");
        tokio::spawn(async move {
            let res = match pipe_methods::sync_with_admission(
                &conn,
                &pipe,
                &payload,
                &kw,
                &defaults,
                Some(admission.clone()),
            )
            .await
            {
                Ok(result) => result,
                Err(error) => SuccessTuple::fail(error.detail_text()),
            };
            super::sync_status::record_data_post(
                &status_headers,
                &conn,
                &pipe,
                &res,
                started,
                registration,
            );
            if !res.success {
                tracing::warn!("Asynchronous sync failed for {pipe}:\n{}", res.message);
            }
        });
        // PARITY: the typo `asyncronous` is v4's (`core/Pipe/_sync.py:539`).
        return Ok(tuple(true, msg));
    }
    let res = pipe_methods::sync_with_admission(
        &conn,
        &pipe,
        &payload,
        &kw,
        &defaults,
        Some(admission.clone()),
    )
    .await?;
    h.invalidate();
    // Queued before the response (a delete that follows forgets it); written in the background.
    super::sync_status::record_data_post(
        &status_headers,
        &conn,
        &pipe,
        &res,
        started,
        registration,
    );
    if let Some(batch) = batch
        && res.success
    {
        let receipt = batch.receipt(&ctx.state).await?;
        #[derive(serde::Serialize)]
        struct BatchResult<'a> {
            result: (bool, &'a str),
            receipt: meerschaum_connector_api::graph_batch::BatchReceipt,
        }
        let mut writer = crate::json::Bounded {
            bytes: Vec::new(),
            limit: meerschaum_connector_api::graph_batch::MAX_BATCH_REPLY,
        };
        serde_json::to_writer(
            &mut writer,
            &BatchResult {
                result: (res.success, &res.message),
                receipt,
            },
        )
        .map_err(|_| {
            ApiError::unavailable("The committed batch result exceeds its delivery limit.")
        })?;
        return Ok(crate::json::raw_json(
            axum::http::StatusCode::OK,
            String::from_utf8(writer.bytes).map_err(|_| ApiError::internal())?,
        ));
    }
    Ok(tuple_response(&res))
}
