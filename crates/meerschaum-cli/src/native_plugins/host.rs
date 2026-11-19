//! `PluginHost` callback services for native Rust plugins.

use super::*;

/// A host call made for an API caller (a route, MCP entry or console page): it may do what the
/// caller's scopes allow, on the instances the server allows.
#[derive(Clone)]
pub(crate) struct ApiAccess {
    pub(super) state: meerschaum_server::AppState,
    /// The caller's granted scopes (`["*"]`: every scope).
    pub(super) scopes: Vec<String>,
    /// Scopes the route or page grants its host calls for every caller (`host_scopes`; never
    /// `*`).
    pub(super) host_scopes: Vec<String>,
    /// Who called (`None`: anonymous, or `--no-auth`).
    pub(super) principal: Option<meerschaum_server::auth::Principal>,
}

/// Serves a native plugin's host calls (`meerschaum_plugin::HostServices`) in one context: an
/// engine run (fetch, sync, hooks, streams), an action, or an API call.
#[derive(Clone)]
pub(crate) struct PluginHost {
    /// The engine run's registry (its connectors, and the native layer `pipe.sync` goes
    /// through).
    pub(super) registry: Option<NativeRegistry>,
    /// The process's connectors and config (from `registry` when unset).
    pub(super) env: Option<HostEnv>,
    /// The sync options `pipe.sync(...)` on the request's Pipe inherits.
    pub(super) args: SyncArgs,
    pub(super) api: Option<ApiAccess>,
    /// Open host-managed SQL transactions (`host::transaction`).
    pub(super) txs: super::host_sql::TxRegistry,
}

pub(super) type HostResult<T> = meerschaum_plugin::Result<T>;

pub(super) fn host_error(message: impl Into<String>) -> meerschaum_plugin::Error {
    meerschaum_plugin::Error::Message(message.into())
}

pub(super) fn tuple_json(tuple: &SuccessTuple) -> serde_json::Value {
    serde_json::json!([tuple.success, tuple.message])
}

/// The refusal of an API caller's SQL (`read_sql`, `exec_sql`) that could reach Meerschaum's
/// internal tables, shared with MCP's `read_sql` over HTTP
/// ([`meerschaum_server::mcp::security::refuse_internal_tables`]).
pub(super) fn refuse_internal_tables(query: &str) -> HostResult<()> {
    meerschaum_server::mcp::security::refuse_internal_tables(query).map_err(host_error)
}

/// A host call's bound as a `DateBound` (`None`: unbounded).
pub(super) fn date_bound(
    call: &meerschaum_plugin::HostCall,
    name: &str,
) -> HostResult<Option<meerschaum_connector::DateBound>> {
    Ok(match call.bound(name)? {
        None => None,
        Some(meerschaum_plugin::Bound::Int(i)) => Some(meerschaum_connector::DateBound::Int(i)),
        Some(meerschaum_plugin::Bound::Datetime(text)) => Some(
            meerschaum_connector::DateBound::parse(&text)
                .ok_or_else(|| host_error(format!("`{name}` is not a timestamp: '{text}'")))?,
        ),
    })
}

/// A JSON object argument as a core map (`None` for `null`).
pub(super) fn map_arg(call: &meerschaum_plugin::HostCall, name: &str) -> HostResult<Option<Map>> {
    match call.arg(name) {
        serde_json::Value::Null => Ok(None),
        value @ serde_json::Value::Object(_) => serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|e| host_error(format!("invalid `{name}`: {e}"))),
        _ => Err(host_error(format!("`{name}` must be a JSON object"))),
    }
}

/// The `DataQuery` of a `get_data` / `get_rowcount` / `clear` call.
pub(super) fn data_query(
    call: &meerschaum_plugin::HostCall,
) -> HostResult<meerschaum_connector::DataQuery> {
    let strings = |name: &str| -> HostResult<Option<Vec<String>>> {
        match call.arg(name) {
            serde_json::Value::Null => Ok(None),
            value => serde_json::from_value(value.clone())
                .map(Some)
                .map_err(|e| host_error(format!("invalid `{name}`: {e}"))),
        }
    };
    Ok(meerschaum_connector::DataQuery {
        begin: date_bound(call, "begin")?,
        end: date_bound(call, "end")?,
        params: map_arg(call, "params")?,
        select_columns: strings("select_columns")?,
        omit_columns: strings("omit_columns")?,
        limit: call.arg("limit").as_i64(),
        order: call.str_arg("order").map(str::to_owned),
        ..Default::default()
    })
}

/// A Pipe's metadata as plugins see it (`{"keys", "parameters", "temporary"}`).
pub(super) fn pipe_metadata(pipe: &Pipe) -> serde_json::Value {
    serde_json::json!({
        "keys": serde_json::to_value(pipe.keys()).unwrap_or_default(),
        "parameters": serde_json::to_value(pipe.raw_parameters()).unwrap_or_default(),
        "temporary": pipe.temporary,
    })
}

impl PluginHost {
    /// Host calls during an engine run (fetch, sync, hooks, streams).
    pub(super) fn engine(registry: NativeRegistry, args: SyncArgs) -> PluginHost {
        PluginHost {
            registry: Some(registry),
            env: None,
            args,
            api: None,
            txs: Default::default(),
        }
    }

    /// Host calls with the full trust of a local process (an action, MCP over stdio).
    pub(crate) fn local(env: HostEnv) -> PluginHost {
        PluginHost {
            registry: None,
            env: Some(env),
            args: SyncArgs::default(),
            api: None,
            txs: Default::default(),
        }
    }

    /// Host calls for the API caller `principal` with `scopes` (`["*"]` grants every scope).
    pub(crate) fn api(
        env: HostEnv,
        state: meerschaum_server::AppState,
        scopes: Vec<String>,
        principal: Option<meerschaum_server::auth::Principal>,
    ) -> PluginHost {
        PluginHost {
            registry: None,
            env: Some(env),
            args: SyncArgs::default(),
            api: Some(ApiAccess {
                state,
                scopes,
                host_scopes: Vec::new(),
                principal,
            }),
            txs: Default::default(),
        }
    }

    /// Also grant the host calls `scopes` for this caller (a route's or page's `host_scopes`).
    /// `*` is ignored: it would hand anonymous callers the whole host.
    pub(super) fn with_host_scopes(mut self, scopes: &[String]) -> PluginHost {
        if let Some(api) = &mut self.api {
            api.host_scopes = scopes.iter().filter(|s| *s != "*").cloned().collect();
        }
        self
    }

    pub(super) fn env(&self) -> HostResult<HostEnv> {
        match (&self.env, &self.registry) {
            (Some(env), _) => Ok(env.clone()),
            (None, Some(registry)) => registry.host_env().map_err(host_error),
            (None, None) => Err(host_error("no Meerschaum process serves these host calls")),
        }
    }

    /// Refuse what only a caller with every scope (an administrator, `--no-auth`) may do.
    pub(super) async fn require_all(&self, what: &str) -> HostResult<()> {
        if let Some(access) = self.api_access("*", what).await?
            && !access.full_admin()
        {
            return Err(host_error(format!(
                "The caller may not {what} from an API request."
            )));
        }
        Ok(())
    }

    /// Resolve the caller again for each API host call. A named plugin grant adds a
    /// permission, never an identity, administrator status, or team membership.
    pub(super) async fn api_access(
        &self,
        scope: &str,
        what: &str,
    ) -> HostResult<Option<meerschaum_server::access::Access>> {
        let Some(api) = &self.api else {
            return Ok(None);
        };
        use meerschaum_server::access::{Access, EVERYONE, TeamRole, rules};
        let mut access = if api.state.config.no_auth {
            Access::everything()
        } else if let Some(principal) = &api.principal {
            // A host call may follow a long-running plugin operation: reload credential
            // validity and scope narrowing as well as its owner's current access.
            use meerschaum_server::auth::{Principal, Scopes};
            // Reloaded as the kind it already is, never re-classified by the look of its name.
            let reloaded = match principal {
                Principal::Execution(identity) => {
                    meerschaum_server::executor::callbacks::load_identity(
                        &api.state,
                        identity.grant.id,
                        identity.grant.launch_id,
                    )
                    .await
                    .map(|identity| Some(Principal::Execution(Box::new(identity))))
                }
                Principal::User(user) => {
                    meerschaum_server::auth::load_user(&api.state, &user.username).await
                }
                Principal::Token(token) => {
                    let id = token.id.map(|id| id.to_string()).unwrap_or_default();
                    meerschaum_server::auth::load_token(&api.state, &id).await
                }
            };
            let principal = reloaded
                .map_err(|e| host_error(e.detail_text()))?
                .ok_or_else(|| host_error("Invalid or revoked credentials."))?;
            let mut access = meerschaum_server::access::access_of_fresh(
                &api.state,
                principal.clone(),
                &Scopes::List(api.scopes.clone()),
            )
            .await
            .map_err(|e| host_error(e.detail_text()))?;
            if let Principal::Token(token) = &principal {
                access.principal.permissions = meerschaum_server::access::intersect(
                    &access.principal.permissions,
                    &Scopes::List(token.scopes.clone()),
                );
            }
            // The request already resolved legacy scope spellings. Keep that exact
            // ceiling even if the user's permissions broaden while the plugin runs.
            if !api.scopes.iter().any(|scope| scope == "*") {
                access
                    .principal
                    .permissions
                    .retain(|permission| api.scopes.contains(permission));
            }
            access
        } else {
            Access {
                auth: None,
                credential: None,
                principal: rules::Principal::new(
                    "",
                    false,
                    Default::default(),
                    vec![(EVERYONE.into(), TeamRole::Member)],
                ),
                primary_team: EVERYONE.into(),
                attributes: None,
                no_auth: false,
                team_records: Vec::new(),
                api_execution: meerschaum_server::backends::ApiExecution::Unavailable,
            }
        };
        // Reading the process config requires the caller's own full administrator rights;
        // a plugin may not assemble them from named host grants.
        if scope != "*" {
            access
                .principal
                .permissions
                .extend(api.host_scopes.iter().cloned());
        }
        if !access.has(scope) {
            return Err(host_error(format!(
                "The caller may not {what}: that needs the `{scope}` scope."
            )));
        }
        Ok(Some(access))
    }

    /// API pipe handles are descriptions from untrusted plugin memory. Always authorize
    /// the instance's registered parameters, and use supplied parameters only for a new
    /// pipe after the same registration and definition guards as the HTTP routes.
    pub(super) async fn api_pipe(
        &self,
        mut pipe: Pipe,
        instance: &Arc<dyn InstanceConnector>,
        access: Option<&meerschaum_server::access::Access>,
        method: &str,
    ) -> HostResult<Pipe> {
        let (Some(api), Some(access)) = (&self.api, access) else {
            return Ok(pipe);
        };
        use meerschaum_plugin::callbacks::method as m;
        use meerschaum_server::access::{self, guard};
        for keys in [pipe.instance_keys(), pipe.connector_keys()] {
            guard::check_connector_teams(&api.state, access, keys)
                .map_err(|e| host_error(e.detail_text()))?;
        }
        let stored = api
            .state
            .get_pipe(
                pipe.connector_keys(),
                pipe.metric_key(),
                pipe.location_key(),
                Some(pipe.instance_keys()),
                true,
            )
            .await
            .map_err(|e| host_error(e.detail_text()))?;
        let registered = api
            .state
            .is_registered(&stored.pipe, Some(pipe.instance_keys()))
            .await
            .map_err(|e| host_error(e.detail_text()))?;
        let writes = matches!(method, m::SYNC | m::CLEAR | m::REGISTER);
        if registered || !matches!(method, m::SYNC | m::REGISTER) {
            pipe = stored.pipe.clone();
        } else {
            access
                .require(&["pipes:register"], &[])
                .map_err(|e| host_error(e.detail_text()))?;
            if !api.state.config.registration_pipes {
                return Err(host_error("The server has not allowed pipe registration."));
            }
            let parameters = guard::stamp_new_pipe(access, pipe.raw_parameters())
                .map_err(|e| host_error(e.detail_text()))?;
            guard::check_parameters(&api.state, access, instance, &pipe, None, &parameters)
                .await
                .map_err(|e| host_error(e.detail_text()))?;
            pipe.set_parameters(parameters);
            pipe.temporary = false;
        }
        if !access.can_see_pipe(&pipe) {
            return Err(host_error(access::hidden_pipe(&pipe).detail_text()));
        }
        if writes && !access.can_edit(pipe.raw_parameters()) {
            return Err(host_error(access::not_editable(&pipe).detail_text()));
        }
        let index = access::pipe_index(&api.state, Some(pipe.instance_keys()))
            .await
            .map_err(|e| host_error(e.detail_text()))?;
        // Internal tables stay unavailable even to administrator-owned narrow tokens, as
        // with API data routes and raw SQL. Administrators have no other hidden targets.
        guard::check_pipe_target(instance.as_ref(), &pipe, &index.hidden_targets(access))
            .map_err(|e| host_error(e.detail_text()))?;
        Ok(pipe)
    }

    /// Refuse `exec_sql` to an API caller who may not run actions, by the rule of the server's
    /// `/actions` route and MCP's `execute_action` (`is_user_allowed_to_execute`): an
    /// administrator may; another user or a token only when the server lets non-administrators
    /// run actions (`system:api:permissions:actions:non_admin`), and never under `--secure`; an
    /// anonymous caller never (except under `--no-auth`).
    pub(super) async fn may_execute(&self) -> HostResult<()> {
        let Some(api) = &self.api else {
            return Ok(());
        };
        if api.state.config.no_auth {
            return Ok(());
        }
        let access = self
            .api_access("actions:execute", "execute SQL statements")
            .await?;
        let Some(principal) = access.as_ref().and_then(|access| access.auth.as_ref()) else {
            return Err(host_error(
                "An anonymous caller may not execute SQL statements.",
            ));
        };
        let non_admins =
            api.state.config.system_actions_non_admin && api.state.config.actions_non_admin;
        let allowed = meerschaum_server::auth::is_user_allowed_to_execute_with(
            &api.state,
            Some(principal),
            non_admins,
        )
        .await;
        if !allowed.success {
            return Err(host_error(format!(
                "The caller may not execute SQL statements: {}",
                allowed.message
            )));
        }
        Ok(())
    }

    /// The instance connector `keys`: the run's, else the session's; an API caller only gets the
    /// instances the server allows.
    pub(super) fn instance(&self, keys: &str) -> HostResult<Arc<dyn InstanceConnector>> {
        if let Some(api) = &self.api {
            return api.state.get_api_connector(Some(keys)).map_err(|e| {
                host_error(
                    e.detail
                        .as_ref()
                        .and_then(|d| d.as_str())
                        .unwrap_or("This instance is not available.")
                        .to_string(),
                )
            });
        }
        if let Some(registry) = &self.registry
            && let Ok(instance) = registry.instance(keys)
        {
            return Ok(instance);
        }
        self.env()?
            .connector(keys)
            .map(|i| i.conn())
            .map_err(host_error)
    }

    /// The Pipe a call addresses: the request's (`call.pipe` is `None`) or the one it describes.
    pub(super) fn pipe_for(
        &self,
        request: &meerschaum_plugin::Request,
        call: &meerschaum_plugin::HostCall,
    ) -> HostResult<Pipe> {
        let metadata = match &call.pipe {
            Some(metadata) => metadata,
            None => &request.context["pipe"],
        };
        if !metadata.is_object() {
            return Err(host_error(format!(
                "`pipe.{}()` needs a Pipe, and this call has none: get one with `host::pipe(...)`.",
                call.method
            )));
        }
        let mut state = metadata.get("keys").unwrap_or(metadata).clone();
        state["parameters"] = metadata
            .get("parameters")
            .filter(|p| p.is_object())
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let state: Map = serde_json::from_value(state).map_err(|e| host_error(e.to_string()))?;
        let defaults = match &self.registry {
            Some(registry) => registry.engine_config.defaults.clone(),
            None => self.env()?.defaults,
        };
        let mut pipe =
            Pipe::from_state(&state, &defaults).map_err(|e| host_error(e.to_string()))?;
        pipe.temporary = metadata["temporary"].as_bool().unwrap_or(false);
        self.refuse_internal(pipe.connector_keys())?;
        Ok(pipe)
    }

    /// API callers never reach Meerschaum's internal `mrsm` pipes (as the HTTP routes refuse).
    pub(super) fn refuse_internal(&self, connector_keys: &str) -> HostResult<()> {
        if self.api.is_some() && connector_keys == "mrsm" {
            return Err(host_error(
                "Meerschaum's internal `mrsm` pipes are not available from API requests.",
            ));
        }
        Ok(())
    }

    /// The refusal of a write to `pipe` on a protected instance (`--allow-protected` and
    /// `MRSM_ALLOW_PROTECTED=1` allow it), as the core write paths refuse it.
    pub(super) fn protected(
        instance: &dyn InstanceConnector,
        pipe: &Pipe,
        op: &str,
    ) -> Option<meerschaum_plugin::HostReply> {
        meerschaum_connector::protected_refusal(instance, pipe, op)
            .map(|refused| meerschaum_plugin::HostReply::json(tuple_json(&refused)))
    }

    /// The sync engine `pipe.sync(...)` writes through: the run's, else (outside a run, or for
    /// a Pipe on an instance the run does not use) one over the session's connectors, the way
    /// [`instance`](Self::instance) reaches them.
    pub(super) fn engine_for_sync(&self, pipe: &Pipe) -> HostResult<meerschaum_engine::Engine> {
        let registry = match &self.registry {
            Some(registry) if registry.instance(pipe.instance_keys()).is_ok() => registry.clone(),
            _ => self.env()?.registry(),
        };
        let config = registry.engine_config.clone();
        Ok(meerschaum_engine::Engine::new(Arc::new(registry), config))
    }

    /// `host::mint_token(...)` and `host::revoke_token(...)`: only during an API request, for its
    /// caller, with the custom scopes the calling plugin declares
    /// ([`meerschaum_server::plugin_tokens`] has the rules).
    pub(super) async fn serve_token(
        &self,
        call: &meerschaum_plugin::HostCall,
    ) -> HostResult<serde_json::Value> {
        use meerschaum_plugin::callbacks::method as m;
        use meerschaum_server::plugin_tokens;
        let name = format!("host::{}()", call.method);
        let Some(api) = &self.api else {
            return Err(host_error(format!(
                "`{name}` works only during an API request (a route, console page or MCP \
                 entry), for its caller."
            )));
        };
        let allowed = plugin_tokens::plugin_scopes(meerschaum_plugin::manifest::declared_scopes(
            self.env()?.natives.plugin_entry(&call.plugin),
        ));
        if call.method == m::REVOKE_TOKEN {
            let id = call.str_arg("id").unwrap_or_default();
            return plugin_tokens::revoke(
                &api.state,
                api.principal.as_ref(),
                &call.plugin,
                &allowed,
                id,
            )
            .await
            .map(|revoked| tuple_json(&revoked))
            .map_err(host_error);
        }
        let scopes: Vec<String> = match call.arg("scopes") {
            serde_json::Value::Null => Vec::new(),
            value => serde_json::from_value(value.clone())
                .map_err(|e| host_error(format!("invalid `scopes`: {e}")))?,
        };
        let expiration = match call.bound("expiration")? {
            None => None,
            Some(meerschaum_plugin::Bound::Int(seconds)) => Some(
                chrono::DateTime::from_timestamp(seconds, 0)
                    .ok_or_else(|| host_error(format!("invalid `expiration`: {seconds}")))?,
            ),
            Some(bound) => Some(
                bound
                    .to_datetime()
                    .map_err(|_| host_error(format!("invalid `expiration`: '{bound}'")))?,
            ),
        };
        plugin_tokens::mint(
            &api.state,
            api.principal.as_ref(),
            &api.scopes,
            &call.plugin,
            &allowed,
            call.str_arg("label").unwrap_or_default(),
            &scopes,
            expiration,
        )
        .await
        .map(|minted| minted.to_json())
        .map_err(host_error)
    }

    pub(super) async fn serve(
        &self,
        request: &meerschaum_plugin::Request,
        call: &meerschaum_plugin::HostCall,
        data: Option<PipeInput>,
    ) -> HostResult<meerschaum_plugin::HostReply> {
        use meerschaum_plugin::HostReply;
        use meerschaum_plugin::callbacks::method as m;
        let json = HostReply::json;
        match call.method.as_str() {
            m::DRAINING => Ok(json(serde_json::json!(
                self.api
                    .as_ref()
                    .is_some_and(|api| api.state.lifecycle.is_draining())
            ))),
            m::ROOT_DIR => Ok(json(serde_json::json!(
                self.env()?
                    .config
                    .paths()
                    .root_dir
                    .to_string_lossy()
                    .into_owned()
            ))),
            m::READ_SQL_WITH => self.serve_sql_with(call, true).await,
            m::EXEC_SQL_WITH => self.serve_sql_with(call, false).await,
            m::TX_BEGIN => self.serve_tx_begin(call).await,
            m::TX_READ_SQL => self.serve_tx_sql(call, true).await,
            m::TX_EXEC_SQL => self.serve_tx_sql(call, false).await,
            m::TX_COMMIT => self.serve_tx_commit(call).await,
            m::TX_ROLLBACK => self.serve_tx_rollback(call).await,
            m::MINT_TOKEN | m::REVOKE_TOKEN => self.serve_token(call).await.map(json),
            m::PLUGIN_CONFIG => Ok(json(self.env()?.plugin_config(&call.plugin))),
            m::WRITE_PLUGIN_CONFIG => {
                let written = self
                    .env()?
                    .write_plugin_config(&call.plugin, call.arg("config"));
                Ok(json(tuple_json(&written)))
            }
            m::CONFIG => {
                self.require_all("read the Meerschaum config").await?;
                let keys: Vec<String> = serde_json::from_value(call.arg("keys").clone())
                    .map_err(|e| host_error(format!("invalid `keys`: {e}")))?;
                let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
                Ok(json(
                    self.env()?
                        .config
                        .get(&keys)
                        .and_then(|value| serde_json::to_value(value).ok())
                        .unwrap_or_default(),
                ))
            }
            m::READ_SQL | m::EXEC_SQL => {
                let keys = call
                    .str_arg("connector_keys")
                    .ok_or_else(|| host_error("`connector_keys` names no connector"))?;
                let query = call.str_arg("query").unwrap_or_default();
                // Permissions first: an API caller learns nothing about connectors it may not use.
                let access = if call.method == m::READ_SQL {
                    let access = self.api_access("sql:read", "run SQL queries").await?;
                    if self.api.is_some() {
                        meerschaum_server::mcp::security::is_read_only_query(query)
                            .map_err(host_error)?;
                    }
                    access
                } else {
                    let access = self
                        .api_access("actions:execute", "execute SQL statements")
                        .await?;
                    self.may_execute().await?;
                    access
                };
                if self.api.is_some() {
                    refuse_internal_tables(query)?;
                }
                if let (Some(api), Some(access)) = (&self.api, &access) {
                    // Named SQL must use a served instance, within its connector's teams.
                    self.instance(keys)?;
                    meerschaum_server::access::guard::check_connector_teams(
                        &api.state, access, keys,
                    )
                    .map_err(|e| host_error(e.detail_text()))?;
                    let index = meerschaum_server::access::pipe_index(&api.state, Some(keys))
                        .await
                        .map_err(|e| host_error(e.detail_text()))?;
                    meerschaum_server::sql_guard::check_definition(
                        query,
                        &index.hidden_targets(access),
                    )
                    .map_err(host_error)?;
                }
                let sql = self.env()?.sql(keys).map_err(host_error)?;
                if call.method == m::READ_SQL {
                    // A protected connector runs only a read statement, read-only.
                    let frame = sql
                        .read_raw(query)
                        .await
                        .map_err(|e| host_error(e.to_string()))?;
                    return Ok(HostReply::data(Some(frame)));
                }
                if let Err(refused) = meerschaum_connector::check_protected(&*sql, "execute SQL") {
                    return Ok(json(tuple_json(&SuccessTuple::fail(refused))));
                }
                // The connector's catalog cache hears of the statement (it may change a table).
                sql.note_raw_statement(query, false);
                let result = sql.driver().exec(query).await;
                sql.note_raw_statement(query, result.is_err());
                Ok(json(tuple_json(&match result {
                    Ok(rows) => SuccessTuple::ok(format!("Executed the statement ({rows} rows).")),
                    Err(error) => SuccessTuple::fail(format!(
                        "Failed to execute the statement on '{keys}':\n{error}"
                    )),
                })))
            }
            m::PIPE => {
                let access = self.api_access("pipes:read", "read pipes").await?;
                let connector_keys = call
                    .str_arg("connector_keys")
                    .ok_or_else(|| host_error("`host::pipe()` needs connector keys"))?;
                let metric_key = call
                    .str_arg("metric_key")
                    .ok_or_else(|| host_error("`host::pipe()` needs a metric key"))?;
                self.refuse_internal(connector_keys)?;
                // The request's Pipe is `{"keys": {...}, ...}` in most calls and flat in a
                // plugin's own `sync` (its keyword arguments' `pipe`).
                let request_pipe = &request.context["pipe"];
                let request_keys = request_pipe.get("keys").unwrap_or(request_pipe);
                let instance_keys = match call.str_arg("instance_keys") {
                    Some(keys) => keys.to_string(),
                    None => match request_keys["instance_keys"].as_str() {
                        Some(keys) if !keys.is_empty() => keys.to_string(),
                        _ => self.env()?.default_instance,
                    },
                };
                let defaults = match &self.registry {
                    Some(registry) => registry.engine_config.defaults.clone(),
                    None => self.env()?.defaults,
                };
                let mut pipe = Pipe::builder(connector_keys, metric_key)
                    .location(call.str_arg("location_key"))
                    .instance(&instance_keys)
                    .defaults(defaults)
                    .build()
                    .map_err(|e| host_error(e.to_string()))?;
                let instance = self.instance(&instance_keys)?;
                if self.api.is_some() {
                    pipe = self
                        .api_pipe(pipe, &instance, access.as_ref(), m::PIPE)
                        .await?;
                } else if let Ok(attributes) = instance.get_pipe_attributes(&pipe).await
                    && let Some(Value::Map(parameters)) = attributes.get("parameters")
                {
                    pipe.set_parameters(parameters.clone());
                }
                Ok(json(pipe_metadata(&pipe)))
            }
            method => {
                let (scope, what) = match method {
                    m::SYNC => ("pipes:write", "sync pipes"),
                    // PARITY: wave 2's permission grid supersedes the original host-call
                    // write alias: deleting rows and registering each need their own scope.
                    m::CLEAR => ("pipes:delete", "clear pipes"),
                    m::REGISTER => ("pipes:register", "register pipes"),
                    m::GET_SYNC_TIME
                    | m::GET_DATA
                    | m::GET_ROWCOUNT
                    | m::GET_COLUMNS_TYPES
                    | m::EXISTS
                    | m::GET_PIPE_ID
                    | m::IS_REGISTERED => ("pipes:read", "read pipes"),
                    other => {
                        return Err(meerschaum_plugin::Error::Unsupported(format!(
                            "unsupported host call: {other}"
                        )));
                    }
                };
                let access = self.api_access(scope, what).await?;
                let pipe = self.pipe_for(request, call)?;
                let instance = self.instance(pipe.instance_keys())?;
                let pipe = self
                    .api_pipe(pipe, &instance, access.as_ref(), method)
                    .await?;
                match method {
                    m::GET_SYNC_TIME => {
                        let newest = call.arg("newest").as_bool().unwrap_or(true);
                        let bound = instance
                            .get_sync_time(&pipe, map_arg(call, "params")?.as_ref(), newest)
                            .await
                            .map_err(|e| host_error(e.to_string()))?;
                        Ok(json(match bound {
                            None => serde_json::Value::Null,
                            Some(meerschaum_connector::DateBound::Int(i)) => serde_json::json!(i),
                            Some(other) => serde_json::json!(other.isoformat()),
                        }))
                    }
                    m::SYNC => {
                        if let Some(refused) = Self::protected(&*instance, &pipe, "sync") {
                            return Ok(refused);
                        }
                        // `pipe.replace(rows, begin, end)`: the window's rows are replaced in one
                        // transaction (a `null` bound comes from the rows); no rows empty it.
                        let replace = call.arg("replace").as_bool() == Some(true);
                        let data = match data {
                            Some(data) => data,
                            None if replace => PipeInput::Frame(meerschaum_plugin::Frame::empty()),
                            None => {
                                return Ok(json(serde_json::json!([true, "No data to sync."])));
                            }
                        };
                        // The request's Pipe syncs with the surrounding sync's options; any other
                        // Pipe with the defaults (its own window, never the caller's `replace`).
                        let mut args = if call.pipe.is_none() {
                            self.args.clone()
                        } else {
                            SyncArgs::default()
                        };
                        if replace {
                            args.begin = match date_bound(call, "begin")? {
                                Some(begin) => SyncBegin::At(begin),
                                None => SyncBegin::Default,
                            };
                            args.end = date_bound(call, "end")?;
                            args.replace = true;
                            args.extra.shift_remove("replace");
                        }
                        // Supplying input bypasses plugin_sync and fetch; no recursive plugin call.
                        let result = self
                            .engine_for_sync(&pipe)?
                            .sync_pipe(&pipe, Some(data), &args)
                            .await;
                        if result.success
                            && let Some(api) = &self.api
                        {
                            api.state.access_cache().forget_pipes();
                        }
                        Ok(json(serde_json::json!([result.success, result.message])))
                    }
                    m::GET_DATA => {
                        let query = data_query(call)?;
                        let frame =
                            meerschaum_connector::pipe_ops::get_data(&*instance, &pipe, &query)
                                .await
                                .map_err(|e| host_error(e.to_string()))?;
                        Ok(HostReply::data(frame))
                    }
                    m::GET_ROWCOUNT => {
                        let count =
                            match instance.get_pipe_rowcount(&pipe, &data_query(call)?).await {
                                Ok(count) => count.unwrap_or(0),
                                Err(meerschaum_connector::ConnectorError::NotImplemented(_)) => 0,
                                Err(e) => return Err(host_error(e.to_string())),
                            };
                        Ok(json(serde_json::json!(count)))
                    }
                    m::CLEAR => {
                        if let Some(refused) = Self::protected(&*instance, &pipe, "clear") {
                            return Ok(refused);
                        }
                        if self.api.is_some()
                            && let Some(error) = meerschaum_server::access::guard::lock_conflict(
                                &pipe,
                                meerschaum_core::access::LockOp::Clear,
                            )
                        {
                            return Err(host_error(error.detail_text()));
                        }
                        let cleared = instance.clear_pipe(&pipe, &data_query(call)?).await;
                        Ok(json(tuple_json(&cleared)))
                    }
                    m::GET_COLUMNS_TYPES => {
                        let types = match instance.get_pipe_columns_types(&pipe).await {
                            Ok(types) => types,
                            Err(meerschaum_connector::ConnectorError::NotImplemented(_)) => {
                                Default::default()
                            }
                            Err(e) => return Err(host_error(e.to_string())),
                        };
                        Ok(json(serde_json::to_value(types).unwrap_or_default()))
                    }
                    m::EXISTS => {
                        let exists = instance
                            .pipe_exists(&pipe)
                            .await
                            .map_err(|e| host_error(e.to_string()))?;
                        Ok(json(serde_json::json!(exists)))
                    }
                    m::GET_PIPE_ID => {
                        let id = instance
                            .get_pipe_id(&pipe)
                            .await
                            .map_err(|e| host_error(e.to_string()))?;
                        Ok(json(
                            serde_json::to_value(id).map_err(|e| host_error(e.to_string()))?,
                        ))
                    }
                    m::IS_REGISTERED => {
                        let registered = instance
                            .get_pipe_id(&pipe)
                            .await
                            .map_err(|e| host_error(e.to_string()))?
                            .is_some();
                        Ok(json(serde_json::json!(registered)))
                    }
                    m::REGISTER => {
                        if let Some(refused) = Self::protected(&*instance, &pipe, "register") {
                            return Ok(refused);
                        }
                        let result = instance.register_pipe(&pipe).await;
                        if result.success
                            && let Some(api) = &self.api
                        {
                            api.state.access_cache().forget_pipes();
                        }
                        Ok(json(tuple_json(&result)))
                    }
                    other => Err(meerschaum_plugin::Error::Unsupported(format!(
                        "unsupported host call: {other}"
                    ))),
                }
            }
        }
    }
}

/// Host services with the full trust of the local process, for native plugin calls a host
/// makes outside an engine run (the Python façade's native connectors).
pub fn local_host(env: HostEnv) -> Arc<dyn meerschaum_plugin::HostServices> {
    Arc::new(PluginHost::local(env))
}

impl meerschaum_plugin::HostServices for PluginHost {
    fn call(
        &self,
        request: &meerschaum_plugin::Request,
        method: &str,
        data: Option<meerschaum_plugin::Frame>,
    ) -> meerschaum_plugin::Result<serde_json::Value> {
        self.call_input(request, method, data.map(PipeInput::Frame))
    }

    /// A single-file plugin's `pipe.sync(rows)` syncs its rows as documents, like a Python
    /// plugin's `pipe.sync(docs)`.
    fn call_input(
        &self,
        request: &meerschaum_plugin::Request,
        method: &str,
        data: Option<PipeInput>,
    ) -> meerschaum_plugin::Result<serde_json::Value> {
        let call = meerschaum_plugin::HostCall::legacy("", method);
        self.host_call(request, &call, data)
            .map(|reply| reply.value)
    }

    fn host_call(
        &self,
        request: &meerschaum_plugin::Request,
        call: &meerschaum_plugin::HostCall,
        data: Option<PipeInput>,
    ) -> meerschaum_plugin::Result<meerschaum_plugin::HostReply> {
        // Native fetch and sync execute on blocking threads; actions on the CLI's thread. The
        // runtime stays free to service SQL/network operations while this callback waits.
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => self.env()?.session.handle(),
        };
        blocking(|| handle.block_on(self.serve(request, call, data)))
    }
}
