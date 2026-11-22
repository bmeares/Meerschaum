//! The native backend's actions: `POST /execute`, pipe operations and inference, extensions,
//! team execution probes, executor and host API-only terminals, and action resolution.

use std::sync::Arc;

use async_trait::async_trait;
use meerschaum_core::{Map, SuccessTuple};

use super::{NativeBackend, argument};
use crate::dispatch::Runtime;
use crate::native;
use crate::value::{ArgsMap, PyValue};

#[async_trait]
impl meerschaum_server::backends::ActionsBackend for NativeBackend {
    fn api_execution(&self) -> meerschaum_server::backends::ApiExecution {
        use meerschaum_server::backends::ApiExecution;
        if self.http
            && self.executors.is_some()
            && cfg!(target_os = "linux")
            && meerschaum_core::self_exe::native_executable().is_some()
        {
            ApiExecution::Tracked {
                terminals: meerschaum_server::executor::terminal::API_TERMINALS_AVAILABLE,
            }
        } else {
            ApiExecution::Unavailable
        }
    }

    async fn accept_pipe_inference(
        &self,
        caller: &meerschaum_server::access::Access,
        change: meerschaum_connector::mutation::MetadataInference,
    ) -> Result<bool, String> {
        match &self.executors {
            Some(controller) => controller.accept_pipe_inference(caller, change).await,
            None => Ok(false),
        }
    }

    async fn extension_catalog(
        &self,
    ) -> Result<Option<meerschaum_server::executor::plugins::Validated>, String> {
        match &self.executors {
            Some(controller) => controller.extension_catalog().await,
            None => Ok(None),
        }
    }
    async fn extension_invoke(
        &self,
        invocation: meerschaum_server::executor::plugin_handlers::Invocation,
    ) -> Result<meerschaum_server::executor::plugin_handlers::Reply, String> {
        self.executors
            .as_ref()
            .ok_or("This API is not an executor controller.")?
            .extension_invoke(invocation)
            .await
    }
    async fn ready_pipe_operation(
        &self,
        caller: &meerschaum_server::access::Access,
        reference: &str,
    ) -> Result<(), String> {
        self.executors
            .as_ref()
            .ok_or("This API has no native parent-window controller.")?
            .ready_prepared_child(caller, reference)
            .await
    }

    async fn pipe_operation_read(
        &self,
        caller: &meerschaum_server::access::Access,
    ) -> Result<Option<meerschaum_server::executor::pipe_operation::ReadAccess>, String> {
        match &self.executors {
            Some(controller) => controller.pipe_operation_read(caller).await,
            None => Ok(None),
        }
    }

    async fn graph_batch(
        &self,
        caller: &meerschaum_server::access::Access,
        request: &meerschaum_connector_api::client::BatchRequest,
    ) -> Result<Option<meerschaum_connector_api::client::BatchReply>, String> {
        self.executors
            .as_ref()
            .ok_or("This API is not a canonical batch controller.")?
            .graph_batch(caller, request)
            .await
    }

    async fn claim_pipe_batch(
        &self,
        caller: &meerschaum_server::access::Access,
        reference: &str,
        selector: &meerschaum_connector_api::client::BatchSelector,
        actual_request_sha256: &str,
    ) -> Result<meerschaum_connector::mutation::BatchClaimOutcome, String> {
        self.executors
            .as_ref()
            .ok_or("This API is not a native batch controller.")?
            .claim_pipe_batch(caller, reference, selector, actual_request_sha256)
            .await
    }

    async fn confirm_pipe_batch(
        &self,
        caller: &meerschaum_server::access::Access,
        batch: &meerschaum_connector::mutation::NativeBatch,
    ) -> Result<(), String> {
        self.executors
            .as_ref()
            .ok_or("This API has no native batch controller.")?
            .confirm_pipe_batch(caller, batch)
            .await
    }

    async fn accepted_pipe_batch(
        &self,
        caller: &meerschaum_server::access::Access,
        batch: &meerschaum_connector::mutation::NativeBatch,
    ) -> Result<meerschaum_connector::mutation::NativeBatchReceipt, String> {
        self.executors
            .as_ref()
            .ok_or("This API has no native batch controller.")?
            .accepted_pipe_batch(caller, batch)
            .await
    }

    async fn prepare_pipe_commit(
        &self,
        caller: &meerschaum_server::access::Access,
        effect: meerschaum_connector::mutation::NativeDataEffect,
    ) -> Result<Arc<dyn meerschaum_connector::mutation::PendingDataCommit>, String> {
        self.executors
            .as_ref()
            .ok_or("This API is not a canonical commit controller.")?
            .prepare_pipe_commit(caller, effect)
            .await
    }

    fn list(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        crate::actions::CORE_ACTIONS
            .iter()
            .map(|action| action.name.to_string())
            .chain(self.plugin_actions.iter().cloned())
            .filter(|name| seen.insert(name.clone()))
            .collect()
    }

    async fn execute(&self, action: &str, keywords: Map) -> SuccessTuple {
        meerschaum_server::executor::context::action_request_scope(async {
            let caller = meerschaum_server::executor::context::current();
            let api_worker = match self.api_worker(caller.as_ref()).await {
                Ok(api_worker) => api_worker,
                Err(error) => return SuccessTuple::fail(error),
            };
            if let Some(executors) = &self.executors {
                if caller
                    .as_ref()
                    .is_some_and(|caller| !caller.no_auth && !caller.full_admin())
                {
                    use meerschaum_server::executor::pipe_operation::DirectAction;
                    match DirectAction::capture(action, &keywords) {
                        Ok(Some(input)) => {
                            return executors
                                .run_direct_action(input, &self.session)
                                .await
                                .map(|(result, _)| result)
                                .unwrap_or_else(SuccessTuple::fail);
                        }
                        Err(error) => return SuccessTuple::fail(error.detail_text()),
                        Ok(None) => {}
                    }
                }
                let mut remote_keywords = keywords.clone();
                for key in ["noask", "yes"] {
                    remote_keywords
                        .entry(key.into())
                        .or_insert(meerschaum_core::Value::Bool(true));
                }
                let argv = meerschaum_server::execute_access::sysargs_of_keywords(
                    action,
                    &remote_keywords,
                );
                match executors.command_team(&argv, &self.session).await {
                    Err(error) => return SuccessTuple::fail(error),
                    Ok(Some(team)) => {
                        return executors
                            .run(&team, argv)
                            .await
                            .map(|(result, _)| result)
                            .unwrap_or_else(SuccessTuple::fail);
                    }
                    Ok(None) => {}
                }
                if api_worker {
                    return meerschaum_server::executor::context::scope(
                        caller,
                        executors.run_host_api(argv),
                    )
                    .await
                    .map(|(result, _)| result)
                    .unwrap_or_else(SuccessTuple::fail);
                }
            } else if api_worker {
                return SuccessTuple::fail("The canonical API execution runtime is unavailable.");
            }
            let backend = self.clone();
            let action = action.to_string();
            let acting = meerschaum_connector::acting_team::current();
            tokio::task::spawn_blocking(move || {
                let mut stderr = std::io::stderr();
                let mut runtime = Runtime::system(&mut stderr);
                // A server console is not a terminal session: no spinners, no colored graphs.
                runtime.progress = crate::format::progress::ProgressSink::Off;
                runtime.live = false;
                let (session, config) = match backend.scoped(acting.as_deref()) {
                    Ok(scoped) => scoped,
                    Err(error) => return SuccessTuple::fail(error.message),
                };
                runtime.native = Some(session);
                runtime.config = Some(config);
                let mut args: ArgsMap = keywords
                    .into_iter()
                    .map(|(key, value)| (key, argument(value)))
                    .collect();
                let mut words = vec![action];
                words.extend(native::words(&args, "action"));
                args.insert("action".into(), PyValue::str_list(&words));
                // A client's `debug` reaches the action, but does not switch on debug output (and
                // `debug=True` for worker calls) for every request the server is handling.
                meerschaum_core::debug::without_scopes(|| {
                    crate::dispatch::entry_with_args(args, &mut runtime, 0)
                })
            })
            .await
            .unwrap_or_else(|error| SuccessTuple::fail(error.to_string()))
        })
        .await
    }

    /// The dispatcher's own resolution of the step ([`crate::dispatch::resolve_sysargs`]), so a
    /// word it would hand to the shell is never mistaken for an action.
    async fn team_execution_probe(
        &self,
        team: &meerschaum_core::access::Team,
    ) -> Option<meerschaum_server::team_execution::HostFacts> {
        let config = self.config().ok()?;
        let name = team.name.clone();
        tokio::task::spawn_blocking(move || crate::team::execution::probe(&config, &name))
            .await
            .ok()
    }

    async fn executor_ready(&self, team: &meerschaum_core::access::Team) -> Result<(), String> {
        self.executors
            .as_ref()
            .ok_or("This API is not an executor controller.")?
            .prepare(team)
            .await
            .map(|_| ())
    }

    async fn executor_terminal(
        &self,
        team: &meerschaum_core::access::Team,
        caller: &meerschaum_server::access::Access,
    ) -> Result<meerschaum_server::webterm::remote::Terminal, String> {
        self.executors
            .as_ref()
            .ok_or("This API is not an executor controller.")?
            .terminal(team, caller)
            .await
    }

    async fn revoke_executor_terminal(&self, team_id: uuid::Uuid, owner: &str) {
        if let Some(controller) = &self.executors {
            // Revocation is a main control operation, never a request for host execution.
            meerschaum_server::executor::context::scope(
                None,
                controller.revoke_terminal(team_id, owner),
            )
            .await;
        }
    }

    async fn host_api_terminal(
        &self,
        team_id: Option<uuid::Uuid>,
        caller: &meerschaum_server::access::Access,
    ) -> Result<meerschaum_server::webterm::remote::Terminal, String> {
        self.executors
            .as_ref()
            .ok_or("This API has no private host terminal runtime.")?
            .host_terminal(team_id, caller)
            .await
    }

    async fn revoke_host_api_terminal(
        &self,
        team_id: Option<uuid::Uuid>,
        owner: &str,
        credential: &str,
    ) {
        if let Some(controller) = &self.executors {
            meerschaum_server::executor::context::scope(
                None,
                controller.revoke_host_terminal(team_id, owner, credential),
            )
            .await;
        }
    }

    fn team_shell(
        &self,
        team: &meerschaum_core::access::Team,
    ) -> Result<meerschaum_server::webterm::ShellCommand, String> {
        let config = self.config().map_err(|e| e.message)?;
        crate::team::execution::team_shell(&config, &team.name)
    }

    async fn resolve(&self, step: &[String]) -> meerschaum_server::backends::ActionTarget {
        use crate::dispatch::ActionTarget as Target;
        use meerschaum_server::backends::ActionTarget;
        let backend = self.clone();
        let step = step.to_vec();
        let target = tokio::task::spawn_blocking(move || {
            let mut stderr = std::io::sink();
            let mut runtime = Runtime::system(&mut stderr);
            runtime.progress = crate::format::progress::ProgressSink::Off;
            runtime.live = false;
            runtime.native = Some(backend.session.clone());
            runtime.config = Some(backend.config().ok()?);
            Some(meerschaum_core::debug::without_scopes(|| {
                crate::dispatch::resolve_sysargs(&step, &mut runtime)
            }))
        })
        .await
        .ok()
        .flatten();
        // Anything that cannot be resolved is refused (fail closed).
        match target {
            Some(Target::Core(resolved)) => ActionTarget::Action {
                name: resolved.action.name.to_string(),
                plugin: None,
                sub: resolved.sub.map(|sub| sub.name.to_string()),
            },
            Some(Target::NativePlugin { plugin, name }) => ActionTarget::Action {
                name,
                plugin: Some(plugin),
                sub: None,
            },
            Some(Target::PythonPlugin { plugin, name }) => ActionTarget::Action {
                plugin: Some(plugin.unwrap_or_else(|| name.clone())),
                name,
                sub: None,
            },
            Some(Target::Shell) | None => ActionTarget::Shell,
        }
    }
}
