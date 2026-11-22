//! The native backend the API and MCP servers call: native actions on the shared session, run
//! on blocking threads, and the static plans of saved Pipelines.

use std::sync::Arc;

use async_trait::async_trait;
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::{Map, Pipe, SuccessTuple};

use super::io_error;
use crate::builtin::ActionError;
use crate::dispatch::Runtime;
use crate::native::{self, Session};

// Native actions synchronously drive the shared Session runtime. Run them on a blocking
// thread so they never nest block_on inside the server's async executor.
#[derive(Clone)]
pub(super) struct NativeBackend {
    pub(super) http: bool,
    pub(super) options: ConfigOptions,
    pub(super) snapshot: Map,
    pub(super) session: Session,
    pub(super) plugin_actions: Vec<String>,
    pub(super) executors: Option<Arc<crate::executor::client::Controller>>,
}

impl NativeBackend {
    pub(super) async fn api_worker(
        &self,
        caller: Option<&Arc<meerschaum_server::access::Access>>,
    ) -> Result<bool, String> {
        match caller {
            Some(caller) if caller.no_auth || caller.full_admin() => Ok(false),
            Some(caller) if caller.auth.is_none() => {
                Err("This HTTP execution has no authenticated original source.".into())
            }
            Some(caller) => {
                Ok(meerschaum_server::team_execution::code_running_team(caller).is_none())
            }
            None if !self.http => Ok(false),
            None => {
                if let Some(executors) = &self.executors
                    && executors.allow_unattributed_jobs()?
                {
                    return Ok(false);
                }
                Err("This HTTP execution lost its original source; retry the authenticated request.".into())
            }
        }
    }
    pub(super) fn config(&self) -> Result<Config, ActionError> {
        let config = Config::new(self.options.clone()).map_err(io_error)?;
        config.set_config(self.snapshot.clone());
        Ok(config)
    }

    /// The session and configuration a call runs with: the team's own (its workers and
    /// connectors, built from its configuration only) when the request acts for an isolated
    /// team, else the server's.
    pub(super) fn scoped(
        &self,
        acting: Option<&meerschaum_connector::acting_team::ActingTeam>,
    ) -> Result<(Session, Config), ActionError> {
        let config = self.config()?;
        let Some(team) = acting else {
            return Ok((self.session.clone(), config));
        };
        let (session, _) =
            self.session
                .for_team(&team.scope, Some(team.member.clone()), &config)?;
        let team_config = match session.team() {
            Some(binding) => binding.new_config()?,
            None => return Err(ActionError::new("RuntimeError", "No team session.")),
        };
        Ok((session, team_config))
    }

    pub(super) async fn sync(
        &self,
        pipe: &Pipe,
        begin: Option<String>,
        end: Option<String>,
        verify: bool,
    ) -> SuccessTuple {
        let caller = meerschaum_server::executor::context::current();
        let api_worker = match self.api_worker(caller.as_ref()).await {
            Ok(api_worker) => api_worker,
            Err(error) => return SuccessTuple::fail(error),
        };
        if let Some(executors) = &self.executors {
            if api_worker {
                let operation = meerschaum_server::executor::pipe_operation::PipeOperation {
                    row_params: None,
                    pipe_ref: format!(
                        "{}|{}|{}|{}",
                        pipe.instance_keys(),
                        pipe.connector_keys(),
                        pipe.metric_key(),
                        pipe.location_key().unwrap_or("None")
                    ),
                    registration: meerschaum_core::Value::Null,
                    verify,
                    begin,
                    end,
                    parents: None,
                    children: None,
                    metadata: None,
                };
                return meerschaum_server::executor::context::scope(
                    caller,
                    executors.run_pipe_operation(operation, &self.session),
                )
                .await
                .map(|(result, _)| result)
                .unwrap_or_else(SuccessTuple::fail);
            }
            let mut keywords = Map::from_iter([
                (
                    "connector_keys".into(),
                    meerschaum_core::Value::List(vec![pipe.connector_keys().to_string().into()]),
                ),
                (
                    "metric_keys".into(),
                    meerschaum_core::Value::List(vec![pipe.metric_key().to_string().into()]),
                ),
                (
                    "location_keys".into(),
                    meerschaum_core::Value::List(vec![
                        pipe.location_key().unwrap_or("[None]").to_string().into(),
                    ]),
                ),
                (
                    "mrsm_instance".into(),
                    pipe.instance_keys().to_string().into(),
                ),
            ]);
            for (key, value) in [("begin", begin.clone()), ("end", end.clone())] {
                if let Some(value) = value {
                    keywords.insert(key.into(), value.into());
                }
            }
            keywords.insert(
                "action".into(),
                meerschaum_core::Value::List(vec!["pipes".into()]),
            );
            let argv = meerschaum_server::execute_access::sysargs_of_keywords(
                if verify { "verify" } else { "sync" },
                &keywords,
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
        } else if api_worker {
            return SuccessTuple::fail("The canonical API execution runtime is unavailable.");
        }
        let backend = self.clone();
        let pipe = pipe.clone();
        let acting = meerschaum_connector::acting_team::current();
        tokio::task::spawn_blocking(move || {
            let mut stderr = std::io::stderr();
            let mut runtime = Runtime::system(&mut stderr);
            // A server console is not a terminal session: no spinners, no colored graphs.
            runtime.progress = crate::format::progress::ProgressSink::Off;
            runtime.live = false;
            let (scoped_session, config) = match backend.scoped(acting.as_deref()) {
                Ok(scoped) => scoped,
                Err(error) => return SuccessTuple::fail(error.message),
            };
            runtime.native = Some(scoped_session);
            runtime.config = Some(config);
            let session = native::session(&mut runtime);
            let instance = match session.instance(pipe.instance_keys(), runtime.config()) {
                Ok(instance) => instance,
                Err(error) => return SuccessTuple::fail(error.message),
            };
            let mut argv = vec!["sync".to_string(), "pipes".to_string()];
            for (flag, bound) in [("--begin", begin), ("--end", end)] {
                if let Some(bound) = bound {
                    argv.extend([flag.to_string(), bound]);
                }
            }
            let args = match crate::parse::parse_arguments(&argv, &runtime.dates) {
                Ok(args) => args,
                Err(error) => return SuccessTuple::fail(error.message),
            };
            let sync_args = native::sync::SyncArgs::from_args(&args);
            let mut selected = native::pipes::SelectedPipe {
                pipe,
                id: None,
                hydrated: false,
            };
            if verify {
                native::verify::verify_pipe(
                    &mut runtime,
                    &session,
                    &instance,
                    &mut selected,
                    &args,
                    &sync_args,
                )
            } else {
                native::sync::sync_pipe(
                    &mut runtime,
                    &session,
                    &instance,
                    &mut selected,
                    &sync_args,
                )
            }
        })
        .await
        .unwrap_or_else(|error| SuccessTuple::fail(error.to_string()))
    }

    /// Run a command line (`["plan", "pipelines", ...]`) the way the server runs actions, with
    /// what it prints captured: the action's result and its output.
    pub(super) async fn run_captured(
        &self,
        argv: Vec<String>,
    ) -> Result<(SuccessTuple, String), String> {
        meerschaum_server::executor::context::action_request_scope(async {
            let caller = meerschaum_server::executor::context::current();
            let api_worker = self.api_worker(caller.as_ref()).await?;
            if let Some(executors) = &self.executors
                && let Some(team) = executors.command_team(&argv, &self.session).await?
            {
                return executors.run(&team, argv).await;
            }
            if api_worker {
                let executors = self
                    .executors
                    .as_ref()
                    .ok_or("The canonical API execution runtime is unavailable.")?;
                return meerschaum_server::executor::context::scope(
                    caller,
                    executors.run_host_api(argv),
                )
                .await;
            }
            let backend = self.clone();
            let acting = meerschaum_connector::acting_team::current();
            tokio::task::spawn_blocking(move || {
                let mut out: Vec<u8> = Vec::new();
                let result = {
                    let mut runtime = Runtime::system(&mut out);
                    // A server console is not a terminal session: no spinners, no colored graphs.
                    runtime.progress = crate::format::progress::ProgressSink::Off;
                    runtime.live = false;
                    let (session, config) =
                        backend.scoped(acting.as_deref()).map_err(|e| e.message)?;
                    runtime.native = Some(session);
                    runtime.config = Some(config);
                    let args = crate::parse::parse_arguments(&argv, &runtime.dates)
                        .map_err(|e| e.message)?;
                    meerschaum_core::debug::without_scopes(|| {
                        crate::dispatch::entry_with_args(args, &mut runtime, 0)
                    })
                };
                Ok((result, String::from_utf8_lossy(&out).into_owned()))
            })
            .await
            .map_err(|e| e.to_string())?
        })
        .await
    }
}

#[async_trait]
impl meerschaum_server::backends::PipelinesBackend for NativeBackend {
    /// `plan pipelines NAME -i REGISTRY --nopretty`: the engine's static plan of the saved
    /// Pipeline, which moves no data and registers nothing.
    async fn plan(
        &self,
        name: &str,
        registry: &str,
        options: &meerschaum_server::backends::PlanOptions,
    ) -> Result<serde_json::Value, meerschaum_server::backends::PlanError> {
        use meerschaum_server::backends::PlanError;
        let caller = meerschaum_server::executor::context::current();
        if self
            .api_worker(caller.as_ref())
            .await
            .map_err(PlanError::Failed)?
        {
            let executors = self.executors.as_ref().ok_or(PlanError::Unavailable)?;
            return meerschaum_server::executor::context::scope(
                caller,
                Box::pin(executors.plan_metadata(name, registry, options)),
            )
            .await;
        }
        let mut argv: Vec<String> = ["plan", "pipelines", name, "-i", registry, "--nopretty"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        for (flag, value) in [("--begin", &options.begin), ("--end", &options.end)] {
            if let Some(value) = value {
                argv.push(flag.into());
                argv.push(value.clone());
            }
        }
        let (result, output) = self.run_captured(argv).await.map_err(PlanError::Failed)?;
        // A plan that cannot be made (a missing member, a cycle, a bad bound) is the
        // Pipeline's problem, not the server's.
        if !result.success {
            return Err(PlanError::Invalid(result.message));
        }
        let line = output
            .lines()
            .rev()
            .find(|l| l.trim_start().starts_with('{'))
            .ok_or_else(|| PlanError::Failed("The planner printed no plan.".into()))?;
        let plan: meerschaum_engine::Plan = serde_json::from_str(line.trim())
            .map_err(|e| PlanError::Failed(format!("The planner printed an invalid plan: {e}")))?;
        Ok(serde_json::json!({
            "plan": plan,
            "text": plan.to_string(),
            "warnings": plan.warnings,
        }))
    }
}
