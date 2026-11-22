//! Canonical persistent jobs own callback authority independently of their parent launch.
//!
//! `capture` captures a job's stored pipe and Pipeline run and serves the pipe-operation reads of
//! its live callback; `job_callbacks` registers, confirms and revokes a job's callback grant;
//! `job_admission` admits a job's original command and source; `delivery` delivers a job's
//! runtime context to its executor; `pipe_windows` is the main-only planner of a job's native
//! sync windows. The `context_delivery_tests`, `future_size_tests`, `identity_tests` and
//! `missing_definition_tests` children are its test suites.

use super::Controller;
use super::{PreparedExecutor, pipe_operation, source_selection, team_pipe_job};
use crate::executor::jobs::{Catalog, JobPlacement, Record};
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::{Method, RequestOptions};
use meerschaum_core::{SuccessTuple, Value};
use meerschaum_server::executor::{callbacks, context, environment};
use std::sync::Arc;
use uuid::Uuid;

mod capture;
mod delivery;
mod job_admission;
mod job_callbacks;
mod pipe_windows;

use job_admission::{admit_job, job_credential};

#[cfg(test)]
mod context_delivery_tests;
#[cfg(test)]
mod future_size_tests;

fn launch(record: &Record) -> Result<Uuid, String> {
    record
        .remote_name
        .strip_prefix("mrsm-")
        .and_then(|s| Uuid::parse_str(s).ok())
        .filter(|id| !id.is_nil())
        .ok_or_else(|| "The job has no canonical launch identity.".into())
}

/// Reuse the chain expansion, normal parser and typed overrides used for launch placement.
fn contains_uncaptured_graph(argv: &[String]) -> Result<bool, String> {
    let clock = crate::types::DateContext::system();
    for step in meerschaum_server::execute_policy::command_steps(argv)? {
        let mut args =
            crate::parse::parse_arguments(&step.words, &clock).map_err(|error| error.message)?;
        args.extend(
            step.patch
                .into_iter()
                .map(|(key, value)| (key, crate::server::argument(value))),
        );
        let words = crate::native::words(&args, "action");
        if crate::actions::get_action(&words, crate::actions::CORE_ACTIONS).is_some_and(
            |resolved| {
                matches!(resolved.action.name, "sync" | "verify")
                    && !matches!(
                        resolved.function_name(),
                        "_verify_packages" | "_verify_plugins" | "_verify_venvs"
                    )
            },
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

impl Controller {
    /// Project display names from immutable team IDs before applying current visibility.
    /// A replacement team/name or username cannot adopt historical canonical metadata.
    pub(crate) async fn job_views(&self, records: Vec<Record>) -> Result<Vec<Record>, String> {
        use meerschaum_core::job_access::{JobAccess, can_see_job};
        if records.is_empty() {
            return Ok(records);
        }
        let teams = self
            .instance
            .get_teams()
            .await
            .map_err(|_| "Could not resolve canonical job teams.")?;
        let caller = context::current();
        let mut visible = Vec::new();
        for mut record in records {
            let everyone = record.placement == JobPlacement::HostApi && record.team_id.is_nil();
            let team = teams.iter().find(|t| {
                t.id.as_deref().and_then(|s| Uuid::parse_str(s).ok()) == Some(record.team_id)
            });
            let old = JobAccess::from_properties(&record.name, &record.properties);
            if let Some(team) = team {
                JobAccess::stamp(
                    &mut record.properties,
                    old.owner.as_deref(),
                    Some(&team.name),
                );
            }
            if let Some(caller) = &caller
                && !caller.no_auth
                && !caller.is_admin()
            {
                let member = everyone && caller.principal.is_member(None)
                    || team.is_some_and(|team| {
                        caller.team_records.iter().any(|t| t.id == team.id)
                            && caller.principal.is_member(Some(&team.name))
                    });
                let within_boundary = if everyone {
                    caller.principal.permits_team(None)
                } else {
                    team.is_some_and(|team| caller.principal.permits_team(Some(&team.name)))
                };
                if caller.principal.team_boundary.is_some() && (!member || !within_boundary) {
                    continue;
                }
                let label = caller.username().map(str::to_string).or_else(|| {
                    meerschaum_server::routes::execute::owner_label(caller.auth.as_ref())
                });
                let owns = old.owner.is_some()
                    && old.owner == label
                    && record
                        .owner_binding
                        .as_ref()
                        .is_some_and(|binding| binding.matches(caller));
                // Unbound legacy records have no provable owner shortcut. Their current team
                // still sees public jobs, and its managers retain private-history oversight.
                if !owns {
                    let mut principal = caller.principal.clone();
                    principal.username.clear();
                    if !member
                        || !can_see_job(
                            &principal,
                            &JobAccess::from_properties(&record.name, &record.properties),
                        )
                    {
                        continue;
                    }
                }
            }
            visible.push(record);
        }
        Ok(visible)
    }

    pub(crate) async fn job_view(&self, record: Record) -> Result<Option<Record>, String> {
        Ok(self.job_views(vec![record]).await?.pop())
    }

    pub(crate) async fn team_id(&self, name: &str) -> Result<Option<Uuid>, String> {
        Ok(self
            .instance
            .get_team(name)
            .await
            .map_err(|_| "Could not resolve the registered team identity.")?
            .and_then(|t| t.id.and_then(|id| Uuid::parse_str(&id).ok())))
    }

    /// Only an explicit start with the canonical lease held may restore a missing definition.
    /// A failed or malformed existence read never authorizes creation or local fallback.
    pub(crate) async fn start_job(&self, record: &mut Record, api: &APIConnector) -> SuccessTuple {
        match remote_job_exists(api, &record.remote_name).await {
            Ok(true) => return api.start_job(&record.remote_name).await,
            Err(error) => return SuccessTuple::fail(error),
            Ok(false) => {}
        }
        match self.restore_job(record).await {
            Ok(result) => result,
            Err(error) => {
                self.suspend_job_context(record, false).await;
                SuccessTuple::fail(error)
            }
        }
    }

    /// Starting an already running process is idempotent. Recheck its original source without
    /// replacing its credential file, generation or revision underneath that live process.
    pub(crate) async fn keep_running_job(&self, record: &Record) -> SuccessTuple {
        let checked = async {
            let state = self.state()?;
            if let Some(grant) = record.callback {
                let identity = admit_job(self, &state, record, grant).await?;
                self.confirm_job_sources(record, &identity.caller()).await?;
                callbacks::load_identity(&state, grant, launch(record)?)
                    .await
                    .map_err(|_| "The running job's original authority is inactive.".to_string())?;
                if record.callback_suspended {
                    return Err("The running job's original authority is suspended.".into());
                }
            } else if !state.config.no_auth {
                return Err("The running job has no provable original source; stop it and recreate it explicitly.".into());
            }
            Ok::<_, String>(())
        }.await;
        match checked {
            Ok(()) => SuccessTuple::ok(format!("Job '{}' is already running.", record.name)),
            Err(error) => {
                self.suspend_job_context(record, false).await;
                SuccessTuple::fail(error)
            }
        }
    }

    async fn restore_job(&self, record: &mut Record) -> Result<SuccessTuple, String> {
        let canonical = Catalog::new(&self.root)
            .get(&record.name)?
            .filter(|current| {
                current.remote_name == record.remote_name
                    && current.callback == record.callback
                    && current.context_revision == record.context_revision
                    && !current.callback_suspended
            })
            .ok_or("This canonical job was removed, replaced or suspended.")?;
        *record = canonical;
        let grant = record.callback.ok_or(
            "The missing definition has no authenticated original source; recreate the job explicitly.",
        )?;
        if record.context_revision <= 1 || record.sysargs.is_empty() {
            return Err(
                "The missing definition needs a fresh explicit launch revision and stored command."
                    .into(),
            );
        }
        let state = self.state()?;
        let identity = admit_job(self, &state, record, grant).await?;
        let caller = identity.caller();
        let actor = Arc::new(caller);
        let api = context::scope(Some(actor), self.prepare_job_executor(record)).await?;
        // Preparation or policy checks may rebuild a runtime. Rotate only this already-issued
        // revision again, rechecking the original registration and current permissions.
        self.deliver_job_context(record, false).await?;
        let _ = admit_job(self, &state, record, grant).await?;
        let mut properties = record.properties.clone();
        for key in [
            meerschaum_core::job_access::TEAM_EXECUTION_KEY,
            meerschaum_jobs::config_files::TEAM_SCOPE_PROPERTY,
            meerschaum_core::job_access::OWNER_BINDING_KEY,
            meerschaum_core::job_access::TEAM_ID_KEY,
            environment::JOB_CONTEXT_PROPERTY,
        ] {
            properties.shift_remove(key);
        }
        properties.insert(
            environment::JOB_CONTEXT_PROPERTY.into(),
            launch(record)?.to_string().into(),
        );
        if let Some(Value::Map(execution)) = properties.get_mut("mrsm_execution") {
            execution.insert("kind".into(), "delegated_execution".into());
        }
        // Only the original main-owned name and command are used. Keep the reservation on a
        // failed/lost response: creation might have succeeded and must never be duplicated.
        Ok(api
            .create_job(&record.remote_name, &record.sysargs, Some(properties))
            .await)
    }

    pub(crate) async fn job_lock(&self, record: &Record) -> Result<std::fs::File, String> {
        meerschaum_server::executor::storage::lock(
            self.root.join("executors/job-leases"),
            launch(record)?,
        )
        .await
        .map_err(|_| "The job's launch lease is busy or unavailable.".into())
    }

    pub(super) async fn maintain_jobs(self: &Arc<Self>) {
        self.expire_host_jobs();
        let Ok(records) = Catalog::new(&self.root).records() else {
            return;
        };
        let mut tasks = tokio::task::JoinSet::new();
        for mut record in records
            .into_values()
            .filter(|r| r.callback.is_some() && !r.callback_suspended)
        {
            if tasks.len() >= 64 {
                let _ = tasks.join_next().await;
            }
            let controller = self.clone();
            tasks.spawn(async move {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    let Ok(_lock) = controller.job_lock(&record).await else {
                        return;
                    };
                    let Ok(Some(current)) = Catalog::new(&controller.root).get(&record.name) else {
                        return;
                    };
                    if current.remote_name != record.remote_name || current.callback_suspended {
                        return;
                    }
                    record = current;
                    let Ok(state) = controller.state() else {
                        return;
                    };
                    if let (Some(id), Ok(launch)) = (record.callback, launch(&record)) {
                        match callbacks::load_identity(&state, id, launch).await {
                            Err(error) if error.status.is_client_error() => {
                                controller.suspend_job_context(&record, false).await;
                                return;
                            }
                            Err(_) => return,
                            Ok(_) => {}
                        }
                    }
                    // A controlled rebuild may leave a stopped job waiting for its fresh
                    // same-revision context. Deliver before interpreting that state as completion.
                    if controller
                        .deliver_job_context(&mut record, false)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    if record.placement == JobPlacement::HostApi {
                        let Ok(jobs) = controller.host_jobs() else {
                            return;
                        };
                        let metadata =
                            context::scope(None, jobs.backend().try_get(&record.remote_name)).await;
                        if let Some(metadata) = metadata.ok().flatten()
                            && chrono::Utc::now().timestamp()
                                >= record.admitted_at.saturating_add(5)
                        {
                            if super::completed_result(&metadata).is_some() {
                                let _ = Catalog::new(&controller.root).observe_stopped(&record);
                                controller.suspend_job_context(&record, false).await;
                            } else if metadata["completion_unavailable"] == true {
                                // Missing/dead supervisor proof never keeps source authority
                                // alive or marks a write completed. Preserve uncertain history.
                                controller.suspend_job_context(&record, false).await;
                            }
                        }
                        return;
                    }
                    let Ok(api) = context::scope(None, controller.prepare_id(record.team_id)).await
                    else {
                        return;
                    };
                    let Ok(metadata) = api.get_job_metadata(&record.remote_name).await else {
                        return;
                    };
                    if chrono::Utc::now().timestamp() < record.admitted_at.saturating_add(5) {
                        return;
                    }
                    let Ok(metadata) = serde_json::to_value(metadata) else {
                        return;
                    };
                    if super::completed_result(&metadata).is_some() {
                        let _ = Catalog::new(&controller.root).observe_stopped(&record);
                        controller.suspend_job_context(&record, false).await;
                    } else if metadata["completion_unavailable"] == true {
                        controller.suspend_job_context(&record, false).await;
                    }
                })
                .await;
            });
        }
        while tasks.join_next().await.is_some() {}
    }
}

async fn remote_job_exists(api: &APIConnector, name: &str) -> Result<bool, String> {
    let response = api
        .make_request(
            Method::Get,
            &format!("/jobs/{name}/exists"),
            RequestOptions {
                timeout: Some(std::time::Duration::from_secs(5)),
                ..Default::default()
            },
        )
        .await
        .map_err(|_| "The executor did not confirm whether the canonical job exists.")?;
    if response.status != 200 {
        return Err("The executor did not confirm whether the canonical job exists.".into());
    }
    response
        .json()
        .map_err(|_| "The executor returned invalid canonical job existence metadata.")?
        .as_bool()
        .ok_or_else(|| "The executor returned invalid canonical job existence metadata.".into())
}

#[cfg(test)]
mod missing_definition_tests;

#[cfg(test)]
mod identity_tests;
