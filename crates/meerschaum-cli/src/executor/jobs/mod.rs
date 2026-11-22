//! Main-owned identity and routing for delegated jobs.
//!
//! An executor may report status and output, but can never choose a main-API job's owner,
//! team, public name, command, retention policy, filesystem path or another executor's identity.
//!
//! `catalog` is the canonical job catalog, `delegating_backend` routes every job operation of
//! `mrsm start api`, `log_sanitizer` cleans executor log chunks, `commit_journal` records durable
//! canonical commit transitions, `schedules` advances each saved graph's schedule, and
//! `source_tests` covers source ceilings across role changes.

use std::path::Path;

use meerschaum_core::SuccessTuple;
use meerschaum_core::{Map, Value};
use meerschaum_server::backends::{CreateJob, JobsBackend};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use std::sync::Arc;
use uuid::Uuid;

mod catalog;
mod commit_journal;
mod delegating_backend;
mod log_sanitizer;
mod schedules;
pub use catalog::Catalog;
pub use log_sanitizer::LogSanitizer;
pub(crate) use schedules::JobSchedule;

/// Immutable transport selected by main at registration, never inferred from a UUID or name.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobPlacement {
    #[default]
    Remote,
    HostApi,
}

/// Canonical job metadata, persisted before a delegated job is admitted.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    /// Public name selected and validated by the main API.
    pub name: String,
    /// Unpredictable executor-local name, never a path provided by an executor.
    pub remote_name: String,
    /// Stable team identity, independent of display-name changes or metadata responses.
    pub team_id: Uuid,
    /// Explicit host API-only placement; older records remain remote.
    #[serde(default)]
    pub placement: JobPlacement,
    /// Main-owned credential ceiling for this canonical launch, independent of restart caller.
    #[serde(default)]
    pub profile: meerschaum_server::executor::environment::Profile,
    /// Main-only typed endpoint admission; client properties never select narrower scopes.
    #[serde(default)]
    pub operation: Option<meerschaum_server::executor::pipe_operation::GraphOperation>,
    /// Main's durable schedule, separate from the one-shot worker and its callback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) schedule: Option<JobSchedule>,
    /// Approved service authority for a Team-owned plugin sync with a jobs-only callback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) team_pipe_authority: Option<String>,
    /// Main-only native target commit evidence; never projected into public job properties.
    #[serde(default)]
    pub commit_journal: meerschaum_server::executor::pipe_operation::commits::Journal,
    /// Main-created action endpoint execution; never accepted from job properties.
    #[serde(default)]
    pub action_request: bool,
    /// Main-created per-source fixed connector hook, never accepted from client properties.
    #[serde(default)]
    pub connector_registration: Option<meerschaum_server::executor::registration::Registration>,
    #[serde(default)]
    pub extension: Option<meerschaum_server::executor::extensions::Operation>,
    /// Main-created credential-free discovery; public properties cannot select this operation.
    #[serde(default)]
    pub discovery: Option<meerschaum_server::executor::plugin_discovery::Discovery>,
    /// Exact executable source snapshot selected by Main before callback admission.
    #[serde(default)]
    pub source_selection: Option<meerschaum_server::executor::registration::Sources>,
    /// Main API's command, after its command and object authorization.
    pub sysargs: Vec<String>,
    /// Main API's owner/team and approved job properties.
    pub properties: Map,
    /// Main API admission time, Unix seconds.
    pub created_at: i64,
    /// First observed stopped state, stamped by the main API (never an executor's timestamp).
    pub completed_at: Option<i64>,
    /// Private main callback authority; never included in public metadata.
    #[serde(default)]
    pub callback: Option<Uuid>,
    /// Main-owned launch revision; an explicit restart advances this before runtime delivery.
    #[serde(default = "first_revision")]
    pub context_revision: u64,
    /// Once stopped, its original proof remains private but no lease is renewed.
    #[serde(default)]
    pub callback_suspended: bool,
    /// The most recent accepted launch; tolerate its brief supervisor startup window.
    #[serde(default)]
    pub admitted_at: i64,
    /// Main-only registered owner identity; never exposed in job metadata.
    #[serde(default)]
    pub owner_binding: Option<meerschaum_server::executor::callbacks::OwnerBinding>,
}

fn first_revision() -> u64 {
    1
}

impl Record {
    /// Private typed discoveries bind an explicit archive or genuine Host source placement.
    /// Expiry belongs to admission; an expired retained record remains readable for cleanup.
    pub(crate) fn discovery_placement(&self) -> bool {
        use meerschaum_server::executor::plugin_host::Placement;
        self.discovery.as_ref().is_some_and(|discovery| {
            discovery.catalog.team_id == self.team_id
                && match (&discovery.catalog.placement, self.placement) {
                    (Placement::Remote, JobPlacement::Remote) => !self.team_id.is_nil(),
                    (Placement::Host(host), JobPlacement::HostApi) => {
                        host.team_id.unwrap_or_else(Uuid::nil) == self.team_id
                    }
                    _ => false,
                }
        })
    }

    pub(crate) fn extension_placement(&self) -> bool {
        self.extension.as_ref().is_some_and(|extension| {
            match (&extension.catalog.placement, self.placement) {
                (
                    meerschaum_server::executor::plugin_host::Placement::Remote,
                    JobPlacement::Remote,
                ) => extension.team_id() == Some(self.team_id),
                (
                    meerschaum_server::executor::plugin_host::Placement::Host(_),
                    JobPlacement::HostApi,
                ) => extension.team_id().unwrap_or_else(Uuid::nil) == self.team_id,
                _ => false,
            }
        })
    }
    fn valid_placement(&self) -> bool {
        if self.schedule.is_some()
            != matches!(&self.operation, Some(meerschaum_server::executor::pipe_operation::GraphOperation::Stored(job)) if job.intent.cadence().ok().flatten().is_some())
            || !self.valid_team_pipe_authority()
            || self.commit_journal.validate().is_err()
            || self.commit_journal.has_pending()
                && (self.operation.is_none()
                    || self.profile != meerschaum_server::executor::environment::Profile::Api)
        {
            return false;
        }
        if let Some(extension) = &self.extension
            && (self.profile != meerschaum_server::executor::environment::Profile::Api
                || !self.extension_placement()
                || self.discovery.is_some()
                || self.operation.is_some()
                || self.action_request
                || self.connector_registration.is_some()
                || extension.sysargs() != self.sysargs
                || extension.validated().is_err())
        {
            return false;
        }
        if let Some(registration) = &self.connector_registration
            && (self.profile != meerschaum_server::executor::environment::Profile::Api
                || self.discovery.is_some()
                || self.operation.is_some()
                || self.action_request
                || registration.sysargs() != self.sysargs
                || registration.validate().is_err())
        {
            return false;
        }
        if let Some(operation) = &self.operation
            && (self.profile != meerschaum_server::executor::environment::Profile::Api
                || self.discovery.is_some()
                || self.action_request
                || operation.sysargs().ok().as_ref() != Some(&self.sysargs))
        {
            return false;
        }
        if self.profile == meerschaum_server::executor::environment::Profile::Metadata
            && (!self.discovery_placement()
                || self
                    .discovery
                    .as_ref()
                    .is_none_or(|operation| operation.sysargs() != self.sysargs)
                || self.operation.is_some()
                || self.connector_registration.is_some()
                || self.action_request)
        {
            return false;
        }
        if self.discovery.is_some()
            && self.profile != meerschaum_server::executor::environment::Profile::Metadata
        {
            return false;
        }
        match self.placement {
            JobPlacement::Remote => !self.team_id.is_nil(),
            JobPlacement::HostApi => {
                let team = meerschaum_core::job_access::JobAccess::from_properties(
                    &self.name,
                    &self.properties,
                )
                .team;
                (self.profile == meerschaum_server::executor::environment::Profile::Api
                    || self.profile == meerschaum_server::executor::environment::Profile::Metadata
                        && self.discovery_placement())
                    && self.callback.is_some_and(|id| !id.is_nil())
                    && self.owner_binding.is_some()
                    && (self.team_id.is_nil() == team.is_none())
            }
        }
    }

    /// Reserve a fresh executor-local identity for an accepted public job.
    pub fn new(name: &str, team_id: Uuid, sysargs: Vec<String>, properties: Map) -> Self {
        Self {
            name: name.into(),
            remote_name: format!("mrsm-{}", Uuid::new_v4().simple()),
            team_id,
            placement: JobPlacement::Remote,
            profile: meerschaum_server::executor::environment::Profile::Team,
            operation: None,
            schedule: None,
            commit_journal: Default::default(),
            discovery: None,
            source_selection: None,
            team_pipe_authority: None,
            action_request: false,
            connector_registration: None,
            extension: None,
            sysargs,
            properties,
            created_at: chrono::Utc::now().timestamp(),
            completed_at: None,
            callback: None,
            context_revision: 1,
            callback_suspended: false,
            admitted_at: chrono::Utc::now().timestamp(),
            owner_binding: None,
        }
    }

    /// Public metadata: only typed status/result/health values come from the executor. Every
    /// property, ownership field and command always comes from this main-owned record.
    fn trusted_properties(&self) -> Map {
        let mut properties = self.properties.clone();
        properties.shift_remove(meerschaum_core::job_access::OWNER_BINDING_KEY);
        if let Some(binding) = self
            .owner_binding
            .as_ref()
            .and_then(|binding| serde_json::to_string(binding).ok())
            .and_then(|json| Value::from_json_str(&json).ok())
        {
            properties.insert(
                meerschaum_core::job_access::OWNER_BINDING_KEY.into(),
                binding,
            );
        }
        properties.insert(
            meerschaum_core::job_access::TEAM_ID_KEY.into(),
            if self.placement == JobPlacement::HostApi && self.team_id.is_nil() {
                Value::Null
            } else {
                self.team_id.to_string().into()
            },
        );
        properties
    }

    pub fn metadata(&self, remote: Option<&Json>) -> Json {
        let properties = self.trusted_properties();
        let status = remote
            .and_then(|v| v.get("status"))
            .and_then(Json::as_str)
            .filter(|s| matches!(*s, "running" | "paused" | "stopped"))
            .unwrap_or("unavailable");
        let completion_pending =
            status == "stopped" && remote.is_some_and(|value| value["completion_pending"] == true);
        let completion_unavailable = status == "unavailable"
            && remote.is_some_and(|value| value["completion_unavailable"] == true);
        let result = remote
            .filter(|_| !completion_pending && !completion_unavailable)
            .and_then(|v| v.get("result"))
            .and_then(Json::as_array)
            .filter(|a| a.len() == 2 && a[0].is_boolean() && a[1].is_string())
            .map(|a| json!([a[0], text(a[1].as_str().unwrap_or_default(), 16 * 1024)]));
        let mut health = json!({"state": "unknown", "message": ""});
        if completion_unavailable {
            health["message"] = "The native job's final result is unavailable.".into();
        } else if status == "unavailable" {
            health["message"] = "The selected executor is unavailable.".into();
        } else if let Some(value) = remote.and_then(|v| v.get("health")) {
            if let Some(state) = value.get("state").and_then(Json::as_str).filter(|s| {
                matches!(
                    *s,
                    "healthy" | "stalled" | "failing" | "stopped" | "unknown"
                )
            }) {
                health["state"] = state.into();
            }
            if let Some(message) = value.get("message").and_then(Json::as_str) {
                health["message"] = text(message, 4096).into();
            }
            for key in ["consecutive_failures", "expected_interval_seconds"] {
                if let Some(number) = value
                    .get(key)
                    .and_then(Json::as_f64)
                    .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 1_000_000_000.0)
                {
                    health[key] = json!(number);
                }
            }
        }
        let restart = self
            .properties
            .get("restart")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || self
                .sysargs
                .iter()
                .any(|s| matches!(s.as_str(), "--restart" | "--loop" | "-s" | "--schedule"));
        let public_words = match &self.operation {
            Some(meerschaum_server::executor::pipe_operation::GraphOperation::Stored(job)) => {
                job.intent.original()
            }
            _ => self.sysargs.as_slice(),
        };
        let mut result = json!({"sysargs": public_words, "result": result, "restart": restart, "status": status,
            "completion_pending": completion_pending, "completion_unavailable": completion_unavailable,
            "daemon": {"status": status, "pid": null, "properties": properties}, "health": health,
            "executor": {"team_id": if self.team_id.is_nil() { None } else { Some(self.team_id) },
                "placement": self.placement, "profile": self.profile, "available": status != "unavailable"}});
        if let Some(schedule) = &self.schedule {
            schedule.metadata(&mut result);
        }
        result
    }
}

fn text(value: &str, limit: usize) -> String {
    // Metadata never carries terminal controls or instructions to the main host. Live log
    // streams use a separate stateful sanitizer, preserving safe color sequences only.
    value
        .chars()
        .filter(|c| !c.is_control() || matches!(c, '\n' | '\t'))
        .take(limit)
        .collect()
}

enum StreamPublication {
    Pending,
    Published(SuccessTuple),
    Unavailable,
}

fn stream_publication(metadata: &Json, launch: &str) -> StreamPublication {
    if metadata["daemon"]["properties"]["process"]["launch_id"].as_str() != Some(launch)
        || metadata["completion_unavailable"] == true
    {
        return StreamPublication::Unavailable;
    }
    if let Some((success, message)) = crate::executor::client::completed_result(metadata) {
        return StreamPublication::Published(SuccessTuple {
            success,
            message: text(message, 16 * 1024),
        });
    }
    if metadata["status"] == "stopped" && metadata["completion_pending"] == true {
        StreamPublication::Pending
    } else {
        StreamPublication::Unavailable
    }
}

/// Local jobs plus main-owned placement for delegated jobs. All mutations consult the catalog;
/// lookup or transport failure never selects the local backend as a fallback.
pub struct DelegatingJobs {
    local: Arc<dyn JobsBackend>,
    controller: Arc<crate::executor::client::Controller>,
    catalog: Catalog,
    creates: tokio::sync::Mutex<()>,
}

enum Placement {
    Local,
    Hidden,
    Remote(Box<Record>),
}

impl DelegatingJobs {
    async fn placement(&self, name: &str) -> Result<Placement, String> {
        let records = self.catalog.records()?;
        Ok(match records.get(name).cloned() {
            None if records.values().any(|record| {
                record.placement == JobPlacement::HostApi && record.remote_name == name
            }) =>
            {
                Placement::Hidden
            }
            None => Placement::Local,
            Some(record) => match self.controller.job_view(record).await? {
                Some(record) => Placement::Remote(Box::new(record)),
                None => Placement::Hidden,
            },
        })
    }

    /// Wrap the main API's local backend; executors themselves use ordinary LocalJobsBackend.
    pub fn new(
        root: &Path,
        local: Arc<dyn JobsBackend>,
        controller: Arc<crate::executor::client::Controller>,
    ) -> Self {
        Self {
            local,
            controller,
            catalog: Catalog::new(root),
            creates: tokio::sync::Mutex::new(()),
        }
    }

    /// A tracked action's caller receives the exact successful admission directly.
    /// Ordinary job requests never supply a receiver.
    pub(crate) async fn create_canonical(
        &self,
        name: &str,
        sysargs: Vec<String>,
        properties: Option<Map>,
        receipt: Option<&mut Option<crate::executor::client::NoAuthAction>>,
    ) -> CreateJob {
        let caller = meerschaum_server::executor::context::current();
        if caller.as_ref().is_none_or(|caller| caller.no_auth) {
            match self.controller.allow_unattributed_jobs() {
                Ok(true) => {}
                Ok(false) => {
                    return CreateJob::Unavailable(
                        "Authenticated job creation requires its accepted original source context."
                            .into(),
                    );
                }
                Err(error) => return CreateJob::Unavailable(error),
            }
        } else if caller.as_ref().is_some_and(|caller| caller.auth.is_none()) {
            return CreateJob::Unavailable("The job has no authenticated original source.".into());
        }
        let _create = self.creates.lock().await;
        match self.catalog.get(name) {
            Ok(Some(_)) => return CreateJob::Exists,
            Err(error) => return CreateJob::Unavailable(error),
            Ok(None) => {}
        }
        if self.local.exists(name).await {
            return CreateJob::Exists;
        }
        let mut properties = properties.unwrap_or_default();
        let ownership = meerschaum_core::job_access::JobAccess::from_properties(name, &properties);
        let selected = match self.controller.selected(ownership.team.as_deref()).await {
            Err(error) => return CreateJob::Unavailable(error),
            Ok(team) => team,
        };
        let extension = meerschaum_server::executor::context::extension();
        if extension.is_some()
            && caller
                .as_ref()
                .is_none_or(|caller| caller.no_auth || caller.auth.is_none())
        {
            return CreateJob::Unavailable(
                "A confined extension needs its original authenticated source.".into(),
            );
        }
        let host_api = selected.is_none()
            && (extension.is_some()
                || caller.as_ref().is_some_and(|caller| {
                    !caller.no_auth
                        && !caller.full_admin()
                        && (api_parent(caller)
                            || !meerschaum_server::team_execution::code_running_team(caller)
                                .is_some_and(|team| {
                                    meerschaum_core::access::same_team(
                                        Some(&team.name),
                                        ownership.team.as_deref(),
                                    )
                                }))
                }));
        if selected.is_none() && !host_api {
            return self.local.create(name, sysargs, Some(properties)).await;
        }
        let team_id = if let Some(team) = &selected {
            match team.id.as_deref().and_then(|v| Uuid::parse_str(v).ok()) {
                Some(id) if !id.is_nil() => id,
                _ => {
                    return CreateJob::Unavailable(
                        "The delegated job needs a registered UUID team.".into(),
                    );
                }
            }
        } else if let Some(team) = ownership.team.as_deref() {
            match self.controller.team_id(team).await {
                Ok(Some(id)) if !id.is_nil() => id,
                _ => {
                    return CreateJob::Unavailable(
                        "The host job's registered team is unavailable.".into(),
                    );
                }
            }
        } else {
            Uuid::nil()
        };
        let prepared = match &selected {
            Some(team) => match self.controller.prepare_bound(team).await {
                Ok(prepared) => Some(prepared),
                Err(error) => return CreateJob::Unavailable(error),
            },
            None => match self.controller.host_jobs() {
                Ok(_) => None,
                Err(error) => return CreateJob::Unavailable(error),
            },
        };
        meerschaum_core::job_access::JobAccess::stamp(
            &mut properties,
            ownership.owner.as_deref(),
            selected
                .as_ref()
                .map(|team| team.name.as_str())
                .or(ownership.team.as_deref()),
        );
        properties.shift_remove(meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY);
        let mut record = Record::new(name, team_id, sysargs, properties);
        if let Some(operation) = meerschaum_server::executor::context::pipe_operation() {
            if operation.sysargs().ok().as_ref() != Some(&record.sysargs) {
                return CreateJob::Unavailable(
                    "The typed endpoint operation differs from its actual command.".into(),
                );
            }
            record.operation = Some((*operation).clone());
        }
        record.action_request =
            record.operation.is_none() && meerschaum_server::executor::context::action_request();
        if let Some(registration) = meerschaum_server::executor::context::connector_registration() {
            if record.operation.is_some() || registration.sysargs() != record.sysargs {
                return CreateJob::Unavailable(
                    "The connector registration differs from its fixed operation.".into(),
                );
            }
            record.connector_registration = Some((*registration).clone());
            record.action_request = false;
        }
        if let Some(extension) = extension {
            if record.operation.is_some()
                || record.connector_registration.is_some()
                || extension.sysargs() != record.sysargs
                || extension.team_id().unwrap_or_else(Uuid::nil) != team_id
                || !matches!(
                    (&extension.catalog.placement, selected.is_some()),
                    (
                        meerschaum_server::executor::plugin_host::Placement::Remote,
                        true
                    ) | (
                        meerschaum_server::executor::plugin_host::Placement::Host(_),
                        false
                    )
                )
            {
                return CreateJob::Unavailable(
                    "The extension differs from its exact selected placement.".into(),
                );
            }
            record.extension = Some((*extension).clone());
            record.action_request = false;
        }
        record.profile = match source_profile(caller.as_deref(), team_id) {
            Ok(profile) => profile,
            Err(error) => return CreateJob::Unavailable(error),
        };
        if record.extension.is_some() {
            // Narrow this one sealed operation after normal registered-Team selection.
            // Generic Api/Team admission and connector registration are unchanged.
            record.profile = meerschaum_server::executor::environment::Profile::Api;
        }
        if record.connector_registration.is_some()
            && record.profile != meerschaum_server::executor::environment::Profile::Api
        {
            return CreateJob::Unavailable(
                "A connector registration requires its API-only source ceiling.".into(),
            );
        }
        if host_api {
            if record.profile != meerschaum_server::executor::environment::Profile::Api {
                return CreateJob::Unavailable(
                    "The host job has a different source profile.".into(),
                );
            }
            record.placement = JobPlacement::HostApi;
        }
        record.callback = match self
            .controller
            .register_job_callback(&mut record, prepared.as_ref())
            .await
        {
            Ok(callback) => callback,
            Err(error) => return CreateJob::Unavailable(error),
        };
        // The initial inactive grant has not been delivered. Commit the schedule only after
        // rechecking the public create request so another API worker can safely claim it.
        if record.schedule.is_some()
            && let Err(error) = self.controller.confirm_graph_request(&record, false).await
        {
            if let Some(id) = record.callback {
                self.controller.discard_job_callback(id).await;
            }
            return CreateJob::Unavailable(error);
        }
        let reserved = self.catalog.reserve(record.clone());
        if !matches!(reserved, Ok(true))
            && let Some(id) = record.callback
        {
            self.controller.discard_job_callback(id).await;
        }
        match reserved {
            Ok(true) => {}
            Ok(false) => return CreateJob::Exists,
            Err(error) => return CreateJob::Unavailable(error),
        }
        if record.schedule.is_some() {
            return CreateJob::Started(SuccessTuple::ok(format!(
                "Scheduled job '{}'.",
                record.name
            )));
        }
        let _lease_lock = match self.controller.job_lock(&record).await {
            Ok(lock) => lock,
            Err(error) => return CreateJob::Unavailable(error),
        };
        if let Err(error) = self
            .controller
            .deliver_job_context(&mut record, false)
            .await
        {
            return CreateJob::Unavailable(error);
        }
        if let Err(error) = self.controller.confirm_graph_request(&record, false).await {
            self.controller.suspend_job_context(&record, true).await;
            return CreateJob::Unavailable(error);
        }
        let delegated = delegated_properties(&record);
        let result = if let Some(prepared) = prepared {
            let result = prepared
                .api
                .create_job(&record.remote_name, &record.sysargs, Some(delegated))
                .await;
            if result.success
                && let Some(receipt) = receipt
            {
                *receipt = self.controller.no_auth_action(&record, &prepared);
            }
            result
        } else {
            let host = match self.controller.host_jobs() {
                Ok(host) => host,
                Err(error) => return CreateJob::Unavailable(error),
            };
            match meerschaum_server::executor::context::scope(
                None,
                host.backend()
                    .create(&record.remote_name, record.sysargs.clone(), Some(delegated)),
            )
            .await
            {
                CreateJob::Started(result) => result,
                CreateJob::Unavailable(error) => return CreateJob::Unavailable(error),
                CreateJob::Exists => return CreateJob::Exists,
            }
        };
        // Keep an uncertain reservation on failure: a lost response may follow a successful
        // remote creation. A retry must never run a duplicate operation under another owner.
        CreateJob::Started(SuccessTuple {
            success: result.success,
            message: text(&result.message, 16 * 1024),
        })
    }

    async fn remote_metadata(&self, record: &Record) -> Json {
        let remote = if record.placement == JobPlacement::HostApi {
            match self.controller.host_jobs() {
                Ok(host) => meerschaum_server::executor::context::scope(
                    None,
                    host.backend().try_get(&record.remote_name),
                )
                .await
                .ok()
                .flatten(),
                Err(_) => None,
            }
        } else {
            match self.controller.prepare_id(record.team_id).await {
                Ok(api) => api
                    .get_job_metadata(&record.remote_name)
                    .await
                    .ok()
                    .and_then(|m| serde_json::to_value(m).ok()),
                Err(_) => None,
            }
        };
        if record.callback.is_none()
            && remote
                .as_ref()
                .is_some_and(|m| m["status"] == "stopped" && m["completion_pending"] != true)
        {
            let _ = self.catalog.observe_stopped(record);
        }
        record.metadata(remote.as_ref())
    }

    async fn mutate(&self, name: &str, operation: &str) -> Option<SuccessTuple> {
        let mut record = match self.placement(name).await {
            Ok(Placement::Local) => return None,
            Ok(Placement::Hidden) => return Some(SuccessTuple::fail("This job was not found.")),
            Ok(Placement::Remote(record)) => record,
            Err(error) => return Some(SuccessTuple::fail(error)),
        };
        let _lease_lock = match self.controller.job_lock(&record).await {
            Ok(lock) => lock,
            Err(error) => return Some(SuccessTuple::fail(error)),
        };
        record = match self.placement(name).await {
            Ok(Placement::Remote(current)) if current.remote_name == record.remote_name => current,
            _ => {
                return Some(SuccessTuple::fail(
                    "This canonical job was removed or replaced.",
                ));
            }
        };
        if let Some(result) = self.schedule_control(&record, operation).await {
            return Some(result);
        }
        let operation = if operation == "pause" && record.schedule.is_some() {
            "stop"
        } else {
            operation
        };
        if operation == "start" {
            if record
                .operation
                .as_ref()
                .is_some_and(|op| op.pipeline_selection().is_some())
            {
                return Some(SuccessTuple::fail(
                    "Submit a new Pipeline run to use its current targets and revision.",
                ));
            }
            if matches!(
                &record.operation,
                Some(meerschaum_server::executor::pipe_operation::GraphOperation::Direct(_))
            ) {
                return Some(SuccessTuple::fail(
                    "A direct action is a single launch. Submit the original action to obtain a fresh window.",
                ));
            }
            if record.profile == meerschaum_server::executor::environment::Profile::Metadata {
                return Some(SuccessTuple::fail(
                    "Metadata discovery cannot be restarted. Trigger a new admitted operation to discover again.",
                ));
            }
            if matches!(
                &record.operation,
                Some(meerschaum_server::executor::pipe_operation::GraphOperation::Stored(_))
            ) && let Err(error) = self.controller.confirm_stored_start(&record).await
            {
                return Some(SuccessTuple::fail(error));
            }
            match self.preflight_start(&record).await {
                Ok(true) => return Some(self.controller.keep_running_job(&record).await),
                Ok(false) => {}
                Err(error) => return Some(SuccessTuple::fail(error)),
            }
        }
        if operation == "start"
            && let Err(error) = self.controller.deliver_job_context(&mut record, true).await
        {
            return Some(SuccessTuple::fail(error));
        }
        if operation == "start"
            && let Err(error) = self.controller.confirm_graph_request(&record, true).await
        {
            self.controller.suspend_job_context(&record, false).await;
            return Some(SuccessTuple::fail(error));
        }
        if record.placement == JobPlacement::HostApi {
            return Some(self.mutate_host(&record, operation).await);
        }
        let prepared = if operation == "start" {
            self.controller.prepare_job_executor(&record).await
        } else {
            self.controller.prepare_id(record.team_id).await
        };
        let api = match prepared {
            Ok(api) => api,
            Err(error) => return Some(SuccessTuple::fail(error)),
        };
        let result = match operation {
            "start" => self.controller.start_job(&mut record, &api).await,
            "stop" => api.stop_job(&record.remote_name).await,
            "pause" => api.pause_job(&record.remote_name).await,
            "delete" => api.delete_job(&record.remote_name).await,
            "promote" => {
                use meerschaum_connector_api::client::RequestOptions;
                let path = format!("/execute/{}/promote", record.remote_name);
                match api.post(&path, RequestOptions::default()).await {
                    Ok(response) if response.ok() => SuccessTuple::success(),
                    _ => SuccessTuple::fail("The selected executor could not promote the job."),
                }
            }
            _ => SuccessTuple::fail("Invalid delegated job operation."),
        };
        if result.success {
            if matches!(operation, "stop" | "delete") {
                self.controller
                    .suspend_job_context(&record, operation == "delete")
                    .await;
            }
            let saved = match operation {
                "delete" => self.catalog.remove(&record).map(|_| ()),
                "start" => self.catalog.update(&record, |r| r.completed_at = None),
                "stop" => self.catalog.observe_stopped(&record),
                "promote" => self.catalog.update(&record, |r| {
                    if let Some(Value::Map(execution)) = r.properties.get_mut("mrsm_execution") {
                        execution.insert("kind".into(), "persistent_job".into());
                    }
                }),
                _ => Ok(()),
            };
            if let Err(error) = saved {
                return Some(SuccessTuple::fail(error));
            }
        }
        Some(SuccessTuple {
            success: result.success,
            message: text(&result.message, 16 * 1024),
        })
    }

    /// A paused process retains its installed Landlock rules and memory. Do not rotate its
    /// source grant or revision before requiring a stop and a fresh native launch.
    async fn preflight_start(&self, record: &Record) -> Result<bool, String> {
        let metadata = if record.placement == JobPlacement::HostApi {
            let host = self.controller.host_jobs()?;
            if !host.definition_exists(&record.remote_name)? {
                return Ok(false);
            }
            meerschaum_server::executor::context::scope(
                None,
                host.backend().try_get(&record.remote_name),
            )
            .await?
            .ok_or("The canonical host definition could not be read.")?
        } else {
            use meerschaum_connector_api::client::{Method, RequestOptions};
            let api = self.controller.prepare_id(record.team_id).await?;
            let response = api
                .make_request(
                    Method::Get,
                    &format!("/jobs/{}/exists", record.remote_name),
                    RequestOptions {
                        timeout: Some(std::time::Duration::from_secs(5)),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|_| "The executor did not confirm canonical job existence.")?;
            if response.status != 200 {
                return Err("The executor did not confirm canonical job existence.".into());
            }
            match response.json().ok().and_then(|value| value.as_bool()) {
                Some(false) => return Ok(false),
                Some(true) => {}
                None => return Err("The executor returned invalid canonical job existence.".into()),
            }
            let metadata = api
                .get_job_metadata(&record.remote_name)
                .await
                .map_err(|_| "The executor's current job state is unavailable.")?;
            serde_json::to_value(metadata).map_err(|_| "Invalid executor job metadata.")?
        };
        match metadata.get("status").and_then(Json::as_str) {
            Some("paused") => Err("This job is paused. Stop it, then start it again to apply its current source permissions in a fresh process.".into()),
            Some("running") => Ok(true),
            Some("stopped") if metadata["completion_pending"] == true => {
                Err("The previous job result is still being finalized.".into())
            }
            Some("stopped") => Ok(false),
            _ => Err("The canonical job's current state could not be verified.".into()),
        }
    }

    async fn mutate_host(&self, record: &Record, operation: &str) -> SuccessTuple {
        let host = match self.controller.host_jobs() {
            Ok(host) => host,
            Err(error) => return SuccessTuple::fail(error),
        };
        let backend = host.backend();
        let exists = if operation == "start" {
            match host.definition_exists(&record.remote_name) {
                Ok(exists) => exists,
                Err(error) => return SuccessTuple::fail(error),
            }
        } else {
            false
        };
        let result = meerschaum_server::executor::context::scope(None, async {
            match operation {
                "start" if exists => backend.start(&record.remote_name).await,
                "start" => match backend
                    .create(
                        &record.remote_name,
                        record.sysargs.clone(),
                        Some(delegated_properties(record)),
                    )
                    .await
                {
                    CreateJob::Started(result) => result,
                    CreateJob::Exists => SuccessTuple::fail("The private host job already exists."),
                    CreateJob::Unavailable(error) => SuccessTuple::fail(error),
                },
                "stop" => backend.stop(&record.remote_name).await,
                "pause" => backend.pause(&record.remote_name).await,
                "delete" => backend.delete(&record.remote_name).await,
                "promote" => backend.promote(&record.remote_name).await,
                _ => SuccessTuple::fail("Invalid canonical host job operation."),
            }
        })
        .await;
        if result.success {
            if matches!(operation, "stop" | "delete") {
                self.controller
                    .suspend_job_context(record, operation == "delete")
                    .await;
            }
            let saved = match operation {
                "delete" => self.catalog.remove(record).map(|_| ()),
                "start" => self.catalog.update(record, |r| r.completed_at = None),
                "stop" => self.catalog.observe_stopped(record),
                "promote" => self.catalog.update(record, |r| {
                    if let Some(Value::Map(execution)) = r.properties.get_mut("mrsm_execution") {
                        execution.insert("kind".into(), "persistent_job".into());
                    }
                }),
                _ => Ok(()),
            };
            if let Err(error) = saved {
                return SuccessTuple::fail(error);
            }
        }
        SuccessTuple {
            success: result.success,
            message: text(&result.message, 16 * 1024),
        }
    }
}

fn api_parent(caller: &meerschaum_server::access::Access) -> bool {
    matches!(caller.auth.as_ref(), Some(meerschaum_server::auth::Principal::Execution(identity))
        if identity.grant.purpose == meerschaum_server::executor::callbacks::Purpose::Command)
}

/// Classify the accepted source before registration. Fresh callbacks recheck the decision;
/// this recorded ceiling cannot be replaced by a later manager or a more privileged parent.
fn source_profile(
    caller: Option<&meerschaum_server::access::Access>,
    team_id: Uuid,
) -> Result<meerschaum_server::executor::environment::Profile, String> {
    use meerschaum_server::executor::environment::Profile;
    let Some(caller) = caller.filter(|caller| !caller.no_auth) else {
        return Ok(Profile::Team);
    };
    if api_parent(caller) {
        return Ok(Profile::Api);
    }
    if caller.full_admin()
        || meerschaum_server::team_execution::code_running_team(caller).is_some_and(|team| {
            team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok()) == Some(team_id)
        })
    {
        return Ok(Profile::Team);
    }
    if matches!(caller.auth.as_ref(), Some(meerschaum_server::auth::Principal::Execution(identity))
        if identity.grant.purpose == meerschaum_server::executor::callbacks::Purpose::Jobs)
    {
        return Err("This jobs-only source cannot become an API data source. Create a new job with the user's current API credentials.".into());
    }
    Ok(Profile::Api)
}

fn delegated_properties(record: &Record) -> Map {
    let mut delegated = record.trusted_properties();
    if record.callback.is_some() {
        delegated.insert(
            meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY.into(),
            record.remote_name.trim_start_matches("mrsm-").into(),
        );
    }
    delegated.shift_remove(meerschaum_core::job_access::TEAM_EXECUTION_KEY);
    delegated.shift_remove(meerschaum_jobs::config_files::TEAM_SCOPE_PROPERTY);
    // Main observes completion and owns retention for both transports.
    if let Some(Value::Map(execution)) = delegated.get_mut("mrsm_execution") {
        execution.insert("kind".into(), "delegated_execution".into());
    }
    delegated
}

fn valid_name(name: &str) -> bool {
    name.len() <= 4096
        && meerschaum_jobs::names::invalid_job_name_reason(name).is_none()
        && !meerschaum_jobs::job::BANNED_CHARS
            .iter()
            .any(|c| name.contains(*c))
}

#[cfg(test)]
mod source_tests;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod stream_publication_tests {
    use super::*;

    #[test]
    fn stopped_stream_requires_same_launch_supervisor_commit_or_known_pending_publication() {
        let launch = "first";
        let mut metadata = json!({
            "status":"stopped", "result":[false, "No result available."], "completion_pending":true,
            "daemon":{"properties":{"process":{"launch_id":launch, "began":"2026-11-17T10:00:00"},"result":null}}
        });
        assert!(matches!(
            stream_publication(&metadata, launch),
            StreamPublication::Pending
        ));
        assert!(matches!(
            stream_publication(&metadata, "replaced"),
            StreamPublication::Unavailable
        ));
        metadata["completion_pending"] = false.into();
        metadata["completion_unavailable"] = true.into();
        assert!(matches!(
            stream_publication(&metadata, launch),
            StreamPublication::Unavailable
        ));
        metadata["completion_unavailable"] = false.into();
        metadata["daemon"]["properties"]["process"]["ended"] = "2026-11-17T10:01:00".into();
        metadata["daemon"]["properties"]["result"] = json!([false, "real worker failure"]);
        assert!(
            matches!(stream_publication(&metadata, launch), StreamPublication::Published(result) if !result.success && result.message == "real worker failure")
        );
    }
}
