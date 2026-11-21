//! Main-owned identity and routing for delegated jobs.
//!
//! An executor may report status and output, but can never choose a main-API job's owner,
//! team, public name, command, retention policy, filesystem path or another executor's identity.
//!
//! `commit_journal` records durable canonical commit transitions, `schedules` advances each
//! saved graph's schedule, and `source_tests` covers source ceilings across role changes.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use meerschaum_core::SuccessTuple;
use meerschaum_core::team_scope::{create_private_dir, write_private};
use meerschaum_core::{Map, Value};
use meerschaum_server::backends::{CreateJob, JobsBackend, LogEvent, LogMonitor};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use std::sync::Arc;
use uuid::Uuid;

mod commit_journal;
mod schedules;
pub(crate) use schedules::JobSchedule;

const MAX_CATALOG_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CATALOG_JOBS: usize = 10_000;

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

/// Owner-only, process-shared canonical metadata. Only the main API reads and writes it.
#[derive(Clone)]
pub struct Catalog {
    directory: PathBuf,
}

impl Catalog {
    /// Use the main API's private executor storage, outside its public desired directory.
    pub fn new(root: &Path) -> Self {
        Self {
            directory: root.join(crate::executor_registry::DIRECTORY),
        }
    }

    fn lock_file(&self) -> Result<std::fs::File, String> {
        create_private_dir(&self.directory)
            .map_err(|_| "Could not create the main executor job catalog.")?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(self.directory.join("catalog.lock"))
            .map_err(|_| "Could not open the executor job catalog lock.")?;
        Ok(file)
    }

    fn lock(&self) -> Result<std::fs::File, String> {
        let file = self.lock_file()?;
        fs4::FileExt::lock(&file).map_err(|_| "Could not lock the executor job catalog.")?;
        Ok(file)
    }

    /// A concurrent status read or fsync may briefly own the lock. Retry only acquisition,
    /// before reading the canonical record or attempting any side effect.
    fn lock_with_deadline(file: &std::fs::File, shared: bool) -> Result<(), String> {
        let until = std::time::Instant::now() + std::time::Duration::from_secs(1);
        loop {
            let lock = if shared {
                fs4::FileExt::try_lock_shared(file)
            } else {
                fs4::FileExt::try_lock(file)
            };
            match lock {
                Ok(()) => return Ok(()),
                Err(fs4::TryLockError::WouldBlock) if std::time::Instant::now() < until => {
                    std::thread::sleep(std::time::Duration::from_millis(3));
                }
                Err(error) => {
                    return Err(format!(
                        "Could not acquire the canonical executor job catalog lock: {error}"
                    ));
                }
            }
        }
    }

    fn read(&self) -> Result<BTreeMap<String, Record>, String> {
        use std::io::Read;
        let file = match std::fs::File::open(self.directory.join("catalog.json")) {
            Ok(file) => file,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && !self.directory.join("catalog.initialized").exists() =>
            {
                return Ok(BTreeMap::new());
            }
            Err(_) => return Err("The main executor job catalog is unavailable.".into()),
        };
        let mut bytes = Vec::new();
        file.take(MAX_CATALOG_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "Could not read the executor job catalog.")?;
        if bytes.len() as u64 > MAX_CATALOG_BYTES {
            return Err("The executor job catalog exceeds its size limit.".into());
        }
        let records: BTreeMap<String, Record> =
            serde_json::from_slice(&bytes).map_err(|_| "Invalid main executor job catalog.")?;
        if records.len() > MAX_CATALOG_JOBS
            || records.iter().any(|(name, record)| {
                name != &record.name
                    || !valid_name(name)
                    || !record.valid_placement()
                    || record
                        .remote_name
                        .strip_prefix("mrsm-")
                        .and_then(|s| Uuid::parse_str(s).ok())
                        .is_none()
            })
        {
            return Err("Invalid main executor job catalog identity.".into());
        }
        Ok(records)
    }

    fn write(&self, records: &BTreeMap<String, Record>) -> Result<(), String> {
        let encoded = serde_json::to_string(records)
            .map_err(|_| "Could not encode the executor job catalog.")?;
        if encoded.len() as u64 > MAX_CATALOG_BYTES || records.len() > MAX_CATALOG_JOBS {
            return Err("The executor job catalog is at capacity; remove completed jobs before starting more.".into());
        }
        write_private(&self.directory.join("catalog.json"), &encoded)
            .map_err(|_| "Could not persist the executor job catalog.")?;
        write_private(&self.directory.join("catalog.initialized"), "1")
            .map_err(|_| "Could not initialize the executor job catalog.".into())
    }

    /// Definitions used for authorization and routing. Failure is an error, never an empty list.
    pub fn records(&self) -> Result<BTreeMap<String, Record>, String> {
        let file = self.lock_file()?;
        fs4::FileExt::lock_shared(&file).map_err(|_| "Could not read the executor job catalog.")?;
        self.read()
    }

    /// Callback reads never wait indefinitely behind another process's catalog mutation.
    pub(crate) fn records_if_available(&self) -> Result<BTreeMap<String, Record>, String> {
        let file = self.lock_file()?;
        Self::lock_with_deadline(&file, true)?;
        self.read()
    }

    /// One canonical definition; no network read is needed for visibility or ownership.
    pub fn get(&self, name: &str) -> Result<Option<Record>, String> {
        Ok(self.records()?.remove(name))
    }

    /// Reserve a public name before contacting an executor. Concurrent creators cannot both
    /// replace an existing owner, including when one executor is unavailable.
    pub fn reserve(&self, record: Record) -> Result<bool, String> {
        if !record.valid_placement() {
            return Err("Invalid canonical job placement.".into());
        }
        if !valid_name(&record.name) {
            return Err("Invalid delegated job name.".into());
        }
        let _lock = self.lock()?;
        let mut records = self.read()?;
        if records.contains_key(&record.name) {
            return Ok(false);
        }
        records.insert(record.name.clone(), record);
        self.write(&records)?;
        Ok(true)
    }

    /// Remove only the expected reservation. A superseded launch cannot remove a new owner's
    /// job after a delayed response from an old executor request.
    pub fn remove(&self, record: &Record) -> Result<bool, String> {
        let _lock = self.lock()?;
        let mut records = self.read()?;
        if records
            .get(&record.name)
            .is_none_or(|r| r.remote_name != record.remote_name)
        {
            return Ok(false);
        }
        if records
            .get(&record.name)
            .is_some_and(|current| current.commit_journal.has_pending())
        {
            return Err("This operation has an unfinished native commit intent. Its evidence cannot be deleted or automatically replayed; retire its owned runtime and inspect the original operation before starting fresh work.".into());
        }
        records.remove(&record.name);
        self.write(&records)?;
        Ok(true)
    }

    /// Stamp completion locally for retention, disregarding executor-supplied filesystem paths
    /// and timestamps. Metadata refreshes cannot repeatedly move the retention deadline.
    pub fn observe_stopped(&self, record: &Record) -> Result<(), String> {
        if record.callback.is_some()
            && chrono::Utc::now().timestamp() < record.admitted_at.saturating_add(5)
        {
            return Ok(());
        }
        if record.completed_at.is_some() {
            return Ok(());
        }
        let _lock = self.lock()?;
        let mut records = self.read()?;
        if let Some(current) = records
            .get_mut(&record.name)
            .filter(|r| r.remote_name == record.remote_name && r.completed_at.is_none())
        {
            current.completed_at = Some(chrono::Utc::now().timestamp());
            self.write(&records)?;
        }
        Ok(())
    }

    pub(crate) fn update(
        &self,
        record: &Record,
        change: impl FnOnce(&mut Record),
    ) -> Result<(), String> {
        let _lock = self.lock()?;
        let mut records = self.read()?;
        let current = records
            .get_mut(&record.name)
            .filter(|r| r.remote_name == record.remote_name)
            .ok_or("The delegated job was removed or replaced.")?;
        change(current);
        self.write(&records)
    }

    pub(crate) fn replace_callback(
        &self,
        record: &Record,
        previous: Uuid,
        next: Uuid,
        operation: Option<meerschaum_server::executor::pipe_operation::GraphOperation>,
    ) -> Result<u64, String> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("catalog.lock"))
            .map_err(|_| "Could not open the canonical callback lock.")?;
        Self::lock_with_deadline(&file, false)?;
        let mut records = self.read()?;
        let current = records
            .get_mut(&record.name)
            .filter(|r| {
                r.remote_name == record.remote_name
                    && r.callback == Some(previous)
                    && r.context_revision == record.context_revision
                    && r.profile == record.profile
                    && r.team_id == record.team_id
                    && r.placement == record.placement
                    && r.sysargs == record.sysargs
                    && r.operation == record.operation
                    && r.schedule == record.schedule
                    && r.source_selection == record.source_selection
                    && r.properties == record.properties
                    && r.owner_binding == record.owner_binding
                    && r.callback_suspended == record.callback_suspended
                    && r.commit_journal == record.commit_journal
                    && !r.commit_journal.has_pending()
            })
            .ok_or("The job's callback authority was removed or replaced.")?;
        current.callback = Some(next);
        current.callback_suspended = false;
        current.admitted_at = chrono::Utc::now().timestamp();
        current.operation = operation;
        current.context_revision = current
            .context_revision
            .checked_add(1)
            .ok_or("The job's launch revision is exhausted.")?;
        let revision = current.context_revision;
        self.write(&records)?;
        Ok(revision)
    }

    /// Canonical team retagging follows main-API administrative changes, never a remote reply.
    pub fn retag(&self, old: &str, new: Option<&str>) -> Result<Vec<String>, String> {
        let _lock = self.lock()?;
        let mut records = self.read()?;
        let mut changed = Vec::new();
        for record in records.values_mut() {
            let job = meerschaum_core::job_access::JobAccess::from_properties(
                &record.name,
                &record.properties,
            );
            if meerschaum_core::access::same_team(job.team.as_deref(), Some(old)) {
                meerschaum_core::job_access::JobAccess::stamp(
                    &mut record.properties,
                    job.owner.as_deref(),
                    new,
                );
                changed.push(record.name.clone());
            }
        }
        if !changed.is_empty() {
            self.write(&records)?;
        }
        Ok(changed)
    }

    fn retag_id(&self, id: Uuid, name: &str) -> Result<Vec<String>, String> {
        let _lock = self.lock()?;
        let mut records = self.read()?;
        let mut changed = Vec::new();
        for record in records.values_mut().filter(|r| r.team_id == id) {
            let access = meerschaum_core::job_access::JobAccess::from_properties(
                &record.name,
                &record.properties,
            );
            if access.team.as_deref() == Some(name) {
                continue;
            }
            meerschaum_core::job_access::JobAccess::stamp(
                &mut record.properties,
                access.owner.as_deref(),
                Some(name),
            );
            changed.push(record.name.clone());
        }
        if !changed.is_empty() {
            self.write(&records)?;
        }
        Ok(changed)
    }
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
    if let Some((success, message)) = crate::executor_client::completed_result(metadata) {
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
    controller: Arc<crate::executor_client::Controller>,
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
        controller: Arc<crate::executor_client::Controller>,
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
        receipt: Option<&mut Option<crate::executor_client::NoAuthAction>>,
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

#[async_trait]
impl JobsBackend for DelegatingJobs {
    fn supports_bound_commands(&self) -> bool {
        true
    }

    async fn ready(&self, name: &str) -> Result<(), String> {
        match self.placement(name).await? {
            Placement::Remote(record) => {
                if record.placement == JobPlacement::HostApi {
                    return self.controller.host_jobs().map(|_| ());
                }
                self.controller.prepare_id(record.team_id).await.map(|_| ())
            }
            Placement::Hidden => Err("This job was not found.".into()),
            Placement::Local => self.local.ready(name).await,
        }
    }

    async fn try_list(&self) -> Result<IndexMap<String, Json>, String> {
        let records = self.catalog.records()?;
        let mut jobs = self.local.try_list().await?;
        for name in records.keys() {
            jobs.shift_remove(name);
        }
        for record in records
            .values()
            .filter(|r| r.placement == JobPlacement::HostApi)
        {
            jobs.shift_remove(&record.remote_name);
        }
        let mut teams: BTreeMap<Uuid, Vec<Record>> = BTreeMap::new();
        for record in self
            .controller
            .job_views(records.into_values().collect())
            .await?
        {
            if record.placement == JobPlacement::HostApi {
                let metadata = self.remote_metadata(&record).await;
                jobs.insert(record.name.clone(), metadata);
                continue;
            }
            teams.entry(record.team_id).or_default().push(record);
        }
        for (team, records) in teams {
            let remote = match self.controller.prepare_id(team).await {
                Ok(api) => api.try_get_jobs().await.ok(),
                Err(_) => None,
            };
            for record in records {
                let metadata = remote
                    .as_ref()
                    .and_then(|jobs| jobs.get(&record.remote_name))
                    .and_then(|m| serde_json::to_value(m).ok());
                if record.callback.is_none()
                    && metadata.as_ref().is_some_and(|m| {
                        m["status"] == "stopped" && m["completion_pending"] != true
                    })
                {
                    let _ = self.catalog.observe_stopped(&record);
                }
                jobs.insert(record.name.clone(), record.metadata(metadata.as_ref()));
            }
        }
        Ok(jobs)
    }

    async fn list(&self) -> IndexMap<String, Json> {
        self.try_list().await.unwrap_or_default()
    }

    async fn try_list_definitions(&self) -> Result<IndexMap<String, Json>, String> {
        let records = self.catalog.records()?;
        let mut jobs = self.local.try_list_definitions().await?;
        for name in records.keys() {
            jobs.shift_remove(name);
        }
        for record in records
            .values()
            .filter(|r| r.placement == JobPlacement::HostApi)
        {
            jobs.shift_remove(&record.remote_name);
        }
        for record in self
            .controller
            .job_views(records.into_values().collect())
            .await?
        {
            jobs.insert(record.name.clone(), record.metadata(None));
        }
        Ok(jobs)
    }

    async fn list_definitions(&self) -> IndexMap<String, Json> {
        self.try_list_definitions().await.unwrap_or_default()
    }

    async fn try_get(&self, name: &str) -> Result<Option<Json>, String> {
        match self.placement(name).await? {
            Placement::Remote(record) => Ok(Some(self.remote_metadata(&record).await)),
            Placement::Hidden => Ok(None),
            Placement::Local => self.local.try_get(name).await,
        }
    }

    async fn get(&self, name: &str) -> Option<Json> {
        self.try_get(name).await.ok().flatten()
    }

    async fn exists(&self, name: &str) -> bool {
        match self.placement(name).await {
            Ok(Placement::Remote(_)) => true,
            Ok(Placement::Hidden) => false,
            Ok(Placement::Local) => self.local.exists(name).await,
            // A corrupted catalog must not authorize replacing an unknown existing job.
            Err(_) => true,
        }
    }

    async fn create(&self, name: &str, sysargs: Vec<String>, properties: Option<Map>) -> CreateJob {
        self.create_canonical(name, sysargs, properties, None).await
    }

    async fn promote(&self, name: &str) -> SuccessTuple {
        match self.mutate(name, "promote").await {
            Some(result) => result,
            None => self.local.promote(name).await,
        }
    }
    async fn delete(&self, name: &str) -> SuccessTuple {
        match self.mutate(name, "delete").await {
            Some(result) => result,
            None => self.local.delete(name).await,
        }
    }
    async fn start(&self, name: &str) -> SuccessTuple {
        match self.mutate(name, "start").await {
            Some(result) => result,
            None => self.local.start(name).await,
        }
    }
    async fn stop(&self, name: &str) -> SuccessTuple {
        match self.mutate(name, "stop").await {
            Some(result) => result,
            None => self.local.stop(name).await,
        }
    }
    async fn pause(&self, name: &str) -> SuccessTuple {
        match self.mutate(name, "pause").await {
            Some(result) => result,
            None => self.local.pause(name).await,
        }
    }

    async fn logs(&self, name: &str) -> Option<String> {
        match self.placement(name).await {
            Ok(Placement::Remote(record)) => {
                if record.placement == JobPlacement::HostApi {
                    let backend = self.controller.host_jobs().ok()?.backend();
                    let logs = meerschaum_server::executor::context::scope(
                        None,
                        backend.logs(&record.remote_name),
                    )
                    .await?;
                    return Some(LogSanitizer::default().push(&logs));
                }
                let api = self.controller.prepare_id(record.team_id).await.ok()?;
                let logs = api.get_logs(&record.remote_name).await.ok()?;
                Some(LogSanitizer::default().push(&logs))
            }
            Ok(Placement::Local) => self.local.logs(name).await,
            _ => None,
        }
    }

    async fn stop_time(&self, name: &str) -> Option<DateTime<Utc>> {
        match self.placement(name).await {
            Ok(Placement::Remote(record)) => {
                let _ = self.remote_metadata(&record).await;
                self.catalog
                    .get(name)
                    .ok()
                    .flatten()?
                    .completed_at
                    .and_then(|t| DateTime::from_timestamp(t, 0))
            }
            Ok(Placement::Local) => self.local.stop_time(name).await,
            _ => None,
        }
    }

    async fn completion_time(&self, name: &str) -> Option<DateTime<Utc>> {
        match self.placement(name).await {
            Ok(Placement::Remote(_)) => self.stop_time(name).await,
            Ok(Placement::Local) => self.local.completion_time(name).await,
            _ => None,
        }
    }

    async fn is_blocking_on_stdin(&self, name: &str) -> bool {
        match self.placement(name).await {
            Ok(Placement::Remote(record)) if record.placement == JobPlacement::HostApi => {
                match self.controller.host_jobs() {
                    Ok(host) => {
                        meerschaum_server::executor::context::scope(
                            None,
                            host.backend().is_blocking_on_stdin(&record.remote_name),
                        )
                        .await
                    }
                    Err(_) => false,
                }
            }
            Ok(Placement::Remote(record)) => match self.controller.prepare_id(record.team_id).await
            {
                Ok(api) => api
                    .try_get_job_is_blocking_on_stdin(&record.remote_name)
                    .await
                    .unwrap_or(false),
                Err(_) => false,
            },
            Ok(Placement::Local) => self.local.is_blocking_on_stdin(name).await,
            _ => false,
        }
    }

    async fn prompt_kwargs(&self, name: &str) -> Map {
        match self.placement(name).await {
            Ok(Placement::Remote(record)) if record.placement == JobPlacement::HostApi => {
                match self.controller.host_jobs() {
                    Ok(host) => {
                        meerschaum_server::executor::context::scope(
                            None,
                            host.backend().prompt_kwargs(&record.remote_name),
                        )
                        .await
                    }
                    Err(_) => Map::new(),
                }
            }
            Ok(Placement::Remote(record)) => match self.controller.prepare_id(record.team_id).await
            {
                Ok(api) => api
                    .try_get_job_prompt_kwargs(&record.remote_name)
                    .await
                    .unwrap_or_default(),
                Err(_) => Map::new(),
            },
            Ok(Placement::Local) => self.local.prompt_kwargs(name).await,
            _ => Map::new(),
        }
    }

    async fn monitor_logs(&self, name: &str) -> Option<LogMonitor> {
        let record = match self.placement(name).await {
            Ok(Placement::Remote(record)) => record,
            Ok(Placement::Local) => return self.local.monitor_logs(name).await,
            _ => return None,
        };
        if record.placement == JobPlacement::HostApi {
            let backend = self.controller.host_jobs().ok()?.backend();
            let monitor = meerschaum_server::executor::context::scope(
                None,
                backend.monitor_logs(&record.remote_name),
            )
            .await?;
            let LogMonitor { mut events, input } = monitor;
            let (sender, received) = tokio::sync::mpsc::channel(16);
            let catalog = self.catalog.clone();
            tokio::spawn(async move {
                let mut sanitizer = LogSanitizer::default();
                while let Some(event) = events.recv().await {
                    let event = match event {
                        LogEvent::Line(line) => LogEvent::Line(sanitizer.push(&line)),
                        LogEvent::Stopped(result) => {
                            let _ = catalog.observe_stopped(&record);
                            LogEvent::Stopped(SuccessTuple {
                                success: result.success,
                                message: text(&result.message, 16 * 1024),
                            })
                        }
                        event => event,
                    };
                    let stopped = matches!(event, LogEvent::Stopped(_));
                    if sender.send(event).await.is_err() || stopped {
                        break;
                    }
                }
            });
            return Some(LogMonitor {
                events: received,
                input,
            });
        }
        let api = self.controller.prepare_id(record.team_id).await.ok()?;
        api.clear_job_metadata_cache();
        let initial = api.get_job_metadata(&record.remote_name).await.ok()?;
        let launch = initial
            .get("daemon")?
            .get("properties")?
            .get("process")?
            .get("launch_id")?
            .as_str()?
            .to_string();
        let mut channel = api.open_log_channel(&record.remote_name).await.ok()?;
        let (events, received) = tokio::sync::mpsc::channel(16);
        let (input, mut inputs) = tokio::sync::mpsc::channel::<String>(4);
        let catalog = self.catalog.clone();
        tokio::spawn(async move {
            use meerschaum_connector_api::client::LogMessage;
            let mut sanitizer = LogSanitizer::default();
            loop {
                tokio::select! {
                    () = events.closed() => break,
                    input = inputs.recv() => match input {
                        Some(input) => if channel.send_input(input).await.is_err() { break; },
                        None => break,
                    },
                    message = channel.next() => {
                        let event = match message {
                            Ok(Some(LogMessage::Line(line))) => LogEvent::Line(sanitizer.push(&line)),
                            Ok(Some(LogMessage::InputRequested)) => LogEvent::StdinRequested,
                            Ok(Some(LogMessage::Stopped(_))) | Ok(None) => {
                                // The worker or stream can stop before the native supervisor
                                // publishes. Only the attached launch's committed result ends it.
                                let mut published = None;
                                loop {
                                    api.clear_job_metadata_cache();
                                    let Ok(meta) = api.get_job_metadata(&record.remote_name).await else { break; };
                                    let Ok(meta) = serde_json::to_value(meta) else { break; };
                                    match stream_publication(&meta, &launch) {
                                        StreamPublication::Published(result) => { published = Some(result); break; }
                                        StreamPublication::Unavailable => break,
                                        StreamPublication::Pending => tokio::select! {
                                            () = events.closed() => break,
                                            () = tokio::time::sleep(std::time::Duration::from_millis(25)) => {},
                                        },
                                    }
                                }
                                match published { Some(result) => LogEvent::Stopped(result), None => break }
                            }
                            _ => break,
                        };
                        let stopped = matches!(event, LogEvent::Stopped(_));
                        if stopped { let _ = catalog.observe_stopped(&record); }
                        if events.send(event).await.is_err() || stopped { break; }
                    }
                }
            }
            channel.close().await;
        });
        Some(LogMonitor {
            events: received,
            input,
        })
    }

    async fn team_jobs(&self, team: &str) -> Vec<String> {
        let Ok(records) = self.catalog.records() else {
            return Vec::new();
        };
        let mut names = self.local.team_jobs(team).await;
        names.retain(|name| {
            !records.contains_key(name)
                && !records.values().any(|record| {
                    record.placement == JobPlacement::HostApi && record.remote_name == *name
                })
        });
        let id = if meerschaum_core::access::same_team(Some(team), None) {
            Some(Uuid::nil())
        } else {
            self.controller.team_id(team).await.ok().flatten()
        };
        let Some(id) = id else {
            return names;
        };
        let records = records.into_values().filter(|r| r.team_id == id).collect();
        if let Ok(visible) = self.controller.job_views(records).await {
            names.extend(visible.into_iter().map(|r| r.name));
        }
        names
    }

    async fn retag_team(&self, old: &str, new: Option<&str>) -> Vec<String> {
        let Ok(records) = self.catalog.records() else {
            return Vec::new();
        };
        let mut names = self.local.retag_team(old, new).await;
        names.retain(|name| {
            !records.values().any(|record| {
                record.placement == JobPlacement::HostApi && record.remote_name == *name
            })
        });
        // Retired executor history remains bound to its original UUID, never made Everyone's.
        if let Some(new) = new
            && let Ok(Some(id)) = self.controller.team_id(new).await
            && let Ok(changed) = self.catalog.retag_id(id, new)
        {
            names.extend(changed);
        }
        names
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

/// Sanitize untrusted log chunks across message boundaries. Preserve plain text and bounded
/// SGR colors; discard OSC clipboard/hyperlink sequences, device control and cursor commands.
#[derive(Default)]
pub struct LogSanitizer {
    state: Escape,
}

#[derive(Default)]
enum Escape {
    #[default]
    Ground,
    Start,
    Csi {
        sequence: String,
        valid: bool,
    },
    Osc,
    OscEnd,
    Discard,
    DiscardEnd,
}

impl LogSanitizer {
    /// Feed a chunk, retaining parser state for an escape split across websocket messages.
    pub fn push(&mut self, input: &str) -> String {
        let mut output = String::new();
        for character in input.chars() {
            let state = std::mem::take(&mut self.state);
            self.state = match (state, character) {
                (_, '\u{9c}') => Escape::Ground,
                (Escape::Ground, '\u{1b}') => Escape::Start,
                (Escape::Ground, '\u{9b}') | (Escape::Start, '[') => Escape::Csi {
                    sequence: String::new(),
                    valid: true,
                },
                (Escape::Ground, '\u{9d}') | (Escape::Start, ']') => Escape::Osc,
                (Escape::Start, 'P' | '^' | '_' | 'X') => Escape::Discard,
                (Escape::Start, _) => Escape::Ground,
                (Escape::Csi { sequence, valid }, final_character)
                    if ('@'..='~').contains(&final_character) =>
                {
                    if valid && final_character == 'm' {
                        output.push_str(&format!("\u{1b}[{sequence}m"));
                    }
                    Escape::Ground
                }
                (
                    Escape::Csi {
                        mut sequence,
                        valid,
                    },
                    character,
                ) => {
                    let valid = valid
                        && sequence.len() < 96
                        && (character.is_ascii_digit() || matches!(character, ';' | ':'));
                    if valid {
                        sequence.push(character);
                    }
                    Escape::Csi { sequence, valid }
                }
                (Escape::Osc | Escape::OscEnd, '\u{7}')
                | (Escape::OscEnd | Escape::DiscardEnd, '\\') => Escape::Ground,
                (Escape::Osc | Escape::OscEnd, '\u{1b}') => Escape::OscEnd,
                (Escape::Osc | Escape::OscEnd, _) => Escape::Osc,
                (Escape::Discard | Escape::DiscardEnd, '\u{1b}') => Escape::DiscardEnd,
                (Escape::Discard | Escape::DiscardEnd, _) => Escape::Discard,
                (Escape::Ground, character) => {
                    if !character.is_control() || matches!(character, '\n' | '\r' | '\t') {
                        output.push(character);
                    }
                    Escape::Ground
                }
            };
        }
        output
    }
}

#[cfg(test)]
mod source_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn absent_or_noauth_task_context_never_adopts_an_authenticated_server_environment() {
        for no_auth in [None, Some(false), Some(true)] {
            let root = tempfile::tempdir().unwrap();
            let instance = Arc::new(
                meerschaum_connector_sql::SqlConnector::from_uri(
                    "main",
                    &format!("sqlite:///{}", root.path().join("model.db").display()),
                )
                .await
                .unwrap(),
            );
            let options = meerschaum_core::config::ConfigOptions::with_env(vec![
                ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
                (
                    "MRSM_PLUGINS_DIR".into(),
                    root.path().join("plugins").display().to_string(),
                ),
            ]);
            let controller = Arc::new(crate::executor_client::Controller::new(
                root.path().into(),
                instance.clone(),
                options,
                Map::new(),
            ));
            let state = no_auth.map(|no_auth| {
                let mut config = meerschaum_server::ServerConfig::for_root("sql:main", root.path());
                config.no_auth = no_auth;
                let state = meerschaum_server::AppState::builder(config, instance)
                    .secret(b"synthetic-host-guard-secret-01234567890123456789".to_vec())
                    .build()
                    .unwrap();
                controller.bind_state(&state).unwrap();
                state
            });
            let jobs = DelegatingJobs::new(
                root.path(),
                Arc::new(meerschaum_server::backends::NoJobs),
                controller,
            );
            for caller in [
                None,
                Some(Arc::new(meerschaum_server::access::Access::everything())),
            ] {
                let result = meerschaum_server::executor::context::scope(
                    caller,
                    jobs.create("host-guard", vec!["show".into(), "version".into()], None),
                )
                .await;
                if no_auth == Some(true) {
                    assert!(
                        matches!(result, CreateJob::Started(_)),
                        "Explicit NoAuth keeps its legacy backend"
                    );
                } else {
                    assert!(
                        matches!(result, CreateJob::Unavailable(_)),
                        "A missing accepted caller cannot fall back locally"
                    );
                    assert!(!root.path().join("jobs/host-guard").exists());
                }
            }
            drop(state);
        }
    }

    fn record() -> Record {
        let mut properties = Map::new();
        meerschaum_core::job_access::JobAccess::stamp(
            &mut properties,
            Some("operator"),
            Some("Wetlands"),
        );
        properties.insert(
            "mrsm_execution".into(),
            Value::Map(Map::from_iter([(
                "kind".into(),
                "temporary_execution".into(),
            )])),
        );
        Record::new(
            "execute-owned",
            Uuid::new_v4(),
            vec!["show".into(), "version".into()],
            properties,
        )
    }

    #[test]
    fn explicit_host_placement_is_required_for_everyone_and_keeps_original_history_markers() {
        let mut everyone = record();
        everyone.team_id = Uuid::nil();
        meerschaum_core::job_access::JobAccess::stamp(
            &mut everyone.properties,
            Some("operator"),
            None,
        );
        assert!(
            !everyone.valid_placement(),
            "nil cannot select a remote executor"
        );
        everyone.placement = JobPlacement::HostApi;
        everyone.profile = meerschaum_server::executor::environment::Profile::Api;
        assert!(
            !everyone.valid_placement(),
            "host placement requires canonical source authority"
        );
        everyone.callback = Some(Uuid::new_v4());
        everyone.owner_binding = Some(meerschaum_server::executor::callbacks::OwnerBinding::User {
            username: "operator".into(),
            user_id: 42.into(),
        });
        assert!(everyone.valid_placement());
        let properties = delegated_properties(&everyone);
        assert!(properties[meerschaum_core::job_access::TEAM_ID_KEY].is_null());
        assert!(properties.contains_key(meerschaum_core::job_access::OWNER_BINDING_KEY));
        everyone.profile = meerschaum_server::executor::environment::Profile::Team;
        assert!(
            !everyone.valid_placement(),
            "host transport never upgrades to Team"
        );
        let mut old = serde_json::to_value(record()).unwrap();
        old.as_object_mut().unwrap().remove("placement");
        let old: Record = serde_json::from_value(old).unwrap();
        assert_eq!(old.placement, JobPlacement::Remote);
    }

    #[test]
    fn hostile_metadata_cannot_choose_ownership_paths_commands_or_executor_identity() {
        let record = record();
        let hostile = json!({"sysargs": ["sh", "-c", "malicious"], "status": "running", "restart": true,
            "daemon": {"pid": 1, "properties": {"owner": "admin", "team": "Another", "log_path": "/main/private", "mrsm_execution": {"kind": "persistent_job"}}},
            "executor": {"team_id": Uuid::new_v4()}, "result": [false, "failure\u{1b}[31m"], "health": {"state": "made-up", "message": "hello\u{1b}", "consecutive_failures": -2}});
        let metadata = record.metadata(Some(&hostile));
        assert_eq!(metadata["sysargs"], json!(["show", "version"]));
        assert_eq!(metadata["daemon"]["properties"]["owner"], "operator");
        assert_eq!(metadata["daemon"]["properties"]["team"], "Wetlands");
        assert_eq!(
            metadata["daemon"]["properties"]["mrsm_execution"]["kind"],
            "temporary_execution"
        );
        assert!(metadata["daemon"]["properties"].get("log_path").is_none());
        assert_eq!(metadata["daemon"]["pid"], Json::Null);
        assert_eq!(metadata["executor"]["team_id"], record.team_id.to_string());
        assert_eq!(metadata["restart"], false);
        assert!(!metadata.to_string().contains("\\u001b"));
    }

    #[test]
    fn unavailable_metadata_is_explicit_and_corruption_cannot_be_an_empty_catalog() {
        let root = tempfile::tempdir().unwrap();
        let catalog = Catalog::new(root.path());
        let record = record();
        assert!(catalog.reserve(record.clone()).unwrap());
        assert!(
            !catalog
                .reserve(Record::new(
                    &record.name,
                    Uuid::new_v4(),
                    vec![],
                    Map::new()
                ))
                .unwrap()
        );
        assert_eq!(record.metadata(None)["status"], "unavailable");
        let persisted = catalog.get(&record.name).unwrap().unwrap();
        assert_eq!(persisted.remote_name, record.remote_name);
        std::fs::remove_file(catalog.directory.join("catalog.json")).unwrap();
        assert!(catalog.records().is_err());
    }

    #[test]
    fn concurrent_callback_readers_see_initialization_and_deletion() {
        let root = tempfile::tempdir().unwrap();
        let catalog = Catalog::new(root.path());
        assert!(catalog.records_if_available().unwrap().is_empty());
        let job = record();
        assert!(catalog.reserve(job.clone()).unwrap());
        let first_reader = catalog.lock_file().unwrap();
        fs4::FileExt::lock_shared(&first_reader).unwrap();
        let records = catalog.records_if_available().unwrap();
        assert_eq!(records[&job.name].remote_name, job.remote_name);
        drop(first_reader);
        assert!(catalog.remove(&job).unwrap());
        assert!(catalog.records_if_available().unwrap().is_empty());
        std::fs::remove_file(catalog.directory.join("catalog.json")).unwrap();
        assert!(catalog.records_if_available().is_err());
    }

    #[test]
    fn callback_read_waits_for_a_short_catalog_write() {
        use std::sync::mpsc;
        use std::time::Duration;

        let root = tempfile::tempdir().unwrap();
        let catalog = Catalog::new(root.path());
        let job = record();
        assert!(catalog.reserve(job.clone()).unwrap());
        let write_lock = catalog.lock().unwrap();
        let (enter, entered) = mpsc::channel();
        let (finish, finished) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            enter.send(()).unwrap();
            finish.send(catalog.records_if_available()).unwrap();
        });
        entered.recv().unwrap();
        assert!(matches!(
            finished.recv_timeout(Duration::from_millis(25)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(write_lock);
        let records = finished
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert_eq!(records[&job.name].remote_name, job.remote_name);
        reader.join().unwrap();
    }

    #[test]
    fn a_delayed_old_launch_cannot_remove_a_new_reservation() {
        let root = tempfile::tempdir().unwrap();
        let catalog = Catalog::new(root.path());
        let first = record();
        assert!(catalog.reserve(first.clone()).unwrap());
        assert!(catalog.remove(&first).unwrap());
        let second = record();
        assert!(catalog.reserve(second.clone()).unwrap());
        assert!(!catalog.remove(&first).unwrap());
        assert_eq!(
            catalog.get(&second.name).unwrap().unwrap().remote_name,
            second.remote_name
        );
    }

    #[test]
    fn ordinary_job_names_keep_spaces_and_unicode_without_becoming_paths() {
        let root = tempfile::tempdir().unwrap();
        let catalog = Catalog::new(root.path());
        let name = "daily estuary 🌊";
        let record = Record::new(name, Uuid::new_v4(), vec![], Map::new());
        assert!(catalog.reserve(record).unwrap());
        assert!(catalog.get(name).unwrap().is_some());
        for name in ["../outside", "..", ".", "a/b", "a\\b", "bad;command"] {
            assert!(!valid_name(name));
        }
    }

    #[test]
    fn log_sanitization_preserves_colors_and_handles_split_clipboard_or_cursor_commands() {
        let mut parser = LogSanitizer::default();
        assert_eq!(parser.push("alpha\u{1b}]52;c;"), "alpha");
        assert_eq!(parser.push("clipboard\u{1b}"), "");
        assert_eq!(
            parser.push("\\beta\u{1b}[31mred\u{1b}[0m\u{1b}[2J"),
            "beta\u{1b}[31mred\u{1b}[0m"
        );
        assert_eq!(
            parser.push("\u{9d}8;;https://malicious\u{7}plain\u{9b}2J"),
            "plain"
        );
    }
}

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
