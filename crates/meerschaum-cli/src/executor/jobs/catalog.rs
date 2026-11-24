//! The canonical job catalog: owner-only, process-shared metadata of every delegated job, read
//! and written only by the main API under its file lock.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::{Record, valid_name};
use meerschaum_core::team_scope::{create_private_dir, write_private};
use uuid::Uuid;

const MAX_CATALOG_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CATALOG_JOBS: usize = 10_000;

/// Owner-only, process-shared canonical metadata. Only the main API reads and writes it.
#[derive(Clone)]
pub struct Catalog {
    pub(super) directory: PathBuf,
}

impl Catalog {
    /// Use the main API's private executor storage, outside its public desired directory.
    pub fn new(root: &Path) -> Self {
        Self {
            directory: root.join(crate::executor::registry::DIRECTORY),
        }
    }

    pub(super) fn lock_file(&self) -> Result<std::fs::File, String> {
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

    pub(super) fn lock(&self) -> Result<std::fs::File, String> {
        let file = self.lock_file()?;
        fs4::FileExt::lock(&file).map_err(|_| "Could not lock the executor job catalog.")?;
        Ok(file)
    }

    /// A concurrent status read or fsync may briefly own the lock. Retry only acquisition,
    /// before reading the canonical record or attempting any side effect.
    pub(super) fn lock_with_deadline(file: &std::fs::File, shared: bool) -> Result<(), String> {
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

    pub(super) fn read(&self) -> Result<BTreeMap<String, Record>, String> {
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

    pub(super) fn write(&self, records: &BTreeMap<String, Record>) -> Result<(), String> {
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
        let file = meerschaum_core::file_lock::open(self.directory.join("catalog.lock"))
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

    pub(super) fn retag_id(&self, id: Uuid, name: &str) -> Result<Vec<String>, String> {
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
