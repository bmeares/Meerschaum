//! Digest receipt from a native Metadata worker's actual bounded result FD.
//! Job properties and HTTP responses never construct a verified MetadataResult.

use super::{JobGeneration, JobProcesses, LifecycleOwner, unavailable};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io;

const MAX_RECEIPT: usize = 16 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    schema: u8,
    generation: JobGeneration,
    launch_id: String,
    supervisor: LifecycleOwner,
    worker: LifecycleOwner,
    success: bool,
    normal_exit_success: bool,
    message_bytes: usize,
    message_sha256: String,
    deadline: i64,
    completed_at: i64,
}

/// Constructible only by a bounded native receipt read, matched against the exact
/// stored generation/lifecycle and both completed process births. Not Deserialize.
pub struct MetadataResult {
    wire: Wire,
    receipt_sha256: String,
}
impl MetadataResult {
    /// Job generation whose supervisor published this receipt.
    pub fn generation(&self) -> &JobGeneration {
        &self.wire.generation
    }
    /// Signed execution deadline recorded for this generation.
    pub fn deadline(&self) -> i64 {
        self.wire.deadline
    }
    /// SHA-256 digest of the published result message bytes.
    pub fn message_sha256(&self) -> &str {
        &self.wire.message_sha256
    }
    /// Length in bytes of the published result message.
    pub fn message_bytes(&self) -> usize {
        self.wire.message_bytes
    }
    /// Identifier of the supervisor launch that produced this receipt.
    pub fn launch_id(&self) -> &str {
        &self.wire.launch_id
    }
    /// Completion time recorded by the native supervisor.
    pub fn completed_at(&self) -> i64 {
        self.wire.completed_at
    }
    /// SHA-256 digest of the verified native receipt.
    pub fn receipt_sha256(&self) -> &str {
        &self.receipt_sha256
    }
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

impl JobGeneration {
    /// Ordinary result publication is pending only while the exact retained native supervisor
    /// birth lives and its recorded worker has ended. Missing/dead/uncertain proof is never a
    /// pending lease, successful result, data EOF or graph-readiness assertion.
    pub fn finalization_pending(&self) -> io::Result<bool> {
        self.validate(&format!("mrsm-{}", self.context))?;
        let processes = processes(self)?;
        let supervisor = processes.supervisor.as_ref().ok_or_else(unavailable)?;
        let worker = processes.worker.as_ref().ok_or_else(unavailable)?;
        Ok(supervisor.alive() && worker.ended()? && supervisor.alive())
    }

    fn metadata_result_path(&self) -> io::Result<std::path::PathBuf> {
        self.validate(&format!("mrsm-{}", self.context))?;
        Ok(self
            .directory
            .parent()
            .ok_or_else(unavailable)?
            .join(format!("metadata-result-{}.json", self.revision)))
    }
    /// A pending/repeated native launch cannot inherit an earlier completion digest.
    pub(super) fn clear_metadata_result(&self) -> io::Result<()> {
        match std::fs::remove_file(self.metadata_result_path()?) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
    /// The caller must obtain this generation from its trusted native Contexts store.
    /// No result/hash supplied by a worker, job properties or HTTP body enters here.
    pub fn metadata_result(&self, deadline: i64) -> io::Result<MetadataResult> {
        if !self.inactive()? {
            return Err(unavailable());
        }
        let raw =
            crate::untrusted_file::read_regular(&self.metadata_result_path()?, MAX_RECEIPT, false)?
                .ok_or_else(unavailable)?;
        let wire: Wire = serde_json::from_slice(&raw).map_err(|_| unavailable())?;
        let processes = processes(self)?;
        let valid_hash = wire.message_sha256.len() == 64
            && wire
                .message_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        if wire.schema != 1
            || wire.generation != *self
            || !wire.success
            || !wire.normal_exit_success
            || wire.launch_id.is_empty()
            || wire.launch_id.len() > 128
            || wire.launch_id.chars().any(char::is_control)
            || wire.message_bytes > meerschaum_core::team_scope::api_execution::MAX_METADATA_BYTES
            || !valid_hash
            || wire.deadline != deadline
            || wire.completed_at <= 0
            || wire.completed_at > deadline
            || deadline <= chrono::Utc::now().timestamp()
            || processes.supervisor.as_ref() != Some(&wire.supervisor)
            || processes.worker.as_ref() != Some(&wire.worker)
            || !processes.cleanup_complete
            || !wire.supervisor.ended()?
            || !wire.worker.ended()?
            || !self.inactive()?
        {
            return Err(unavailable());
        }
        Ok(MetadataResult {
            wire,
            receipt_sha256: digest(&raw),
        })
    }
}

fn processes(generation: &JobGeneration) -> io::Result<JobProcesses> {
    let raw = crate::untrusted_file::read_regular(&generation.record_path()?, MAX_RECEIPT, false)?
        .ok_or_else(unavailable)?;
    let processes: JobProcesses = serde_json::from_slice(&raw).map_err(|_| unavailable())?;
    if processes.generation != *generation {
        return Err(unavailable());
    }
    Ok(processes)
}

/// Called ONLY from the native supervisor, after result-FD validation and descendant
/// cleanup, before publishing the result into ordinary mutable job properties.
pub(in crate::supervisor) fn record(
    generation: &JobGeneration,
    launch_id: &str,
    deadline: i64,
    success: bool,
    normal_exit_success: bool,
    message: &str,
) -> io::Result<()> {
    generation.validate(&format!("mrsm-{}", generation.context))?;
    let processes = processes(generation)?;
    let supervisor = LifecycleOwner::current()?;
    let worker = processes.worker.ok_or_else(unavailable)?;
    if processes.supervisor.as_ref() != Some(&supervisor)
        || !processes.cleanup_complete
        || !worker.ended()?
        || launch_id.is_empty()
        || launch_id.len() > 128
        || launch_id.chars().any(char::is_control)
        || message.len() > meerschaum_core::team_scope::api_execution::MAX_METADATA_BYTES
    {
        return Err(unavailable());
    }
    let completed_at = chrono::Utc::now().timestamp();
    if deadline <= 0 || completed_at > deadline {
        return Err(unavailable());
    }
    let wire = Wire {
        schema: 1,
        generation: generation.clone(),
        launch_id: launch_id.into(),
        supervisor,
        worker,
        success,
        normal_exit_success,
        message_bytes: message.len(),
        message_sha256: digest(message.as_bytes()),
        deadline,
        completed_at,
    };
    let bytes = serde_json::to_string(&wire).map_err(io::Error::other)?;
    if bytes.len() > MAX_RECEIPT {
        return Err(unavailable());
    }
    meerschaum_core::team_scope::write_private(&generation.metadata_result_path()?, &bytes)
}
