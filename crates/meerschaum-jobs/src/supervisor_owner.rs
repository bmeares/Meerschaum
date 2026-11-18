//! Immutable host-selected process ownership for a detached supervisor.
//!
//! This is a crash guard, not a signed lease or API capability. Job properties, persisted
//! environment, and paths never select it. The host resolves it before the supervisor forks.

use std::io;

use serde::{Deserialize, Serialize};
#[path = "metadata_result.rs"]
mod metadata_result;
pub use metadata_result::MetadataResult;
pub(super) use metadata_result::record as record_metadata_result;
#[path = "supervisor_terminal_lock.rs"]
mod terminal_lock;
pub use terminal_lock::cancel_unlaunched_terminal;

/// One exact process: a reused PID cannot become an earlier supervisor's owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleOwner {
    pid: i32,
    started_at: chrono::NaiveDateTime,
}

/// A callback-free Factory supervisor holds this file exclusively until it has finished
/// publishing. The kernel releases the lock if the supervisor exits without a result.
pub struct FactoryPublication;

impl FactoryPublication {
    const FILE: &'static str = ".factory-publication";

    fn open(directory: &std::path::Path, writer: bool) -> io::Result<std::fs::File> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(writer).create(writer);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(directory.join(Self::FILE))?;
        if !file.metadata()?.is_file() {
            return Err(unavailable());
        }
        Ok(file)
    }

    #[cfg(unix)]
    pub(super) fn claim(directory: &std::path::Path, launch: &str) -> io::Result<std::fs::File> {
        use std::io::Write;
        if launch.is_empty()
            || launch.len() > 64
            || !launch.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return Err(unavailable());
        }
        let mut file = Self::open(directory, true)?;
        file.try_lock().map_err(io::Error::from)?;
        file.set_len(0)?;
        file.write_all(launch.as_bytes())?;
        Ok(file)
    }

    /// True only while a supervisor holds the private file for this exact local launch.
    pub fn pending(directory: &std::path::Path, launch: &str) -> bool {
        use std::io::Read;
        let Ok(file) = Self::open(directory, false) else {
            return false;
        };
        if !matches!(
            file.try_lock_shared(),
            Err(std::fs::TryLockError::WouldBlock)
        ) {
            return false;
        }
        let mut token = String::new();
        file.take(65).read_to_string(&mut token).is_ok() && !token.is_empty() && token == launch
    }
}

/// Host-selected generation binding. No persisted environment or job property selects it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobGeneration {
    /// Canonical lowercase UUID without separators.
    pub context: String,
    /// Exact Main-owned launch revision.
    pub revision: u64,
    /// Original actor label, used only as an identity binding.
    pub owner: String,
    /// Registered source identity when the actor is a user.
    pub owner_id: Option<String>,
    /// Digest of the host's complete immutable context identity.
    pub identity: String,
    /// Sealed generation selected by the host, outside writable worker state.
    pub directory: std::path::PathBuf,
}

impl JobGeneration {
    /// Reject noncanonical names, invalid identities and unsealed generation selections.
    pub fn validate(&self, name: &str) -> io::Result<()> {
        let hex = |text: &str, length| {
            text.len() == length
                && text
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        };
        let generation = self
            .directory
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| {
                name.strip_prefix("generation-")
                    .or_else(|| name.strip_prefix("team-sources-"))
            });
        if !hex(&self.context, 32)
            || self.context.bytes().all(|byte| byte == b'0')
            || name != format!("mrsm-{}", self.context)
            || self.revision == 0
            || self.owner.is_empty()
            || self.owner.len() > 256
            || self.owner.chars().any(char::is_control)
            || self.owner_id.as_ref().is_some_and(|id| {
                id.is_empty() || id.len() > 256 || id.chars().any(char::is_control)
            })
            || !hex(&self.identity, 64)
            || !self.directory.is_absolute()
            || generation.is_none_or(|name| !hex(name, 32))
            || self
                .directory
                .parent()
                .and_then(|path| path.file_name())
                .and_then(|name| name.to_str())
                != Some(self.context.as_str())
        {
            return Err(unavailable());
        }
        Ok(())
    }

    fn record_path(&self) -> io::Result<std::path::PathBuf> {
        self.validate(&format!("mrsm-{}", self.context))?;
        Ok(self
            .directory
            .parent()
            .ok_or_else(unavailable)?
            .join(format!("job-lifecycle-{}.json", self.revision)))
    }

    /// Both actual processes have ended, or their PID was reused after the recorded birth.
    /// Missing/unreadable proof is an error, never evidence that a generation is inactive.
    pub fn inactive(&self) -> io::Result<bool> {
        use std::io::Read;
        let path = self.record_path()?;
        let metadata = std::fs::symlink_metadata(&path)?;
        if !metadata.is_file() || metadata.len() > 16 * 1024 {
            return Err(unavailable());
        }
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(16 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 16 * 1024 {
            return Err(unavailable());
        }
        let record: JobProcesses = serde_json::from_slice(&bytes).map_err(|_| unavailable())?;
        if record.generation != *self {
            return Err(unavailable());
        }
        let supervisor = record.supervisor.as_ref().ok_or_else(unavailable)?;
        let supervisor_ended = supervisor.ended()?;
        if supervisor_ended && !record.cleanup_complete {
            return Err(unavailable());
        }
        let worker_ended = record
            .worker
            .as_ref()
            .map(LifecycleOwner::ended)
            .transpose()?
            .unwrap_or(true);
        Ok(record.cleanup_complete && supervisor_ended && worker_ended)
    }

    /// Invalidate an earlier completion before the trusted daemon forks another run.
    pub fn pending(&self) -> io::Result<()> {
        self.clear_metadata_result()?;
        let record = JobProcesses {
            generation: self.clone(),
            supervisor: None,
            worker: None,
            cleanup_complete: false,
        };
        meerschaum_core::team_scope::write_private(
            &self.record_path()?,
            &serde_json::to_string(&record).map_err(io::Error::other)?,
        )
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JobProcesses {
    generation: JobGeneration,
    supervisor: Option<LifecycleOwner>,
    worker: Option<LifecycleOwner>,
    #[serde(default)]
    cleanup_complete: bool,
}

pub(super) fn record_job_child(generation: &JobGeneration, child_pid: i32) -> io::Result<()> {
    let supervisor = LifecycleOwner::current()?;
    if !crate::process::children(supervisor.pid)
        .is_some_and(|children| children.contains(&child_pid))
    {
        return Err(unavailable());
    }
    // The owned Child is not reaped yet, so even a very short worker's zombie retains its birth.
    let worker = LifecycleOwner {
        pid: child_pid,
        started_at: crate::process::process_start_time(child_pid).ok_or_else(unavailable)?,
    };
    let contents = serde_json::to_string(&JobProcesses {
        generation: generation.clone(),
        supervisor: Some(supervisor),
        worker: Some(worker),
        cleanup_complete: false,
    })
    .map_err(io::Error::other)?;
    meerschaum_core::team_scope::write_private(&generation.record_path()?, &contents)
}

pub(super) fn record_job_cleanup(generation: &JobGeneration, no_worker: bool) -> io::Result<()> {
    use std::io::Read;
    let path = generation.record_path()?;
    let metadata = std::fs::symlink_metadata(&path)?;
    if !metadata.is_file() || metadata.len() > 16 * 1024 {
        return Err(unavailable());
    }
    let mut bytes = Vec::new();
    std::fs::File::open(&path)?
        .take(16 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 16 * 1024 {
        return Err(unavailable());
    }
    let mut record: JobProcesses = serde_json::from_slice(&bytes).map_err(|_| unavailable())?;
    if record.generation != *generation {
        return Err(unavailable());
    }
    let supervisor = LifecycleOwner::current()?;
    if no_worker && record.supervisor.is_none() && record.worker.is_none() {
        record.supervisor = Some(supervisor);
    } else if record.supervisor.as_ref() != Some(&supervisor)
        || record
            .worker
            .as_ref()
            .is_none_or(|worker| !worker.ended().unwrap_or(false))
    {
        return Err(unavailable());
    }
    record.cleanup_complete = true;
    meerschaum_core::team_scope::write_private(
        &path,
        &serde_json::to_string(&record).map_err(io::Error::other)?,
    )
}

impl LifecycleOwner {
    /// Capture this host's process identity. A platform which cannot prove start identity
    /// refuses guarded execution rather than silently accepting PID-only lifetime checks.
    pub fn current() -> io::Result<Self> {
        let pid = i32::try_from(std::process::id()).map_err(|_| unavailable())?;
        Self::capture(pid)
    }

    fn capture(pid: i32) -> io::Result<Self> {
        if pid <= 0 {
            return Err(unavailable());
        }
        let started_at = crate::process::process_start_time(pid).ok_or_else(unavailable)?;
        let owner = Self { pid, started_at };
        if !owner.alive() {
            return Err(unavailable());
        }
        Ok(owner)
    }

    /// The exact owner remains alive or paused. Zombies and unreadable/reused identities fail
    /// closed. This function only observes the owner; it never sends that PID a signal.
    pub fn alive(&self) -> bool {
        self.pid > 0
            && matches!(
                crate::process::proc_state(self.pid),
                crate::process::ProcState::Alive | crate::process::ProcState::Stopped
            )
            && crate::process::process_start_time(self.pid) == Some(self.started_at)
    }

    fn ended(&self) -> io::Result<bool> {
        if self.pid <= 0 {
            return Err(unavailable());
        }
        match crate::process::proc_state(self.pid) {
            crate::process::ProcState::Gone | crate::process::ProcState::Zombie => Ok(true),
            _ => crate::process::process_start_time(self.pid)
                .map(|birth| birth != self.started_at)
                .ok_or_else(unavailable),
        }
    }

    pub(super) fn to_arg(&self) -> String {
        serde_json::to_string(self).expect("fixed process identity is serializable")
    }

    pub(super) fn from_arg(value: &str) -> Result<Self, String> {
        if value.len() > 512 {
            return Err("Invalid supervisor lifecycle owner.".into());
        }
        let owner: Self =
            serde_json::from_str(value).map_err(|_| "Invalid supervisor lifecycle owner.")?;
        if owner.pid <= 0 {
            return Err("Invalid supervisor lifecycle owner.".into());
        }
        Ok(owner)
    }
}

fn unavailable() -> io::Error {
    io::Error::other("The executor runtime process identity cannot be proven on this platform.")
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalChild {
    guardian: LifecycleOwner,
    child: LifecycleOwner,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalLaunch {
    version: u32,
    id: uuid::Uuid,
    spec: std::path::PathBuf,
    owner: LifecycleOwner,
    guardian: Option<LifecycleOwner>,
    child: Option<LifecycleOwner>,
    cleanup_complete: bool,
}

fn terminal_launch_read(
    spec: &std::path::Path,
    path: &std::path::Path,
) -> io::Result<TerminalLaunch> {
    let selected = std::fs::canonicalize(spec)?;
    if path.parent() != selected.parent() {
        return Err(unavailable());
    }
    let bytes = crate::untrusted_file::read_regular(path, 4096, false)?.ok_or_else(unavailable)?;
    let record: TerminalLaunch = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    if record.version != 1
        || record.id.is_nil()
        || record.spec != selected
        || path.file_name().and_then(|name| name.to_str())
            != Some(&format!("terminal-launch-{}.json", record.id.simple()))
    {
        return Err(unavailable());
    }
    Ok(record)
}

fn terminal_launch_write(path: &std::path::Path, record: &TerminalLaunch) -> io::Result<()> {
    meerschaum_core::team_scope::write_private(
        path,
        &serde_json::to_string(record).map_err(io::Error::other)?,
    )
}

fn terminal_launch_paths(spec: &std::path::Path) -> io::Result<Vec<std::path::PathBuf>> {
    let selected = std::fs::canonicalize(spec)?;
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(selected.parent().ok_or_else(unavailable)?)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with("terminal-launch-")
        {
            if paths.len() >= 4096 {
                return Err(unavailable());
            }
            paths.push(entry.path());
        }
    }
    Ok(paths)
}

fn terminal_launch_inactive(record: &TerminalLaunch) -> io::Result<bool> {
    if !record.cleanup_complete {
        return Ok(false);
    }
    let Some(guardian) = &record.guardian else {
        return Err(unavailable());
    };
    if !guardian.ended()? {
        return Ok(false);
    }
    match &record.child {
        Some(child) => child.ended(),
        None => Ok(true),
    }
}

/// Persist a unique sealed pending launch before handing a terminal command to a process
/// launcher. Completed records are retired only after their exact processes are gone.
pub fn prepare_terminal_launch(
    spec: &std::path::Path,
    owner: &LifecycleOwner,
) -> io::Result<std::path::PathBuf> {
    let _lock = terminal_lock::lock(spec)?;
    if !owner.alive() {
        return Err(unavailable());
    }
    let spec = std::fs::canonicalize(spec)?;
    let mut outstanding = 0;
    for path in terminal_launch_paths(&spec)? {
        if terminal_launch_inactive(&terminal_launch_read(&spec, &path)?)? {
            std::fs::remove_file(path)?;
        } else {
            outstanding += 1;
        }
    }
    if outstanding >= 4096 {
        return Err(unavailable());
    }
    let id = uuid::Uuid::new_v4();
    let path = spec
        .parent()
        .ok_or_else(unavailable)?
        .join(format!("terminal-launch-{}.json", id.simple()));
    let record = TerminalLaunch {
        version: 1,
        id,
        spec,
        owner: owner.clone(),
        guardian: None,
        child: None,
        cleanup_complete: false,
    };
    terminal_launch_write(&path, &record)?;
    Ok(path)
}

/// Claim one host-created pending launch in its dedicated native guardian, before forking.
pub fn claim_terminal_launch(
    spec: &std::path::Path,
    path: &std::path::Path,
    owner: &LifecycleOwner,
) -> io::Result<()> {
    let _lock = terminal_lock::lock(spec)?;
    let mut record = terminal_launch_read(spec, path)?;
    if &record.owner != owner
        || !owner.alive()
        || record.guardian.is_some()
        || record.cleanup_complete
    {
        return Err(unavailable());
    }
    record.guardian = Some(LifecycleOwner::current()?);
    terminal_launch_write(path, &record)
}

/// Bind an actual direct child to the claimed native guardian and existing status descriptor.
pub fn record_terminal_launch_child(
    child_pid: i32,
    spec: &std::path::Path,
    path: &std::path::Path,
    owner: &LifecycleOwner,
) -> io::Result<std::path::PathBuf> {
    let _lock = terminal_lock::lock(spec)?;
    let mut record = terminal_launch_read(spec, path)?;
    if &record.owner != owner
        || record.guardian != Some(LifecycleOwner::current()?)
        || record.child.is_some()
        || record.cleanup_complete
    {
        return Err(unavailable());
    }
    let descriptor = record_terminal_child(child_pid, spec)?;
    record.child = Some(LifecycleOwner::capture(child_pid)?);
    terminal_launch_write(path, &record)?;
    Ok(descriptor)
}

/// Persist native cleanup evidence only after the guardian has retired every owned child.
pub fn complete_terminal_launch(
    spec: &std::path::Path,
    path: &std::path::Path,
    owner: &LifecycleOwner,
) -> io::Result<()> {
    let _lock = terminal_lock::lock(spec)?;
    let mut record = terminal_launch_read(spec, path)?;
    if &record.owner != owner || record.guardian != Some(LifecycleOwner::current()?) {
        return Err(unavailable());
    }
    if let Some(child) = &record.child
        && !child.ended()?
    {
        return Err(unavailable());
    }
    record.cleanup_complete = true;
    terminal_launch_write(path, &record)
}

/// Prove all tracked terminal launches in one sealed generation finished native cleanup.
/// An empty set is meaningful only when its host context attests this tracking protocol.
pub fn terminal_launches_inactive(spec: &std::path::Path) -> io::Result<bool> {
    let _lock = terminal_lock::lock(spec)?;
    terminal_launches_inactive_locked(spec)
}
fn terminal_launches_inactive_locked(spec: &std::path::Path) -> io::Result<bool> {
    for path in terminal_launch_paths(spec)? {
        if !terminal_launch_inactive(&terminal_launch_read(spec, &path)?)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Request graceful shutdown of exact tracked guardians, preserving pending or uncertain
/// evidence. A caller must poll cleanup proof before removing context state.
pub fn stop_terminal_launches(spec: &std::path::Path) -> io::Result<bool> {
    let _lock = terminal_lock::lock(spec)?;
    for path in terminal_launch_paths(spec)? {
        let record = terminal_launch_read(spec, &path)?;
        if terminal_launch_inactive(&record)? {
            continue;
        }
        let Some(guardian) = record.guardian else {
            continue;
        };
        #[cfg(target_os = "linux")]
        if !guardian.ended()? {
            use rustix::process::{Pid, PidfdFlags, Signal};
            let pid = Pid::from_raw(guardian.pid).ok_or_else(unavailable)?;
            let descriptor = rustix::process::pidfd_open(pid, PidfdFlags::empty())?;
            if guardian.alive() {
                rustix::process::pidfd_send_signal(&descriptor, Signal::TERM)?;
            } else if !guardian.ended()? {
                return Err(unavailable());
            }
        }
        #[cfg(not(target_os = "linux"))]
        return Err(unavailable());
    }
    terminal_launches_inactive_locked(spec)
}

fn terminal_child_path(pane_pid: i32, spec: &std::path::Path) -> Option<std::path::PathBuf> {
    if pane_pid <= 0 || !spec.is_absolute() {
        return None;
    }
    Some(spec.parent()?.join(format!("terminal-{pane_pid}.json")))
}

/// Record an actual guardian child in the sealed context directory, outside writable shell
/// state. The descriptor is for status lookup only; it grants no process or API authority.
pub fn record_terminal_child(
    child_pid: i32,
    spec: &std::path::Path,
) -> io::Result<std::path::PathBuf> {
    let guardian = LifecycleOwner::current()?;
    if !crate::process::children(guardian.pid).is_some_and(|children| children.contains(&child_pid))
    {
        return Err(unavailable());
    }
    let child = LifecycleOwner::capture(child_pid)?;
    let path = terminal_child_path(guardian.pid, spec).ok_or_else(unavailable)?;
    let contents =
        serde_json::to_string(&TerminalChild { guardian, child }).map_err(io::Error::other)?;
    meerschaum_core::team_scope::write_private(&path, &contents)?;
    Ok(path)
}

/// Resolve only the live child bound to this exact guardian birth. Supply the trusted context
/// spec path; a shell-writable status directory must never select this descriptor.
pub fn terminal_child_pid(pane_pid: i32, spec: &std::path::Path) -> Option<i32> {
    use std::io::Read;
    let path = terminal_child_path(pane_pid, spec)?;
    let metadata = std::fs::symlink_metadata(&path).ok()?;
    if !metadata.is_file() || metadata.len() > 1024 {
        return None;
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .ok()?
        .take(1025)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() > 1024 {
        return None;
    }
    let record: TerminalChild = serde_json::from_slice(&bytes).ok()?;
    (record.guardian.pid == pane_pid && record.guardian.alive() && record.child.alive())
        .then_some(record.child.pid)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn dead_pids_without_cleanup_are_not_recovery_proof_and_pending_invalidates_completion() {
        let root = tempfile::tempdir().unwrap();
        let generation = JobGeneration {
            context: "1".repeat(32),
            revision: 1,
            owner: "alice".into(),
            owner_id: Some("registered-alice".into()),
            identity: "2".repeat(64),
            directory: root
                .path()
                .join("1".repeat(32))
                .join(format!("generation-{}", "3".repeat(32))),
        };
        std::fs::create_dir_all(&generation.directory).unwrap();
        let mut ended = LifecycleOwner::current().unwrap();
        // Same PID, different birth models a gone earlier process without signalling anyone.
        ended.started_at -= chrono::TimeDelta::seconds(1);
        let mut record = JobProcesses {
            generation: generation.clone(),
            supervisor: Some(ended.clone()),
            worker: Some(ended),
            cleanup_complete: false,
        };
        let write = |record: &JobProcesses| {
            std::fs::write(
                generation.record_path().unwrap(),
                serde_json::to_vec(record).unwrap(),
            )
            .unwrap()
        };
        write(&record);
        assert!(
            generation.inactive().is_err(),
            "Two ended process identities do not prove descendant cleanup."
        );
        record.cleanup_complete = true;
        write(&record);
        assert!(generation.inactive().unwrap());
        let mut replacement = generation.clone();
        replacement.owner_id = Some("replacement-registration".into());
        assert!(replacement.inactive().is_err());
        generation.pending().unwrap();
        assert!(
            generation.inactive().is_err(),
            "A previous run's completed proof cannot cover a new pending fork."
        );
    }

    #[test]
    fn process_start_identity_prevents_pid_reuse_adoption_and_round_trips_strictly() {
        let owner = LifecycleOwner::current().unwrap();
        assert!(owner.alive());
        assert_eq!(LifecycleOwner::from_arg(&owner.to_arg()).unwrap(), owner);
        let mut reused = owner.clone();
        reused.started_at += chrono::TimeDelta::seconds(1);
        assert!(!reused.alive());
        assert!(LifecycleOwner::capture(0).is_err());
        for invalid in [
            "{}",
            "{\"pid\":-1,\"started_at\":\"2026-01-01T00:00:00\"}",
            "{\"pid\":1,\"started_at\":\"2026-01-01T00:00:00\",\"path\":\"/untrusted\"}",
        ] {
            assert!(LifecycleOwner::from_arg(invalid).is_err());
        }
    }
}

#[cfg(all(test, unix))]
mod publication_tests {
    use super::FactoryPublication;

    #[test]
    fn only_the_locked_exact_launch_may_still_publish_its_result() {
        let job = tempfile::tempdir().unwrap();
        let first = FactoryPublication::claim(job.path(), "first").unwrap();
        assert!(FactoryPublication::pending(job.path(), "first"));
        assert!(!FactoryPublication::pending(job.path(), "second"));
        assert!(FactoryPublication::claim(job.path(), "second").is_err());
        drop(first);
        assert!(!FactoryPublication::pending(job.path(), "first"));
        let second = FactoryPublication::claim(job.path(), "second").unwrap();
        assert!(!FactoryPublication::pending(job.path(), "first"));
        assert!(FactoryPublication::pending(job.path(), "second"));
        drop(second);
        assert!(!FactoryPublication::pending(job.path(), "second"));
    }
}
