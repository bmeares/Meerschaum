//! Per-pipe leases (§5.8): the instance's `acquire_pipe_lease` / `release_pipe_lease` pair,
//! with a machine-local fallback.
//!
//! [`InstanceLeases`] calls the instance connector's lease methods (SQL: an insert into an
//! internal lease table; Valkey: `SET NX PX`; API: `/engine/leases`). An instance that does not
//! implement them answers `NotImplemented`; the engine then falls back to [`FileLeases`] —
//! interprocess file locks under a directory on this machine — and warns once per instance that
//! the lease is machine-local.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use meerschaum_connector::ConnectorError;
use sha2::{Digest, Sha256};

use crate::executor::LeaseSource;
use crate::instance_state::InstanceState;
use crate::node::PipeRef;
use crate::registry::ConnectorRegistry;
use crate::state::StateError;

/// Machine-local leases: one exclusive `flock` per pipe on a file under `dir`. A lease dies
/// with its process, so the TTL is not needed for expiry; renewal is a no-op.
#[derive(Debug)]
pub struct FileLeases {
    dir: PathBuf,
    held: Mutex<HashMap<PipeRef, (String, File)>>,
}

impl FileLeases {
    /// Leases under `dir` (created on first use).
    pub fn new(dir: impl Into<PathBuf>) -> FileLeases {
        FileLeases {
            dir: dir.into(),
            held: Mutex::new(HashMap::new()),
        }
    }

    /// The lock directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The lock file for a pipe (`<sha256(pipe_ref)>.lock`).
    pub fn path_for(&self, node: &PipeRef) -> PathBuf {
        let digest = Sha256::digest(node.as_str().as_bytes());
        let hex: String = digest.iter().take(16).map(|b| format!("{b:02x}")).collect();
        self.dir.join(format!("{hex}.lock"))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<PipeRef, (String, File)>> {
        self.held.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Try to take the lease.
    pub fn try_acquire(&self, node: &PipeRef, holder: &str) -> Result<bool, StateError> {
        let mut held = self.lock();
        if let Some((h, _)) = held.get(node) {
            return Ok(h == holder);
        }
        std::fs::create_dir_all(&self.dir)
            .map_err(|e| StateError::Other(format!("cannot create {}: {e}", self.dir.display())))?;
        let path = self.path_for(node);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| StateError::Other(format!("cannot open {}: {e}", path.display())))?;
        match fs4::FileExt::try_lock(&file) {
            Ok(()) => {}
            Err(fs4::TryLockError::WouldBlock) => return Ok(false),
            Err(fs4::TryLockError::Error(e)) => {
                return Err(StateError::Other(format!(
                    "cannot lock {}: {e}",
                    path.display()
                )));
            }
        }
        let _ = file.set_len(0);
        let _ = writeln!(file, "{holder}\n{node}");
        held.insert(node.clone(), (holder.to_string(), file));
        Ok(true)
    }

    /// Release the lease if `holder` holds it.
    pub fn release_lease(&self, node: &PipeRef, holder: &str) {
        let mut held = self.lock();
        if held.get(node).is_some_and(|(h, _)| h == holder) {
            if let Some((_, file)) = held.remove(node) {
                let _ = fs4::FileExt::unlock(&file);
            }
        }
    }
}

impl LeaseSource for FileLeases {
    async fn acquire(
        &self,
        node: &PipeRef,
        holder: &str,
        _ttl: Duration,
    ) -> Result<bool, StateError> {
        self.try_acquire(node, holder)
    }

    async fn renew(
        &self,
        node: &PipeRef,
        holder: &str,
        _ttl: Duration,
    ) -> Result<bool, StateError> {
        Ok(self.lock().get(node).is_some_and(|(h, _)| h == holder))
    }

    async fn release(&self, node: &PipeRef, holder: &str) -> Result<(), StateError> {
        self.release_lease(node, holder);
        Ok(())
    }
}

/// Leases through each pipe's instance connector, falling back to [`FileLeases`].
pub struct InstanceLeases {
    registry: Arc<dyn ConnectorRegistry>,
    state: Arc<InstanceState>,
    files: FileLeases,
    fallback: Mutex<HashSet<String>>,
    warnings: Mutex<Vec<String>>,
}

impl std::fmt::Debug for InstanceLeases {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstanceLeases")
            .field("files", &self.files)
            .finish_non_exhaustive()
    }
}

impl InstanceLeases {
    /// Leases over `registry`; machine-local fallback locks live under `lock_dir`.
    pub fn new(
        registry: Arc<dyn ConnectorRegistry>,
        state: Arc<InstanceState>,
        lock_dir: impl Into<PathBuf>,
    ) -> InstanceLeases {
        InstanceLeases {
            registry,
            state,
            files: FileLeases::new(lock_dir),
            fallback: Mutex::new(HashSet::new()),
            warnings: Mutex::new(Vec::new()),
        }
    }

    /// The one-time fallback warnings issued so far.
    pub fn warnings(&self) -> Vec<String> {
        self.warnings
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn uses_files(&self, instance: &str) -> bool {
        self.fallback
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(instance)
    }

    fn fall_back(&self, instance: &str) {
        let first = self
            .fallback
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(instance.to_string());
        if first {
            let msg = format!(
                "Instance '{instance}' does not implement pipe leases; using machine-local file \
                 locks under {} (jobs on other machines are not excluded).",
                self.files.dir().display()
            );
            tracing::warn!("{msg}");
            self.warnings
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(msg);
        }
    }
}

impl LeaseSource for InstanceLeases {
    async fn acquire(
        &self,
        node: &PipeRef,
        holder: &str,
        ttl: Duration,
    ) -> Result<bool, StateError> {
        let instance = node.instance_keys().to_string();
        if self.uses_files(&instance) {
            return self.files.try_acquire(node, holder);
        }
        let conn = self
            .registry
            .instance(&instance)
            .map_err(|e| StateError::Unreachable {
                instance: instance.clone(),
                message: e,
            })?;
        let pipe = self.state.pipe_for(node);
        match conn.acquire_pipe_lease(&pipe, holder, ttl).await {
            Ok(b) => Ok(b),
            Err(ConnectorError::NotImplemented(_)) => {
                self.fall_back(&instance);
                self.files.try_acquire(node, holder)
            }
            Err(e) => Err(StateError::Unreachable {
                instance,
                message: e.to_string(),
            }),
        }
    }

    async fn renew(&self, node: &PipeRef, holder: &str, ttl: Duration) -> Result<bool, StateError> {
        if self.uses_files(node.instance_keys()) {
            return self.files.renew(node, holder, ttl).await;
        }
        // `acquire_pipe_lease` extends the lease for its current holder.
        self.acquire(node, holder, ttl).await
    }

    async fn release(&self, node: &PipeRef, holder: &str) -> Result<(), StateError> {
        let instance = node.instance_keys().to_string();
        if self.uses_files(&instance) {
            self.files.release_lease(node, holder);
            return Ok(());
        }
        let Ok(conn) = self.registry.instance(&instance) else {
            return Ok(());
        };
        let pipe = self.state.pipe_for(node);
        match conn.release_pipe_lease(&pipe, holder).await {
            Ok(_) | Err(ConnectorError::NotImplemented(_)) => Ok(()),
            Err(e) => Err(StateError::Unreachable {
                instance,
                message: e.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn file_leases_exclude_other_holders() {
        let dir =
            std::env::temp_dir().join(format!("mrsm_leases_{}", uuid::Uuid::new_v4().simple()));
        let a = FileLeases::new(&dir);
        let b = FileLeases::new(&dir);
        let r = PipeRef::from_string("sql:x|sql:x|m|None");
        let ttl = Duration::from_secs(1);
        assert!(a.acquire(&r, "a", ttl).await.unwrap());
        assert!(a.acquire(&r, "a", ttl).await.unwrap());
        assert!(!a.acquire(&r, "b", ttl).await.unwrap());
        // A second handle (a second open file description) is excluded by the flock.
        assert!(!b.acquire(&r, "b", ttl).await.unwrap());
        assert!(a.renew(&r, "a", ttl).await.unwrap());
        a.release(&r, "a").await.unwrap();
        assert!(b.acquire(&r, "b", ttl).await.unwrap());
        b.release(&r, "b").await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
