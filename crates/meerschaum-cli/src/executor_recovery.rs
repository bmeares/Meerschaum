//! Inert, bounded intent for work stopped by a controlled executor configuration rebuild.
//!
//! This is not a command log. It never recreates a definition or authorizes a launch: a fresh
//! signed context at the captured revision must arrive before one existing job may restart.

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use meerschaum_core::team_scope::{create_private_dir, write_private};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const MAX_INTENTS: usize = 1024;
const MAX_BYTES: u64 = 256 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    pub revision: u64,
    /// Persist capture before stopping, then confirm only after this controlled stop succeeds.
    /// An unobserved crash during capture/stop is an explicit-start recovery case.
    pub stopped: bool,
    /// A paused process cannot be recreated as paused without first executing its command.
    /// Keep its definition stopped; only an explicit main-authorized start may run it again.
    pub paused: bool,
}

pub(super) struct Recovery {
    directory: PathBuf,
    lock: Mutex<()>,
}

fn unavailable() -> io::Error {
    io::Error::other("Controlled executor recovery is unavailable; use an explicit job start.")
}

fn canonical(name: &str) -> bool {
    name.strip_prefix("mrsm-")
        .and_then(|value| Uuid::parse_str(value).ok())
        .is_some_and(|id| !id.is_nil() && name == format!("mrsm-{}", id.simple()))
}

impl Recovery {
    pub(super) fn new(root: &Path) -> Self {
        Self {
            directory: root.join("executor-recovery"),
            lock: Mutex::new(()),
        }
    }

    fn read(&self) -> io::Result<BTreeMap<String, Intent>> {
        let path = self.directory.join("pending.json");
        match std::fs::symlink_metadata(&self.directory) {
            Ok(metadata) if !metadata.is_dir() => return Err(unavailable()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(_) => return Err(unavailable()),
            Ok(_) => {}
        }
        #[cfg(unix)]
        let opened = rustix::fs::open(
            path,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK,
            rustix::fs::Mode::empty(),
        )
        .map(std::fs::File::from)
        .map_err(io::Error::from);
        #[cfg(not(unix))]
        let opened = std::fs::File::open(path);
        let file = match opened {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(_) => return Err(unavailable()),
        };
        let metadata = file.metadata().map_err(|_| unavailable())?;
        if !metadata.is_file() || metadata.len() > MAX_BYTES {
            return Err(unavailable());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(unavailable());
            }
        }
        let mut bytes = Vec::new();
        file.take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| unavailable())?;
        let intents: BTreeMap<String, Intent> =
            serde_json::from_slice(&bytes).map_err(|_| unavailable())?;
        if bytes.len() as u64 > MAX_BYTES
            || intents.len() > MAX_INTENTS
            || intents
                .iter()
                .any(|(name, intent)| !canonical(name) || intent.revision == 0)
        {
            return Err(unavailable());
        }
        Ok(intents)
    }

    fn write(&self, intents: &BTreeMap<String, Intent>) -> io::Result<()> {
        let encoded = serde_json::to_string(intents).map_err(|_| unavailable())?;
        if intents.len() > MAX_INTENTS || encoded.len() as u64 > MAX_BYTES {
            return Err(unavailable());
        }
        create_private_dir(&self.directory).map_err(|_| unavailable())?;
        write_private(&self.directory.join("pending.json"), &encoded).map_err(|_| unavailable())
    }

    /// The caller has observed these jobs actually running or paused, with a current signed
    /// context. Persist before stopping them, and retain earlier waiting intents across retries.
    pub(super) fn capture(&self, active: Vec<(String, Intent)>) -> io::Result<()> {
        let _lock = self.lock.lock().unwrap_or_else(|error| error.into_inner());
        let mut intents = self.read()?;
        for (name, mut intent) in active {
            if !canonical(&name) || intent.revision == 0 {
                return Err(unavailable());
            }
            if intents
                .get(&name)
                .is_some_and(|old| old.revision > intent.revision)
            {
                return Err(unavailable());
            }
            intent.stopped = false;
            intents.insert(name, intent);
        }
        self.write(&intents)
    }

    pub(super) fn confirm_stopped(&self, name: &str, revision: u64) -> io::Result<()> {
        let _lock = self.lock.lock().unwrap_or_else(|error| error.into_inner());
        let mut intents = self.read()?;
        let intent = intents.get_mut(name).ok_or_else(unavailable)?;
        if intent.revision != revision {
            return Err(unavailable());
        }
        intent.stopped = true;
        self.write(&intents)
    }

    /// Consume durably BEFORE attempting a launch. A crash or uncertain start cannot replay a
    /// one-off command; an explicit main start can recover that availability failure instead.
    /// A later revision belongs to an explicit start, so it only retires the earlier intent.
    pub(super) fn take(&self, name: &str, revision: u64) -> io::Result<Option<Intent>> {
        let _lock = self.lock.lock().unwrap_or_else(|error| error.into_inner());
        let mut intents = self.read()?;
        let Some(intent) = intents.get(name) else {
            return Ok(None);
        };
        if revision < intent.revision {
            return Err(unavailable());
        }
        let intent = intents
            .remove(name)
            .expect("entry checked under the same lock");
        self.write(&intents)?;
        Ok((revision == intent.revision && intent.stopped).then_some(intent))
    }

    /// Context removal must cancel its pending rebuild even after the old contexts were cleared.
    /// Delayed retirement cannot cancel a newer intent.
    pub(super) fn cancel(&self, name: &str, revision: u64) -> io::Result<()> {
        let _lock = self.lock.lock().unwrap_or_else(|error| error.into_inner());
        let mut intents = self.read()?;
        let Some(intent) = intents.get(name) else {
            return Ok(());
        };
        if intent.revision != revision {
            return Err(unavailable());
        }
        intents.remove(name);
        self.write(&intents)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name() -> String {
        format!("mrsm-{}", Uuid::new_v4().simple())
    }

    #[test]
    fn failed_build_retry_keeps_intent_but_consumed_one_off_is_never_replayed() {
        let root = tempfile::tempdir().unwrap();
        let name = name();
        let recovery = Recovery::new(root.path());
        let intent = Intent {
            revision: 3,
            stopped: true,
            paused: false,
        };
        recovery
            .capture(vec![(name.clone(), intent.clone())])
            .unwrap();
        recovery.confirm_stopped(&name, 3).unwrap();
        // A failed build calls revoke then retries without any still-running jobs.
        recovery.capture(Vec::new()).unwrap();
        let reopened = Recovery::new(root.path());
        assert_eq!(reopened.take(&name, 3).unwrap(), Some(intent));
        assert_eq!(Recovery::new(root.path()).take(&name, 3).unwrap(), None);
    }

    #[test]
    fn unconfirmed_stop_after_a_crash_requires_explicit_start() {
        let root = tempfile::tempdir().unwrap();
        let name = name();
        let recovery = Recovery::new(root.path());
        recovery
            .capture(vec![(
                name.clone(),
                Intent {
                    revision: 1,
                    stopped: true,
                    paused: false,
                },
            )])
            .unwrap();
        // Even a caller-supplied true is reset during capture. No observed stop, no replay.
        assert!(!recovery.read().unwrap()[&name].stopped);
        assert_eq!(Recovery::new(root.path()).take(&name, 1).unwrap(), None);
        assert_eq!(recovery.take(&name, 1).unwrap(), None);
    }

    #[test]
    fn only_matching_revision_recovers_and_late_delete_cannot_cancel_newer_intent() {
        let root = tempfile::tempdir().unwrap();
        let recovery = Recovery::new(root.path());
        let name = name();
        recovery
            .capture(vec![(
                name.clone(),
                Intent {
                    revision: 4,
                    stopped: false,
                    paused: true,
                },
            )])
            .unwrap();
        recovery.confirm_stopped(&name, 4).unwrap();
        assert!(recovery.cancel(&name, 3).is_err());
        assert!(recovery.take(&name, 3).is_err());
        assert!(recovery.take(&name, 4).unwrap().unwrap().paused);
        recovery
            .capture(vec![(
                name.clone(),
                Intent {
                    revision: 4,
                    stopped: false,
                    paused: false,
                },
            )])
            .unwrap();
        assert_eq!(recovery.take(&name, 5).unwrap(), None);
        assert_eq!(recovery.take(&name, 4).unwrap(), None);
        recovery
            .capture(vec![(
                name.clone(),
                Intent {
                    revision: 6,
                    stopped: false,
                    paused: false,
                },
            )])
            .unwrap();
        recovery.cancel(&name, 6).unwrap();
        assert_eq!(recovery.take(&name, 6).unwrap(), None);
    }

    #[test]
    fn malformed_or_unbounded_storage_and_noncanonical_names_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let recovery = Recovery::new(root.path());
        assert!(
            recovery
                .capture(vec![(
                    "../job".into(),
                    Intent {
                        revision: 1,
                        stopped: false,
                        paused: false
                    }
                )])
                .is_err()
        );
        create_private_dir(&recovery.directory).unwrap();
        write_private(&recovery.directory.join("pending.json"), "{broken").unwrap();
        assert!(recovery.capture(Vec::new()).is_err());
        let names = (0..=MAX_INTENTS)
            .map(|_| {
                (
                    name(),
                    Intent {
                        revision: 1,
                        stopped: false,
                        paused: false,
                    },
                )
            })
            .collect();
        write_private(&recovery.directory.join("pending.json"), "{}").unwrap();
        assert!(recovery.capture(names).is_err());
        assert!(recovery.read().unwrap().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn recovery_storage_cannot_follow_a_symlink_or_fifo() {
        let root = tempfile::tempdir().unwrap();
        let recovery = Recovery::new(root.path());
        create_private_dir(&recovery.directory).unwrap();
        let path = recovery.directory.join("pending.json");
        let external = root.path().join("external.json");
        write_private(&external, "{}").unwrap();
        std::os::unix::fs::symlink(&external, &path).unwrap();
        assert!(recovery.read().is_err());
        std::fs::remove_file(&path).unwrap();
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            &path,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .unwrap();
        assert!(recovery.read().is_err());
    }
}
