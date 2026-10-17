//! Back up and restore configuration key files so that edits may be rolled back: a port of
//! `meerschaum/config/_backup.py`.
//!
//! A snapshot of a key's on-disk file (e.g. `stack.yaml`) is taken before it is edited. If an edit
//! corrupts the file or overwrites values with defaults, the previous state can be restored with
//! [`ConfigState::restore_config_backup`]. Backups live under
//! `CONFIG_BACKUPS_RESOURCES_PATH/<key>/<UTC timestamp><suffix>`, newest first by file name.

use std::path::{Path, PathBuf};

use super::ConfigState;
use crate::static_config::static_config;

/// The timestamp format of a backup file name (UTC, lexically sortable): `%Y%m%d-%H%M%S-%f`.
const TIMESTAMP_FORMAT: &str = "%Y%m%d-%H%M%S-%6f";

/// `shutil.copy2`: the bytes, the permissions and the modification time.
fn copy2(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::copy(from, to)?;
    let modified = std::fs::metadata(from)?.modified()?;
    let file = std::fs::OpenOptions::new().write(true).open(to)?;
    file.set_modified(modified)
}

/// `Path.suffix`: the last `.ext` of the file name (empty for none or a leading-dot name).
fn suffix(path: &Path) -> String {
    match path.file_name().and_then(|n| n.to_str()) {
        Some(name) => match name.rfind('.') {
            Some(i) if i > 0 && i + 1 < name.len() => name[i..].to_string(),
            _ => String::new(),
        },
        None => String::new(),
    }
}

impl ConfigState {
    /// The number of backups to retain per config key.
    pub fn max_backups_per_key() -> usize {
        usize::try_from(static_config().config.max_backups_per_key).unwrap_or(10)
    }

    /// The directory holding the backups of a config `key` (created when `create`).
    pub fn key_backups_dir(&self, key: &str, create: bool) -> PathBuf {
        let dir = self.paths().path("CONFIG_BACKUPS_RESOURCES_PATH").join(key);
        if create {
            let _ = std::fs::create_dir_all(&dir);
        }
        dir
    }

    /// The existing backups of `key`, newest first.
    pub fn list_config_backups(&self, key: &str) -> Vec<PathBuf> {
        let dir = self.key_backups_dir(key, false);
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return Vec::new();
        };
        let mut backups: Vec<PathBuf> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .collect();
        backups.sort_by(|a, b| b.file_name().cmp(&a.file_name()));
        backups
    }

    /// Copy the current on-disk file for `key` into the backups directory.
    ///
    /// Returns the new backup, the most recent backup when the file is unchanged since it, or
    /// `None` when there was nothing to back up.
    pub fn backup_config_key(&mut self, key: &str) -> Option<PathBuf> {
        let keyfile = self.get_keyfile_path(key, false, None);
        let Some(keyfile) = keyfile.filter(|k| k.exists()) else {
            self.debug(format!("No config file to back up for '{key}'."));
            return None;
        };
        // Skip redundant backups if the newest one is byte-for-byte identical.
        let existing = self.list_config_backups(key);
        if let (Some(newest), Ok(current)) = (existing.first(), std::fs::read(&keyfile)) {
            if std::fs::read(newest).is_ok_and(|b| b == current) {
                let newest = newest.clone();
                self.debug(format!("'{key}' unchanged since last backup; skipping."));
                return Some(newest);
            }
        }
        let timestamp = chrono::Utc::now().format(TIMESTAMP_FORMAT).to_string();
        let backups_dir = self.key_backups_dir(key, true);
        let backup_path = backups_dir.join(format!("{timestamp}{}", suffix(&keyfile)));
        if let Err(e) = copy2(&keyfile, &backup_path) {
            self.debug(format!("Failed to back up '{key}':\n{e}"));
            return None;
        }
        self.debug(format!(
            "Backed up '{key}' to '{}'.",
            backup_path.to_string_lossy()
        ));
        self.prune_backups(key);
        Some(backup_path)
    }

    /// Delete the oldest backups beyond the retention limit.
    fn prune_backups(&self, key: &str) {
        for old in self
            .list_config_backups(key)
            .into_iter()
            .skip(Self::max_backups_per_key())
        {
            let _ = std::fs::remove_file(old);
        }
    }

    /// Restore a key's config file from a backup (`restore_config_backup`, minus the reload).
    ///
    /// The current (possibly broken) file is itself backed up first, so a restore can always be
    /// undone. Without `backup_path`, the `index`-th most recent backup is used (Python indexing:
    /// a negative index counts from the oldest). Returns `(success, message)`.
    pub fn restore_config_backup(
        &mut self,
        key: &str,
        backup_path: Option<&Path>,
        index: i64,
    ) -> (bool, String) {
        let backups = self.list_config_backups(key);
        let backup_path: PathBuf = match backup_path {
            Some(p) => p.to_path_buf(),
            None => {
                if backups.is_empty() {
                    return (
                        false,
                        format!("No configuration backups found for '{key}'."),
                    );
                }
                let n = backups.len() as i64;
                let resolved = if index < 0 { index + n } else { index };
                match usize::try_from(resolved).ok().and_then(|i| backups.get(i)) {
                    Some(p) => p.clone(),
                    None => {
                        return (
                            false,
                            format!(
                                "No backup at index {index} for '{key}' (found {}).",
                                backups.len()
                            ),
                        );
                    }
                }
            }
        };
        if !backup_path.exists() {
            return (
                false,
                format!("Backup '{}' does not exist.", backup_path.display()),
            );
        }
        let Some(keyfile) = self.get_keyfile_path(key, true, None) else {
            return (false, format!("No config file for '{key}'."));
        };
        let parent = keyfile
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        // Restore using the backup's original filetype.
        let target = parent.join(format!("{key}{}", suffix(&backup_path)));

        // Snapshot the current state before overwriting it.
        let _ = self.backup_config_key(key);

        if let Err(e) = copy2(&backup_path, &target) {
            return (
                false,
                format!(
                    "Failed to restore '{key}' from '{}':\n{e}",
                    backup_path.display()
                ),
            );
        }

        // Remove stale key files with a different filetype to avoid ambiguity
        // (e.g. a leftover `stack.json` when restoring `stack.yaml`).
        let target_resolved = std::fs::canonicalize(&target).unwrap_or_else(|_| target.clone());
        if let Ok(entries) = std::fs::read_dir(&parent) {
            let prefix = format!("{key}.");
            for entry in entries.filter_map(|e| e.ok()) {
                let name = entry.file_name().to_string_lossy().to_string();
                if !name.starts_with(&prefix) {
                    continue;
                }
                let path = entry.path();
                let resolved = std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
                if resolved != target_resolved {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }

        (
            true,
            format!(
                "Restored '{key}' from backup '{}'.",
                backup_path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default()
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{Config, ConfigOptions};

    fn registry(root: &Path) -> Config {
        Config::new(ConfigOptions::with_env(vec![(
            "MRSM_ROOT_DIR".to_string(),
            root.to_string_lossy().into_owned(),
        )]))
        .unwrap()
    }

    use std::path::Path;

    #[test]
    fn backups_dedupe_prune_and_restore() {
        let root = tempfile::tempdir().unwrap();
        let config = registry(root.path());
        let dir = config.paths().config_dir;
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("thing.yaml");
        std::fs::write(&file, "a: 1\n").unwrap();

        config.with_state(|s| {
            // Nothing to back up for a key without a file.
            assert!(s.backup_config_key("absent").is_none());
            let first = s.backup_config_key("thing").unwrap();
            assert_eq!(std::fs::read_to_string(&first).unwrap(), "a: 1\n");
            assert!(first.extension().is_some_and(|e| e == "yaml"));
            // Unchanged: the same backup comes back.
            assert_eq!(s.backup_config_key("thing").unwrap(), first);
            std::fs::write(&file, "a: 2\n").unwrap();
            let second = s.backup_config_key("thing").unwrap();
            assert_ne!(second, first);
            assert_eq!(
                s.list_config_backups("thing"),
                vec![second.clone(), first.clone()]
            );

            // Restore the older one (index 1): the current file is snapshotted first.
            let (ok, message) = s.restore_config_backup("thing", None, 1);
            assert!(ok, "{message}");
            assert!(message.starts_with("Restored 'thing' from backup '"));
            assert_eq!(std::fs::read_to_string(&file).unwrap(), "a: 1\n");

            let (ok, message) = s.restore_config_backup("thing", None, 9);
            assert!(!ok);
            assert!(message.starts_with("No backup at index 9 for 'thing' (found "));
            let (ok, message) = s.restore_config_backup("nothing", None, 0);
            assert!(!ok);
            assert_eq!(message, "No configuration backups found for 'nothing'.");
        });
    }

    #[test]
    fn a_restore_removes_stale_files_of_another_filetype() {
        let root = tempfile::tempdir().unwrap();
        let config = registry(root.path());
        let dir = config.paths().config_dir;
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("thing.json"), "{\"a\": 1}").unwrap();
        config.with_state(|s| {
            let backup = s.backup_config_key("thing").unwrap();
            std::fs::remove_file(dir.join("thing.json")).unwrap();
            std::fs::write(dir.join("thing.yaml"), "b: 2\n").unwrap();
            let (ok, message) = s.restore_config_backup("thing", Some(&backup), 0);
            assert!(ok, "{message}");
            assert!(dir.join("thing.json").exists());
            assert!(!dir.join("thing.yaml").exists());
        });
    }
}
