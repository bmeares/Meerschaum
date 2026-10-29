//! The DataFrame library of a root (`meerschaum:dataframe`, rc15 Q54).
//!
//! Meerschaum 5.0.0rc15 returns Polars DataFrames by default (`dataframe: polars` in the default
//! `meerschaum` config, so a fresh root's `meerschaum.yaml` is written with it). A root created by
//! Meerschaum 4 or by an earlier 5.0 release candidate keeps pandas: the first time an rc15 (or
//! newer) process opens it, [`pin_old_root`] adds `dataframe: pandas` to its `meerschaum.yaml`.
//!
//! **The marker is the key itself.** Every release from rc15 on writes `meerschaum.yaml` with a
//! `dataframe` key (from the defaults, or loaded from the file), so a `meerschaum` file without
//! one was written by an older release (or by hand), and a fresh rc15 root is never mistaken for
//! an old one on its second run. A root without a `meerschaum` file yet is new. Removing the line
//! later brings the pin back (with its notice); `dataframe: polars` is how a root switches.
//!
//! The hosts call it once per process, before their registry loads any key (the CLI at startup,
//! the Python config boundary before its first file read), so no registry ever merges the
//! `polars` default into an old root's loaded config and writes it back before the pin.
//!
//! The pin appends one line and keeps the rest of the file byte for byte (comments included); a
//! file the line cannot be appended to (a flow-style mapping, a document end marker) is rewritten
//! from its parsed content instead, as `write_config` would. Concurrent processes serialize on a
//! lock file in the temporary directory, and the file is re-read under the lock.

use std::io::Write;
use std::path::{Path, PathBuf};

use super::lossless::{ConfigValue, json_dumps, json_loads};
use super::{ConfigState, DEFAULT_HEADER_COMMENT, FILETYPES};
use crate::pyfmt::JsonStyle;
use crate::yaml::dumper::{DumpOptions, dump_node};
use crate::yaml::loader::{LoadOptions, load_text};

/// The config key (under `meerschaum`) that names the DataFrame library.
pub const DATAFRAME_KEY: &str = "dataframe";

/// The library a root that predates rc15 keeps.
pub const PINNED_LIBRARY: &str = "pandas";

/// The library of a fresh root (the default config's value).
pub const DEFAULT_LIBRARY: &str = "polars";

/// What [`pin_old_root`] found or did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinOutcome {
    /// No `meerschaum` file: a new root (its file is written from the defaults).
    NewRoot,
    /// The `meerschaum` file sets `dataframe` already.
    Decided,
    /// The file is not a mapping, or cannot be read or parsed: left alone (the registry reports
    /// a parse error itself).
    Skipped,
    /// `dataframe: pandas` was added; `notice` is the one line to show.
    Pinned {
        /// The notice.
        notice: String,
    },
    /// The root predates rc15 but the pin could not be written (a read-only config directory):
    /// `notice` says what to add by hand.
    Failed {
        /// The notice.
        notice: String,
    },
}

/// The `meerschaum` keyfile of a config directory, chosen as the registry chooses it.
fn meerschaum_keyfile(config_dir: &Path) -> Option<PathBuf> {
    let mut names: Vec<String> = FILETYPES
        .iter()
        .map(|ft| format!("meerschaum.{ft}"))
        .filter(|name| config_dir.join(name).is_file())
        .collect();
    if names.is_empty() {
        return None;
    }
    names.sort();
    let selection = ConfigState::select_filekeys(&names, Some(&["meerschaum".to_string()]));
    selection.filekeys.first().map(|name| config_dir.join(name))
}

/// Parse a `meerschaum` file's text (`None` when it does not parse).
fn parse(text: &str, path: &Path, json: bool) -> Option<ConfigValue> {
    if json {
        return json_loads(text).ok();
    }
    load_text(
        text,
        &super::paths::posix(path),
        false,
        LoadOptions {
            safe: false,
            project: false,
        },
    )
    .ok()
    .map(|node| ConfigValue::from_node(&node))
}

/// The one-line notice of a pinned root.
pub fn pinned_notice(path: &Path) -> String {
    format!(
        "Meerschaum now returns Polars DataFrames by default, but this root was created by an \
         earlier version: added `dataframe: pandas` to {} to keep pandas (set it to `polars` \
         to switch).",
        super::paths::posix(path)
    )
}

/// The notice of a root whose pin could not be written.
pub fn failed_notice(path: &Path, error: &str) -> String {
    format!(
        "Meerschaum now returns Polars DataFrames by default, but this root was created by an \
         earlier version and `dataframe: pandas` could not be added to {} ({error}): add \
         `dataframe: pandas` (or `polars`) to it to choose.",
        super::paths::posix(path)
    )
}

/// The new content of a `meerschaum` file that lacks `dataframe`: the original text with one line
/// appended when that parses to the same mapping plus the key, else the mapping re-emitted.
fn pinned_text(text: &str, mapping: &ConfigValue, path: &Path, json: bool) -> Option<String> {
    let mut expected = mapping.clone();
    expected
        .as_map_mut()?
        .insert(DATAFRAME_KEY.to_string(), ConfigValue::str(PINNED_LIBRARY));
    if json {
        return json_dumps(&expected, JsonStyle::Indent(2), true).ok();
    }
    let mut appended = text.to_string();
    if !appended.is_empty() && !appended.ends_with('\n') {
        appended.push('\n');
    }
    appended.push_str(&format!("{DATAFRAME_KEY}: {PINNED_LIBRARY}\n"));
    if parse(&appended, path, false).as_ref() == Some(&expected) {
        return Some(appended);
    }
    let dumped = dump_node(
        &expected.to_node(),
        &DumpOptions {
            sort_keys: false,
            ..DumpOptions::default()
        },
    )
    .ok()?;
    let rewritten = format!("{DEFAULT_HEADER_COMMENT}{dumped}");
    (parse(&rewritten, path, false).as_ref() == Some(&expected)).then_some(rewritten)
}

/// A lock that serializes the pin of one config directory across processes (a file in the
/// temporary directory, so nothing is added to the root). Best effort: a lock that cannot be
/// taken within a few seconds is skipped (the file is re-read before it is written either way).
struct PinLock(Option<std::fs::File>);

impl PinLock {
    fn acquire(config_dir: &Path) -> PinLock {
        // FNV-1a of the canonical path: the same name in every build (the CLI and the extension).
        let canonical = std::fs::canonicalize(config_dir).unwrap_or_else(|_| config_dir.into());
        let hash = canonical
            .to_string_lossy()
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
                (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
            });
        let path = std::env::temp_dir().join(format!("mrsm-dataframe-pin-{hash:016x}.lock"));
        let Ok(file) = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
        else {
            return PinLock(None);
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match file.try_lock() {
                Ok(()) => return PinLock(Some(file)),
                Err(std::fs::TryLockError::WouldBlock) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(25));
                }
                Err(_) => return PinLock(None),
            }
        }
    }
}

impl Drop for PinLock {
    fn drop(&mut self) {
        if let Some(file) = self.0.take() {
            let _ = file.unlock();
        }
    }
}

/// Write the pin: append the line in place (keeps permissions, symlinks and every byte of the
/// file) or, when the text had to be re-emitted, replace the content in place.
fn write_pin(path: &Path, original: &str, new_text: &str) -> std::io::Result<()> {
    if let Some(suffix) = new_text.strip_prefix(original) {
        let mut file = std::fs::OpenOptions::new().append(true).open(path)?;
        file.write_all(suffix.as_bytes())?;
        return file.sync_all();
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)?;
    file.write_all(new_text.as_bytes())?;
    file.sync_all()
}

/// Read the `meerschaum` file of `config_dir` and decide (see the module docs).
fn check(config_dir: &Path) -> Result<(PathBuf, String, ConfigValue, bool), PinOutcome> {
    let Some(path) = meerschaum_keyfile(config_dir) else {
        return Err(PinOutcome::NewRoot);
    };
    let json = path.extension().is_some_and(|ext| ext == "json");
    let Ok(bytes) = std::fs::read(&path) else {
        return Err(PinOutcome::Skipped);
    };
    let Ok(text) = String::from_utf8(bytes) else {
        return Err(PinOutcome::Skipped);
    };
    let Some(mapping) = parse(&text, &path, json) else {
        return Err(PinOutcome::Skipped);
    };
    let Some(map) = mapping.as_map() else {
        return Err(PinOutcome::Skipped);
    };
    if map.contains_key(DATAFRAME_KEY) {
        return Err(PinOutcome::Decided);
    }
    Ok((path, text, mapping, json))
}

/// The environment variable that overrides `meerschaum:dataframe` for a process (and the jobs and
/// workers it starts).
pub const DATAFRAME_ENV_VAR: &str = "MRSM_DATAFRAME";

/// The DataFrame library of this process, as a host tells the Python workers it calls (so a
/// custom instance connector's `sync_pipe(pipe, df)` gets the host's library, also when the
/// setting changed after the worker started): `MRSM_DATAFRAME` when set, else
/// `meerschaum:dataframe` of the live config ([`super::Config::current`]); `None` when neither
/// names `polars` or `pandas` (the worker then reads its own setting, and reports a bad value).
pub fn process_library() -> Option<String> {
    let normalize = |value: &str| -> Option<String> {
        let value = value.trim().to_lowercase();
        matches!(value.as_str(), "polars" | "pandas").then_some(value)
    };
    if let Ok(value) = std::env::var(DATAFRAME_ENV_VAR)
        && !value.trim().is_empty()
    {
        return normalize(&value);
    }
    let lookup = |config: &super::Config| -> Option<String> {
        let (valid, value) = config.get_with(
            &["meerschaum", DATAFRAME_KEY],
            super::GetOptions {
                patch: false,
                substitute: false,
                write_missing: false,
                warn: false,
            },
        );
        if !valid {
            return None;
        }
        value.as_str().and_then(normalize)
    };
    // A Python host publishes its live config before each native call (it may change); a host
    // without one (the CLI) reads its root's files once.
    let live = super::LIVE_SNAPSHOT
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    match live {
        Some(config) => lookup(&config),
        None => {
            static FROM_FILES: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
            FROM_FILES
                .get_or_init(|| super::Config::from_env().ok().and_then(|c| lookup(&c)))
                .clone()
        }
    }
}

/// [`pin_old_root`] for the config directory the process environment names (`MRSM_ROOT_DIR`,
/// `MRSM_CONFIG_DIR`, ...), without creating any directory: what the CLI runs at startup. `None`
/// when the paths are invalid (the registry reports that itself).
pub fn pin_process_root() -> Option<PinOutcome> {
    let opts = super::ConfigOptions::from_process();
    let lookup = |key: &str| opts.lookup(key);
    let paths = super::paths::Paths::from_env_with_writes(
        &lookup,
        &opts.home,
        opts.windows,
        opts.package_root.clone(),
        false,
    )
    .ok()?;
    Some(pin_old_root(&paths.config_dir))
}

/// Give a root created before rc15 `dataframe: pandas` (see the module docs). Idempotent: a root
/// whose `meerschaum` file sets `dataframe` (or that has no such file yet) is left alone.
pub fn pin_old_root(config_dir: &Path) -> PinOutcome {
    if let Err(outcome) = check(config_dir) {
        return outcome;
    }
    let _lock = PinLock::acquire(config_dir);
    // Another process may have pinned the root while this one waited for the lock.
    let (path, text, mapping, json) = match check(config_dir) {
        Ok(found) => found,
        Err(outcome) => return outcome,
    };
    let Some(new_text) = pinned_text(&text, &mapping, &path, json) else {
        return PinOutcome::Skipped;
    };
    match write_pin(&path, &text, &new_text) {
        Ok(()) => PinOutcome::Pinned {
            notice: pinned_notice(&path),
        },
        Err(e) => PinOutcome::Failed {
            notice: failed_notice(&path, &e.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mrsm-dataframe-pin-test-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn new_root_is_left_alone() {
        let dir = tmp("new");
        assert_eq!(pin_old_root(&dir), PinOutcome::NewRoot);
        assert!(std::fs::read_dir(&dir).unwrap().next().is_none());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn old_yaml_root_gets_one_appended_line() {
        let dir = tmp("yaml");
        let original = "# my comment\ninstance: sql:local\nconnectors:\n  sql:\n    local:\n      flavor: sqlite\n      database: /tmp/x.db";
        std::fs::write(dir.join("meerschaum.yaml"), original).unwrap();
        let PinOutcome::Pinned { notice } = pin_old_root(&dir) else {
            panic!("expected a pin");
        };
        assert!(notice.contains("dataframe: pandas"), "{notice}");
        assert!(!notice.contains('\n'), "one line: {notice}");
        let text = std::fs::read_to_string(dir.join("meerschaum.yaml")).unwrap();
        assert_eq!(text, format!("{original}\ndataframe: pandas\n"));
        // Idempotent: the second run (of any process) finds the key.
        assert_eq!(pin_old_root(&dir), PinOutcome::Decided);
        assert_eq!(
            std::fs::read_to_string(dir.join("meerschaum.yaml")).unwrap(),
            text
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn explicit_setting_is_respected() {
        let dir = tmp("explicit");
        let original = "instance: sql:local\ndataframe: polars\n";
        std::fs::write(dir.join("meerschaum.yaml"), original).unwrap();
        assert_eq!(pin_old_root(&dir), PinOutcome::Decided);
        assert_eq!(
            std::fs::read_to_string(dir.join("meerschaum.yaml")).unwrap(),
            original
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn flow_mapping_is_rewritten() {
        let dir = tmp("flow");
        std::fs::write(dir.join("meerschaum.yaml"), "{instance: 'sql:local'}\n").unwrap();
        assert!(matches!(pin_old_root(&dir), PinOutcome::Pinned { .. }));
        let text = std::fs::read_to_string(dir.join("meerschaum.yaml")).unwrap();
        let parsed = parse(&text, &dir.join("meerschaum.yaml"), false).unwrap();
        assert_eq!(
            parsed.get("dataframe").and_then(ConfigValue::as_str),
            Some("pandas")
        );
        assert_eq!(
            parsed.get("instance").and_then(ConfigValue::as_str),
            Some("sql:local")
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn json_root_is_pinned() {
        let dir = tmp("json");
        std::fs::write(dir.join("meerschaum.json"), "{\"instance\": \"sql:local\"}").unwrap();
        assert!(matches!(pin_old_root(&dir), PinOutcome::Pinned { .. }));
        let parsed =
            json_loads(&std::fs::read_to_string(dir.join("meerschaum.json")).unwrap()).unwrap();
        assert_eq!(
            parsed.get("dataframe").and_then(ConfigValue::as_str),
            Some("pandas")
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn unparseable_or_non_mapping_files_are_left_alone() {
        let dir = tmp("bad");
        std::fs::write(dir.join("meerschaum.yaml"), "instance: [unclosed\n").unwrap();
        assert_eq!(pin_old_root(&dir), PinOutcome::Skipped);
        std::fs::write(dir.join("meerschaum.yaml"), "- a\n- b\n").unwrap();
        assert_eq!(pin_old_root(&dir), PinOutcome::Skipped);
        std::fs::write(dir.join("meerschaum.yaml"), "").unwrap();
        assert_eq!(pin_old_root(&dir), PinOutcome::Skipped);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn read_only_file_fails_with_a_notice() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tmp("readonly");
        let path = dir.join("meerschaum.yaml");
        std::fs::write(&path, "instance: sql:local\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
        // Root can write a read-only file anyway: only check the outcome when the write failed.
        match pin_old_root(&dir) {
            PinOutcome::Failed { notice } => assert!(notice.contains("could not be added")),
            PinOutcome::Pinned { .. } => {}
            other => panic!("unexpected {other:?}"),
        }
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn concurrent_pins_append_once() {
        let dir = tmp("concurrent");
        std::fs::write(dir.join("meerschaum.yaml"), "instance: sql:local\n").unwrap();
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let dir = dir.clone();
                std::thread::spawn(move || pin_old_root(&dir))
            })
            .collect();
        let outcomes: Vec<PinOutcome> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(
            outcomes
                .iter()
                .filter(|o| matches!(o, PinOutcome::Pinned { .. }))
                .count(),
            1,
            "{outcomes:?}"
        );
        let text = std::fs::read_to_string(dir.join("meerschaum.yaml")).unwrap();
        assert_eq!(text.matches("dataframe: pandas").count(), 1, "{text}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
