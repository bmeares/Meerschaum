//! The stack's database across PostgreSQL majors, and its backups (v5):
//!
//! - `upgrade postgres [<major> | <image> | rollback]` moves the stack's database to another
//!   PostgreSQL major ([`upgrade`]).
//! - `backup postgres` takes an online backup of it ([`backup`]).
//! - `restore postgres [<timestamp> | <path> | rollback]` puts a backup into a new volume and
//!   switches to it ([`restore_cmd`]).
//!
//! A new major cannot read the old data directory, and the stack's images ship one PostgreSQL
//! major each (no old binaries for `pg_upgrade`), so the upgrade is a dump and a restore, done the
//! way TimescaleDB requires (the same TimescaleDB version on both sides,
//! `timescaledb_pre_restore()` / `timescaledb_post_restore()`). The three commands share:
//!
//! - [`dump`]: `pg_dumpall --globals-only` and `pg_dump -Fc --create` of every database, each of a
//!   snapshot a session exports, with the database's fingerprint taken in the same snapshot,
//!   every file hashed and listed with `pg_restore --list`, into `<root>/backups/postgres/<UTC
//!   timestamp>/` (0700 directories, 0600 files);
//! - [`restore`]: a NEW volume set up by the target image in a temporary container (no network,
//!   no published port), the restore, the server settings, the verification against the
//!   fingerprints and the extension updates;
//! - [`switch`]: `stack.yaml` copied byte for byte into the run's directory, only the `db` image,
//!   its data volume and that volume's declaration edited, the stack brought up, and the undo of
//!   all of it when anything fails.
//!
//! The OLD VOLUME IS NEVER MODIFIED OR REMOVED here: `rollback` brings it back, and the commands to
//! remove it and the dumps are printed for when the user is satisfied.

mod backup;
mod docker;
mod dump;
mod fence;
mod fingerprint;
mod images;
mod leftovers;
mod plan;
mod restore;
mod restore_cmd;
mod rollback;
mod server;
mod signals;
mod sqltext;
mod stack_guard;
mod stackcfg;
mod switch;
mod upgrade;
mod yamledit;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::{Value as Json, json};
use sha2::Digest;

use meerschaum_core::SuccessTuple;
use meerschaum_core::config::{ConfigMap, ConfigValue};

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::value::ArgsMap;

use super::{flag, words};

pub(crate) use stack_guard::stack_up_preflight;

/// The stack's database service.
pub(crate) const DB_SERVICE: &str = "db";

/// The label on every container these commands create (its value is the run's ID).
pub(crate) const LABEL: &str = "io.meerschaum.upgrade-postgres";

/// Where the docs describe the manual procedure.
pub(crate) const DOCS_HINT: &str =
    "See \"Upgrading PostgreSQL\" in the docs (reference/stack-upgrades) for the manual procedure.";

/// The confirmation flags.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Options {
    /// `--yes` or `--force`: answer yes.
    pub yes: bool,
    /// `--noask`: take each question's default.
    pub noask: bool,
    /// `--force`: also go on despite the warnings that otherwise stop the command (other
    /// clients connected, low disk space).
    pub force: bool,
    /// `--debug`: print the Docker commands, keep a failed run's new volume for inspection.
    pub debug: bool,
}

impl Options {
    fn from_args(args: &ArgsMap) -> Options {
        Options {
            yes: flag(args, "yes") || flag(args, "force"),
            noask: flag(args, "noask"),
            force: flag(args, "force"),
            debug: flag(args, "debug"),
        }
    }
}

/// `upgrade postgres [<major> | <image> | rollback]`.
pub fn upgrade_postgres(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let opts = Options::from_args(args);
    docker::set_debug(opts.debug);
    let words = words(args, "action");
    match words.as_slice() {
        [w] if w == "rollback" => rollback::rollback(rt, &opts),
        [] => upgrade::upgrade(rt, &opts, None),
        [w] => upgrade::upgrade(rt, &opts, Some(w.as_str())),
        _ => Ok(SuccessTuple::fail(
            "`upgrade postgres` takes one word: a PostgreSQL major (e.g. `18`), an image (e.g. \
             `timescale/timescaledb-ha:pg18`), or `rollback`.",
        )),
    }
}

/// `backup postgres`.
pub fn backup_postgres(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let opts = Options::from_args(args);
    docker::set_debug(opts.debug);
    if !words(args, "action").is_empty() {
        return Ok(SuccessTuple::fail(
            "`backup postgres` takes no word: it backs up every database of the stack's `db`.",
        ));
    }
    backup::backup(rt, &opts)
}

/// `restore postgres [<timestamp> | <path> | rollback]`.
pub fn restore_postgres(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let opts = Options::from_args(args);
    docker::set_debug(opts.debug);
    let words = words(args, "action");
    match words.as_slice() {
        [w] if w == "rollback" => rollback::rollback(rt, &opts),
        [] => restore_cmd::restore(rt, &opts, None),
        [w] => restore_cmd::restore(rt, &opts, Some(w.as_str())),
        _ => Ok(SuccessTuple::fail(
            "`restore postgres` takes one word: a backup's timestamp or directory, or `rollback`.",
        )),
    }
}

/// A question, with the default action of the signals while it waits (see [`signals`]).
pub(crate) fn ask(
    rt: &mut Runtime<'_>,
    question: &str,
    default: &str,
    opts: &Options,
) -> Result<bool, crate::builtin::ActionError> {
    signals::prompt(|| super::yes_no(rt, question, default, opts.yes, opts.noask))
}

/// What Docker says when it does not answer.
pub(crate) const NO_DOCKER: &str = "Could not connect to Docker. Is the Docker service running? \
                                     (On Windows or macOS, make sure Docker Desktop is running.)";

/// Progress lines and their durations.
pub(crate) struct Steps {
    pub started: Instant,
    pub timings: Vec<(String, Duration)>,
}

impl Steps {
    pub fn new() -> Steps {
        Steps {
            started: Instant::now(),
            timings: Vec::new(),
        }
    }

    pub fn run<T>(
        &mut self,
        rt: &mut Runtime<'_>,
        what: &str,
        f: impl FnOnce(&mut Runtime<'_>) -> docker::Res<T>,
    ) -> docker::Res<T> {
        super::info(rt, &format!("{what}..."));
        let start = Instant::now();
        let result = f(rt);
        self.timings.push((what.to_string(), start.elapsed()));
        result
    }

    /// `Timings:` and one line per step.
    pub fn timings_text(&self) -> String {
        let mut text = String::from("Timings:\n");
        for (step, took) in &self.timings {
            text.push_str(&format!("  {:>8}  {step}\n", format_duration(*took)));
        }
        text
    }
}

/// `1.5s`, `2m 05s`.
pub(crate) fn format_duration(d: Duration) -> String {
    let s = d.as_secs();
    if s >= 60 {
        format!("{}m {:02}s", s / 60, s % 60)
    } else {
        format!("{:.1}s", d.as_secs_f64())
    }
}

/// `YYYYmmddTHHMMSSZ` (UTC): the run's ID and backup directory name.
pub(crate) fn run_id() -> String {
    chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()
}

/// A run ID no directory in `backups` uses yet (runs are serialized by the lock, but two may
/// start within the same second).
pub(crate) fn fresh_run_id(backups: &Path) -> String {
    loop {
        let id = run_id();
        if !backups.join(&id).exists() && !backups.join(format!("{id}.partial")).exists() {
            return id;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// `1.2 GB`.
pub(crate) fn human_bytes(bytes: i64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes.max(0) as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < units.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", units[unit])
    }
}

/// The stack as `mrsm stack` sees it: its Compose project, the resolved Compose config, and the
/// `stack` config key's raw value and file.
pub(crate) struct Stack {
    pub compose: docker::Compose,
    pub resolved: Json,
    /// The raw `stack` config (symlinks unresolved), from its file or the defaults.
    pub raw: ConfigValue,
    /// The `stack` key's config file, when there is one.
    pub file: Option<PathBuf>,
    /// The compose file `mrsm stack` writes and runs (`STACK_COMPOSE_PATH`).
    pub compose_path: PathBuf,
    /// The root's config directory.
    pub config_dir: PathBuf,
    /// `<root>/backups/postgres`.
    pub backups: PathBuf,
}

impl Stack {
    /// Write the stack files (as `mrsm stack` does before every Compose command) and resolve the
    /// project. `editing`: the command will edit the stack config (refused when something else
    /// patches it).
    pub fn load(rt: &mut Runtime<'_>, editing: bool) -> Result<Stack, String> {
        if editing {
            refuse_stack_patches(rt)?;
        }
        super::os_sql_stack::write_stack(rt)
            .map_err(|e| format!("Cannot write the stack files: {e}"))?;
        let project = rt
            .config()
            .get(&["stack", "project_name"])
            .map(|v| v.py_str())
            .filter(|p| !p.trim().is_empty())
            .unwrap_or_else(|| "mrsm".into());
        let paths = rt.config().paths();
        let compose_path = paths.path("STACK_COMPOSE_PATH");
        let cwd = compose_path
            .parent()
            .map(Path::to_path_buf)
            .ok_or("The stack's compose file has no directory.")?;
        let compose = docker::Compose {
            project,
            cwd,
            env: stack_env(rt),
        };
        let (raw, file) = read_raw_stack(rt)?;
        let resolved = compose.config().map_err(|e| e.message)?;
        Ok(Stack {
            compose,
            resolved,
            raw,
            file,
            compose_path,
            config_dir: paths.config_dir.clone(),
            backups: paths.root_dir.join("backups").join("postgres"),
        })
    }

    /// The stack config file the switch edits (or creates): the existing one, else
    /// `<config dir>/stack.yaml`.
    pub fn config_file(&self) -> PathBuf {
        self.file
            .clone()
            .unwrap_or_else(|| self.config_dir.join("stack.yaml"))
    }

    /// The resolved `db` service.
    pub fn db(&self) -> Option<&Json> {
        self.resolved.get("services")?.get(DB_SERVICE)
    }

    /// The resolved project name (Compose's normalized `--project-name`).
    pub fn project_label(&self) -> String {
        self.resolved
            .get("name")
            .and_then(Json::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| self.compose.project.clone())
    }

    /// The services that depend on `db` (`depends_on` or `links`).
    pub fn dependents(&self) -> Vec<String> {
        let Some(services) = self.resolved.get("services").and_then(Json::as_object) else {
            return Vec::new();
        };
        let mut out: Vec<String> = services
            .iter()
            .filter(|(name, _)| name.as_str() != DB_SERVICE)
            .filter(|(_, svc)| {
                let depends = svc.get("depends_on").is_some_and(|d| match d {
                    Json::Object(m) => m.contains_key(DB_SERVICE),
                    Json::Array(a) => a.iter().any(|x| x.as_str() == Some(DB_SERVICE)),
                    _ => false,
                });
                let links = svc.get("links").and_then(Json::as_array).is_some_and(|a| {
                    a.iter()
                        .filter_map(Json::as_str)
                        .any(|l| l == DB_SERVICE || l.starts_with(&format!("{DB_SERVICE}:")))
                });
                depends || links
            })
            .map(|(name, _)| name.clone())
            .collect();
        out.sort();
        out
    }
}

/// The stack's own variables (`POSTGRES_USER`, ...) for Compose, as `mrsm stack` sets them.
fn stack_env(rt: &mut Runtime<'_>) -> Vec<(String, String)> {
    let dict = super::os_sql_stack::stack_env_dict();
    rt.config()
        .search_and_substitute(&dict)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(k, v)| match v {
            meerschaum_core::value::Value::Str(s) => Some((k, s)),
            _ => None,
        })
        .collect()
}

/// The `stack` key must come from its file: a `MRSM_CONFIG` / `MRSM_PATCH` patch or a patch
/// directory would keep deciding the stack after `stack.yaml` is edited.
fn refuse_stack_patches(rt: &mut Runtime<'_>) -> Result<(), String> {
    let names = &meerschaum_core::static_config().environment;
    for var in [names.config.as_str(), names.patch.as_str()] {
        let Some(text) = (rt.env)(var) else {
            continue;
        };
        let patched = serde_json::from_str::<Json>(&text)
            .ok()
            .is_some_and(|v| v.get("stack").is_some());
        if patched {
            return Err(format!(
                "`{var}` patches the `stack` config, so editing `stack.yaml` would not move the \
                 stack. Put those settings in `stack.yaml` (`mrsm edit config stack`) and unset \
                 `{var}`, then run this again."
            ));
        }
    }
    let paths = rt.config().paths();
    for dir in ["PATCH_DIR_PATH", "PERMANENT_PATCH_DIR_PATH"] {
        let dir = paths.path(dir);
        let found = std::fs::read_dir(&dir).ok().is_some_and(|entries| {
            entries.flatten().any(|e| {
                e.file_name()
                    .to_string_lossy()
                    .split('.')
                    .next()
                    .is_some_and(|stem| stem == "stack")
            })
        });
        if found {
            return Err(format!(
                "A patch directory ({}) patches the `stack` config, so editing `stack.yaml` would \
                 not move the stack. Remove the patch, then run this again.",
                dir.display()
            ));
        }
    }
    Ok(())
}

/// The `stack` key as its file holds it (no `MRSM{}` substitution), or the raw defaults when
/// there is no file; and the file.
fn read_raw_stack(rt: &mut Runtime<'_>) -> Result<(ConfigValue, Option<PathBuf>), String> {
    rt.config().with_state(|state| {
        let file = state
            .get_keyfile_path("stack", false, None)
            .filter(|p| p.exists());
        let (config, _) = state
            .read_config_lossless(None, Some(&["stack".to_string()]), false, false, true)
            .map_err(|e| format!("Cannot read the stack config: {e:?}"))?;
        let raw = config
            .get("stack")
            .cloned()
            .ok_or("The config has no `stack` key.")?;
        Ok((raw, file))
    })
}

/// Write the `stack` key through the config layer and make the next `get_config('stack', ...)`
/// read it back from the file. Returns the file written.
pub(crate) fn write_stack_config(
    rt: &mut Runtime<'_>,
    stack: &ConfigValue,
) -> Result<PathBuf, String> {
    let written = rt.config().with_state(|state| {
        let mut config = ConfigMap::new();
        config.insert("stack".into(), stack.clone());
        if !state.write_config_lossless(Some(&config), None) {
            let reason = state
                .take_write_error()
                .map(|(_, e)| e)
                .unwrap_or_else(|| "writes are disabled".into());
            return Err(format!("Cannot write the stack config: {reason}"));
        }
        state
            .get_keyfile_path("stack", false, None)
            .ok_or_else(|| "The stack config file was not written.".to_string())
    })?;
    docker::sync_file(&written).map_err(|e| e.message)?;
    forget_stack_config(rt);
    Ok(written)
}

/// Drop the in-memory `stack` key so the next read loads the file.
pub(crate) fn forget_stack_config(rt: &mut Runtime<'_>) {
    let links_key = meerschaum_core::static_config().config.symlinks_key.clone();
    rt.config().with_state(|state| {
        let mut config = state.take_config_lossless();
        config.shift_remove("stack");
        if let Some(ConfigValue::Map(links)) = config.get_mut(&links_key) {
            links.shift_remove("stack");
        }
        state.set_config_lossless(config);
    });
}

/// Put back a config file's original bytes (or remove it when it did not exist), and remove a
/// file written under another name.
pub(crate) fn restore_config_file(
    original_path: &Path,
    original: Option<&[u8]>,
    written: Option<&Path>,
) -> Result<(), String> {
    match original {
        Some(bytes) => std::fs::write(original_path, bytes)
            .map_err(|e| format!("Cannot restore '{}': {e}", original_path.display()))?,
        None => {
            if original_path.exists() {
                std::fs::remove_file(original_path)
                    .map_err(|e| format!("Cannot remove '{}': {e}", original_path.display()))?;
            }
        }
    }
    if let Some(written) = written
        && written != original_path
        && written.exists()
    {
        std::fs::remove_file(written)
            .map_err(|e| format!("Cannot remove '{}': {e}", written.display()))?;
    }
    if original.is_some() {
        docker::sync_file(original_path).map_err(|e| e.message)?;
    }
    if let Some(parent) = original_path.parent() {
        docker::sync_dir(parent).map_err(|e| e.message)?;
    }
    if let Some(written) = written
        && let Some(parent) = written.parent()
    {
        docker::sync_dir(parent).map_err(|e| e.message)?;
    }
    Ok(())
}

// --- The manifest --------------------------------------------------------------------------

/// `<backup dir>/manifest.json`: what a run did, for the user and for `rollback`.
pub(crate) fn write_manifest(dir: &Path, manifest: &Json) -> Result<(), String> {
    let text = serde_json::to_string_pretty(manifest).unwrap_or_default();
    let tmp = dir.join("manifest.json.tmp");
    docker::write_private(&tmp, format!("{text}\n").as_bytes())
        .map_err(|e| e.message)
        .and_then(|_| {
            std::fs::rename(&tmp, dir.join("manifest.json"))
                .map_err(|e| format!("Cannot write the manifest in '{}': {e}", dir.display()))?;
            docker::sync_dir(dir).map_err(|e| e.message)
        })
}

pub(crate) fn read_manifest(dir: &Path) -> Option<Json> {
    let text = std::fs::read_to_string(dir.join("manifest.json")).ok()?;
    serde_json::from_str(&text).ok()
}

/// The run directories with a manifest, newest first.
pub(crate) fn runs(backups: &Path) -> Vec<(PathBuf, Json)> {
    let Ok(entries) = std::fs::read_dir(backups) else {
        return Vec::new();
    };
    let mut dirs: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();
    dirs.into_iter()
        .rev()
        .filter_map(|dir| {
            let manifest = read_manifest(&dir)?;
            Some((dir, manifest))
        })
        .collect()
}

/// The newest run directory whose manifest is in `state`.
pub(crate) fn latest_run(backups: &Path, state: &str) -> Option<(PathBuf, Json)> {
    runs(backups)
        .into_iter()
        .find(|(_, manifest)| manifest.get("state").and_then(Json::as_str) == Some(state))
}

/// The states a run ends in (any other state is a run that was interrupted).
pub(crate) const FINAL_STATES: &[&str] = &["switched", "failed", "rolled_back", "complete"];

/// Whether a manifest is of an upgrade or a restore that never ended.
pub(crate) fn interrupted(manifest: &Json) -> bool {
    let kind = manifest.get("kind").and_then(Json::as_str).unwrap_or("");
    let state = manifest.get("state").and_then(Json::as_str).unwrap_or("");
    matches!(kind, "upgrade" | "restore") && !FINAL_STATES.contains(&state)
}

/// Set a manifest's state (and error) and write it.
pub(crate) fn set_state(dir: &Path, manifest: &mut Json, state: &str, error: Option<&str>) {
    manifest["state"] = json!(state);
    manifest["updated"] = json!(chrono::Utc::now().to_rfc3339());
    if let Some(error) = error {
        manifest["error"] = json!(error);
    }
    let _ = write_manifest(dir, manifest);
}

/// An exclusive lock of a file (`Err` when another process holds it).
fn lock_file(path: &Path, held: &str) -> Result<std::fs::File, String> {
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(false).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(path)
        .map_err(|e| format!("Cannot open '{}': {e}", path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(format!(
            "Another `upgrade postgres`, `backup postgres` or `restore postgres` is running \
             {held}."
        )),
        Err(std::fs::TryLockError::Error(e)) => {
            Err(format!("Cannot lock '{}': {e}", path.display()))
        }
    }
}

/// An exclusive lock on `<root>/backups/postgres/.lock` for the run (it keeps the backups of the
/// root apart: retention never removes what a restore reads).
pub(crate) fn lock(backups: &Path) -> Result<std::fs::File, String> {
    docker::private_dir(backups).map_err(|e| e.message)?;
    lock_file(
        &backups.join(".lock"),
        "for this root (it holds the lock in backups/postgres/.lock)",
    )
}

/// Where the per-project locks live: per user and outside any root (two roots may name the same
/// Compose project, e.g. the default `mrsm`).
fn lock_dir() -> Result<PathBuf, String> {
    #[cfg(unix)]
    let dir = match std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|d| d.is_absolute() && d.is_dir())
    {
        Some(runtime) => runtime.join("meerschaum"),
        None => {
            std::env::temp_dir().join(format!("meerschaum-{}", rustix::process::getuid().as_raw()))
        }
    };
    #[cfg(not(unix))]
    let dir = std::env::temp_dir().join("meerschaum");
    docker::private_dir(&dir).map_err(|e| e.message)?;
    #[cfg(unix)]
    {
        // A shared temporary directory: the directory must be ours (not a link, not another
        // user's).
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::symlink_metadata(&dir)
            .map_err(|e| format!("Cannot use '{}': {e}", dir.display()))?;
        if !meta.is_dir() || meta.uid() != rustix::process::getuid().as_raw() {
            return Err(format!(
                "Cannot use '{}' for the locks: it is not a directory of this user.",
                dir.display()
            ));
        }
    }
    Ok(dir)
}

/// The lock file of a Compose project on a Docker daemon.
fn project_lock_path(dir: &Path, project: &str, daemon: &str) -> PathBuf {
    let safe: String = project
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let daemon = hex::encode(sha2::Sha256::digest(daemon.as_bytes()));
    dir.join(format!("postgres-{safe}-{}.lock", &daemon[..16]))
}

/// The locks a command holds from its start to its end: the root's, and the Compose project's on
/// this Docker daemon (another root naming the same project waits for it too).
pub(crate) struct Locks {
    _root: std::fs::File,
    _project: std::fs::File,
}

impl Locks {
    pub fn take(stack: &Stack) -> Result<Locks, String> {
        let root = lock(&stack.backups)?;
        let daemon = docker::daemon_id().map_err(|e| e.message)?;
        let project = stack.project_label();
        let project_lock = lock_file(
            &project_lock_path(&lock_dir()?, &project, &daemon),
            &format!("for the stack project '{project}' on this Docker daemon"),
        )?;
        Ok(Locks {
            _root: root,
            _project: project_lock,
        })
    }
}

/// Whether two paths name the same file or directory (resolved when they exist).
pub(crate) fn same_path(a: &Path, b: &Path) -> bool {
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => {
            let trim = |p: &Path| {
                p.to_string_lossy()
                    .trim_end_matches(['/', '\\'])
                    .to_string()
            };
            trim(a) == trim(b)
        }
    }
}

/// Whether a container of the project was created from `compose_path` (its Compose labels).
pub(super) fn created_from(
    labels: &std::collections::BTreeMap<String, String>,
    compose_path: &Path,
) -> bool {
    let files = labels
        .get("com.docker.compose.project.config_files")
        .filter(|f| !f.trim().is_empty());
    let dir = labels
        .get("com.docker.compose.project.working_dir")
        .filter(|d| !d.trim().is_empty());
    match (files, dir) {
        (Some(files), _) => files
            .split(',')
            .any(|f| same_path(Path::new(f.trim()), compose_path)),
        (None, Some(dir)) => compose_path
            .parent()
            .is_some_and(|parent| same_path(Path::new(dir), parent)),
        _ => false,
    }
}

/// The stack's containers must come from this root's compose file: another root whose stack has
/// the same project name (the default `mrsm`) is never moved, backed up or restored from here.
pub(crate) fn check_owner(stack: &Stack) -> Result<(), String> {
    let project = stack.project_label();
    let containers =
        docker::containers_with(&format!("label=com.docker.compose.project={project}"))
            .map_err(|e| e.message)?;
    let foreign: Vec<String> = containers
        .iter()
        .filter(|c| !created_from(&c.labels, &stack.compose_path))
        .map(|c| {
            let origin = c
                .labels
                .get("com.docker.compose.project.config_files")
                .or_else(|| c.labels.get("com.docker.compose.project.working_dir"))
                .filter(|o| !o.is_empty())
                .cloned()
                .unwrap_or_else(|| "an unknown compose file".into());
            format!("'{}' (from {origin})", c.name)
        })
        .collect();
    if foreign.is_empty() {
        return Ok(());
    }
    Err(format!(
        "The stack project '{project}' runs containers that were not created from this root's \
         stack ({}): {}.\nAnother Meerschaum root uses the same project name \
         (`stack:project_name`, `mrsm` by default), and these commands act only on the stack this \
         root runs. Run the command from that root, or give this root's stack its own project \
         name (`mrsm edit config stack`).",
        stack.compose_path.display(),
        foreign.join(", ")
    ))
}

/// What every command does first: Docker must answer, the stack is read, the locks are taken, and
/// the stack's containers must be this root's.
pub(crate) fn prologue(rt: &mut Runtime<'_>, editing: bool) -> Result<(Stack, Locks), String> {
    if !docker::docker_available() {
        return Err(NO_DOCKER.into());
    }
    let stack = Stack::load(rt, editing)?;
    let locks = Locks::take(&stack)?;
    check_owner(&stack)?;
    Ok((stack, locks))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_are_human() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(2048), "2.0 KB");
        assert_eq!(human_bytes(5 * 1024 * 1024 * 1024), "5.0 GB");
    }

    #[test]
    fn run_ids_sort_by_time() {
        let id = run_id();
        assert_eq!(id.len(), 16);
        assert!(id.ends_with('Z') && id.contains('T'));
    }

    #[test]
    fn the_latest_run_in_a_state_is_found() {
        let dir = tempfile::tempdir().unwrap();
        for (name, state) in [
            ("20261001T000000Z", "switched"),
            ("20261002T000000Z", "failed"),
            ("20261003T000000Z", "switched"),
            ("20261004T000000Z", "rolled_back"),
        ] {
            let run = dir.path().join(name);
            std::fs::create_dir_all(&run).unwrap();
            write_manifest(&run, &json!({"state": state, "run_id": name})).unwrap();
        }
        let (run, manifest) = latest_run(dir.path(), "switched").unwrap();
        assert!(run.ends_with("20261003T000000Z"));
        assert_eq!(manifest["run_id"], "20261003T000000Z");
        assert!(latest_run(dir.path(), "started").is_none());
    }

    #[test]
    fn config_files_are_restored_byte_for_byte() {
        let dir = tempfile::tempdir().unwrap();
        let original = dir.path().join("stack.yaml");
        std::fs::write(&original, "edited: true\n").unwrap();
        restore_config_file(&original, Some(b"# mine\nimage: x\n"), Some(&original)).unwrap();
        assert_eq!(std::fs::read(&original).unwrap(), b"# mine\nimage: x\n");
        // A file that did not exist before is removed again, and so is one written elsewhere.
        let other = dir.path().join("stack.json");
        std::fs::write(&other, "{}").unwrap();
        restore_config_file(&original, None, Some(&other)).unwrap();
        assert!(!original.exists() && !other.exists());
    }

    #[test]
    fn a_second_run_cannot_take_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let first = lock(dir.path()).unwrap();
        assert!(lock(dir.path()).unwrap_err().contains("Another"));
        drop(first);
        assert!(lock(dir.path()).is_ok());
    }
}
