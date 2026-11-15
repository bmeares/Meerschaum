//! A job gets its configuration patches in files (5.1).
//!
//! A job started from a process that holds `MRSM_CONFIG` or `MRSM_PATCH` (a Compose project's
//! `config:` block, or a user's own patch) used to receive the text in its environment, and a
//! `systemd` job in its unit file, so every connector's credentials were in `/proc/<pid>/environ`
//! and in the unit. Since 5.1 the text goes to an owner-only file in the job's directory
//! ([`CONFIG_FILENAME`], [`PATCH_FILENAME`]: `jobs/<name>/` for a local job, the unit's
//! directory `systemd/services/<name>/` for a `systemd` job), and the job gets
//! `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE` instead, which Meerschaum reads the same way.
//!
//! - The files are written each time a local job is launched and each time a `systemd` unit is
//!   written, from the launching process's environment: `compose up` (which recreates its jobs)
//!   updates them, and a variable the launching process no longer has removes its file. They go
//!   with the job's directory when the job is deleted.
//! - A job whose launching process holds a job's file (one job started by another, or by an API
//!   server running as a job) gets its own copy, so deleting the other job takes nothing away.
//! - A user's own `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE` is passed on as it is.
//! - `MRSM__COMPOSE_CONFIG` (the Compose project's whole file, which jobs read only for the
//!   project's name) loses its `config:` block and the `connectors` resolved from it, which the
//!   file holds.
//! - Jobs started before 5.1 keep the environment they were started with until they are next
//!   launched or their unit is written again.

use std::path::{Path, PathBuf};

/// A job's copy of `MRSM_CONFIG`.
pub const CONFIG_FILENAME: &str = ".mrsm_config.json";
/// A job's copy of `MRSM_PATCH`.
pub const PATCH_FILENAME: &str = ".mrsm_patch.json";

/// The Compose project's configuration variable (see the module documentation).
pub const COMPOSE_CONFIG_VAR: &str = "MRSM__COMPOSE_CONFIG";

/// The keys of [`COMPOSE_CONFIG_VAR`] a job does not get: the project's `config:` block and the
/// `connectors` Compose resolves from it (both are in [`CONFIG_FILENAME`]).
pub const COMPOSE_SECRET_KEYS: &[&str] = &["config", "connectors"];

/// How a job's environment differs from the launching process's.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JobEnvironment {
    /// Variables to set.
    pub set: Vec<(String, String)>,
    /// Variables to remove.
    pub remove: Vec<String>,
}

impl JobEnvironment {
    /// `env` with the changes applied (removed first, then set).
    pub fn apply(&self, env: &[(String, String)]) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = env
            .iter()
            .filter(|(k, _)| !self.remove.contains(k) && !self.set.iter().any(|(s, _)| s == k))
            .cloned()
            .collect();
        out.extend(self.set.iter().cloned());
        out
    }

    /// Apply the changes to a command whose environment starts from this process's.
    pub fn apply_to(&self, command: &mut std::process::Command) {
        for name in &self.remove {
            command.env_remove(name);
        }
        for (name, value) in &self.set {
            command.env(name, value);
        }
    }
}

/// The last value of `name` in `env`.
fn lookup<'a>(env: &'a [(String, String)], name: &str) -> Option<&'a str> {
    env.iter()
        .rev()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

/// Write `contents` to `path`, readable by its owner only, atomically.
fn write_private(path: &Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write;
    let dir = path.parent().unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let temporary = dir.join(format!("{name}.{}.tmp", std::process::id()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let written = (|| {
        let mut file = options.open(&temporary)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)
    })();
    if written.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    written
}

/// Whether `path` is a job's copy of a patch (another job's, inherited).
fn is_job_file(path: &str) -> bool {
    Path::new(path)
        .file_name()
        .is_some_and(|n| n == CONFIG_FILENAME || n == PATCH_FILENAME)
}

/// Write the patches the launching environment `env` holds to `dir` (the job's directory,
/// which must exist) and say how the job's environment changes (see the module
/// documentation). `Err` when a file cannot be written: the caller then leaves the environment
/// as it is, so the job never starts without its configuration.
pub fn externalize(dir: &Path, env: &[(String, String)]) -> std::io::Result<JobEnvironment> {
    let mut out = JobEnvironment::default();
    for (var, file_var) in meerschaum_core::config::environment::patch_variables() {
        let filename = if var.ends_with("_PATCH") {
            PATCH_FILENAME
        } else {
            CONFIG_FILENAME
        };
        let path: PathBuf = dir.join(filename);
        let value = lookup(env, &var).filter(|v| !v.is_empty());
        let inherited = lookup(env, &file_var).filter(|v| !v.is_empty());
        let contents = match (value, inherited) {
            (Some(value), _) => Some(value.to_string()),
            // Another job's copy: this job keeps its own.
            (None, Some(other)) if is_job_file(other) && Path::new(other) != path => {
                std::fs::read_to_string(other).ok()
            }
            _ => None,
        };
        match contents {
            Some(contents) => {
                write_private(&path, &contents)?;
                out.remove.push(var);
                out.set
                    .push((file_var, path.to_string_lossy().into_owned()));
            }
            None => {
                // The launching process no longer has it (or passes a file of its own).
                if inherited.is_none_or(|other| Path::new(other) != path) {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }
    if let Some(text) = lookup(env, COMPOSE_CONFIG_VAR)
        && let Ok(serde_json::Value::Object(config)) =
            serde_json::from_str::<serde_json::Value>(text)
        && COMPOSE_SECRET_KEYS.iter().any(|k| config.contains_key(*k))
    {
        let kept: serde_json::Map<String, serde_json::Value> = config
            .into_iter()
            .filter(|(k, _)| !COMPOSE_SECRET_KEYS.contains(&k.as_str()))
            .collect();
        out.set.push((
            COMPOSE_CONFIG_VAR.to_string(),
            serde_json::Value::Object(kept).to_string(),
        ));
    }
    Ok(out)
}

/// The `env` property of a job (the environment an API server records for the jobs it creates:
/// its whole environment) with `MRSM_CONFIG` / `MRSM_PATCH` moved to the job's files in `dir`
/// (the same files a launch writes, [`externalize`]) and replaced by `MRSM_CONFIG_FILE` /
/// `MRSM_PATCH_FILE` naming them, so `properties.json` never holds their text (5.1). The job
/// starts the same way: a launch reads the file form and keeps the job's own file. `Ok(false)`:
/// nothing to move. `Err`: a file could not be written; `env` is then unchanged.
pub fn externalize_properties_env(
    dir: &Path,
    env: &mut meerschaum_core::value::Map,
) -> std::io::Result<bool> {
    use meerschaum_core::value::Value;
    let mut moved = Vec::new();
    for (var, file_var) in meerschaum_core::config::environment::patch_variables() {
        let Some(Value::Str(text)) = env.get(&var) else {
            continue;
        };
        if text.is_empty() {
            continue;
        }
        let filename = if var.ends_with("_PATCH") {
            PATCH_FILENAME
        } else {
            CONFIG_FILENAME
        };
        let path = dir.join(filename);
        write_private(&path, text)?;
        moved.push((var, file_var, path.to_string_lossy().into_owned()));
    }
    let changed = !moved.is_empty();
    for (var, file_var, path) in moved {
        env.shift_remove(&var);
        env.insert(file_var, Value::Str(path));
    }
    Ok(changed)
}

/// This process's environment (variables whose name or value is not Unicode are left out).
pub fn process_env() -> Vec<(String, String)> {
    meerschaum_core::process_env::vars()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn the_patches_move_to_owner_only_files() {
        let dir = tempfile::tempdir().unwrap();
        let launching = env(&[
            (
                "MRSM_CONFIG",
                r#"{"meerschaum": {"connectors": {"sql": {"x": {"password": "s3"}}}}}"#,
            ),
            ("MRSM_PATCH", "{}"),
            (
                "MRSM__COMPOSE_CONFIG",
                r#"{"project_name": "p", "config": {"meerschaum": {"password": "s3"}},
                    "connectors": {"sql": {"x": {"password": "s3"}}}}"#,
            ),
            ("OTHER", "kept"),
        ]);
        let changes = externalize(dir.path(), &launching).unwrap();
        let config = dir.path().join(CONFIG_FILENAME);
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            launching[0].1.as_str()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&config).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        let job = changes.apply(&launching);
        let text = format!("{job:?}");
        assert!(!text.contains("s3"), "{text}");
        assert_eq!(
            lookup(&job, "MRSM_CONFIG_FILE"),
            Some(config.to_string_lossy().as_ref())
        );
        assert!(lookup(&job, "MRSM_PATCH_FILE").is_some());
        assert_eq!(lookup(&job, "MRSM_CONFIG"), None);
        assert_eq!(lookup(&job, "OTHER"), Some("kept"));
        assert_eq!(
            lookup(&job, "MRSM__COMPOSE_CONFIG"),
            Some(r#"{"project_name":"p"}"#)
        );

        // Launched again without the patch: its file goes.
        let changes = externalize(dir.path(), &env(&[("MRSM_CONFIG", "{}")])).unwrap();
        assert!(!dir.path().join(PATCH_FILENAME).exists());
        assert_eq!(changes.remove, vec!["MRSM_CONFIG".to_string()]);
    }

    #[test]
    fn a_users_file_is_passed_on_and_another_jobs_is_copied() {
        let dir = tempfile::tempdir().unwrap();
        let mine = dir.path().join("mine.json");
        std::fs::write(&mine, "{}").unwrap();
        let launching = env(&[("MRSM_CONFIG_FILE", mine.to_str().unwrap())]);
        let changes = externalize(&dir.path().join("job"), &launching).unwrap();
        assert_eq!(changes, JobEnvironment::default());
        assert_eq!(changes.apply(&launching), launching);

        let other_job = dir.path().join("other");
        std::fs::create_dir_all(&other_job).unwrap();
        std::fs::write(other_job.join(CONFIG_FILENAME), "{\"a\": 1}").unwrap();
        let job = dir.path().join("job");
        std::fs::create_dir_all(&job).unwrap();
        let launching = env(&[(
            "MRSM_CONFIG_FILE",
            other_job.join(CONFIG_FILENAME).to_str().unwrap(),
        )]);
        let changes = externalize(&job, &launching).unwrap();
        assert_eq!(
            std::fs::read_to_string(job.join(CONFIG_FILENAME)).unwrap(),
            "{\"a\": 1}"
        );
        assert_eq!(
            lookup(&changes.apply(&launching), "MRSM_CONFIG_FILE"),
            Some(job.join(CONFIG_FILENAME).to_string_lossy().as_ref())
        );
        // Its own file (a restart from inside the job) is kept.
        let launching = env(&[(
            "MRSM_CONFIG_FILE",
            job.join(CONFIG_FILENAME).to_str().unwrap(),
        )]);
        externalize(&job, &launching).unwrap();
        assert!(job.join(CONFIG_FILENAME).exists());
    }

    #[test]
    fn nothing_to_move_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let launching = env(&[
            ("PATH", "/usr/bin"),
            ("MRSM__COMPOSE_CONFIG", r#"{"project_name": "p"}"#),
        ]);
        assert_eq!(
            externalize(dir.path(), &launching).unwrap(),
            JobEnvironment::default()
        );
        // A directory that cannot be written is an error (the caller keeps the environment).
        let missing = dir.path().join("missing");
        assert!(externalize(&missing, &env(&[("MRSM_CONFIG", "{}")])).is_err());
    }
}
