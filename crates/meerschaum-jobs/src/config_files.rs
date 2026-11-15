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
    /// Start from an empty environment (5.1: an isolated team's job, whose environment is
    /// [`JobEnvironment::set`] only).
    pub clear: bool,
}

impl JobEnvironment {
    /// `env` with the changes applied (cleared or removed first, then set).
    pub fn apply(&self, env: &[(String, String)]) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = if self.clear {
            Vec::new()
        } else {
            env.iter()
                .filter(|(k, _)| !self.remove.contains(k) && !self.set.iter().any(|(s, _)| s == k))
                .cloned()
                .collect()
        };
        out.extend(self.set.iter().cloned());
        out
    }

    /// Apply the changes to a command whose environment starts from this process's.
    pub fn apply_to(&self, command: &mut std::process::Command) {
        if self.clear {
            command.env_clear();
        }
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

/// The job property that ties a job to its isolated team's scope for good (5.1): the scope's key
/// (`meerschaum_core::team_scope::TeamScope::key`), written at the job's first team-scoped launch.
/// A rename or a deletion of the team (which moves its jobs to Everyone) never gives the job the
/// server's configuration: a deleted team's jobs get no connector at all.
pub const TEAM_SCOPE_PROPERTY: &str = "team_scope";

/// The isolated team's scope a job is launched with (5.1), from its properties and the scopes the
/// API server recorded under `root` (`team_scopes/`): the scope its `team_scope` property names
/// (a missing record counts as a deleted team), else the scope of its `team`. `None` for a job of
/// Everyone, of an open team, or of a team that is no longer isolated.
pub fn job_team_scope(
    root: &Path,
    props: &meerschaum_core::value::Map,
) -> Option<(String, meerschaum_core::team_scope::TeamScope)> {
    use meerschaum_core::team_scope::{TeamScope, read_scope_record, scope_key_of};
    use meerschaum_core::value::Value;
    let text = |key: &str| {
        props
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    let (key, record) = match text(TEAM_SCOPE_PROPERTY) {
        Some(key) => {
            let record = read_scope_record(root, &key)
                .unwrap_or_else(|| TeamScope::deleted(&key, Some(&key)));
            (key, record)
        }
        None => {
            let team = text("team").filter(|t| !meerschaum_core::access::is_everyone(Some(t)))?;
            let key = scope_key_of(root, &team)?;
            let record = read_scope_record(root, &key)?;
            (key, record)
        }
    };
    (!record.open).then_some((key, record))
}

/// Whether a job's properties might name an isolated team (a cheap check before
/// [`job_team_scope`]).
pub fn may_be_team_job(props: &meerschaum_core::value::Map) -> bool {
    use meerschaum_core::value::Value;
    props.get(TEAM_SCOPE_PROPERTY).is_some_and(|v| !v.is_null())
        || props
            .get("team")
            .and_then(Value::as_str)
            .is_some_and(|t| !meerschaum_core::access::is_everyone(Some(t)))
}

/// Write an isolated team's job configuration to its directory `dir` and say what the job's
/// environment is (5.1): the team's configuration ([`meerschaum_core::team_scope::team_config`]
/// of `config`, the launching process's) in [`CONFIG_FILENAME`] (owner-only) named by
/// `MRSM_CONFIG_FILE`, an empty config directory of the job's own named by `MRSM_CONFIG_DIR`, and
/// an environment built from an allowlist ([`meerschaum_core::team_scope::team_env`]) of
/// `launching` (the launching process's environment) and `job` (the job's own variables): none
/// of the launching process's patches, connector variables or other variables outside it. `Err`
/// when a file cannot be written: the caller must not launch the job then.
pub fn externalize_team(
    dir: &Path,
    launching: &[(String, String)],
    job: &[(String, String)],
    config: &meerschaum_core::Config,
    scope: &meerschaum_core::team_scope::TeamScope,
) -> std::io::Result<JobEnvironment> {
    use meerschaum_core::team_scope::{
        JOB_CONFIG_DIR, Passthrough, ScopeFiles, create_private_dir, fingerprint, team_config,
        team_env,
    };
    let team = team_config(config, scope);
    let fingerprint = fingerprint(scope, &team);
    let files = ScopeFiles {
        dir: dir.to_path_buf(),
        config_dir: dir.join(JOB_CONFIG_DIR),
        config_file: dir.join(CONFIG_FILENAME),
    };
    create_private_dir(&files.config_dir)?;
    write_private(
        &files.config_file,
        &meerschaum_core::value::Value::Map(team).to_json_string(),
    )?;
    let _ = std::fs::remove_file(dir.join(PATCH_FILENAME));
    let passthrough = Passthrough::from_config(config);
    passthrough.warn_refused();
    let mut set = team_env(launching, job, &files, scope, &fingerprint, &passthrough);
    // 5.1: the job's `mrsm` restricts itself to the team's filesystem sandbox before it runs the
    // job's command (`system.engine.python_team_sandbox`); in `require` mode on a system without
    // Landlock, the job does not start.
    use meerschaum_core::team_scope::sandbox;
    if let Some(prepared) = sandbox::prepare(
        config,
        scope,
        &files,
        Some(dir),
        &dir.join(sandbox::JOB_SPEC_FILE),
    )? {
        sandbox::apply_env(&mut set, &prepared);
    }
    Ok(JobEnvironment {
        set,
        remove: Vec::new(),
        clear: true,
    })
}

/// A team job's `env` property reduced to what its process may receive (5.1,
/// [`meerschaum_core::team_scope::may_record`]): an API server records its whole environment for
/// the jobs it creates, and a launch (also the restart monitor's after a reboot) applies it.
/// `passthrough` is the administrator's list ([`meerschaum_core::team_scope::PASSTHROUGH_KEY`]).
/// `true` when something went.
pub fn strip_team_env(
    env: &mut meerschaum_core::value::Map,
    passthrough: &meerschaum_core::team_scope::Passthrough,
) -> bool {
    let before = env.len();
    env.retain(|k, _| meerschaum_core::team_scope::may_record(k, passthrough));
    env.len() != before
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

    fn props(json: &str) -> meerschaum_core::value::Map {
        meerschaum_core::value::Value::from_json_str(json)
            .unwrap()
            .to_map()
    }

    #[test]
    fn a_team_job_follows_its_scope_for_good() {
        use meerschaum_core::team_scope::{TeamScope, write_scope_record};
        let root = tempfile::tempdir().unwrap();
        let scope = TeamScope {
            team: "Wetlands".into(),
            team_id: Some("id-w".into()),
            connectors: ["sql:wetlands".to_string()].into_iter().collect(),
            ..TeamScope::default()
        };
        write_scope_record(root.path(), &scope).unwrap();
        // By its team's name (any case), and by its recorded scope.
        let (key, found) = job_team_scope(root.path(), &props(r#"{"team": "wetlands"}"#)).unwrap();
        assert_eq!((key.as_str(), &found), ("id-w", &scope));
        assert!(job_team_scope(root.path(), &props(r#"{"team_scope": "id-w"}"#)).is_some());
        // Everyone's jobs and open teams' jobs are not team jobs.
        assert!(job_team_scope(root.path(), &props(r#"{"team": null}"#)).is_none());
        assert!(job_team_scope(root.path(), &props(r#"{"team": "Open"}"#)).is_none());
        assert!(!may_be_team_job(&props(r#"{"team": "Everyone"}"#)));
        // A deletion moves the job to Everyone (`team: null`): its recorded scope still holds,
        // with no connector at all; a missing record counts as deleted.
        write_scope_record(root.path(), &TeamScope::deleted("Wetlands", Some("id-w"))).unwrap();
        let (_, deleted) = job_team_scope(
            root.path(),
            &props(r#"{"team": null, "team_scope": "id-w"}"#),
        )
        .unwrap();
        assert!(deleted.deleted && !deleted.allows("sql:wetlands"));
        let (_, missing) =
            job_team_scope(root.path(), &props(r#"{"team_scope": "nowhere"}"#)).unwrap();
        assert!(missing.deleted);
        // A team that is no longer isolated: its jobs get the server's configuration.
        let mut team = meerschaum_core::access::Team::new("Wetlands");
        team.id = Some("id-w".into());
        write_scope_record(root.path(), &TeamScope::opened(&team)).unwrap();
        assert!(job_team_scope(root.path(), &props(r#"{"team_scope": "id-w"}"#)).is_none());
    }

    #[test]
    fn a_team_jobs_files_and_environment_hold_only_its_connectors() {
        use meerschaum_core::team_scope::{JOB_CONFIG_DIR, TEAM_SCOPE_VAR, TeamScope};
        let root = tempfile::tempdir().unwrap();
        let config_dir = root.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(
            config_dir.join("meerschaum.yaml"),
            "connectors:\n  sql:\n    wetlands:\n      flavor: sqlite\n      database: /tmp/w.db\n      password: w-pw\n      teams: [wetlands]\n    drylands:\n      flavor: sqlite\n      database: /tmp/d.db\n      password: d-pw\n      teams: [drylands]\n",
        )
        .unwrap();
        // An administrator passes `TPR_PASSED` on; `MRSM_SQL_MAIN` is refused even if listed.
        std::fs::write(
            config_dir.join("system.yaml"),
            "engine:\n  python_team_env_passthrough: [TPR_PASSED, MRSM_SQL_MAIN]\n",
        )
        .unwrap();
        let launching = env(&[
            ("MRSM_ROOT_DIR", root.path().to_str().unwrap()),
            ("MRSM_SQL_MAIN", "postgresql://u:server-pw@db/m"),
            (
                "MRSM_CONFIG",
                r#"{"meerschaum": {"connectors": {"sql": {"x": {"password": "patch-pw"}}}}}"#,
            ),
            ("PATH", "/usr/bin"),
            ("TPR_CLOUD_KEY", "cloud-secret"),
            ("TPR_PASSED", "passed"),
            ("MRSM_DAEMON_ID", "the-servers-own-job"),
        ]);
        let config = meerschaum_core::Config::new(
            meerschaum_core::config::ConfigOptions::with_env(launching.clone()),
        )
        .unwrap();
        let scope = TeamScope {
            team: "wetlands".into(),
            connectors: ["sql:wetlands".to_string()].into_iter().collect(),
            instance: Some("sql:wetlands".into()),
            ..TeamScope::default()
        };
        let dir = root.path().join("jobs/team-job");
        std::fs::create_dir_all(&dir).unwrap();
        let own = env(&[
            ("MRSM_DAEMON_ID", "team-job"),
            ("TPR_CLOUD_KEY", "recorded"),
        ]);
        let changes = externalize_team(&dir, &launching, &own, &config, &scope).unwrap();
        assert!(changes.clear);
        let file = std::fs::read_to_string(dir.join(CONFIG_FILENAME)).unwrap();
        assert!(file.contains("w-pw"));
        for secret in ["d-pw", "server-pw", "patch-pw"] {
            assert!(!file.contains(secret), "{secret}: {file}");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.join(CONFIG_FILENAME))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let job = changes.apply(&launching);
        assert_eq!(lookup(&job, "MRSM_SQL_MAIN"), None);
        assert_eq!(lookup(&job, "MRSM_CONFIG"), None);
        assert_eq!(lookup(&job, TEAM_SCOPE_VAR), Some("wetlands"));
        assert_eq!(
            lookup(&job, "MRSM_CONFIG_DIR"),
            Some(dir.join(JOB_CONFIG_DIR).to_str().unwrap())
        );
        assert_eq!(lookup(&job, "PATH"), Some("/usr/bin"));
        // An allowlist: the sentinel stays out (from the launching process and from the job's
        // recorded variables), the passed-through name is in, the job's own name is its own.
        assert_eq!(lookup(&job, "TPR_CLOUD_KEY"), None);
        assert_eq!(lookup(&job, "TPR_PASSED"), Some("passed"));
        assert_eq!(lookup(&job, "MRSM_DAEMON_ID"), Some("team-job"));
        let mut command = std::process::Command::new("env");
        changes.apply_to(&mut command);
        assert!(command.get_envs().all(|(k, _)| k != "TPR_CLOUD_KEY"));
        // The job's own process (and a relaunch from inside it) sees the team's connector only.
        let inside = meerschaum_core::Config::new(
            meerschaum_core::config::ConfigOptions::with_env(job.clone()),
        )
        .unwrap();
        assert!(
            inside
                .get(&["meerschaum", "connectors", "sql", "wetlands"])
                .is_some()
        );
        for label in ["drylands", "main", "x"] {
            assert!(
                inside
                    .get(&["meerschaum", "connectors", "sql", label])
                    .is_none_or(|v| v.is_null()),
                "{label}"
            );
        }
        // Relaunched from inside (its own variables again): the same environment.
        let again = externalize_team(&dir, &job, &own[..1], &inside, &scope).unwrap();
        assert_eq!(again.apply(&job), job);
        assert!(
            !std::fs::read_to_string(dir.join(CONFIG_FILENAME))
                .unwrap()
                .contains("d-pw")
        );

        // The `env` property an API server recorded keeps the allowlist and passthrough names.
        let mut recorded = props(
            r#"{"MRSM_SQL_MAIN": "postgresql://u:p@h/d", "MRSM_CONFIG": "{}", "LANG": "C",
                "TPR_CLOUD_KEY": "s", "TPR_PASSED": "p", "MRSM_DAEMON_ID": "j"}"#,
        );
        let passthrough = meerschaum_core::team_scope::Passthrough::from_config(&config);
        assert!(strip_team_env(&mut recorded, &passthrough));
        assert_eq!(
            recorded.keys().collect::<Vec<_>>(),
            vec!["LANG", "TPR_PASSED", "MRSM_DAEMON_ID"]
        );
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
