//! Preparing a team process: the spec file, the team's plugin links and virtual environments,
//! its database files, and the environment that points the process at them.

use super::*;

/// What [`prepare`] wrote for a team process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepared {
    /// The mode.
    pub mode: Mode,
    /// The spec file ([`SPEC_VAR`]).
    pub spec_file: PathBuf,
    /// The team's temporary directory (`TMPDIR`).
    pub tmp_dir: PathBuf,
    /// The team's cache directory (`XDG_CACHE_HOME`).
    pub cache_dir: PathBuf,
    /// The team's plugins directory (`MRSM_PLUGINS_DIR`): links to its plugins only.
    pub plugins_dir: PathBuf,
    /// The team's own virtual environments (`MRSM_VENVS_DIR`).
    pub venvs_dir: PathBuf,
    /// The spec.
    pub spec: Spec,
}

/// Write the sandbox spec of a process of `scope` (a worker of its pool, or with `job_dir` a
/// job) to `spec_file`, after creating the team's temporary, cache and (for a job) work
/// directories and the canary. `None` in `off` mode (a stale spec file is removed). In `require`
/// mode, `Err` when this system cannot sandbox a process, so nothing of the team's starts.
pub fn prepare(
    config: &Config,
    scope: &TeamScope,
    files: &ScopeFiles,
    job_dir: Option<&Path>,
    spec_file: &Path,
) -> std::io::Result<Option<Prepared>> {
    prepare_with(config, scope, files, job_dir, spec_file, false)
}

/// [`prepare`] for a process of team execution (5.1, `required`): a command, job or terminal a
/// member of a team that runs code started, which must never run outside the sandbox at
/// [`LEVEL_SOCKETS`]. Its mode is [`Mode::RequireSockets`] whatever the setting says, and `off`
/// is an error.
pub fn prepare_with(
    config: &Config,
    scope: &TeamScope,
    files: &ScopeFiles,
    job_dir: Option<&Path>,
    spec_file: &Path,
    required: bool,
) -> std::io::Result<Option<Prepared>> {
    let mut mode = Mode::from_config(config);
    if required {
        if mode == Mode::Off {
            return Err(std::io::Error::other(format!(
                "This process of the isolated team '{}' runs a member's command, which needs the \
                 sandbox at \"{LEVEL_SOCKETS}\", but `system.engine.python_team_sandbox` is \
                 `off`.",
                scope.team
            )));
        }
        mode = Mode::RequireSockets;
    }
    if mode == Mode::Off {
        let _ = std::fs::remove_file(spec_file);
        return Ok(None);
    }
    let support = kernel_support();
    if mode.requires()
        && let Support::Unsupported { reason } = &support
    {
        return Err(std::io::Error::other(format!(
            "The isolated team '{}' cannot run: its processes must be sandboxed \
             (`system.engine.python_team_sandbox: {}`), but Landlock is unavailable here: \
             {reason}.",
            scope.team,
            mode.as_str()
        )));
    }
    if mode == Mode::RequireSockets {
        // ABI 6 can scope signals, with the seccomp filter supplying pathname socket denial.
        // ABI alone proves neither that a container permits installing that filter nor that
        // its canary/socket probes hold. Use a fresh child, and recheck in the actual launch.
        let proof = probe_team_process(mode, None).map_err(std::io::Error::other)?;
        proof.require_sockets_verdict().map_err(|why| {
            std::io::Error::other(format!(
                "The isolated team '{}' cannot run: {why}.",
                scope.team
            ))
        })?;
    }
    let paths = config.paths();
    let root = paths.root_dir.clone();
    let key = scope.key();
    let scope_dir = super::scope_dir(&root, &key);
    super::create_private_dir(&scope_dir)?;
    let cache_dir = scope_dir.join(CACHE_DIR);
    super::create_private_dir(&cache_dir)?;
    let tmp_dir = team_tmp_dir(&root, &key)?;
    ensure_canary(&root)?;
    // The plugins package a team process imports its plugins through (read-only to it: it is
    // created here when the root has none yet).
    let plugins_package = root.join(".internal").join("plugins");
    std::fs::create_dir_all(&plugins_package)?;
    let init = plugins_package.join("__init__.py");
    if !init.exists() {
        std::fs::write(&init, "")?;
    }
    link_team_plugins(&plugins_package, &paths.plugins_dirs, &scope.plugins);
    let plugins_view = scope_dir.join(PLUGINS_VIEW_DIR);
    super::create_private_dir(&plugins_view)?;
    if let Ok(entries) = std::fs::read_dir(&plugins_view) {
        for entry in entries.flatten() {
            if entry.file_type().is_ok_and(|t| t.is_symlink()) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
    link_team_plugins(&plugins_view, &paths.plugins_dirs, &scope.plugins);
    let venvs_dir = paths
        .venvs_dir
        .clone()
        .unwrap_or_else(|| root.join("venvs"));
    let team_venvs_dir = scope_dir.join(VENVS_DIR);
    prepare_team_venvs(&venvs_dir, &team_venvs_dir, &scope.plugins)?;
    let work_dir = match job_dir {
        Some(dir) => {
            let work = dir.join(JOB_WORK_DIR);
            super::create_private_dir(&work)?;
            Some(work)
        }
        None => None,
    };
    let limits = ResourceLimits::from_config(config);
    if let Some(max_disk_mb) = limits.disk_mb {
        let mut dirs: Vec<&Path> = vec![&tmp_dir, &cache_dir, &team_venvs_dir];
        if let Some(work) = work_dir.as_deref() {
            dirs.push(work);
        }
        limits::check_disk_quota(&scope.team, &dirs, max_disk_mb)?;
    }
    let mut job_dirs = team_job_dirs(&root.join("jobs"), &key, &scope.team);
    if let Some(dir) = job_dir
        && !job_dirs.iter().any(|d| d == dir)
    {
        job_dirs.push(dir.to_path_buf());
    }
    let home = std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .map(PathBuf::from);
    let mut spec = build_spec(&SpecInputs {
        mode,
        team: &scope.team,
        root: &root,
        home: home.as_deref(),
        plugins_dirs: &paths.plugins_dirs,
        venvs_dir: &venvs_dir,
        team_venvs_dir: &team_venvs_dir,
        plugins: &scope.plugins,
        plugins_view: Some(&plugins_view),
        files,
        spec_file,
        tmp_dir: &tmp_dir,
        cache_dir: &cache_dir,
        job_dirs: &job_dirs,
        work_dir: work_dir.as_deref(),
    });
    spec.limits = limits;
    spec.runtime = Some(runtime::Runtime::prepare(
        &mut spec,
        &crate::process_env::vars(),
    )?);
    let mut extra = team_database_files(config, scope);
    // `uv` (outside the interpreter's prefix, as `~/.local/bin/uv` often is): a team's plugin code
    // installs missing requirements into its plugins' environments with it, as it always could.
    if let Some(paths) = std::env::var_os("PATH") {
        for uv in std::env::split_paths(&paths)
            .map(|dir| dir.join("uv"))
            .filter(|candidate| candidate.is_file())
            .take(1)
        {
            extra.push(Rule::new(
                uv,
                Access::ReadExec,
                "uv (installing requirements)",
            ));
        }
    }
    spec.rules.extend(spec.allowed(extra).0);
    super::write_private(
        spec_file,
        &serde_json::to_string_pretty(&spec).map_err(std::io::Error::other)?,
    )?;
    Ok(Some(Prepared {
        mode,
        spec_file: spec_file.to_path_buf(),
        tmp_dir,
        cache_dir,
        plugins_dir: plugins_view,
        venvs_dir: team_venvs_dir,
        spec,
    }))
}

/// The team's own virtual environments directory `team` (created owner-only): a link to the
/// server's shared `mrsm` environment (read-only to the team), and a copy of the server's
/// environment of each of the team's `plugins` that it does not have yet ([`seed_team_venv`]).
/// A copy that fails is left out (the team's code installs what it needs at run time).
fn prepare_team_venvs(
    shared: &Path,
    team: &Path,
    plugins: &BTreeSet<String>,
) -> std::io::Result<()> {
    super::create_private_dir(team)?;
    let link = team.join(SHARED_VENV);
    if std::fs::symlink_metadata(&link).is_err() && shared.join(SHARED_VENV).is_dir() {
        #[cfg(unix)]
        let _ = std::os::unix::fs::symlink(shared.join(SHARED_VENV), &link);
    }
    for plugin in plugins
        .iter()
        .filter(|p| plain_name(p) && *p != SHARED_VENV)
    {
        if let Err(e) = seed_team_venv(&shared.join(plugin), &team.join(plugin)) {
            tracing::warn!(
                "The virtual environment of plugin '{plugin}' could not be copied for an isolated                  team ({e}): its requirements are installed again when the team's code needs them."
            );
        }
    }
    Ok(())
}

/// Make sure the plugins package links each of the team's plugins (`.internal/plugins/<name>`
/// to the plugin in its plugins directory), as every unsandboxed process keeps it: a team
/// process may read the links but not write them. Existing entries are left alone.
fn link_team_plugins(package: &Path, plugins_dirs: &[PathBuf], plugins: &BTreeSet<String>) {
    for plugin in plugins.iter().filter(|p| plain_name(p)) {
        let found = plugins_dirs.iter().find_map(|dir| {
            [dir.join(plugin), dir.join(format!("{plugin}.py"))]
                .into_iter()
                .find(|candidate| candidate.exists())
        });
        let Some(target) = found else {
            continue;
        };
        let Some(name) = target.file_name() else {
            continue;
        };
        let link = package.join(name);
        if std::fs::symlink_metadata(&link).is_ok() {
            continue;
        }
        #[cfg(unix)]
        let _ = std::os::unix::fs::symlink(&target, &link);
        #[cfg(not(unix))]
        let _ = (&target, &link);
    }
}

/// The files of the team's file-database connectors (`sqlite`, `duckdb`, `geopackage`): the
/// database and its existing journal files, read and write (the files only: a journal SQLite
/// must create next to a database needs the database in WAL mode, or the server's writes).
fn team_database_files(config: &Config, scope: &TeamScope) -> Vec<Rule> {
    use crate::value::Value;
    let team = super::team_config(config, scope);
    let Some(Value::Map(sql)) = team
        .get("meerschaum")
        .and_then(|m| m.as_map())
        .and_then(|m| m.get("connectors"))
        .and_then(|c| c.as_map())
        .and_then(|c| c.get("sql"))
        .cloned()
    else {
        return Vec::new();
    };
    let mut rules = Vec::new();
    for (label, attrs) in sql {
        if label == "default" || !scope.connectors.contains(&format!("sql:{label}")) {
            continue;
        }
        let Some(attrs) = attrs.as_map() else {
            continue;
        };
        let flavor = attrs
            .get("flavor")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !matches!(flavor, "sqlite" | "duckdb" | "geopackage") {
            continue;
        }
        let Some(database) = attrs.get("database").and_then(Value::as_str) else {
            continue;
        };
        if database.is_empty() || database == ":memory:" {
            continue;
        }
        for suffix in ["", "-wal", "-shm", "-journal", ".wal"] {
            rules.push(Rule::new(
                format!("{database}{suffix}"),
                Access::DataFile,
                "the team's file database",
            ));
        }
    }
    rules
}

/// Point a team process's environment at its sandbox: [`SPEC_VAR`], `TMPDIR` / `TMP` / `TEMP`
/// (the team's temporary directory), `XDG_CACHE_HOME` (its cache directory), `MRSM_PLUGINS_DIR`
/// (its own plugins directory, [`PLUGINS_VIEW_DIR`]) and `MRSM_VENVS_DIR` (its own virtual
/// environments, [`VENVS_DIR`]).
pub fn apply_env(env: &mut Vec<(String, String)>, prepared: &Prepared) {
    // The native bootstrap must reach confinement before any loader or Python hook can run.
    env.retain(|(name, _)| !runtime::injection_name(name));
    let venvs = prepared.venvs_dir.to_string_lossy().into_owned();
    let tmp = prepared.tmp_dir.to_string_lossy().into_owned();
    let cache = prepared.cache_dir.to_string_lossy().into_owned();
    let spec = prepared.spec_file.to_string_lossy().into_owned();
    let plugins = prepared.plugins_dir.to_string_lossy().into_owned();
    for (name, value) in [
        ("MRSM_PLUGINS_DIR", plugins.as_str()),
        ("MRSM_VENVS_DIR", venvs.as_str()),
        ("TMPDIR", tmp.as_str()),
        ("TMP", tmp.as_str()),
        ("TEMP", tmp.as_str()),
        ("XDG_CACHE_HOME", cache.as_str()),
        (SPEC_VAR, spec.as_str()),
    ] {
        env.retain(|(k, _)| k != name);
        env.push((name.to_string(), value.to_string()));
    }
}

/// The job directories under `jobs_dir` whose properties name the team (its scope `key` or its
/// name).
pub fn team_job_dirs(jobs_dir: &Path, key: &str, team: &str) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(jobs_dir) else {
        return Vec::new();
    };
    let mut dirs: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|dir| {
            let Ok(text) = std::fs::read_to_string(dir.join("properties.json")) else {
                return false;
            };
            let Ok(props) = serde_json::from_str::<serde_json::Value>(&text) else {
                return false;
            };
            let text = |name: &str| props.get(name).and_then(|v| v.as_str()).map(str::trim);
            text("team_scope") == Some(key)
                || text("team").is_some_and(|t| t.eq_ignore_ascii_case(team.trim()))
        })
        .collect();
    dirs.sort();
    dirs
}

/// The team's temporary directory: `<system temp>/mrsm-<uid>-team-<hash of root and key>`,
/// created owner-only. Short on purpose (a Unix socket's path is at most 107 bytes, and the
/// team's workers bind theirs here). An existing one must be a directory (not a link) owned by
/// this user and private, or it is refused.
pub fn team_tmp_dir(root: &Path, key: &str) -> std::io::Result<PathBuf> {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    hasher.update(root.to_string_lossy().as_bytes());
    hasher.update([0]);
    hasher.update(key.as_bytes());
    let digest = hasher.finalize();
    let hash: String = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
    imp::private_tmp_dir(&format!("team-{hash}"))
}
