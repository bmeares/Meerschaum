//! Pure diagnostic decision functions for `mrsm doctor`.

use super::*;

// ---------------------------------------------------------------------------------------------
// Decisions: pure functions over plain data (unit-tested).
// ---------------------------------------------------------------------------------------------

pub(super) const MIB: u64 = 1024 * 1024;
pub(super) const GIB: u64 = 1024 * MIB;

/// Free space under which a filesystem is a WARN / a FAIL.
pub const DISK_WARN_BYTES: u64 = GIB;
/// See [`DISK_WARN_BYTES`].
pub const DISK_FAIL_BYTES: u64 = 64 * MIB;
/// Free inodes (percent) under which a filesystem is a WARN / a FAIL.
pub const INODE_WARN_PERCENT: f64 = 5.0;
/// See [`INODE_WARN_PERCENT`].
pub const INODE_FAIL_PERCENT: f64 = 1.0;
/// The oldest Python the `meerschaum` package supports.
pub const MIN_PYTHON: (u32, u32) = (3, 11);

/// `1.5 GB` (binary units, like `df -h`).
pub fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Whether two version strings name the same release (`5.0.0-rc.1` is `5.0.0rc1`, PEP 440);
/// strings that do not parse compare as text.
pub fn versions_match(a: &str, b: &str) -> bool {
    match (
        meerschaum_core::version::parse_version(a.trim()),
        meerschaum_core::version::parse_version(b.trim()),
    ) {
        (Ok(x), Ok(y)) => x == y,
        _ => a.trim() == b.trim(),
    }
}

/// `3.13.1` -> `(3, 13)`.
pub fn parse_major_minor(version: &str) -> Option<(u32, u32)> {
    let mut parts = version.split('.');
    let major = parts.next()?.trim().parse().ok()?;
    let minor = parts
        .next()?
        .trim()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()?;
    Some((major, minor))
}

/// A filesystem's free space and inodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskFact {
    /// Bytes available to an unprivileged process.
    pub free_bytes: u64,
    /// Bytes in the filesystem.
    pub total_bytes: u64,
    /// Free inodes (`None`: the filesystem has no inode count).
    pub free_inodes: Option<u64>,
    /// Total inodes.
    pub total_inodes: Option<u64>,
}

impl DiskFact {
    /// Free inodes as a percentage (`None` without an inode count).
    pub fn inode_free_percent(&self) -> Option<f64> {
        match (self.free_inodes, self.total_inodes) {
            (Some(free), Some(total)) if total > 0 => Some(free as f64 * 100.0 / total as f64),
            _ => None,
        }
    }
}

/// Free space and inodes of the filesystem holding `what`.
pub fn judge_disk(what: &str, f: &DiskFact) -> Verdict {
    let percent = f.inode_free_percent();
    let inodes = percent
        .map(|p| format!(", inodes {p:.0}% free"))
        .unwrap_or_default();
    let detail = format!(
        "{what}: {} free of {}{inodes}",
        human_bytes(f.free_bytes),
        human_bytes(f.total_bytes)
    );
    let fix = format!(
        "free space on the filesystem of {what} (or point it elsewhere: MRSM_ROOT_DIR, TMPDIR)"
    );
    if f.free_bytes < DISK_FAIL_BYTES || percent.is_some_and(|p| p < INODE_FAIL_PERCENT) {
        return Verdict::fail(detail, fix);
    }
    if f.free_bytes < DISK_WARN_BYTES || percent.is_some_and(|p| p < INODE_WARN_PERCENT) {
        return Verdict::warn(detail, fix);
    }
    Verdict::pass(detail)
}

/// What a directory is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirRole {
    /// `MRSM_ROOT_DIR`: everything is written below it.
    Root,
    /// The config directory.
    Config,
    /// A plugins directory.
    Plugins,
}

/// A directory's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirFact {
    /// The path exists.
    pub exists: bool,
    /// It is a directory.
    pub is_dir: bool,
    /// The current user may write to it.
    pub writable: bool,
}

/// Exists and is writable.
pub fn judge_dir(path: &str, role: DirRole, f: &DirFact) -> Verdict {
    let (what, severe) = match role {
        DirRole::Root => ("the root directory (config, jobs, venvs, caches)", true),
        DirRole::Config => ("the config directory", false),
        DirRole::Plugins => ("the plugins directory", false),
    };
    if f.exists && !f.is_dir {
        return Verdict::fail(
            format!("{path} exists but is not a directory"),
            format!("move it away or point {what} elsewhere"),
        );
    }
    if !f.exists {
        return match role {
            DirRole::Root => Verdict::fail(
                format!("{path} does not exist"),
                "create it, or set MRSM_ROOT_DIR to an existing directory",
            ),
            DirRole::Config => Verdict::pass(format!("{path} (created on first write)")),
            DirRole::Plugins => Verdict::pass(format!(
                "{path} (not created yet; `mrsm install plugins` creates it)"
            )),
        };
    }
    if !f.writable {
        let fix = format!(
            "make {path} writable for this user (e.g. `chmod u+w {path}`), or use another directory"
        );
        let detail = format!("{path} is not writable: {what} cannot be updated");
        return if severe {
            Verdict::fail(detail, fix)
        } else {
            Verdict::warn(detail, fix)
        };
    }
    Verdict::pass(format!("{path} (writable)"))
}

/// One config file of the config directory.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigFile {
    /// The file name (`meerschaum.yaml`).
    pub name: String,
    /// The config key it holds (`meerschaum`).
    pub stem: String,
    /// The parse error, if any.
    pub error: Option<String>,
    /// The parsed content.
    pub value: Option<Value>,
}

/// v4-only config keys v5 ignores when they are set: `(path, advice)`. A key v5's own default
/// config still writes (`join_fetch: false`) only counts when it is switched on.
pub const DEPRECATED_KEYS: &[(&[&str], &str)] = &[(
    &["system", "experimental", "join_fetch"],
    "`join_fetch: true` is ignored in v5 (it was undocumented and off by default): remove it",
)];

/// The keys of [`DEPRECATED_KEYS`] set in the parsed files: `(file, dotted path, advice)`.
pub fn deprecated_keys(files: &[ConfigFile]) -> Vec<(String, String, &'static str)> {
    let mut out = Vec::new();
    for file in files {
        let Some(value) = &file.value else { continue };
        for (path, advice) in DEPRECATED_KEYS {
            if path[0] != file.stem {
                continue;
            }
            if value.get_path(&path[1..]).is_some_and(Value::truthy) {
                out.push((file.name.clone(), path.join(":"), *advice));
            }
        }
    }
    out
}

/// The config files and keys: parse errors, files that are not a config key, unknown
/// `meerschaum:` sections and deprecated keys. `known_top` and `known_meerschaum` come from the
/// registry's defaults.
pub fn judge_config(
    files: &[ConfigFile],
    known_top: &[String],
    known_meerschaum: &[String],
) -> Vec<(&'static str, Verdict)> {
    let mut out = Vec::new();
    let broken: Vec<String> = files
        .iter()
        .filter_map(|f| f.error.as_ref().map(|e| format!("{}: {e}", f.name)))
        .collect();
    if !broken.is_empty() {
        out.push((
            "files",
            Verdict::fail(
                format!(
                    "{} config file{} not parse (the defaults are used instead): {}",
                    broken.len(),
                    if broken.len() == 1 { " does" } else { "s do" },
                    broken.join("; ")
                ),
                "fix the syntax (`mrsm edit config <key>`), or move the file away so defaults are rewritten",
            ),
        ));
    } else if files.is_empty() {
        out.push((
            "files",
            Verdict::pass("no config files yet (defaults in use)"),
        ));
    } else {
        out.push((
            "files",
            Verdict::pass(format!("{} config files parsed", files.len())),
        ));
    }
    let mut notes = Vec::new();
    for f in files {
        if !known_top.contains(&f.stem) {
            notes.push(format!(
                "{} is not a config key (known: {})",
                f.name,
                known_top.join(", ")
            ));
        }
        if f.stem == "meerschaum"
            && let Some(map) = f.value.as_ref().and_then(Value::as_map)
        {
            for key in map.keys().filter(|k| !known_meerschaum.contains(k)) {
                notes.push(format!("meerschaum:{key} in {} is unknown", f.name));
            }
        }
    }
    let deprecated = deprecated_keys(files);
    for (file, path, advice) in &deprecated {
        notes.push(format!("{path} in {file}: {advice}"));
    }
    if notes.is_empty() {
        out.push(("keys", Verdict::pass("no unknown or deprecated keys")));
    } else {
        out.push((
            "keys",
            Verdict::warn(
                notes.join("; "),
                "edit or remove them (`mrsm edit config <key>`); unknown files are ignored unless a plugin reads them",
            ),
        ));
    }
    out
}

/// `MRSM_*` variable names split into connector variables (`MRSM_<TYPE>_<LABEL>` of a connector
/// type that exists) and others that merely look like one (`MRSM_DUCKDB_LIB`, `MRSM_TEST_*`).
pub fn split_env_vars(names: &[String], custom_types: &[String]) -> (Vec<String>, Vec<String>) {
    let mut connectors = Vec::new();
    let mut others = Vec::new();
    for name in names {
        let known =
            meerschaum_core::config::parse_connector_env_var(name).is_some_and(|(typ, _)| {
                meerschaum_core::config::connector_types::is_builtin_connector_type(&typ)
                    || custom_types.contains(&typ)
            });
        if known {
            connectors.push(name.clone());
        } else {
            others.push(name.clone());
        }
    }
    (connectors, others)
}

/// What a connector section needs from the native drivers: `(label, flavor)` of every SQL
/// connector (the default instance's label is `instance`).
pub fn driver_users<'a>(connectors: &'a [(String, String)], flavor: &str) -> Vec<&'a str> {
    connectors
        .iter()
        .filter(|(_, f)| f == flavor)
        .map(|(l, _)| l.as_str())
        .collect()
}

/// A native driver library (DuckDB, Oracle client).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverFact {
    /// `duckdb` or `oracle`.
    pub name: &'static str,
    /// `show drivers`' source (`env`, `cache`, `system`, `error`, `missing`).
    pub source: String,
    /// Its version, if known.
    pub version: String,
    /// Its path (or the error text when the source is `error`).
    pub path: String,
}

/// Found, or missing and needed by a configured connector (FAIL when the default instance needs
/// it and nothing will fetch it, WARN otherwise).
pub fn judge_driver(
    d: &DriverFact,
    needed_by: &[&str],
    instance_label: Option<&str>,
    auto_install: bool,
) -> Verdict {
    let need_instance = instance_label.is_some_and(|l| needed_by.contains(&l));
    let needed = needed_by.join(", ");
    match d.source.as_str() {
        "missing" | "error" => {
            let broken = d.source == "error";
            let what = if broken {
                format!("{} cannot be loaded: {}", d.name, d.path)
            } else {
                format!("{} is not installed", d.name)
            };
            let fix = match d.name {
                "duckdb" => {
                    "`mrsm install driver duckdb` (or set MRSM_DUCKDB_LIB to a libduckdb you provide)"
                }
                _ => {
                    "install Oracle Instant Client from Oracle and put it on the library path (LD_LIBRARY_PATH, DYLD_LIBRARY_PATH or PATH)"
                }
            };
            if needed_by.is_empty() {
                return Verdict::skip(format!("{what}; no configured connector needs it"));
            }
            let first_use = !broken && d.name == "duckdb" && auto_install;
            if need_instance && !first_use {
                return Verdict::fail(format!("{what}; the default instance needs it"), fix);
            }
            if first_use {
                return Verdict::warn(
                    format!("{what}; it is downloaded on first use (needed by {needed})"),
                    fix,
                );
            }
            Verdict::warn(format!("{what}; needed by {needed}"), fix)
        }
        _ => {
            let version = d.version.trim();
            let mut detail = if version.starts_with('(') || version.is_empty() {
                "found".to_string()
            } else {
                version.to_string()
            };
            if !d.path.is_empty() {
                detail = format!("{detail} at {} ({})", d.path, d.source);
            }
            Verdict::pass(detail)
        }
    }
}

/// What the plugin manifest says (no plugin code is imported).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PluginFacts {
    /// The plugins (Python and native), by name.
    pub names: Vec<String>,
    /// `(plugin, first line of the import error)`.
    pub import_errors: Vec<(String, String)>,
    /// Errors raised importing all plugins at once.
    pub load_errors: Vec<(String, String)>,
    /// Plugins with `attempt_import` findings of undeclared packages.
    pub venv_warnings: Vec<String>,
    /// Plugins that need the API host (`api` extra).
    pub needs_api: Vec<String>,
    /// Plugins that need Dash (`dash` extra).
    pub needs_dash: Vec<String>,
    /// Plugins on disk the manifest does not list (it was never built, or is stale): their
    /// imports have not been checked, and the doctor never imports plugin code.
    pub unbuilt: Vec<String>,
}

pub(super) fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").trim().to_string()
}

/// Read the manifest.
pub fn plugin_facts(m: &Manifest) -> PluginFacts {
    let mut f = PluginFacts::default();
    for (name, p) in &m.plugins {
        f.names.push(name.clone());
        if let Some(e) = &p.import_error {
            f.import_errors.push((name.clone(), first_line(e)));
        }
        if !p.venv_warnings.is_empty() {
            f.venv_warnings.push(name.clone());
        }
        let host_pages = p.web_pages.iter().any(|w| w.mode != "dash");
        if !p.api_plugins.is_empty() || host_pages || !p.mcp_tools.is_empty() {
            f.needs_api.push(name.clone());
        }
        if !p.dash_plugins.is_empty() || p.web_pages.iter().any(|w| w.mode == "dash") {
            f.needs_dash.push(name.clone());
            // Dash apps are served by the API host.
            if !f.needs_api.contains(name) {
                f.needs_api.push(name.clone());
            }
        }
    }
    f.load_errors = m
        .load_errors
        .iter()
        .map(|(k, v)| (k.clone(), first_line(v)))
        .collect();
    f
}

/// The plugins checks: how many, import errors, undeclared packages.
pub fn judge_plugins(f: &PluginFacts) -> Vec<(&'static str, Verdict)> {
    let mut out = Vec::new();
    if f.names.is_empty() {
        out.push(("installed", Verdict::skip("no plugins installed")));
        return out;
    }
    out.push((
        "installed",
        Verdict::pass(format!(
            "{} plugin{}: {}",
            f.names.len(),
            crate::native::plural(f.names.len()),
            f.names.join(", ")
        )),
    ));
    let mut errors: Vec<String> = f
        .import_errors
        .iter()
        .map(|(n, e)| format!("{n}: {e}"))
        .collect();
    errors.extend(f.load_errors.iter().map(|(n, e)| format!("{n}: {e}")));
    if errors.is_empty() && !f.unbuilt.is_empty() {
        out.push((
            "imports",
            Verdict::skip(format!(
                "not imported yet: {} (the doctor never imports plugins; `mrsm reload` builds the manifest)",
                f.unbuilt.join(", ")
            )),
        ));
    } else if errors.is_empty() {
        out.push(("imports", Verdict::pass("every plugin imported")));
    } else {
        out.push((
            "imports",
            Verdict::warn(
                format!("import errors: {}", errors.join("; ")),
                "fix the plugin (`mrsm verify plugins` imports each one and prints the traceback); missing packages: `mrsm install required <plugin>`",
            ),
        ));
    }
    if !f.venv_warnings.is_empty() {
        out.push((
            "requirements",
            Verdict::warn(
                format!(
                    "plugins importing packages they do not list in `required`: {}",
                    f.venv_warnings.join(", ")
                ),
                "add the packages to the plugin's `required` list",
            ),
        ));
    }
    out
}

/// The cached plugin manifest against the plugins on disk (read-only: the doctor never builds
/// it). `stale` is `stale_plugins` of the cached manifest, `changed` the plugins it names.
pub fn judge_manifest(
    cached: bool,
    stale: bool,
    changed: &[String],
    on_disk: usize,
    listed: usize,
) -> Verdict {
    const FIX: &str = "`mrsm reload` rebuilds it (any command that loads plugins also rebuilds it when a plugin changed)";
    match (cached, stale, on_disk) {
        (false, _, 0) => Verdict::pass("no plugins on disk, so no manifest is needed"),
        (false, _, n) => Verdict::warn(
            format!(
                "the plugin manifest is missing: {n} plugin{} on disk {} not been imported (the doctor never imports plugins)",
                crate::native::plural(n),
                if n == 1 { "has" } else { "have" }
            ),
            FIX,
        ),
        (true, true, _) => Verdict::warn(
            format!(
                "the plugin manifest is stale{}: the plugin details below are from the last build",
                if changed.is_empty() {
                    String::new()
                } else {
                    format!(" (changed: {})", changed.join(", "))
                }
            ),
            FIX,
        ),
        (true, false, _) => Verdict::pass(format!(
            "up to date ({listed} plugin{})",
            crate::native::plural(listed)
        )),
    }
}

/// One extra: installed, or missing while a plugin needs it.
pub fn judge_extra(
    extra: &str,
    modules: &[(&str, Option<&ModuleProbe>)],
    needed_by: &[String],
    python: &str,
) -> Verdict {
    let found: Vec<String> = modules
        .iter()
        .filter_map(|(m, p)| {
            p.filter(|p| p.found).map(|p| match &p.version {
                Some(v) => format!("{m} {v}"),
                None => m.to_string(),
            })
        })
        .collect();
    let missing: Vec<&str> = modules
        .iter()
        .filter(|(_, p)| !p.is_some_and(|p| p.found))
        .map(|(m, _)| *m)
        .collect();
    let fix = format!("`{python} -m pip install 'meerschaum[{extra}]'`");
    if missing.is_empty() {
        return Verdict::pass(found.join(", "));
    }
    if !needed_by.is_empty() {
        return Verdict::warn(
            format!(
                "plugins {} need the `{extra}` extra; missing: {}",
                needed_by.join(", "),
                missing.join(", ")
            ),
            fix,
        );
    }
    if found.is_empty() {
        return Verdict::skip("not installed (optional)");
    }
    // Optional and needed by nothing installed: informational (the missing packages are named).
    Verdict::pass(format!(
        "{}; not installed: {}",
        found.join(", "),
        missing.join(", ")
    ))
}

/// A virtual environment on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenvFact {
    /// The directory name (`mrsm`, or a plugin's name).
    pub name: String,
    /// The `X.Y` of every `lib/pythonX.Y/site-packages`.
    pub minors: Vec<(u32, u32)>,
    /// `bin/python*` entries that no longer resolve (dangling links).
    pub dangling: Vec<String>,
}

/// Venvs built by another Python than the worker's, or whose interpreter link is gone.
pub fn judge_venvs(venvs: &[VenvFact], worker: Option<(u32, u32)>) -> Verdict {
    if venvs.is_empty() {
        return Verdict::skip("no virtual environments");
    }
    let mut stale = Vec::new();
    for v in venvs {
        if !v.dangling.is_empty() {
            stale.push(format!(
                "{} (interpreter {} is gone)",
                v.name,
                v.dangling.join(", ")
            ));
        } else if let Some((major, minor)) = worker
            && !v.minors.is_empty()
            && !v.minors.contains(&(major, minor))
        {
            let built: Vec<String> = v.minors.iter().map(|(a, b)| format!("{a}.{b}")).collect();
            stale.push(format!(
                "{} (built for Python {}, the worker is {major}.{minor})",
                v.name,
                built.join("/")
            ));
        }
    }
    if stale.is_empty() {
        return Verdict::pass(format!(
            "{} virtual environment{} match the worker interpreter",
            venvs.len(),
            crate::native::plural(venvs.len())
        ));
    }
    Verdict::warn(
        format!("stale virtual environments: {}", stale.join("; ")),
        "`mrsm verify venvs` relinks a lost interpreter; after a Python upgrade `mrsm delete venvs <name>` then `mrsm install required <plugin>` rebuilds it",
    )
}

/// One job directory, read without any status call (those unlink stale PID files).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobFact {
    /// The job's name.
    pub name: String,
    /// The PID file's content.
    pub pid: Option<i32>,
    /// The process state: `None` without a PID.
    pub process: Option<ProcessState>,
    /// The recorded result: `(success, message)`.
    pub result: Option<(bool, String)>,
}

/// What the doctor distinguishes of a process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessState {
    /// Running.
    Alive,
    /// Stopped by a signal (a paused job).
    Paused,
    /// No such process (or a zombie).
    Gone,
}

/// Jobs: how many run, which failed, which PID files are stale.
pub fn judge_jobs(jobs: &[JobFact]) -> Vec<(&'static str, Verdict)> {
    if jobs.is_empty() {
        return vec![("state", Verdict::skip("no jobs"))];
    }
    let running = jobs
        .iter()
        .filter(|j| j.process == Some(ProcessState::Alive))
        .count();
    let paused = jobs
        .iter()
        .filter(|j| j.process == Some(ProcessState::Paused))
        .count();
    let stale: Vec<&str> = jobs
        .iter()
        .filter(|j| j.pid.is_some() && j.process == Some(ProcessState::Gone))
        .map(|j| j.name.as_str())
        .collect();
    let failed: Vec<String> = jobs
        .iter()
        .filter(|j| matches!(j.process, None | Some(ProcessState::Gone)))
        .filter_map(|j| match &j.result {
            Some((false, message)) => Some(format!("{} ({})", j.name, first_line(message))),
            _ => None,
        })
        .collect();
    let mut out = vec![(
        "state",
        Verdict::pass(format!(
            "{} job{}: {running} running, {paused} paused, {} stopped",
            jobs.len(),
            crate::native::plural(jobs.len()),
            jobs.len() - running - paused
        )),
    )];
    if !failed.is_empty() {
        out.push((
            "failed jobs",
            Verdict::warn(
                format!("the last run failed: {}", failed.join("; ")),
                "`mrsm show logs <name>` for the output, then `mrsm restart jobs <name>`",
            ),
        ));
    }
    if !stale.is_empty() {
        out.push((
            "pid files",
            Verdict::warn(
                format!(
                    "stale PID files (the process is gone): {}",
                    stale.join(", ")
                ),
                "`mrsm show jobs` removes them; `mrsm start jobs <name>` starts the job again",
            ),
        ));
    }
    out
}

/// One container of the stack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceFact {
    /// The compose service (`db`).
    pub service: String,
    /// The container name.
    pub container: String,
    /// The image reference.
    pub image: String,
    /// `running`, `exited`, `restarting`, ...
    pub state: String,
    /// `Up 3 hours (healthy)`.
    pub status: String,
}

/// Parse `docker ps --format '{{label service}}\t{{.Names}}\t{{.Image}}\t{{.State}}\t{{.Status}}'`.
pub fn parse_services(text: &str) -> Vec<ServiceFact> {
    text.lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.split('\t').collect();
            (parts.len() >= 5).then(|| ServiceFact {
                service: parts[0].to_string(),
                container: parts[1].to_string(),
                image: parts[2].to_string(),
                state: parts[3].to_string(),
                status: parts[4].to_string(),
            })
        })
        .collect()
}

/// The exit code of `Exited (137) 2 minutes ago`.
pub fn exit_code(status: &str) -> Option<i32> {
    let rest = status.strip_prefix("Exited (")?;
    rest.split(')').next()?.trim().parse().ok()
}

/// One stack service's check.
pub fn judge_service(s: &ServiceFact, project: &str) -> Verdict {
    let status = s.status.to_lowercase();
    let detail = format!("{} ({}): {}", s.image, s.container, s.status);
    let service = if s.service.is_empty() {
        "<service>"
    } else {
        &s.service
    };
    if s.state == "running" && status.contains("unhealthy") {
        return Verdict::fail(
            detail,
            format!(
                "`docker logs {}` shows why; `mrsm stack restart {service}` restarts it",
                s.container
            ),
        );
    }
    if s.state == "running" && status.contains("health: starting") {
        return Verdict::warn(detail, "wait a moment and run `mrsm doctor` again");
    }
    if s.state == "running" {
        return Verdict::pass(detail);
    }
    // A service stopped on purpose (`docker stop`, `mrsm stack stop`) exits 0, or 137 / 143 for
    // the signal that stopped it: a warning, not a failure.
    if s.state == "exited" && matches!(exit_code(&s.status), Some(0 | 137 | 143)) {
        return Verdict::warn(
            format!("stopped: {detail}"),
            format!("`mrsm stack up -d {service}` starts it again (project {project})"),
        );
    }
    Verdict::fail(
        detail,
        format!(
            "`mrsm stack up -d {service}` (project {project}); `docker logs {}` shows why it stopped",
            s.container
        ),
    )
}

/// The API server's version against this client's.
pub fn judge_api_version(server: Option<&str>, client: &str) -> Verdict {
    match server {
        None => Verdict::warn(
            "the server did not report its version (`/version/mrsm`)",
            "check the server is Meerschaum and reachable at the configured host and port",
        ),
        Some(v) if versions_match(v, client) => {
            Verdict::pass(format!("server and client are {client}"))
        }
        Some(v) => Verdict::warn(
            format!("the server runs {v}, this client is {client}"),
            "upgrade the older side so both run the same release",
        ),
    }
}

/// The doctor's name for the check of `key` (`pipes key`, `users key`).
pub fn unique_key_check_name(key: &UniqueKey) -> String {
    format!("{} key", key.table.trim_start_matches("mrsm_"))
}

/// An instance table has its unique key (5.0: `mrsm_pipes` on the pipe keys, `mrsm_users` on the
/// username, `mrsm_plugins` on the plugin name). A table created by v4 gets it the first time 5.0 writes to the instance, unless a
/// key value is held by more than one row.
pub fn judge_unique_key(
    key: &UniqueKey,
    status: &Result<KeyStatus, String>,
    table: &str,
) -> Verdict {
    let (what, rows, keep) = if *key == PIPES_KEY {
        (
            "the pipe keys",
            "pipe(s) are registered",
            "the parameters you want",
        )
    } else if *key == PLUGINS_KEY {
        (
            "the plugin name",
            "plugin(s) are registered",
            "the version and owner you want",
        )
    } else {
        (
            "the username",
            "user(s) are registered",
            "the password and plugins you want",
        )
    };
    match status {
        Ok(KeyStatus::Present) => {
            Verdict::pass(format!("{} has a unique key on {what}", key.table))
        }
        Ok(KeyStatus::NoTable) => {
            Verdict::skip(format!("the instance has no {} table yet", key.table))
        }
        Ok(KeyStatus::Missing { duplicates }) if duplicates.is_empty() => Verdict::warn(
            format!("{} has no unique key on {what} yet", key.table),
            "it is added the next time Meerschaum registers, syncs or edits a pipe or user on the instance with permission to create indexes",
        ),
        Ok(KeyStatus::Missing { duplicates }) => {
            let shown: Vec<String> = duplicates
                .iter()
                .take(5)
                .map(|d| describe_duplicate(key, d))
                .collect();
            let more = duplicates.len().saturating_sub(shown.len());
            let extra: Vec<String> = duplicates
                .iter()
                .flat_map(|d| d.ids.iter().skip(1))
                .map(i64::to_string)
                .collect();
            Verdict::warn(
                format!(
                    "{} {rows} more than once, so {} has no unique key on {what}: {}{}",
                    duplicates.len(),
                    key.table,
                    shown.join("; "),
                    if more > 0 {
                        format!("; and {more} more")
                    } else {
                        String::new()
                    },
                ),
                format!(
                    "keep one row of each and delete the others (rows other than the first: `DELETE FROM {table} WHERE {} IN ({})`; check which row holds {keep} first); the key is added the next time a pipe or user is registered, synced or edited",
                    key.id_column,
                    extra.join(", ")
                ),
            )
        }
        Err(e) => Verdict::warn(
            format!("could not check {} for its unique key: {e}", key.table),
            "re-run `mrsm doctor` once the instance is reachable",
        ),
    }
}

pub(super) fn describe_duplicate(key: &UniqueKey, d: &Duplicate) -> String {
    let ids: Vec<String> = d.ids.iter().map(i64::to_string).collect();
    let ids = format!("({} {})", key.id_column, ids.join(", "));
    let text = |v: &Option<String>| v.clone().unwrap_or_default();
    if *key == PIPES_KEY {
        let location = match d.values.get(2) {
            Some(Some(lk)) => format!(", '{lk}'"),
            _ => String::new(),
        };
        format!(
            "Pipe('{}', '{}'{location}) {ids}",
            d.values.first().map(text).unwrap_or_default(),
            d.values.get(1).map(text).unwrap_or_default(),
        )
    } else {
        let values: Vec<String> = d.values.iter().map(|v| format!("'{}'", text(v))).collect();
        format!("{} {ids}", values.join(", "))
    }
}
