//! Local host, sandbox, plugin, job, and stack fact gatherers for `mrsm doctor`.

use super::*;

// ---------------------------------------------------------------------------------------------
// Gathering: thin wrappers over the system.
// ---------------------------------------------------------------------------------------------

/// How long a network, database or subprocess probe may take.
pub(super) const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) fn timed(f: impl FnOnce() -> Vec<Check>) -> Vec<Check> {
    let start = Instant::now();
    let mut checks = f();
    let ms = start.elapsed().as_millis();
    for c in &mut checks {
        c.millis.get_or_insert(ms);
    }
    checks
}

/// `flavor` of a SQL connector's attributes: the attribute, else the URI scheme.
pub fn flavor_of(attrs: &Map) -> String {
    if let Some(f) = attrs
        .get("flavor")
        .and_then(Value::as_str)
        .filter(|f| !f.is_empty())
    {
        return f.to_lowercase();
    }
    let flavor = attrs
        .get("uri")
        .and_then(Value::as_str)
        .and_then(|u| u.split("://").next())
        .and_then(|s| s.split('+').next())
        .unwrap_or("")
        .to_lowercase();
    if flavor == "postgres" {
        "postgresql".into()
    } else {
        flavor
    }
}

/// The database file a SQLite-like connector names (`None` when it names none).
pub fn sqlite_path(attrs: &Map, label: &str, root_db: &str) -> Option<String> {
    let from_uri = attrs.get("uri").and_then(Value::as_str).map(|u| {
        let rest = u.split_once("://").map(|(_, r)| r).unwrap_or(u);
        let rest = rest.strip_prefix('/').unwrap_or(rest);
        rest.split('?').next().unwrap_or(rest).to_string()
    });
    let path = from_uri
        .or_else(|| {
            attrs
                .get("database")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| (label == "local").then(|| root_db.to_string()))?;
    Some(path.replace("{SQLITE_DB_PATH}", root_db))
}

pub(super) fn config_opts() -> GetOptions {
    GetOptions {
        write_missing: false,
        warn: false,
        ..GetOptions::default()
    }
}

pub(crate) fn probe_python(python: &str) -> Result<PyProbe, String> {
    let modules: Vec<Json> = PROBE_MODULES.iter().map(|(m, d)| json!([m, d])).collect();
    let spec = CommandSpec {
        scrub_secrets: true,
        program: python.to_string(),
        args: vec![
            "-c".into(),
            PROBE_SCRIPT.into(),
            Json::Array(modules).to_string(),
        ],
        env: vec![("PYTHONSAFEPATH".into(), "1".into())],
        ..CommandSpec::default()
    };
    let out = crate::native::terminal::capture(&spec, Some(Duration::from_secs(10)))
        .map_err(|e| format!("{e}"))?;
    if out.timed_out {
        return Err("it did not answer within 10 seconds".into());
    }
    if out.code != 0 {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(format!(
            "it exited with {}: {}",
            out.code,
            stderr.lines().last().unwrap_or("").trim()
        ));
    }
    parse_probe(&String::from_utf8_lossy(&out.stdout))
}

pub(super) fn how_it_runs(env: fn(&str) -> Option<String>) -> String {
    let command = meerschaum_core::self_exe::self_command();
    let mut text = if meerschaum_core::self_exe::runs_in_python() {
        format!(
            "Python extension, in-process (re-launches itself as `{}`)",
            command.join(" ")
        )
    } else {
        format!(
            "standalone binary {}",
            command.first().map(String::as_str).unwrap_or("mrsm")
        )
    };
    if let Some(bin) = env("MRSM_BIN").filter(|b| !b.is_empty()) {
        text.push_str(&format!("; MRSM_BIN={bin}"));
    }
    text
}

pub(super) fn os_description() -> String {
    let base = format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH);
    let pretty = std::fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|t| {
            t.lines()
                .find_map(|l| l.strip_prefix("PRETTY_NAME="))
                .map(|v| v.trim_matches('"').to_string())
        });
    match pretty {
        Some(p) => format!("{base} ({p})"),
        None => base,
    }
}

pub(super) fn install_checks(
    env: fn(&str) -> Option<String>,
    python_command: &str,
    probe: &Result<PyProbe, String>,
    python_plugins: usize,
) -> Vec<Check> {
    let build = format!(
        "{} build; oracle driver {}; DuckDB loads its own library at run time",
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        if cfg!(feature = "oracle") {
            "compiled in"
        } else {
            "not compiled in"
        },
    );
    vec![
        Check::pass(
            "install",
            "meerschaum",
            format!("{}; {}", crate::VERSION, how_it_runs(env)),
        ),
        Check::pass("install", "build", build),
        Check::pass("install", "platform", os_description()),
        Check::from_verdict(
            "install",
            "worker python",
            judge_interpreter(probe, python_command, python_plugins),
        ),
        Check::from_verdict(
            "install",
            "meerschaum package",
            judge_package(
                probe,
                crate::VERSION,
                meerschaum_core::python_guard::WORKER_PROTOCOL_VERSION,
                python_plugins,
            ),
        ),
    ]
}

pub(super) fn writable(path: &Path) -> bool {
    #[cfg(unix)]
    {
        rustix::fs::access(path, rustix::fs::Access::WRITE_OK).is_ok()
    }
    #[cfg(not(unix))]
    {
        std::fs::metadata(path).is_ok_and(|m| !m.permissions().readonly())
    }
}

pub(super) fn dir_fact(path: &Path) -> DirFact {
    match std::fs::metadata(path) {
        Ok(m) => DirFact {
            exists: true,
            is_dir: m.is_dir(),
            writable: writable(path),
        },
        Err(_) => DirFact {
            exists: false,
            is_dir: false,
            writable: false,
        },
    }
}

#[cfg(unix)]
pub(super) fn disk_fact(path: &Path) -> Option<DiskFact> {
    let s = rustix::fs::statvfs(path).ok()?;
    let unit = if s.f_frsize > 0 {
        s.f_frsize
    } else {
        s.f_bsize
    };
    let total_inodes = (s.f_files > 0).then_some(s.f_files);
    Some(DiskFact {
        free_bytes: s.f_bavail.saturating_mul(unit),
        total_bytes: s.f_blocks.saturating_mul(unit),
        free_inodes: total_inodes.map(|_| s.f_ffree),
        total_inodes,
    })
}

#[cfg(not(unix))]
pub(super) fn disk_fact(_path: &Path) -> Option<DiskFact> {
    None
}

/// Isolated teams' filesystem sandbox: the mode, the level and what this system enforces
/// (Landlock ABI and features), and the sockets of concern a "filesystem only" sandbox does not
/// stop.
pub(super) fn team_sandbox_checks(config: &Config) -> Vec<Check> {
    use meerschaum_core::team_scope::sandbox::{self, Mode};
    let root = config.paths().root_dir;
    let mode = Mode::from_config(config);
    let support = sandbox::kernel_support();
    let isolated = meerschaum_core::team_scope::isolated_teams_recorded(&root);
    let system = support.describe();
    let mut out = Vec::new();
    let (enforced, state) = match (mode, support.available()) {
        (Mode::Off, _) => (
            false,
            "off: isolated teams' processes are not sandboxed".to_string(),
        ),
        (Mode::Auto, false) => (
            false,
            "auto, not enforced: isolated teams' processes run unsandboxed here".to_string(),
        ),
        (mode, false) => (
            false,
            format!(
                "{}, unavailable: isolated teams' code will not run here",
                mode.as_str()
            ),
        ),

        (mode, true) => (
            true,
            format!(
                "{}, enforced, {}",
                mode.as_str(),
                support.level().unwrap_or_default()
            ),
        ),
    };
    let detail = format!("{state} ({system})");
    if !isolated {
        out.push(Check::pass(
            "teams",
            "sandbox",
            format!("{detail}; no isolated team is recorded"),
        ));
    } else if enforced {
        out.push(Check::pass("teams", "sandbox", detail));
    } else {
        out.push(Check::warn(
            "teams",
            "sandbox",
            detail,
            "Run the API server on Linux 5.13 or newer with Landlock enabled, and set \
             `system.engine.python_team_sandbox` to `auto` or `require`.",
        ));
    }
    // Whether Unix sockets are denied to team code (Landlock from Linux 7.1, else the
    // seccomp filter: a probe process applies the sandbox as a team process would), and what
    // that means for web terminals.
    let denied = if isolated {
        let (denied, words) = crate::team::sandbox::team_sockets(mode, &root);
        if denied {
            out.push(Check::pass("teams", "unix sockets", words));
        } else {
            out.push(Check::warn(
                "teams",
                "unix sockets",
                words,
                if mode == Mode::Off {
                    "Turn the sandbox on: `system.engine.python_team_sandbox: auto`."
                } else {
                    "Let the server install seccomp filters (a container runtime's seccomp \
                     profile must allow `seccomp()`), or run it on Linux 7.1 or newer."
                },
            ));
        }
        let (refused, words) = crate::team::sandbox::terminals_words(&root, denied);
        if refused {
            out.push(Check::warn(
                "teams",
                "terminals",
                words,
                "Deny Unix sockets to team code (above), or turn off plugin installs for the \
                 isolated teams named.",
            ));
        } else {
            out.push(Check::pass("teams", "terminals", words));
        }
        denied
    } else {
        false
    };
    if isolated && enforced && support.abi() < sandbox::SOCKETS_ABI && !denied {
        let reason = sandbox::cached_probe(mode, &root).sockets_reason;
        out.push(Check::warn(
            "teams",
            "sandbox level",
            crate::team::sandbox::filesystem_only_warning(
                support.abi(),
                reason.as_deref(),
                &crate::team::sandbox::sockets_of_concern(),
            ),
            "Let the server install seccomp filters, run it on Linux 7.1 or newer, run it in a \
             container or as a service without a session bus, or set \
             `system.engine.python_team_sandbox: require_sockets`.",
        ));
    } else if isolated
        && !enforced
        && !denied
        && let Some(socket) = crate::team::sandbox::container_socket_reachable()
    {
        out.push(Check::warn(
            "teams",
            "container socket",
            format!(
                "this user can open `{}` and the sandbox does not stop isolated teams' code from \
                 doing the same (root on this host)",
                socket.display()
            ),
            "Run the API server as a user without Docker access while isolated teams run code.",
        ));
    }
    let limits = sandbox::ResourceLimits::from_config(config);
    if limits.is_configured() {
        out.push(Check::pass("teams", "resource limits", limits.describe()));
    } else if isolated {
        out.push(Check::pass(
            "teams",
            "resource limits",
            "unrestricted (default; set system.engine.python_team_max_* or python_team_egress to cap host-team processes)",
        ));
    }
    // Per team that runs code, whether its members' commands may run here.
    for team in crate::team::execution::code_running_teams(config) {
        let label = format!("team execution ({team})");
        match crate::team::execution::team_status(config, &team) {
            Ok(()) => out.push(Check::pass(
                "teams",
                &label,
                "available: its members' commands and terminal run as its sandboxed processes",
            )),
            Err(why) => out.push(Check::warn(
                "teams",
                &label,
                format!("not available: {why}"),
                "Its members keep the commands every non-administrator may run. Team execution \
                 needs `system.engine.python_team_sandbox` other than `off`, Linux 7.1 or newer \
                 (Landlock ABI 9) and a Python `meerschaum` 5.1.0 or newer.",
            )),
        }
    }
    out
}

pub(super) fn path_checks(config: &Config) -> Vec<Check> {
    let paths = config.paths();
    let mut out = Vec::new();
    let show = |p: &Path| p.display().to_string();
    out.push(Check::from_verdict(
        "paths",
        "root",
        judge_dir(
            &show(&paths.root_dir),
            DirRole::Root,
            &dir_fact(&paths.root_dir),
        ),
    ));
    out.push(Check::from_verdict(
        "paths",
        "config",
        judge_dir(
            &show(&paths.config_dir),
            DirRole::Config,
            &dir_fact(&paths.config_dir),
        ),
    ));
    for dir in &paths.plugins_dirs {
        out.push(Check::from_verdict(
            "paths",
            "plugins",
            judge_dir(&show(dir), DirRole::Plugins, &dir_fact(dir)),
        ));
    }
    let temp = std::env::temp_dir();
    for (name, path) in [("disk: root", paths.root_dir.clone()), ("disk: temp", temp)] {
        let existing = if path.exists() {
            path.clone()
        } else {
            PathBuf::from(".")
        };
        match disk_fact(&existing) {
            Some(fact) => out.push(Check::from_verdict(
                "paths",
                name,
                judge_disk(&show(&path), &fact),
            )),
            None => out.push(Check::skip(
                "paths",
                name,
                "free space is not available on this platform",
            )),
        }
    }
    out
}

pub(super) fn read_config_files(dir: &Path) -> Vec<ConfigFile> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut files: Vec<ConfigFile> = entries
        .flatten()
        .filter_map(|e| {
            let path = e.path();
            let ext = path.extension()?.to_str()?.to_lowercase();
            if !matches!(ext.as_str(), "yaml" | "yml" | "json") || !path.is_file() {
                return None;
            }
            let name = path.file_name()?.to_string_lossy().into_owned();
            let stem = path.file_stem()?.to_string_lossy().into_owned();
            let parsed = std::fs::read_to_string(&path)
                .map_err(|e| e.to_string())
                .and_then(|text| {
                    if ext == "json" {
                        Value::from_json_str(&text).map_err(|e| e.to_string())
                    } else {
                        meerschaum_core::yaml::load(&text).map_err(|e| e.0)
                    }
                });
            Some(match parsed {
                Ok(value) => ConfigFile {
                    name,
                    stem,
                    error: None,
                    value: Some(value),
                },
                Err(error) => ConfigFile {
                    name,
                    stem,
                    error: Some(first_line(&error)),
                    value: None,
                },
            })
        })
        .collect();
    files.sort_by(|a, b| a.name.cmp(&b.name));
    files
}

pub(super) fn config_checks(
    config: &Config,
    vars: &[(String, String)],
    env: fn(&str) -> Option<String>,
    instance_keys: &str,
    explicit_instance: bool,
    custom_types: &[String],
) -> Vec<Check> {
    let mut out = Vec::new();
    let defaults = config.defaults();
    let known_top: Vec<String> = defaults.keys().cloned().collect();
    let known_meerschaum: Vec<String> = defaults
        .get("meerschaum")
        .and_then(Value::as_map)
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    let files = read_config_files(&config.paths().config_dir);
    for (name, verdict) in judge_config(&files, &known_top, &known_meerschaum) {
        out.push(Check::from_verdict("config", name, verdict));
    }
    let (connector_vars, other_vars) = split_env_vars(
        &meerschaum_core::config::get_connector_env_vars(vars),
        custom_types,
    );
    let mut env_detail = if connector_vars.is_empty() {
        "no MRSM_<TYPE>_<LABEL> connector variables".to_string()
    } else {
        format!("connector variables: {}", connector_vars.join(", "))
    };
    if !other_vars.is_empty() {
        env_detail.push_str(&format!(
            "; other MRSM_ variables: {}",
            other_vars.join(", ")
        ));
    }
    // `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE` hold the same text in a file.
    let bad_patch = [
        "MRSM_CONFIG",
        "MRSM_PATCH",
        "MRSM_CONFIG_FILE",
        "MRSM_PATCH_FILE",
    ]
    .iter()
    .find_map(|var| {
        let text = if var.ends_with("_FILE") {
            env(var)
                .filter(|path| !path.is_empty())
                .and_then(|path| std::fs::read_to_string(path).ok())
        } else {
            env(var)
        };
        text.filter(|v| !v.trim().is_empty())
            .and_then(|v| serde_json::from_str::<Json>(&v).err().map(|e| (*var, e)))
    });
    match bad_patch {
        Some((var, e)) => out.push(Check::warn(
            "config",
            "environment",
            format!("{env_detail}; {var} is not valid JSON and is ignored ({e})"),
            format!("fix the JSON in {var}"),
        )),
        None => {
            env_detail.push_str("; values are never printed");
            out.push(Check::pass("config", "environment", env_detail));
        }
    }
    out.push(Check::pass(
        "config",
        "instance",
        if explicit_instance {
            format!("instance is {instance_keys} (`-i`)")
        } else {
            format!("default instance is {instance_keys}")
        },
    ));
    out
}

pub(super) fn driver_checks(
    rows: &[crate::native::drivers::DriverRow],
    connectors: &[(String, String)],
    instance_label: Option<&str>,
    auto_install: bool,
    host: Option<&DuckdbHostDiagnostic>,
) -> Vec<Check> {
    let mut checks: Vec<Check> = rows
        .iter()
        .map(|row| {
            let fact = DriverFact {
                name: row.name,
                source: row.source.clone(),
                version: row.version.clone(),
                path: row.path.clone(),
            };
            let needed = driver_users(connectors, row.name);
            Check::from_verdict(
                "drivers",
                row.name,
                judge_driver(&fact, &needed, instance_label, auto_install),
            )
        })
        .collect();
    if let Some(host) = host
        && !matches!(host, DuckdbHostDiagnostic::Native)
    {
        let needed = driver_users(connectors, "duckdb");
        let needed_instance = instance_label.is_some_and(|label| needed.contains(&label));
        checks.push(Check::from_verdict(
            "drivers",
            "duckdb",
            judge_duckdb_host(host, !needed.is_empty(), needed_instance),
        ));
    }
    checks
}

/// The extra table is the packaging-checked source of the minimum, not another diagnostic
/// version literal. DuckDB Engine is optional for Pipe-only public-driver transport.
pub(super) fn duckdb_requirement() -> &'static str {
    crate::extras::EXTRAS
        .iter()
        .find(|(name, _)| *name == "sql-duckdb")
        .and_then(|(_, requirements)| {
            requirements.iter().find_map(|requirement| {
                requirement.strip_prefix("duckdb").filter(|rest| {
                    rest.chars()
                        .next()
                        .is_some_and(|c| matches!(c, '>' | '<' | '=' | '!' | '~'))
                })
            })
        })
        .expect("sql-duckdb extra specifies its driver minimum")
}

/// Host metadata is never represented as a successful import or database connection.
pub(super) fn judge_duckdb_host(
    host: &DuckdbHostDiagnostic,
    needed: bool,
    needed_instance: bool,
) -> Verdict {
    let problem = |detail: String, fix: &str| {
        if !needed {
            Verdict::skip(detail)
        } else if needed_instance {
            Verdict::fail(detail, fix)
        } else {
            Verdict::warn(detail, fix)
        }
    };
    match host {
        DuckdbHostDiagnostic::Native => Verdict::skip("native transport uses the library checks"),
        DuckdbHostDiagnostic::Refused(reason) => problem(
            reason.clone(),
            "restart with a fresh interpreter (spawn or exec); keep MRSM_DUCKDB_LIB unchanged after selecting a transport",
        ),
        DuckdbHostDiagnostic::Python {
            version,
            path,
            pinned,
        } => {
            let policy = if *pinned {
                "selected"
            } else {
                "prospective (policy unchanged)"
            };
            let location = path
                .as_deref()
                .map(|p| format!(" at {p}"))
                .unwrap_or_default();
            let unverified = "; driver import and database connection were not probed";
            let Some(version) = version else {
                return Verdict::warn(
                    format!(
                        "Python DuckDB {policy}; installed version metadata unavailable{unverified}"
                    ),
                    "install or repair the sql-duckdb extra and restart the interpreter",
                );
            };
            let requirement = duckdb_requirement();
            match meerschaum_core::version::version_satisfies(version, requirement) {
                Some(true) => Verdict::pass(format!(
                    "Python DuckDB {version} metadata{location}; {policy}{unverified}"
                )),
                Some(false) => problem(
                    format!(
                        "Python DuckDB {version} metadata{location} does not meet duckdb{requirement}{unverified}"
                    ),
                    "upgrade the sql-duckdb extra and restart the interpreter",
                ),
                None => Verdict::warn(
                    format!(
                        "Python DuckDB version metadata {version:?} cannot be checked against duckdb{requirement}{unverified}"
                    ),
                    "repair the sql-duckdb extra and restart the interpreter",
                ),
            }
        }
    }
}

pub(super) fn sql_connectors(config: &Config) -> Vec<(String, String)> {
    let (_, section) = config.get_with(&["meerschaum", "connectors", "sql"], config_opts());
    section
        .as_map()
        .map(|m| {
            m.keys()
                .filter(|label| *label != "default")
                .map(|label| {
                    let attrs = if let Some(v) = m.get(label).and_then(Value::as_map) {
                        v.clone()
                    } else {
                        Map::new()
                    };
                    (label.clone(), flavor_of(&attrs))
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn jobs_dir_facts(jobs_dir: &Path) -> Vec<JobFact> {
    let Ok(entries) = std::fs::read_dir(jobs_dir) else {
        return Vec::new();
    };
    let mut jobs: Vec<JobFact> = entries
        .flatten()
        .filter(|e| e.path().join("properties.json").is_file())
        .map(|e| {
            let dir = e.path();
            let pid = std::fs::read_to_string(dir.join("process.pid"))
                .ok()
                .and_then(|t| t.trim().parse::<i32>().ok());
            let process = pid.map(|pid| match meerschaum_jobs::process::proc_state(pid) {
                meerschaum_jobs::process::ProcState::Alive => ProcessState::Alive,
                meerschaum_jobs::process::ProcState::Stopped => ProcessState::Paused,
                _ => ProcessState::Gone,
            });
            let result = std::fs::read_to_string(dir.join("properties.json"))
                .ok()
                .and_then(|t| serde_json::from_str::<Json>(&t).ok())
                .and_then(|p| p.get("result").cloned())
                .and_then(|r| {
                    let list = r.as_array()?;
                    Some((
                        list.first()?.as_bool()?,
                        list.get(1).and_then(Json::as_str).unwrap_or("").to_string(),
                    ))
                });
            JobFact {
                name: e.file_name().to_string_lossy().into_owned(),
                pid,
                process,
                result,
            }
        })
        .collect();
    jobs.sort_by(|a, b| a.name.cmp(&b.name));
    jobs
}

pub(super) fn venv_facts(venvs_dir: &Path) -> Vec<VenvFact> {
    let Ok(entries) = std::fs::read_dir(venvs_dir) else {
        return Vec::new();
    };
    let mut venvs: Vec<VenvFact> = entries
        .flatten()
        .filter(|e| e.path().is_dir())
        .filter_map(|e| {
            let root = e.path();
            let sites = crate::native::venvs::site_packages(&root);
            if sites.is_empty() {
                return None;
            }
            let minors = sites
                .iter()
                .filter_map(|s| {
                    let version = s.parent()?.file_name()?.to_string_lossy().into_owned();
                    parse_major_minor(version.strip_prefix("python")?)
                })
                .collect();
            let bin = root.join(if cfg!(windows) { "Scripts" } else { "bin" });
            let dangling = std::fs::read_dir(&bin)
                .map(|entries| {
                    entries
                        .flatten()
                        .filter(|b| {
                            let name = b.file_name().to_string_lossy().into_owned();
                            name.starts_with("python")
                                && std::fs::symlink_metadata(b.path())
                                    .is_ok_and(|m| m.file_type().is_symlink())
                                && std::fs::metadata(b.path()).is_err()
                        })
                        .map(|b| b.file_name().to_string_lossy().into_owned())
                        .collect()
                })
                .unwrap_or_default();
            Some(VenvFact {
                name: e.file_name().to_string_lossy().into_owned(),
                minors,
                dangling,
            })
        })
        .collect();
    venvs.sort_by(|a, b| a.name.cmp(&b.name));
    venvs
}

pub(super) fn extras_checks(
    probe: &Result<PyProbe, String>,
    facts: Option<&PluginFacts>,
    python: &str,
) -> Vec<Check> {
    let Ok(p) = probe else {
        return vec![Check::skip(
            "extras",
            "python extras",
            "no Python interpreter to ask",
        )];
    };
    let interpreter = if p.executable.is_empty() {
        python
    } else {
        &p.executable
    };
    EXTRA_MODULES
        .iter()
        .map(|(extra, modules)| {
            let mods: Vec<(&str, Option<&ModuleProbe>)> =
                modules.iter().map(|m| (*m, p.modules.get(*m))).collect();
            let needed_by: Vec<String> = match (*extra, facts) {
                ("api", Some(f)) => f.needs_api.clone(),
                ("dash", Some(f)) => f.needs_dash.clone(),
                _ => Vec::new(),
            };
            Check::from_verdict(
                "extras",
                *extra,
                judge_extra(extra, &mods, &needed_by, interpreter),
            )
        })
        .collect()
}

pub(super) fn plugin_checks(
    manifest: &Result<Arc<Manifest>, crate::builtin::ActionError>,
    facts: Option<&PluginFacts>,
    cache: &Verdict,
    venvs: &[VenvFact],
    probe: &Result<PyProbe, String>,
) -> Vec<Check> {
    let mut out = vec![Check::from_verdict("plugins", "manifest", cache.clone())];
    match (manifest, facts) {
        (Err(e), _) => out.push(Check::warn(
            "plugins",
            "manifest read",
            format!(
                "the plugin manifest could not be read: {}",
                first_line(&e.message)
            ),
            "`mrsm reload` rebuilds it; `mrsm verify plugins` shows the import errors",
        )),
        (_, Some(f)) => {
            for (name, verdict) in judge_plugins(f) {
                out.push(Check::from_verdict("plugins", name, verdict));
            }
        }
        _ => {}
    }
    let worker = probe
        .as_ref()
        .ok()
        .and_then(|p| parse_major_minor(&p.version));
    out.push(Check::from_verdict(
        "plugins",
        "venvs",
        judge_venvs(venvs, worker),
    ));
    out
}

pub(super) fn job_checks(jobs_dir: &Path) -> Vec<Check> {
    let facts = jobs_dir_facts(jobs_dir);
    judge_jobs(&facts)
        .into_iter()
        .map(|(name, verdict)| Check::from_verdict("jobs", name, verdict))
        .collect()
}

pub(super) fn gather_stack(config: &Config, path: Option<String>) -> Vec<Check> {
    let compose = config.paths().path("STACK_COMPOSE_PATH");
    if !compose.exists() {
        return vec![Check::skip(
            "stack",
            "services",
            "no stack file (`mrsm stack` writes it on first use)",
        )];
    }
    let Some(docker) = crate::native::pip::which("docker", path.as_deref()) else {
        return vec![Check::skip(
            "stack",
            "services",
            "the `docker` command was not found",
        )];
    };
    let (_, project) = config.get_with(&["stack", "project_name"], config_opts());
    let project = project.as_str().unwrap_or("mrsm").to_string();
    // `docker ps` with the compose project label: read-only, needs no compose file parsing and
    // never prints a container's environment.
    let spec = CommandSpec {
        scrub_secrets: true,
        program: docker.to_string_lossy().into_owned(),
        args: vec![
            "ps".into(),
            "-a".into(),
            "--filter".into(),
            format!("label=com.docker.compose.project={project}"),
            "--format".into(),
            "{{.Label \"com.docker.compose.service\"}}\t{{.Names}}\t{{.Image}}\t{{.State}}\t{{.Status}}"
                .into(),
        ],
        ..CommandSpec::default()
    };
    let out = match crate::native::terminal::capture(&spec, Some(Duration::from_secs(4))) {
        Ok(o) if !o.timed_out && o.code == 0 => o,
        Ok(o) => {
            let why = if o.timed_out {
                "did not answer within 4 seconds".to_string()
            } else {
                first_line(&String::from_utf8_lossy(&o.stderr))
            };
            return vec![Check::skip(
                "stack",
                "services",
                format!("Docker does not answer ({why})"),
            )];
        }
        Err(e) => {
            return vec![Check::skip(
                "stack",
                "services",
                format!("Docker does not answer ({e})"),
            )];
        }
    };
    let services = parse_services(&String::from_utf8_lossy(&out.stdout));
    if services.is_empty() {
        return vec![Check::skip(
            "stack",
            "services",
            format!("no containers for project '{project}' (`mrsm stack up -d` starts them)"),
        )];
    }
    services
        .iter()
        .map(|s| {
            Check::from_verdict(
                "stack",
                format!("service: {}", s.service),
                judge_service(s, &project),
            )
        })
        .collect()
}
