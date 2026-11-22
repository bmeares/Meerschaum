//! Host-prepared runtime paths. Preparation reads files and never starts an interpreter.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::{Access, Rule, Spec};

/// Internal native entry point: confinement precedes execution of the following Python command.
pub const PYTHON_CHILD: &str = "--_mrsm-confined-python";

/// Reserved host launch provenance, set only after a complete filtered environment is built.
/// Lifecycle wrappers consume it; their actual command child never inherits it.
pub const LIFECYCLE_VAR: &str = "MRSM_NATIVE_JOB_LIFECYCLE";

/// Loader and Python startup variables explicitly removed from scoped systemd units. Unlike
/// direct child spawning, UnsetEnvironment has no wildcard. This includes the documented
/// glibc ld.so environment controls, also when absent from the host's current environment.
pub const BOOTSTRAP_UNSET: &[&str] = &[
    "LD_ASSUME_KERNEL",
    "LD_BIND_NOW",
    "LD_LIBRARY_PATH",
    "LD_PRELOAD",
    "LD_TRACE_LOADED_OBJECTS",
    "LD_AUDIT",
    "LD_BIND_NOT",
    "LD_DEBUG",
    "LD_DEBUG_OUTPUT",
    "LD_DYNAMIC_WEAK",
    "LD_HWCAP_MASK",
    "LD_ORIGIN_PATH",
    "LD_POINTER_GUARD",
    "LD_PROFILE",
    "LD_PROFILE_OUTPUT",
    "LD_SHOW_AUXV",
    "LD_TRACE_PRELINKING",
    "LD_USE_LOAD_BIAS",
    "LD_VERBOSE",
    "LD_WARN",
    "LD_PREFER_MAP_32BIT_EXEC",
    "LD_TRACE_LOADED_OBJECTS_ALL",
    "LD_PRELOAD_32",
    "LD_PRELOAD_64",
    "LD_CONFIG_FILE",
    "DYLD_LIBRARY_PATH",
    "DYLD_FALLBACK_LIBRARY_PATH",
    "DYLD_FRAMEWORK_PATH",
    "DYLD_FALLBACK_FRAMEWORK_PATH",
    "DYLD_INSERT_LIBRARIES",
    "DYLD_ROOT_PATH",
    "DYLD_IMAGE_SUFFIX",
    "GCONV_PATH",
    "LOCPATH",
    "GLIBC_TUNABLES",
    "MALLOC_TRACE",
    "GETCONF_DIR",
    "NLSPATH",
    "BASH_ENV",
    "ENV",
    "PYTHONPATH",
    "PYTHONHOME",
    "PYTHONSTARTUP",
    "PYTHONUSERBASE",
    "PYTHONINSPECT",
    "PYTHONBREAKPOINT",
    "PYTHONEXECUTABLE",
    "__PYVENV_LAUNCHER__",
];

/// The exact host-prepared native compiler material in this protected generation.
/// It is never an execution-request, environment or source manifest selector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCompilerBinding {
    /// Immutable administrator-selected compiler snapshot manifest digest.
    pub manifest_sha256: String,
    /// Digest of the complete fixed sibling compiler descriptor, including its deadline.
    pub prepared_sha256: String,
}

/// Runtime provenance written by the host, outside the team's writable directories.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Runtime {
    /// Native host's API-only profile. Sticky enforcement captures this before Python sites.
    #[serde(default, skip_serializing_if = "ordinary_runtime")]
    pub api_only: bool,
    /// Optional empty read-only per-generation cache for a constructor-attested SDK.
    /// Never taken from PYTHONPYCACHEPREFIX, source/configuration or child properties.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_pycache_prefix: Option<PathBuf>,
    /// Optional trusted native compiler preparation, bound independently of plugin sources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_compiler: Option<NativeCompilerBinding>,
    /// Registered canonical native CLI, never a Python launcher or MRSM_BIN override.
    pub launcher: PathBuf,
    /// Host-selected interpreter, retaining a virtual environment's executable symlink.
    pub python: PathBuf,
    /// Host-selected import/loader settings restored only after confinement.
    pub environment: Vec<(String, String)>,
    /// Exact top-level import targets, so a source root's adjacent private files stay denied.
    pub packages: BTreeMap<String, PathBuf>,
    /// Host-derived standard venv site-packages; site hooks run only after kernel confinement.
    pub site_paths: Vec<PathBuf>,
    /// The logical virtual-environment prefix, when pyvenv.cfg establishes one.
    pub prefix: Option<PathBuf>,
}

fn ordinary_runtime(api_only: &bool) -> bool {
    !api_only
}

/// Variables that can execute code during a native or Python process's initial loading.
pub fn injection_name(name: &str) -> bool {
    let name = name.to_ascii_uppercase();
    name.starts_with("LD_")
        || name.starts_with("DYLD_")
        || name.starts_with("PYTHON")
        || BOOTSTRAP_UNSET.contains(&name.as_str())
        || matches!(
            name.as_str(),
            "GCONV_PATH" | "LOCPATH" | "GLIBC_TUNABLES" | "BASH_ENV" | "ENV"
        )
}

/// Job-controlled values never select an interpreter, bootstrap, library or import search path.
pub fn host_runtime_name(name: &str) -> bool {
    injection_name(name)
        || matches!(
            name,
            "PATH"
                | "VIRTUAL_ENV"
                | "CONDA_PREFIX"
                | "CONDA_DEFAULT_ENV"
                | "MRSM_BIN"
                | "MRSM_BIN_EXEC"
                | "MRSM_PYTHON"
                | "MRSM_JOB_SUPERVISOR"
                | "MRSM_TEAM_SOURCE_CONTEXT"
                | "MRSM_JOB_PROGRAM"
        )
}

/// Remove loader/import injection before a native bootstrap is started. The complete child
/// environment is otherwise already filtered to the team's own configuration.
pub fn sanitize_command(command: &mut std::process::Command) {
    for (name, _) in crate::process_env::vars() {
        if injection_name(&name) {
            command.env_remove(name);
        }
    }
    let explicit: Vec<_> = command
        .get_envs()
        .filter(|(name, _)| injection_name(&name.to_string_lossy()))
        .map(|(name, _)| name.to_os_string())
        .collect();
    for name in explicit {
        command.env_remove(name);
    }
}

fn resolve_program(program: &Path, environment: &[(String, String)]) -> std::io::Result<PathBuf> {
    let path = if program.is_absolute() {
        program.to_path_buf()
    } else if program.components().count() == 1 {
        let paths = environment
            .iter()
            .rev()
            .find(|(key, _)| key == "PATH")
            .map(|(_, value)| value)
            .ok_or_else(|| {
                std::io::Error::other("The trusted host has no executable search path.")
            })?;
        std::env::split_paths(paths)
            .filter(|path| path.is_absolute())
            .map(|path| path.join(program))
            .find(|path| path.is_file())
            .ok_or_else(|| std::io::Error::other("The trusted Python executable is unavailable."))?
    } else {
        return Err(std::io::Error::other(
            "The trusted interpreter must use an absolute path.",
        ));
    };
    let parent = std::fs::canonicalize(
        path.parent()
            .ok_or_else(|| std::io::Error::other("Invalid interpreter path."))?,
    )?;
    let path = parent.join(
        path.file_name()
            .ok_or_else(|| std::io::Error::other("Invalid interpreter path."))?,
    );
    if !path.is_file() {
        return Err(std::io::Error::other(
            "The trusted Python executable is unavailable.",
        ));
    }
    Ok(path)
}

fn prefix_rules(prefix: &Path, rules: &mut Vec<Rule>, site_paths: &mut Vec<PathBuf>) {
    let config = prefix.join("pyvenv.cfg");
    if config.is_file() {
        rules.push(Rule::new(
            config,
            Access::Read,
            "host-selected Python configuration",
        ));
    }
    // Venv and managed-CPython layouts. Do not grant the whole prefix: it may contain unrelated
    // files. In particular, no sys.path/site hook output is ever turned into a filesystem grant.
    for name in ["lib", "lib64"] {
        if let Ok(entries) = std::fs::read_dir(prefix.join(name)) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                let version = name
                    .strip_prefix("python")
                    .map(|version| version.strip_suffix(".zip").unwrap_or(version));
                if (version.is_some_and(|version| {
                    !version.is_empty()
                        && version
                            .bytes()
                            .all(|c| c.is_ascii_digit() || matches!(c, b'.' | b't'))
                })) || (name.starts_with("libpython") && entry.path().is_file())
                {
                    rules.push(Rule::new(
                        entry.path(),
                        Access::ReadExec,
                        "host-selected Python libraries",
                    ));
                    for directory in ["site-packages", "dist-packages"] {
                        let site = entry.path().join(directory);
                        if site.is_dir() && !site_paths.contains(&site) {
                            site_paths.push(site);
                        }
                    }
                }
            }
        }
    }
}

fn package_rules(
    path: &Path,
    rules: &mut Vec<Rule>,
    packages: &mut BTreeMap<String, PathBuf>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(path)?.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let (name, target, rule_path) =
            if entry.path().is_dir() && entry.path().join("__init__.py").is_file() {
                (name, entry.path().join("__init__.py"), entry.path())
            } else if entry.path().is_file() && name.ends_with(".py") {
                (
                    name.trim_end_matches(".py").to_string(),
                    entry.path(),
                    entry.path(),
                )
            } else {
                continue;
            };
        if name.is_empty()
            || name.starts_with('.')
            || !name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
        {
            continue;
        }
        let target = std::fs::canonicalize(target)?;
        let rule_path = std::fs::canonicalize(rule_path)?;
        rules.push(Rule::new(
            rule_path,
            Access::ReadExec,
            "host-selected import package or module",
        ));
        packages.entry(name).or_insert(target);
    }
    Ok(())
}

fn loader_rules(path: &Path, rules: &mut Vec<Rule>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(path)?.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.path().is_file()
            && (name.ends_with(".so")
                || name.contains(".so.")
                || name.ends_with(".dylib")
                || name.ends_with(".dll"))
        {
            rules.push(Rule::new(
                std::fs::canonicalize(entry.path())?,
                Access::ReadExec,
                "host-selected shared library",
            ));
        }
    }
    Ok(())
}

impl Runtime {
    /// Capture only the native host's environment. Persisted job variables are not inputs.
    pub fn prepare(spec: &mut Spec, environment: &[(String, String)]) -> std::io::Result<Self> {
        let launcher = crate::self_exe::native_executable().ok_or_else(|| std::io::Error::other(
            "Isolated team execution needs a trusted standalone mrsm launcher before Python startup. Start the server with the native mrsm binary; Python embedding cannot establish this boundary."
        ))?;
        let python = environment
            .iter()
            .rev()
            .find(|(key, value)| key == "MRSM_PYTHON" && !value.trim().is_empty())
            .map(|(_, value)| PathBuf::from(value))
            .unwrap_or_else(|| PathBuf::from(if cfg!(windows) { "python" } else { "python3" }));
        let python = resolve_program(&python, environment)?;
        let real = std::fs::canonicalize(&python)?;
        let mut rules = vec![
            Rule::new(
                &launcher,
                Access::ReadExec,
                "host-selected native bootstrap",
            ),
            Rule::new(&real, Access::ReadExec, "host-selected Python executable"),
        ];
        let mut site_paths = Vec::new();
        let mut prefix = None;
        for executable in [&python, &real] {
            if let Some(prefix) = executable.parent().and_then(Path::parent) {
                prefix_rules(prefix, &mut rules, &mut site_paths);
            }
        }
        if real.starts_with("/usr/bin") {
            prefix_rules(Path::new("/usr/local"), &mut rules, &mut site_paths);
        }
        if let Some(logical_prefix) = python
            .parent()
            .and_then(Path::parent)
            .filter(|path| path.join("pyvenv.cfg").is_file())
        {
            prefix = Some(logical_prefix.to_path_buf());
            let system_site = std::fs::read_to_string(logical_prefix.join("pyvenv.cfg"))?
                .lines()
                .filter_map(|line| line.split_once('='))
                .any(|(key, value)| {
                    key.trim()
                        .eq_ignore_ascii_case("include-system-site-packages")
                        && value.trim().eq_ignore_ascii_case("true")
                });
            if !system_site {
                site_paths.retain(|path| path.starts_with(logical_prefix));
            }
        }
        let mut packages = BTreeMap::new();
        let mut kept = Vec::new();
        for (key, value) in environment {
            if matches!(key.as_str(), "PATH") {
                kept.push((key.clone(), value.clone()));
            }
            if matches!(
                key.as_str(),
                "PYTHONPATH"
                    | "LD_LIBRARY_PATH"
                    | "DYLD_LIBRARY_PATH"
                    | "DYLD_FALLBACK_LIBRARY_PATH"
            ) {
                let mut paths = Vec::new();
                for path in std::env::split_paths(value).filter(|path| !path.as_os_str().is_empty())
                {
                    if !path.is_absolute() {
                        return Err(std::io::Error::other(
                            "A trusted runtime search path must be absolute.",
                        ));
                    }
                    let path = std::fs::canonicalize(path)?;
                    if key == "PYTHONPATH" {
                        package_rules(&path, &mut rules, &mut packages)?;
                    } else {
                        loader_rules(&path, &mut rules)?;
                    }
                    paths.push(path);
                }
                let value = std::env::join_paths(paths).map_err(std::io::Error::other)?;
                if key != "PYTHONPATH" {
                    kept.push((key.clone(), value.to_string_lossy().into_owned()));
                }
            }
        }
        let rules: Vec<_> = rules
            .into_iter()
            .map(|mut rule| {
                rule.path = std::fs::canonicalize(&rule.path).unwrap_or(rule.path);
                rule
            })
            .collect();
        let (allowed, dropped) = spec.allowed(rules);
        if !dropped.is_empty() {
            return Err(std::io::Error::other(
                "A trusted runtime path covers protected server data.",
            ));
        }
        spec.rules.extend(allowed);
        kept.extend([
            ("MRSM_BIN".into(), launcher.to_string_lossy().into_owned()),
            ("MRSM_PYTHON".into(), python.to_string_lossy().into_owned()),
            ("PYTHONNOUSERSITE".into(), "1".into()),
            ("PYTHONSAFEPATH".into(), "1".into()),
        ]);
        Ok(Self {
            api_only: false,
            sdk_pycache_prefix: None,
            native_compiler: None,
            launcher,
            python,
            environment: kept,
            packages,
            site_paths,
            prefix,
        })
    }
}
