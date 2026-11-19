//! The environment of an isolated team's processes (5.1): an allowlist, never the launching
//! process's environment minus a few names.
//!
//! A team's workers and jobs start from an empty environment and get:
//!
//! - the variables a process and its libraries need to run and reach the network
//!   ([`SYSTEM_NAMES`], any `LC_*`): the executable search path, the user and home directory,
//!   locale and time zone, temporary directories, the terminal, certificate locations, proxy
//!   settings, the interpreter's and virtual environment's variables, the native library path;
//! - Meerschaum's variables that carry no secret ([`MEERSCHAUM_NAMES`]): the root, plugins and
//!   venvs directories, the interpreter and binary selection, the worker protocol, the
//!   auto-install, non-interactive, debug and trace switches;
//! - for a job, its own context ([`JOB_NAMES`]: its name, its Compose stamps), from the job's
//!   own settings only (never from the launching process, which may itself be a job);
//! - the names an administrator lists under `system.engine.python_team_env_passthrough`
//!   ([`PASSTHROUGH_KEY`]), except any `MRSM_*` name that is not allowed above (configuration,
//!   patches and connector variables), which is refused with a warning ([`Passthrough`]);
//! - the team's config directory, configuration file, name and configuration fingerprint (set,
//!   never inherited).
//!
//! Everything else the server holds (cloud credentials, tokens, its own settings) stays out.

use std::collections::BTreeSet;

use super::{CONFIG_DIR_VAR, ScopeFiles, TEAM_SCOPE_VAR, TeamScope};
use crate::config::Config;

/// `system.engine.python_team_env_passthrough` (5.1): further variables (by name) an isolated
/// team's workers and jobs receive from the server's environment.
pub const PASSTHROUGH_KEY: [&str; 3] = ["system", "engine", "python_team_env_passthrough"];

/// What any process needs to run and reach the network.
pub const SYSTEM_NAMES: &[&str] = &[
    // The executable search path, the user, the home directory, the shell.
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "USERNAME",
    "SHELL",
    // Locale and time (and every `LC_*`).
    "LANG",
    "LANGUAGE",
    "TZ",
    "TZDIR",
    // Temporary and per-user directories.
    "TMPDIR",
    "TEMP",
    "TMP",
    "XDG_RUNTIME_DIR",
    "XDG_CONFIG_HOME",
    "XDG_CACHE_HOME",
    "XDG_DATA_HOME",
    // The terminal.
    "TERM",
    "COLORTERM",
    "NO_COLOR",
    "COLUMNS",
    "LINES",
    // Certificate locations.
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    // Proxies, in both cases.
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
    "all_proxy",
    // Native libraries (the Oracle Instant Client, a DuckDB library).
    "LD_LIBRARY_PATH",
    "DYLD_LIBRARY_PATH",
    "DYLD_FALLBACK_LIBRARY_PATH",
    // The interpreter and its virtual environment.
    "PYTHONPATH",
    "PYTHONHOME",
    "PYTHONNOUSERSITE",
    "PYTHONUSERBASE",
    "PYTHONSAFEPATH",
    "PYTHONDONTWRITEBYTECODE",
    "PYTHONPYCACHEPREFIX",
    "PYTHONUNBUFFERED",
    "PYTHONUTF8",
    "PYTHONIOENCODING",
    "PYTHONLEGACYWINDOWSSTDIO",
    "PYTHONWARNINGS",
    "PYTHONFAULTHANDLER",
    "VIRTUAL_ENV",
    "CONDA_PREFIX",
    "CONDA_DEFAULT_ENV",
    // Rust diagnostics.
    "RUST_LOG",
    "RUST_BACKTRACE",
    "RUST_LIB_BACKTRACE",
    // Windows.
    "SYSTEMROOT",
    "SYSTEMDRIVE",
    "WINDIR",
    "COMSPEC",
    "PATHEXT",
    "USERPROFILE",
    "APPDATA",
    "LOCALAPPDATA",
    "PROGRAMDATA",
    "PROGRAMFILES",
    "HOMEDRIVE",
    "HOMEPATH",
    "COMPUTERNAME",
    "NUMBER_OF_PROCESSORS",
    "PROCESSOR_ARCHITECTURE",
];

/// Meerschaum's variables that carry no secret.
pub const MEERSCHAUM_NAMES: &[&str] = &[
    "MRSM_ROOT_DIR",
    "MRSM_PLUGINS_DIR",
    "MRSM_VENVS_DIR",
    "MRSM_WORK_DIR",
    "MRSM_HOME",
    "MRSM_SRC",
    "MRSM_USER",
    "MRSM_UID",
    "MRSM_GID",
    "MRSM_RUNTIME",
    "MRSM_DEP_GROUP",
    "MRSM_SERVER_ID",
    // The interpreter and binary selection.
    "MRSM_PYTHON",
    "MRSM_BIN",
    "MRSM_BIN_EXEC",
    "MRSM_JOB_SUPERVISOR",
    "MRSM_JOB_PROGRAM",
    // The worker protocol.
    "MRSM_WORKER_STARTUP_TIMEOUT",
    "MRSM_WORKER_EXPECT_VERSION",
    "MRSM_WORKER_EXPECT_PROTOCOL",
    "MRSM_WORKER_PURE_MSGPACK",
    "MRSM_WORKER_RELEASE_MEMORY",
    "MRSM_WORKER_ALLOW_CRASH",
    "MRSM_GUARD_WARN",
    "MRSM_GUARD_RETRY_HINT",
    // Switches.
    "MRSM_NO_AUTO_INSTALL",
    "MRSM_NO_UPDATE_CHECK",
    "MRSM_NOASK",
    "MRSM_NONINTERACTIVE",
    "MRSM_DATAFRAME",
    "MRSM_CONNECT_TIMEOUT",
    "MRSM_DUCKDB_LIB",
    "MRSM_CORE_TRACE",
];

/// A job's own context: taken from the job's settings only, never from the launching process.
pub const JOB_NAMES: &[&str] = &[
    "MRSM_DAEMON_ID",
    "MRSM__COMPOSE_PROJECT",
    "MRSM__COMPOSE_ROOT",
];

/// The fingerprint of the team's configuration ([`super::fingerprint`]) in its processes: their
/// Python workers echo it in their handshake, which proves they loaded that configuration.
pub const FINGERPRINT_VAR: &str = "MRSM_TEAM_FINGERPRINT";

/// Variable names compare without case on Windows.
fn same_name(a: &str, b: &str) -> bool {
    if cfg!(windows) {
        a.eq_ignore_ascii_case(b)
    } else {
        a == b
    }
}

fn listed(name: &str, names: &[&str]) -> bool {
    names.iter().any(|n| same_name(n, name))
}

/// Whether a team process receives `name` from the launching process's environment.
pub fn is_allowed(name: &str) -> bool {
    listed(name, SYSTEM_NAMES)
        || listed(name, MEERSCHAUM_NAMES)
        || (name.len() > 3 && same_name(&name[..3], "LC_"))
}

/// Whether a team job receives `name` from its own settings (besides [`is_allowed`] names).
pub fn is_job_name(name: &str) -> bool {
    listed(name, JOB_NAMES)
}

/// Whether `name` may never be passed through: every `MRSM_*` name that is not allowed
/// (`MRSM_CONFIG`, `MRSM_PATCH`, their file forms, `MRSM_CONFIG_DIR`, the team's name, the
/// Compose configuration, any `MRSM_<TYPE>_<LABEL>` connector variable), and a name no
/// environment can hold.
pub fn is_refused(name: &str) -> bool {
    let prefix = &crate::static_config::static_config().environment.prefix;
    name.is_empty()
        || super::sandbox::runtime::injection_name(name)
        || name.contains('=')
        || name.contains('\0')
        || (name.len() >= prefix.len()
            && name.is_char_boundary(prefix.len())
            && name[..prefix.len()].eq_ignore_ascii_case(prefix)
            && !is_allowed(name)
            && !is_job_name(name))
}

/// The names an administrator passes on to team processes ([`PASSTHROUGH_KEY`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Passthrough {
    /// The names passed on.
    pub names: Vec<String>,
    /// The names listed but refused ([`is_refused`]).
    pub refused: Vec<String>,
}

impl Passthrough {
    /// The names listed under [`PASSTHROUGH_KEY`] of `config` (a list of names, or one name).
    pub fn from_config(config: &Config) -> Passthrough {
        use crate::value::Value;
        let listed: Vec<String> = match config.get(&PASSTHROUGH_KEY) {
            Some(Value::List(items)) => items
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.trim().to_string()))
                .collect(),
            Some(Value::Str(name)) => vec![name.trim().to_string()],
            _ => Vec::new(),
        };
        Passthrough::new(listed)
    }

    /// `names`, with the refused ones set apart.
    pub fn new(names: impl IntoIterator<Item = String>) -> Passthrough {
        let mut out = Passthrough::default();
        for name in names {
            if name.is_empty() {
                continue;
            }
            let list = if is_refused(&name) {
                &mut out.refused
            } else {
                &mut out.names
            };
            if !list.contains(&name) {
                list.push(name);
            }
        }
        out
    }

    /// Whether `name` is passed on.
    pub fn contains(&self, name: &str) -> bool {
        self.names.iter().any(|n| same_name(n, name))
    }

    /// Warn (once per process and name) about the refused names.
    pub fn warn_refused(&self) {
        static WARNED: std::sync::Mutex<BTreeSet<String>> = std::sync::Mutex::new(BTreeSet::new());
        let mut warned = WARNED.lock().unwrap_or_else(|e| e.into_inner());
        for name in &self.refused {
            if warned.insert(name.clone()) {
                tracing::warn!(
                    "`{name}` is listed under `system.engine.python_team_env_passthrough` but is \
                     not passed to isolated teams' processes: Meerschaum's configuration, patch \
                     and connector variables never are."
                );
            }
        }
    }
}

/// The environment of a team's process: from `launching` (the launching process's), the allowed
/// variables ([`is_allowed`]) and the passed-through ones; from `job` (a job's own settings, applied
/// after), those and the job's context ([`is_job_name`]); then the team's config directory,
/// configuration file, name and configuration `fingerprint` ([`FINGERPRINT_VAR`]). The process
/// must start from an empty environment.
pub fn team_env(
    launching: &[(String, String)],
    job: &[(String, String)],
    files: &ScopeFiles,
    scope: &TeamScope,
    fingerprint: &str,
    passthrough: &Passthrough,
) -> Vec<(String, String)> {
    let config_file_var = &crate::static_config::static_config()
        .environment
        .config_file;
    let mut out: Vec<(String, String)> = Vec::new();
    let mut put = |name: &str, value: &str| {
        out.retain(|(k, _)| !same_name(k, name));
        out.push((name.to_string(), value.to_string()));
    };
    for (name, value) in launching {
        if !super::sandbox::runtime::injection_name(name)
            && (is_allowed(name) || passthrough.contains(name))
        {
            put(name, value);
        }
    }
    for (name, value) in job {
        if !super::sandbox::runtime::host_runtime_name(name)
            && (is_allowed(name) || is_job_name(name) || passthrough.contains(name))
        {
            put(name, value);
        }
    }
    put(CONFIG_DIR_VAR, &files.config_dir.to_string_lossy());
    put(config_file_var, &files.config_file.to_string_lossy());
    put(TEAM_SCOPE_VAR, &scope.team);
    // What the team's workers echo back in their handshake (`meerschaum_worker`'s check).
    put(FINGERPRINT_VAR, fingerprint);
    out
}

/// Whether a team process may record `name` (a job's `env` property): [`is_allowed`],
/// [`is_job_name`] or passed through.
pub fn may_record(name: &str, passthrough: &Passthrough) -> bool {
    !super::sandbox::runtime::host_runtime_name(name)
        && (is_allowed(name) || is_job_name(name) || passthrough.contains(name))
}
