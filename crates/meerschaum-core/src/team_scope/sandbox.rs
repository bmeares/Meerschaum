//! The filesystem sandbox of an isolated team's processes (5.1).
//!
//! A team's worker (on the API server) and a team's job (its own `mrsm` process) restrict
//! themselves at start-up, before any of the team's code runs, with Linux's Landlock: from then
//! on that process, and everything it starts, may only open the paths of a [`Spec`]. The server
//! writes the spec ([`prepare`]) next to the team's configuration and names it in
//! [`SPEC_VAR`]; the process reads it and calls [`enforce`] (a Python worker through the
//! extension, a job's `mrsm` in `meerschaum-cli`). Nothing is sandboxed when the variable is
//! unset: the server, the shared workers, the API host, administrators' work and the open side.
//!
//! **What a team process may open** ([`build_spec`], [`SYSTEM_RULES`]):
//!
//! - read and execute: the system's programs and libraries (`/usr`, `/lib*`, `/bin`, `/sbin`,
//!   `/opt`, `/nix/store`), the interpreter's prefixes and import path (the worker passes them; a
//!   job's `mrsm` asks its interpreters), Meerschaum's shared virtual environment (`venvs/mrsm`);
//! - read: the name-resolution, time zone, certificate and loader files of `/etc`, the team's
//!   configuration file and directory, the team's plugins (and only those), the team's job
//!   directories, the processor and memory facts of `/proc` and `/sys`, its own `/proc/<pid>`;
//! - list (names only): the plugins symlink directory under `.internal/`;
//! - read and write: the team's temporary directory ([`team_tmp_dir`], `TMPDIR`), its cache
//!   directory (`XDG_CACHE_HOME`), its own virtual environments ([`VENVS_DIR`],
//!   `MRSM_VENVS_DIR`: a team's plugin code installs missing requirements there at run time, as
//!   it always could), a job's work directory ([`JOB_WORK_DIR`]: its result, heartbeat and
//!   start-up release files);
//! - devices: `/dev/null`, `/dev/zero`, `/dev/full`, `/dev/random`, `/dev/urandom`, `/dev/tty`;
//!   `/dev/shm` to create, use and remove shared memory (POSIX semaphores, `multiprocessing`),
//!   but not to list it.
//!
//! Nothing else: not the root's `config/` (every connector's password), its secret files, other
//! teams' configuration, jobs or plugins, the home directory, `~/.ssh`, cloud credentials.
//!
//! **A team writes nothing others load.** A team process has write access to nothing a process
//! outside the team later executes, imports or loads: its virtual environments are its own
//! ([`VENVS_DIR`], seeded from the server's copy of each of its plugins' environments by
//! [`prepare`], never shared), the server's virtual environments, the plugins directories and
//! their links, the bytecode next to shared source and the server's lock files are read-only or
//! out of reach, and the locks of its own environments are kept in its cache directory.
//! A rule that would cover the root, its `config/`, `jobs/`, `team_scopes/`, `venvs/`, a plugins
//! directory or the home directory (a `PYTHONPATH` of `~`, a root under `/opt`) is dropped
//! ([`Spec::protected`]).
//!
//! **Kernel features** ([`Feature`]): the filesystem rules need Landlock ABI 1 (Linux 5.13);
//! newer ABIs add `refer` (2), `truncate` (3), `ioctl_dev` (5), the signal and abstract Unix
//! socket scopes (6: team code can neither signal nor attach to processes outside its sandbox),
//! `all_threads` (8) and `resolve_unix` (9: connecting to a pathname Unix socket such as Docker's
//! is denied too). Each is used when the kernel has it and reported as unavailable otherwise.
//! Outbound TCP and UDP are never restricted: a team reaches its databases and the network.
//!
//! **Levels** ([`level_of`]): where the sandbox is enforced, Landlock covers either the filesystem
//! only ([`LEVEL_FILESYSTEM`], Landlock ABI 1 to 8; before ABI 6 a team's code can still signal
//! its processes) or the filesystem plus Unix-socket and signal scoping ([`LEVEL_SOCKETS`], ABI 9,
//! Linux 7.1). Every report, log line and `doctor` check names the level with these words.
//!
//! **Unix sockets** ([`Report::unix_sockets`], [`unix_sockets_words`]): at [`LEVEL_SOCKETS`]
//! Landlock denies those outside the rules ([`SOCKETS_BY_LANDLOCK`]). Below it, [`enforce`]
//! installs a seccomp filter right after Landlock ([`SOCKETS_BY_SECCOMP`], x86-64 and AArch64):
//! `socket(AF_UNIX, ...)` and datagram `socketpair`s fail with `EAFNOSUPPORT`, `io_uring_setup`
//! with `EPERM` (x32 numbers included), and another architecture's system calls kill the process
//! (the 32-bit `socketcall` entry on x86-64). Connected stream and seqpacket pairs and sockets the
//! process already holds keep working. The process then tries to create a Unix socket
//! ([`SocketProbe`]); a host stops one that reports the filter but could. Where neither holds
//! (another architecture, a container runtime that refuses `seccomp()`), team code can connect to
//! any Unix socket the server's user can, and the reports say so. The API server learns which
//! case it is in from a probe process ([`probe_team_process`], [`cached_probe`]).
//!
//! **Modes** ([`MODE_KEY`]): `auto` (enforce where the kernel supports it, else run without and
//! warn), `require` (never run a team's code without the filesystem rules), `require_sockets`
//! (never run it unless Unix sockets are denied, by Landlock or the seccomp filter, and signals
//! are scoped), `off`.
//!
//! **Proof** ([`Report`]): after restricting itself, a process tries to open a canary file the
//! rules deny ([`CANARY_FILE`]) and reports the result with the ABI and features in use; the
//! host stops a worker whose report fails [`Report::verdict`].

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::{ScopeFiles, TeamScope};
use crate::config::Config;

/// `system.engine.python_team_sandbox` (5.1): `auto`, `require`, `require_sockets` or `off`.
pub const MODE_KEY: [&str; 3] = ["system", "engine", "python_team_sandbox"];
/// Names the spec file of a team process. Set by the server, never inherited from it.
pub const SPEC_VAR: &str = "MRSM_TEAM_SANDBOX";
/// A team's spec in its scope directory (its worker pool's).
pub const SPEC_FILE: &str = "sandbox.json";
/// A team job's spec in the job's directory.
pub const JOB_SPEC_FILE: &str = ".mrsm_sandbox.json";
/// A team job's own writable directory inside its job directory: what the job's command writes
/// (its result, its lap heartbeats, its start-up release). The rest of the job directory
/// (`properties.json`, `process.pid`, which the server trusts) is read-only to the team.
pub const JOB_WORK_DIR: &str = ".mrsm_team_work";
/// The canary every team process must fail to open (`team_scopes/.sandbox_canary`).
pub const CANARY_FILE: &str = ".sandbox_canary";
/// A team's cache directory in its scope directory (`XDG_CACHE_HOME`).
pub const CACHE_DIR: &str = "cache";
/// A team's own virtual environments in its scope directory (`MRSM_VENVS_DIR` of its processes).
/// Nothing outside the team imports from them; the server's environments stay read-only to it.
pub const VENVS_DIR: &str = "venvs";
/// The shared virtual environment of Meerschaum's own dependencies (read-only to a team; linked
/// from the team's [`VENVS_DIR`]).
pub const SHARED_VENV: &str = "mrsm";

/// How isolated teams' processes are sandboxed (`system.engine.python_team_sandbox`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Enforce where the kernel supports it; elsewhere run unsandboxed and warn.
    #[default]
    Auto,
    /// Never run a team's code unless the sandbox's filesystem rules are enforced.
    Require,
    /// Never run a team's code unless Unix sockets are denied to it (Landlock at
    /// [`LEVEL_SOCKETS`], or the seccomp filter) and its signals are scoped (Landlock ABI 6).
    #[serde(rename = "require_sockets")]
    RequireSockets,
    /// No sandbox.
    Off,
}

/// The level of a sandbox that covers the filesystem only (Landlock ABI 1 to 8).
pub const LEVEL_FILESYSTEM: &str = "filesystem only";
/// The level of a sandbox that also scopes Unix sockets and signals (Landlock ABI 9).
pub const LEVEL_SOCKETS: &str = "filesystem and sockets";
/// The first Landlock ABI at [`LEVEL_SOCKETS`].
pub const SOCKETS_ABI: u32 = 9;

/// [`Report::unix_sockets`] when Landlock denies Unix sockets outside the rules (ABI 9).
pub const SOCKETS_BY_LANDLOCK: &str = "landlock";
/// [`Report::unix_sockets`] when the seccomp filter refuses to create Unix sockets (a Landlock
/// that cannot deny them, before Linux 7.1).
pub const SOCKETS_BY_SECCOMP: &str = "seccomp";

/// Whether, and how, Unix sockets are denied to isolated teams' code, in the words every status
/// uses (the server's start-up log, `mrsm doctor`, a worker's handshake report, a team job's
/// first log line): `mechanism` is [`SOCKETS_BY_LANDLOCK`], [`SOCKETS_BY_SECCOMP`] or anything
/// else for "not denied", with `reason` saying why not.
pub fn unix_sockets_words(mechanism: &str, reason: Option<&str>) -> String {
    match mechanism {
        SOCKETS_BY_LANDLOCK => "Unix sockets are denied to team code by Landlock \
                                (Linux 7.1 or newer)"
            .to_string(),
        SOCKETS_BY_SECCOMP => "Unix sockets are denied to team code by a seccomp filter \
                               (this kernel's Landlock cannot deny them)"
            .to_string(),
        _ => {
            let mut text = "Unix sockets are not denied to team code: it can connect to any Unix \
                            socket this server's user can"
                .to_string();
            if let Some(reason) = reason.filter(|r| !r.is_empty()) {
                text.push_str(&format!(" ({reason})"));
            }
            text
        }
    }
}

/// What a process found when it tried to create a Unix socket after installing the seccomp
/// filter (the proof of [`SOCKETS_BY_SECCOMP`], as the canary is of the filesystem rules).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SocketProbe {
    /// Refused (the proof).
    Denied,
    /// Created: the filter does not hold.
    Created,
    /// Not tried (no filter).
    #[default]
    NotChecked,
    /// Another error (no proof).
    Error(String),
}

/// The level an enforced sandbox of Landlock ABI `abi` reaches ([`LEVEL_FILESYSTEM`] or
/// [`LEVEL_SOCKETS`]).
pub fn level_of(abi: u32) -> &'static str {
    if abi >= SOCKETS_ABI {
        LEVEL_SOCKETS
    } else {
        LEVEL_FILESYSTEM
    }
}

impl Mode {
    /// `auto`, `require`, `require_sockets` or `off` (any case; `true` / `false` and `on` read
    /// as `require` / `off`).
    pub fn parse(text: &str) -> Option<Mode> {
        match text.trim().to_ascii_lowercase().as_str() {
            "auto" | "" => Some(Mode::Auto),
            "require" | "required" | "on" | "true" | "enforce" => Some(Mode::Require),
            "require_sockets" | "require-sockets" => Some(Mode::RequireSockets),
            "off" | "false" | "none" | "disabled" => Some(Mode::Off),
            _ => None,
        }
    }

    /// The mode `config` sets ([`MODE_KEY`]); an unknown value is `require` (with a warning, once):
    /// a typo must not turn the sandbox off.
    pub fn from_config(config: &Config) -> Mode {
        Mode::from_value(config.get(&MODE_KEY).as_ref())
    }

    /// The mode the value of [`MODE_KEY`] sets (see [`Mode::from_config`]).
    pub fn from_value(value: Option<&crate::value::Value>) -> Mode {
        use crate::value::Value;
        match value.cloned() {
            None | Some(Value::Null) => Mode::Auto,
            Some(Value::Bool(true)) => Mode::Require,
            Some(Value::Bool(false)) => Mode::Off,
            Some(Value::Str(text)) => Mode::parse(&text).unwrap_or_else(|| {
                warn_once(&format!(
                    "`system.engine.python_team_sandbox` is `{text}`, which is not `auto`, \
                     `require`, `require_sockets` or `off`: isolated teams' processes run as with \
                     `require`."
                ));
                Mode::Require
            }),
            Some(other) => {
                warn_once(&format!(
                    "`system.engine.python_team_sandbox` is `{}`, which is not `auto`, `require`, \
                     `require_sockets` or `off`: isolated teams' processes run as with `require`.",
                    other.py_str()
                ));
                Mode::Require
            }
        }
    }

    /// `auto`, `require`, `require_sockets` or `off`.
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Auto => "auto",
            Mode::Require => "require",
            Mode::RequireSockets => "require_sockets",
            Mode::Off => "off",
        }
    }

    /// Whether this mode refuses to run a team's code without an enforced sandbox.
    pub fn requires(self) -> bool {
        matches!(self, Mode::Require | Mode::RequireSockets)
    }

    /// The stricter of two modes (`off` < `auto` < `require` < `require_sockets`).
    pub fn stricter(self, other: Mode) -> Mode {
        let rank = |m: Mode| match m {
            Mode::Off => 0,
            Mode::Auto => 1,
            Mode::Require => 2,
            Mode::RequireSockets => 3,
        };
        if rank(other) > rank(self) {
            other
        } else {
            self
        }
    }
}

fn warn_once(message: &str) {
    static WARNED: std::sync::Mutex<BTreeSet<String>> = std::sync::Mutex::new(BTreeSet::new());
    if WARNED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(message.to_string())
    {
        tracing::warn!("{message}");
    }
}

/// What a rule allows beneath its path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Access {
    /// Read files, list directories and execute.
    ReadExec,
    /// Read files and list directories.
    Read,
    /// List directories (names only).
    List,
    /// Everything but creating device nodes: read, write, create, remove, rename, execute,
    /// truncate, bind and connect Unix sockets.
    ReadWrite,
    /// Read, write and truncate (and `ioctl`) a device file.
    Device,
    /// Create, read, write and remove files without listing (`/dev/shm`).
    SharedMemory,
    /// Connect to a pathname Unix socket.
    Connect,
    /// Open an existing file for reading and writing (a lock file another process shares).
    Lock,
    /// Read, write and truncate an existing file (a file database of the team's).
    DataFile,
}

/// One path and what may be done beneath it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    /// The path (a directory covers everything beneath it). `/proc/self` is the enforcing
    /// process's own entry.
    pub path: PathBuf,
    /// What is allowed.
    pub access: Access,
    /// Why (for reports).
    #[serde(default)]
    pub why: String,
}

impl Rule {
    /// A rule.
    pub fn new(path: impl Into<PathBuf>, access: Access, why: &str) -> Rule {
        Rule {
            path: path.into(),
            access,
            why: why.to_string(),
        }
    }
}

/// What the system rules allow, by path (only those that exist are added).
pub const SYSTEM_RULES: &[(&str, Access, &str)] = &[
    // Programs and libraries.
    ("/usr", Access::ReadExec, "programs and libraries"),
    ("/lib", Access::ReadExec, "libraries"),
    ("/lib32", Access::ReadExec, "libraries"),
    ("/lib64", Access::ReadExec, "libraries"),
    ("/libx32", Access::ReadExec, "libraries"),
    ("/bin", Access::ReadExec, "programs"),
    ("/sbin", Access::ReadExec, "programs"),
    (
        "/opt",
        Access::ReadExec,
        "add-on software (database clients)",
    ),
    ("/nix/store", Access::ReadExec, "Nix packages"),
    // The loader, name resolution, users, time zones, certificates, locales.
    ("/etc/ld.so.cache", Access::Read, "the loader"),
    ("/etc/ld.so.conf", Access::Read, "the loader"),
    ("/etc/ld.so.conf.d", Access::Read, "the loader"),
    ("/etc/hosts", Access::Read, "name resolution"),
    ("/etc/resolv.conf", Access::Read, "name resolution"),
    ("/etc/nsswitch.conf", Access::Read, "name resolution"),
    ("/etc/host.conf", Access::Read, "name resolution"),
    ("/etc/gai.conf", Access::Read, "name resolution"),
    ("/etc/services", Access::Read, "name resolution"),
    ("/etc/protocols", Access::Read, "name resolution"),
    ("/etc/networks", Access::Read, "name resolution"),
    (
        "/run/systemd/resolve/resolv.conf",
        Access::Read,
        "name resolution",
    ),
    (
        "/run/systemd/resolve/stub-resolv.conf",
        Access::Read,
        "name resolution",
    ),
    (
        "/run/systemd/resolve/io.systemd.Resolve",
        Access::Connect,
        "name resolution",
    ),
    (
        "/run/resolvconf/resolv.conf",
        Access::Read,
        "name resolution",
    ),
    (
        "/run/NetworkManager/resolv.conf",
        Access::Read,
        "name resolution",
    ),
    (
        "/run/NetworkManager/no-stub-resolv.conf",
        Access::Read,
        "name resolution",
    ),
    ("/run/nscd/socket", Access::Connect, "name resolution"),
    ("/var/run/nscd/socket", Access::Connect, "name resolution"),
    ("/etc/passwd", Access::Read, "user names"),
    ("/etc/group", Access::Read, "group names"),
    ("/etc/localtime", Access::Read, "the time zone"),
    ("/etc/timezone", Access::Read, "the time zone"),
    ("/etc/ssl", Access::Read, "certificates"),
    ("/etc/pki", Access::Read, "certificates"),
    ("/etc/ca-certificates", Access::Read, "certificates"),
    ("/etc/ca-certificates.conf", Access::Read, "certificates"),
    ("/etc/crypto-policies", Access::Read, "TLS settings"),
    ("/etc/alternatives", Access::Read, "program alternatives"),
    ("/etc/mime.types", Access::Read, "file types"),
    ("/etc/os-release", Access::Read, "the platform"),
    ("/etc/lsb-release", Access::Read, "the platform"),
    ("/etc/debian_version", Access::Read, "the platform"),
    ("/etc/redhat-release", Access::Read, "the platform"),
    ("/etc/fedora-release", Access::Read, "the platform"),
    ("/etc/system-release", Access::Read, "the platform"),
    ("/etc/terminfo", Access::Read, "terminals"),
    ("/etc/fonts", Access::Read, "fonts (plotting)"),
    (
        "/etc/odbcinst.ini",
        Access::Read,
        "ODBC drivers (SQL Server)",
    ),
    // Devices.
    ("/dev/null", Access::Device, "a device"),
    ("/dev/zero", Access::Device, "a device"),
    ("/dev/full", Access::Device, "a device"),
    ("/dev/random", Access::Device, "randomness"),
    ("/dev/urandom", Access::Device, "randomness"),
    ("/dev/tty", Access::Device, "the terminal"),
    (
        "/dev/shm",
        Access::SharedMemory,
        "shared memory (semaphores, multiprocessing)",
    ),
    // Processor, memory and limits.
    ("/proc/self", Access::Read, "the process's own entry"),
    ("/proc/cpuinfo", Access::Read, "processor facts"),
    ("/proc/meminfo", Access::Read, "memory facts"),
    ("/proc/stat", Access::Read, "processor facts"),
    ("/proc/loadavg", Access::Read, "load"),
    ("/proc/uptime", Access::Read, "uptime"),
    ("/proc/version", Access::Read, "the kernel"),
    ("/proc/filesystems", Access::Read, "file systems"),
    ("/proc/sys", Access::Read, "kernel settings (vm, kernel)"),
    ("/sys/devices/system/cpu", Access::Read, "processor facts"),
    ("/sys/devices/system/node", Access::Read, "memory nodes"),
    ("/sys/fs/cgroup", Access::Read, "resource limits"),
    (
        "/sys/kernel/mm/transparent_hugepage",
        Access::Read,
        "memory settings",
    ),
];

/// The rules a team process is started with.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Spec {
    /// The mode it was written for.
    pub mode: Mode,
    /// The team.
    pub team: String,
    /// The paths and what may be done beneath them.
    pub rules: Vec<Rule>,
    /// Paths no rule may cover (a rule on one of them or on an ancestor is dropped): the root,
    /// its `config/`, `jobs/`, `team_scopes/` and `venvs/`, the plugins directories, the home
    /// directory, `/`.
    pub protected: Vec<PathBuf>,
    /// The file the process must fail to open.
    pub canary: PathBuf,
    /// The newest Landlock ABI to use (`None`: the kernel's, up to [`NEWEST_ABI`]). Meerschaum
    /// never sets it; tests use it to run a process at an older kernel's level.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_abi: Option<u32>,
}

/// What [`build_spec`] needs.
#[derive(Debug, Clone)]
pub struct SpecInputs<'a> {
    /// The mode.
    pub mode: Mode,
    /// The team's name.
    pub team: &'a str,
    /// The server's root directory.
    pub root: &'a Path,
    /// The home directory.
    pub home: Option<&'a Path>,
    /// The plugins directories.
    pub plugins_dirs: &'a [PathBuf],
    /// The server's virtual environments directory (only its `mrsm` environment is readable).
    pub venvs_dir: &'a Path,
    /// The team's own virtual environments directory (read and write).
    pub team_venvs_dir: &'a Path,
    /// The team's plugins.
    pub plugins: &'a BTreeSet<String>,
    /// The team's own plugins directory (links to its plugins), if any.
    pub plugins_view: Option<&'a Path>,
    /// The team's configuration files.
    pub files: &'a ScopeFiles,
    /// The spec file itself (read by processes the team's processes start).
    pub spec_file: &'a Path,
    /// The team's temporary directory.
    pub tmp_dir: &'a Path,
    /// The team's cache directory.
    pub cache_dir: &'a Path,
    /// The team's job directories (read-only).
    pub job_dirs: &'a [PathBuf],
    /// A job's work directory (read and write).
    pub work_dir: Option<&'a Path>,
}

/// Whether `name` may name a plugin's file (no separators, no parent references, no hidden
/// names).
fn plain_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

/// The rules of a team process ([`SYSTEM_RULES`] and the team's own), without the
/// interpreter's paths (a worker adds those at [`enforce`]).
pub fn build_spec(inputs: &SpecInputs<'_>) -> Spec {
    let mut rules: Vec<Rule> = SYSTEM_RULES
        .iter()
        .map(|(path, access, why)| Rule::new(*path, *access, why))
        .collect();
    let root = inputs.root;
    rules.push(Rule::new(
        &inputs.files.config_file,
        Access::Read,
        "the team's configuration",
    ));
    rules.push(Rule::new(
        &inputs.files.config_dir,
        Access::Read,
        "the team's configuration directory",
    ));
    rules.push(Rule::new(inputs.spec_file, Access::Read, "these rules"));
    for plugin in inputs.plugins.iter().filter(|p| plain_name(p)) {
        for dir in inputs.plugins_dirs {
            for name in [plugin.clone(), format!("{plugin}.py")] {
                rules.push(Rule::new(dir.join(name), Access::Read, "the team's plugin"));
            }
        }
    }
    // Its own virtual environments only: the server's environment of a plugin is also imported
    // by the server, the open side and every other team granted the plugin.
    rules.push(Rule::new(
        inputs.team_venvs_dir,
        Access::ReadWrite,
        "the team's own virtual environments (requirements installed at run time)",
    ));
    if let Some(view) = inputs.plugins_view {
        rules.push(Rule::new(
            view,
            Access::Read,
            "the team's plugins directory (links)",
        ));
    }
    let internal = root.join(".internal");
    rules.push(Rule::new(
        &internal,
        Access::List,
        "the import path of the plugins package (names only)",
    ));
    rules.push(Rule::new(
        internal.join("plugins").join("__init__.py"),
        Access::Read,
        "the plugins package",
    ));
    rules.push(Rule::new(
        inputs.venvs_dir.join(SHARED_VENV),
        Access::ReadExec,
        "Meerschaum's shared virtual environment",
    ));
    rules.push(Rule::new(
        inputs.tmp_dir,
        Access::ReadWrite,
        "the team's temporary directory",
    ));
    rules.push(Rule::new(
        inputs.cache_dir,
        Access::ReadWrite,
        "the team's cache directory",
    ));
    for dir in inputs.job_dirs {
        rules.push(Rule::new(dir, Access::Read, "the team's job"));
    }
    if let Some(dir) = inputs.work_dir {
        rules.push(Rule::new(
            dir,
            Access::ReadWrite,
            "the job's work directory",
        ));
    }
    let mut protected = vec![
        PathBuf::from("/"),
        root.to_path_buf(),
        root.join("config"),
        root.join("jobs"),
        root.join(super::SCOPES_DIR),
        inputs.venvs_dir.to_path_buf(),
    ];
    protected.extend(inputs.plugins_dirs.iter().cloned());
    if let Some(home) = inputs.home {
        protected.push(home.to_path_buf());
    }
    let mut spec = Spec {
        mode: inputs.mode,
        team: inputs.team.to_string(),
        rules,
        protected,
        canary: canary_path(root),
        max_abi: None,
    };
    spec.rules = spec.allowed(spec.rules.clone()).0;
    spec
}

/// Copy the server's virtual environment `shared` of a plugin to the team's own `private` (5.1),
/// unless `private` exists: the team then starts with what the server installed, and what its
/// code installs at run time lands in its copy only. Never links a file (a hard link would let
/// the team write the server's copy); the file system may share blocks copy-on-write. Bytecode
/// caches are left out, and a symbolic link into `shared` points into the copy. The copy is
/// made beside `private` and renamed into place, so a team process never sees half of one.
/// `Ok(false)`: nothing to copy (no `shared`, or `private` exists).
pub fn seed_team_venv(shared: &Path, private: &Path) -> std::io::Result<bool> {
    if !shared.is_dir() || std::fs::symlink_metadata(private).is_ok() {
        return Ok(false);
    }
    let parent = private.parent().unwrap_or(Path::new("."));
    let name = private
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let staging = parent.join(format!(
        ".{name}.seed-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    ));
    let copied = copy_tree(shared, &staging, shared, private);
    let placed = copied.and_then(|()| std::fs::rename(&staging, private));
    if placed.is_err() {
        let _ = std::fs::remove_dir_all(&staging);
        // Another of the team's processes placed its copy first.
        if private.is_dir() {
            return Ok(false);
        }
    }
    placed.map(|()| true)
}

/// Copy the directory `from` to `to` (see [`seed_team_venv`]): files by content, links as links
/// (an absolute link under `shared` pointing under `private` instead), `__pycache__` skipped.
fn copy_tree(from: &Path, to: &Path, shared: &Path, private: &Path) -> std::io::Result<()> {
    std::fs::create_dir(to)?;
    if let Ok(metadata) = std::fs::metadata(from) {
        let _ = std::fs::set_permissions(to, metadata.permissions());
    }
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == "__pycache__" {
            continue;
        }
        let source = entry.path();
        let target = to.join(&name);
        let kind = entry.file_type()?;
        if kind.is_symlink() {
            let mut link = std::fs::read_link(&source)?;
            if let Ok(inside) = link.strip_prefix(shared) {
                link = private.join(inside);
            }
            #[cfg(unix)]
            std::os::unix::fs::symlink(&link, &target)?;
            #[cfg(not(unix))]
            let _ = (&link, &target);
        } else if kind.is_dir() {
            copy_tree(&source, &target, shared, private)?;
        } else if kind.is_file() {
            std::fs::copy(&source, &target)?;
        }
    }
    Ok(())
}

/// `path` made absolute and without `.` / `..` components (no file system access).
pub fn lexical(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

impl Spec {
    /// `rules` without those that would cover a protected path (`.0`), and the dropped ones
    /// (`.1`). A relative path is dropped too.
    pub fn allowed(&self, rules: Vec<Rule>) -> (Vec<Rule>, Vec<Rule>) {
        let protected: Vec<PathBuf> = self.protected.iter().map(|p| lexical(p)).collect();
        let mut kept = Vec::new();
        let mut dropped = Vec::new();
        for rule in rules {
            let path = lexical(&rule.path);
            let covers = !rule.path.is_absolute() || protected.iter().any(|p| p.starts_with(&path));
            if covers {
                dropped.push(rule);
            } else {
                kept.push(rule);
            }
        }
        (kept, dropped)
    }

    /// Read a spec file.
    pub fn read(path: &Path) -> std::io::Result<Spec> {
        let text = std::fs::read_to_string(path)?;
        serde_json::from_str(&text).map_err(std::io::Error::other)
    }
}

/// The canary of the root `root`.
pub fn canary_path(root: &Path) -> PathBuf {
    root.join(super::SCOPES_DIR).join(CANARY_FILE)
}

/// Create the canary under `root` (owner-only; it holds no secret).
pub fn ensure_canary(root: &Path) -> std::io::Result<PathBuf> {
    let path = canary_path(root);
    if !path.is_file() {
        if let Some(dir) = path.parent() {
            super::create_private_dir(dir)?;
        }
        super::write_private(
            &path,
            "A file isolated teams' processes must not be able to read (Meerschaum's sandbox canary).\n",
        )?;
    }
    Ok(path)
}

/// One part of the sandbox, by the Landlock ABI that introduced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Feature {
    /// Its name in reports.
    pub name: &'static str,
    /// The first ABI with it.
    pub abi: u32,
    /// What it stops.
    pub what: &'static str,
}

/// The parts of the sandbox Meerschaum uses (ABI 4's TCP rules and ABI 7's audit switches are
/// deliberately not: a team's network reach is not restricted).
pub const FEATURES: &[Feature] = &[
    Feature {
        name: "filesystem",
        abi: 1,
        what: "opening, listing, executing, creating and removing paths outside the rules",
    },
    Feature {
        name: "refer",
        abi: 2,
        what: "linking or renaming files between directories outside the rules",
    },
    Feature {
        name: "truncate",
        abi: 3,
        what: "truncating files outside the rules",
    },
    Feature {
        name: "ioctl_dev",
        abi: 5,
        what: "device ioctls outside the rules",
    },
    Feature {
        name: "scope_signal",
        abi: 6,
        what: "signalling processes outside the sandbox",
    },
    Feature {
        name: "scope_abstract_unix",
        abi: 6,
        what: "connecting to abstract Unix sockets outside the sandbox",
    },
    Feature {
        name: "all_threads",
        abi: 8,
        what: "threads started before the sandbox staying unrestricted",
    },
    Feature {
        name: "resolve_unix",
        abi: 9,
        what: "connecting to pathname Unix sockets (such as Docker's) outside the rules",
    },
];

/// The newest Landlock ABI Meerschaum knows.
pub const NEWEST_ABI: u32 = 9;

/// The features of ABI `abi` (`.0`) and those it lacks (`.1`).
pub fn features_of(abi: u32) -> (Vec<String>, Vec<String>) {
    let (have, lack): (Vec<&Feature>, Vec<&Feature>) = FEATURES.iter().partition(|f| f.abi <= abi);
    (
        have.iter().map(|f| f.name.to_string()).collect(),
        lack.iter().map(|f| f.name.to_string()).collect(),
    )
}

/// Whether this system can sandbox a process (asked without restricting anything).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Support {
    /// Landlock is available: the kernel's ABI.
    Available {
        /// The kernel's Landlock ABI.
        kernel_abi: u32,
    },
    /// It is not, and why.
    Unsupported {
        /// Why.
        reason: String,
    },
}

impl Support {
    /// The ABI Meerschaum uses (the kernel's, at most [`NEWEST_ABI`]); `0` when unsupported.
    pub fn abi(&self) -> u32 {
        match self {
            Support::Available { kernel_abi } => (*kernel_abi).min(NEWEST_ABI),
            Support::Unsupported { .. } => 0,
        }
    }

    /// Whether Landlock is available.
    pub fn available(&self) -> bool {
        matches!(self, Support::Available { .. })
    }

    /// The level a sandbox enforced here reaches ([`level_of`]); `None` when unsupported.
    pub fn level(&self) -> Option<&'static str> {
        self.available().then(|| level_of(self.abi()))
    }

    /// One line: `Landlock ABI 9 (kernel 11): filesystem, refer, ...; unavailable: ...`, or why
    /// it is unsupported.
    pub fn describe(&self) -> String {
        match self {
            Support::Available { kernel_abi } => {
                let abi = self.abi();
                let (have, lack) = features_of(abi);
                let kernel = if *kernel_abi != abi {
                    format!(" (the kernel offers ABI {kernel_abi})")
                } else {
                    String::new()
                };
                let mut text = format!("Landlock ABI {abi}{kernel}: {}", have.join(", "));
                if !lack.is_empty() {
                    text.push_str(&format!("; unavailable: {}", lack.join(", ")));
                }
                text
            }
            Support::Unsupported { reason } => format!("Landlock unavailable: {reason}"),
        }
    }
}

/// Whether Landlock itself refuses isolated teams' sandboxed processes connections to pathname
/// Unix sockets outside their rules (5.1): the sandbox is not off and the kernel has Landlock ABI
/// 9 (`resolve_unix`, Linux 7.1). Before, the seccomp filter may deny Unix sockets instead, which
/// only a process that applies it can tell ([`probe_team_process`], [`cached_probe`]).
pub fn landlock_stops_unix_sockets(mode: Mode, support: &Support) -> bool {
    mode != Mode::Off && support.abi() >= 9
}

/// Whether this system can sandbox a process: the kernel's Landlock ABI, asked without
/// restricting anything (`landlock_create_ruleset(NULL, 0, LANDLOCK_CREATE_RULESET_VERSION)`).
/// A kernel without Landlock, one where it is not enabled at boot, and a container runtime whose
/// seccomp profile denies its system calls are all unsupported.
pub fn kernel_support() -> Support {
    imp::kernel_support()
}

/// What a process found when it tried to open the canary.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Canary {
    /// Refused (the proof).
    Denied,
    /// Opened: the sandbox does not hold.
    Readable,
    /// The canary does not exist (no proof).
    Missing,
    /// Not tried (no sandbox).
    #[default]
    NotChecked,
    /// Another error (no proof).
    Error(String),
}

/// Try to open `path` for reading.
pub fn probe_canary(path: &Path) -> Canary {
    match std::fs::File::open(path) {
        Ok(_) => Canary::Readable,
        Err(e) => match e.raw_os_error() {
            Some(code) if code == EACCES || code == EPERM => Canary::Denied,
            _ if e.kind() == std::io::ErrorKind::NotFound => Canary::Missing,
            _ => Canary::Error(e.to_string()),
        },
    }
}

const EACCES: i32 = 13;
const EPERM: i32 = 1;

/// What a team process reports about its sandbox (in its handshake, its job log, `doctor`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Report {
    /// The mode it ran in.
    pub mode: Mode,
    /// Whether the process restricted itself.
    pub enforced: bool,
    /// The ABI Meerschaum used.
    #[serde(default)]
    pub abi: u32,
    /// The kernel's ABI.
    #[serde(default)]
    pub kernel_abi: u32,
    /// The features in force.
    #[serde(default)]
    pub features: Vec<String>,
    /// The features the kernel lacks.
    #[serde(default)]
    pub unavailable: Vec<String>,
    /// The canary's result.
    #[serde(default)]
    pub canary: Canary,
    /// Why it is not enforced (or a caveat).
    #[serde(default)]
    pub reason: Option<String>,
    /// The rules added.
    #[serde(default)]
    pub rules: usize,
    /// Rules dropped because they would cover a protected path.
    #[serde(default)]
    pub dropped: Vec<String>,
    /// The level in force ([`LEVEL_FILESYSTEM`] or [`LEVEL_SOCKETS`]; empty when not enforced).
    #[serde(default)]
    pub level: String,
    /// How Unix sockets are denied to the process: [`SOCKETS_BY_LANDLOCK`],
    /// [`SOCKETS_BY_SECCOMP`], or empty (not denied; [`Report::sockets_reason`] says why).
    #[serde(default)]
    pub unix_sockets: String,
    /// The proof of [`SOCKETS_BY_SECCOMP`]: creating a Unix socket after the filter.
    #[serde(default)]
    pub socket_probe: SocketProbe,
    /// Why Unix sockets are not denied (no filter for this architecture, a container runtime
    /// refused it).
    #[serde(default)]
    pub sockets_reason: Option<String>,
}

impl Report {
    /// A process that was not sandboxed, and why.
    pub fn not_enforced(mode: Mode, reason: impl Into<String>) -> Report {
        Report {
            mode,
            reason: Some(reason.into()),
            ..Report::default()
        }
    }

    /// `Err` (why) when a process with this report must not run the team's code: in `require`
    /// mode unless the sandbox is enforced and the canary was denied; in `auto` mode when the
    /// sandbox claims to be enforced but the canary was not denied.
    pub fn verdict(&self) -> Result<(), String> {
        match self.mode {
            Mode::Off => Ok(()),
            Mode::Auto if !self.enforced => Ok(()),
            mode if mode.requires() && !self.enforced => Err(format!(
                "the filesystem sandbox is required (`system.engine.python_team_sandbox: {}`) \
                 but is not enforced: {}",
                mode.as_str(),
                self.reason.as_deref().unwrap_or("no reason given")
            )),
            Mode::RequireSockets if !self.covers_sockets_and_signals() => Err(format!(
                "the sandbox must deny Unix sockets and signals to team code \
                 (`system.engine.python_team_sandbox: require_sockets`), but here: {}{}",
                self.sockets_words(),
                if self.denies_unix_sockets() {
                    format!(
                        " and this kernel's Landlock ABI {} cannot scope signals (that needs \
                         ABI 6, Linux 6.12)",
                        self.abi
                    )
                } else {
                    String::new()
                }
            )),
            _ if self.unix_sockets == SOCKETS_BY_SECCOMP
                && self.socket_probe != SocketProbe::Denied =>
            {
                Err(format!(
                    "the sandbox reports Unix sockets denied by its seccomp filter, but the \
                     process could still create one ({})",
                    match &self.socket_probe {
                        SocketProbe::Created => "it was created".to_string(),
                        SocketProbe::NotChecked => "not checked".to_string(),
                        SocketProbe::Error(e) => e.clone(),
                        SocketProbe::Denied => String::new(),
                    }
                ))
            }
            _ => match &self.canary {
                Canary::Denied => Ok(()),
                Canary::Readable => Err(
                    "the filesystem sandbox reports itself enforced, but the process could still \
                     read the canary file it must not reach"
                        .to_string(),
                ),
                Canary::Missing => Err(
                    "the filesystem sandbox could not be proven: the canary file does not exist"
                        .to_string(),
                ),
                Canary::NotChecked => {
                    Err("the filesystem sandbox was not checked against its canary".to_string())
                }
                Canary::Error(e) => Err(format!(
                    "the filesystem sandbox could not be proven: opening the canary failed with \
                     {e}"
                )),
            },
        }
    }

    /// Whether Unix sockets are denied to the process (by Landlock or the seccomp filter).
    pub fn denies_unix_sockets(&self) -> bool {
        self.enforced
            && (self.unix_sockets == SOCKETS_BY_LANDLOCK
                || self.unix_sockets == SOCKETS_BY_SECCOMP
                || (self.unix_sockets.is_empty() && self.level() == LEVEL_SOCKETS))
    }

    /// What `require_sockets` asks for: Unix sockets denied (either mechanism) and signals
    /// scoped (Landlock ABI 6).
    pub fn covers_sockets_and_signals(&self) -> bool {
        self.denies_unix_sockets()
            && (self.level() == LEVEL_SOCKETS || self.features.iter().any(|f| f == "scope_signal"))
    }

    /// [`unix_sockets_words`] for this report.
    pub fn sockets_words(&self) -> String {
        let mechanism = if self.denies_unix_sockets() && self.unix_sockets.is_empty() {
            SOCKETS_BY_LANDLOCK
        } else if self.enforced {
            self.unix_sockets.as_str()
        } else {
            ""
        };
        let reason = if self.enforced {
            self.sockets_reason.as_deref()
        } else {
            Some("the sandbox is not enforced")
        };
        unix_sockets_words(mechanism, reason)
    }

    /// The level in force: [`Report::level`] as recorded, else from the features (a report of an
    /// older package); empty when not enforced.
    pub fn level(&self) -> &str {
        if !self.enforced {
            return "";
        }
        if !self.level.is_empty() {
            return &self.level;
        }
        let has = |f: &str| self.features.iter().any(|x| x == f);
        if has("resolve_unix") && has("scope_signal") && has("scope_abstract_unix") {
            LEVEL_SOCKETS
        } else {
            LEVEL_FILESYSTEM
        }
    }

    /// One line for logs and `doctor`.
    pub fn describe(&self) -> String {
        if !self.enforced {
            return format!(
                "not enforced ({}): {}; {}",
                self.mode.as_str(),
                self.reason.as_deref().unwrap_or("off"),
                self.sockets_words()
            );
        }
        let kernel = if self.kernel_abi != self.abi {
            format!(" (kernel ABI {})", self.kernel_abi)
        } else {
            String::new()
        };
        let mut text = format!(
            "enforced, {} ({}): Landlock ABI {}{kernel}, {} rules: {}",
            self.level(),
            self.mode.as_str(),
            self.abi,
            self.rules,
            self.features.join(", ")
        );
        if !self.unavailable.is_empty() {
            text.push_str(&format!("; unavailable: {}", self.unavailable.join(", ")));
        }
        text.push_str(match &self.canary {
            Canary::Denied => "; canary denied",
            Canary::Readable => "; CANARY READABLE",
            _ => "; canary not proven",
        });
        text.push_str(&format!("; {}", self.sockets_words()));
        if let Some(reason) = &self.reason {
            text.push_str(&format!("; {reason}"));
        }
        text
    }
}

/// The report of this process's sandbox, once [`enforce`] ran.
static ENFORCED: std::sync::OnceLock<Report> = std::sync::OnceLock::new();

/// This process's report (`None` until it enforced its spec).
pub fn current() -> Option<Report> {
    ENFORCED.get().cloned()
}

/// Restrict this process (and everything it starts) to `spec`'s rules plus `extra` (the
/// interpreter's paths; protected paths dropped), then try the canary. Irreversible: call it
/// in a process started for the team, before any of the team's code runs. Once per process:
/// a second call returns the first report. Never fails: a sandbox that could not be applied
/// is a report that is not enforced, which [`Report::verdict`] judges by mode.
pub fn enforce(spec: &Spec, extra: &[Rule]) -> Report {
    if let Some(report) = ENFORCED.get() {
        return report.clone();
    }
    let report = if spec.mode == Mode::Off {
        Report::not_enforced(Mode::Off, "off")
    } else {
        let mut rules = spec.rules.clone();
        rules.extend(extra.iter().cloned());
        let (rules, dropped) = spec.allowed(rules);
        let mut report = imp::restrict(spec.mode, &rules, spec.max_abi);
        report.dropped = dropped
            .iter()
            .map(|r| r.path.display().to_string())
            .collect();
        if report.enforced {
            report.canary = probe_canary(&spec.canary);
            report.level = level_of(report.abi).to_string();
            // Unix sockets: Landlock denies those outside the rules from ABI 9; before, a
            // seccomp filter refuses to create any (installed after Landlock, which set
            // `no_new_privs`, and before any of the team's code runs).
            if report.abi >= SOCKETS_ABI {
                report.unix_sockets = SOCKETS_BY_LANDLOCK.to_string();
            } else {
                imp::deny_unix_sockets(&mut report);
            }
        }
        report
    };
    ENFORCED.get_or_init(|| report).clone()
}

/// [`enforce`] the spec [`SPEC_VAR`] names; `None` when it is unset (not a sandboxed team
/// process). A spec that cannot be read is a report that is not enforced, in `require` mode
/// (the server asked for a sandbox).
pub fn enforce_from_env(extra: &[Rule]) -> Option<Report> {
    if let Some(report) = ENFORCED.get() {
        return Some(report.clone());
    }
    let path = std::env::var_os(SPEC_VAR).filter(|v| !v.is_empty())?;
    match Spec::read(Path::new(&path)) {
        Ok(spec) => Some(enforce(&spec, extra)),
        Err(e) => Some(
            ENFORCED
                .get_or_init(|| {
                    Report::not_enforced(
                        Mode::Require,
                        format!(
                            "the sandbox's rules `{}` could not be read: {e}",
                            Path::new(&path).display()
                        ),
                    )
                })
                .clone(),
        ),
    }
}

/// The first argument of the sandbox probe: this program started as
/// `<program> __mrsm_team_sandbox_probe__ <mode> <canary> [<max ABI>]` applies a team process's
/// sandbox to itself and prints its [`Report`] as JSON (see [`probe_team_process`]).
pub const PROBE_ARG: &str = "__mrsm_team_sandbox_probe__";

/// The probe's side ([`PROBE_ARG`]): `Some(exit code)` when `args` asks for it. Restricts this
/// process (irreversibly: hosts call it first thing, in the process started for it) with no
/// rules but `canary`, as a team process would at `mode` (and at most `max ABI`), then prints the
/// report on one line.
pub fn probe_if_requested(args: &[String]) -> Option<i32> {
    if args.first().map(String::as_str) != Some(PROBE_ARG) {
        return None;
    }
    let mode = args
        .get(1)
        .and_then(|m| Mode::parse(m))
        .unwrap_or(Mode::Require);
    let Some(canary) = args.get(2) else {
        eprintln!("The sandbox probe needs a canary file.");
        return Some(2);
    };
    let spec = Spec {
        mode,
        team: "probe".into(),
        canary: PathBuf::from(canary),
        max_abi: args.get(3).and_then(|a| a.parse().ok()),
        ..Spec::default()
    };
    let report = enforce(&spec, &[]);
    println!("{}", serde_json::to_string(&report).unwrap_or_default());
    Some(0)
}

/// What a team process would enforce here, found by starting this program as the probe
/// ([`PROBE_ARG`], [`crate::self_exe::self_command`]) with a fresh canary file: the server asks
/// before it lets the web terminal use tmux (an isolated team that may install plugins, on a
/// system where neither Landlock nor the seccomp filter denies Unix sockets, could drive it).
/// `max_abi` caps Landlock's ABI (tests). `Err` when the probe could not run or answer.
pub fn probe_team_process(mode: Mode, max_abi: Option<u32>) -> Result<Report, String> {
    if mode == Mode::Off {
        return Ok(Report::not_enforced(Mode::Off, "off"));
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let canary =
        std::env::temp_dir().join(format!("mrsm-sandbox-probe-{}-{nanos}", std::process::id()));
    std::fs::write(&canary, b"canary").map_err(|e| format!("cannot write the canary: {e}"))?;
    let result = (|| {
        let command = crate::self_exe::self_command();
        let (program, leading) = command
            .split_first()
            .ok_or_else(|| "this program's command is unknown".to_string())?;
        let mut cmd = std::process::Command::new(program);
        cmd.args(leading)
            .arg(PROBE_ARG)
            .arg(mode.as_str())
            .arg(&canary)
            .env_remove(SPEC_VAR)
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if let Some(abi) = max_abi {
            cmd.arg(abi.to_string());
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| format!("cannot start `{program}`: {e}"))?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                _ => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err("the probe did not answer in 20 s".to_string());
                }
            }
        }
        let out = child.wait_with_output().map_err(|e| e.to_string())?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        stdout
            .lines()
            .rev()
            .find_map(|line| serde_json::from_str::<Report>(line.trim()).ok())
            .ok_or_else(|| {
                format!(
                    "the probe gave no report ({}): {}",
                    out.status,
                    String::from_utf8_lossy(&out.stderr).trim()
                )
            })
    })();
    let _ = std::fs::remove_file(&canary);
    result
}

/// [`probe_team_process`] once per mode and state of the isolated teams recorded under `root`
/// (the probe runs again after a team change): a failed probe counts as a sandbox that denies
/// nothing.
pub fn cached_probe(mode: Mode, root: &Path) -> Report {
    type Cached = (Mode, PathBuf, Option<std::time::SystemTime>, Report);
    static CACHE: std::sync::Mutex<Option<Cached>> = std::sync::Mutex::new(None);
    let stamp = std::fs::metadata(root.join(super::SCOPES_DIR).join(super::INDEX_FILE))
        .and_then(|m| m.modified())
        .ok();
    let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((m, r, t, report)) = cache.as_ref()
        && *m == mode
        && r == root
        && *t == stamp
    {
        return report.clone();
    }
    let report = probe_team_process(mode, None).unwrap_or_else(|why| {
        Report::not_enforced(mode, format!("the sandbox probe failed: {why}"))
    });
    *cache = Some((mode, root.to_path_buf(), stamp, report.clone()));
    report
}

/// The mode of the spec [`SPEC_VAR`] names, without enforcing it (`None`: not a sandboxed team
/// process; an unreadable spec is `require`).
pub fn mode_from_env() -> Option<Mode> {
    let path = std::env::var_os(SPEC_VAR).filter(|v| !v.is_empty())?;
    Some(
        Spec::read(Path::new(&path))
            .map(|s| s.mode)
            .unwrap_or(Mode::Require),
    )
}

/// The cache directory of this sandboxed team process (`XDG_CACHE_HOME`, which [`apply_env`]
/// points at the team's own), or `None` outside one. Caches every process of the root shares
/// (the plugin manifests under `<root>/cache/`) are kept there instead: a team process may
/// neither read the server's (it lists every team's plugins) nor write it.
pub fn private_cache_dir() -> Option<PathBuf> {
    std::env::var_os(SPEC_VAR).filter(|v| !v.is_empty())?;
    std::env::var_os("XDG_CACHE_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// A team's own plugins directory in its scope directory: links to its plugins only, the
/// `MRSM_PLUGINS_DIR` of its processes (they cannot list the server's plugins directories).
pub const PLUGINS_VIEW_DIR: &str = "plugins";

/// The work directory of the sandboxed team job this process supervises or runs (5.1): next to
/// the job's spec ([`JOB_SPEC_FILE`]) that [`SPEC_VAR`] names, when it exists. What the job's
/// command writes (its result, heartbeat and start-up release) goes there.
pub fn job_work_dir_from_env() -> Option<PathBuf> {
    let spec = PathBuf::from(std::env::var_os(SPEC_VAR).filter(|v| !v.is_empty())?);
    if spec.file_name()? != JOB_SPEC_FILE {
        return None;
    }
    let work = spec.parent()?.join(JOB_WORK_DIR);
    work.is_dir().then_some(work)
}

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
    let mode = Mode::from_config(config);
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
    if mode == Mode::RequireSockets && support.abi() < SOCKETS_ABI {
        return Err(std::io::Error::other(format!(
            "The isolated team '{}' cannot run: its sandbox must cover Unix sockets and signals \
             (`system.engine.python_team_sandbox: require_sockets`), but this kernel's Landlock \
             ABI {} covers the filesystem only (\"{LEVEL_FILESYSTEM}\"); that needs ABI \
             {SOCKETS_ABI}, Linux 7.1.",
            scope.team,
            support.abi()
        )));
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

#[cfg(target_os = "linux")]
#[path = "sandbox/linux.rs"]
mod imp;

#[cfg(not(target_os = "linux"))]
mod imp {
    use super::{Mode, Report, Rule, Support};
    use std::path::PathBuf;

    pub(super) fn kernel_support() -> Support {
        Support::Unsupported {
            reason: "Landlock exists on Linux only".to_string(),
        }
    }

    pub(super) fn restrict(mode: Mode, _rules: &[Rule], _max_abi: Option<u32>) -> Report {
        Report::not_enforced(mode, "Landlock exists on Linux only")
    }

    pub(super) fn deny_unix_sockets(report: &mut Report) {
        report.sockets_reason = Some("seccomp exists on Linux only".to_string());
    }

    pub(super) fn private_tmp_dir(name: &str) -> std::io::Result<PathBuf> {
        let dir = std::env::temp_dir().join(format!("mrsm-{name}"));
        super::super::create_private_dir(&dir)?;
        Ok(dir)
    }
}

#[cfg(test)]
#[path = "sandbox/tests.rs"]
mod tests;
