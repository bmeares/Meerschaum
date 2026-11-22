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

use super::{
    INDEX_FILE, SCOPES_DIR, ScopeFiles, TeamScope, api_execution, create_private_dir, scope_dir,
    team_config, write_private,
};
use crate::config::Config;

pub mod limits;
pub mod runtime;

pub use limits::{EgressProbe, HostEgress, LimitsReport, ResourceLimits};

mod enforcement;
mod prepare;
mod probe;
mod spec;
mod support;

pub use enforcement::{
    Report, current, deny_inbound_network, deny_network, enforce, enforce_from_env, is_native_api,
    with_api_transport,
};
pub use prepare::{Prepared, apply_env, prepare, prepare_with, team_job_dirs, team_tmp_dir};
pub use probe::{PROBE_ARG, cached_probe, probe_if_requested, probe_team_process};
pub use spec::{
    Access, Rule, SYSTEM_RULES, Spec, SpecInputs, build_spec, canary_path, ensure_canary, lexical,
    seed_team_venv,
};
pub use support::{
    Canary, FEATURES, Feature, NEWEST_ABI, Support, connects, container_socket_candidates,
    container_socket_reachable, features_of, kernel_support, landlock_stops_unix_sockets,
    probe_canary,
};

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

/// Whether `name` may name a plugin's file (no separators, no parent references, no hidden
/// names).
fn plain_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
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

// Platform selection: `imp` is the Landlock implementation on Linux and an inline stub elsewhere.
#[cfg(target_os = "linux")]
#[path = "sandbox/linux.rs"]
mod imp;

#[cfg(not(target_os = "linux"))]
mod imp {
    use super::{LimitsReport, Mode, Report, ResourceLimits, Rule, Support};
    use std::path::PathBuf;

    pub(super) fn kernel_support() -> Support {
        Support::Unsupported {
            reason: "Landlock exists on Linux only".to_string(),
        }
    }

    pub(super) fn restrict(
        mode: Mode,
        _rules: &[Rule],
        _max_abi: Option<u32>,
        _limits: &ResourceLimits,
    ) -> Report {
        Report::not_enforced(mode, "Landlock exists on Linux only")
    }

    pub(super) fn deny_unix_sockets(report: &mut Report) {
        report.sockets_reason = Some("seccomp exists on Linux only".to_string());
    }

    pub(super) fn apply_rlimits(limits: &ResourceLimits, report: &mut LimitsReport) {
        if limits.cpu_seconds.is_some()
            || limits.memory_mb.is_some()
            || limits.file_mb.is_some()
            || limits.processes.is_some()
        {
            report
                .errors
                .push("host-team rlimit enforcement is available on Linux only".to_string());
        }
    }

    pub(super) fn enforce_egress(limits: &ResourceLimits, report: &mut LimitsReport) {
        if limits.egress != super::HostEgress::Open {
            report
                .errors
                .push("host-team egress filtering is available on Linux only".to_string());
        }
    }

    pub(super) fn private_tmp_dir(name: &str) -> std::io::Result<PathBuf> {
        let dir = std::env::temp_dir().join(format!("mrsm-{name}"));
        super::super::create_private_dir(&dir)?;
        Ok(dir)
    }
}

#[cfg(test)]
mod tests;
