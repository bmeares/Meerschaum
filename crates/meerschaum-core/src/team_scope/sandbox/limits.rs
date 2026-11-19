//! Resource limits and outbound egress controls for isolated host-team processes (5.1).
//!
//! Configured under `system.engine`:
//! - `python_team_max_cpu_seconds`: hard/soft `RLIMIT_CPU` cap (seconds of CPU time)
//! - `python_team_max_memory_mb`: hard/soft `RLIMIT_AS` address-space cap (MiB)
//! - `python_team_max_file_mb`: hard/soft `RLIMIT_FSIZE` per-file write cap (MiB)
//! - `python_team_max_disk_mb`: total writable-directory quota across the team's `tmp`, `cache`,
//!   `venvs`, and job work directories (MiB), checked before launching team processes
//! - `python_team_max_processes`: hard/soft `RLIMIT_NPROC` cap
//! - `python_team_egress`: `open` (default), `deny` (seccomp blocks IPv4/IPv6 sockets), or
//!   `allow_ports` (Landlock ABI 4+ TCP connect allowlist + seccomp blocks non-TCP IPv4/IPv6)
//! - `python_team_egress_ports`: list of permitted outbound TCP ports when `egress: allow_ports`

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::value::Value;

/// `system.engine.python_team_max_cpu_seconds`: hard CPU-time limit (`RLIMIT_CPU`, seconds).
pub const MAX_CPU_SECONDS_KEY: [&str; 3] = ["system", "engine", "python_team_max_cpu_seconds"];
/// `system.engine.python_team_max_memory_mb`: hard address-space limit (`RLIMIT_AS`, MiB).
pub const MAX_MEMORY_MB_KEY: [&str; 3] = ["system", "engine", "python_team_max_memory_mb"];
/// `system.engine.python_team_max_file_mb`: hard single-file write limit (`RLIMIT_FSIZE`, MiB).
pub const MAX_FILE_MB_KEY: [&str; 3] = ["system", "engine", "python_team_max_file_mb"];
/// `system.engine.python_team_max_disk_mb`: writable-directory disk quota (MiB).
pub const MAX_DISK_MB_KEY: [&str; 3] = ["system", "engine", "python_team_max_disk_mb"];
/// `system.engine.python_team_max_processes`: hard process/thread count limit (`RLIMIT_NPROC`).
pub const MAX_PROCESSES_KEY: [&str; 3] = ["system", "engine", "python_team_max_processes"];
/// `system.engine.python_team_egress`: `open` (default), `deny`, or `allow_ports`.
pub const EGRESS_KEY: [&str; 3] = ["system", "engine", "python_team_egress"];
/// `system.engine.python_team_egress_ports`: allowed outbound TCP ports for `allow_ports`.
pub const EGRESS_PORTS_KEY: [&str; 3] = ["system", "engine", "python_team_egress_ports"];

/// Outbound network egress policy for isolated host-team processes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostEgress {
    /// Outbound IPv4/IPv6 connections remain unrestricted (default).
    #[default]
    Open,
    /// Block all IPv4 and IPv6 socket creation via seccomp while keeping inherited Unix pairs.
    Deny,
    /// Permit outbound TCP only to [`ResourceLimits::egress_ports`] (Landlock ABI 4+) and block
    /// non-TCP IPv4/IPv6 sockets via seccomp.
    AllowPorts,
}

impl HostEgress {
    /// Parse `open`, `deny`/`none`/`off`, or `allow_ports`/`ports`.
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "" | "open" | "allow" | "unrestricted" => Some(Self::Open),
            "deny" | "none" | "off" | "block" | "disabled" => Some(Self::Deny),
            "allow_ports" | "allow-ports" | "ports" => Some(Self::AllowPorts),
            _ => None,
        }
    }

    /// Canonical string representation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Deny => "deny",
            Self::AllowPorts => "allow_ports",
        }
    }

    /// Parse from a Meerschaum configuration value; unknown values fail closed to `Deny`.
    pub fn from_value(value: Option<&Value>) -> Self {
        match value {
            None | Some(Value::Null) => Self::Open,
            Some(Value::Bool(true)) => Self::Open,
            Some(Value::Bool(false)) => Self::Deny,
            Some(Value::Str(text)) => Self::parse(text).unwrap_or(Self::Deny),
            Some(_) => Self::Deny,
        }
    }
}

/// Result of the live post-enforcement outbound egress probe.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EgressProbe {
    /// Not tested (egress is open or enforcement was skipped).
    #[default]
    NotChecked,
    /// Outbound socket creation was refused by the kernel (the proof).
    Denied,
    /// An outbound socket succeeded despite the requested egress policy.
    Allowed,
    /// Probing failed with an unexpected error.
    Error(String),
}

/// Optional OS-level resource limits and egress policy for a host-team sandbox spec.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ResourceLimits {
    /// CPU time cap in seconds (`RLIMIT_CPU`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_seconds: Option<u64>,
    /// Virtual address space cap in MiB (`RLIMIT_AS`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_mb: Option<u64>,
    /// Maximum single file size in MiB (`RLIMIT_FSIZE`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_mb: Option<u64>,
    /// Maximum aggregate writable disk footprint across team directories in MiB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_mb: Option<u64>,
    /// Maximum processes/threads (`RLIMIT_NPROC`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processes: Option<u64>,
    /// Outbound network egress policy.
    #[serde(skip_serializing_if = "is_open_egress")]
    pub egress: HostEgress,
    /// Allowed outbound TCP ports when [`Self::egress`] is [`HostEgress::AllowPorts`].
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub egress_ports: Vec<u16>,
}

fn is_open_egress(egress: &HostEgress) -> bool {
    *egress == HostEgress::Open
}

fn positive_u64(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Int(n) if *n > 0 => u64::try_from(*n).ok(),
        Value::Float(f) if f.is_finite() && *f >= 1.0 => Some(*f as u64),
        Value::Str(s) => s.trim().parse::<u64>().ok().filter(|n| *n > 0),
        _ => None,
    }
}

fn parse_ports(value: Option<&Value>) -> Vec<u16> {
    let mut ports = Vec::new();
    match value {
        Some(Value::List(items)) => {
            for item in items {
                if let Some(n) = positive_u64(Some(item))
                    && let Ok(port) = u16::try_from(n)
                    && port > 0
                {
                    ports.push(port);
                }
            }
        }
        Some(Value::Str(text)) => {
            for part in text.split([',', ' ', ';']) {
                if let Ok(port) = part.trim().parse::<u16>()
                    && port > 0
                {
                    ports.push(port);
                }
            }
        }
        _ => {}
    }
    ports.sort_unstable();
    ports.dedup();
    ports
}

impl ResourceLimits {
    /// Read host-team resource limits from `config` (`system.engine.python_team_*`).
    pub fn from_config(config: &Config) -> Self {
        Self {
            cpu_seconds: positive_u64(config.get(&MAX_CPU_SECONDS_KEY).as_ref()),
            memory_mb: positive_u64(config.get(&MAX_MEMORY_MB_KEY).as_ref()),
            file_mb: positive_u64(config.get(&MAX_FILE_MB_KEY).as_ref()),
            disk_mb: positive_u64(config.get(&MAX_DISK_MB_KEY).as_ref()),
            processes: positive_u64(config.get(&MAX_PROCESSES_KEY).as_ref()),
            egress: HostEgress::from_value(config.get(&EGRESS_KEY).as_ref()),
            egress_ports: parse_ports(config.get(&EGRESS_PORTS_KEY).as_ref()),
        }
    }

    /// True when no limit or egress restriction is configured.
    pub fn is_default(&self) -> bool {
        !self.is_configured()
    }

    /// True when at least one resource limit or egress restriction is configured.
    pub fn is_configured(&self) -> bool {
        self.cpu_seconds.is_some()
            || self.memory_mb.is_some()
            || self.file_mb.is_some()
            || self.disk_mb.is_some()
            || self.processes.is_some()
            || self.egress != HostEgress::Open
    }

    /// True when at least one in-process kernel limit (`rlimit` or egress) is configured.
    pub fn has_process_limits(&self) -> bool {
        self.cpu_seconds.is_some()
            || self.memory_mb.is_some()
            || self.file_mb.is_some()
            || self.processes.is_some()
            || self.egress != HostEgress::Open
    }

    /// Human-readable one-line summary for logs and `mrsm doctor`.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        if let Some(secs) = self.cpu_seconds {
            parts.push(format!("cpu={secs}s"));
        }
        if let Some(mb) = self.memory_mb {
            parts.push(format!("memory={mb} MiB"));
        }
        if let Some(mb) = self.file_mb {
            parts.push(format!("max_file={mb} MiB"));
        }
        if let Some(mb) = self.disk_mb {
            parts.push(format!("disk_quota={mb} MiB"));
        }
        if let Some(n) = self.processes {
            parts.push(format!("processes={n}"));
        }
        match self.egress {
            HostEgress::Open => {}
            HostEgress::Deny => parts.push("egress=deny".to_string()),
            HostEgress::AllowPorts => {
                let ports: Vec<String> = self.egress_ports.iter().map(u16::to_string).collect();
                parts.push(format!("egress=allow_ports([{}])", ports.join(", ")));
            }
        }
        if parts.is_empty() {
            "unrestricted (default)".to_string()
        } else {
            parts.join(", ")
        }
    }
}

/// What a team process reports about its applied resource limits and egress policy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LimitsReport {
    /// Configured CPU time limit (seconds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_seconds: Option<u64>,
    /// Configured virtual memory limit (MiB).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_mb: Option<u64>,
    /// Configured max file size limit (MiB).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_mb: Option<u64>,
    /// Configured writable-directory disk quota (MiB).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_mb: Option<u64>,
    /// Configured max process limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processes: Option<u64>,
    /// Configured egress policy.
    #[serde(skip_serializing_if = "is_open_egress")]
    pub egress: HostEgress,
    /// Configured allowed outbound TCP ports.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub egress_ports: Vec<u16>,
    /// Enforcement mechanism (`"seccomp"`, `"landlock+seccomp"`, or empty).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub egress_mechanism: String,
    /// Result of probing egress denial after installation.
    #[serde(skip_serializing_if = "is_not_checked")]
    pub egress_probe: EgressProbe,
    /// Labels of kernel limits successfully applied (`rlimit_cpu`, `rlimit_as`, etc.).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub applied: Vec<String>,
    /// Errors encountered while applying requested limits (fails closed in [`Self::verdict`]).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

fn is_not_checked(probe: &EgressProbe) -> bool {
    *probe == EgressProbe::NotChecked
}

impl LimitsReport {
    /// Populate the configured fields from `limits` before enforcement.
    pub fn from_limits(limits: &ResourceLimits) -> Self {
        Self {
            cpu_seconds: limits.cpu_seconds,
            memory_mb: limits.memory_mb,
            file_mb: limits.file_mb,
            disk_mb: limits.disk_mb,
            processes: limits.processes,
            egress: limits.egress,
            egress_ports: limits.egress_ports.clone(),
            egress_mechanism: String::new(),
            egress_probe: EgressProbe::NotChecked,
            applied: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// True when no limits were configured, applied, or errored.
    pub fn is_empty(&self) -> bool {
        self.cpu_seconds.is_none()
            && self.memory_mb.is_none()
            && self.file_mb.is_none()
            && self.disk_mb.is_none()
            && self.processes.is_none()
            && self.egress == HostEgress::Open
            && self.egress_ports.is_empty()
            && self.egress_mechanism.is_empty()
            && self.egress_probe == EgressProbe::NotChecked
            && self.applied.is_empty()
            && self.errors.is_empty()
    }

    /// Fail closed whenever a configured resource limit or egress restriction failed to apply or
    /// failed its live probe.
    pub fn verdict(&self) -> Result<(), String> {
        if !self.errors.is_empty() {
            return Err(format!(
                "configured team resource limits could not be enforced: {}",
                self.errors.join("; ")
            ));
        }
        if self.egress != HostEgress::Open && self.egress_probe != EgressProbe::Denied {
            return Err(format!(
                "team egress policy `{}` was requested, but the post-enforcement probe did not \
                 prove denial ({:?})",
                self.egress.as_str(),
                self.egress_probe
            ));
        }
        Ok(())
    }

    /// Concise summary for `Report::describe`.
    pub fn describe(&self) -> String {
        let mut parts = self.applied.clone();
        if let Some(mb) = self.disk_mb {
            parts.push(format!("disk_quota={mb}MiB"));
        }
        if !self.egress_mechanism.is_empty() {
            parts.push(format!(
                "egress={}({})",
                self.egress.as_str(),
                self.egress_mechanism
            ));
        }
        if !self.errors.is_empty() {
            parts.push(format!("errors=[{}]", self.errors.join("; ")));
        }
        parts.join(", ")
    }
}

const MAX_WALK_DEPTH: usize = 64;

/// Sum regular-file bytes beneath `path` without following symbolic links.
pub fn directory_bytes(path: &Path) -> std::io::Result<u64> {
    fn walk(path: &Path, depth: usize, total: &mut u64) -> std::io::Result<()> {
        if depth > MAX_WALK_DEPTH {
            return Ok(());
        }
        let meta = match std::fs::symlink_metadata(path) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        if meta.file_type().is_symlink() {
            return Ok(());
        }
        if meta.is_file() {
            *total = total.saturating_add(meta.len());
            return Ok(());
        }
        if meta.is_dir() {
            let entries = match std::fs::read_dir(path) {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(e),
            };
            for entry in entries.flatten() {
                walk(&entry.path(), depth + 1, total)?;
            }
        }
        Ok(())
    }

    let mut total = 0u64;
    walk(path, 0, &mut total)?;
    Ok(total)
}

/// Verify that the combined byte footprint of `dirs` does not exceed `max_disk_mb` MiB.
/// Returns the measured bytes when within quota, or a descriptive error when exceeded.
pub fn check_disk_quota(team: &str, dirs: &[&Path], max_disk_mb: u64) -> std::io::Result<u64> {
    let limit_bytes = max_disk_mb.saturating_mul(1024 * 1024);
    let mut used = 0u64;
    for dir in dirs {
        used = used.saturating_add(directory_bytes(dir)?);
    }
    if used > limit_bytes {
        let used_mb = used as f64 / (1024.0 * 1024.0);
        return Err(std::io::Error::other(format!(
            "The isolated team '{team}' exceeded its disk quota (`system.engine.python_team_max_disk_mb`: \
             {max_disk_mb} MiB; currently using {used_mb:.1} MiB across its temporary, cache, \
             virtual-environment, and job directories)."
        )));
    }
    Ok(used)
}
