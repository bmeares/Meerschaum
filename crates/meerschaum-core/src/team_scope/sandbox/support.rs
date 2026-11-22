//! What the kernel supports: the Landlock features of each ABI, whether Landlock or the seccomp
//! filter can deny Unix sockets, the container runtime's socket, and probing the canary file.

use super::*;

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

/// The Docker (or Podman) API sockets a process may find (5.1): `DOCKER_HOST`'s, the system's
/// and the user's runtime directory's.
pub fn container_socket_candidates() -> Vec<PathBuf> {
    let mut candidates: Vec<PathBuf> = vec![
        PathBuf::from("/var/run/docker.sock"),
        PathBuf::from("/run/docker.sock"),
        PathBuf::from("/run/podman/podman.sock"),
    ];
    if let Some(runtime) = std::env::var_os("XDG_RUNTIME_DIR").filter(|v| !v.is_empty()) {
        let runtime = PathBuf::from(runtime);
        candidates.push(runtime.join("docker.sock"));
        candidates.push(runtime.join("podman").join("podman.sock"));
    }
    if let Ok(host) = std::env::var("DOCKER_HOST")
        && let Some(path) = host.strip_prefix("unix://")
    {
        candidates.insert(0, PathBuf::from(path));
    }
    candidates
}

/// Whether this process can connect to the Unix socket `path` (nothing is sent).
pub fn connects(path: &Path) -> bool {
    #[cfg(unix)]
    {
        std::os::unix::net::UnixStream::connect(path).is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        false
    }
}

/// The Docker (or Podman) API socket this process's user can connect to, if any (a connection
/// only: nothing is sent).
pub fn container_socket_reachable() -> Option<PathBuf> {
    container_socket_candidates()
        .into_iter()
        .find(|path| connects(path))
}

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
