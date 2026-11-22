//! The spec of a team process: its rules ([`SYSTEM_RULES`] and the team's own paths), how they
//! are built and checked, a team's private copy of the shared virtual environment, and the
//! canary file the rules deny.

use super::*;

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
    // Public trust stores only. Their parents also contain service private keys and
    // NSS key databases, which a team worker must never inherit from the server UID.
    ("/etc/ssl/certs", Access::Read, "certificates"),
    ("/etc/ssl/cert.pem", Access::Read, "certificates"),
    ("/etc/pki/ca-trust/extracted", Access::Read, "certificates"),
    (
        "/etc/pki/ca-trust/source/anchors",
        Access::Read,
        "certificates",
    ),
    ("/etc/pki/tls/certs", Access::Read, "certificates"),
    ("/etc/pki/tls/cert.pem", Access::Read, "certificates"),
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
    (
        "/etc/flexiblasrc",
        Access::Read,
        "BLAS configuration (numpy)",
    ),
    (
        "/etc/flexiblasrc.d",
        Access::Read,
        "BLAS configuration (numpy)",
    ),
    (
        "/etc/flexiblas64rc",
        Access::Read,
        "BLAS configuration (numpy)",
    ),
    (
        "/etc/flexiblas64rc.d",
        Access::Read,
        "BLAS configuration (numpy)",
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
    /// Host-selected native bootstrap and runtime environment; never a persisted job override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<runtime::Runtime>,
    /// Optional OS-level resource limits (CPU, memory, file size, disk quota, processes, egress).
    #[serde(default, skip_serializing_if = "ResourceLimits::is_default")]
    pub limits: ResourceLimits,
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
        runtime: None,
        limits: ResourceLimits::default(),
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
