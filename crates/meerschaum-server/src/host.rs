//! The Python API host lifecycle (§7.3, D17, D21): start `python -m meerschaum.api.host` on a Unix
//! socket under the root directory (TCP loopback with a per-start token on Windows) when an
//! installed plugin needs it or the Dash web console is available, supervise it like a job, and
//! restart it with backoff when it crashes.
//!
//! Launching is behind [`HostLauncher`] so tests (and embedders) can substitute a fake host.
//!
//! **The manifest seam.** Whether the host is needed comes from the plugin manifest (§4.3),
//! which the CLI writes when plugins are installed or set up. Its real producer is being built
//! elsewhere; this module reads it through [`ManifestSource`] and only needs the
//! [`HostManifest`] subset. [`spawn_manifest_watcher`] polls the source: when the manifest
//! changes, the host is restarted (so newly installed plugins load), started on demand if a
//! plugin now needs it, or stopped if none does, and the MCP federation's registry is
//! refetched.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::{Mutex, Notify};

/// The parts of the plugin manifest (§4.3) that decide whether the API host is needed: the
/// plugins registering `@api_plugin`, `@dash_plugin`, a Python `@web_page` or an MCP tool.
///
/// The manifest is produced by the CLI when plugins are installed or set up; this is the
/// subset the server reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct HostManifest {
    /// Plugins with `@api_plugin` functions.
    #[serde(default)]
    pub api_plugins: Vec<String>,
    /// Plugins with `@dash_plugin` functions.
    #[serde(default)]
    pub dash_plugins: Vec<String>,
    /// Plugins with Python `@web_page` layouts.
    #[serde(default)]
    pub web_pages: Vec<String>,
    /// Plugins registering MCP tools or resources.
    #[serde(default)]
    pub mcp_tools: Vec<String>,
    /// The Dash web console is available (the `dash` extra imports, [`probe_console`]): the
    /// host runs for it even when no plugin needs it (D21).
    #[serde(default)]
    pub console: bool,
}

impl HostManifest {
    /// Whether the API host is needed: by an installed plugin or by the Dash console.
    pub fn needs_host(&self) -> bool {
        !self.reasons().is_empty()
    }

    /// Why the host is needed (`api_plugins`, `dash_plugins`, `web_pages`, `mcp_tools`,
    /// `console`); empty when it never starts.
    pub fn reasons(&self) -> Vec<&'static str> {
        [
            ("api_plugins", !self.api_plugins.is_empty()),
            ("dash_plugins", !self.dash_plugins.is_empty()),
            ("web_pages", !self.web_pages.is_empty()),
            ("mcp_tools", !self.mcp_tools.is_empty()),
            ("console", self.console),
        ]
        .into_iter()
        .filter_map(|(name, present)| present.then_some(name))
        .collect()
    }

    /// The same manifest with the console reason set (the start-up probe's answer).
    pub fn with_console(mut self, console: bool) -> HostManifest {
        self.console = console;
        self
    }

    /// Read a manifest JSON file (a missing file means no plugin needs the host).
    pub fn from_file(path: &Path) -> std::io::Result<HostManifest> {
        match std::fs::read_to_string(path) {
            Ok(s) => serde_json::from_str(&s).map_err(std::io::Error::other),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HostManifest::default()),
            Err(e) => Err(e),
        }
    }
}

/// Where the plugin manifest comes from (the seam with the manifest's producer).
#[async_trait]
pub trait ManifestSource: Send + Sync {
    /// Read the current manifest (a missing manifest means no plugin needs the host).
    async fn load(&self) -> std::io::Result<HostManifest>;
}

/// A manifest JSON file.
#[derive(Debug, Clone)]
pub struct FileManifest(pub PathBuf);

#[async_trait]
impl ManifestSource for FileManifest {
    async fn load(&self) -> std::io::Result<HostManifest> {
        HostManifest::from_file(&self.0)
    }
}

/// A fixed manifest (tests, embedders).
#[derive(Debug, Clone, Default)]
pub struct StaticManifest(pub HostManifest);

#[async_trait]
impl ManifestSource for StaticManifest {
    async fn load(&self) -> std::io::Result<HostManifest> {
        Ok(self.0.clone())
    }
}

/// A manifest source with the console reason forced on or off: `console` is a property of the
/// interpreter and flags of this process ([`probe_console`]), not of the plugin tree, so it is
/// applied on top of whatever the manifest file says.
#[derive(Debug, Clone)]
pub struct WithConsole<S>(pub S, pub bool);

#[async_trait]
impl<S: ManifestSource> ManifestSource for WithConsole<S> {
    async fn load(&self) -> std::io::Result<HostManifest> {
        Ok(self.0.load().await?.with_console(self.1))
    }
}

/// The Python modules the Dash console needs (the `dash` extra): [`probe_console`] looks for
/// each of them in the interpreter's `sys.path` and in the `mrsm` virtual environment.
pub const CONSOLE_MODULES: &[&str] = &[
    "meerschaum",
    "dash",
    "dash_bootstrap_components",
    "dash_ace",
    "dash_extensions",
    "a2wsgi",
    "fastapi",
    "uvicorn",
    "multipart",
    "jwt",
    "websockets",
    "pandas",
    "pyarrow",
];

/// What [`probe_console`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsoleProbe {
    /// Whether every module of [`CONSOLE_MODULES`] is importable.
    pub available: bool,
    /// The modules that are not (or the reason the probe could not run, as one entry).
    pub missing: Vec<String>,
}

impl ConsoleProbe {
    /// The one-sentence warning for an unavailable console.
    pub fn warning(&self) -> String {
        format!(
            "The web console is not available ({}): it comes with the Python package; install it with `pip install \"meerschaum[dash]\"`. `/dash` shows a pointer page.",
            self.missing.join(", ")
        )
    }
}

const PROBE_SCRIPT: &str = r#"
import importlib.util, json, os, sys
root, mods = sys.argv[1], sys.argv[2].split(',')
venvs = os.environ.get('MRSM_VENVS_DIR') or os.path.join(root, 'venvs')
base = os.path.join(venvs, 'mrsm')
if os.name == 'nt':
    packages = os.path.join(base, 'Lib', 'site-packages')
else:
    packages = os.path.join(base, 'lib', 'python%d.%d' % sys.version_info[:2], 'site-packages')
if os.path.isdir(packages):
    sys.path.append(packages)
missing = []
for module in mods:
    try:
        found = importlib.util.find_spec(module) is not None
    except Exception:
        found = False
    if not found:
        missing.append(module)
print(json.dumps(missing))
"#;

/// How long [`probe_console`] waits for the interpreter before it gives up on the console.
pub const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Ask `python` whether the Dash console's modules ([`CONSOLE_MODULES`]) are importable, without
/// importing them (about 30 ms): they may be in the interpreter's site-packages or in
/// `<root>/venvs/mrsm`. A probe that cannot run (no interpreter) or does not answer within
/// [`PROBE_TIMEOUT`] (a `sitecustomize` that blocks, a stalled network home) answers "not
/// available": the server binds anyway and `/dash` is the pointer page.
pub fn probe_console(python: &Path, root: &Path, env: &[(String, String)]) -> ConsoleProbe {
    probe_console_within(python, root, env, PROBE_TIMEOUT)
}

/// [`probe_console`] with an explicit deadline.
pub fn probe_console_within(
    python: &Path,
    root: &Path,
    env: &[(String, String)],
    timeout: std::time::Duration,
) -> ConsoleProbe {
    use std::io::Read;
    let unavailable = |why: String| ConsoleProbe {
        available: false,
        missing: vec![why],
    };
    let spawned = std::process::Command::new(python)
        .args([
            meerschaum_core::self_exe::PYTHON_SAFE_PATH_FLAG,
            "-c",
            PROBE_SCRIPT,
        ])
        .arg(root)
        .arg(CONSOLE_MODULES.join(","))
        .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn();
    let mut child = match spawned {
        Ok(child) => child,
        Err(e) => {
            return unavailable(format!("no Python interpreter ({}: {e})", python.display()));
        }
    };
    let mut stdout = child.stdout.take();
    let reader = std::thread::spawn(move || {
        let mut text = String::new();
        if let Some(out) = stdout.as_mut() {
            let _ = out.read_to_string(&mut text);
        }
        text
    });
    let deadline = std::time::Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if std::time::Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                // The reader ends with the pipe; a grandchild holding it open must not hold us.
                drop(reader);
                return unavailable(format!(
                    "the interpreter did not answer within {} seconds ({})",
                    timeout.as_secs_f64().max(0.001),
                    python.display()
                ));
            }
            Ok(None) => std::thread::sleep(std::time::Duration::from_millis(5)),
            Err(e) => return unavailable(format!("the interpreter could not be waited for: {e}")),
        }
    };
    let text = reader.join().unwrap_or_default();
    if !status.success() {
        return unavailable(format!("the interpreter exited with {status}"));
    }
    match serde_json::from_slice::<Vec<String>>(text.trim().as_bytes()) {
        Ok(missing) => ConsoleProbe {
            available: missing.is_empty(),
            missing,
        },
        Err(e) => unavailable(format!("the interpreter's answer was unreadable: {e}")),
    }
}

/// How the host is reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostEndpoint {
    /// A Unix-domain socket (everywhere but Windows).
    Unix(PathBuf),
    /// TCP on loopback, with a per-start token every forwarded request carries in
    /// [`PROXY_TOKEN_HEADER`] (Windows, where Unix sockets are not used; also
    /// `MRSM_API_HOST_TRANSPORT=tcp` anywhere, for tests).
    Tcp {
        /// The loopback address.
        addr: std::net::SocketAddr,
        /// The token the host requires (`MRSM_API_HOST_PROXY_TOKEN`).
        token: String,
    },
}

impl From<PathBuf> for HostEndpoint {
    fn from(socket: PathBuf) -> HostEndpoint {
        HostEndpoint::Unix(socket)
    }
}

impl From<&Path> for HostEndpoint {
    fn from(socket: &Path) -> HostEndpoint {
        HostEndpoint::Unix(socket.to_path_buf())
    }
}

impl HostEndpoint {
    /// The proxy token (TCP only).
    pub fn token(&self) -> Option<&str> {
        match self {
            HostEndpoint::Unix(_) => None,
            HostEndpoint::Tcp { token, .. } => Some(token),
        }
    }
}

/// The host's status route (`GET /_mrsm/host/status`, internal token): `{"console": {"mounted":
/// bool, "reason": str|null, "dash_version": str|null}}`.
pub const STATUS_PATH: &str = "/_mrsm/host/status";
/// The identity check of a TCP host (`GET /_mrsm/host/proof?nonce=...`, answered without the
/// token): the hex HMAC-SHA256 of the nonce keyed with the per-start token. The port is chosen
/// before the host binds it, so a local process could take it in between; only the real host can
/// answer, and the proxy forwards nothing (cookies, bearer tokens, the proxy token) before it did.
pub const PROOF_PATH: &str = "/_mrsm/host/proof";
/// The header carrying the per-start token on the TCP transport.
pub const PROXY_TOKEN_HEADER: &str = "x-mrsm-host-proxy-token";
/// The environment variable that gives the host its per-start token (TCP transport).
pub const PROXY_TOKEN_ENV: &str = "MRSM_API_HOST_PROXY_TOKEN";
/// `unix` or `tcp`: overrides the platform's default transport.
pub const TRANSPORT_ENV: &str = "MRSM_API_HOST_TRANSPORT";

/// Which transport the supervisor starts the host on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostTransport {
    /// A Unix-domain socket.
    Unix,
    /// TCP loopback with a token.
    Tcp,
}

impl HostTransport {
    /// Unix sockets, except on Windows (TCP loopback with a token); `MRSM_API_HOST_TRANSPORT`
    /// (`unix`, `tcp`) overrides.
    pub fn from_env() -> HostTransport {
        match std::env::var(TRANSPORT_ENV).ok().as_deref() {
            Some("tcp") => HostTransport::Tcp,
            Some("unix") if cfg!(unix) => HostTransport::Unix,
            _ if cfg!(windows) => HostTransport::Tcp,
            _ => HostTransport::Unix,
        }
    }
}

/// Whether the host mounted the Dash console (`GET /_mrsm/host/status`), per host process.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ConsoleStatus {
    /// Not asked yet (or the host restarted since).
    #[default]
    Unknown,
    /// The Dash app is mounted: `/dash/*` goes to the host.
    Mounted,
    /// It is not (the reason), or there is no host: `/dash/*` shows the pointer page.
    Unavailable(String),
}

impl ConsoleStatus {
    /// Parse the `console` object of the host's status (or registry) document.
    pub fn from_document(document: &serde_json::Value) -> Option<ConsoleStatus> {
        let console = document.get("console")?;
        if console
            .get("mounted")
            .and_then(serde_json::Value::as_bool)?
        {
            return Some(ConsoleStatus::Mounted);
        }
        Some(ConsoleStatus::Unavailable(
            console
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .filter(|r| !r.is_empty())
                .unwrap_or("the API host did not mount the console")
                .to_string(),
        ))
    }
}

/// The longest socket path used as is: `sun_path` holds 108 bytes on Linux and 104 on macOS,
/// including the terminating NUL.
pub const MAX_SOCKET_PATH: usize = 100;

/// The host's socket for a root directory: `<root>/api/host/host.sock`, or, when that path is
/// too long or its directory cannot be made private by this user, a per-user, per-root name
/// under `$XDG_RUNTIME_DIR` (else the temporary directory). Existing foreign ownership is
/// never changed; mounted roots may have been initialized by a different container UID.
///
/// The socket's directory is private (`0700`, see [`HostSupervisor`]): uvicorn makes the socket
/// itself world-writable, and a local process that could connect to it would bypass the
/// server's authentication, allowlist and rate limits.
pub fn default_socket_path(root: &Path) -> PathBuf {
    let p = root.join("api").join("host").join("host.sock");
    if p.as_os_str().len() <= MAX_SOCKET_PATH && private_dir(p.parent().unwrap()).is_ok() {
        return p;
    }
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    root.hash(&mut h);
    #[cfg(unix)]
    rustix::process::geteuid().as_raw().hash(&mut h);
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|d| d.is_dir())
        .unwrap_or_else(std::env::temp_dir);
    let dirname = format!("mrsm-host-{:016x}", h.finish());
    let mut dir = base.join(&dirname);
    if dir.join("host.sock").as_os_str().len() > MAX_SOCKET_PATH {
        dir = std::env::temp_dir().join(&dirname);
    }
    if private_dir(&dir).is_err() {
        // An unrelated process may have reserved the predictable name. Never reuse an
        // untrusted directory or socket; choose a fresh private name instead.
        dir = std::env::temp_dir().join(format!("mrsm-host-{}", uuid::Uuid::new_v4().simple()));
        let _ = private_dir(&dir);
    }
    dir.join("host.sock")
}

fn private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = std::fs::symlink_metadata(dir)?;
        if !metadata.is_dir() || metadata.uid() != rustix::process::geteuid().as_raw() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "the API host socket directory is not owned by this user",
            ));
        }
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// A running host process.
#[async_trait]
pub trait HostProcess: Send {
    /// Wait for the process to exit; returns its exit code if known.
    async fn wait(&mut self) -> std::io::Result<Option<i32>>;
    /// Terminate the process.
    async fn kill(&mut self) -> std::io::Result<()>;
}

/// Starts host processes.
#[async_trait]
pub trait HostLauncher: Send + Sync {
    /// Start a host serving on `socket`.
    async fn launch(&self, socket: &Path) -> std::io::Result<Box<dyn HostProcess>>;

    /// Start a host on `endpoint`. The default serves a Unix socket through [`launch`] and
    /// refuses TCP (launchers that support the Windows transport override this).
    ///
    /// [`launch`]: HostLauncher::launch
    async fn launch_endpoint(
        &self,
        endpoint: &HostEndpoint,
    ) -> std::io::Result<Box<dyn HostProcess>> {
        match endpoint {
            HostEndpoint::Unix(socket) => self.launch(socket).await,
            HostEndpoint::Tcp { .. } => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "this launcher cannot serve the API host over TCP",
            )),
        }
    }
}

/// Launches the API host, `python -m meerschaum.api.host --socket <path>` behind the version guard.
#[derive(Debug, Clone)]
pub struct PythonHostLauncher {
    /// The interpreter (`MRSM_PYTHON` or the environment that installed the binary).
    pub python: PathBuf,
    /// Extra environment variables (`MRSM_ROOT_DIR`, `MRSM_PLUGINS_DIR`, ...).
    pub env: Vec<(String, String)>,
}

/// The environment variable naming the interpreter that runs the host.
pub const PYTHON_ENV: &str = "MRSM_PYTHON";

impl PythonHostLauncher {
    /// The interpreter: `MRSM_PYTHON`, else `python3`.
    pub fn python_from_env() -> PathBuf {
        std::env::var_os(PYTHON_ENV)
            .filter(|p| !p.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("python3"))
    }

    /// A launcher for a server: the host serves the same root and instance with the same
    /// `--no-auth`, `--production`, `--private`, `--no-dash` and `--debug` flags
    /// (`MRSM_API_HOST_CONFIG`, read by `meerschaum.api` in host mode). The host never serves the
    /// web terminal (this crate does, [`crate::webterm`]), so `no_webterm` is always `true`.
    pub fn for_config(cfg: &crate::config::ServerConfig, python: PathBuf) -> PythonHostLauncher {
        let host_config = serde_json::json!({
            "mrsm_instance": cfg.instance_keys,
            "no_auth": cfg.no_auth,
            "production": cfg.production,
            "private": cfg.private,
            "debug": cfg.debug,
            // `no_dash`: mount no console and no plugin pages (v4's `--no-dash`).
            "no_dash": cfg.no_dash,
            // The Dash console is the host's (D21): mount it unless `--no-dash`.
            "console": !cfg.no_dash,
            // The Rust server on loopback, for calls back into it (execute, native pages).
            "server_url": format!("http://{}:{}", loopback_host(&cfg.host), cfg.port),
            // The terminal is served by the Rust server, never proxied to the host.
            "no_webterm": true,
            // Whether the Rust server serves `/webterm/<session-id>` (the console shows the
            // terminal page and its iframe only then).
            "webterm": !cfg.no_dash && !cfg.no_webterm,
            // The public preset (`api:permissions:preset: public`, `--public`): the host serves
            // the login page and plugin pages, not the console's own pages.
            "public": cfg.public,
            // Whether non-administrators may run actions and edit pipes from the console:
            // `api:permissions:actions:non_admin`, forced off by `--secure`.
            "actions_non_admin": cfg.actions_non_admin,
        });
        PythonHostLauncher {
            python,
            env: vec![
                ("MRSM_ROOT_DIR".into(), cfg.root_dir.display().to_string()),
                ("MRSM_API_HOST".into(), "1".into()),
                ("MRSM_API_HOST_CONFIG".into(), host_config.to_string()),
            ],
        }
    }
}

/// The address a client on this machine uses for a server bound to `host`.
fn loopback_host(host: &str) -> String {
    match host {
        "" | "0.0.0.0" => "127.0.0.1".to_string(),
        "::" | "[::]" => "[::1]".to_string(),
        h if h.contains(':') && !h.starts_with('[') => format!("[{h}]"),
        h => h.to_string(),
    }
}

impl PythonHostLauncher {
    /// The arguments after `meerschaum.api.host` that bind `endpoint`.
    pub fn endpoint_args(endpoint: &HostEndpoint) -> Vec<std::ffi::OsString> {
        match endpoint {
            HostEndpoint::Unix(socket) => vec!["--socket".into(), socket.as_os_str().to_owned()],
            HostEndpoint::Tcp { addr, .. } => vec!["--tcp".into(), addr.to_string().into()],
        }
    }

    /// The environment that goes with `endpoint` (the proxy token for TCP).
    pub fn endpoint_env(endpoint: &HostEndpoint) -> Vec<(&'static str, String)> {
        endpoint
            .token()
            .map(|token| vec![(PROXY_TOKEN_ENV, token.to_string())])
            .unwrap_or_default()
    }
}

/// How long a terminating host may take to exit before it is killed.
const TERMINATE_GRACE: Duration = Duration::from_secs(10);

struct ChildProcess(tokio::process::Child);

#[async_trait]
impl HostProcess for ChildProcess {
    async fn wait(&mut self) -> std::io::Result<Option<i32>> {
        Ok(self.0.wait().await?.code())
    }
    async fn kill(&mut self) -> std::io::Result<()> {
        // SIGTERM first so the host runs its shutdown events (it stops the terminal process and
        // its tmux session); SIGKILL only when it does not exit within the grace period.
        #[cfg(unix)]
        if let Some(pid) = self.0.id() {
            let _ = tokio::process::Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .await;
            if tokio::time::timeout(TERMINATE_GRACE, self.0.wait())
                .await
                .is_ok()
            {
                return Ok(());
            }
        }
        self.0.kill().await
    }
}

#[async_trait]
impl HostLauncher for PythonHostLauncher {
    async fn launch(&self, socket: &Path) -> std::io::Result<Box<dyn HostProcess>> {
        self.launch_endpoint(&HostEndpoint::from(socket)).await
    }

    async fn launch_endpoint(
        &self,
        endpoint: &HostEndpoint,
    ) -> std::io::Result<Box<dyn HostProcess>> {
        // `python -c <guard> + runpy` instead of `python -m`: the guard refuses a `meerschaum`
        // this program cannot work with before anything of it is imported (importing a stale v4
        // would run its start-up), then the module runs as `-m` would run it.
        let program = format!(
            "{}\n_mrsm_guard()\nimport runpy\nrunpy.run_module('meerschaum.api.host', run_name='__main__', alter_sys=True)\n",
            meerschaum_core::python_guard::SCRIPT
        );
        let child = tokio::process::Command::new(&self.python)
            .args([
                meerschaum_core::self_exe::PYTHON_SAFE_PATH_FLAG,
                "-c",
                program.as_str(),
            ])
            .args(PythonHostLauncher::endpoint_args(endpoint))
            .envs(PythonHostLauncher::endpoint_env(endpoint))
            .envs(meerschaum_core::python_guard::expect_env())
            // `start api --allow-protected`: plugin routes may write to protected connectors.
            .envs(meerschaum_core::protected::child_env())
            .env(meerschaum_core::python_guard::WARN_ENV, "1")
            .env(
                meerschaum_core::python_guard::RETRY_HINT_ENV,
                "Then restart `mrsm start api` (or it is retried within 5 minutes, and when the plugins change).",
            )
            .envs(self.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .kill_on_drop(true)
            .spawn()?;
        Ok(Box::new(ChildProcess(child)))
    }
}

/// Supervision options.
#[derive(Debug, Clone, Copy)]
pub struct SupervisorOptions {
    /// How long to wait for the socket to accept connections after a start.
    pub ready_timeout: Duration,
    /// The first restart delay (doubled after each consecutive crash).
    pub backoff_initial: Duration,
    /// The longest restart delay.
    pub backoff_max: Duration,
}

impl Default for SupervisorOptions {
    fn default() -> Self {
        SupervisorOptions {
            ready_timeout: Duration::from_secs(60),
            backoff_initial: Duration::from_millis(500),
            backoff_max: Duration::from_secs(30),
        }
    }
}

/// Starts the host on demand, keeps it running, and restarts it on crash.
pub struct HostSupervisor {
    launcher: Arc<dyn HostLauncher>,
    /// The Unix socket path (also the host's identity under TCP, where it is unused).
    socket: PathBuf,
    transport: HostTransport,
    /// Where the current host process listens (renewed at every launch under TCP).
    endpoint: std::sync::Mutex<HostEndpoint>,
    /// Whether the current host process mounted the console.
    console: std::sync::Mutex<ConsoleStatus>,
    opts: SupervisorOptions,
    started: Mutex<bool>,
    starts: AtomicUsize,
    stopping: AtomicBool,
    stop: Notify,
    /// Whether the manifest says a plugin needs the host.
    needed: AtomicBool,
    /// The manifest last applied.
    manifest: std::sync::Mutex<HostManifest>,
    restart: Notify,
    /// Set once the supervised process is gone after a shutdown request.
    terminated: tokio::sync::watch::Sender<bool>,
    /// The host's socket has accepted a connection at least once: a later exit is a crash, never
    /// a refusal to start.
    ever_ready: AtomicBool,
    /// Remembered guard refusals (see [`Refusals`]).
    refusals: std::sync::Mutex<Refusals>,
}

/// A host that refused to start (its guard found a `meerschaum` package this program cannot work
/// with, `python_guard::REFUSED_EXIT_CODE`) is not launched again for every proxied request: the
/// refusal is remembered and the host is asked again after a delay that doubles, or at once when
/// the plugin manifest changes (or [`HostSupervisor::clear_refusal`]).
#[derive(Debug, Clone, Copy)]
struct Refusals {
    /// No launch before this instant.
    until: Option<std::time::Instant>,
    /// The next delay.
    delay: Duration,
}

/// The first delay after a refusal, and the longest.
const REFUSAL_DELAY_INITIAL: Duration = Duration::from_secs(10);
const REFUSAL_DELAY_MAX: Duration = Duration::from_secs(300);

impl Refusals {
    const NONE: Refusals = Refusals {
        until: None,
        delay: REFUSAL_DELAY_INITIAL,
    };
}

impl std::fmt::Debug for HostSupervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostSupervisor")
            .field("socket", &self.socket)
            .field("starts", &self.starts.load(Ordering::SeqCst))
            .finish()
    }
}

/// The proof a host that knows `token` gives for `nonce` ([`PROOF_PATH`]).
pub fn host_proof(token: &str, nonce: &str) -> String {
    use hmac::{Hmac, KeyInit, Mac};
    let mut mac =
        Hmac::<sha2::Sha256>::new_from_slice(token.as_bytes()).expect("HMAC takes any key length");
    mac.update(nonce.as_bytes());
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Whether whatever listens at a TCP `endpoint` holds the per-start token (a Unix socket is
/// private to its directory and needs no proof). Nothing secret is sent: the nonce is ours.
pub async fn endpoint_proves_token(endpoint: &HostEndpoint) -> std::io::Result<bool> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let HostEndpoint::Tcp { token, .. } = endpoint else {
        return Ok(true);
    };
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let attempt = async {
        let mut stream = crate::proxy::connect_endpoint(endpoint).await?;
        let request = format!(
            "GET {PROOF_PATH}?nonce={nonce} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await?;
        let mut answer = Vec::new();
        // A host answers a few dozen bytes; anything long is not it.
        stream.take(16 * 1024).read_to_end(&mut answer).await?;
        Ok::<_, std::io::Error>(answer)
    };
    let answer = tokio::time::timeout(Duration::from_secs(3), attempt)
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "no proof in time"))??;
    let text = String::from_utf8_lossy(&answer);
    let ok = text.starts_with("HTTP/1.1 200")
        && text
            .split_once("\r\n\r\n")
            .is_some_and(|(_, body)| body.trim() == host_proof(token, &nonce));
    Ok(ok)
}

async fn endpoint_ready(supervisor: &HostSupervisor, timeout: Duration) -> std::io::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    // Something answered on a TCP port but could not prove it is our host.
    let mut impostor = false;
    loop {
        let endpoint = supervisor.endpoint();
        if crate::proxy::connect_endpoint(&endpoint).await.is_ok() {
            match endpoint_proves_token(&endpoint).await {
                Ok(true) => return Ok(()),
                Ok(false) => impostor = true,
                // The host accepts connections before its app answers: ask again.
                Err(_) => {}
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(if impostor {
                std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "another process answers on the API host's loopback port and cannot prove it is the host; nothing was forwarded to it",
                )
            } else {
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "the API host did not start listening in time",
                )
            });
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

impl HostSupervisor {
    /// A supervisor for `socket`, or `None` when the manifest shows no plugin needs the host
    /// (it never starts then).
    pub fn for_manifest(
        manifest: &HostManifest,
        launcher: Arc<dyn HostLauncher>,
        socket: impl Into<PathBuf>,
        opts: SupervisorOptions,
    ) -> Option<Arc<HostSupervisor>> {
        manifest
            .needs_host()
            .then(|| HostSupervisor::new(manifest, launcher, socket, opts))
    }

    /// A supervisor whatever the manifest says. The host starts on demand only while the
    /// manifest needs it ([`HostSupervisor::set_manifest`]).
    pub fn new(
        manifest: &HostManifest,
        launcher: Arc<dyn HostLauncher>,
        socket: impl Into<PathBuf>,
        opts: SupervisorOptions,
    ) -> Arc<HostSupervisor> {
        HostSupervisor::with_transport(manifest, launcher, socket, opts, HostTransport::from_env())
    }

    /// [`HostSupervisor::new`] on an explicit transport.
    pub fn with_transport(
        manifest: &HostManifest,
        launcher: Arc<dyn HostLauncher>,
        socket: impl Into<PathBuf>,
        opts: SupervisorOptions,
        transport: HostTransport,
    ) -> Arc<HostSupervisor> {
        let socket = socket.into();
        Arc::new(HostSupervisor {
            launcher,
            transport,
            endpoint: std::sync::Mutex::new(HostEndpoint::Unix(socket.clone())),
            console: std::sync::Mutex::new(ConsoleStatus::Unknown),
            socket,
            opts,
            started: Mutex::new(false),
            starts: AtomicUsize::new(0),
            stopping: AtomicBool::new(false),
            stop: Notify::new(),
            needed: AtomicBool::new(manifest.needs_host()),
            manifest: std::sync::Mutex::new(manifest.clone()),
            restart: Notify::new(),
            terminated: tokio::sync::watch::channel(false).0,
            ever_ready: AtomicBool::new(false),
            refusals: std::sync::Mutex::new(Refusals::NONE),
        })
    }

    /// Whether the host process has been started (and not stopped since).
    pub async fn is_running(&self) -> bool {
        *self.started.lock().await
    }

    /// Whether the manifest says a plugin needs the host (else it is never started).
    pub fn is_needed(&self) -> bool {
        self.needed.load(Ordering::SeqCst)
    }

    /// Apply a manifest: when it differs from the current one, a running host is restarted (to
    /// load the changed plugins), or stopped when no plugin needs it any more. Returns whether
    /// the manifest changed.
    pub async fn set_manifest(&self, manifest: &HostManifest) -> bool {
        {
            let mut current = self
                .manifest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if *current == *manifest {
                return false;
            }
            *current = manifest.clone();
        }
        self.needed.store(manifest.needs_host(), Ordering::SeqCst);
        // A changed plugin set may come with a fixed install: ask the host again.
        self.clear_refusal();
        if *self.started.lock().await {
            self.restart.notify_one();
        }
        true
    }

    /// Forget a remembered refusal: the next request starts the host again.
    pub fn clear_refusal(&self) {
        *self
            .refusals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Refusals::NONE;
    }

    /// Remember a refusal; the delay before the next attempt (doubled each time).
    fn remember_refusal(&self) -> Duration {
        let mut r = self
            .refusals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let delay = r.delay;
        r.until = Some(std::time::Instant::now() + delay);
        r.delay = (delay * 2).min(REFUSAL_DELAY_MAX);
        delay
    }

    /// How long until a refused host may be launched again (`None`: it may).
    fn refusal_remaining(&self) -> Option<Duration> {
        let r = self
            .refusals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        r.until
            .and_then(|u| u.checked_duration_since(std::time::Instant::now()))
            .filter(|d| !d.is_zero())
    }

    /// The host's socket (the Unix transport's path).
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Where the current host process listens.
    pub fn endpoint(&self) -> HostEndpoint {
        self.endpoint
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The transport the host is started on.
    pub fn transport(&self) -> HostTransport {
        self.transport
    }

    /// Whether the current host process mounted the console, as last reported.
    pub fn console_status(&self) -> ConsoleStatus {
        self.console
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Record the host's answer about the console.
    pub fn set_console_status(&self, status: ConsoleStatus) {
        *self
            .console
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = status;
    }

    /// How many times the host has been started (1 + restarts).
    pub fn starts(&self) -> usize {
        self.starts.load(Ordering::SeqCst)
    }

    /// A fresh endpoint for the next host process: the socket path, or a free loopback port
    /// with a new token.
    fn next_endpoint(&self) -> std::io::Result<HostEndpoint> {
        match self.transport {
            HostTransport::Unix => {
                let _ = std::fs::remove_file(&self.socket);
                if let Some(parent) = self.socket.parent() {
                    private_dir(parent)?;
                }
                Ok(HostEndpoint::Unix(self.socket.clone()))
            }
            HostTransport::Tcp => {
                // Bind port 0 to learn a free port, then release it for the host (a small race
                // the readiness wait and the restart backoff absorb).
                let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
                let addr = listener.local_addr()?;
                drop(listener);
                let token = format!(
                    "{}{}",
                    uuid::Uuid::new_v4().simple(),
                    uuid::Uuid::new_v4().simple()
                );
                Ok(HostEndpoint::Tcp { addr, token })
            }
        }
    }

    async fn launch_once(&self) -> std::io::Result<Box<dyn HostProcess>> {
        let endpoint = self.next_endpoint()?;
        *self
            .endpoint
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = endpoint.clone();
        // A new process has not said anything about the console yet.
        self.set_console_status(ConsoleStatus::Unknown);
        let proc = self.launcher.launch_endpoint(&endpoint).await?;
        self.terminated.send_replace(false);
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(proc)
    }

    /// Start the host if it is not running, then wait until its socket accepts connections.
    pub async fn ensure_started(self: &Arc<Self>) -> std::io::Result<()> {
        if !self.is_needed() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "no installed plugin needs the API host",
            ));
        }
        {
            let mut started = self.started.lock().await;
            if !*started {
                if let Some(wait) = self.refusal_remaining() {
                    return Err(std::io::Error::other(format!(
                        "the API host refused to start (see the earlier message); not asking again for {}s, or until the plugins change",
                        wait.as_secs().max(1)
                    )));
                }
                let proc = self.launch_once().await?;
                *started = true;
                let me = self.clone();
                tokio::spawn(async move { me.supervise(proc).await });
            }
        }
        // A host that refused to start (or died for good) ends the wait at once.
        let mut terminated = self.terminated.subscribe();
        tokio::select! {
            ready = async {
                let ready = endpoint_ready(self, self.opts.ready_timeout).await;
                if ready.is_ok() {
                    self.ever_ready.store(true, Ordering::SeqCst);
                }
                ready
            } => ready,
            _ = terminated.wait_for(|done| *done) => Err(std::io::Error::other(
                "the API host was not started (see the message above)",
            )),
        }
    }

    async fn supervise(self: Arc<Self>, mut proc: Box<dyn HostProcess>) {
        enum Event {
            Exited(std::io::Result<Option<i32>>),
            Stop,
            Restart,
        }
        let mut backoff = self.opts.backoff_initial;
        loop {
            let event = tokio::select! {
                status = proc.wait() => Event::Exited(status),
                _ = self.stop.notified() => Event::Stop,
                _ = self.restart.notified() => Event::Restart,
            };
            if matches!(event, Event::Stop) || self.stopping.load(Ordering::SeqCst) {
                let _ = proc.kill().await;
                self.terminated.send_replace(true);
                return;
            }
            match event {
                Event::Restart => {
                    let _ = proc.kill().await;
                    let _ = proc.wait().await;
                    if !self.is_needed() {
                        tracing::info!("No installed plugin needs the API host; stopped it.");
                        *self.started.lock().await = false;
                        let _ = std::fs::remove_file(&self.socket);
                        self.terminated.send_replace(true);
                        return;
                    }
                    tracing::info!("The plugin manifest changed; restarting the API host.");
                    backoff = self.opts.backoff_initial;
                }
                Event::Exited(Ok(Some(meerschaum_core::python_guard::REFUSED_EXIT_CODE)))
                    if !self.ever_ready.load(Ordering::SeqCst) =>
                {
                    // The host's guard said why on stderr (a `meerschaum` this program cannot
                    // work with, before the socket was ever ready): restarting would only print
                    // it again, and so would a launch per request. Remember it.
                    let delay = self.remember_refusal();
                    tracing::error!(
                        "The API host was not started: the Python package `meerschaum` it would import does not fit this program (see the message above); not restarting it, and not asking again for {}s or until the plugins change.",
                        delay.as_secs()
                    );
                    *self.started.lock().await = false;
                    let _ = std::fs::remove_file(&self.socket);
                    self.terminated.send_replace(true);
                    return;
                }
                Event::Exited(status) => {
                    tracing::warn!("The API host exited ({status:?}); restarting in {backoff:?}.");
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = self.stop.notified() => {
                            self.terminated.send_replace(true);
                            return;
                        }
                    }
                    backoff = (backoff * 2).min(self.opts.backoff_max);
                }
                Event::Stop => return,
            }
            loop {
                match self.launch_once().await {
                    Ok(p) => {
                        proc = p;
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to restart the API host: {e}");
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = self.stop.notified() => {
                                self.terminated.send_replace(true);
                                return;
                            }
                        }
                        backoff = (backoff * 2).min(self.opts.backoff_max);
                    }
                }
            }
            if endpoint_ready(&self, self.opts.ready_timeout).await.is_ok() {
                self.ever_ready.store(true, Ordering::SeqCst);
                backoff = self.opts.backoff_initial;
            }
        }
    }

    /// Stop supervising and terminate the host, waiting (up to the termination grace period)
    /// for it to exit so that its own shutdown work (the terminal process) completes.
    pub async fn shutdown(&self) {
        let running = *self.started.lock().await;
        self.stopping.store(true, Ordering::SeqCst);
        self.stop.notify_waiters();
        if running {
            let mut terminated = self.terminated.subscribe();
            let _ = tokio::time::timeout(
                TERMINATE_GRACE + Duration::from_secs(5),
                terminated.wait_for(|done| *done),
            )
            .await;
        }
        let _ = std::fs::remove_file(&self.socket);
    }
}

/// Poll the manifest every `interval` and apply changes: restart, start on demand or stop the
/// host ([`HostSupervisor::set_manifest`]) and refetch the MCP federation's registry.
pub fn spawn_manifest_watcher(
    source: Arc<dyn ManifestSource>,
    supervisor: Arc<HostSupervisor>,
    federation: Option<Arc<crate::mcp::Federation>>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last: Option<HostManifest> = None;
        loop {
            match source.load().await {
                Ok(manifest) => {
                    if last.as_ref() != Some(&manifest) {
                        let running = supervisor.is_running().await;
                        let before = supervisor.starts();
                        let changed = supervisor.set_manifest(&manifest).await;
                        if changed && running && manifest.needs_host() {
                            // Wait for the new process so the registry is not read from the old.
                            let deadline =
                                tokio::time::Instant::now() + supervisor.opts.ready_timeout;
                            while supervisor.starts() == before
                                && tokio::time::Instant::now() < deadline
                            {
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                        }
                        if let Some(f) = federation.as_ref().filter(|_| changed) {
                            f.invalidate().await;
                            if manifest.needs_host() {
                                let _ = f.refresh().await;
                            }
                        }
                        last = Some(manifest);
                    }
                }
                Err(e) => tracing::warn!("Failed to read the plugin manifest: {e}"),
            }
            tokio::time::sleep(interval).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serve `body` as the answer to every request on a fresh loopback port.
    async fn squatter(answer: String) -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let answer = answer.clone();
                tokio::spawn(async move {
                    let mut seen = [0u8; 1024];
                    let _ = stream.read(&mut seen).await;
                    let _ = stream
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                                answer.len()
                            )
                            .as_bytes(),
                        )
                        .await;
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn a_tcp_port_must_prove_it_holds_the_token() {
        let proof = host_proof("secret-token", "abc");
        // The value the Python host computes (`hmac.new(token, nonce, sha256).hexdigest()`).
        assert_eq!(
            proof,
            "ab01d25807daceae7b41624d27e76ff480e9d529a2f43afbcd40817414c61eab"
        );
        assert_eq!(proof.len(), 64);
        assert_ne!(proof, host_proof("other-token", "abc"));
        assert_ne!(proof, host_proof("secret-token", "abd"));
        // A process that took the port and answers anything (or a wrong proof) is refused ...
        for answer in [
            "hello".to_string(),
            "{\"console\": {\"mounted\": true}}".to_string(),
            proof.clone(),
        ] {
            // (the real proof is for another nonce, so it does not match either)
            let addr = squatter(answer.clone()).await;
            let endpoint = HostEndpoint::Tcp {
                addr,
                token: "secret-token".into(),
            };
            assert!(!endpoint_proves_token(&endpoint).await.unwrap(), "{answer}");
        }
        // ... a Unix endpoint needs none ...
        assert!(
            endpoint_proves_token(&HostEndpoint::Unix(PathBuf::from("/nowhere")))
                .await
                .unwrap()
        );
        // ... and nothing listening is an error to retry, not a proof.
        let free = {
            let l = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
            l.local_addr().unwrap()
        };
        assert!(
            endpoint_proves_token(&HostEndpoint::Tcp {
                addr: free,
                token: "t".into()
            })
            .await
            .is_err()
        );
    }

    #[test]
    fn manifest_flags() {
        assert!(!HostManifest::default().needs_host());
        let m: HostManifest = serde_json::from_str(r#"{"api_plugins": ["portal"]}"#).unwrap();
        assert!(m.needs_host());
        let dir = tempfile::tempdir().unwrap();
        assert!(
            !HostManifest::from_file(&dir.path().join("missing.json"))
                .unwrap()
                .needs_host()
        );
    }

    #[test]
    fn socket_paths() {
        let root = tempfile::tempdir().unwrap();
        let short = default_socket_path(root.path());
        assert_eq!(short, root.path().join("api/host/host.sock"));
        let long_root = PathBuf::from(format!("/{}", "x".repeat(200)));
        let long = default_socket_path(&long_root);
        assert!(long.as_os_str().len() < 108, "{long:?}");
        assert_eq!(long, default_socket_path(&long_root));
        assert!(long.ends_with("host.sock"));
        std::fs::remove_dir(long.parent().unwrap()).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn socket_path_avoids_inaccessible_or_foreign_directories() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let root = tempfile::tempdir().unwrap();
        let api = root.path().join("api");
        std::fs::create_dir(&api).unwrap();
        // A regular file obstructs the parent for every UID, including root in CI.
        std::fs::write(api.join("host"), "leave this alone").unwrap();
        let fallback = default_socket_path(root.path());
        assert!(!fallback.starts_with(root.path()));
        assert_eq!(fallback, default_socket_path(root.path()));
        assert_eq!(
            std::fs::read_to_string(api.join("host")).unwrap(),
            "leave this alone"
        );
        let metadata = std::fs::metadata(fallback.parent().unwrap()).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
        assert_eq!(metadata.uid(), rustix::process::geteuid().as_raw());
        std::fs::remove_dir(fallback.parent().unwrap()).unwrap();

        // Real permission denial as an ordinary user; root bypasses mode checks.
        if rustix::process::geteuid().as_raw() != 0 {
            std::fs::remove_file(api.join("host")).unwrap();
            std::fs::set_permissions(&api, std::fs::Permissions::from_mode(0o500)).unwrap();
            let fallback = default_socket_path(root.path());
            assert!(!fallback.starts_with(root.path()));
            assert_eq!(
                std::fs::metadata(&api).unwrap().permissions().mode() & 0o777,
                0o500
            );
            std::fs::set_permissions(&api, std::fs::Permissions::from_mode(0o700)).unwrap();
            std::fs::remove_dir(fallback.parent().unwrap()).unwrap();
        }
    }

    fn host_config(cfg: &crate::config::ServerConfig) -> serde_json::Value {
        let launcher = PythonHostLauncher::for_config(cfg, PathBuf::from("python3"));
        let (_, config) = launcher
            .env
            .iter()
            .find(|(k, _)| k == "MRSM_API_HOST_CONFIG")
            .expect("host config");
        serde_json::from_str(config).unwrap()
    }

    #[test]
    fn host_config_carries_the_console_flags() {
        let root = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::ServerConfig::for_root("sql:main", root.path());
        let config = host_config(&cfg);
        assert_eq!(config["no_dash"], false);
        // The terminal is the Rust server's, whatever the flags say.
        assert_eq!(config["no_webterm"], true);
        cfg.no_webterm = true;
        assert_eq!(host_config(&cfg)["no_webterm"], true);
        cfg.no_webterm = false;
        cfg.no_dash = true;
        let config = host_config(&cfg);
        assert_eq!(config["no_dash"], true);
        assert_eq!(config["no_webterm"], true);
        assert_eq!(config["console"], false);
        assert_eq!(config["webterm"], false);
        // `--secure` (`actions_non_admin: false`) reaches the console: only administrators act.
        assert_eq!(config["actions_non_admin"], true);
        cfg.actions_non_admin = false;
        assert_eq!(host_config(&cfg)["actions_non_admin"], false);
    }

    #[test]
    fn host_config_names_the_console_and_the_server_to_call_back() {
        let root = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::ServerConfig::for_root("sql:main", root.path());
        cfg.host = "0.0.0.0".into();
        cfg.port = 8123;
        let config = host_config(&cfg);
        assert_eq!(config["console"], true);
        assert_eq!(config["webterm"], true);
        assert_eq!(config["server_url"], "http://127.0.0.1:8123");
        cfg.no_webterm = true;
        assert_eq!(host_config(&cfg)["webterm"], false);
        cfg.no_webterm = false;
        cfg.host = "::".into();
        assert_eq!(host_config(&cfg)["server_url"], "http://[::1]:8123");
        cfg.host = "192.0.2.7".into();
        assert_eq!(host_config(&cfg)["server_url"], "http://192.0.2.7:8123");
        cfg.host = "fd00::1".into();
        assert_eq!(host_config(&cfg)["server_url"], "http://[fd00::1]:8123");
    }

    #[test]
    fn endpoints_become_host_arguments_and_environment() {
        let unix = HostEndpoint::from(PathBuf::from("/r/host.sock"));
        assert_eq!(
            PythonHostLauncher::endpoint_args(&unix),
            vec![
                std::ffi::OsString::from("--socket"),
                std::ffi::OsString::from("/r/host.sock")
            ]
        );
        assert!(PythonHostLauncher::endpoint_env(&unix).is_empty());
        let tcp = HostEndpoint::Tcp {
            addr: "127.0.0.1:4242".parse().unwrap(),
            token: "t0k".into(),
        };
        assert_eq!(
            PythonHostLauncher::endpoint_args(&tcp),
            vec![
                std::ffi::OsString::from("--tcp"),
                std::ffi::OsString::from("127.0.0.1:4242")
            ]
        );
        assert_eq!(
            PythonHostLauncher::endpoint_env(&tcp),
            vec![(PROXY_TOKEN_ENV, "t0k".to_string())]
        );
    }

    #[test]
    fn the_transport_is_the_platforms_unless_overridden() {
        // Only the pure parts: the environment is the process's, so no variable is set here.
        if std::env::var_os(TRANSPORT_ENV).is_none() {
            let expected = if cfg!(windows) {
                HostTransport::Tcp
            } else {
                HostTransport::Unix
            };
            assert_eq!(HostTransport::from_env(), expected);
        }
    }

    #[test]
    fn manifest_reasons_include_the_console() {
        assert!(HostManifest::default().reasons().is_empty());
        let console = HostManifest::default().with_console(true);
        assert!(console.needs_host());
        assert_eq!(console.reasons(), vec!["console"]);
        let m: HostManifest =
            serde_json::from_str(r#"{"web_pages": ["a"], "console": true}"#).unwrap();
        assert_eq!(m.reasons(), vec!["web_pages", "console"]);
    }

    /// A launched process that ignores nothing: it exits on SIGTERM, so shutdown returns promptly
    /// without the SIGKILL fallback.
    #[cfg(unix)]
    #[tokio::test]
    async fn child_is_terminated_with_sigterm_first() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("terminated");
        let script = format!(
            "trap 'touch {}; exit 0' TERM; while true; do sleep 0.05; done",
            marker.display()
        );
        let child = tokio::process::Command::new("sh")
            .args(["-c", &script])
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut process = ChildProcess(child);
        // Let the shell install its trap.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let started = std::time::Instant::now();
        process.kill().await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(marker.exists(), "the host did not receive SIGTERM");
    }
}
