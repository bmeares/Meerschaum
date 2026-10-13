//! The Python API host lifecycle (§7.3, D17): start `python -m meerschaum.api.host` on a Unix
//! socket under the root directory only when the plugin manifest says an installed plugin needs
//! it, supervise it like a job, and restart it with backoff when it crashes.
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
}

impl HostManifest {
    /// Whether any installed plugin needs the API host.
    pub fn needs_host(&self) -> bool {
        !(self.api_plugins.is_empty()
            && self.dash_plugins.is_empty()
            && self.web_pages.is_empty()
            && self.mcp_tools.is_empty())
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

/// The longest socket path used as is: `sun_path` holds 108 bytes on Linux and 104 on macOS,
/// including the terminating NUL.
pub const MAX_SOCKET_PATH: usize = 100;

/// The host's socket for a root directory: `<root>/api/host/host.sock`, or, when that path is
/// too long for a Unix socket, a per-root name under `$XDG_RUNTIME_DIR` (else the temporary
/// directory).
///
/// The socket's directory is private (`0700`, see [`HostSupervisor`]): uvicorn makes the socket
/// itself world-writable, and a local process that could connect to it would bypass the
/// server's authentication, allowlist and rate limits.
pub fn default_socket_path(root: &Path) -> PathBuf {
    let p = root.join("api").join("host").join("host.sock");
    if p.as_os_str().len() <= MAX_SOCKET_PATH {
        return p;
    }
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    root.hash(&mut h);
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|d| d.is_dir())
        .unwrap_or_else(std::env::temp_dir);
    base.join(format!("mrsm-host-{:016x}", h.finish()))
        .join("host.sock")
}

fn private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
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
}

/// Launches `python -m meerschaum.api.host --socket <path>`.
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
    /// `--no-auth`, `--production`, `--private` and `--debug` flags (`MRSM_API_HOST_CONFIG`,
    /// read by `meerschaum.api` in host mode).
    pub fn for_config(cfg: &crate::config::ServerConfig, python: PathBuf) -> PythonHostLauncher {
        let host_config = serde_json::json!({
            "mrsm_instance": cfg.instance_keys,
            "no_auth": cfg.no_auth,
            "production": cfg.production,
            "private": cfg.private,
            "debug": cfg.debug,
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

struct ChildProcess(tokio::process::Child);

#[async_trait]
impl HostProcess for ChildProcess {
    async fn wait(&mut self) -> std::io::Result<Option<i32>> {
        Ok(self.0.wait().await?.code())
    }
    async fn kill(&mut self) -> std::io::Result<()> {
        self.0.kill().await
    }
}

#[async_trait]
impl HostLauncher for PythonHostLauncher {
    async fn launch(&self, socket: &Path) -> std::io::Result<Box<dyn HostProcess>> {
        let child = tokio::process::Command::new(&self.python)
            .args(["-m", "meerschaum.api.host", "--socket"])
            .arg(socket)
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
    socket: PathBuf,
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
}

impl std::fmt::Debug for HostSupervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostSupervisor")
            .field("socket", &self.socket)
            .field("starts", &self.starts.load(Ordering::SeqCst))
            .finish()
    }
}

async fn socket_ready(socket: &Path, timeout: Duration) -> std::io::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if crate::proxy::connect_host(socket).await.is_ok() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "the API host did not start listening in time",
            ));
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
        Arc::new(HostSupervisor {
            launcher,
            socket: socket.into(),
            opts,
            started: Mutex::new(false),
            starts: AtomicUsize::new(0),
            stopping: AtomicBool::new(false),
            stop: Notify::new(),
            needed: AtomicBool::new(manifest.needs_host()),
            manifest: std::sync::Mutex::new(manifest.clone()),
            restart: Notify::new(),
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
        if *self.started.lock().await {
            self.restart.notify_one();
        }
        true
    }

    /// The host's socket.
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// How many times the host has been started (1 + restarts).
    pub fn starts(&self) -> usize {
        self.starts.load(Ordering::SeqCst)
    }

    async fn launch_once(&self) -> std::io::Result<Box<dyn HostProcess>> {
        let _ = std::fs::remove_file(&self.socket);
        if let Some(parent) = self.socket.parent() {
            private_dir(parent)?;
        }
        let proc = self.launcher.launch(&self.socket).await?;
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
                let proc = self.launch_once().await?;
                *started = true;
                let me = self.clone();
                tokio::spawn(async move { me.supervise(proc).await });
            }
        }
        socket_ready(&self.socket, self.opts.ready_timeout).await
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
                        return;
                    }
                    tracing::info!("The plugin manifest changed; restarting the API host.");
                    backoff = self.opts.backoff_initial;
                }
                Event::Exited(status) => {
                    tracing::warn!("The API host exited ({status:?}); restarting in {backoff:?}.");
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = self.stop.notified() => return,
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
                            _ = self.stop.notified() => return,
                        }
                        backoff = (backoff * 2).min(self.opts.backoff_max);
                    }
                }
            }
            if socket_ready(&self.socket, self.opts.ready_timeout)
                .await
                .is_ok()
            {
                backoff = self.opts.backoff_initial;
            }
        }
    }

    /// Stop supervising and terminate the host.
    pub async fn shutdown(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        self.stop.notify_waiters();
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
        let short = default_socket_path(Path::new("/srv/mrsm"));
        assert_eq!(short, PathBuf::from("/srv/mrsm/api/host/host.sock"));
        let long_root = PathBuf::from(format!("/{}", "x".repeat(200)));
        let long = default_socket_path(&long_root);
        assert!(long.as_os_str().len() < 108, "{long:?}");
        assert_eq!(long, default_socket_path(&long_root));
        assert!(long.ends_with("host.sock"));
    }
}
