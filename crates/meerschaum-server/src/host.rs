//! The Python API host lifecycle (§7.3, D17): start `python -m meerschaum.api.host` on a Unix
//! socket under the root directory only when the plugin manifest says an installed plugin needs
//! it, supervise it like a job, and restart it with backoff when it crashes.
//!
//! Launching is behind [`HostLauncher`] so tests (and embedders) can substitute a fake host.

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
        if tokio::net::UnixStream::connect(socket).await.is_ok() {
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
        manifest.needs_host().then(|| {
            Arc::new(HostSupervisor {
                launcher,
                socket: socket.into(),
                opts,
                started: Mutex::new(false),
                starts: AtomicUsize::new(0),
                stopping: AtomicBool::new(false),
                stop: Notify::new(),
            })
        })
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
            std::fs::create_dir_all(parent)?;
        }
        let proc = self.launcher.launch(&self.socket).await?;
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(proc)
    }

    /// Start the host if it is not running, then wait until its socket accepts connections.
    pub async fn ensure_started(self: &Arc<Self>) -> std::io::Result<()> {
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
        let mut backoff = self.opts.backoff_initial;
        loop {
            let exited = tokio::select! {
                status = proc.wait() => Some(status),
                _ = self.stop.notified() => None,
            };
            if exited.is_none() || self.stopping.load(Ordering::SeqCst) {
                let _ = proc.kill().await;
                return;
            }
            tracing::warn!("The API host exited ({exited:?}); restarting in {backoff:?}.");
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = self.stop.notified() => return,
            }
            backoff = (backoff * 2).min(self.opts.backoff_max);
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
}
