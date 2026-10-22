//! One Python worker process and its connection.
//!
//! [`Worker::spawn`] binds a private socket, starts `python -m meerschaum.worker --socket PATH`,
//! accepts its connection and checks the `hello` handshake (protocol version). A reader task
//! demultiplexes the worker's messages: `chunk` / `result` / `error` go to the waiting call,
//! `callback` runs the installed [`CallbackHandler`] and answers with `cb_result`. When the
//! process dies, every pending call fails with [`WorkerError::Crashed`] and nothing else.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use meerschaum_core::pipe::PipeDefaults;
use meerschaum_core::{Pipe, SuccessTuple};
use rmpv::Value as Wire;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};

use crate::callback::{CallbackHandler, CallbackRequest};
use crate::chunk::FetchChunk;
use crate::error::{PyErrorKind, WorkerError, WorkerResult};
use crate::protocol::{Msg, PROTOCOL_VERSION, read_envelope, wire_map_to_core, write_envelope};
use crate::transport::{Listener, ReadHalf, WriteHalf};

/// How worker processes are started.
#[derive(Debug, Clone)]
pub struct WorkerOptions {
    /// The Python interpreter (`MRSM_PYTHON`, else `python3` / `python`).
    pub python: PathBuf,
    /// The module to run (`meerschaum.worker`).
    pub module: String,
    /// Extra environment variables (`MRSM_ROOT_DIR`, `MRSM_PLUGINS_DIR`, `PYTHONPATH`, ...).
    pub env: Vec<(String, String)>,
    /// Environment variables to remove.
    pub env_remove: Vec<String>,
    /// The working directory.
    pub current_dir: Option<PathBuf>,
    /// How long to wait, in all, for the worker to connect and say hello (`MRSM_WORKER_STARTUP_TIMEOUT`
    /// seconds in [`WorkerOptions::default`], 60 by default). A worker that is silent for longer is
    /// killed and the interpreter is refused ([`WorkerError::Refused`]) for 10 seconds (a
    /// timeout alone is never remembered longer or with a growing delay).
    pub startup_timeout: Duration,
    /// How long the preflight check of the interpreter (`python -P -c ...`, reads files, imports
    /// nothing) may take before the interpreter is declared hung.
    pub preflight_timeout: Duration,
    /// Where to create the socket directory (default: the system temporary directory).
    pub socket_dir: Option<PathBuf>,
    /// Allow interactive CLI actions to read the host terminal. Servers leave this disabled.
    pub inherit_stdin: bool,
    /// Discard the worker's stdout / stderr instead of inheriting them.
    pub quiet: bool,
    /// The defaults used to rebuild pipes sent back by the worker.
    pub pipe_defaults: PipeDefaults,
}

/// Whether a start already had the same-major warning enabled (one per program).
static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The first delay after a refusal, and the longest (doubled each time).
pub const REFUSAL_DELAY_INITIAL: Duration = Duration::from_secs(10);
/// See [`REFUSAL_DELAY_INITIAL`].
pub const REFUSAL_DELAY_MAX: Duration = Duration::from_secs(300);

/// Interpreters that cannot host a worker (the preflight or the host refused the `meerschaum`
/// package, the interpreter would not start, or it never connected): asking again at once would
/// fail the same way, slowly, for every caller. So each is left alone, and every caller gets the
/// same message at once, until its delay (10 s, doubling to 5 min, per consecutive failure)
/// passes, a `reload` or a pool recycle.
#[derive(Debug, Default)]
struct Refusals {
    /// `(interpreter, no start before, the next delay, why)`.
    entries: Vec<(PathBuf, Instant, Duration, String)>,
    /// Interpreters whose preflight passed, with what it found (`meerschaum 5.0.0rc1 at ...`).
    checked: Vec<(PathBuf, String)>,
}

impl Refusals {
    /// How long `python` is still refused at `now` (`None`: it may be started), and why.
    fn remaining(&self, python: &Path, now: Instant) -> Option<(Duration, &str)> {
        self.entries
            .iter()
            .find(|(p, _, _, _)| p == python)
            .and_then(|(_, until, _, why)| {
                until.checked_duration_since(now).map(|d| (d, why.as_str()))
            })
            .filter(|(d, _)| !d.is_zero())
    }

    /// Remember a refusal at `now`; the delay before `python` is asked again.
    fn note(&mut self, python: &Path, now: Instant, why: &str) -> Duration {
        self.checked.retain(|(p, _)| p != python);
        match self.entries.iter_mut().find(|(p, _, _, _)| p == python) {
            Some((_, until, delay, message)) => {
                let this = *delay;
                *until = now + this;
                *delay = (this * 2).min(REFUSAL_DELAY_MAX);
                *message = why.to_string();
                this
            }
            None => {
                self.entries.push((
                    python.to_path_buf(),
                    now + REFUSAL_DELAY_INITIAL,
                    (REFUSAL_DELAY_INITIAL * 2).min(REFUSAL_DELAY_MAX),
                    why.to_string(),
                ));
                REFUSAL_DELAY_INITIAL
            }
        }
    }

    /// Remember that `python` did not answer in time: a slow host (a cold first import, a busy
    /// disk, antivirus, a bulk start) is not a broken one, so this is remembered for
    /// [`REFUSAL_DELAY_INITIAL`] only, never grows, and never shortens what a missing or stale
    /// package refusal already remembered. What the preflight found stays valid.
    fn note_timeout(&mut self, python: &Path, now: Instant, why: &str) {
        match self.entries.iter_mut().find(|(p, _, _, _)| p == python) {
            Some((_, until, _, message)) => {
                let end = now + REFUSAL_DELAY_INITIAL;
                if end > *until {
                    *until = end;
                    *message = why.to_string();
                }
            }
            None => self.entries.push((
                python.to_path_buf(),
                now + REFUSAL_DELAY_INITIAL,
                REFUSAL_DELAY_INITIAL,
                why.to_string(),
            )),
        }
    }

    fn checked(&self, python: &Path) -> Option<&str> {
        self.checked
            .iter()
            .find(|(p, _)| p == python)
            .map(|(_, found)| found.as_str())
    }
}

fn refused() -> std::sync::MutexGuard<'static, Refusals> {
    static REFUSED: Mutex<Refusals> = Mutex::new(Refusals {
        entries: Vec::new(),
        checked: Vec::new(),
    });
    REFUSED.lock().unwrap_or_else(|e| e.into_inner())
}

/// Forget refusals and passed preflights, so the next start asks the interpreter again (`reload`,
/// a pool recycle after a manifest change).
pub fn clear_refusals() {
    let mut r = refused();
    r.entries.clear();
    r.checked.clear();
}

/// The error for an interpreter that is still refused (`None`: it may be started).
pub fn refusal(python: &Path) -> Option<WorkerError> {
    refused()
        .remaining(python, Instant::now())
        .map(|(_, why)| WorkerError::Refused(why.to_string()))
}

/// A worker that did not answer in time: see [`Refusals::note_timeout`].
fn refuse_timeout(python: &Path, why: String) -> WorkerError {
    refused().note_timeout(python, Instant::now(), &why);
    WorkerError::Refused(why)
}

/// Refuse `python` for a while and return the error that says why.
fn refuse(python: &Path, why: String) -> WorkerError {
    refused().note(python, Instant::now(), &why);
    WorkerError::Refused(why)
}

/// The interpreter to use: `MRSM_PYTHON`, else `python3` (`python` on Windows).
pub fn default_python() -> PathBuf {
    if let Ok(p) = std::env::var("MRSM_PYTHON")
        && !p.trim().is_empty()
    {
        return PathBuf::from(p);
    }
    PathBuf::from(if cfg!(windows) { "python" } else { "python3" })
}

/// The default of [`WorkerOptions::startup_timeout`]: a healthy worker connects in well under a
/// second, a cold first start in a few, and one on a slow disk, a Raspberry Pi or behind
/// real-time antivirus in tens of seconds (the rc1 limit, kept).
pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

/// The default of [`WorkerOptions::preflight_timeout`].
pub const PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(10);

fn startup_timeout_default() -> Duration {
    std::env::var("MRSM_WORKER_STARTUP_TIMEOUT")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|s| s.is_finite() && *s > 0.0)
        .map(Duration::from_secs_f64)
        .unwrap_or(STARTUP_TIMEOUT)
}

impl Default for WorkerOptions {
    fn default() -> Self {
        WorkerOptions {
            python: default_python(),
            module: "meerschaum.worker".into(),
            env: Vec::new(),
            env_remove: Vec::new(),
            current_dir: None,
            startup_timeout: startup_timeout_default(),
            preflight_timeout: PREFLIGHT_TIMEOUT,
            socket_dir: None,
            quiet: false,
            inherit_stdin: false,
            pipe_defaults: PipeDefaults::default(),
        }
    }
}

/// The worker's `hello` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    /// The worker's protocol version.
    pub protocol: u32,
    /// The worker's process ID.
    pub pid: u32,
    /// `platform.python_version()`.
    pub python: String,
    /// `meerschaum.__version__`.
    pub meerschaum: String,
    /// The MessagePack implementation (`msgpack` or `stdlib`).
    pub msgpack: String,
}

/// Options for one call.
#[derive(Debug, Default, Clone)]
pub struct CallOptions {
    /// Kill the worker and fail the call after this long.
    pub timeout: Option<Duration>,
    /// Stream chunks here instead of collecting them into [`Reply::chunks`].
    pub chunks: Option<mpsc::Sender<FetchChunk>>,
}

/// A successful call's result.
#[derive(Debug)]
pub struct Reply {
    /// The `value` of the `result` message.
    pub value: Wire,
    /// The data frames that followed it.
    pub frames: Vec<Vec<u8>>,
    /// The chunks streamed before the result (unless [`CallOptions::chunks`] was set).
    pub chunks: Vec<FetchChunk>,
}

enum Incoming {
    Chunk(FetchChunk),
    Done(Reply),
    Failed(WorkerError),
}

struct Shared {
    writer: tokio::sync::Mutex<WriteHalf>,
    pending: Mutex<HashMap<u64, mpsc::UnboundedSender<Incoming>>>,
    handler: Mutex<Option<Arc<dyn CallbackHandler>>>,
    alive: AtomicBool,
    next_id: AtomicU64,
    defaults: PipeDefaults,
    dead_tx: watch::Sender<bool>,
}

impl Shared {
    async fn send(&self, msg: Msg, frames: &[Vec<u8>]) -> WorkerResult<()> {
        let mut w = self.writer.lock().await;
        write_envelope(&mut *w, msg, frames).await
    }
}

/// A running worker process.
pub struct Worker {
    shared: Arc<Shared>,
    hello: Hello,
    child: Arc<tokio::sync::Mutex<Option<Child>>>,
    dead_rx: watch::Receiver<bool>,
    reader: tokio::task::JoinHandle<()>,
    _listener: Listener,
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("pid", &self.hello.pid)
            .field("alive", &self.is_alive())
            .finish()
    }
}

fn hello_from(msg: &Msg) -> WorkerResult<Hello> {
    if msg.str("t") != Some("hello") {
        return Err(WorkerError::Handshake(format!(
            "expected `hello`, got {:?}",
            msg.str("t")
        )));
    }
    let protocol = msg.u64("protocol").unwrap_or(0) as u32;
    if protocol != PROTOCOL_VERSION {
        return Err(WorkerError::ProtocolVersion {
            expected: PROTOCOL_VERSION,
            got: protocol,
        });
    }
    Ok(Hello {
        protocol,
        pid: msg.u64("pid").unwrap_or(0) as u32,
        python: msg.str("python").unwrap_or_default().to_string(),
        meerschaum: msg.str("meerschaum").unwrap_or_default().to_string(),
        msgpack: msg.str("msgpack").unwrap_or_default().to_string(),
    })
}

static IN_WORKER: AtomicBool = AtomicBool::new(false);
static PLUGIN_IMPORTS: AtomicUsize = AtomicUsize::new(0);

/// Mark this process as a Python worker's own interpreter (the worker program does it at
/// startup, through the extension).
pub fn set_in_worker(in_worker: bool) {
    IN_WORKER.store(in_worker, Ordering::SeqCst);
}

/// A plugin module starts importing in this process (the code that imports plugins calls this,
/// and [`plugin_import_end`] when done; imports nest and overlap, hence a counter).
pub fn plugin_import_begin() {
    PLUGIN_IMPORTS.fetch_add(1, Ordering::SeqCst);
}

/// A plugin import started with [`plugin_import_begin`] ended.
pub fn plugin_import_end() {
    let mut current = PLUGIN_IMPORTS.load(Ordering::SeqCst);
    while current > 0 {
        match PLUGIN_IMPORTS.compare_exchange_weak(
            current,
            current - 1,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

static SPAWN_HOOK: std::sync::OnceLock<fn()> = std::sync::OnceLock::new();
static OUTPUT_HOOK: std::sync::OnceLock<fn(bool, &[u8])> = std::sync::OnceLock::new();

/// Forward non-quiet worker output through a host writer. The boolean selects stderr.
/// Install before spawning workers. Interactive shells use this to clear live progress before
/// writing worker bytes and resume it only after a complete line.
pub fn set_output_hook(hook: fn(bool, &[u8])) {
    let _ = OUTPUT_HOOK.set(hook);
}

async fn forward_output(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    stderr: bool,
    hook: fn(bool, &[u8]),
) {
    use tokio::io::AsyncReadExt;
    let mut buffer = [0u8; 8192];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(count) => hook(stderr, &buffer[..count]),
        }
    }
}

/// Register a function that runs just before every non-quiet worker starts (once per process;
/// later calls are ignored). A worker's output goes straight to the inherited terminal, so the
/// CLI uses this to stop its spinner from redrawing over it.
pub fn set_spawn_hook(hook: fn()) {
    let _ = SPAWN_HOOK.set(hook);
}

/// Whether starting a worker is refused: in a worker's own interpreter, while a plugin is being
/// imported. A plugin whose import syncs a `plugin:` pipe (or calls `mrsm.entry()`) would
/// otherwise start workers without end, each importing the plugin again. Nothing else is
/// refused: a plugin action may run other plugins' actions and sync plugin pipes, and the
/// processes that are not workers start workers freely.
pub fn spawn_refused() -> bool {
    IN_WORKER.load(Ordering::SeqCst) && PLUGIN_IMPORTS.load(Ordering::SeqCst) > 0
}

/// Check, before a worker is started, that `opts.python` can host one: it starts, finds a
/// `meerschaum` this program works with (`python_guard`), and answers within
/// [`WorkerOptions::preflight_timeout`]. Reads files only (nothing of `meerschaum` is imported)
/// and costs one interpreter start per interpreter per program. A failure is ONE message that
/// names the interpreter, the package found and the fix, shared by every caller (the
/// interpreter is not asked again for 10 s, doubling), instead of a worker that never connects.
async fn preflight(opts: &WorkerOptions) -> WorkerResult<()> {
    // One check at a time: concurrent first calls share the answer instead of each paying
    // (or timing out) for it.
    static GATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _gate = GATE.lock().await;
    if let Some(e) = refusal(&opts.python) {
        return Err(e);
    }
    if refused().checked(&opts.python).is_some() {
        return Ok(());
    }
    let python = opts.python.display().to_string();
    let version = meerschaum_core::python_guard::expected_version();
    let fix = format!(
        "Install the matching release (`{python} -m pip install 'meerschaum=={version}'`) or set \
         MRSM_PYTHON to an interpreter that has it."
    );
    let mut cmd = Command::new(&opts.python);
    cmd.arg(meerschaum_core::self_exe::PYTHON_SAFE_PATH_FLAG)
        .arg("-c")
        .arg(format!(
            "{}\n{}\n_mrsm_preflight()",
            meerschaum_core::python_guard::SCRIPT,
            include_str!("../python/preflight.py")
        ))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for k in &opts.env_remove {
        cmd.env_remove(k);
    }
    cmd.envs(meerschaum_core::python_guard::expect_env());
    cmd.envs(opts.env.iter().map(|(k, v)| (k, v)));
    if let Some(dir) = &opts.current_dir {
        cmd.current_dir(dir);
    }
    let child = cmd.spawn().map_err(|e| {
        refuse(
            &opts.python,
            format!(
                "The Python interpreter `{python}` could not be started ({e}). The `meerschaum` \
                 program needs Python 3.11 or newer with the `meerschaum=={version}` package for \
                 plugins, `plugin:` connectors and Python actions: install it, or set MRSM_PYTHON \
                 to such an interpreter."
            ),
        )
    })?;
    let out = match tokio::time::timeout(opts.preflight_timeout, child.wait_with_output()).await {
        Ok(Ok(out)) => out,
        Ok(Err(e)) => return Err(WorkerError::Io(e)),
        Err(_) => {
            // `kill_on_drop` stopped it.
            return Err(refuse(
                &opts.python,
                format!(
                    "The Python interpreter `{python}` did not answer a start-up check within \
                     {secs:.0} s (it hangs before running anything: a wrapper script, `.pth` file \
                     or `sitecustomize` that blocks, or a stalled file system). Set MRSM_PYTHON to \
                     a working interpreter that has `meerschaum=={version}`.",
                    secs = opts.preflight_timeout.as_secs_f64(),
                ),
            ));
        }
    };
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stderr = stderr.trim();
    if out.status.code() == Some(meerschaum_core::python_guard::REFUSED_EXIT_CODE) {
        let why = if stderr.is_empty() {
            format!(
                "The Python package `meerschaum` found by `{python}` does not fit this program ({version}). {fix}"
            )
        } else {
            stderr.to_string()
        };
        return Err(refuse(&opts.python, why));
    }
    if !out.status.success() {
        let tail: Vec<&str> = stderr.lines().rev().take(3).collect();
        let tail = tail.into_iter().rev().collect::<Vec<_>>().join(" | ");
        return Err(refuse(
            &opts.python,
            format!(
                "The Python interpreter `{python}` failed a start-up check ({}{}). The `meerschaum` \
                 workers need Python 3.11 or newer. Use a newer interpreter that has \
                 `meerschaum=={version}`: set MRSM_PYTHON to it.",
                out.status,
                if tail.is_empty() {
                    String::new()
                } else {
                    format!(": {tail}")
                },
            ),
        ));
    }
    let found: serde_json::Value = String::from_utf8_lossy(&out.stdout)
        .lines()
        .last()
        .and_then(|l| serde_json::from_str(l).ok())
        .unwrap_or_default();
    let found = format!(
        "meerschaum {} at {}",
        found
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("(unknown version)"),
        found
            .get("directory")
            .and_then(|v| v.as_str())
            .unwrap_or("?"),
    );
    refused().checked.push((opts.python.clone(), found));
    Ok(())
}

impl Worker {
    /// Start a worker and complete the handshake. Refused (`WorkerError::Spawn`) while
    /// [`spawn_refused`].
    pub async fn spawn(opts: &WorkerOptions) -> WorkerResult<Worker> {
        if spawn_refused() {
            return Err(WorkerError::Spawn(
                "not starting a Python worker while a plugin is being imported (a plugin whose \
                 import syncs a plugin pipe or calls `mrsm.entry()` would recurse)"
                    .into(),
            ));
        }
        if let Some(e) = refusal(&opts.python) {
            return Err(e);
        }
        if opts.module == "meerschaum.worker" {
            preflight(opts).await?;
        }
        if !opts.quiet
            && OUTPUT_HOOK.get().is_none()
            && let Some(hook) = SPAWN_HOOK.get()
        {
            hook();
        }
        let listener = Listener::bind(opts.socket_dir.as_deref())?;
        let mut cmd = Command::new(&opts.python);
        // `-P`: the working directory (the user's, a project's) stays off `sys.path`, where a
        // `meerschaum.py` or `meerschaum/` would replace the package (`-c` and `-m` both put it
        // first). Plugins are imported through the plugins directory, never the working one.
        cmd.arg(meerschaum_core::self_exe::PYTHON_SAFE_PATH_FLAG);
        if opts.module == "meerschaum.worker" {
            cmd.arg("-c").arg(format!(
                "{}\n_mrsm_guard()\n{}",
                meerschaum_core::python_guard::SCRIPT,
                include_str!("../python/host.py")
            ));
        } else {
            cmd.arg("-m").arg(&opts.module);
        }
        cmd.arg("--socket")
            .arg(listener.address())
            .stdin(if opts.inherit_stdin {
                Stdio::inherit()
            } else {
                Stdio::null()
            })
            .kill_on_drop(true);
        if opts.quiet {
            cmd.stdout(Stdio::null()).stderr(Stdio::null());
        } else if OUTPUT_HOOK.get().is_some() {
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
            cmd.env("PYTHONUNBUFFERED", "1");
        }
        for k in &opts.env_remove {
            cmd.env_remove(k);
        }
        // The host refuses a `meerschaum` this program cannot work with, before importing it
        // (`meerschaum_core::python_guard`); one start per program may warn about another release.
        if opts.module == "meerschaum.worker" {
            cmd.envs(meerschaum_core::python_guard::expect_env());
            if !WARNED.swap(true, std::sync::atomic::Ordering::SeqCst) {
                cmd.env(meerschaum_core::python_guard::WARN_ENV, "1");
            }
        }
        cmd.envs(opts.env.iter().map(|(k, v)| (k, v)));
        if let Some(dir) = &opts.current_dir {
            cmd.current_dir(dir);
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| WorkerError::Spawn(format!("{}: {e}", opts.python.display())))?;
        if let Some(&hook) = OUTPUT_HOOK.get() {
            if let Some(stdout) = child.stdout.take() {
                tokio::spawn(forward_output(stdout, false, hook));
            }
            if let Some(stderr) = child.stderr.take() {
                tokio::spawn(forward_output(stderr, true, hook));
            }
        }

        // One deadline for the connection and the `hello` together.
        let deadline = tokio::time::Instant::now() + opts.startup_timeout;
        let silent = |what: &str| {
            let found = refused()
                .checked(&opts.python)
                .map(|f| format!(" ({f})"))
                .unwrap_or_default();
            refuse_timeout(
                &opts.python,
                format!(
                    "The Python worker started by `{python}`{found} {what} within {secs:.0} s, so it \
                     was stopped (a worker normally connects in well under a second). Check that \
                     `{python} -c 'import meerschaum'` returns quickly and that nothing in its \
                     environment (a plugin, `sitecustomize`, a `.pth` file) blocks at start-up, or \
                     set MRSM_PYTHON to another interpreter that has `meerschaum=={version}` \
                     (`MRSM_WORKER_STARTUP_TIMEOUT` raises the limit).",
                    python = opts.python.display(),
                    secs = opts.startup_timeout.as_secs_f64(),
                    version = meerschaum_core::python_guard::expected_version(),
                ),
            )
        };
        let accepted = tokio::select! {
            res = tokio::time::timeout_at(deadline, listener.accept()) => match res {
                Ok(Ok(halves)) => halves,
                Ok(Err(e)) => return Err(WorkerError::Io(e)),
                Err(_) => {
                    let _ = child.kill().await;
                    return Err(silent("did not connect"));
                }
            },
            status = child.wait() => {
                if status.as_ref().is_ok_and(|s| {
                    s.code() == Some(meerschaum_core::python_guard::REFUSED_EXIT_CODE)
                }) && opts.module == "meerschaum.worker" {
                    // The host said why on stderr (the preflight normally catches this first,
                    // with the message in the error). Do not start another one for this
                    // interpreter until `clear_refusals` (`reload`).
                    return Err(refuse(
                        &opts.python,
                        format!(
                            "The Python worker host started by `{python}` refused to start \
                             (its message is above): the `meerschaum` package it would import \
                             does not fit this program ({version}). Install the matching \
                             release (`{python} -m pip install 'meerschaum=={version}'`) or set \
                             MRSM_PYTHON to an interpreter that has it.",
                            python = opts.python.display(),
                            version = meerschaum_core::python_guard::expected_version(),
                        ),
                    ));
                }
                return Err(WorkerError::Spawn(format!(
                    "the worker started by `{python}` exited before connecting ({status}); its \
                     output is above. Check that `{python} -c 'import meerschaum.worker'` works, \
                     or set MRSM_PYTHON to an interpreter with `meerschaum=={version}`",
                    python = opts.python.display(),
                    status = status.map(|s| s.to_string()).unwrap_or_else(|e| e.to_string()),
                    version = meerschaum_core::python_guard::expected_version(),
                )));
            }
        };
        let (mut reader, writer): (ReadHalf, WriteHalf) = accepted;
        let hello = match tokio::time::timeout_at(deadline, read_envelope(&mut reader)).await {
            Ok(Ok(env)) => hello_from(&env.msg),
            Ok(Err(e)) => Err(WorkerError::Handshake(e.to_string())),
            Err(_) => Err(silent("connected but sent no `hello`")),
        };
        let hello = match hello {
            Ok(h) => h,
            Err(e) => {
                let _ = child.kill().await;
                return Err(e);
            }
        };

        let (dead_tx, dead_rx) = watch::channel(false);
        let shared = Arc::new(Shared {
            writer: tokio::sync::Mutex::new(writer),
            pending: Mutex::new(HashMap::new()),
            handler: Mutex::new(None),
            alive: AtomicBool::new(true),
            next_id: AtomicU64::new(1),
            defaults: opts.pipe_defaults.clone(),
            dead_tx,
        });
        let reader_task = tokio::spawn(read_loop(Arc::clone(&shared), reader));
        tracing::debug!(pid = hello.pid, "Python worker started");
        Ok(Worker {
            shared,
            hello,
            child: Arc::new(tokio::sync::Mutex::new(Some(child))),
            dead_rx,
            reader: reader_task,
            _listener: listener,
        })
    }

    /// The handshake.
    pub fn hello(&self) -> &Hello {
        &self.hello
    }

    /// The worker's process ID.
    pub fn pid(&self) -> u32 {
        self.hello.pid
    }

    /// Whether the connection is still open.
    pub fn is_alive(&self) -> bool {
        self.shared.alive.load(Ordering::SeqCst)
    }

    /// Install (or clear) the handler for callbacks from plugin code.
    pub fn set_callback_handler(&self, handler: Option<Arc<dyn CallbackHandler>>) {
        *self
            .shared
            .handler
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = handler;
    }

    /// Wait until the worker's connection closes (the process exited or was killed).
    pub async fn closed(&self) {
        let mut rx = self.dead_rx.clone();
        let _ = rx.wait_for(|dead| *dead).await;
    }

    async fn exit_detail(&self) -> String {
        let mut guard = self.child.lock().await;
        if let Some(child) = guard.as_mut()
            && let Ok(Ok(status)) = tokio::time::timeout(Duration::from_secs(2), child.wait()).await
        {
            return format!(" ({status})");
        }
        String::new()
    }

    /// Run `op` with `args` (and data `frames`) and wait for its result.
    pub async fn call(
        &self,
        op: &str,
        args: Msg,
        frames: Vec<Vec<u8>>,
        opts: CallOptions,
    ) -> WorkerResult<Reply> {
        if !self.is_alive() {
            return Err(WorkerError::Crashed {
                op: op.to_string(),
                detail: format!(
                    "{} (the worker had already exited)",
                    self.exit_detail().await
                ),
            });
        }
        let id = self.shared.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.shared
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, tx);
        let msg = Msg::new()
            .set("t", "call")
            .set("id", id)
            .set("op", op)
            .set("args", Wire::from(args));
        if let Err(e) = self.shared.send(msg, &frames).await {
            self.forget(id);
            return Err(match e {
                WorkerError::Io(_) => WorkerError::Crashed {
                    op: op.to_string(),
                    detail: self.exit_detail().await,
                },
                other => other,
            });
        }

        let chunk_sink = opts.chunks.clone();
        let wait = async {
            let mut chunks = Vec::new();
            let r: Result<Reply, Option<WorkerError>> = loop {
                match rx.recv().await {
                    Some(Incoming::Chunk(c)) => match &chunk_sink {
                        Some(sink) => {
                            let _ = sink.send(c).await;
                        }
                        None => chunks.push(c),
                    },
                    Some(Incoming::Done(mut reply)) => {
                        reply.chunks = chunks;
                        break Ok(reply);
                    }
                    Some(Incoming::Failed(e)) => break Err(Some(e)),
                    None => break Err(None),
                }
            };
            r
        };
        let outcome = match opts.timeout {
            Some(t) => match tokio::time::timeout(t, wait).await {
                Ok(r) => r,
                Err(_) => {
                    self.forget(id);
                    self.kill().await;
                    return Err(WorkerError::Timeout {
                        op: op.to_string(),
                        after: t,
                    });
                }
            },
            None => wait.await,
        };
        match outcome {
            Ok(reply) => Ok(reply),
            Err(Some(e)) => Err(e),
            Err(None) => Err(WorkerError::Crashed {
                op: op.to_string(),
                detail: self.exit_detail().await,
            }),
        }
    }

    fn forget(&self, id: u64) {
        self.shared
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
    }

    /// Kill the process immediately.
    pub async fn kill(&self) {
        self.shared.alive.store(false, Ordering::SeqCst);
        if let Some(child) = self.child.lock().await.as_mut() {
            let _ = child.kill().await;
        }
    }

    /// Ask the worker to exit, then kill it if it has not exited within `grace`.
    pub async fn shutdown(self, grace: Duration) {
        if self.is_alive() {
            let _ = tokio::time::timeout(
                grace,
                self.call("shutdown", Msg::new(), Vec::new(), CallOptions::default()),
            )
            .await;
        }
        let mut guard = self.child.lock().await;
        if let Some(child) = guard.as_mut()
            && tokio::time::timeout(grace, child.wait()).await.is_err()
        {
            let _ = child.kill().await;
        }
        self.reader.abort();
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.reader.abort();
        if let Ok(mut guard) = self.child.try_lock()
            && let Some(child) = guard.as_mut()
        {
            let _ = child.start_kill();
        }
    }
}

fn python_error(msg: &Msg) -> WorkerError {
    WorkerError::Python {
        kind: PyErrorKind::parse(msg.str("kind").unwrap_or("Exception")),
        exception: msg.str("exception").unwrap_or("Exception").to_string(),
        message: msg.str("message").unwrap_or_default().to_string(),
        traceback: msg.str("traceback").map(String::from),
    }
}

async fn read_loop(shared: Arc<Shared>, mut reader: ReadHalf) {
    loop {
        let env = match read_envelope(&mut reader).await {
            Ok(env) => env,
            Err(e) => {
                tracing::debug!("worker connection closed: {e}");
                break;
            }
        };
        let mut msg = env.msg;
        let mut frames = env.frames;
        let id = msg.u64("id").unwrap_or(0);
        let route = |incoming: Incoming, done: bool| {
            let mut pending = shared.pending.lock().unwrap_or_else(|e| e.into_inner());
            let tx = if done {
                pending.remove(&id)
            } else {
                pending.get(&id).cloned()
            };
            if let Some(tx) = tx {
                let _ = tx.send(incoming);
            }
        };
        match msg.str("t") {
            Some("chunk") => {
                let kind = msg.str("kind").unwrap_or("none").to_string();
                let value = msg.take("value").unwrap_or(Wire::Nil);
                match FetchChunk::from_wire(&kind, &value, &mut frames) {
                    Ok(Some(chunk)) => route(Incoming::Chunk(chunk), false),
                    Ok(None) => {}
                    Err(e) => route(Incoming::Failed(e), true),
                }
            }
            Some("result") => {
                let value = msg.take("value").unwrap_or(Wire::Nil);
                route(
                    Incoming::Done(Reply {
                        value,
                        frames,
                        chunks: Vec::new(),
                    }),
                    true,
                );
            }
            Some("error") => route(Incoming::Failed(python_error(&msg)), true),
            Some("callback") => {
                let shared = Arc::clone(&shared);
                tokio::spawn(async move {
                    let cb = msg.u64("cb").unwrap_or(0);
                    let result = run_callback(&shared, msg, frames).await;
                    let reply = Msg::new().set("t", "cb_result").set("cb", cb).set(
                        "value",
                        Wire::Array(vec![
                            Wire::Boolean(result.success),
                            Wire::from(result.message.as_str()),
                        ]),
                    );
                    if let Err(e) = shared.send(reply, &[]).await {
                        tracing::warn!("failed to answer a worker callback: {e}");
                    }
                });
            }
            other => tracing::debug!("ignoring worker message {other:?}"),
        }
    }
    shared.alive.store(false, Ordering::SeqCst);
    // Dropping the senders fails every pending call with `Crashed`.
    shared
        .pending
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
    let _ = shared.dead_tx.send(true);
}

async fn run_callback(shared: &Shared, mut msg: Msg, mut frames: Vec<Vec<u8>>) -> SuccessTuple {
    let handler = shared
        .handler
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    let Some(handler) = handler else {
        return SuccessTuple::fail(
            "No callback handler is installed for this worker; `pipe.sync()` cannot run here.",
        );
    };
    let op = msg.str("op").unwrap_or("sync").to_string();
    let call_id = msg.u64("id").unwrap_or(0);
    let args = match msg.take("args") {
        Some(Wire::Map(m)) => Msg(m),
        _ => Msg::new(),
    };
    let pipe = match args.get("pipe") {
        Some(Wire::Map(m)) => match Pipe::from_state(&wire_map_to_core(m), &shared.defaults) {
            Ok(p) => Some(p),
            Err(e) => return SuccessTuple::fail(format!("Invalid pipe from the worker: {e}")),
        },
        _ => None,
    };
    let kind = args.str("kind").unwrap_or("none").to_string();
    let chunk =
        match FetchChunk::from_wire(&kind, args.get("value").unwrap_or(&Wire::Nil), &mut frames) {
            Ok(c) => c,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
    let kwargs = match args.get("kwargs") {
        Some(Wire::Map(m)) => wire_map_to_core(m),
        _ => Default::default(),
    };
    handler
        .handle(CallbackRequest {
            op,
            call_id,
            pipe,
            chunk,
            kwargs,
        })
        .await
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_refusal_expires_with_a_doubling_delay_and_is_cleared_by_reload() {
        use super::*;
        let python = Path::new("/usr/bin/python3");
        let other = Path::new("/opt/python");
        let t0 = Instant::now();
        let left = |r: &Refusals, p: &Path, t: Instant| r.remaining(p, t).map(|(d, _)| d);
        let mut r = Refusals::default();
        assert_eq!(left(&r, python, t0), None, "nothing refused yet");
        // First refusal: 10 s.
        assert_eq!(r.note(python, t0, "why"), Duration::from_secs(10));
        assert_eq!(left(&r, python, t0), Some(Duration::from_secs(10)));
        assert_eq!(r.remaining(python, t0).map(|(_, why)| why), Some("why"));
        assert!(left(&r, python, t0 + Duration::from_secs(9)).is_some());
        // Another interpreter is not refused.
        assert_eq!(left(&r, other, t0), None);
        // It recovers on its own once the delay passes.
        assert_eq!(left(&r, python, t0 + Duration::from_secs(10)), None);
        // Refused again: 20 s, 40 s, ... capped at 5 min.
        let t1 = t0 + Duration::from_secs(11);
        assert_eq!(r.note(python, t1, "again"), Duration::from_secs(20));
        assert_eq!(r.remaining(python, t1).map(|(_, why)| why), Some("again"));
        assert!(left(&r, python, t1 + Duration::from_secs(19)).is_some());
        assert_eq!(left(&r, python, t1 + Duration::from_secs(20)), None);
        let mut delay = Duration::ZERO;
        for _ in 0..10 {
            delay = r.note(python, t1, "again");
        }
        assert_eq!(delay, REFUSAL_DELAY_MAX);
        // `reload` (clear) forgets everything at once.
        r.entries.clear();
        assert_eq!(left(&r, python, t1), None);
        assert_eq!(r.note(python, t1, "why"), REFUSAL_DELAY_INITIAL);
    }

    #[test]
    fn a_start_up_timeout_is_remembered_briefly_and_never_grows() {
        use super::*;
        let python = Path::new("/usr/bin/python3");
        let t0 = Instant::now();
        let left = |r: &Refusals, t: Instant| r.remaining(python, t).map(|(d, _)| d);
        let mut r = Refusals::default();
        r.checked
            .push((python.to_path_buf(), "meerschaum 5".into()));
        // Every timeout is remembered for 10 s, however many there were.
        for _ in 0..8 {
            r.note_timeout(python, t0, "slow");
        }
        assert_eq!(left(&r, t0), Some(Duration::from_secs(10)));
        assert_eq!(left(&r, t0 + Duration::from_secs(10)), None);
        r.note_timeout(python, t0 + Duration::from_secs(11), "slow again");
        assert_eq!(
            left(&r, t0 + Duration::from_secs(11)),
            Some(Duration::from_secs(10))
        );
        // The preflight's finding stays: the interpreter is fine, just slow.
        assert_eq!(r.checked(python), Some("meerschaum 5"));
        // A refusal for a missing or stale package keeps its doubling memory, and a timeout
        // never shortens it.
        let t1 = t0 + Duration::from_secs(100);
        assert_eq!(r.note(python, t1, "stale package"), Duration::from_secs(10));
        assert_eq!(r.note(python, t1, "stale package"), Duration::from_secs(20));
        r.note_timeout(python, t1, "slow");
        assert_eq!(left(&r, t1), Some(Duration::from_secs(20)));
        assert_eq!(
            r.remaining(python, t1).map(|(_, why)| why),
            Some("stale package")
        );
    }

    #[test]
    fn the_process_wide_registry_is_cleared_by_clear_refusals() {
        let python = std::path::Path::new("/w17c/process-wide-registry-python");
        super::refused().note(python, std::time::Instant::now(), "why");
        assert!(super::refusal(python).is_some());
        super::refused()
            .checked
            .push((python.to_path_buf(), "found".into()));
        super::clear_refusals();
        assert!(super::refusal(python).is_none());
        assert!(super::refused().checked(python).is_none());
    }

    use super::*;

    #[test]
    fn handshake_checks_the_protocol_version() {
        let hello = Msg::new()
            .set("t", "hello")
            .set("protocol", PROTOCOL_VERSION as u64)
            .set("pid", 42u64)
            .set("msgpack", "stdlib");
        let h = hello_from(&hello).unwrap();
        assert_eq!((h.pid, h.msgpack.as_str()), (42, "stdlib"));

        let newer = hello.clone().set("protocol", PROTOCOL_VERSION as u64 + 1);
        assert!(matches!(
            hello_from(&newer),
            Err(WorkerError::ProtocolVersion { .. })
        ));
        let wrong = Msg::new().set("t", "result");
        assert!(matches!(hello_from(&wrong), Err(WorkerError::Handshake(_))));
    }
}
