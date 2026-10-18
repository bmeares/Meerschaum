//! One Python worker process and its connection.
//!
//! [`Worker::spawn`] binds a private socket, starts `python -m meerschaum.worker --socket PATH`,
//! accepts its connection and checks the `hello` handshake (protocol version). A reader task
//! demultiplexes the worker's messages: `chunk` / `result` / `error` go to the waiting call,
//! `callback` runs the installed [`CallbackHandler`] and answers with `cb_result`. When the
//! process dies, every pending call fails with [`WorkerError::Crashed`] and nothing else.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    /// How long to wait for the worker to connect and say hello.
    pub startup_timeout: Duration,
    /// Where to create the socket directory (default: the system temporary directory).
    pub socket_dir: Option<PathBuf>,
    /// Allow interactive CLI actions to read the host terminal. Servers leave this disabled.
    pub inherit_stdin: bool,
    /// Discard the worker's stdout / stderr instead of inheriting them.
    pub quiet: bool,
    /// The defaults used to rebuild pipes sent back by the worker.
    pub pipe_defaults: PipeDefaults,
}

/// The interpreter to use: `MRSM_PYTHON`, else `python3` (`python` on Windows).
pub fn default_python() -> PathBuf {
    if let Ok(p) = std::env::var("MRSM_PYTHON") {
        if !p.trim().is_empty() {
            return PathBuf::from(p);
        }
    }
    PathBuf::from(if cfg!(windows) { "python" } else { "python3" })
}

impl Default for WorkerOptions {
    fn default() -> Self {
        WorkerOptions {
            python: default_python(),
            module: "meerschaum.worker".into(),
            env: Vec::new(),
            env_remove: Vec::new(),
            current_dir: None,
            startup_timeout: Duration::from_secs(60),
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
        if !opts.quiet
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
            cmd.arg("-c").arg(include_str!("../python/host.py"));
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
        }
        for k in &opts.env_remove {
            cmd.env_remove(k);
        }
        cmd.envs(opts.env.iter().map(|(k, v)| (k, v)));
        if let Some(dir) = &opts.current_dir {
            cmd.current_dir(dir);
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| WorkerError::Spawn(format!("{}: {e}", opts.python.display())))?;

        let accepted = tokio::select! {
            res = tokio::time::timeout(opts.startup_timeout, listener.accept()) => match res {
                Ok(Ok(halves)) => halves,
                Ok(Err(e)) => return Err(WorkerError::Io(e)),
                Err(_) => {
                    let _ = child.kill().await;
                    return Err(WorkerError::Handshake(format!(
                        "no connection within {:?}", opts.startup_timeout
                    )));
                }
            },
            status = child.wait() => {
                return Err(WorkerError::Spawn(format!(
                    "the worker exited before connecting ({})",
                    status.map(|s| s.to_string()).unwrap_or_else(|e| e.to_string())
                )));
            }
        };
        let (mut reader, writer): (ReadHalf, WriteHalf) = accepted;
        let hello =
            match tokio::time::timeout(opts.startup_timeout, read_envelope(&mut reader)).await {
                Ok(Ok(env)) => hello_from(&env.msg),
                Ok(Err(e)) => Err(WorkerError::Handshake(e.to_string())),
                Err(_) => Err(WorkerError::Handshake("no `hello` received".into())),
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
        if let Some(child) = guard.as_mut() {
            if let Ok(Ok(status)) = tokio::time::timeout(Duration::from_secs(2), child.wait()).await
            {
                return format!(" ({status})");
            }
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
        if let Some(child) = guard.as_mut() {
            if tokio::time::timeout(grace, child.wait()).await.is_err() {
                let _ = child.kill().await;
            }
        }
        self.reader.abort();
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.reader.abort();
        if let Ok(mut guard) = self.child.try_lock() {
            if let Some(child) = guard.as_mut() {
                let _ = child.start_kill();
            }
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
