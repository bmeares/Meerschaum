//! The worker pool (§4.2 "Pool policy"): one pool sized by `system.engine.python_workers`
//! serves every plugin venv (venvs are `sys.path` entries, not separate interpreters).
//!
//! - Workers start lazily, on the first call that needs one.
//! - A call holds one worker exclusively. A crash, timeout or protocol error discards that
//!   worker and fails only that call; the next call starts a fresh worker.
//! - Spawning is throttled with exponential backoff after consecutive failures (crashes or
//!   failed starts), so a plugin that kills its interpreter cannot fork-bomb the host.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use meerschaum_core::Config;
use meerschaum_core::pipe::PipeDefaults;
use tokio::sync::Semaphore;

use crate::callback::{CallbackHandler, CallbackRequest};
use crate::error::{WorkerError, WorkerResult};
use crate::protocol::Msg;
use crate::worker::{CallOptions, Reply, Worker, WorkerOptions};

tokio::task_local! {
    static CALLBACK_DEPTH: usize;
}

/// Run native callback processing in a reentrant worker-call scope.
/// Hosts which relay callbacks through channels must preserve this scope around native work.
pub async fn callback_scope<F: std::future::Future>(future: F) -> F::Output {
    let depth = CALLBACK_DEPTH.try_with(|depth| *depth).unwrap_or(0) + 1;
    CALLBACK_DEPTH.scope(depth, future).await
}

/// Enter callback scope on a blocking host thread, e.g. while constructing a lazy connector.
/// This lets an existing host runtime execute nested worker calls without taking a main permit.
pub fn callback_scope_blocking<T>(f: impl FnOnce() -> T) -> T {
    let depth = CALLBACK_DEPTH.try_with(|depth| *depth).unwrap_or(0) + 1;
    CALLBACK_DEPTH.sync_scope(depth, f)
}

struct ReentrantHandler {
    handler: Arc<dyn CallbackHandler>,
    depth: usize,
}
#[meerschaum_connector::async_trait]
impl CallbackHandler for ReentrantHandler {
    async fn handle(&self, request: CallbackRequest) -> meerschaum_core::SuccessTuple {
        CALLBACK_DEPTH
            .scope(self.depth, self.handler.handle(request))
            .await
    }
}

/// The config key for the pool size.
pub const PYTHON_WORKERS_KEY: [&str; 3] = ["system", "engine", "python_workers"];

/// The pool size when `system.engine.python_workers` is not set: the CPU count clamped to
/// `1..=4` (Python workers mostly wait on I/O or hold the GIL; more rarely helps).
pub fn default_python_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .clamp(1, 4)
}

/// Pool settings.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// The maximum number of concurrent workers.
    pub size: usize,
    /// How workers are started.
    pub worker: WorkerOptions,
    /// The default per-call timeout (`None`: wait forever).
    pub call_timeout: Option<Duration>,
    /// The first backoff delay after a failure.
    pub backoff_initial: Duration,
    /// The longest backoff delay.
    pub backoff_max: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            size: default_python_workers(),
            worker: WorkerOptions::default(),
            call_timeout: None,
            backoff_initial: Duration::from_millis(250),
            backoff_max: Duration::from_secs(30),
        }
    }
}

impl PoolConfig {
    /// Settings from the config registry: `system.engine.python_workers` (falling back to
    /// [`default_python_workers`]), the root directory and plugins directories of `config`
    /// passed to the workers as `MRSM_ROOT_DIR` / `MRSM_PLUGINS_DIR`, and the pipe defaults.
    pub fn from_config(config: &Config) -> PoolConfig {
        let size = config
            .get(&PYTHON_WORKERS_KEY)
            .and_then(|v| v.as_i64())
            .filter(|n| *n > 0)
            .map(|n| n as usize)
            .unwrap_or_else(default_python_workers);
        let paths = config.paths();
        let mut env = vec![(
            "MRSM_ROOT_DIR".to_string(),
            paths.root_dir.to_string_lossy().into_owned(),
        )];
        if !paths.plugins_dirs.is_empty() {
            let dirs: Vec<String> = paths
                .plugins_dirs
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect();
            env.push((
                "MRSM_PLUGINS_DIR".to_string(),
                serde_json::to_string(&dirs).unwrap_or_default(),
            ));
        }
        PoolConfig {
            size,
            worker: WorkerOptions {
                env,
                pipe_defaults: PipeDefaults::from_config(config),
                ..WorkerOptions::default()
            },
            ..PoolConfig::default()
        }
    }
}

#[derive(Debug, Default)]
struct Backoff {
    failures: u32,
    not_before: Option<Instant>,
}

/// Counters for monitoring.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    /// Workers started.
    pub spawned: u64,
    /// Workers discarded after a crash, timeout or protocol error.
    pub discarded: u64,
    /// Calls completed (successfully or with a Python exception).
    pub calls: u64,
}

struct Inner {
    config: PoolConfig,
    permits: Arc<Semaphore>,
    callback_permits: Arc<Semaphore>,
    idle: Mutex<Vec<(u64, Worker)>>,
    generation: AtomicU64,
    backoff: Mutex<Backoff>,
    closed: AtomicBool,
    spawned: AtomicU64,
    discarded: AtomicU64,
    calls: AtomicU64,
}

/// A pool of Python workers. Cloning shares the pool.
#[derive(Clone)]
pub struct WorkerPool {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for WorkerPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerPool")
            .field("size", &self.inner.config.size)
            .field("stats", &self.stats())
            .finish()
    }
}

impl WorkerPool {
    /// A new pool (no worker is started until the first call).
    pub fn new(config: PoolConfig) -> WorkerPool {
        let size = config.size.max(1);
        WorkerPool {
            inner: Arc::new(Inner {
                permits: Arc::new(Semaphore::new(size)),
                callback_permits: Arc::new(Semaphore::new(size.saturating_mul(8))),
                config,
                idle: Mutex::new(Vec::new()),
                generation: AtomicU64::new(0),
                backoff: Mutex::new(Backoff::default()),
                closed: AtomicBool::new(false),
                spawned: AtomicU64::new(0),
                discarded: AtomicU64::new(0),
                calls: AtomicU64::new(0),
            }),
        }
    }

    /// The pool settings.
    pub fn config(&self) -> &PoolConfig {
        &self.inner.config
    }

    /// The pool's counters.
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            spawned: self.inner.spawned.load(Ordering::SeqCst),
            discarded: self.inner.discarded.load(Ordering::SeqCst),
            calls: self.inner.calls.load(Ordering::SeqCst),
        }
    }

    /// The number of idle workers.
    pub fn idle_count(&self) -> usize {
        self.inner
            .idle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    fn backoff_delay(&self, failures: u32) -> Duration {
        let cfg = &self.inner.config;
        let factor = 2u32.saturating_pow(failures.saturating_sub(1).min(16));
        cfg.backoff_initial
            .saturating_mul(factor)
            .min(cfg.backoff_max)
    }

    fn record_failure(&self) {
        let mut b = self.inner.backoff.lock().unwrap_or_else(|e| e.into_inner());
        b.failures = b.failures.saturating_add(1);
        b.not_before = Some(Instant::now() + self.backoff_delay(b.failures));
    }

    fn record_success(&self) {
        let mut b = self.inner.backoff.lock().unwrap_or_else(|e| e.into_inner());
        b.failures = 0;
        b.not_before = None;
    }

    /// Start a worker outside the pool's accounting (e.g. a dedicated stream worker), waiting
    /// out any backoff first.
    pub async fn spawn_worker(&self) -> WorkerResult<Worker> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(WorkerError::ShuttingDown);
        }
        let not_before = self
            .inner
            .backoff
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .not_before;
        if let Some(t) = not_before {
            tokio::time::sleep_until(t.into()).await;
        }
        match Worker::spawn(&self.inner.config.worker).await {
            Ok(w) => {
                self.inner.spawned.fetch_add(1, Ordering::SeqCst);
                Ok(w)
            }
            Err(e) => {
                self.record_failure();
                Err(e)
            }
        }
    }

    async fn checkout(&self) -> WorkerResult<(u64, Worker)> {
        loop {
            let candidate = self
                .inner
                .idle
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pop();
            match candidate {
                Some((generation, w)) if w.is_alive() => return Ok((generation, w)),
                Some(_) => {
                    self.inner.discarded.fetch_add(1, Ordering::SeqCst);
                    continue;
                }
                None => {
                    let generation = self.inner.generation.load(Ordering::SeqCst);
                    return Ok((generation, self.spawn_worker().await?));
                }
            }
        }
    }

    async fn checkin(&self, generation: u64, worker: Worker) {
        if self.inner.closed.load(Ordering::SeqCst) || !worker.is_alive() {
            return;
        }
        if generation != self.inner.generation.load(Ordering::SeqCst) {
            // Recycled while busy: it may hold stale plugin modules.
            worker.shutdown(Duration::from_secs(5)).await;
            return;
        }
        let excess = {
            let mut idle = self.inner.idle.lock().unwrap_or_else(|e| e.into_inner());
            if idle.len() < self.inner.config.size.max(1) {
                idle.push((generation, worker));
                None
            } else {
                Some(worker)
            }
        };
        if let Some(worker) = excess {
            worker.shutdown(Duration::from_secs(5)).await;
        }
    }

    /// Run `op` on a pooled worker. `handler` receives callbacks from plugin code made during
    /// the call. A fatal error (crash, timeout, protocol) discards the worker; a Python
    /// exception returns it to the pool.
    pub async fn call(
        &self,
        op: &str,
        args: Msg,
        frames: Vec<Vec<u8>>,
        mut opts: CallOptions,
        handler: Option<Arc<dyn CallbackHandler>>,
    ) -> WorkerResult<Reply> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(WorkerError::ShuttingDown);
        }
        let depth = CALLBACK_DEPTH.try_with(|depth| *depth).unwrap_or(0);
        let _permit = if depth > 0 {
            if depth > 8 {
                return Err(WorkerError::Protocol(
                    "Python callback nesting exceeds 8 levels.".into(),
                ));
            }
            // A worker awaiting this callback retains its main permit. Never queue behind
            // that permit: a size-one pool would deadlock. The reserve is bounded and fails
            // immediately when saturated, including for callbacks on parallel plugin threads.
            self.inner
                .callback_permits
                .clone()
                .try_acquire_owned()
                .map_err(|_| {
                    WorkerError::Protocol("Python callback worker reserve is exhausted.".into())
                })?
        } else {
            Arc::clone(&self.inner.permits)
                .acquire_owned()
                .await
                .map_err(|_| WorkerError::ShuttingDown)?
        };
        let (generation, worker) = self.checkout().await?;
        if opts.timeout.is_none() {
            opts.timeout = self.inner.config.call_timeout;
        }
        worker.set_callback_handler(handler.map(|handler| {
            Arc::new(ReentrantHandler {
                handler,
                depth: depth + 1,
            }) as Arc<dyn CallbackHandler>
        }));
        let result = worker.call(op, args, frames, opts).await;
        worker.set_callback_handler(None);
        self.inner.calls.fetch_add(1, Ordering::SeqCst);
        match &result {
            Err(e) if e.is_fatal() => {
                tracing::warn!("discarding Python worker {}: {e}", worker.pid());
                worker.kill().await;
                self.inner.discarded.fetch_add(1, Ordering::SeqCst);
                self.record_failure();
            }
            _ => {
                self.record_success();
                self.checkin(generation, worker).await;
            }
        }
        result
    }

    /// Replace every worker: idle workers stop now, busy workers stop when their call returns.
    /// The next call starts a fresh interpreter, which re-imports plugins (e.g. after a manifest
    /// rebuild detected changed plugin sources).
    pub async fn recycle(&self) {
        self.inner.generation.fetch_add(1, Ordering::SeqCst);
        let workers: Vec<(u64, Worker)> =
            std::mem::take(&mut *self.inner.idle.lock().unwrap_or_else(|e| e.into_inner()));
        for (_, w) in workers {
            w.shutdown(Duration::from_secs(5)).await;
        }
    }

    /// Stop every idle worker and refuse new calls.
    pub async fn shutdown(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
        self.inner.permits.close();
        self.inner.callback_permits.close();
        let workers: Vec<(u64, Worker)> =
            std::mem::take(&mut *self.inner.idle.lock().unwrap_or_else(|e| e.into_inner()));
        for (_, w) in workers {
            w.shutdown(Duration::from_secs(5)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let pool = WorkerPool::new(PoolConfig {
            backoff_initial: Duration::from_millis(100),
            backoff_max: Duration::from_secs(1),
            ..PoolConfig::default()
        });
        assert_eq!(pool.backoff_delay(1), Duration::from_millis(100));
        assert_eq!(pool.backoff_delay(2), Duration::from_millis(200));
        assert_eq!(pool.backoff_delay(4), Duration::from_millis(800));
        assert_eq!(pool.backoff_delay(10), Duration::from_secs(1));
    }

    #[tokio::test]
    async fn callback_reserve_rejects_excessive_nesting_without_spawning() {
        let pool = WorkerPool::new(PoolConfig::default());
        let result = CALLBACK_DEPTH
            .scope(
                9,
                pool.call("ping", Msg::new(), vec![], CallOptions::default(), None),
            )
            .await;
        assert!(result.unwrap_err().to_string().contains("nesting exceeds"));
        assert_eq!(pool.stats().spawned, 0);
    }

    #[test]
    fn default_size_is_bounded() {
        let n = default_python_workers();
        assert!((1..=4).contains(&n));
    }
}
