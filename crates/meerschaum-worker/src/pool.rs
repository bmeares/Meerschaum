//! The worker pool (§4.2 "Pool policy"): one pool sized by `system.engine.python_workers`
//! serves every plugin venv (venvs are `sys.path` entries, not separate interpreters).
//!
//! - Workers start lazily, on the first call that needs one.
//! - A call holds one worker exclusively. A crash, timeout or protocol error discards that
//!   worker and fails only that call; the next call starts a fresh worker.
//! - Spawning is throttled with exponential backoff after consecutive failures (crashes or
//!   failed starts), so a plugin that kills its interpreter cannot fork-bomb the host.
//! - Workers do not live forever: one is replaced after [`PoolConfig::max_calls`] calls or when
//!   its private memory exceeds [`PoolConfig::max_rss_bytes`] after a call, and an idle one stops
//!   after [`PoolConfig::idle_timeout`]. A job loops for months; a long-lived interpreter keeps
//!   the high-water mark of its largest call (and whatever plugin code accumulates).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use meerschaum_core::Config;
use meerschaum_core::pipe::PipeDefaults;
use tokio::sync::Semaphore;

use crate::callback::{CallbackHandler, CallbackRequest};
use crate::error::{WorkerError, WorkerResult};
use crate::protocol::{Msg, map_to_wire};
use crate::worker::{CallOptions, Reply, Worker, WorkerOptions};
use meerschaum_core::Map;
use rmpv::Value as Wire;

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

/// `system.engine.python_worker_max_calls`: replace a worker after this many calls (`0`: never).
pub const MAX_CALLS_KEY: [&str; 3] = ["system", "engine", "python_worker_max_calls"];
/// The default of [`MAX_CALLS_KEY`].
pub const DEFAULT_MAX_CALLS: u64 = 10_000;
/// `system.engine.python_worker_max_memory_mb`: replace a worker whose private resident memory
/// exceeds this many MiB after a call (`0`: never; measured on Linux only).
pub const MAX_MEMORY_KEY: [&str; 3] = ["system", "engine", "python_worker_max_memory_mb"];
/// The default of [`MAX_MEMORY_KEY`].
pub const DEFAULT_MAX_MEMORY_MB: u64 = 1024;
/// `system.engine.python_worker_idle_seconds`: stop a worker idle for this long (`0`: never).
pub const IDLE_SECONDS_KEY: [&str; 3] = ["system", "engine", "python_worker_idle_seconds"];
/// The default of [`IDLE_SECONDS_KEY`].
pub const DEFAULT_IDLE_SECONDS: u64 = 600;

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
    /// Replace a worker after this many calls (`None`: never).
    pub max_calls: Option<u64>,
    /// Replace a worker whose private resident memory (`RssAnon`) exceeds this many bytes after a
    /// call (`None`: never; Linux only).
    pub max_rss_bytes: Option<u64>,
    /// Stop a worker that has been idle this long (`None`: keep idle workers).
    pub idle_timeout: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            size: default_python_workers(),
            worker: WorkerOptions::default(),
            call_timeout: None,
            backoff_initial: Duration::from_millis(250),
            backoff_max: Duration::from_secs(30),
            max_calls: None,
            max_rss_bytes: None,
            idle_timeout: None,
        }
    }
}

impl PoolConfig {
    /// Settings from the config registry: `system.engine.python_workers` (falling back to
    /// [`default_python_workers`]), the worker lifetime limits ([`MAX_CALLS_KEY`],
    /// [`MAX_MEMORY_KEY`], [`IDLE_SECONDS_KEY`]), the root directory and plugins directories of
    /// `config` passed to the workers as `MRSM_ROOT_DIR` / `MRSM_PLUGINS_DIR`, and the pipe
    /// defaults.
    pub fn from_config(config: &Config) -> PoolConfig {
        // A missing or invalid value is the default; zero (or less) turns the limit off.
        let limit = |key: &[&str], default: u64| -> Option<u64> {
            match config.get(key).and_then(|v| v.as_i64()) {
                Some(n) if n <= 0 => None,
                Some(n) => Some(n as u64),
                None => Some(default),
            }
        };
        let max_calls = limit(&MAX_CALLS_KEY, DEFAULT_MAX_CALLS);
        let max_rss_bytes =
            limit(&MAX_MEMORY_KEY, DEFAULT_MAX_MEMORY_MB).map(|mb| mb.saturating_mul(1 << 20));
        let idle_timeout = limit(&IDLE_SECONDS_KEY, DEFAULT_IDLE_SECONDS).map(Duration::from_secs);
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
            max_calls,
            max_rss_bytes,
            idle_timeout,
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
    /// Workers replaced after their call limit or memory limit, or stopped when idle too long.
    pub retired: u64,
}

/// An idle worker: its pool generation, how many calls it has served, and since when it idles.
struct IdleWorker {
    generation: u64,
    worker: Worker,
    calls: u64,
    since: Instant,
}

/// A checked-out worker: its pool generation and how many calls it had served.
struct Checkout {
    generation: u64,
    worker: Worker,
    calls: u64,
}

/// A worker's private resident memory in bytes (`RssAnon` of `/proc/<pid>/status`); `None` off
/// Linux or when it cannot be read.
pub fn worker_rss_anon(pid: u32) -> Option<u64> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let line = status.lines().find(|l| l.starts_with("RssAnon:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib.saturating_mul(1024))
}

struct Inner {
    config: PoolConfig,
    /// Attributes of connectors the host built itself (`mrsm.get_connector('sql', 'x',
    /// flavor='sqlite', ...)`: in no config file), by `type:label`. Sent with every call so the
    /// worker can build the same connectors.
    connector_attributes: Mutex<Vec<(String, Wire)>>,
    permits: Arc<Semaphore>,
    callback_permits: Arc<Semaphore>,
    idle: Mutex<Vec<IdleWorker>>,
    /// An idle-timeout reaper task is scheduled.
    reaper: AtomicBool,
    generation: AtomicU64,
    backoff: Mutex<Backoff>,
    closed: AtomicBool,
    spawned: AtomicU64,
    discarded: AtomicU64,
    calls: AtomicU64,
    retired: AtomicU64,
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
                connector_attributes: Mutex::new(Vec::new()),
                permits: Arc::new(Semaphore::new(size)),
                callback_permits: Arc::new(Semaphore::new(size.saturating_mul(8))),
                config,
                idle: Mutex::new(Vec::new()),
                reaper: AtomicBool::new(false),
                generation: AtomicU64::new(0),
                backoff: Mutex::new(Backoff::default()),
                closed: AtomicBool::new(false),
                spawned: AtomicU64::new(0),
                discarded: AtomicU64::new(0),
                calls: AtomicU64::new(0),
                retired: AtomicU64::new(0),
            }),
        }
    }

    /// The pool settings.
    pub fn config(&self) -> &PoolConfig {
        &self.inner.config
    }

    /// Define a connector the host built itself (attributes that are in no config file) for the
    /// workers: every later call carries `connector_attributes` and the worker builds
    /// `mrsm.get_connector(type, label, **attributes)` before running the operation. Defining
    /// the same keys again replaces the attributes.
    pub fn define_connector(&self, keys: &str, attributes: &Map) {
        let wire = map_to_wire(attributes);
        let mut defined = self
            .inner
            .connector_attributes
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match defined.iter_mut().find(|(k, _)| k == keys) {
            Some(slot) => slot.1 = wire,
            None => defined.push((keys.to_string(), wire)),
        }
    }

    /// The `connector_attributes` message value (`None` when nothing was defined).
    pub fn connector_attributes_wire(&self) -> Option<Wire> {
        let defined = self
            .inner
            .connector_attributes
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if defined.is_empty() {
            return None;
        }
        Some(Wire::Map(
            defined
                .iter()
                .map(|(k, v)| (Wire::from(k.as_str()), v.clone()))
                .collect(),
        ))
    }

    /// The pool's counters.
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            spawned: self.inner.spawned.load(Ordering::SeqCst),
            discarded: self.inner.discarded.load(Ordering::SeqCst),
            calls: self.inner.calls.load(Ordering::SeqCst),
            retired: self.inner.retired.load(Ordering::SeqCst),
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
        // An interpreter that cannot host a worker answers at once with its message, whatever
        // the backoff of earlier failures says: waiting would not change the answer.
        if let Some(e) = crate::worker::refusal(&self.inner.config.worker.python) {
            return Err(e);
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
            // Not a flaky start: the message is the same every time, so no backoff builds up.
            Err(e @ WorkerError::Refused(_)) => Err(e),
            Err(e) => {
                self.record_failure();
                Err(e)
            }
        }
    }

    async fn checkout(&self) -> WorkerResult<Checkout> {
        for worker in self.take_expired() {
            self.retire(worker);
        }
        loop {
            let candidate = self
                .inner
                .idle
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pop();
            match candidate {
                Some(idle) if idle.worker.is_alive() => {
                    return Ok(Checkout {
                        generation: idle.generation,
                        worker: idle.worker,
                        calls: idle.calls,
                    });
                }
                Some(_) => {
                    self.inner.discarded.fetch_add(1, Ordering::SeqCst);
                    continue;
                }
                None => {
                    let generation = self.inner.generation.load(Ordering::SeqCst);
                    return Ok(Checkout {
                        generation,
                        worker: self.spawn_worker().await?,
                        calls: 0,
                    });
                }
            }
        }
    }

    async fn checkin(&self, checkout: Checkout) {
        let Checkout {
            generation,
            worker,
            calls,
        } = checkout;
        if self.inner.closed.load(Ordering::SeqCst) || !worker.is_alive() {
            return;
        }
        if generation != self.inner.generation.load(Ordering::SeqCst) {
            // Recycled while busy: it may hold stale plugin modules.
            worker.shutdown(Duration::from_secs(5)).await;
            return;
        }
        if self.worn_out(&worker, calls) {
            self.retire(worker);
            return;
        }
        let excess = {
            let mut idle = self.inner.idle.lock().unwrap_or_else(|e| e.into_inner());
            if idle.len() < self.inner.config.size.max(1) {
                idle.push(IdleWorker {
                    generation,
                    worker,
                    calls,
                    since: Instant::now(),
                });
                None
            } else {
                Some(worker)
            }
        };
        match excess {
            Some(worker) => worker.shutdown(Duration::from_secs(5)).await,
            None => self.schedule_reaper(),
        }
    }

    /// Whether a worker that has served `calls` calls is due for replacement.
    fn worn_out(&self, worker: &Worker, calls: u64) -> bool {
        let config = &self.inner.config;
        if config.max_calls.is_some_and(|max| calls >= max) {
            return true;
        }
        config
            .max_rss_bytes
            .is_some_and(|max| worker_rss_anon(worker.pid()).is_some_and(|rss| rss > max))
    }

    /// Stop a worker in the background (the call that used it returns now).
    fn retire(&self, worker: Worker) {
        self.inner.retired.fetch_add(1, Ordering::SeqCst);
        tracing::debug!("replacing Python worker {}", worker.pid());
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(worker.shutdown(Duration::from_secs(5)));
            }
            // No runtime to wait on: dropping the handle kills the process.
            Err(_) => drop(worker),
        }
    }

    /// Remove the idle workers past the idle timeout.
    fn take_expired(&self) -> Vec<Worker> {
        let Some(timeout) = self.inner.config.idle_timeout else {
            return Vec::new();
        };
        let mut idle = self.inner.idle.lock().unwrap_or_else(|e| e.into_inner());
        let (expired, kept): (Vec<IdleWorker>, Vec<IdleWorker>) = std::mem::take(&mut *idle)
            .into_iter()
            .partition(|w| w.since.elapsed() >= timeout);
        *idle = kept;
        expired.into_iter().map(|w| w.worker).collect()
    }

    /// When the earliest idle worker reaches the idle timeout (`None`: no idle worker).
    fn next_expiry(&self) -> Option<Duration> {
        let timeout = self.inner.config.idle_timeout?;
        let idle = self.inner.idle.lock().unwrap_or_else(|e| e.into_inner());
        idle.iter()
            .map(|w| timeout.saturating_sub(w.since.elapsed()))
            .min()
    }

    /// Start the task that stops idle workers at the idle timeout, unless one runs. It holds the
    /// pool weakly and ends when no worker is idle.
    fn schedule_reaper(&self) {
        if self.inner.config.idle_timeout.is_none()
            || self.inner.reaper.swap(true, Ordering::SeqCst)
        {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self.inner.reaper.store(false, Ordering::SeqCst);
            return;
        };
        let weak = Arc::downgrade(&self.inner);
        // A runtime that shuts down drops the task: clear the flag so a later check-in schedules
        // a new one (checkouts also stop expired workers in any case).
        struct Scheduled(std::sync::Weak<Inner>);
        impl Scheduled {
            fn disarm(&mut self) {
                self.0 = std::sync::Weak::new();
            }
        }
        impl Drop for Scheduled {
            fn drop(&mut self) {
                if let Some(inner) = self.0.upgrade() {
                    inner.reaper.store(false, Ordering::SeqCst);
                }
            }
        }
        let mut scheduled = Scheduled(weak.clone());
        handle.spawn(async move {
            loop {
                let wait = {
                    let Some(inner) = weak.upgrade() else { return };
                    let pool = WorkerPool { inner };
                    for worker in pool.take_expired() {
                        pool.retire(worker);
                    }
                    match pool.next_expiry() {
                        Some(wait) => wait,
                        None => {
                            pool.inner.reaper.store(false, Ordering::SeqCst);
                            // A worker checked in after the expiry check schedules its own task
                            // unless this one still claimed the flag: look once more.
                            if pool.next_expiry().is_none()
                                || pool.inner.reaper.swap(true, Ordering::SeqCst)
                            {
                                // Ending here must not clear a flag another task now holds.
                                scheduled.disarm();
                                return;
                            }
                            continue;
                        }
                    }
                };
                tokio::time::sleep(wait + Duration::from_millis(10)).await;
            }
        });
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
        let mut args = args;
        if let Some(defined) = self.connector_attributes_wire() {
            args.insert("connector_attributes", defined);
        }
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
        let mut checkout = self.checkout().await?;
        let worker = &checkout.worker;
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
        checkout.calls += 1;
        match &result {
            Err(e) if e.is_fatal() => {
                tracing::warn!("discarding Python worker {}: {e}", checkout.worker.pid());
                checkout.worker.kill().await;
                self.inner.discarded.fetch_add(1, Ordering::SeqCst);
                self.record_failure();
            }
            _ => {
                self.record_success();
                self.checkin(checkout).await;
            }
        }
        result
    }

    /// Replace every worker: idle workers stop now, busy workers stop when their call returns.
    /// The next call starts a fresh interpreter, which re-imports plugins (e.g. after a manifest
    /// rebuild detected changed plugin sources).
    pub async fn recycle(&self) {
        crate::worker::clear_refusals();
        self.inner.generation.fetch_add(1, Ordering::SeqCst);
        let workers: Vec<IdleWorker> =
            std::mem::take(&mut *self.inner.idle.lock().unwrap_or_else(|e| e.into_inner()));
        for idle in workers {
            idle.worker.shutdown(Duration::from_secs(5)).await;
        }
    }

    /// Stop every idle worker and refuse new calls.
    pub async fn shutdown(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
        self.inner.permits.close();
        self.inner.callback_permits.close();
        let workers: Vec<IdleWorker> =
            std::mem::take(&mut *self.inner.idle.lock().unwrap_or_else(|e| e.into_inner()));
        for idle in workers {
            idle.worker.shutdown(Duration::from_secs(5)).await;
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
