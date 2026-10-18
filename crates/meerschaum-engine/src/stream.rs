//! Micro-batching for stream sources (§5.6).
//!
//! The engine-provided sink buffers rows and flushes on whichever trigger fires first:
//!
//! | Trigger | Default | Purpose |
//! |---|---|---|
//! | `idle_ms` | 250 ms | a burst ended |
//! | `max_latency_ms` | 2000 ms | limits staleness (age of the oldest buffered row) |
//! | `max_rows` / `max_bytes` | 5000 / 8 MB | caps memory and transaction size |
//!
//! `idle_ms: auto` sets the idle window from the moving average of gaps between messages,
//! bounded by `[50 ms, max_latency_ms]`. [`MicroBatcher`] is the pure state machine.
//!
//! On top of it:
//!
//! - [`run_sink`] is the engine-provided sink: it buffers [`StreamMessage`]s from a source,
//!   flushes through a [`StreamFlush`] (one sync, with the ledger written before the commit),
//!   and only then acknowledges the flushed messages ([`StreamSource::ack`]). A failed flush
//!   acknowledges nothing and keeps the rows for the next trigger.
//! - [`supervise`] runs a [`StreamSource::subscribe`] (or a v4-pattern [`LegacyStream`]) and
//!   restarts it with exponential backoff, emitting `stream_restarted`.
//!
//! **Seam for the Python adapter (worker agent).** A Python connector with `subscribe` is a
//! [`StreamSource`]; one that only has the v4 pattern (a `sync()` that subscribes and returns,
//! with callbacks calling `pipe.sync()`) is a [`LegacyStream`]: its `run` starts the `sync()`
//! in a worker, keeps the worker alive until `stop`, and forwards each callback `pipe.sync()`
//! through the engine's [`LegacySink`] (the native sync path with the ledger written first,
//! reported as a `stream_flushed` with trigger [`FlushTrigger::Callback`]). The engine
//! supervises both the same way, and both trigger the stream's children.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use indexmap::IndexMap;
use meerschaum_connector::async_trait;
use meerschaum_core::success::SyncResult;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::Value as KValue;
use meerschaum_kernel::enforce::PipeInput;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};

use crate::events::{Emitter, EventKind};
use crate::node::PipeRef;
use crate::sync::SyncArgs;

/// Which trigger caused a flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlushTrigger {
    /// No message for `idle_ms`.
    Idle,
    /// The oldest buffered row reached `max_latency_ms`.
    MaxLatency,
    /// The buffer reached `max_rows`.
    MaxRows,
    /// The buffer reached `max_bytes`.
    MaxBytes,
    /// A graceful stop.
    Shutdown,
    /// A v4-pattern connector's callback called `pipe.sync()` ([`LegacyStream`]): every such
    /// sync is its own flush.
    Callback,
}

/// The idle window setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdleWindow {
    /// A fixed window in milliseconds.
    Fixed(u64),
    /// From the moving average of message gaps.
    Auto,
}

/// `parameters['stream']`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamConfig {
    /// `idle_ms`.
    pub idle: IdleWindow,
    /// `max_latency_ms`.
    pub max_latency_ms: u64,
    /// `max_rows`.
    pub max_rows: u64,
    /// `max_bytes`.
    pub max_bytes: u64,
}

impl Default for StreamConfig {
    fn default() -> Self {
        StreamConfig {
            idle: IdleWindow::Fixed(250),
            max_latency_ms: 2000,
            max_rows: 5000,
            max_bytes: 8 * 1024 * 1024,
        }
    }
}

impl StreamConfig {
    /// Read `parameters['stream']`, falling back to the defaults for missing or invalid values.
    pub fn from_parameters(parameters: &Map) -> StreamConfig {
        let mut cfg = StreamConfig::default();
        let Some(Value::Map(s)) = parameters.get("stream") else {
            return cfg;
        };
        let num = |k: &str| s.get(k).and_then(Value::as_i64).filter(|v| *v > 0);
        match s.get("idle_ms") {
            Some(Value::Str(a)) if a == "auto" => cfg.idle = IdleWindow::Auto,
            Some(v) => {
                if let Some(ms) = v.as_i64().filter(|v| *v > 0) {
                    cfg.idle = IdleWindow::Fixed(ms as u64);
                }
            }
            None => {}
        }
        if let Some(v) = num("max_latency_ms") {
            cfg.max_latency_ms = v as u64;
        }
        if let Some(v) = num("max_rows") {
            cfg.max_rows = v as u64;
        }
        if let Some(v) = num("max_bytes") {
            cfg.max_bytes = v as u64;
        }
        cfg
    }

    /// Cap `max_rows` at the broker's in-flight limit (MQTT 3.1.1).
    pub fn cap_rows(&mut self, in_flight_limit: u64) {
        self.max_rows = self.max_rows.min(in_flight_limit.max(1));
    }
}

/// The micro-batching state machine. Times are milliseconds on any monotonic clock.
#[derive(Debug, Clone)]
pub struct MicroBatcher {
    config: StreamConfig,
    rows: u64,
    bytes: u64,
    oldest_ms: Option<u64>,
    last_ms: Option<u64>,
    gap_ewma_ms: Option<f64>,
}

/// The EWMA weight of the newest gap for `idle_ms: auto`.
const GAP_ALPHA: f64 = 0.2;

impl MicroBatcher {
    /// An empty buffer.
    pub fn new(config: StreamConfig) -> MicroBatcher {
        MicroBatcher {
            config,
            rows: 0,
            bytes: 0,
            oldest_ms: None,
            last_ms: None,
            gap_ewma_ms: None,
        }
    }

    /// The buffered rows and bytes.
    pub fn buffered(&self) -> (u64, u64) {
        (self.rows, self.bytes)
    }

    /// The current idle window in milliseconds.
    pub fn idle_window_ms(&self) -> u64 {
        match self.config.idle {
            IdleWindow::Fixed(ms) => ms,
            IdleWindow::Auto => {
                let hi = self.config.max_latency_ms.max(50);
                let est = self.gap_ewma_ms.map_or(250.0, |g| 2.0 * g);
                (est.round() as u64).clamp(50, hi)
            }
        }
    }

    /// Buffer a message. Returns a size trigger if the buffer is now full.
    pub fn push(&mut self, now_ms: u64, rows: u64, bytes: u64) -> Option<FlushTrigger> {
        if let Some(last) = self.last_ms {
            let gap = now_ms.saturating_sub(last) as f64;
            self.gap_ewma_ms = Some(match self.gap_ewma_ms {
                Some(g) => GAP_ALPHA * gap + (1.0 - GAP_ALPHA) * g,
                None => gap,
            });
        }
        self.last_ms = Some(now_ms);
        self.oldest_ms.get_or_insert(now_ms);
        self.rows += rows;
        self.bytes += bytes;
        if self.rows >= self.config.max_rows {
            Some(FlushTrigger::MaxRows)
        } else if self.bytes >= self.config.max_bytes {
            Some(FlushTrigger::MaxBytes)
        } else {
            None
        }
    }

    /// Check the time triggers.
    pub fn poll(&self, now_ms: u64) -> Option<FlushTrigger> {
        if self.rows == 0 {
            return None;
        }
        let oldest = self.oldest_ms.unwrap_or(now_ms);
        if now_ms.saturating_sub(oldest) >= self.config.max_latency_ms {
            return Some(FlushTrigger::MaxLatency);
        }
        let last = self.last_ms.unwrap_or(now_ms);
        if now_ms.saturating_sub(last) >= self.idle_window_ms() {
            return Some(FlushTrigger::Idle);
        }
        None
    }

    /// When the next time trigger fires, if anything is buffered.
    pub fn next_deadline_ms(&self) -> Option<u64> {
        if self.rows == 0 {
            return None;
        }
        let latency = self.oldest_ms? + self.config.max_latency_ms;
        let idle = self.last_ms? + self.idle_window_ms();
        Some(latency.min(idle))
    }

    /// Empty the buffer, returning `(rows, bytes)`. The caller syncs them, then acknowledges.
    pub fn take(&mut self) -> (u64, u64) {
        let out = (self.rows, self.bytes);
        self.rows = 0;
        self.bytes = 0;
        self.oldest_ms = None;
        out
    }
}

/// A cooperative stop signal shared by the engine, sinks and sources.
#[derive(Debug, Clone, Default)]
pub struct StopToken(Arc<(AtomicBool, Notify)>);

impl StopToken {
    /// A token that has not stopped.
    pub fn new() -> StopToken {
        StopToken::default()
    }

    /// Request a stop (idempotent).
    pub fn stop(&self) {
        self.0.0.store(true, Ordering::SeqCst);
        self.0.1.notify_waiters();
    }

    /// Whether a stop was requested.
    pub fn is_stopped(&self) -> bool {
        self.0.0.load(Ordering::SeqCst)
    }

    /// Resolve once a stop is requested.
    pub async fn stopped(&self) {
        loop {
            let notified = self.0.1.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_stopped() {
                return;
            }
            notified.await;
        }
    }

    /// Sleep for `d` or until stopped. Returns `true` if stopped.
    pub async fn sleep(&self, d: Duration) -> bool {
        tokio::select! {
            _ = self.stopped() => true,
            _ = tokio::time::sleep(d) => self.is_stopped(),
        }
    }
}

/// An opaque acknowledgement token (an MQTT packet ID, a Kafka offset, an AMQP delivery tag).
pub type AckToken = u64;

/// One message from a source: rows plus how to acknowledge them.
#[derive(Debug, Clone, Default)]
pub struct StreamMessage {
    /// The rows.
    pub docs: Vec<IndexMap<String, KValue>>,
    /// The message size in bytes (for `max_bytes`).
    pub bytes: u64,
    /// The token to acknowledge after the rows commit (`None`: the source cannot acknowledge).
    pub ack: Option<AckToken>,
    /// A redelivery (MQTT `dup`): sync with `check_existing=True` even when the stream skips it.
    pub redelivered: bool,
}

/// The sending side handed to [`StreamSource::subscribe`].
pub type StreamSender = mpsc::Sender<StreamMessage>;

/// A push source (`IS_STREAMING = True`, `subscribe(pipe, sink)`, §5.6).
#[async_trait]
pub trait StreamSource: Send + Sync {
    /// Subscribe and push messages into `sink` until `stop`; return `Err` when the
    /// subscription breaks (the supervisor restarts it).
    async fn subscribe(
        &self,
        pipe: &Pipe,
        sink: StreamSender,
        stop: StopToken,
    ) -> Result<(), String>;

    /// Acknowledge messages whose rows committed (only ever called after the commit).
    async fn ack(&self, tokens: Vec<AckToken>) -> Result<(), String> {
        let _ = tokens;
        Ok(())
    }

    /// The broker's in-flight limit, if it caps `max_rows` (MQTT 3.1.1).
    fn in_flight_limit(&self) -> Option<u64> {
        None
    }
}

/// Where a [`LegacyStream`]'s callback syncs go.
///
/// The engine's implementation runs every `pipe.sync()` the connector's callbacks make through
/// the native sync path with the dirty-range ledger written before the data commit (§5.4), and
/// reports it as a `stream_flushed` event with trigger [`FlushTrigger::Callback`] (rows,
/// begin/end, success), so stream progress, children triggering (§5.10 "Stream-flush
/// triggering") and the ledger see v4-pattern connectors exactly like `subscribe` streams.
/// A sync of a pipe outside the engine's graph is a bare `pipe.sync()` (R1) and reports nothing.
#[async_trait]
pub trait LegacySink: Send + Sync {
    /// Sync `input` (`None`: fetch, as a v4 `pipe.sync()` without a DataFrame) into `pipe` and
    /// return the result the plugin's callback receives.
    async fn sync(&self, pipe: &Pipe, input: Option<PipeInput>, args: &SyncArgs) -> SyncResult;

    /// The connector's own `sync(pipe)` is about to be called (each start or restart of the
    /// stream). The engine runs the `@pre_sync_hook`s here, as v4's lap did before calling it.
    async fn starting(&self, pipe: &Pipe) {
        let _ = pipe;
    }

    /// The connector's `sync(pipe)` returned `result` (the worker stays alive for its
    /// callbacks). The engine runs the `@post_sync_hook`s and reports the result as the pipe's
    /// lap outcome (v4: `Finished syncing ...: Syncing ... in a background thread.`).
    async fn returned(&self, pipe: &Pipe, result: SyncResult) {
        let _ = (pipe, result);
    }
}

/// A v4-pattern streaming connector: its `sync()` subscribes and returns immediately, and its
/// callbacks call `pipe.sync()` (see the module docs).
#[async_trait]
pub trait LegacyStream: Send + Sync {
    /// Start the connector and keep it alive until `stop`; `Err` when it dies. Every
    /// `pipe.sync()` a callback makes must go through `sink` (that is how the engine sees the
    /// connector's flushes).
    async fn run(
        &self,
        pipe: &Pipe,
        sink: Arc<dyn LegacySink>,
        stop: StopToken,
    ) -> Result<(), String>;

    /// Whether `run` calls [`LegacySink::returned`] once the connector's `sync()` returns. The
    /// first lap of `--listen` then waits (bounded) for that return and reports it like v4's
    /// lap did. Implementations that never report return `false` (the default).
    fn reports_return(&self) -> bool {
        false
    }
}

/// Commits one micro-batch: one sync of the buffered rows (the engine's implementation writes
/// the ledger entry before the data commit and advances nothing else).
#[async_trait]
pub trait StreamFlush: Send + Sync {
    /// Sync the rows. `check_existing` is forced on when any message was redelivered.
    async fn flush(&self, pipe: &Pipe, input: PipeInput, check_existing: bool) -> SyncResult;
}

/// Sink options beyond `parameters['stream']`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SinkOptions {
    /// `--skip-check-existing` for the stream.
    pub skip_check_existing: bool,
    /// Delay before retrying a failed flush.
    pub retry_delay: Duration,
}

impl Default for SinkOptions {
    fn default() -> Self {
        SinkOptions {
            skip_check_existing: false,
            retry_delay: Duration::from_millis(500),
        }
    }
}

/// What a sink did, for tests and the run record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SinkStats {
    /// Successful flushes.
    pub flushes: u64,
    /// Failed flushes (rows kept for retry).
    pub failed_flushes: u64,
    /// Rows committed.
    pub rows: u64,
    /// Tokens acknowledged.
    pub acked: u64,
}

struct Buffer {
    docs: Vec<IndexMap<String, KValue>>,
    tokens: Vec<AckToken>,
    redelivered: bool,
}

/// The engine-provided micro-batching sink: receive from `rx` until the channel closes or
/// `stop`, flushing on the [`MicroBatcher`] triggers and acknowledging only after a commit.
/// On stop, the buffer is flushed (and acknowledged) before returning.
#[allow(clippy::too_many_arguments)]
pub async fn run_sink(
    pipe: &Pipe,
    mut config: StreamConfig,
    options: SinkOptions,
    mut rx: mpsc::Receiver<StreamMessage>,
    source: &dyn StreamSource,
    flusher: &dyn StreamFlush,
    emitter: Option<&Emitter>,
    stop: &StopToken,
) -> SinkStats {
    if let Some(limit) = source.in_flight_limit() {
        config.cap_rows(limit);
    }
    let pipe_ref = PipeRef::from_pipe(pipe);
    let clock = tokio::time::Instant::now();
    let now_ms = || clock.elapsed().as_millis() as u64;
    let mut batcher = MicroBatcher::new(config);
    let mut buf = Buffer {
        docs: Vec::new(),
        tokens: Vec::new(),
        redelivered: false,
    };
    let mut stats = SinkStats::default();
    let mut retry_at: Option<u64> = None;
    let mut closed = false;

    async fn do_flush(
        pipe: &Pipe,
        pipe_ref: &PipeRef,
        buf: &mut Buffer,
        batcher: &mut MicroBatcher,
        trigger: FlushTrigger,
        options: &SinkOptions,
        source: &dyn StreamSource,
        flusher: &dyn StreamFlush,
        emitter: Option<&Emitter>,
        stats: &mut SinkStats,
    ) -> bool {
        if buf.docs.is_empty() {
            return true;
        }
        let (rows, bytes) = batcher.buffered();
        let check_existing = !options.skip_check_existing || buf.redelivered;
        let result = flusher
            .flush(pipe, PipeInput::Docs(buf.docs.clone()), check_existing)
            .await;
        if !result.success {
            stats.failed_flushes += 1;
            tracing::warn!(
                "Failed to flush a micro-batch for {pipe}:\n{}",
                result.message
            );
            return false;
        }
        batcher.take();
        stats.flushes += 1;
        stats.rows += buf.docs.len() as u64;
        let tokens = std::mem::take(&mut buf.tokens);
        buf.docs.clear();
        buf.redelivered = false;
        if !tokens.is_empty() {
            let n = tokens.len() as u64;
            match source.ack(tokens).await {
                Ok(()) => stats.acked += n,
                Err(e) => tracing::warn!("Failed to acknowledge messages for {pipe}: {e}"),
            }
        }
        if let Some(em) = emitter {
            em.emit(
                Some(pipe_ref),
                EventKind::StreamFlushed {
                    rows,
                    bytes,
                    trigger,
                    success: true,
                    begin: result.begin,
                    end: result.end,
                    inserted: result.inserted,
                    updated: result.updated,
                    upserted: result.upserted,
                    message: String::new(),
                },
            );
        }
        true
    }

    loop {
        let deadline = match (batcher.next_deadline_ms(), retry_at) {
            (Some(d), Some(r)) => Some(d.max(r)),
            (d, None) => d,
            (None, Some(r)) => Some(r),
        };
        let sleep_ms = deadline.map(|d| d.saturating_sub(now_ms()));
        let mut trigger: Option<FlushTrigger> = None;
        tokio::select! {
            biased;
            _ = stop.stopped() => {
                // Messages already delivered to the channel are part of the final flush.
                while let Ok(m) = rx.try_recv() {
                    let n = m.docs.len() as u64;
                    buf.docs.extend(m.docs);
                    if let Some(t) = m.ack {
                        buf.tokens.push(t);
                    }
                    buf.redelivered |= m.redelivered;
                    batcher.push(now_ms(), n, m.bytes);
                }
                trigger = Some(FlushTrigger::Shutdown);
            }
            msg = rx.recv(), if !closed => match msg {
                Some(m) => {
                    let n = m.docs.len() as u64;
                    buf.docs.extend(m.docs);
                    if let Some(t) = m.ack {
                        buf.tokens.push(t);
                    }
                    buf.redelivered |= m.redelivered;
                    if let Some(t) = batcher.push(now_ms(), n, m.bytes)
                        && retry_at.is_none_or(|r| now_ms() >= r) {
                            trigger = Some(t);
                        }
                }
                None => {
                    closed = true;
                    trigger = Some(FlushTrigger::Shutdown);
                }
            },
            _ = tokio::time::sleep(Duration::from_millis(sleep_ms.unwrap_or(3_600_000))), if sleep_ms.is_some() => {
                let now = now_ms();
                if retry_at.is_none_or(|r| now >= r) {
                    trigger = batcher.poll(now).or(retry_at.map(|_| FlushTrigger::MaxLatency));
                }
            }
        }
        if let Some(t) = trigger {
            let ok = do_flush(
                pipe,
                &pipe_ref,
                &mut buf,
                &mut batcher,
                t,
                &options,
                source,
                flusher,
                emitter,
                &mut stats,
            )
            .await;
            retry_at = (!ok).then(|| now_ms() + options.retry_delay.as_millis() as u64);
            if t == FlushTrigger::Shutdown && (closed || stop.is_stopped()) {
                if !ok {
                    // One more attempt on the way out; unacknowledged rows are redelivered by
                    // sources that acknowledge.
                    tokio::time::sleep(options.retry_delay).await;
                    let _ = do_flush(
                        pipe,
                        &pipe_ref,
                        &mut buf,
                        &mut batcher,
                        t,
                        &options,
                        source,
                        flusher,
                        emitter,
                        &mut stats,
                    )
                    .await;
                }
                return stats;
            }
        }
    }
}

/// Supervision options (restart with exponential backoff).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SuperviseOptions {
    /// The first restart delay.
    pub initial_backoff: Duration,
    /// The longest restart delay.
    pub max_backoff: Duration,
    /// Give up after this many consecutive failures (`None`: never).
    pub max_restarts: Option<u32>,
}

impl Default for SuperviseOptions {
    fn default() -> Self {
        SuperviseOptions {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            max_restarts: None,
        }
    }
}

/// Run a subscription (`run` is called again after every failure) until `stop`, emitting
/// `stream_restarted` and backing off exponentially. Returns the number of restarts.
pub async fn supervise<F, Fut>(
    pipe_ref: &PipeRef,
    mut run: F,
    options: SuperviseOptions,
    emitter: Option<&Emitter>,
    stop: &StopToken,
) -> u32
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let mut backoff = options.initial_backoff;
    let mut restarts = 0u32;
    loop {
        if stop.is_stopped() {
            return restarts;
        }
        match run().await {
            Ok(()) if stop.is_stopped() => return restarts,
            Ok(()) => {
                // A clean return without a stop: the source reconnects after the initial
                // backoff (never a busy loop over a source that returns at once).
                backoff = options.initial_backoff;
                if stop.sleep(backoff).await {
                    return restarts;
                }
            }
            Err(error) => {
                if let Some(em) = emitter {
                    em.emit(
                        Some(pipe_ref),
                        EventKind::StreamRestarted {
                            error: error.clone(),
                        },
                    );
                }
                tracing::warn!("Stream for {} stopped: {error}", pipe_ref.label());
                restarts += 1;
                if options.max_restarts.is_some_and(|m| restarts > m) {
                    return restarts;
                }
                if stop.sleep(backoff).await {
                    return restarts;
                }
                backoff = (backoff * 2).min(options.max_backoff);
            }
        }
    }
}

/// An in-memory [`StreamSource`] for tests: replays scripted messages, records acks, and can
/// fail its subscription once.
#[derive(Debug, Default)]
pub struct MemorySource {
    messages: std::sync::Mutex<Vec<StreamMessage>>,
    acked: std::sync::Mutex<Vec<AckToken>>,
    fail_first: AtomicBool,
    gap: Duration,
}

impl MemorySource {
    /// A source that replays `messages`, waiting `gap` between them.
    pub fn new(messages: Vec<StreamMessage>, gap: Duration) -> MemorySource {
        MemorySource {
            messages: std::sync::Mutex::new(messages),
            gap,
            ..Default::default()
        }
    }

    /// Fail the first subscription before sending anything.
    pub fn fail_first(self) -> MemorySource {
        self.fail_first.store(true, Ordering::SeqCst);
        self
    }

    /// The acknowledged tokens, in order.
    pub fn acked(&self) -> Vec<AckToken> {
        self.acked.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

#[async_trait]
impl StreamSource for MemorySource {
    async fn subscribe(
        &self,
        _pipe: &Pipe,
        sink: StreamSender,
        stop: StopToken,
    ) -> Result<(), String> {
        if self.fail_first.swap(false, Ordering::SeqCst) {
            return Err("connection refused".into());
        }
        let messages: Vec<StreamMessage> =
            std::mem::take(&mut *self.messages.lock().unwrap_or_else(|e| e.into_inner()));
        for m in messages {
            if stop.is_stopped() {
                break;
            }
            if sink.send(m).await.is_err() {
                break;
            }
            if !self.gap.is_zero() && stop.sleep(self.gap).await {
                break;
            }
        }
        Ok(())
    }

    async fn ack(&self, tokens: Vec<AckToken>) -> Result<(), String> {
        self.acked
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend(tokens);
        Ok(())
    }
}

/// `Value` re-exported for stream configs built in code.
pub fn stream_parameters(config: &StreamConfig) -> Map {
    let mut s = Map::new();
    s.insert(
        "idle_ms".into(),
        match config.idle {
            IdleWindow::Fixed(ms) => Value::Int(ms as i64),
            IdleWindow::Auto => Value::Str("auto".into()),
        },
    );
    s.insert(
        "max_latency_ms".into(),
        Value::Int(config.max_latency_ms as i64),
    );
    s.insert("max_rows".into(), Value::Int(config.max_rows as i64));
    s.insert("max_bytes".into(), Value::Int(config.max_bytes as i64));
    let mut out = Map::new();
    out.insert("stream".into(), Value::Map(s));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn triggers() {
        let mut b = MicroBatcher::new(StreamConfig {
            max_rows: 3,
            ..StreamConfig::default()
        });
        assert_eq!(b.push(0, 1, 10), None);
        assert_eq!(b.poll(100), None);
        assert_eq!(b.poll(250), Some(FlushTrigger::Idle));
        assert_eq!(b.push(100, 1, 10), None);
        assert_eq!(b.next_deadline_ms(), Some(350));
        assert_eq!(b.push(200, 1, 10), Some(FlushTrigger::MaxRows));
        assert_eq!(b.take(), (3, 30));
        assert_eq!(b.poll(10_000), None);
        // A steady trickle never goes idle, so max latency fires.
        let mut b = MicroBatcher::new(StreamConfig::default());
        for t in (0..=2000).step_by(100) {
            b.push(t, 1, 1);
        }
        assert_eq!(b.poll(2000), Some(FlushTrigger::MaxLatency));
    }

    #[test]
    fn auto_idle_window() {
        let params = Value::from_json_str(
            r#"{"stream": {"idle_ms": "auto", "max_latency_ms": 1000, "max_bytes": 64}}"#,
        )
        .unwrap()
        .to_map();
        let cfg = StreamConfig::from_parameters(&params);
        assert_eq!(cfg.idle, IdleWindow::Auto);
        assert_eq!(cfg.max_bytes, 64);
        let mut b = MicroBatcher::new(cfg);
        for t in [0, 10, 20, 30] {
            b.push(t, 1, 1);
        }
        assert_eq!(b.idle_window_ms(), 50);
        let mut b = MicroBatcher::new(cfg);
        for t in [0, 900, 1800] {
            b.push(t, 1, 1);
        }
        assert_eq!(b.idle_window_ms(), 1000);
        assert_eq!(b.push(1801, 1, 70), Some(FlushTrigger::MaxBytes));
    }

    /// Records flushes; fails the first `fail` of them.
    #[derive(Default)]
    struct Recorder {
        flushes: std::sync::Mutex<Vec<(usize, bool)>>,
        fail: std::sync::Mutex<usize>,
    }

    #[async_trait]
    impl StreamFlush for Recorder {
        async fn flush(&self, _pipe: &Pipe, input: PipeInput, check_existing: bool) -> SyncResult {
            {
                let mut fail = self.fail.lock().unwrap();
                if *fail > 0 {
                    *fail -= 1;
                    return SyncResult::new(false, "database is locked");
                }
            }
            let n = match input {
                PipeInput::Docs(d) => d.len(),
                _ => 0,
            };
            self.flushes.lock().unwrap().push((n, check_existing));
            SyncResult::new(true, format!("Inserted {n}, updated 0 rows."))
        }
    }

    fn msg(i: u64, redelivered: bool) -> StreamMessage {
        let mut doc = IndexMap::new();
        doc.insert("id".to_string(), KValue::Int(i as i64));
        StreamMessage {
            docs: vec![doc],
            bytes: 10,
            ack: Some(i),
            redelivered,
        }
    }

    fn pipe() -> Pipe {
        Pipe::builder("mqtt:test", "readings")
            .instance("sql:main")
            .build()
            .unwrap()
    }

    /// Feed `messages` at `gap_ms` intervals into a sink, then stop it after `stop_after_ms`.
    async fn run(
        config: StreamConfig,
        options: SinkOptions,
        messages: Vec<(u64, StreamMessage)>,
        recorder: &Recorder,
        source: &MemorySource,
        stop_after_ms: u64,
    ) -> SinkStats {
        let (tx, rx) = mpsc::channel(64);
        let stop = StopToken::new();
        let feeder = {
            let stop = stop.clone();
            async move {
                for (at, m) in messages {
                    tokio::time::sleep_until(
                        tokio::time::Instant::now() + Duration::from_millis(at),
                    )
                    .await;
                    tx.send(m).await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(stop_after_ms)).await;
                stop.stop();
                drop(tx);
            }
        };
        let p = pipe();
        let (stats, ()) = tokio::join!(
            run_sink(&p, config, options, rx, source, recorder, None, &stop),
            feeder
        );
        stats
    }

    #[tokio::test(start_paused = true)]
    async fn sink_flushes_on_rows_and_idle_then_acks() {
        let recorder = Recorder::default();
        let source = MemorySource::default();
        let config = StreamConfig {
            idle: IdleWindow::Fixed(100),
            max_rows: 3,
            ..StreamConfig::default()
        };
        // Three back to back (max_rows), then two more and silence (idle).
        let messages = (1..=5).map(|i| (0, msg(i, false))).collect();
        let stats = run(
            config,
            SinkOptions::default(),
            messages,
            &recorder,
            &source,
            500,
        )
        .await;
        assert_eq!(
            *recorder.flushes.lock().unwrap(),
            vec![(3, true), (2, true)]
        );
        assert_eq!(source.acked(), vec![1, 2, 3, 4, 5]);
        assert_eq!(stats.flushes, 2);
        assert_eq!(stats.rows, 5);
    }

    #[tokio::test(start_paused = true)]
    async fn sink_flushes_on_max_latency_under_a_steady_trickle() {
        let recorder = Recorder::default();
        let source = MemorySource::default();
        let config = StreamConfig {
            idle: IdleWindow::Fixed(100),
            max_latency_ms: 500,
            ..StreamConfig::default()
        };
        // One message every 60 ms never goes idle; max latency cuts batches.
        let messages = (1..=20).map(|i| (60, msg(i, false))).collect();
        let stats = run(
            config,
            SinkOptions::default(),
            messages,
            &recorder,
            &source,
            1000,
        )
        .await;
        let flushes = recorder.flushes.lock().unwrap().clone();
        assert!(flushes.len() >= 2, "{flushes:?}");
        assert!(flushes.iter().all(|(n, _)| *n <= 10), "{flushes:?}");
        assert_eq!(flushes.iter().map(|(n, _)| n).sum::<usize>(), 20);
        assert_eq!(stats.acked, 20);
    }

    #[tokio::test(start_paused = true)]
    async fn sink_acks_only_after_a_commit_and_retries_failed_flushes() {
        let recorder = Recorder {
            fail: std::sync::Mutex::new(1),
            ..Default::default()
        };
        let source = MemorySource::default();
        let config = StreamConfig {
            idle: IdleWindow::Fixed(50),
            ..StreamConfig::default()
        };
        let options = SinkOptions {
            skip_check_existing: true,
            retry_delay: Duration::from_millis(200),
        };
        // A redelivered message forces `check_existing` on for its batch.
        let messages = vec![(0, msg(1, false)), (0, msg(2, true))];
        let stats = run(config, options, messages, &recorder, &source, 1000).await;
        assert_eq!(stats.failed_flushes, 1);
        assert_eq!(*recorder.flushes.lock().unwrap(), vec![(2, true)]);
        assert_eq!(source.acked(), vec![1, 2]);
    }

    #[tokio::test(start_paused = true)]
    async fn sink_flushes_and_acks_on_stop() {
        let recorder = Recorder::default();
        let source = MemorySource::default();
        let config = StreamConfig {
            idle: IdleWindow::Fixed(10_000),
            max_latency_ms: 60_000,
            ..StreamConfig::default()
        };
        let messages = vec![(0, msg(1, false)), (0, msg(2, false))];
        let stats = run(
            config,
            SinkOptions::default(),
            messages,
            &recorder,
            &source,
            100,
        )
        .await;
        assert_eq!(*recorder.flushes.lock().unwrap(), vec![(2, true)]);
        assert_eq!(source.acked(), vec![1, 2]);
        assert_eq!(stats.flushes, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn supervisor_restarts_with_backoff_until_stopped() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stop = StopToken::new();
        let r = PipeRef::from_string("sql:main|mqtt:test|readings|None");
        let options = SuperviseOptions {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(400),
            max_restarts: None,
        };
        let stopper = {
            let stop = stop.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(1000)).await;
                stop.stop();
            }
        };
        let c = calls.clone();
        let (restarts, ()) = tokio::join!(
            supervise(
                &r,
                move || {
                    c.fetch_add(1, Ordering::SeqCst);
                    async { Err::<(), String>("connection refused".into()) }
                },
                options,
                None,
                &stop,
            ),
            stopper
        );
        // Backoff 100, 200, 400, 400 ... within one second: 0, 100, 300, 700 → 4 attempts.
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(restarts, 4);
    }
}
