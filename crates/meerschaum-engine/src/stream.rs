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
//! bounded by `[50 ms, max_latency_ms]`. [`MicroBatcher`] is the pure state machine; the
//! subscription supervisor (restart with backoff, acknowledge after commit) arrives with the
//! connectors.

use meerschaum_core::{Map, Value};
use serde::{Deserialize, Serialize};

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
}
