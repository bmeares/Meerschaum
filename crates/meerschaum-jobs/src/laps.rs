//! Lap loops (§5.5 of the design): run a closure, then sleep until `min_seconds` after the lap
//! began, honoring stop and pause requests.
//!
//! PARITY: v4.1.4's loops (`sync pipes --loop` in `actions/sync.py`, pipelines in
//! `actions/start.py`) sleep `min_seconds` *after* each lap ends, so a lap of `d` seconds repeats
//! every `d + min_seconds`. The v5 design reproduces laps as "sleep until `min_seconds` after the
//! lap began". [`SleepFrom::LapBegin`] is the design's behavior (the default) and
//! [`SleepFrom::LapEnd`] reproduces v4 exactly.
//!
//! A failed lap (the closure returns `Err`) is retried after a cooldown of `2 * (min_seconds + 1)`
//! seconds, growing by half (`int(cooldown * 1.5)`) on consecutive failures, like `_sync_pipes`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

/// Where the inter-lap sleep is measured from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SleepFrom {
    /// Sleep until `min_seconds` after the lap began (the v5 design, §5.5).
    #[default]
    LapBegin,
    /// Sleep `min_seconds` after the lap ended (v4's `time.sleep(min_seconds)`).
    LapEnd,
}

/// The run state requested through a [`LoopControl`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    /// Keep running laps.
    Running,
    /// Finish the current lap, then wait (without starting laps) until resumed.
    Paused,
    /// Stop after the current lap (and wake any sleep).
    Stopped,
}

/// A handle to stop, pause and resume a [`LoopRunner`] from elsewhere.
#[derive(Debug, Clone)]
pub struct LoopControl {
    tx: Arc<watch::Sender<RunState>>,
}

impl Default for LoopControl {
    fn default() -> Self {
        LoopControl::new()
    }
}

impl LoopControl {
    /// A new control in the running state.
    pub fn new() -> LoopControl {
        let (tx, _rx) = watch::channel(RunState::Running);
        LoopControl { tx: Arc::new(tx) }
    }

    /// Request a stop (like `request_stop()` / SIGTERM).
    pub fn stop(&self) {
        self.tx.send_replace(RunState::Stopped);
    }

    /// Pause between laps.
    pub fn pause(&self) {
        self.tx.send_if_modified(|s| {
            if *s == RunState::Running {
                *s = RunState::Paused;
                true
            } else {
                false
            }
        });
    }

    /// Resume after a pause.
    pub fn resume(&self) {
        self.tx.send_if_modified(|s| {
            if *s == RunState::Paused {
                *s = RunState::Running;
                true
            } else {
                false
            }
        });
    }

    /// The current state.
    pub fn state(&self) -> RunState {
        *self.tx.borrow()
    }

    fn subscribe(&self) -> watch::Receiver<RunState> {
        self.tx.subscribe()
    }
}

/// What happened during a [`LoopRunner::run`].
#[derive(Debug, Clone, PartialEq)]
pub struct LoopSummary {
    /// Laps started.
    pub laps: u64,
    /// Laps whose closure failed.
    pub failures: u64,
    /// When each lap began, relative to the start of the run.
    pub lap_starts: Vec<Duration>,
}

/// Runs laps of a closure on a cadence.
#[derive(Debug, Clone)]
pub struct LoopRunner {
    /// `--min-seconds`.
    pub min_seconds: f64,
    /// Where the sleep is measured from.
    pub sleep_from: SleepFrom,
    /// Stop after this many laps (`x3` pipelines); `None` loops until stopped.
    pub max_laps: Option<u64>,
    control: LoopControl,
}

impl LoopRunner {
    /// A runner with `min_seconds` between laps (measured per [`SleepFrom::LapBegin`]).
    pub fn new(min_seconds: f64) -> LoopRunner {
        LoopRunner {
            min_seconds,
            sleep_from: SleepFrom::default(),
            max_laps: None,
            control: LoopControl::new(),
        }
    }

    /// Measure the sleep from the lap's end (v4) or begin (v5).
    pub fn sleep_from(mut self, sleep_from: SleepFrom) -> LoopRunner {
        self.sleep_from = sleep_from;
        self
    }

    /// Stop after `n` laps.
    pub fn max_laps(mut self, n: u64) -> LoopRunner {
        self.max_laps = Some(n);
        self
    }

    /// The control handle.
    pub fn control(&self) -> LoopControl {
        self.control.clone()
    }

    fn cooldown(min_seconds: f64) -> f64 {
        2.0 * (min_seconds + 1.0)
    }

    /// Sleep until `deadline`, waking early on a stop. Returns `false` if stopped.
    async fn sleep_until(rx: &mut watch::Receiver<RunState>, deadline: Instant) -> bool {
        loop {
            if *rx.borrow() == RunState::Stopped {
                return false;
            }
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => return true,
                changed = rx.changed() => {
                    if changed.is_err() {
                        tokio::time::sleep_until(deadline).await;
                        return true;
                    }
                }
            }
        }
    }

    /// Wait while paused. Returns `false` if stopped.
    async fn wait_unpaused(rx: &mut watch::Receiver<RunState>) -> bool {
        loop {
            let state = *rx.borrow_and_update();
            match state {
                RunState::Running => return true,
                RunState::Stopped => return false,
                RunState::Paused => {
                    if rx.changed().await.is_err() {
                        return false;
                    }
                }
            }
        }
    }

    /// Run laps until stopped (or `max_laps`). The closure receives the lap number (from 0).
    pub async fn run<F, Fut, E>(&self, mut lap: F) -> LoopSummary
    where
        F: FnMut(u64) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let mut rx = self.control.subscribe();
        let start = Instant::now();
        let mut summary = LoopSummary {
            laps: 0,
            failures: 0,
            lap_starts: Vec::new(),
        };
        let mut cooldown = Self::cooldown(self.min_seconds);
        loop {
            if !Self::wait_unpaused(&mut rx).await {
                break;
            }
            if self.max_laps.is_some_and(|m| summary.laps >= m) {
                break;
            }
            let lap_begin = Instant::now();
            summary.lap_starts.push(lap_begin - start);
            let n = summary.laps;
            summary.laps += 1;
            let ok = lap(n).await.is_ok();
            if !ok {
                summary.failures += 1;
                let deadline = Instant::now() + Duration::from_secs_f64(cooldown.max(0.0));
                if !Self::sleep_until(&mut rx, deadline).await {
                    break;
                }
                cooldown = (cooldown * 1.5).trunc();
                continue;
            }
            cooldown = Self::cooldown(self.min_seconds);
            if self.max_laps.is_some_and(|m| summary.laps >= m) {
                break;
            }
            if self.min_seconds > 0.0 {
                let min = Duration::from_secs_f64(self.min_seconds);
                let deadline = match self.sleep_from {
                    SleepFrom::LapBegin => lap_begin + min,
                    SleepFrom::LapEnd => Instant::now() + min,
                };
                if !Self::sleep_until(&mut rx, deadline).await {
                    break;
                }
            } else if *rx.borrow() == RunState::Stopped {
                break;
            }
        }
        summary
    }
}

/// What a synchronous loop does between two laps (see [`LapPacer`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LapPause {
    /// The lap succeeded: wait this long (`--min-seconds`).
    Sleep(Duration),
    /// The lap failed: wait this cooldown before trying again.
    Cooldown(Duration),
}

impl LapPause {
    /// How long to wait.
    pub fn duration(self) -> Duration {
        match self {
            LapPause::Sleep(d) | LapPause::Cooldown(d) => d,
        }
    }
}

/// The longest a [`LapPacer`]'s cooldown grows to: a loop that keeps failing (an outage of a
/// day) tries again at least hourly, unless its first cooldown is already longer.
pub const MAX_COOLDOWN_SECONDS: f64 = 3600.0;

/// The pace of a synchronous lap loop (`mrsm <action> --loop`): the same rules as [`LoopRunner`]
/// and `sync pipes --loop` for callers that run each lap on their own thread. A successful lap
/// is followed by `min_seconds` (after the lap ends by default, like `sync pipes --loop`); a
/// failed one by a cooldown of `2 * (min_seconds + 1)` seconds, growing by half on consecutive
/// failures up to [`MAX_COOLDOWN_SECONDS`] (or the first cooldown, when that is longer).
#[derive(Debug, Clone)]
pub struct LapPacer {
    min_seconds: f64,
    sleep_from: SleepFrom,
    cooldown: f64,
}

impl LapPacer {
    /// A pacer for `--min-seconds` (negative values count as 0), sleeping after each lap ends.
    pub fn new(min_seconds: f64) -> LapPacer {
        let min_seconds = if min_seconds.is_finite() {
            min_seconds.max(0.0)
        } else {
            0.0
        };
        LapPacer {
            min_seconds,
            sleep_from: SleepFrom::LapEnd,
            cooldown: LoopRunner::cooldown(min_seconds),
        }
    }

    /// Measure the sleep from the lap's begin instead (see [`SleepFrom`]).
    pub fn sleep_from(mut self, sleep_from: SleepFrom) -> LapPacer {
        self.sleep_from = sleep_from;
        self
    }

    /// `--min-seconds`.
    pub fn min_seconds(&self) -> f64 {
        self.min_seconds
    }

    /// The pause after a lap that began at `lap_began` and ended now.
    pub fn after_lap(&mut self, lap_began: std::time::Instant, success: bool) -> LapPause {
        let duration =
            |seconds: f64| Duration::try_from_secs_f64(seconds.max(0.0)).unwrap_or(Duration::MAX);
        if !success {
            let pause = duration(self.cooldown);
            let cap = MAX_COOLDOWN_SECONDS.max(LoopRunner::cooldown(self.min_seconds));
            self.cooldown = (self.cooldown * 1.5).trunc().min(cap);
            return LapPause::Cooldown(pause);
        }
        self.cooldown = LoopRunner::cooldown(self.min_seconds);
        let min = duration(self.min_seconds);
        LapPause::Sleep(match self.sleep_from {
            SleepFrom::LapEnd => min,
            SleepFrom::LapBegin => min.saturating_sub(lap_began.elapsed()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pacer_sleeps_after_successes_and_backs_off_after_failures() {
        let began = std::time::Instant::now();
        let mut pacer = LapPacer::new(1.0);
        assert_eq!(
            pacer.after_lap(began, true),
            LapPause::Sleep(Duration::from_secs(1))
        );
        // Cooldowns: 2 * (1 + 1) = 4, then int(4 * 1.5) = 6, then 9; a success resets them.
        for expected in [4, 6, 9] {
            assert_eq!(
                pacer.after_lap(began, false),
                LapPause::Cooldown(Duration::from_secs(expected))
            );
        }
        assert_eq!(
            pacer.after_lap(began, true).duration(),
            Duration::from_secs(1)
        );
        assert_eq!(
            pacer.after_lap(began, false),
            LapPause::Cooldown(Duration::from_secs(4))
        );
        // `--min-seconds 0` loops without sleeping; a negative value counts as 0.
        let mut fast = LapPacer::new(-3.0);
        assert_eq!(fast.min_seconds(), 0.0);
        assert_eq!(fast.after_lap(began, true), LapPause::Sleep(Duration::ZERO));
        // Measured from the lap's begin, a long lap leaves no sleep.
        let mut from_begin = LapPacer::new(0.001).sleep_from(SleepFrom::LapBegin);
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(
            from_begin.after_lap(began, true),
            LapPause::Sleep(Duration::ZERO)
        );
    }

    #[test]
    fn the_cooldown_stops_growing_at_an_hour() {
        let began = std::time::Instant::now();
        // A day of failures (`--min-seconds 1`): 4, 6, 9, ... then hourly, never days apart.
        let mut pacer = LapPacer::new(1.0);
        let pauses: Vec<f64> = (0..40)
            .map(|_| pacer.after_lap(began, false).duration().as_secs_f64())
            .collect();
        assert_eq!(&pauses[..3], &[4.0, 6.0, 9.0]);
        assert!(pauses.windows(2).all(|w| w[0] <= w[1]), "{pauses:?}");
        assert_eq!(pauses.last(), Some(&MAX_COOLDOWN_SECONDS));
        assert_eq!(
            pauses
                .iter()
                .filter(|p| **p == MAX_COOLDOWN_SECONDS)
                .count(),
            pauses.len()
                - pauses
                    .iter()
                    .position(|p| *p == MAX_COOLDOWN_SECONDS)
                    .unwrap()
        );
        // A success resets it.
        pacer.after_lap(began, true);
        assert_eq!(
            pacer.after_lap(began, false),
            LapPause::Cooldown(Duration::from_secs(4))
        );
        // A first cooldown longer than an hour (`--min-seconds 3600`) stays as it is.
        let mut hourly = LapPacer::new(3600.0);
        for _ in 0..5 {
            assert_eq!(
                hourly.after_lap(began, false),
                LapPause::Cooldown(Duration::from_secs(7202))
            );
        }
    }

    fn secs(v: &[f64]) -> Vec<Duration> {
        v.iter().map(|s| Duration::from_secs_f64(*s)).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn sleeps_until_min_seconds_after_lap_began() {
        let runner = LoopRunner::new(10.0).max_laps(4);
        let summary = runner
            .run(|n| async move {
                // Laps take 3, 12 (longer than min_seconds), 0 and 1 seconds.
                let d = [3.0, 12.0, 0.0, 1.0][n as usize];
                tokio::time::sleep(Duration::from_secs_f64(d)).await;
                Ok::<(), ()>(())
            })
            .await;
        assert_eq!(summary.laps, 4);
        assert_eq!(summary.lap_starts, secs(&[0.0, 10.0, 22.0, 32.0]));
    }

    #[tokio::test(start_paused = true)]
    async fn v4_sleeps_after_lap_end() {
        let runner = LoopRunner::new(10.0)
            .sleep_from(SleepFrom::LapEnd)
            .max_laps(3);
        let summary = runner
            .run(|_| async {
                tokio::time::sleep(Duration::from_secs(3)).await;
                Ok::<(), ()>(())
            })
            .await;
        assert_eq!(summary.lap_starts, secs(&[0.0, 13.0, 26.0]));
    }

    #[tokio::test(start_paused = true)]
    async fn stop_wakes_the_sleep_and_pause_holds_laps() {
        let runner = LoopRunner::new(60.0);
        let control = runner.control();
        let c2 = control.clone();
        let handle = tokio::spawn(async move { runner.run(|_| async { Ok::<(), ()>(()) }).await });
        // During the first sleep: pause, wait past the deadline, then resume.
        tokio::time::sleep(Duration::from_secs(5)).await;
        c2.pause();
        tokio::time::sleep(Duration::from_secs(100)).await;
        assert_eq!(control.state(), RunState::Paused);
        c2.resume();
        tokio::time::sleep(Duration::from_secs(1)).await;
        // Second lap began at 105 s (resumed), then stop in the middle of its sleep.
        control.stop();
        let summary = handle.await.unwrap();
        assert_eq!(summary.lap_starts, secs(&[0.0, 105.0]));
    }

    #[tokio::test(start_paused = true)]
    async fn failures_back_off() {
        let runner = LoopRunner::new(1.0).max_laps(4);
        let summary = runner
            .run(|n| async move { if n < 3 { Err(()) } else { Ok(()) } })
            .await;
        // Cooldowns: 2 * (1 + 1) = 4, then int(4 * 1.5) = 6, then 9.
        assert_eq!(summary.failures, 3);
        assert_eq!(summary.lap_starts, secs(&[0.0, 4.0, 10.0, 19.0]));
    }
}
