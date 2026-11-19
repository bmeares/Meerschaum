//! The retry loop of `meerschaum.connectors.poll.retry_connect()` (v4's `_wrap_retry_connect`).
//!
//! `retry_connect()` keeps trying to connect to an instance until it answers, `max_retries`
//! attempts have failed, or the user quits. The loop and its messages are here, independent of
//! the connector type and of the host that prints: a [`RetryProbe`] makes one attempt (the SQL,
//! Valkey and API connector crates each implement one), [`RetryEvents`] receives the messages
//! (the Python extension prints them through `warn()`, `info()` and `dprint()`) and a
//! [`RetryWaiter`] waits between attempts (the Python extension also listens on stdin for
//! Enter and `q`, which this crate does not know about).
//!
//! The counting is v4's, attempt for attempt: the first try is attempt 1 of `max_retries`; after
//! every failed attempt, the last one included, the loop waits `retry_wait` seconds (nothing when
//! it is not positive) before counting the attempt; `max_retries` of `0` makes no attempt at
//! all. The messages of a failed attempt, with `warn` set:
//!
//! 1. the probe's own lines (v4 printed the driver's error text),
//! 2. a warning, `Connection to '<keys>' failed.` and `Press [Enter] to retry or wait <n> seconds.`
//!    on the next line,
//! 3. with `workers > 1`, `To quit, press CTRL-C, then 'q' + [Enter] for each worker (<workers>).`
//! 4. `Failed connection attempt (<attempt> / <max_retries>)`.
//!
//! `print_on_connect` prints `Connected to '<keys>'.` when an attempt succeeds. With `debug`,
//! each attempt starts with two debug lines.
//!
//! The text never contains the connector's attributes, only `keys`: what the caller passes for
//! `str(connector)` (`sql:main`).

use std::future::Future;
use std::time::Duration;

/// How an attempt ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attempt {
    /// The instance answered.
    Connected,
    /// The attempt failed; the loop reports it and tries again.
    Failed,
    /// Do not try again (an API instance that refuses chaining).
    Refused,
}

/// How the loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome {
    /// An attempt connected.
    Connected,
    /// `max_retries` attempts failed (also `max_retries` of `0`).
    Exhausted,
    /// A probe refused to go on ([`Attempt::Refused`]).
    Refused,
    /// The user answered the wait with `q` (or another quit word).
    Quit,
}

impl RetryOutcome {
    /// `retry_connect()`'s return value: only a connection is `True`.
    pub fn connected(self) -> bool {
        self == RetryOutcome::Connected
    }
}

/// What a wait ended with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wait {
    /// The time passed, or the user pressed Enter (retry now).
    Elapsed,
    /// The user typed a quit word.
    Quit,
}

/// The words that quit a wait (v4's `('q', 'quit', 'pass', 'exit', 'stop')`).
pub const QUIT_WORDS: [&str; 5] = ["q", "quit", "pass", "exit", "stop"];

/// How a line typed during a wait ends it: a quit word quits (the line is compared as typed,
/// without its line ending, as v4 compared `input()`'s text), anything else retries at once.
pub fn classify_input_line(line: &str) -> Wait {
    let text = line.strip_suffix('\n').unwrap_or(line);
    let text = text.strip_suffix('\r').unwrap_or(text);
    if QUIT_WORDS.contains(&text) {
        Wait::Quit
    } else {
        Wait::Elapsed
    }
}

/// The settings of one loop.
#[derive(Debug, Clone)]
pub struct RetryOptions {
    /// `str(connector)`, the name in every message (`sql:main`).
    pub keys: String,
    /// How many attempts to make at most.
    pub max_retries: u64,
    /// Seconds between attempts; a wait is skipped when this is not positive.
    pub retry_wait: f64,
    /// `retry_wait` as the caller wrote it (`3`, `0.5`), for `or wait <n> seconds.`
    pub wait_text: String,
    /// `workers`: more than one adds the quit hint to the messages.
    pub workers: i64,
    /// Print the warnings and the failed attempt's lines.
    pub warn: bool,
    /// Print a message when an attempt connects.
    pub print_on_connect: bool,
    /// Print the debug lines.
    pub debug: bool,
    /// An attempt that has not ended after this long counts as failed. v4 had no bound (a server
    /// that accepts and never answers held the loop forever).
    pub attempt_timeout: Option<Duration>,
}

impl RetryOptions {
    /// Options with v4's defaults: 50 attempts, 3 seconds apart, warnings on.
    pub fn new(keys: impl Into<String>) -> RetryOptions {
        RetryOptions {
            keys: keys.into(),
            max_retries: 50,
            retry_wait: 3.0,
            wait_text: "3".into(),
            workers: 1,
            warn: true,
            print_on_connect: false,
            debug: false,
            attempt_timeout: None,
        }
    }

    /// The pause after a failed attempt, `None` when it is skipped.
    pub fn wait_duration(&self) -> Option<Duration> {
        (self.retry_wait.is_finite() && self.retry_wait > 0.0)
            .then(|| Duration::try_from_secs_f64(self.retry_wait).unwrap_or(Duration::MAX))
    }
}

/// The receiver of the loop's messages. Called from the loop's thread, one at a time.
pub trait RetryEvents: Send {
    /// `warn(message, stack=False)`.
    fn warn(&mut self, message: &str);
    /// `info(message)`.
    fn info(&mut self, message: &str);
    /// `dprint(message)`.
    fn debug(&mut self, message: &str);
    /// A line on standard output without a prefix: v4 `print`ed a failed attempt's error text.
    fn stdout(&mut self, line: &str);
}

/// Collects the messages as `"warn: ..."`, `"info: ..."`, `"debug: ..."` and `"out: ..."` lines,
/// for hosts that report them later and for tests.
#[derive(Debug, Default)]
pub struct CollectEvents {
    /// The messages, in order.
    pub lines: Vec<String>,
}

impl RetryEvents for CollectEvents {
    fn warn(&mut self, message: &str) {
        self.lines.push(format!("warn: {message}"));
    }

    fn info(&mut self, message: &str) {
        self.lines.push(format!("info: {message}"));
    }

    fn debug(&mut self, message: &str) {
        self.lines.push(format!("debug: {message}"));
    }

    fn stdout(&mut self, line: &str) {
        self.lines.push(format!("out: {line}"));
    }
}

/// What an attempt may say, bound to the loop's `warn` setting.
pub struct AttemptCx<'a> {
    events: &'a mut dyn RetryEvents,
    warn: bool,
}

impl<'a> AttemptCx<'a> {
    /// A context over `events`; `warn` is the loop's setting.
    pub fn new(events: &'a mut dyn RetryEvents, warn: bool) -> AttemptCx<'a> {
        AttemptCx { events, warn }
    }

    /// Whether the caller wants the failed attempts explained (`warn`).
    pub fn warns(&self) -> bool {
        self.warn
    }

    /// A line of the failed attempt's own text, on standard output; skipped without `warn`.
    pub fn detail(&mut self, line: &str) {
        if self.warn {
            self.events.stdout(line);
        }
    }

    /// A warning, skipped without `warn`.
    pub fn warn_user(&mut self, message: &str) {
        if self.warn {
            self.events.warn(message);
        }
    }
}

/// One connection attempt against an instance. A probe keeps what must survive between
/// attempts (the API probe remembers the chaining status, the Valkey probe its connection).
pub trait RetryProbe: Send {
    /// Make one attempt.
    fn attempt(&mut self, cx: &mut AttemptCx<'_>) -> impl Future<Output = Attempt> + Send;
}

/// The wait between attempts.
pub trait RetryWaiter: Send {
    /// Wait `wait`, or until the user answers.
    fn wait(
        &mut self,
        wait: Duration,
        events: &mut dyn RetryEvents,
    ) -> impl Future<Output = Wait> + Send;
}

/// A waiter that only sleeps.
#[derive(Debug, Default, Clone, Copy)]
pub struct SleepWaiter;

impl RetryWaiter for SleepWaiter {
    async fn wait(&mut self, wait: Duration, _events: &mut dyn RetryEvents) -> Wait {
        tokio::time::sleep(wait).await;
        Wait::Elapsed
    }
}

fn describe_secs(limit: Duration) -> String {
    let secs = limit.as_secs_f64();
    if (secs - secs.round()).abs() < 0.05 {
        format!("{} s", secs.round() as u64)
    } else {
        format!("{secs:.1} s")
    }
}

/// Run the loop. See the module documentation for the counting and the messages.
pub async fn retry_connect<P, W>(
    probe: &mut P,
    options: &RetryOptions,
    events: &mut dyn RetryEvents,
    waiter: &mut W,
) -> RetryOutcome
where
    P: RetryProbe,
    W: RetryWaiter,
{
    let keys = &options.keys;
    let max_retries = options.max_retries;
    let mut retries: u64 = 0;
    while retries < max_retries {
        if options.debug {
            events.debug(&format!("Trying to connect to '{keys}'..."));
            events.debug(&format!("Attempt ({} / {max_retries})", retries + 1));
        }

        let attempt = {
            let mut cx = AttemptCx::new(events, options.warn);
            match options.attempt_timeout {
                Some(limit) => match tokio::time::timeout(limit, probe.attempt(&mut cx)).await {
                    Ok(attempt) => attempt,
                    Err(_) => {
                        cx.detail(&format!(
                            "The connection attempt to '{keys}' did not finish within {}.",
                            describe_secs(limit)
                        ));
                        Attempt::Failed
                    }
                },
                None => probe.attempt(&mut cx).await,
            }
        };

        match attempt {
            Attempt::Connected => {
                if options.print_on_connect {
                    events.info(&format!("Connected to '{keys}'."));
                }
                return RetryOutcome::Connected;
            }
            Attempt::Refused => return RetryOutcome::Refused,
            Attempt::Failed => {}
        }

        if options.warn {
            events.warn(&format!(
                "Connection to '{keys}' failed.\n    Press [Enter] to retry or wait {} seconds.",
                options.wait_text
            ));
            if options.workers > 1 {
                events.info(&format!(
                    "To quit, press CTRL-C, then 'q' + [Enter] for each worker ({}).",
                    options.workers
                ));
            }
            events.info(&format!(
                "Failed connection attempt ({} / {max_retries})",
                retries + 1
            ));
        }

        if let Some(wait) = options.wait_duration()
            && waiter.wait(wait, events).await == Wait::Quit
        {
            return RetryOutcome::Quit;
        }
        retries += 1;
    }
    RetryOutcome::Exhausted
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    type Log = CollectEvents;

    /// Answers from a script; `None` entries after the script ends fail.
    struct Scripted {
        script: VecDeque<Attempt>,
        calls: usize,
        detail: Option<&'static str>,
    }

    impl Scripted {
        fn new(script: &[Attempt]) -> Scripted {
            Scripted {
                script: script.iter().copied().collect(),
                calls: 0,
                detail: None,
            }
        }
    }

    impl RetryProbe for Scripted {
        async fn attempt(&mut self, cx: &mut AttemptCx<'_>) -> Attempt {
            self.calls += 1;
            let next = self.script.pop_front().unwrap_or(Attempt::Failed);
            if next == Attempt::Failed
                && let Some(detail) = self.detail
            {
                cx.detail(detail);
            }
            next
        }
    }

    /// Records the waits instead of sleeping (the injected clock).
    #[derive(Default)]
    struct Clock {
        waits: Vec<Duration>,
        answers: VecDeque<Wait>,
    }

    impl RetryWaiter for Clock {
        async fn wait(&mut self, wait: Duration, _events: &mut dyn RetryEvents) -> Wait {
            self.waits.push(wait);
            self.answers.pop_front().unwrap_or(Wait::Elapsed)
        }
    }

    fn options(max_retries: u64, retry_wait: f64) -> RetryOptions {
        RetryOptions {
            max_retries,
            retry_wait,
            wait_text: format!("{retry_wait}"),
            ..RetryOptions::new("sql:test")
        }
    }

    async fn run(
        probe: &mut Scripted,
        options: &RetryOptions,
        clock: &mut Clock,
    ) -> (RetryOutcome, Vec<String>) {
        let mut log = Log::default();
        let outcome = retry_connect(probe, options, &mut log, clock).await;
        (outcome, log.lines)
    }

    #[tokio::test]
    async fn connects_on_the_first_try_without_waiting() {
        let mut probe = Scripted::new(&[Attempt::Connected]);
        let mut clock = Clock::default();
        let (outcome, lines) = run(&mut probe, &options(3, 2.0), &mut clock).await;
        assert_eq!(outcome, RetryOutcome::Connected);
        assert!(outcome.connected());
        assert_eq!(probe.calls, 1);
        assert!(clock.waits.is_empty());
        // `print_on_connect` is off: nothing is printed.
        assert!(lines.is_empty(), "{lines:?}");
    }

    #[tokio::test]
    async fn print_on_connect_names_the_keys() {
        let mut probe = Scripted::new(&[Attempt::Connected]);
        let mut clock = Clock::default();
        let mut opts = options(3, 0.0);
        opts.print_on_connect = true;
        let (_, lines) = run(&mut probe, &opts, &mut clock).await;
        assert_eq!(lines, ["info: Connected to 'sql:test'."]);
    }

    #[tokio::test]
    async fn connects_after_failures_and_waits_after_each_failure() {
        let mut probe = Scripted::new(&[Attempt::Failed, Attempt::Failed, Attempt::Connected]);
        let mut clock = Clock::default();
        let (outcome, lines) = run(&mut probe, &options(5, 2.0), &mut clock).await;
        assert_eq!(outcome, RetryOutcome::Connected);
        assert_eq!(probe.calls, 3);
        // One wait per failure, none after the success.
        assert_eq!(
            clock.waits,
            [Duration::from_secs(2), Duration::from_secs(2)]
        );
        assert_eq!(
            lines,
            [
                "warn: Connection to 'sql:test' failed.\n    Press [Enter] to retry or wait 2 seconds.",
                "info: Failed connection attempt (1 / 5)",
                "warn: Connection to 'sql:test' failed.\n    Press [Enter] to retry or wait 2 seconds.",
                "info: Failed connection attempt (2 / 5)",
            ]
        );
    }

    #[tokio::test]
    async fn exhaustion_waits_after_the_last_failure_too() {
        // v4 waits after every failure, the last included, then returns `False`.
        let mut probe = Scripted::new(&[]);
        let mut clock = Clock::default();
        let (outcome, lines) = run(&mut probe, &options(3, 1.5), &mut clock).await;
        assert_eq!(outcome, RetryOutcome::Exhausted);
        assert!(!outcome.connected());
        assert_eq!(probe.calls, 3);
        assert_eq!(clock.waits.len(), 3);
        assert_eq!(clock.waits[0], Duration::from_millis(1500));
        assert_eq!(
            lines.last().unwrap(),
            "info: Failed connection attempt (3 / 3)"
        );
    }

    #[tokio::test]
    async fn zero_retries_make_no_attempt() {
        let mut probe = Scripted::new(&[Attempt::Connected]);
        let mut clock = Clock::default();
        let (outcome, lines) = run(&mut probe, &options(0, 1.0), &mut clock).await;
        assert_eq!(outcome, RetryOutcome::Exhausted);
        assert_eq!(probe.calls, 0);
        assert!(lines.is_empty());
        assert!(clock.waits.is_empty());
    }

    #[tokio::test]
    async fn a_wait_that_is_not_positive_is_skipped() {
        for wait in [0.0, -1.0, f64::NAN] {
            let mut probe = Scripted::new(&[]);
            let mut clock = Clock::default();
            let (outcome, _) = run(&mut probe, &options(2, wait), &mut clock).await;
            assert_eq!(outcome, RetryOutcome::Exhausted);
            assert_eq!(probe.calls, 2);
            assert!(clock.waits.is_empty(), "wait {wait}");
        }
    }

    #[tokio::test]
    async fn warn_off_prints_nothing_but_the_connect_line() {
        let mut probe = Scripted::new(&[Attempt::Failed, Attempt::Connected]);
        probe.detail = Some("boom");
        let mut clock = Clock::default();
        let mut opts = options(3, 0.0);
        opts.warn = false;
        opts.print_on_connect = true;
        let (outcome, lines) = run(&mut probe, &opts, &mut clock).await;
        assert!(outcome.connected());
        assert_eq!(lines, ["info: Connected to 'sql:test'."]);
    }

    #[tokio::test]
    async fn a_failed_attempt_prints_its_detail_before_the_warning() {
        let mut probe = Scripted::new(&[]);
        probe.detail = Some("connection refused");
        let mut clock = Clock::default();
        let mut opts = options(1, 0.0);
        opts.workers = 3;
        let (_, lines) = run(&mut probe, &opts, &mut clock).await;
        assert_eq!(
            lines,
            [
                "out: connection refused",
                "warn: Connection to 'sql:test' failed.\n    Press [Enter] to retry or wait 0 seconds.",
                "info: To quit, press CTRL-C, then 'q' + [Enter] for each worker (3).",
                "info: Failed connection attempt (1 / 1)",
            ]
        );
    }

    #[tokio::test]
    async fn the_quit_hint_needs_more_than_one_worker() {
        for workers in [0, 1] {
            let mut probe = Scripted::new(&[]);
            let mut clock = Clock::default();
            let mut opts = options(1, 0.0);
            opts.workers = workers;
            let (_, lines) = run(&mut probe, &opts, &mut clock).await;
            assert!(!lines.iter().any(|l| l.contains("To quit")), "{lines:?}");
        }
    }

    #[tokio::test]
    async fn debug_lines_start_each_attempt() {
        let mut probe = Scripted::new(&[Attempt::Failed, Attempt::Connected]);
        let mut clock = Clock::default();
        let mut opts = options(4, 0.0);
        opts.debug = true;
        opts.warn = false;
        let (_, lines) = run(&mut probe, &opts, &mut clock).await;
        assert_eq!(
            lines,
            [
                "debug: Trying to connect to 'sql:test'...",
                "debug: Attempt (1 / 4)",
                "debug: Trying to connect to 'sql:test'...",
                "debug: Attempt (2 / 4)",
            ]
        );
    }

    #[tokio::test]
    async fn a_refusal_ends_the_loop_at_once() {
        let mut probe = Scripted::new(&[Attempt::Refused]);
        let mut clock = Clock::default();
        let (outcome, lines) = run(&mut probe, &options(5, 2.0), &mut clock).await;
        assert_eq!(outcome, RetryOutcome::Refused);
        assert!(!outcome.connected());
        assert_eq!(probe.calls, 1);
        assert!(clock.waits.is_empty());
        assert!(lines.is_empty());
    }

    #[tokio::test]
    async fn quitting_a_wait_ends_the_loop_after_that_attempt() {
        let mut probe = Scripted::new(&[]);
        let mut clock = Clock::default();
        clock.answers.push_back(Wait::Elapsed);
        clock.answers.push_back(Wait::Quit);
        let (outcome, lines) = run(&mut probe, &options(10, 1.0), &mut clock).await;
        assert_eq!(outcome, RetryOutcome::Quit);
        assert!(!outcome.connected());
        assert_eq!(probe.calls, 2);
        assert_eq!(
            lines.last().unwrap(),
            "info: Failed connection attempt (2 / 10)"
        );
    }

    /// An attempt that never ends.
    struct Hangs;

    impl RetryProbe for Hangs {
        async fn attempt(&mut self, _cx: &mut AttemptCx<'_>) -> Attempt {
            std::future::pending::<()>().await;
            Attempt::Connected
        }
    }

    #[tokio::test]
    async fn an_attempt_that_outlasts_its_timeout_fails() {
        let mut opts = options(2, 0.0);
        opts.attempt_timeout = Some(Duration::from_millis(40));
        let mut log = Log::default();
        let mut clock = Clock::default();
        let started = std::time::Instant::now();
        let outcome = retry_connect(&mut Hangs, &opts, &mut log, &mut clock).await;
        assert_eq!(outcome, RetryOutcome::Exhausted);
        assert!(started.elapsed() >= Duration::from_millis(80));
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(
            log.lines
                .iter()
                .filter(
                    |l| l.starts_with("out: The connection attempt to 'sql:test' did not finish")
                )
                .count(),
            2,
            "{:?}",
            log.lines
        );
        assert!(
            log.lines
                .contains(&"info: Failed connection attempt (2 / 2)".to_string())
        );
    }

    #[tokio::test]
    async fn a_timeout_does_not_limit_a_probe_that_answers_in_time() {
        let mut probe = Scripted::new(&[Attempt::Connected]);
        let mut opts = options(2, 0.0);
        opts.attempt_timeout = Some(Duration::from_secs(30));
        let mut clock = Clock::default();
        let (outcome, _) = run(&mut probe, &opts, &mut clock).await;
        assert!(outcome.connected());
    }

    #[tokio::test]
    async fn the_sleep_waiter_waits_real_time() {
        let mut waiter = SleepWaiter;
        let mut log = Log::default();
        let started = std::time::Instant::now();
        let result = waiter.wait(Duration::from_millis(30), &mut log).await;
        assert_eq!(result, Wait::Elapsed);
        assert!(started.elapsed() >= Duration::from_millis(30));
    }

    #[test]
    fn typed_lines_retry_or_quit() {
        for word in QUIT_WORDS {
            assert_eq!(classify_input_line(&format!("{word}\n")), Wait::Quit);
            assert_eq!(classify_input_line(&format!("{word}\r\n")), Wait::Quit);
            assert_eq!(classify_input_line(word), Wait::Quit);
        }
        // Compared as typed: no trimming, no case folding.
        for text in ["\n", "", "Q\n", " q\n", "quit now\n", "retry\n"] {
            assert_eq!(classify_input_line(text), Wait::Elapsed, "{text:?}");
        }
    }

    #[test]
    fn the_wait_duration_follows_the_option() {
        let mut opts = RetryOptions::new("sql:test");
        assert_eq!(opts.wait_duration(), Some(Duration::from_secs(3)));
        opts.retry_wait = 0.25;
        assert_eq!(opts.wait_duration(), Some(Duration::from_millis(250)));
        opts.retry_wait = 0.0;
        assert_eq!(opts.wait_duration(), None);
        opts.retry_wait = f64::INFINITY;
        assert_eq!(opts.wait_duration(), None);
    }
}
