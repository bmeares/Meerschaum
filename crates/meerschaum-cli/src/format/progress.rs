//! Waiting feedback for slow actions: one spinner / progress line on **stderr**.
//!
//! The facility is process-wide and shared by every action:
//!
//! ```ignore
//! let progress = Progress::start(rt, args, "Counting rows");
//! progress.set_total(pipes.len());          // optional: "12/40" and a bar
//! let counter = progress.counter();         // `Clone + Send + 'static`, for async closures
//! counter.inc();
//! drop(progress);                           // clears the line; the output starts on a clean line
//! ```
//!
//! - It runs only when the runtime's [`ProgressSink`] says so: stderr is a terminal (set by
//!   `Runtime::system`; tests and piped output never see a byte), `--nopretty` is not given, and
//!   the Python entry's `sys.stderr` is a TTY. [`Progress::start`] otherwise returns an inert
//!   handle whose methods do nothing.
//! - A background thread redraws the line in place (`\r` + clear line) every 100 ms, so a
//!   blocked `block_on` still animates. Nothing is drawn for the first 250 ms: quick commands
//!   never flicker.
//! - `Runtime::print` / `eprint` and the prompts suspend the line while they write
//!   ([`suspend`] / [`resume`]), so interleaved output never garbles it.
//! - `formatting:unicode` off gives ASCII frames (`|/-\` and `#`/`-` bars); `formatting:ansi`
//!   off, `NO_COLOR` (and a dumb terminal) give the plain line without colors.
//! - `--debug` does not redraw in place: each stage prints one `message...` line instead.
//! - Nested [`Progress`] handles stack: the newest is shown and the previous one returns when it
//!   is dropped.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::dispatch::Runtime;
use crate::value::ArgsMap;

/// How often the line is redrawn.
const TICK: Duration = Duration::from_millis(100);
/// Nothing is drawn until the wait lasts this long.
const DELAY: Duration = Duration::from_millis(250);
/// Clear the current terminal line and return to column 0.
const CLEAR: &str = "\r\x1b[2K";
const UNICODE_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const ASCII_FRAMES: &[&str] = &["|", "/", "-", "\\"];

/// Where the progress line is written. The default is off.
#[derive(Clone, Default)]
pub enum ProgressSink {
    /// No progress output (tests, pipes, `--nopretty`-style hosts).
    #[default]
    Off,
    /// The process's stderr (only chosen when it is a terminal).
    Stderr,
    /// A host-provided writer (the Python entry writes through `sys.stderr`). Once `disabled`
    /// is set (the host abandoned the call) the sink is off: nothing is written and
    /// [`Progress::start`] returns an inert handle.
    Custom {
        /// Writes the text to the host's stderr.
        write: Arc<dyn Fn(&str) + Send + Sync>,
        /// Set to turn this sink off for good.
        disabled: Arc<AtomicBool>,
    },
}

impl ProgressSink {
    /// [`ProgressSink::Stderr`] when stderr is a terminal, else [`ProgressSink::Off`].
    pub fn stderr_if_tty() -> ProgressSink {
        if std::io::IsTerminal::is_terminal(&std::io::stderr()) {
            ProgressSink::Stderr
        } else {
            ProgressSink::Off
        }
    }

    /// A host-provided sink (see [`ProgressSink::Custom`]).
    pub fn custom(
        write: Arc<dyn Fn(&str) + Send + Sync>,
        disabled: Arc<AtomicBool>,
    ) -> ProgressSink {
        ProgressSink::Custom { write, disabled }
    }

    /// Whether anything is written.
    pub fn is_on(&self) -> bool {
        match self {
            ProgressSink::Off => false,
            ProgressSink::Stderr => true,
            ProgressSink::Custom { disabled, .. } => !disabled.load(Ordering::SeqCst),
        }
    }

    fn write(&self, text: &str) {
        use std::io::Write as _;
        match self {
            ProgressSink::Off => {}
            ProgressSink::Stderr => {
                let mut err = std::io::stderr().lock();
                let _ = err.write_all(text.as_bytes());
                let _ = err.flush();
            }
            ProgressSink::Custom { write, disabled } => {
                if !disabled.load(Ordering::SeqCst) {
                    write(text);
                }
            }
        }
    }
}

impl std::fmt::Debug for ProgressSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ProgressSink::Off => "Off",
            ProgressSink::Stderr => "Stderr",
            ProgressSink::Custom { .. } => "Custom",
        })
    }
}

/// How the line looks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineStyle {
    /// Braille spinner and block bars (else ASCII).
    pub unicode: bool,
    /// ANSI colors (spinner cyan, details dim).
    pub color: bool,
}

/// One displayed stage.
struct Frame {
    id: u64,
    message: Mutex<String>,
    done: AtomicU64,
    total: AtomicU64,
    started: Instant,
}

/// The process-wide spinner state.
struct State {
    frames: Vec<Arc<Frame>>,
    sink: ProgressSink,
    style: LineStyle,
    /// One `message...` line per stage instead of the animated line (`--debug`).
    lines: bool,
    /// A progress line is currently on the terminal.
    drawn: bool,
    /// The last stage printed in `lines` mode.
    last_line: String,
    /// Output is being written (or ended without a newline): do not draw.
    held: bool,
    /// Bumped when a new drawing thread starts, so an older one exits.
    generation: u64,
    next_id: u64,
    tick: usize,
    /// When the stack last went from empty to non-empty.
    since: Option<Instant>,
}

static STATE: Mutex<State> = Mutex::new(State {
    frames: Vec::new(),
    sink: ProgressSink::Off,
    style: LineStyle {
        unicode: true,
        color: false,
    },
    lines: false,
    drawn: false,
    last_line: String::new(),
    held: false,
    generation: 0,
    next_id: 1,
    tick: 0,
    since: None,
});

/// Make sure the process's signal watcher runs, so that a signal never leaves the spinner line on
/// the terminal.
///
/// The watcher is `shell::interrupt`'s (one thread for SIGINT and SIGTERM, started the first time
/// a progress line is drawn or an action runs, and kept for the life of the process: unregistering
/// a handler leaves the library's in place without restoring the default action). It erases the
/// line ([`clear_for_signal`]) before it ends the process, cancels the running action or ignores
/// the signal, depending on what is running. The spinner itself never re-raises a signal: Ctrl-C
/// belongs to the action (decision 1 of wave 18), which is no longer killed with the last frame on
/// the screen.
mod guard {
    /// Start the watcher (once per process).
    pub fn install() {
        crate::shell::interrupt::install(None);
    }
}

/// Erase the spinner line, if one is drawn: a signal is about to end the process, cancel the action
/// or be ignored, and nothing may be left on the terminal. Safe to call from any thread (the state
/// lock is never held across a draw).
pub(crate) fn clear_for_signal() {
    state().clear_line_for_signal();
    super::live::clear_for_signal();
}

/// A Python worker was started in this process: its output goes straight to the inherited
/// terminal, where an in-place redraw would glue the spinner to it. From then on spinners print
/// plain `message...` lines (like `--debug`) and never redraw.
static WORKERS: AtomicBool = AtomicBool::new(false);

/// Called (through `meerschaum_worker::worker::set_spawn_hook`) just before a worker starts:
/// erase the line now and stop redrawing.
fn workers_started() {
    WORKERS.store(true, Ordering::SeqCst);
    state().clear();
}

/// Drop every progress line of this process now and erase the one on screen: the host
/// abandoned the call that owned them (a second Ctrl-C in Python). The abandoned thread's
/// handles keep working but find nothing to draw. Call it without holding the GIL when the sink
/// writes through Python.
pub fn abandon() {
    super::live::clear_for_signal();
    let mut st = state();
    st.clear();
    st.frames.clear();
    st.held = false;
    st.since = None;
    st.generation += 1;
}

/// Coordinate Python worker output with the shell's progress line.
pub fn forward_shell_worker_output() {
    meerschaum_worker::worker::set_output_hook(write_worker_output);
    WORKERS.store(false, Ordering::SeqCst);
}

fn write_worker_output(stderr: bool, bytes: &[u8]) {
    // Held while the run monitor owns the terminal; written once it returns.
    if super::live::hold_or_pass(stderr, bytes) {
        return;
    }
    emit_worker_output(stderr, bytes);
}

/// Write forwarded worker output now, around the progress line and the inline live region.
pub(crate) fn emit_worker_output(stderr: bool, bytes: &[u8]) {
    use std::io::Write;
    let ended = bytes.is_empty() || bytes.ends_with(b"\n");
    let mut st = state();
    st.clear();
    // The inline live view of a graph run is erased too, and drawn again after a full line.
    super::live::suspend();
    if !ended {
        // A prompt (`input()`): the terminal goes back to the worker before the prompt shows.
        super::live::worker_prompt(true);
    }
    if stderr {
        let mut writer = std::io::stderr().lock();
        let _ = writer.write_all(bytes);
        let _ = writer.flush();
    } else {
        let mut writer = std::io::stdout().lock();
        let _ = writer.write_all(bytes);
        let _ = writer.flush();
    }
    if !bytes.is_empty() {
        st.held = !ended;
        if ended {
            super::live::worker_prompt(false);
        }
    }
    super::live::resume(ended);
}

fn state() -> MutexGuard<'static, State> {
    STATE.lock().unwrap_or_else(|e| e.into_inner())
}

impl State {
    fn clear(&mut self) {
        if self.drawn {
            self.sink.write(CLEAR);
            self.drawn = false;
        }
    }

    /// Erase the line a signal is about to end the process with, and leave the state as if
    /// every stage had ended.
    fn clear_line_for_signal(&mut self) {
        if self.drawn {
            self.sink.write(CLEAR);
            self.drawn = false;
        }
    }

    /// `--debug`: one `message...` line per stage, once the wait is noticeable.
    fn print_stage(&mut self) {
        let Some(top) = self.frames.last() else {
            return;
        };
        let message = top
            .message
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if message != self.last_line {
            self.sink.write(&format!("{message}...\n"));
            self.last_line = message;
        }
    }

    fn draw(&mut self) {
        let Some(top) = self.frames.last().cloned() else {
            return;
        };
        let first = self.frames.first().map_or(top.started, |f| f.started);
        let width = crate::format::terminal_width(2).unwrap_or(80);
        let text = line_text(
            &top.message.lock().unwrap_or_else(|e| e.into_inner()),
            top.done.load(Ordering::Relaxed),
            top.total.load(Ordering::Relaxed),
            first.elapsed(),
            self.tick,
            self.style,
            width,
        );
        self.tick = self.tick.wrapping_add(1);
        guard::install();
        self.sink.write(&format!("{CLEAR}{text}"));
        self.drawn = true;
    }
}

/// A shareable, `'static` handle that advances a [`Progress`] from async closures.
#[derive(Clone)]
pub struct Counter(Option<Arc<Frame>>);

impl Counter {
    /// One more item done.
    pub fn inc(&self) {
        if let Some(frame) = &self.0 {
            frame.done.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// One stage of waiting feedback. Dropping it clears the line.
pub struct Progress {
    frame: Option<Arc<Frame>>,
}

impl Progress {
    /// A handle that never draws (what [`Progress::start`] returns when feedback is off).
    pub fn disabled() -> Progress {
        Progress { frame: None }
    }

    /// Start waiting feedback with `message` (no trailing dots; "Counting rows"). Inert unless
    /// the runtime's sink is on and `--nopretty` is not given.
    pub fn start(rt: &mut Runtime<'_>, args: &ArgsMap, message: impl Into<String>) -> Progress {
        let nopretty = crate::native::flag(args, "nopretty");
        let debug = crate::native::flag(args, "debug");
        Progress::start_flags(rt, nopretty, debug, message)
    }

    /// [`Progress::start`] for callers that have no parsed arguments yet (only the raw
    /// command line).
    pub fn start_flags(
        rt: &mut Runtime<'_>,
        nopretty: bool,
        debug: bool,
        message: impl Into<String>,
    ) -> Progress {
        match Progress::context(rt, nopretty, debug) {
            Some((sink, style, lines)) => Progress::begin(sink, style, lines, message.into()),
            None => Progress::disabled(),
        }
    }

    /// The sink, style and `lines` mode [`Progress::begin`] needs for this runtime, or `None` when
    /// feedback is off (not a terminal, `--nopretty`). For callers that start the stage later, from
    /// another thread (the first-use driver download).
    pub fn context(
        rt: &mut Runtime<'_>,
        nopretty: bool,
        debug: bool,
    ) -> Option<(ProgressSink, LineStyle, bool)> {
        if !rt.progress.is_on() || nopretty {
            return None;
        }
        let settings = rt.settings();
        let term = (rt.env)("TERM").map(|t| t.to_lowercase());
        let dumb = matches!(term.as_deref(), Some("dumb" | "unknown"));
        let style = LineStyle {
            unicode: settings.unicode,
            color: settings.ansi && !settings.no_color && !dumb,
        };
        Some((rt.progress.clone(), style, debug))
    }

    /// Start on an explicit sink (the shared body of [`Progress::start`]; tests use it).
    pub fn begin(sink: ProgressSink, style: LineStyle, lines: bool, message: String) -> Progress {
        meerschaum_worker::worker::set_spawn_hook(workers_started);
        let mut st = state();
        let id = st.next_id;
        st.next_id += 1;
        let frame = Arc::new(Frame {
            id,
            message: Mutex::new(message.clone()),
            done: AtomicU64::new(0),
            total: AtomicU64::new(0),
            started: Instant::now(),
        });
        let first = st.frames.is_empty();
        if first {
            st.sink = sink;
            st.style = style;
            st.lines = lines;
            st.held = false;
            st.drawn = false;
            st.last_line.clear();
            st.tick = 0;
            st.since = Some(Instant::now());
        }
        st.frames.push(frame.clone());
        if first {
            st.generation += 1;
            let generation = st.generation;
            drop(st);
            std::thread::Builder::new()
                .name("mrsm-progress".into())
                .spawn(move || tick_loop(generation))
                .ok();
        }
        Progress { frame: Some(frame) }
    }

    /// Replace the message (a new stage of the same wait).
    pub fn set_message(&self, message: impl Into<String>) {
        let Some(frame) = &self.frame else { return };
        let message = message.into();
        *frame.message.lock().unwrap_or_else(|e| e.into_inner()) = message;
    }

    /// The number of items the wait is for: the line shows `done/total` and a bar.
    pub fn set_total(&self, total: usize) {
        if let Some(frame) = &self.frame {
            frame.total.store(total as u64, Ordering::Relaxed);
        }
    }

    /// One more item done.
    pub fn inc(&self) {
        self.counter().inc();
    }

    /// The number of items done so far (for serial loops that know their index).
    pub fn set_done(&self, done: usize) {
        if let Some(frame) = &self.frame {
            frame.done.store(done as u64, Ordering::Relaxed);
        }
    }

    /// A `Clone + Send + 'static` handle that advances this progress.
    pub fn counter(&self) -> Counter {
        Counter(self.frame.clone())
    }

    /// Clear the line now (same as dropping).
    pub fn finish(self) {}
}

impl Drop for Progress {
    fn drop(&mut self) {
        let Some(frame) = self.frame.take() else {
            return;
        };
        let mut st = state();
        st.frames.retain(|f| f.id != frame.id);
        st.clear();
        if st.frames.is_empty() {
            st.held = false;
            st.since = None;
            // Stops the drawing thread at its next tick.
            st.generation += 1;
        }
    }
}

fn tick_loop(generation: u64) {
    loop {
        std::thread::sleep(TICK);
        let mut st = state();
        if st.generation != generation || st.frames.is_empty() {
            return;
        }
        if st.held || st.since.is_none_or(|s| s.elapsed() < DELAY) {
            continue;
        }
        if st.lines || WORKERS.load(Ordering::SeqCst) {
            st.print_stage();
        } else {
            st.draw();
        }
    }
}

/// Erase the line and keep it off while output is written (called around every
/// `Runtime::print` / `eprint` and by the prompts). Pair with [`resume`]. The inline live region
/// of a graph run ([`super::live`]) is suspended and resumed with it.
pub fn suspend() {
    super::live::suspend();
    let mut st = state();
    if st.frames.is_empty() {
        return;
    }
    st.clear();
    st.held = true;
}

/// Let the line come back after output was written: `ended_with_newline` is whether the cursor
/// is at the start of a line (a partial line keeps the spinner off until a newline is written).
pub fn resume(ended_with_newline: bool) {
    super::live::resume(ended_with_newline);
    let mut st = state();
    if st.frames.is_empty() {
        return;
    }
    st.held = !ended_with_newline;
}

/// Whether a progress line is on screen or being shown (for tests).
pub fn is_active() -> bool {
    !state().frames.is_empty()
}

fn elapsed_text(elapsed: Duration) -> String {
    let secs = elapsed.as_secs();
    if secs < 10 {
        format!("{:.1}s", elapsed.as_secs_f64())
    } else if secs < 60 {
        format!("{secs}s")
    } else {
        format!("{}m{:02}s", secs / 60, secs % 60)
    }
}

/// Shorten `text` to `max` characters with an ellipsis.
fn truncate(text: &str, max: usize, unicode: bool) -> String {
    let len = text.chars().count();
    if len <= max {
        return text.to_string();
    }
    let ellipsis = if unicode { "…" } else { "..." };
    let keep = max.saturating_sub(ellipsis.chars().count());
    format!("{}{ellipsis}", text.chars().take(keep).collect::<String>())
}

/// The progress line for one tick, at most `width - 1` visible characters wide.
pub fn line_text(
    message: &str,
    done: u64,
    total: u64,
    elapsed: Duration,
    tick: usize,
    style: LineStyle,
    width: usize,
) -> String {
    let frames = if style.unicode {
        UNICODE_FRAMES
    } else {
        ASCII_FRAMES
    };
    let spinner = frames[tick % frames.len()];
    let budget = width.saturating_sub(1).max(20);
    let elapsed = elapsed_text(elapsed);
    let counts = (total > 0).then(|| format!("{}/{total}", done.min(total)));
    // `<spinner> <message>[ <bar> <done>/<total>]  <elapsed>`
    let fixed = 2 + 2 + elapsed.chars().count();
    let counts_w = counts.as_ref().map_or(0, |c| c.chars().count() + 1);
    let (fill, empty) = if style.unicode {
        ("█", "░")
    } else {
        ("#", "-")
    };
    let bar_w = if total > 0 && budget >= 60 { 10 } else { 0 };
    let bar_part = if bar_w > 0 {
        let filled = ((done.min(total) as usize) * bar_w) / (total as usize).max(1);
        format!(" {}{}", fill.repeat(filled), empty.repeat(bar_w - filled))
    } else {
        String::new()
    };
    let used = fixed + counts_w + if bar_w > 0 { bar_w + 1 } else { 0 };
    let message = truncate(message, budget.saturating_sub(used).max(8), style.unicode);
    let counts_part = counts.map(|c| format!(" {c}")).unwrap_or_default();
    if style.color {
        format!(
            "\x1b[36m{spinner}\x1b[0m {message}\x1b[2m{bar_part}{counts_part}  {elapsed}\x1b[0m"
        )
    } else {
        format!("{spinner} {message}{bar_part}{counts_part}  {elapsed}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    /// The state is process-wide: tests that start a `Progress` take this lock.
    static SERIAL: StdMutex<()> = StdMutex::new(());

    const PLAIN: LineStyle = LineStyle {
        unicode: true,
        color: false,
    };

    fn capture() -> (ProgressSink, Arc<StdMutex<String>>) {
        let out = Arc::new(StdMutex::new(String::new()));
        let sink = {
            let out = out.clone();
            ProgressSink::custom(
                Arc::new(move |s: &str| out.lock().unwrap().push_str(s)),
                Arc::new(AtomicBool::new(false)),
            )
        };
        (sink, out)
    }

    #[test]
    fn indeterminate_line() {
        let text = line_text(
            "Counting rows",
            0,
            0,
            Duration::from_millis(1400),
            0,
            PLAIN,
            80,
        );
        assert_eq!(text, "⠋ Counting rows  1.4s");
        let text = line_text(
            "Counting rows",
            0,
            0,
            Duration::from_secs(75),
            1,
            LineStyle {
                unicode: false,
                color: false,
            },
            80,
        );
        assert_eq!(text, "/ Counting rows  1m15s");
    }

    #[test]
    fn determinate_line_has_a_bar_and_counts() {
        let text = line_text(
            "Counting rows",
            12,
            40,
            Duration::from_secs(3),
            2,
            PLAIN,
            80,
        );
        assert_eq!(text, "⠹ Counting rows ███░░░░░░░ 12/40  3.0s");
        let ascii = LineStyle {
            unicode: false,
            color: false,
        };
        let text = line_text(
            "Counting rows",
            40,
            40,
            Duration::from_secs(3),
            0,
            ascii,
            80,
        );
        assert_eq!(text, "| Counting rows ########## 40/40  3.0s");
    }

    #[test]
    fn narrow_terminals_truncate_the_message_and_drop_the_bar() {
        let text = line_text(
            "Calculating the size of every selected pipe",
            3,
            9,
            Duration::from_secs(1),
            0,
            PLAIN,
            40,
        );
        assert!(text.chars().count() <= 39, "{text}");
        assert!(text.contains('…'), "{text}");
        assert!(text.contains("3/9"), "{text}");
        assert!(!text.contains('█') && !text.contains('░'), "{text}");
    }

    #[test]
    fn color_wraps_the_spinner_and_dims_the_details() {
        let text = line_text(
            "Working",
            0,
            0,
            Duration::from_secs(1),
            0,
            LineStyle {
                unicode: true,
                color: true,
            },
            80,
        );
        assert_eq!(text, "\x1b[36m⠋\x1b[0m Working\x1b[2m  1.0s\x1b[0m");
    }

    #[test]
    fn a_disabled_sink_never_draws_and_start_is_inert() {
        let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = Vec::new();
        let mut rt = Runtime::new(crate::types::DateContext::system(), 80, &mut out, |_| None);
        assert!(!rt.progress.is_on());
        let progress = Progress::start(&mut rt, &ArgsMap::new(), "Working");
        assert!(progress.frame.is_none());
        assert!(!is_active());
        progress.set_total(3);
        progress.inc();
        drop(progress);
        assert!(!is_active());
    }

    #[test]
    fn the_line_is_drawn_after_the_delay_and_cleared_when_done() {
        let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let (sink, out) = capture();
        let progress = Progress::begin(sink, PLAIN, false, "Working".into());
        // Nothing before the delay.
        std::thread::sleep(Duration::from_millis(120));
        assert_eq!(*out.lock().unwrap(), "");
        std::thread::sleep(Duration::from_millis(500));
        let drawn = out.lock().unwrap().clone();
        assert!(drawn.starts_with(CLEAR), "{drawn:?}");
        assert!(drawn.contains("Working"), "{drawn:?}");
        drop(progress);
        let all = out.lock().unwrap().clone();
        assert!(all.ends_with(CLEAR), "the line is cleared on drop: {all:?}");
        assert!(!is_active());
        // No more frames after the drop.
        let len = all.len();
        std::thread::sleep(Duration::from_millis(250));
        assert_eq!(out.lock().unwrap().len(), len);
    }

    #[test]
    fn quick_waits_draw_nothing_at_all() {
        let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let (sink, out) = capture();
        let progress = Progress::begin(sink, PLAIN, false, "Quick".into());
        progress.inc();
        drop(progress);
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(*out.lock().unwrap(), "");
    }

    #[test]
    fn suspend_clears_and_holds_until_a_full_line_is_written() {
        let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let (sink, out) = capture();
        let progress = Progress::begin(sink, PLAIN, false, "Working".into());
        std::thread::sleep(Duration::from_millis(500));
        suspend();
        let after_suspend = out.lock().unwrap().clone();
        assert!(after_suspend.ends_with(CLEAR), "{after_suspend:?}");
        // A partial line keeps it held.
        resume(false);
        std::thread::sleep(Duration::from_millis(250));
        assert_eq!(*out.lock().unwrap(), after_suspend);
        // A finished line lets it come back.
        resume(true);
        std::thread::sleep(Duration::from_millis(250));
        assert!(out.lock().unwrap().len() > after_suspend.len());
        drop(progress);
    }

    #[test]
    fn debug_mode_prints_one_line_per_stage_and_never_redraws() {
        let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let (sink, out) = capture();
        let progress = Progress::begin(sink, PLAIN, true, "Loading pipes".into());
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(*out.lock().unwrap(), "Loading pipes...\n");
        progress.set_message("Counting rows");
        std::thread::sleep(Duration::from_millis(250));
        drop(progress);
        assert_eq!(*out.lock().unwrap(), "Loading pipes...\nCounting rows...\n");
    }

    #[test]
    fn counters_advance_the_shown_frame() {
        let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let (sink, out) = capture();
        let progress = Progress::begin(sink, PLAIN, false, "Counting rows".into());
        progress.set_total(4);
        let counter = progress.counter();
        counter.inc();
        counter.clone().inc();
        std::thread::sleep(Duration::from_millis(500));
        assert!(
            out.lock().unwrap().contains("2/4"),
            "{:?}",
            out.lock().unwrap()
        );
        drop(progress);
    }

    #[test]
    fn abandon_erases_the_line_and_a_disabled_sink_stays_off() {
        let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let out = Arc::new(StdMutex::new(String::new()));
        let disabled = Arc::new(AtomicBool::new(false));
        let sink = {
            let out = out.clone();
            ProgressSink::custom(
                Arc::new(move |s: &str| out.lock().unwrap().push_str(s)),
                disabled.clone(),
            )
        };
        assert!(sink.is_on());
        let progress = Progress::begin(sink.clone(), PLAIN, false, "Working".into());
        std::thread::sleep(Duration::from_millis(500));
        assert!(out.lock().unwrap().contains("Working"));
        abandon();
        disabled.store(true, Ordering::SeqCst);
        let after = out.lock().unwrap().clone();
        assert!(after.ends_with(CLEAR), "{after:?}");
        assert!(!is_active());
        assert!(!sink.is_on());
        // Nothing more is drawn, and the stale handle is harmless.
        std::thread::sleep(Duration::from_millis(300));
        progress.inc();
        drop(progress);
        assert_eq!(*out.lock().unwrap(), after);
    }

    #[test]
    fn forwarded_worker_lines_clear_and_resume_the_spinner() {
        let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        WORKERS.store(false, Ordering::SeqCst);
        let (sink, out) = capture();
        let progress = Progress::begin(sink, PLAIN, false, "Syncing Pipes".into());
        progress.set_total(2);
        std::thread::sleep(Duration::from_millis(500));
        write_worker_output(false, b"worker partial");
        assert!(state().held);
        assert!(out.lock().unwrap().ends_with(CLEAR));
        write_worker_output(false, b" line\n");
        assert!(!state().held);
        let after = out.lock().unwrap().len();
        progress.inc();
        std::thread::sleep(Duration::from_millis(250));
        assert!(out.lock().unwrap()[after..].contains("1/2"));
        drop(progress);
    }

    #[test]
    fn a_started_worker_stops_the_redraw_and_leaves_plain_lines() {
        let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let (sink, out) = capture();
        let progress = Progress::begin(sink, PLAIN, false, "Loading plugins".into());
        std::thread::sleep(Duration::from_millis(500));
        assert!(out.lock().unwrap().contains('⠋'));
        // The worker's output is about to reach the terminal.
        workers_started();
        let after = out.lock().unwrap().clone();
        assert!(after.ends_with(CLEAR), "{after:?}");
        std::thread::sleep(Duration::from_millis(350));
        drop(progress);
        let tail = out.lock().unwrap()[after.len()..].to_string();
        assert_eq!(
            tail, "Loading plugins...\n",
            "no redraw after the worker started"
        );
        WORKERS.store(false, Ordering::SeqCst);
    }
}
