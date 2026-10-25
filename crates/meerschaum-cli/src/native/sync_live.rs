//! The inline live view of graph runs on a terminal (`format::live` draws it): the run goes on in
//! a task while this thread folds its events into the region, prints the permanent lines and
//! opens the full-screen run monitor when `w` is pressed.
//!
//! The events come through an unbounded channel, never a bounded broadcast receiver: a graph run
//! gives the engine a [`RunOptions::on_event`](meerschaum_engine::RunOptions) sink, and `--loop`
//! / `--listen` drain their broadcast receiver on a task. Nothing is lost while this thread is
//! busy (the run monitor is open, or a print is slow).

use std::sync::Arc;
use std::time::Instant;

use meerschaum_engine::{Event, EventSink};
use tokio::sync::broadcast;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::dispatch::Runtime;
use crate::format::live::{Keys, LiveRun, LiveStyle, REDRAW, Region, RegionSink, stdout_size};
use crate::native::{Instance, Session, flag};
use crate::value::ArgsMap;

/// Whether this run can show the inline view: stdout is a terminal that takes cursor movement
/// (ANSI on, not a dumb terminal), and neither `--nopretty` nor `--debug` (whose lines would be
/// written around the region) is given.
pub(super) fn possible(rt: &mut Runtime<'_>, args: &ArgsMap) -> bool {
    if !rt.live || flag(args, "nopretty") || flag(args, "debug") {
        return false;
    }
    if !std::io::IsTerminal::is_terminal(&std::io::stdout()) {
        return false;
    }
    let term = (rt.env)("TERM").map(|t| t.to_lowercase());
    if matches!(term.as_deref(), Some("dumb" | "unknown")) {
        return false;
    }
    rt.settings().ansi
}

/// Call before the engine is built when [`possible`]: Python workers started from now on write
/// through this process (like in the shell), so their output erases the region instead of
/// writing over it.
pub(super) fn prepare(rt: &mut Runtime<'_>, args: &ArgsMap) {
    if possible(rt, args) {
        crate::format::progress::forward_shell_worker_output();
    }
}

pub(super) fn style(rt: &mut Runtime<'_>) -> LiveStyle {
    let settings = rt.settings();
    LiveStyle {
        unicode: settings.unicode,
        color: settings.ansi && !settings.no_color,
    }
}

/// The key help of the header when the full-screen monitor can open here.
pub(super) const WATCH_HINT: &str = "w: watch";

/// Aborts the run's task when dropped armed: an interrupted wait (Ctrl-C drops the future
/// `Session::block_on` polls) or an unwind cancels the run, as when the run future itself was
/// dropped.
struct AbortOnDrop(Option<tokio::task::AbortHandle>);

impl AbortOnDrop {
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

enum Step {
    Event(Option<Event>),
    Done(Box<Result<meerschaum_engine::RunResult, tokio::task::JoinError>>),
    Tick,
}

/// Forwards a run's events to the inline view (and to the sink the options already had).
struct LiveSink {
    tx: UnboundedSender<Event>,
    next: Option<Arc<dyn EventSink>>,
}

impl EventSink for LiveSink {
    fn emit(&self, event: Event) {
        if let Some(next) = &self.next {
            next.emit(event.clone());
        }
        let _ = self.tx.send(event);
    }
}

/// Open the run monitor for the current run (on `w`), then come back to the inline view. Python
/// worker output is held while the monitor owns the terminal and printed when it returns.
pub(super) fn watch(
    rt: &mut Runtime<'_>,
    instance: &Instance,
    run_id: &str,
    events: &broadcast::Receiver<Event>,
    region: &Region,
    keys: &mut Keys,
) {
    region.hide();
    keys.pause();
    crate::format::live::hold_worker_output();
    let feed = crate::tui::RunFeed::Live(events.resubscribe());
    let exit = crate::tui::run_monitor(rt, instance, run_id, feed);
    crate::format::live::release_worker_output(crate::format::progress::emit_worker_output);
    keys.resume();
    // Quit leaves the run going, like detaching: the inline view comes back either way.
    if let Err(error) = exit {
        super::warn(rt, &error.message);
    }
}

/// Run `opts` on `engine` with the inline live view; returns when the run ends.
pub(super) fn run(
    rt: &mut Runtime<'_>,
    session: &Session,
    engine: meerschaum_engine::Engine,
    mut opts: meerschaum_engine::RunOptions,
    instance: &Instance,
    args: &ArgsMap,
) -> meerschaum_engine::RunResult {
    // The monitor's feed is a fresh receiver of this one (never read here).
    let monitor_events = engine.subscribe();
    let (tx, mut events) = unbounded_channel();
    opts.on_event = Some(Arc::new(LiveSink {
        tx,
        next: opts.on_event.take(),
    }));
    let mut model = LiveRun::new(style(rt), true);
    let watchable = crate::tui::available(rt, args);
    let hint = watchable.then_some(WATCH_HINT);
    let mut task = session
        .handle()
        .spawn(async move { engine.run(opts).await });
    let abort = task.abort_handle();
    // Any unwind out of this function (a print checks for Ctrl-C) cancels the run.
    let outer = AbortOnDrop(Some(abort.clone()));
    let region = Region::start(RegionSink::Stdout);
    let mut keys = Keys::start(watchable);
    let mut events_open = true;
    let mut tick = 0usize;
    let mut next_draw = Instant::now();
    let draw = |model: &LiveRun, region: &Region, tick: usize| {
        let (cols, rows) = stdout_size();
        region.show(model.lines(Instant::now(), tick, cols, rows, hint));
    };
    let result = loop {
        let wait = next_draw.saturating_duration_since(Instant::now());
        let step = session.block_on(async {
            let guard = AbortOnDrop(Some(abort.clone()));
            let step = tokio::select! {
                biased;
                event = events.recv(), if events_open => Step::Event(event),
                result = &mut task => Step::Done(Box::new(result)),
                _ = tokio::time::sleep(wait) => Step::Tick,
            };
            guard.disarm();
            step
        });
        match step {
            Step::Event(Some(event)) => {
                super::print_hook_results(rt, session);
                model.set_width(stdout_size().0);
                let lines = model.apply(&event, Instant::now());
                print_lines(rt, &region, &model, tick, hint, lines);
            }
            Step::Event(None) => events_open = false,
            Step::Done(result) => break result,
            Step::Tick => {}
        }
        if keys.pressed().iter().any(|key| matches!(key, 'w' | 'W'))
            && let Some(run_id) = model.run_id().map(str::to_owned)
        {
            watch(rt, instance, &run_id, &monitor_events, &region, &mut keys);
            next_draw = Instant::now();
        }
        if Instant::now() >= next_draw {
            draw(&model, &region, tick);
            tick = tick.wrapping_add(1);
            next_draw = Instant::now() + REDRAW;
        }
    };
    outer.disarm();
    let result = match *result {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => meerschaum_engine::RunResult {
            success: false,
            message: format!("The run was cancelled: {error}"),
            plan: None,
            record: None,
            outcomes: Vec::new(),
            critical_path: Vec::new(),
            warnings: Vec::new(),
            duration: std::time::Duration::ZERO,
        },
    };
    super::print_hook_results(rt, session);
    while let Ok(event) = events.try_recv() {
        let lines = model.apply(&event, Instant::now());
        print_lines(rt, &region, &model, tick, hint, lines);
    }
    let lines = model.resync(&result.outcomes, result.record.as_ref());
    region.hide();
    drop(keys);
    drop(region);
    for line in lines {
        rt.print(&format!("{line}\n"));
    }
    result
}

/// Print permanent lines above the region: the region is erased by the print and redrawn after
/// it with the model's current rows.
pub(super) fn print_lines(
    rt: &mut Runtime<'_>,
    region: &Region,
    model: &LiveRun,
    tick: usize,
    hint: Option<&str>,
    lines: Vec<String>,
) {
    if lines.is_empty() {
        return;
    }
    let (cols, rows) = stdout_size();
    region.set(model.lines(Instant::now(), tick, cols, rows, hint));
    let mut text = String::new();
    for line in lines {
        text.push_str(&line);
        text.push('\n');
    }
    rt.print(&text);
}

/// The live rows of `sync pipes --loop / --listen` on a terminal: the same running rows as a
/// graph run, while the laps keep their v4 per-pipe lines (printed by the caller).
pub(super) struct LoopView {
    /// The events, drained from the broadcast receiver on a task (`Err`: that many were lost).
    events: UnboundedReceiver<Result<Event, u64>>,
    /// The monitor's feed is a fresh receiver of this one (never read here).
    monitor_events: broadcast::Receiver<Event>,
    model: LiveRun,
    hint: Option<&'static str>,
    tick: usize,
    next_draw: Instant,
    keys: Keys,
    // Dropped last: erases the rows.
    region: Region,
}

impl LoopView {
    /// Start on `events` (subscribed before the loop was spawned), when [`possible`].
    pub(super) fn start(
        rt: &mut Runtime<'_>,
        args: &ArgsMap,
        session: &Session,
        mut events: broadcast::Receiver<Event>,
    ) -> LoopView {
        use broadcast::error::RecvError;
        let watchable = crate::tui::available(rt, args);
        let monitor_events = events.resubscribe();
        let (tx, rx) = unbounded_channel();
        // Drained continuously (also while the monitor is open): the broadcast buffer never fills.
        session.spawn(async move {
            loop {
                let item = match events.recv().await {
                    Ok(event) => Ok(event),
                    Err(RecvError::Lagged(count)) => Err(count),
                    Err(RecvError::Closed) => break,
                };
                if tx.send(item).is_err() {
                    break;
                }
            }
        });
        LoopView {
            events: rx,
            monitor_events,
            model: LiveRun::new(style(rt), false),
            hint: watchable.then_some(WATCH_HINT),
            tick: 0,
            next_draw: Instant::now(),
            keys: Keys::start(watchable),
            region: Region::start(RegionSink::Stdout),
        }
    }

    /// Fold the pending events in, open the monitor on `w`, and redraw at most every
    /// [`REDRAW`]. Never blocks.
    pub(super) fn poll(&mut self, rt: &mut Runtime<'_>, instance: &Instance) {
        while let Ok(item) = self.events.try_recv() {
            match item {
                Ok(event) => {
                    self.model.apply(&event, Instant::now());
                }
                Err(count) => {
                    self.model.missed(count);
                }
            }
        }
        if self
            .keys
            .pressed()
            .iter()
            .any(|key| matches!(key, 'w' | 'W'))
            && let Some(run_id) = self.model.run_id().map(str::to_owned)
        {
            watch(
                rt,
                instance,
                &run_id,
                &self.monitor_events,
                &self.region,
                &mut self.keys,
            );
            self.next_draw = Instant::now();
        }
        if Instant::now() >= self.next_draw {
            let (cols, rows) = stdout_size();
            self.region.show(
                self.model
                    .lines(Instant::now(), self.tick, cols, rows, self.hint),
            );
            self.tick = self.tick.wrapping_add(1);
            self.next_draw = Instant::now() + REDRAW;
        }
    }
}
