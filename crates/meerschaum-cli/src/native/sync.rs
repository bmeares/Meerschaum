//! `sync pipes`: engine execution with v4 action output and lap timing.

#[path = "engine.rs"]
pub mod engine;
#[path = "sync_live.rs"]
mod live;

use std::time::Instant;

use meerschaum_connector::{DateBound, SyncOptions, pipe_ops};
use meerschaum_core::SuccessTuple;
use meerschaum_core::value::Map;
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::{PipeInput, pipe_enforce_dtypes};

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::format::{OptionsArgs, TupleArgs};
use crate::value::ArgsMap;

use super::pipes::{SelectedPipe, hydrate};
use super::show::bound;
use super::{Instance, Session, flag, float, info, plural, print_tuple, session, warn};

/// `humanfriendly.format_timespan(seconds)` (`interval_str(timedelta)`).
pub fn interval_str(seconds: f64) -> String {
    let neg = seconds < 0.0;
    let s = format_timespan(seconds.abs());
    if neg { format!("{s} ago") } else { s }
}

/// `humanfriendly.format_timespan(seconds)` (`detailed=False`, `max_units=3`).
pub fn format_timespan(seconds: f64) -> String {
    fn round_number(x: f64) -> String {
        let s = format!("{x:.2}");
        let s = s.trim_end_matches('0');
        s.trim_end_matches('.').to_string()
    }
    fn pluralize(count: &str, singular: &str) -> String {
        let n: f64 = count.parse().unwrap_or(0.0);
        if n.floor() == 1.0 {
            format!("{count} {singular}")
        } else {
            format!("{count} {singular}s")
        }
    }
    if seconds < 60.0 {
        return pluralize(&round_number(seconds), "second");
    }
    let units: [(&str, f64); 6] = [
        ("year", 31_536_000.0),
        ("week", 604_800.0),
        ("day", 86_400.0),
        ("hour", 3_600.0),
        ("minute", 60.0),
        ("second", 1.0),
    ];
    let mut remaining = seconds;
    let mut parts = Vec::new();
    for (i, (name, divider)) in units.iter().enumerate() {
        let count = remaining / divider;
        remaining %= divider;
        let text = if i + 1 < units.len() {
            (count.trunc() as i64).to_string()
        } else {
            round_number(count)
        };
        if text != "0" {
            parts.push(pluralize(&text, name));
        }
    }
    if parts.len() == 1 {
        return parts.remove(0);
    }
    parts.truncate(3);
    let last = parts.pop().unwrap_or_default();
    if parts.is_empty() {
        last
    } else {
        format!("{} and {last}", parts.join(", "))
    }
}

/// The keyword arguments `_wrap_pipe` passes to `Pipe.sync()`.
#[derive(Debug, Clone, Default)]
pub struct SyncArgs {
    /// `begin` (`None` is Python's `''`: the sync time minus the backtrack interval).
    pub begin: Option<DateBound>,
    /// `end`.
    pub end: Option<DateBound>,
    /// `params`.
    pub params: Option<Map>,
    /// `check_existing` (`--skip-check-existing` turns it off).
    pub check_existing: bool,
    /// `chunksize` (`-1` → `system:connectors:sql:chunksize`).
    pub chunksize: Option<i64>,
    /// Additional plugin arguments passed through to Python.
    pub extra: Map,
}

impl SyncArgs {
    /// From the parsed action arguments.
    pub fn from_args(args: &ArgsMap) -> SyncArgs {
        let mut extra: Map = args
            .iter()
            .map(|(k, v)| (k.clone(), super::py_to_value(v)))
            .collect();
        extra.insert(
            "blocking".into(),
            meerschaum_core::Value::Bool(!flag(args, "unblock")),
        );
        extra.insert(
            "debug".into(),
            meerschaum_core::Value::Bool(flag(args, "debug")),
        );
        extra
            .entry("bounded".to_owned())
            .or_insert(meerschaum_core::Value::Null);
        extra
            .entry("chunk_interval".to_owned())
            .or_insert(meerschaum_core::Value::Null);
        SyncArgs {
            begin: bound(args, "begin"),
            end: bound(args, "end"),
            params: super::params(args),
            check_existing: !flag(args, "skip_check_existing"),
            chunksize: super::int(args, "chunksize").or(Some(-1)),
            extra,
        }
    }
}

/// `_sync(p, df)` with a frame: enforce dtypes and `sync_pipe` (the SQL instance also adds the
/// autotime column and persists new special columns).
async fn sync_frame(
    inst: &Instance,
    sp: &mut SelectedPipe,
    df: Frame,
    args: &SyncArgs,
) -> SuccessTuple {
    let opts = SyncOptions {
        check_existing: args.check_existing,
        chunksize: args.chunksize,
        begin: args.begin.clone(),
        end: args.end.clone(),
        extra: Map::new(),
    };
    match inst {
        Instance::Sql(sql) => {
            sql.sync_mut(&mut sp.pipe, PipeInput::Frame(df), &opts)
                .await
        }
        Instance::Other(conn) => {
            let ctx = match pipe_ops::dtypes_context(conn.as_ref(), &sp.pipe, sp.pipe.parameters())
                .await
            {
                Ok(c) => c,
                Err(e) => return SuccessTuple::fail(e.to_string()),
            };
            let df = match pipe_enforce_dtypes(PipeInput::Frame(df), &ctx) {
                Ok(df) => df,
                Err(e) => return SuccessTuple::fail(e.to_string()),
            };
            conn.sync_pipe(&sp.pipe, df, &opts).await
        }
    }
}

/// `pipe.sync(df)` with a frame (blocking).
pub fn sync_frame_public(
    session: &Session,
    inst: &Instance,
    sp: &mut SelectedPipe,
    df: Frame,
    args: &SyncArgs,
) -> SuccessTuple {
    session.block_on(sync_frame(inst, sp, df, args))
}

/// Run a bare `pipe.sync()` through the same native engine path as worker callbacks.
pub fn sync_pipe(
    rt: &mut Runtime<'_>,
    session: &Session,
    inst: &Instance,
    sp: &mut SelectedPipe,
    args: &SyncArgs,
) -> SuccessTuple {
    hydrate(rt, inst, sp);
    let engine = match engine::build(
        rt,
        session,
        inst,
        std::slice::from_ref(sp),
        true,
        false,
        None,
    ) {
        Ok(e) => e,
        Err(e) => return SuccessTuple::fail(e.message),
    };
    let args = meerschaum_engine::SyncArgs {
        begin: args
            .begin
            .clone()
            .map(meerschaum_engine::SyncBegin::At)
            .unwrap_or_default(),
        end: args.end.clone(),
        params: args.params.clone(),
        check_existing: args.check_existing,
        chunksize: args.chunksize,
        extra: args.extra.clone(),
        force: args.extra.get("force").is_some_and(|v| v.truthy()),
        ..Default::default()
    };
    let result = session.block_on(engine.sync_pipe(&sp.pipe, None, &args));
    print_hook_results(rt, session);
    print_chunk_progress(rt, &result.message);
    SuccessTuple {
        success: result.success,
        message: result.message,
    }
}

fn print_hook_results(rt: &mut Runtime<'_>, session: &Session) {
    for result in session.take_hook_results() {
        print_tuple(rt, &result, TupleArgs::default());
    }
}

/// `format_pipe_success_tuple(pipe, success_tuple, nopretty)`.
pub fn format_pipe_success_tuple(
    rt: &mut Runtime<'_>,
    pipe: &meerschaum_core::Pipe,
    tup: &SuccessTuple,
    nopretty: bool,
) -> String {
    if nopretty {
        let mut m = Map::new();
        m.insert(
            "pipe".into(),
            meerschaum_core::Value::Map(super::pipes::pipe_meta(pipe)),
        );
        m.insert("success".into(), meerschaum_core::Value::Bool(tup.success));
        m.insert(
            "message".into(),
            meerschaum_core::Value::Str(tup.message.clone()),
        );
        return super::show::dumps(&meerschaum_core::Value::Map(m));
    }
    rt.with_formatter(|f| {
        let unicode = f.settings.unicode;
        let underline = if unicode { "\u{2015}" } else { "-" };
        let prefix = if unicode {
            let key = if tup.success {
                "success_calm"
            } else {
                "failure_calm"
            };
            format!("{} ", f.formatting_str(&["emoji", key]))
        } else {
            String::new()
        };
        let pipe_str = format!("{prefix}{pipe}");
        let n = pipe_str.chars().count() + usize::from(unicode);
        format!("{pipe_str}\n{}\n{}\n\n", underline.repeat(n), tup.message)
    })
}

/// v4's per-pipe lap block (`Finished syncing {pipe}:` / `Error while syncing {pipe}:` and the
/// message); `suffix` goes after the pipe (the duration and count of a run of several pipes).
fn print_pipe_block(rt: &mut Runtime<'_>, pipe: &str, tup: &SuccessTuple, suffix: &str) {
    let header = if tup.success {
        format!("Finished syncing {pipe}{suffix}:\n")
    } else {
        format!("Error while syncing {pipe}{suffix}:\n")
    };
    print_tuple(
        rt,
        &SuccessTuple {
            success: tup.success,
            message: format!("{header}{}\n", tup.message),
        },
        TupleArgs {
            calm: true,
            ..TupleArgs::default()
        },
    );
}

/// Reports a run of `sync pipes` pipe by pipe: each pipe's block prints the moment its node
/// ends (v4 printed each one as its worker finished) and, when the run has several pipes, says
/// how long the pipe took and how far the run is (`in 1.2 seconds (3/10)`). A run of one pipe
/// keeps v4's block byte for byte. [`PipeReport::summary`] adds the totals to the lap message.
pub(super) struct PipeReport {
    defaults: meerschaum_core::pipe::PipeDefaults,
    total: usize,
    done: usize,
    started: std::collections::HashMap<meerschaum_engine::PipeRef, Instant>,
    /// How long each ended pipe took and its place in the run (`3` of `3/10`).
    ended: std::collections::HashMap<meerschaum_engine::PipeRef, (std::time::Duration, usize)>,
    printed: std::collections::HashSet<meerschaum_engine::PipeRef>,
}

impl PipeReport {
    pub(super) fn new(defaults: meerschaum_core::pipe::PipeDefaults) -> PipeReport {
        PipeReport {
            defaults,
            total: 0,
            done: 0,
            started: Default::default(),
            ended: Default::default(),
            printed: Default::default(),
        }
    }

    /// Take in one event; `true` when it ended a node and its block was printed (`print`: the
    /// node lines of a terminal without the live region say it in one line instead).
    fn observe(
        &mut self,
        rt: &mut Runtime<'_>,
        event: &meerschaum_engine::Event,
        print: bool,
    ) -> bool {
        use meerschaum_engine::EventKind;
        let ended = match (&event.kind, &event.pipe_ref) {
            (EventKind::RunStarted { nodes, .. }, _) => {
                self.total = *nodes;
                return false;
            }
            (EventKind::NodeStarted { .. }, Some(r)) => {
                self.started.entry(r.clone()).or_insert_with(Instant::now);
                return false;
            }
            (EventKind::NodeFinished { result }, Some(r)) => (
                r,
                SuccessTuple {
                    success: result.success,
                    message: result.message.clone(),
                },
                result.duration,
            ),
            (
                EventKind::NodeFailed {
                    error,
                    will_retry: false,
                    ..
                },
                Some(r),
            ) => (r, SuccessTuple::fail(error.clone()), None),
            _ => return false,
        };
        let (pipe_ref, tup, duration) = ended;
        let duration = duration
            .or_else(|| self.started.get(pipe_ref).map(Instant::elapsed))
            .unwrap_or_default();
        self.done += 1;
        self.ended.insert(pipe_ref.clone(), (duration, self.done));
        if !print {
            return false;
        }
        let parts = pipe_ref.parts();
        let Ok(pipe) = super::pipes::build_pipe(
            parts.connector_keys,
            parts.metric_key,
            parts.location_key,
            parts.instance_keys,
            None,
            &self.defaults,
        ) else {
            return false;
        };
        print_chunk_progress(rt, &tup.message);
        let suffix = self.suffix(pipe_ref);
        print_pipe_block(rt, &pipe.to_string(), &tup, &suffix);
        self.printed.insert(pipe_ref.clone());
        true
    }

    /// ` (1.2 seconds)`: how long an ended pipe took (a node line's tail).
    fn took(&self, pipe_ref: &meerschaum_engine::PipeRef) -> Option<String> {
        let (duration, _) = self.ended.get(pipe_ref)?;
        Some(format!(
            " ({})",
            interval_str(round_seconds(duration.as_secs_f64()))
        ))
    }

    /// Whether the run already printed this pipe's block.
    fn printed(&self, pipe_ref: &meerschaum_engine::PipeRef) -> bool {
        self.printed.contains(pipe_ref)
    }

    /// ` in 1.2 seconds (3/10)` for a run of several pipes (empty for one pipe: v4's block).
    fn suffix(&self, pipe_ref: &meerschaum_engine::PipeRef) -> String {
        if self.total <= 1 {
            return String::new();
        }
        let Some((duration, position)) = self.ended.get(pipe_ref) else {
            return String::new();
        };
        format!(
            " in {} ({position}/{})",
            interval_str(round_seconds(duration.as_secs_f64())),
            self.total
        )
    }

    /// The lines a lap of several pipes adds under `It took ... to sync N pipes`: the rows the
    /// run changed, the slowest pipes and every failure (empty for one pipe: its block says it).
    pub(super) fn summary(&self, outcomes: &[meerschaum_engine::NodeOutcome]) -> String {
        if outcomes.len() <= 1 {
            return String::new();
        }
        let mut text = String::new();
        let (inserted, updated, upserted) = outcomes.iter().fold((0, 0, 0), |acc, o| {
            (
                acc.0 + o.result.inserted,
                acc.1 + o.result.updated,
                acc.2 + o.result.upserted,
            )
        });
        text.push_str(&format!(
            "\n    Rows: {inserted} inserted, {updated} updated{}.",
            if upserted > 0 {
                format!(", {upserted} upserted")
            } else {
                String::new()
            }
        ));
        let mut timed: Vec<(&meerschaum_engine::NodeOutcome, std::time::Duration)> = outcomes
            .iter()
            .filter_map(|o| {
                self.ended
                    .get(&o.pipe_ref)
                    .map(|(d, _)| *d)
                    .or(o.result.duration)
                    .map(|d| (o, d))
            })
            .collect();
        if timed.len() > 2 {
            timed.sort_by_key(|t| std::cmp::Reverse(t.1));
            text.push_str("\n    Slowest:");
            for (o, d) in timed.iter().take(3) {
                text.push_str(&format!(
                    "\n      - {} ({})",
                    o.pipe,
                    interval_str(round_seconds(d.as_secs_f64()))
                ));
            }
        }
        let failed: Vec<&meerschaum_engine::NodeOutcome> =
            outcomes.iter().filter(|o| !o.result.success).collect();
        if !failed.is_empty() {
            text.push_str("\n    Failed:");
            for o in failed {
                let reason = o
                    .result
                    .message
                    .lines()
                    .map(str::trim)
                    .find(|l| !l.is_empty() && !l.contains(o.pipe.as_str()))
                    .or_else(|| {
                        o.result
                            .message
                            .lines()
                            .map(str::trim)
                            .find(|l| !l.is_empty())
                    })
                    .unwrap_or_default();
                text.push_str(&format!("\n      - {}: {reason}", o.pipe));
            }
        }
        text
    }
}

/// Seconds to two decimals (a duration for people, not a benchmark).
fn round_seconds(seconds: f64) -> f64 {
    (seconds * 100.0).round() / 100.0
}

/// `print_pipes_results(results, success_header, failure_header, nopretty)`.
pub fn print_pipes_results(
    rt: &mut Runtime<'_>,
    results: &[(meerschaum_core::Pipe, SuccessTuple)],
    success_header: &str,
    failure_header: &str,
    nopretty: bool,
) {
    let mut success_options = Vec::new();
    let mut failure_options = Vec::new();
    for (p, t) in results {
        let s = format_pipe_success_tuple(rt, p, t, nopretty);
        if t.success {
            success_options.push(s);
        } else {
            failure_options.push(s);
        }
    }
    for (options, header) in [
        (success_options, success_header),
        (failure_options, failure_header),
    ] {
        if options.is_empty() {
            continue;
        }
        let opts = OptionsArgs {
            header: Some(header.into()),
            nopretty,
            ..OptionsArgs::default()
        };
        let text = rt.with_formatter(|f| f.print_options(&options, &opts));
        rt.print(&text);
    }
}

/// Say which pipes a run did not sync because their definition changed and nobody approved the
/// rebuild: never a silent stop. Pretty mode warns; `--nopretty` prints one `rebuild_pending:`
/// line per pipe (and the way out) on stdout so a script or a job log sees it too.
pub(in crate::native) fn report_rebuild_pending(
    rt: &mut Runtime<'_>,
    pending: &[meerschaum_engine::PendingRebuild],
    nopretty: bool,
) {
    if pending.is_empty() {
        return;
    }
    let how = meerschaum_engine::PendingRebuild::HOW_TO_APPROVE;
    if nopretty {
        let mut text = String::new();
        for p in pending {
            text.push_str(&format!(
                "rebuild_pending: {}: {}\n",
                p.pipe_ref.label(),
                p.reason
            ));
        }
        text.push_str(&format!("rebuild_pending: {how}\n"));
        rt.print(&text);
        return;
    }
    let mut text = format!(
        "{} pipe{} {} NOT synced: the definition changed and the rebuild needs approval.",
        pending.len(),
        plural(pending.len()),
        if pending.len() == 1 { "was" } else { "were" }
    );
    for p in pending {
        text.push_str(&format!("\n  - {}: {}", p.pipe_ref.label(), p.reason));
    }
    text.push_str(&format!("\n{how}"));
    warn(rt, &text);
}

/// Which `Pipe` method a lap calls (`_wrap_pipe`'s `sync_method`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LapMethod {
    /// `pipe.sync`.
    Sync,
    /// `pipe.verify`.
    Verify,
    /// `pipe.deduplicate`.
    Deduplicate,
}

/// `_sync_pipes(**kw)`: laps of syncing (`--loop`, `--min-seconds`), shared by `verify pipes`
/// and `deduplicate pipes`.
pub fn sync_pipes_with(args: &ArgsMap, rt: &mut Runtime<'_>, method: LapMethod) -> ActionResult {
    if method == LapMethod::Sync && (flag(args, "listen") || flag(args, "loop")) {
        return listen(args, rt);
    }
    let nopretty = flag(args, "nopretty");
    let do_loop = flag(args, "loop");
    let unblock = flag(args, "unblock");
    let min_seconds_arg = float(args, "min_seconds");
    let min_seconds = min_seconds_arg.unwrap_or(1.0);
    let min_seconds_str = match args.get("min_seconds") {
        Some(v) if !matches!(v, crate::value::PyValue::None) => v.py_str(),
        _ => "1".to_string(),
    };
    let session = session(rt);
    let sync_args = SyncArgs::from_args(args);
    if method == LapMethod::Sync
        && (flag(args, "parents") || flag(args, "children") || args.contains_key("resume"))
    {
        prepare_live(rt, args);
    }
    let stop = crate::native::jobs::stop_requested_flag();
    if do_loop || flag(args, "listen") {
        crate::native::jobs::install_stop_handler(&session);
    }
    let mut msg;
    let mut success_count;
    loop {
        let lap_begin = Instant::now();
        let mut gantt_text: Option<String> = None;
        let mut pending: Vec<meerschaum_engine::PendingRebuild> = Vec::new();
        let mut report = PipeReport::new(super::pipes::defaults(rt));
        let mut summary_extra = String::new();
        let (inst, pipes) = super::pipes::selected_pipes(rt, args)?;
        let engine_results = if method == LapMethod::Sync {
            let traversal = flag(args, "parents") || flag(args, "children");
            // The graph build reads every registration: cover the silence before the first line.
            let progress =
                crate::format::progress::Progress::start(rt, args, "Building the sync graph");
            let engine = engine::build(rt, &session, &inst, &pipes, !traversal, false, None);
            drop(progress);
            let engine = engine?;
            let opts = run_options(args, &inst, &pipes, &sync_args);
            let graph_run = traversal || args.contains_key("resume");
            let result = if nopretty {
                session.block_on(engine.run(opts))
            } else if graph_run && rt.live && live::possible(rt, args) {
                let view = LiveView::Nodes {
                    instance: &inst,
                    args,
                };
                run_live(rt, &session, engine, opts, view)
            } else {
                // Each pipe's block prints as its node ends, not after the whole run.
                let spinner = rt.in_shell && !flag(args, "debug");
                let show_nodes = graph_run && rt.live;
                run_lines(
                    rt,
                    &session,
                    engine,
                    opts,
                    show_nodes,
                    spinner,
                    Some(&mut report),
                )
            };
            print_hook_results(rt, &session);
            for warning in &result.warnings {
                warn(rt, warning);
            }
            pending = result.rebuild_pending();
            if !result.success && result.outcomes.is_empty() {
                return Ok(SuccessTuple::fail(result.message));
            }
            summary_extra = report.summary(&result.outcomes);
            // §5.9: a graph run with several nodes ends with the Gantt chart. A flat run keeps
            // v4's output byte for byte (`show run <id>` still draws it).
            if graph_run
                && !nopretty
                && let Some(record) = &result.record
                && record.nodes.len() > 1
                && result.plan.as_ref().is_some_and(|p| !p.flat)
            {
                gantt_text = Some(super::engine_views::gantt(record, rt.columns));
            }
            Some(result.outcomes)
        } else {
            None
        };
        let pipes = if let Some(outcomes) = &engine_results {
            let defaults = super::pipes::defaults(rt);
            outcomes
                .iter()
                .map(|outcome| {
                    let parts = outcome.pipe_ref.parts();
                    super::pipes::build_pipe(
                        parts.connector_keys,
                        parts.metric_key,
                        parts.location_key,
                        parts.instance_keys,
                        None,
                        &defaults,
                    )
                    .map(|pipe| SelectedPipe {
                        pipe,
                        id: None,
                        hydrated: false,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            pipes
        };
        let mut results: Vec<(meerschaum_core::Pipe, SuccessTuple)> = Vec::new();
        for mut sp in pipes {
            let tup = match method {
                LapMethod::Sync => {
                    let result = &engine_results
                        .as_ref()
                        .expect("engine outcomes")
                        .iter()
                        .find(|o| o.pipe_ref == meerschaum_engine::PipeRef::from_pipe(&sp.pipe))
                        .expect("selected outcome")
                        .result;
                    SuccessTuple {
                        success: result.success,
                        message: result.message.clone(),
                    }
                }
                LapMethod::Verify => {
                    super::verify::verify_pipe(rt, &session, &inst, &mut sp, args, &sync_args)
                }
                LapMethod::Deduplicate => {
                    super::verify::deduplicate_pipe(rt, &session, &inst, &mut sp, args)
                }
            };
            let pipe_ref = meerschaum_engine::PipeRef::from_pipe(&sp.pipe);
            // A block the run already printed as its node ended is not printed again.
            if !report.printed(&pipe_ref) {
                if method == LapMethod::Sync {
                    print_chunk_progress(rt, &tup.message);
                }
                if !nopretty {
                    let suffix = report.suffix(&pipe_ref);
                    print_pipe_block(rt, &sp.pipe.to_string(), &tup, &suffix);
                }
            }
            results.push((sp.pipe.clone(), tup));
            if stop.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
        }
        let lap_secs = lap_begin.elapsed().as_secs_f64();
        rt.print("\n");
        success_count = results.iter().filter(|r| r.1.success).count();
        let failure_count = results.len() - success_count;
        if !results.is_empty() {
            let success_header = if unblock {
                "Successfully spawned threads for pipes:"
            } else {
                "Successfully synced pipes:"
            };
            print_pipes_results(
                rt,
                &results,
                success_header,
                "Failed to sync pipes:",
                nopretty,
            );
        }
        if let Some(text) = gantt_text {
            rt.print("\n");
            rt.print(&text);
        }
        report_rebuild_pending(rt, &pending, nopretty);
        let total = success_count + failure_count;
        msg = format!(
            "It took {} to sync {total} pipe{}\n    ({success_count} succeeded, {failure_count} failed).",
            interval_str(lap_secs),
            plural(total)
        );
        msg.push_str(&summary_extra);
        if !pending.is_empty() {
            msg.push_str(&format!(
                "\n    {} not synced: rebuild_pending.",
                pending.len()
            ));
        }
        // A job's startup slot (D23) is held until its first lap is done.
        meerschaum_jobs::startup::signal_first_lap_done();
        if min_seconds > 0.0 && do_loop {
            rt.print("\n");
            let s = if min_seconds.abs() != 1.0 { "s" } else { "" };
            info(rt, &format!("Sleeping for {min_seconds_str} second{s}."));
            let deadline = Instant::now() + std::time::Duration::from_secs_f64(min_seconds);
            while Instant::now() < deadline {
                if stop.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
        if !do_loop || stop.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
    }
    if flag(args, "listen") && !stop.load(std::sync::atomic::Ordering::SeqCst) {
        info(
            rt,
            "Listening for new data; subscribed connectors will keep syncing.\n    Stop with `stop jobs` (or CTRL-C).",
        );
        while !stop.load(std::sync::atomic::Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    }
    Ok(SuccessTuple {
        success: success_count > 0,
        message: msg,
    })
}

/// `sync pipes`.
pub fn sync_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let method = if flag(args, "verify") {
        LapMethod::Verify
    } else if flag(args, "deduplicate") {
        LapMethod::Deduplicate
    } else {
        LapMethod::Sync
    };
    sync_pipes_with(args, rt, method)
}

pub(in crate::native) fn depth(args: &ArgsMap, key: &str) -> Option<meerschaum_engine::Depth> {
    match args.get(key) {
        Some(crate::value::PyValue::Int(n)) if *n > 0 => Some(meerschaum_engine::Depth::N(
            u32::try_from(*n).unwrap_or(u32::MAX),
        )),
        Some(v) if v.is_truthy() => Some(meerschaum_engine::Depth::All),
        _ => None,
    }
}

pub(in crate::native) fn run_options(
    args: &ArgsMap,
    inst: &Instance,
    pipes: &[SelectedPipe],
    sync_args: &SyncArgs,
) -> meerschaum_engine::RunOptions {
    let selection = meerschaum_engine::Selection::refs(
        pipes
            .iter()
            .map(|p| meerschaum_engine::PipeRef::from_pipe(&p.pipe)),
    )
    .with_parents(depth(args, "parents"))
    .with_children(depth(args, "children"));
    let resume = args
        .get("resume")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let explicit_selection = [
        "connector_keys",
        "metric_keys",
        "location_keys",
        "tags",
        "parents",
        "children",
    ]
    .iter()
    .any(|key| args.get(*key).is_some_and(|v| v.is_truthy()));
    meerschaum_engine::RunOptions {
        selection: if resume.is_some() && !explicit_selection {
            None
        } else {
            Some(selection)
        },
        instances: vec![inst.keys()],
        begin: sync_args
            .begin
            .as_ref()
            .and_then(meerschaum_engine::sync::date_bound_to_bound),
        end: sync_args
            .end
            .as_ref()
            .and_then(meerschaum_engine::sync::date_bound_to_bound),
        workers: super::int(args, "workers")
            .and_then(|w| usize::try_from(w).ok())
            .filter(|w| *w > 0),
        yes: flag(args, "yes") || flag(args, "force"),
        resume,
        chunk_pipelining: flag(args, "chunk_pipelining").then_some(true),
        run_id: super::string(args, "run_id"),
        sync: meerschaum_engine::NodeSyncOptions {
            check_existing: sync_args.check_existing,
            retries: super::int(args, "retries")
                .and_then(|n| u32::try_from(n).ok())
                .unwrap_or(10),
            chunksize: sync_args.chunksize,
            params: sync_args.params.clone(),
            force: flag(args, "force"),
            min_seconds: float(args, "min_seconds").unwrap_or(1.0),
            extra: sync_args.extra.clone(),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// The `Sleeping for N seconds.` line of a lap: v4 formats the `--min-seconds` value as parsed
/// (`type=float`, so `--min-seconds 300` prints `300.0`; the default is the int `1`), which the
/// engine's `f64` cannot tell apart. A sleep shortened by a pipe's `schedule` (Rust-only) keeps the
/// engine's message.
fn sleeping_message(args: &ArgsMap, seconds: f64, message: String) -> String {
    match args.get("min_seconds") {
        Some(value)
            if !matches!(value, crate::value::PyValue::None)
                && float(args, "min_seconds") == Some(seconds) =>
        {
            // PARITY: `actions/sync.py:414` `f"Sleeping for {min_seconds} second" + ...`.
            let s = if seconds.abs() != 1.0 { "s" } else { "" };
            format!("Sleeping for {} second{s}.", value.py_str())
        }
        _ => message,
    }
}

/// What [`run_live`] shows while the run goes on.
#[derive(Clone, Copy)]
pub(super) enum LiveView<'a> {
    /// The nodes of a graph run: the inline live region on a terminal that takes it
    /// (`sync_live.rs`), else one line per node transition.
    Nodes {
        /// The run's instance (the run monitor reads it).
        instance: &'a Instance,
        /// The action's arguments.
        args: &'a ArgsMap,
    },
}

/// Call before the engine of a run shown with [`LiveView::Nodes`] is built (see
/// `sync_live::prepare`).
pub(super) fn prepare_live(rt: &mut Runtime<'_>, args: &ArgsMap) {
    live::prepare(rt, args);
}

/// Run a graph selection on the session's runtime while showing its progress (the terminal's
/// live view; the Gantt chart follows once the run ends).
pub(super) fn run_live(
    rt: &mut Runtime<'_>,
    session: &Session,
    engine: meerschaum_engine::Engine,
    opts: meerschaum_engine::RunOptions,
    view: LiveView<'_>,
) -> meerschaum_engine::RunResult {
    let LiveView::Nodes { instance, args } = view;
    if live::possible(rt, args) {
        return live::run(rt, session, engine, opts, instance, args);
    }
    let spinner = rt.in_shell;
    run_lines(rt, session, engine, opts, true, spinner, None)
}

/// Run a graph selection and report it pipe by pipe: the terminal's live view when there is
/// one, else each pipe's block as its node ends (job logs, pipes, the console's run output) and
/// the shell's spinner. Returns the result and the summary lines for its message
/// ([`PipeReport::summary`]). `--nopretty` prints nothing while the run goes on.
pub(super) fn run_reported(
    rt: &mut Runtime<'_>,
    session: &Session,
    engine: meerschaum_engine::Engine,
    opts: meerschaum_engine::RunOptions,
    view: LiveView<'_>,
    nopretty: bool,
) -> (meerschaum_engine::RunResult, String) {
    let LiveView::Nodes { instance, args } = view;
    let mut report = PipeReport::new(super::pipes::defaults(rt));
    let result = if nopretty {
        session.block_on(engine.run(opts))
    } else if rt.live && live::possible(rt, args) {
        live::run(rt, session, engine, opts, instance, args)
    } else {
        let spinner = rt.in_shell && !flag(args, "debug");
        let show_nodes = rt.live;
        run_lines(
            rt,
            session,
            engine,
            opts,
            show_nodes,
            spinner,
            Some(&mut report),
        )
    };
    print_hook_results(rt, session);
    let summary = report.summary(&result.outcomes);
    (result, summary)
}

/// [`run_live`] without the inline region: one line per node transition (`show_nodes`), the
/// shell's spinner (`spinner`), and each pipe's block as its node ends (`report`).
fn run_lines(
    rt: &mut Runtime<'_>,
    session: &Session,
    engine: meerschaum_engine::Engine,
    opts: meerschaum_engine::RunOptions,
    show_nodes: bool,
    spinner: bool,
    mut report: Option<&mut PipeReport>,
) -> meerschaum_engine::RunResult {
    use meerschaum_engine::EventKind;
    let progress = if spinner {
        crate::format::progress::Progress::start_flags(rt, false, false, "Syncing Pipes")
    } else {
        crate::format::progress::Progress::disabled()
    };
    progress.set_total(
        opts.selection
            .as_ref()
            .map_or(0, |selection| match &selection.targets {
                meerschaum_engine::Targets::Refs(refs) => refs.len(),
                _ => 0,
            }),
    );
    let counter = progress.counter();
    let mut events = engine.subscribe();
    let mut line = |rt: &mut Runtime<'_>, event: meerschaum_engine::Event| {
        if let EventKind::RunStarted { nodes, .. } = &event.kind {
            progress.set_total(*nodes);
        }
        let reported = match report.as_deref_mut() {
            Some(report) => report.observe(rt, &event, !show_nodes),
            None => false,
        };
        let Some(node) = event.pipe_ref.as_ref().map(|r| r.label()) else {
            return;
        };
        if matches!(
            &event.kind,
            EventKind::NodeFinished { .. }
                | EventKind::NodeFailed {
                    will_retry: false,
                    ..
                }
                | EventKind::NodeSkipped { .. }
        ) {
            counter.inc();
        }
        if !show_nodes {
            return;
        }
        let took = event
            .pipe_ref
            .as_ref()
            .and_then(|r| report.as_deref().and_then(|report| report.took(r)))
            .unwrap_or_default();
        let text = match &event.kind {
            EventKind::NodeStarted { action, .. } => format!("  \u{25b6} {node} ({action})"),
            EventKind::NodeFinished { .. } | EventKind::NodeFailed { .. } if reported => return,
            EventKind::NodeFinished { result } => format!(
                "  \u{2714} {node} +{} ins ~{} upd +{} ups{took}",
                result.inserted, result.updated, result.upserted
            ),
            EventKind::NodeFailed {
                error,
                will_retry: false,
                ..
            } => format!(
                "  \u{2718} {node} {}",
                error.lines().next().unwrap_or_default()
            ),
            EventKind::NodeSkipped { reason } => format!("  \u{25cb} {node} {reason}"),
            _ => return,
        };
        rt.print(&format!("{text}\n"));
    };
    // Keep the run inside the cancellable future. Detaching it while waiting on a
    // blocking receiver bypasses Session's Ctrl-C cleanup and can leave a sync running.
    session.block_on(async {
        let mut run = Box::pin(engine.run(opts));
        let mut events_open = true;
        loop {
            tokio::select! {
                result = &mut run => {
                    while let Ok(event) = events.try_recv() {
                        line(rt, event);
                    }
                    print_hook_results(rt, session);
                    return result;
                }
                event = events.recv(), if events_open => {
                    match event {
                        Ok(event) => {
                            // A pipe's block first, then what its hooks returned.
                            line(rt, event);
                            print_hook_results(rt, session);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => events_open = false,
                    }
                }
            }
        }
    })
}

/// Keep the engine's subscriptions alive and render its lap events on the CLI thread.
fn listen(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    use meerschaum_engine::{LapEvent, LoopOptions, StopToken};
    use std::sync::{Arc, mpsc};
    let session = session(rt);
    super::jobs::install_stop_handler(&session);
    let signal = super::jobs::stop_requested_flag();
    let stop = StopToken::default();
    live::prepare(rt, args);
    let (inst, pipes) = super::pipes::selected_pipes(rt, args)?;
    let sync_args = SyncArgs::from_args(args);
    let mut stream_kwargs = sync_args.extra.clone();
    stream_kwargs.insert(
        "force".into(),
        meerschaum_core::Value::Bool(flag(args, "force")),
    );
    stream_kwargs.insert(
        "min_seconds".into(),
        meerschaum_core::Value::Float(float(args, "min_seconds").unwrap_or(1.0)),
    );
    stream_kwargs
        .entry("workers".to_owned())
        .or_insert(meerschaum_core::Value::Null);
    stream_kwargs.insert(
        "check_existing".into(),
        meerschaum_core::Value::Bool(sync_args.check_existing),
    );
    stream_kwargs.insert(
        "chunksize".into(),
        sync_args
            .chunksize
            .map(meerschaum_core::Value::Int)
            .unwrap_or(meerschaum_core::Value::Null),
    );
    if sync_args.begin.is_none() {
        stream_kwargs.insert("begin".into(), meerschaum_core::Value::Str(String::new()));
    }
    let engine = engine::build(
        rt,
        &session,
        &inst,
        &pipes,
        depth(args, "parents").is_none() && depth(args, "children").is_none(),
        flag(args, "listen"),
        Some(stream_kwargs),
    )?;
    let opts = run_options(args, &inst, &pipes, &sync_args);
    let defaults = super::pipes::defaults(rt);
    let nopretty = flag(args, "nopretty");
    let loop_opts = LoopOptions {
        run_loop: flag(args, "loop"),
        listen: flag(args, "listen"),
        min_seconds: float(args, "min_seconds").unwrap_or(1.0),
        stop: stop.clone(),
        ..Default::default()
    };
    // The running rows under the laps' lines, from the engine's events (a terminal only).
    let mut rows = live::possible(rt, args)
        .then(|| engine.subscribe())
        .map(|events| live::LoopView::start(rt, args, &session, events));
    let (sender, receiver) = mpsc::channel();
    session.spawn(async move {
        engine
            .run_loop(
                opts,
                loop_opts,
                Arc::new(move |event| {
                    let _ = sender.send(event);
                }),
            )
            .await;
    });
    let mut results = Vec::new();
    let mut result = SuccessTuple::success();
    loop {
        if signal.load(std::sync::atomic::Ordering::SeqCst) {
            stop.stop();
        }
        match receiver.recv_timeout(std::time::Duration::from_millis(50)) {
            Ok(LapEvent::PipeFinished(outcome)) => {
                print_hook_results(rt, &session);
                print_chunk_progress(rt, &outcome.result.message);
                if !nopretty {
                    print_tuple(
                        rt,
                        &SuccessTuple {
                            success: outcome.result.success,
                            message: outcome.v4_message(),
                        },
                        TupleArgs {
                            calm: true,
                            ..Default::default()
                        },
                    );
                }
                let parts = outcome.pipe_ref.parts();
                let pipe = super::pipes::build_pipe(
                    parts.connector_keys,
                    parts.metric_key,
                    parts.location_key,
                    parts.instance_keys,
                    None,
                    &defaults,
                )?;
                results.push((
                    pipe,
                    SuccessTuple {
                        success: outcome.result.success,
                        message: outcome.result.message,
                    },
                ));
            }
            Ok(LapEvent::LapFinished(summary)) => {
                // A job's startup slot (D23) is held until its first lap is done.
                meerschaum_jobs::startup::signal_first_lap_done();
                rt.print("\n");
                print_pipes_results(
                    rt,
                    &results,
                    "Successfully synced pipes:",
                    "Failed to sync pipes:",
                    nopretty,
                );
                results.clear();
                // Every lap says it again: an unattended `--loop` job must not stop quietly.
                report_rebuild_pending(rt, &summary.rebuild_pending, nopretty);
                result = SuccessTuple {
                    success: summary.v4_success(),
                    message: summary.v4_message(&interval_str(summary.duration.as_secs_f64())),
                };
                if !flag(args, "loop") {
                    info(
                        rt,
                        "Listening for new data; subscribed connectors will keep syncing.\n    Stop with `stop jobs` (or CTRL-C).",
                    );
                }
            }
            Ok(LapEvent::Sleeping { seconds, message }) => {
                rt.print("\n");
                info(rt, &sleeping_message(args, seconds, message));
            }
            Ok(LapEvent::Cooldown { message, .. }) => warn(rt, &message),
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        print_hook_results(rt, &session);
        if let Some(rows) = rows.as_mut() {
            rows.poll(rt, &inst);
        }
    }
    drop(rows);
    Ok(result)
}

/// Resolve native pipe callbacks only when plugin code actually invokes a pipe method.
pub(crate) fn callbacks(
    rt: &mut Runtime<'_>,
    _args: &ArgsMap,
) -> Result<
    std::sync::Arc<dyn meerschaum_worker::callback::CallbackHandler>,
    crate::builtin::ActionError,
> {
    use meerschaum_core::config::{Config, ConfigOptions};
    let session = session(rt);
    let original = rt.config();
    let paths = original.paths();
    let mut options = ConfigOptions::from_process();
    let plugins = std::env::join_paths(&paths.plugins_dirs)
        .map_err(|e| crate::builtin::ActionError::new("ConfigError", e.to_string()))?;
    for (key, value) in [
        (
            "MRSM_ROOT_DIR",
            paths.root_dir.to_string_lossy().into_owned(),
        ),
        (
            "MRSM_CONFIG_DIR",
            paths.config_dir.to_string_lossy().into_owned(),
        ),
        ("MRSM_PLUGINS_DIR", plugins.to_string_lossy().into_owned()),
    ] {
        options.env.retain(|(k, _)| k != key);
        options.env.push((key.into(), value));
    }
    options.allow_writes = false;
    let config = Config::new(options)
        .map_err(|e| crate::builtin::ActionError::new("ConfigError", e.to_string()))?;
    config.set_config(original.snapshot());
    Ok(std::sync::Arc::new(ActionCallbacks {
        session,
        config: std::sync::Arc::new(config),
    }))
}

struct ActionCallbacks {
    session: Session,
    config: std::sync::Arc<meerschaum_core::config::Config>,
}
#[meerschaum_connector::async_trait]
impl meerschaum_worker::callback::CallbackHandler for ActionCallbacks {
    async fn handle(&self, request: meerschaum_worker::callback::CallbackRequest) -> SuccessTuple {
        let Some(pipe) = request.pipe.clone() else {
            return SuccessTuple::fail("Missing pipe in callback.");
        };
        let config = self.config.clone();
        let session = self.session.clone();
        let registry = tokio::task::spawn_blocking(move || {
            meerschaum_worker::pool::callback_scope_blocking(|| {
                let inst = session.instance(pipe.instance_keys(), &config)?;
                let selected = SelectedPipe {
                    pipe,
                    id: None,
                    hydrated: true,
                };
                engine::registry(&config, &session, &inst, &[selected], true)
                    .map(|bundle| bundle.python)
            })
        })
        .await;
        let registry = match registry {
            Ok(Ok(registry)) => registry,
            Ok(Err(error)) => return SuccessTuple::fail(error.message),
            Err(error) => return SuccessTuple::fail(error.to_string()),
        };
        let result =
            meerschaum_worker::pool::callback_scope(registry.callbacks().handle(request)).await;
        drop(registry);
        result
    }
}

/// PARITY: v4 prints every chunk after the first in addition to the aggregate result.
fn print_chunk_progress(rt: &mut Runtime<'_>, message: &str) {
    if !message.starts_with("Synced ") || !message.contains(" succeeded, ") {
        return;
    }
    for chunk in message.split("\n\n") {
        if chunk.starts_with("Synced a chunk (") || chunk.starts_with("Failed to sync a chunk (") {
            print_tuple(
                rt,
                &SuccessTuple {
                    success: chunk.starts_with("Synced"),
                    message: chunk.to_owned(),
                },
                TupleArgs {
                    calm: true,
                    ..Default::default()
                },
            );
        }
    }
}

/// Resolve a saved pipeline from instance metadata and execute its exact membership.
/// Jobs call this action on every scheduled lap, so membership edits take effect immediately.
pub fn sync_pipeline(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    super::pipelines::run("sync", args, rt)
}

#[cfg(test)]
mod tests {
    use super::sleeping_message;
    use crate::parse::parse_arguments;
    use crate::types::DateContext;

    fn args(sysargs: &[&str]) -> crate::value::ArgsMap {
        let sysargs: Vec<String> = sysargs.iter().map(|s| s.to_string()).collect();
        parse_arguments(&sysargs, &DateContext::system()).unwrap()
    }

    #[test]
    fn a_lap_sleeps_for_the_min_seconds_as_v4_formats_them() {
        // PARITY: argparse `type=float`: `--min-seconds 300` prints `300.0`.
        let given = args(&["sync", "pipes", "--loop", "--min-seconds", "300"]);
        assert_eq!(
            sleeping_message(&given, 300.0, "Sleeping for 300 seconds.".into()),
            "Sleeping for 300.0 seconds."
        );
        let one = args(&["sync", "pipes", "--loop", "--min-seconds", "1"]);
        assert_eq!(
            sleeping_message(&one, 1.0, "Sleeping for 1 second.".into()),
            "Sleeping for 1.0 second."
        );
        // The default is v4's `min_seconds: int = 1`: the engine's message stands.
        let default = args(&["sync", "pipes", "--loop"]);
        assert_eq!(
            sleeping_message(&default, 1.0, "Sleeping for 1 second.".into()),
            "Sleeping for 1 second."
        );
        // A sleep a pipe's `schedule` shortened keeps the engine's message.
        assert_eq!(
            sleeping_message(&given, 12.5, "Sleeping for 12.5 seconds.".into()),
            "Sleeping for 12.5 seconds."
        );
    }
}
