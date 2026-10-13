//! `sync pipes`: engine execution with v4 action output and lap timing.

#[path = "engine.rs"]
mod engine;

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
    print_chunk_progress(rt, &result.message);
    SuccessTuple {
        success: result.success,
        message: result.message,
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
    let stop = crate::native::jobs::stop_requested_flag();
    if do_loop || flag(args, "listen") {
        crate::native::jobs::install_stop_handler(&session);
    }
    let mut msg;
    let mut success_count;
    loop {
        let lap_begin = Instant::now();
        let (inst, pipes) = super::pipes::selected_pipes(rt, args)?;
        let engine_results = if method == LapMethod::Sync {
            let traversal = flag(args, "parents") || flag(args, "children");
            let engine = engine::build(rt, &session, &inst, &pipes, !traversal, false, None)?;
            let opts = run_options(args, &inst, &pipes, &sync_args);
            let result = session.block_on(engine.run(opts));
            for warning in &result.warnings {
                warn(rt, warning);
            }
            if !result.success && result.outcomes.is_empty() {
                return Ok(SuccessTuple::fail(result.message));
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
            if method == LapMethod::Sync {
                print_chunk_progress(rt, &tup.message);
            }
            if !nopretty {
                let m = format!(
                    "{}{}\n",
                    if tup.success {
                        format!("Finished syncing {}:\n", sp.pipe)
                    } else {
                        format!("Error while syncing {}:\n", sp.pipe)
                    },
                    tup.message
                );
                print_tuple(
                    rt,
                    &SuccessTuple {
                        success: tup.success,
                        message: m,
                    },
                    TupleArgs {
                        calm: true,
                        ..TupleArgs::default()
                    },
                );
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
        let total = success_count + failure_count;
        msg = format!(
            "It took {} to sync {total} pipe{}\n    ({success_count} succeeded, {failure_count} failed).",
            interval_str(lap_secs),
            plural(total)
        );
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

fn depth(args: &ArgsMap, key: &str) -> Option<meerschaum_engine::Depth> {
    match args.get(key) {
        Some(crate::value::PyValue::Int(n)) if *n > 0 => Some(meerschaum_engine::Depth::N(
            u32::try_from(*n).unwrap_or(u32::MAX),
        )),
        Some(v) if v.is_truthy() => Some(meerschaum_engine::Depth::All),
        _ => None,
    }
}

fn run_options(
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
        sync: meerschaum_engine::NodeSyncOptions {
            check_existing: sync_args.check_existing,
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

/// Keep the engine's subscriptions alive and render its lap events on the CLI thread.
fn listen(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    use meerschaum_engine::{LapEvent, LoopOptions, StopToken};
    use std::sync::{Arc, mpsc};
    let session = session(rt);
    super::jobs::install_stop_handler(&session);
    let signal = super::jobs::stop_requested_flag();
    let stop = StopToken::default();
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
                rt.print("\n");
                print_pipes_results(
                    rt,
                    &results,
                    "Successfully synced pipes:",
                    "Failed to sync pipes:",
                    nopretty,
                );
                results.clear();
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
            Ok(LapEvent::Sleeping { message, .. }) => {
                rt.print("\n");
                info(rt, &message);
            }
            Ok(LapEvent::Cooldown { message, .. }) => warn(rt, &message),
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
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
