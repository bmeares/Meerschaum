//! `sync pipes` (`meerschaum/actions/sync.py`) and the sync seam.
//!
//! # The sync seam
//!
//! [`sync_pipe`] is the CLI's stand-in for the engine's `Engine::run` (§5.5): it reproduces
//! `Pipe.sync()` (`meerschaum/core/Pipe/_sync.py`) for `sql:` source pipes over the
//! `InstanceConnector` trait: register → (in-place sync when source == instance) → fetch the
//! metadefinition (`_determine_begin` + `SQLConnector.fetch`) → per chunk: enforce dtypes, add the
//! autotime column, capture new special columns and `sync_pipe` → assemble v4's chunk message.
//! The orchestrator swaps this function for `Engine::run` once the engine's executor lands;
//! keep the signature narrow so that swap is local.

use std::time::Instant;

use meerschaum_connector::{DateBound, SyncOptions, pipe_ops};
use meerschaum_core::SuccessTuple;
use meerschaum_core::pipe::chunk::ChunkInterval;
use meerschaum_core::value::Map;
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::{PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};
use meerschaum_sql::instance::fetch::{FetchArgs, metadef_bound};
use meerschaum_sql::queries::metadef::Bound;

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
}

impl SyncArgs {
    /// From the parsed action arguments.
    pub fn from_args(args: &ArgsMap) -> SyncArgs {
        SyncArgs {
            begin: bound(args, "begin"),
            end: bound(args, "end"),
            params: super::params(args),
            check_existing: !flag(args, "skip_check_existing"),
            chunksize: super::int(args, "chunksize").or(Some(-1)),
        }
    }
}

fn to_bound(b: Option<&DateBound>) -> Bound {
    metadef_bound(b)
}

/// `_determine_begin(pipe, begin, end, check_existing)`.
async fn determine_begin(
    inst: &Instance,
    sp: &SelectedPipe,
    args: &SyncArgs,
    chunk_defaults: &meerschaum_core::pipe::PipeDefaults,
) -> Option<DateBound> {
    if let Some(b) = &args.begin {
        return Some(b.clone());
    }
    if args.end.is_some() {
        return None;
    }
    let sync_time = inst
        .conn()
        .get_sync_time(&sp.pipe, None, true)
        .await
        .ok()
        .flatten()?;
    let default_minutes = backtrack_default(inst);
    let interval = sp
        .pipe
        .get_backtrack_interval(args.check_existing, default_minutes, chunk_defaults)
        .ok()?;
    Some(match (sync_time, interval) {
        (DateBound::Int(i), ChunkInterval::Int(n)) => DateBound::Int(i - n),
        (DateBound::Int(i), ChunkInterval::Delta(d)) => DateBound::Int(i - d.num_minutes()),
        (DateBound::Datetime(dt), ChunkInterval::Delta(d)) => DateBound::Datetime(DateTimeValue {
            utc: dt.utc - d,
            tz: dt.tz,
        }),
        (DateBound::Datetime(dt), ChunkInterval::Int(n)) => DateBound::Datetime(DateTimeValue {
            utc: dt.utc - chrono::Duration::minutes(n),
            tz: dt.tz,
        }),
    })
}

/// `pipes:parameters:fetch:backtrack_minutes`.
fn backtrack_default(inst: &Instance) -> i64 {
    match inst {
        Instance::Sql(s) => s.settings.backtrack_minutes,
        Instance::Other(_) => meerschaum_sql::pipe::DEFAULT_BACKTRACK_MINUTES,
    }
}

/// `get_datetime_bound_from_df(chunk, dt_col, minimum)` as `str()`.
fn chunk_bound(df: &Frame, dt_col: Option<&str>, minimum: bool) -> Option<String> {
    let col = df.column(dt_col?)?;
    let values: Vec<KValue> = col
        .data
        .values()
        .into_iter()
        .filter(|v| !v.is_na())
        .collect();
    let mut best: Option<&KValue> = None;
    for v in &values {
        best = Some(match best {
            None => v,
            Some(b) => {
                let ord = pipe_ops::compare_values(v, b);
                let better = if minimum {
                    ord == std::cmp::Ordering::Less
                } else {
                    ord == std::cmp::Ordering::Greater
                };
                if better { v } else { b }
            }
        });
    }
    best.map(KValue::py_str)
}

/// `Pipe._get_chunk_label(chunk, dt_col)`.
fn chunk_label(df: &Frame, dt_col: Option<&str>) -> String {
    match (
        chunk_bound(df, dt_col, true),
        chunk_bound(df, dt_col, false),
    ) {
        (Some(a), Some(b)) => format!("{a} - {b}"),
        _ => String::new(),
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

/// THE SYNC SEAM: `pipe.sync(**sync_kwargs)` for a pipe selected from `inst`.
///
/// The engine agent replaces this with `Engine::run` (`meerschaum-engine`); everything the
/// actions need from a sync is the returned [`SuccessTuple`].
pub fn sync_pipe(
    rt: &mut Runtime<'_>,
    session: &Session,
    inst: &Instance,
    sp: &mut SelectedPipe,
    args: &SyncArgs,
) -> SuccessTuple {
    hydrate(rt, inst, sp);
    let defaults = super::pipes::defaults(rt);
    let pipe_str = sp.pipe.to_string();
    let conn = inst.conn();

    // Ensure that the pipe is registered.
    if !sp.pipe.temporary && sp.id.is_none() {
        let res = session.block_on(conn.register_pipe(&sp.pipe));
        if !res.success && !res.message.contains("already") {
            return res;
        }
        if let Ok(Some(id)) = session.block_on(conn.get_pipe_id(&sp.pipe)) {
            sp.id = Some(id);
        }
    }

    let ck = sp.pipe.connector_keys().to_string();
    if !ck.contains(':') {
        return SuccessTuple::ok(format!(
            "{pipe_str} does not support fetching; nothing to do."
        ));
    }
    let typ = ck.split(':').next().unwrap_or_default().to_string();
    if typ != "sql" {
        // Plugin and custom-connector pipes run through the Python worker (§4.4).
        return SuccessTuple::fail(format!(
            "Syncing {pipe_str} requires the Python worker for '{typ}' connectors, \
             which the Rust CLI does not start yet."
        ));
    }
    let source = match session.instance(&ck, rt.config()) {
        Ok(Instance::Sql(s)) => s,
        _ => return SuccessTuple::fail(format!("{pipe_str} does not have a valid connector.")),
    };

    // Sync in place when the source is the instance.
    let inplace_enabled = rt
        .config()
        .get(&["system", "experimental", "inplace_sync"])
        .is_none_or(|v| v.truthy());
    if let Instance::Sql(sql) = inst
        && ck == inst.keys()
        && inplace_enabled
    {
        let begin = match &args.begin {
            Some(b) => to_bound(Some(b)),
            None => Bound::Default,
        };
        return session.block_on(sql.sync_pipe_inplace(
            &sp.pipe,
            args.params.as_ref(),
            begin,
            to_bound(args.end.as_ref()),
            args.check_existing,
        ));
    }

    // Fetch.
    let begin = session.block_on(determine_begin(inst, sp, args, &defaults));
    let fetch_args = FetchArgs {
        begin: to_bound(begin.as_ref()),
        end: to_bound(args.end.as_ref()),
        check_existing: args.check_existing,
        params: args.params.clone(),
        sync_time: None,
        parent: None,
    };
    let df = match session.block_on(source.fetch(&sp.pipe, &fetch_args)) {
        Ok(Some(df)) => df,
        Ok(None) => return SuccessTuple::fail(format!("No data were fetched for {pipe_str}.")),
        Err(e) => {
            warn(rt, &format!("Failed to fetch data from {ck}:\n    {e}"));
            return SuccessTuple::fail(format!("No data were fetched for {pipe_str}."));
        }
    };

    // `read(chunksize=...)` yields chunks of `chunksize` rows (at least one, possibly empty).
    let chunksize = match args.chunksize {
        Some(c) if c > 0 => c as usize,
        _ => source.settings.chunksize.max(1) as usize,
    };
    let n = df.height();
    let mut chunks: Vec<Frame> = Vec::new();
    if n <= chunksize {
        chunks.push(df);
    } else {
        let mut start = 0;
        while start < n {
            let end = (start + chunksize).min(n);
            let ix: Vec<usize> = (start..end).collect();
            chunks.push(df.take(&ix));
            start = end;
        }
    }
    let dt_col = sp.pipe.column("datetime");
    let mut results: Vec<(bool, String)> = Vec::new();
    for (i, chunk) in chunks.into_iter().enumerate() {
        let label = chunk_label(&chunk, dt_col.as_deref());
        let rows = chunk.height();
        let res = session.block_on(sync_frame(inst, sp, chunk, args));
        if i == 0 {
            let msg = format!("\n{label}\n{}", res.message);
            if !res.success {
                return SuccessTuple::fail(format!(
                    "Unable to sync initial chunk for {pipe_str}:\n{msg}"
                ));
            }
            results.push((true, msg));
        } else {
            let rows_str = if rows != 1 {
                format!("{} rows", super::thousands(rows as i64))
            } else {
                format!("{rows} row")
            };
            let msg = format!(
                "{} a chunk ({rows_str}) to {pipe_str}:\n{label}\n{}",
                if res.success {
                    "Synced"
                } else {
                    "Failed to sync"
                },
                res.message
            );
            print_tuple(
                rt,
                &SuccessTuple {
                    success: res.success,
                    message: msg.clone(),
                },
                TupleArgs {
                    calm: true,
                    ..TupleArgs::default()
                },
            );
            results.push((res.success, msg));
        }
    }
    results.sort();
    let successes = results.iter().filter(|r| r.0).count();
    let failures = results.len() - successes;
    let joined: Vec<&str> = results.iter().map(|r| r.1.as_str()).collect();
    let msg = format!(
        "Synced {} chunk{} to {pipe_str}\n({successes} succeeded, {failures} failed):\n\n{}",
        super::thousands(results.len() as i64),
        plural(results.len()),
        joined.join("\n\n").trim()
    );
    SuccessTuple {
        success: failures == 0,
        message: msg.trim().to_string(),
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
        let mut results: Vec<(meerschaum_core::Pipe, SuccessTuple)> = Vec::new();
        for mut sp in pipes {
            let tup = match method {
                LapMethod::Sync => sync_pipe(rt, &session, &inst, &mut sp, &sync_args),
                LapMethod::Verify => {
                    super::verify::verify_pipe(rt, &session, &inst, &mut sp, args, &sync_args)
                }
                LapMethod::Deduplicate => {
                    super::verify::deduplicate_pipe(rt, &session, &inst, &mut sp, args)
                }
            };
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
