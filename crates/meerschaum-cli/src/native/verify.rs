//! `verify pipes` / `verify rowcounts` (`actions/verify.py`) and `deduplicate pipes`
//! (`actions/deduplicate.py`): `Pipe.verify()` (`core/Pipe/_verify.py`) and `Pipe.deduplicate()`
//! (`core/Pipe/_deduplicate.py`) over the sync seam ([`super::sync::sync_pipe`]).
//!
//! Chunks are processed in order on the calling thread (v4 maps batches over a thread pool;
//! with one worker, as on SQLite, the output is identical).

use chrono::Duration;
use meerschaum_connector::{DataQuery, DateBound};
use meerschaum_connector_sql::instance::data::{ChunkBounds, get_chunk_bounds, subtract_interval};
use meerschaum_core::pipe::{ChunkInterval, ChunkIntervalArg};
use meerschaum_core::value::{Map, Value};
use meerschaum_core::{SuccessTuple, pyfmt, static_config};
use meerschaum_kernel::value::DateTimeValue;

use crate::dispatch::Runtime;
use crate::format::TupleArgs;
use crate::value::{ArgsMap, PyValue};

use super::pipes::{SelectedPipe, hydrate};
use super::sync::{SyncArgs, interval_str, sync_pipe};
use super::{Instance, Session, flag, info, int, print_tuple, thousands, warn};

/// Whether the pipe's datetime axis is timezone-aware (`'utc' in dtypes[dt_col]`, with
/// `'datetime'` as the default).
pub fn dt_is_aware(session: &Session, inst: &Instance, sp: &SelectedPipe) -> bool {
    let Some(dt_col) = sp.pipe.column("datetime") else {
        return true;
    };
    let conn = inst.conn();
    let dtypes = session
        .block_on(meerschaum_connector::pipe_ops::get_dtypes(
            conn.as_ref(),
            &sp.pipe,
            sp.pipe.parameters(),
            true,
        ))
        .unwrap_or_default();
    let typ = dtypes
        .get(&dt_col)
        .cloned()
        .unwrap_or_else(|| "datetime".into());
    typ == "datetime" || typ.to_lowercase().contains("utc")
}

/// `coerce_timezone(bound, strip_utc=not aware)` (`Pipe.parse_date_bounds`).
pub fn coerce_bound(b: DateBound, aware: bool) -> DateBound {
    match b {
        DateBound::Datetime(dt) => DateBound::Datetime(if aware {
            DateTimeValue::utc(dt.utc)
        } else {
            DateTimeValue::naive(dt.utc)
        }),
        other => other,
    }
}

/// `get_sync_time(params, newest, remote)`. A remote sync time is asked of `source`
/// (v4: `self.connector if remote else self.instance_connector`).
pub fn sync_time(
    session: &Session,
    inst: &Instance,
    source: &Instance,
    sp: &SelectedPipe,
    params: Option<&Map>,
    newest: bool,
    remote: bool,
) -> Option<DateBound> {
    // PARITY: v4 returns the sync time in the pipe's timezone (aware for UTC pipes).
    let conn = if remote { source } else { inst };
    let b = sync_time_raw(session, conn, sp, params, newest, remote)?;
    Some(coerce_bound(b, dt_is_aware(session, inst, sp)))
}

fn sync_time_raw(
    session: &Session,
    inst: &Instance,
    sp: &SelectedPipe,
    params: Option<&Map>,
    newest: bool,
    remote: bool,
) -> Option<DateBound> {
    match inst {
        Instance::Sql(sql) => session
            .block_on(sql.get_sync_time(&sp.pipe, params, newest, remote))
            .ok()
            .flatten(),
        Instance::Other(conn) => {
            if remote {
                return None;
            }
            session
                .block_on(conn.get_sync_time(&sp.pipe, params, newest))
                .ok()
                .flatten()
        }
    }
}

/// `get_rowcount(begin, end, params, remote)`; `inst` is the source connector when `remote`.
fn rowcount(
    session: &Session,
    inst: &Instance,
    sp: &SelectedPipe,
    bounds: &ChunkBounds,
    params: Option<&Map>,
    remote: bool,
) -> Option<i64> {
    let q = DataQuery {
        begin: bounds.0.clone(),
        end: bounds.1.clone(),
        params: params.cloned(),
        remote,
        ..Default::default()
    };
    // PARITY: `Pipe.get_rowcount()` returns 0 when the connector returns `None`.
    match session.block_on(inst.conn().get_pipe_rowcount(&sp.pipe, &q)) {
        Ok(rc) => Some(rc.unwrap_or(0)),
        Err(_) => None,
    }
}

/// `end += timedelta(minutes=1) if hasattr(end, 'tzinfo') else 1`.
fn bump(b: DateBound) -> DateBound {
    match b {
        DateBound::Int(i) => DateBound::Int(i + 1),
        DateBound::Datetime(dt) => DateBound::Datetime(DateTimeValue {
            utc: dt.utc + Duration::minutes(1),
            tz: dt.tz,
        }),
    }
}

/// The connector remote sync times and row-counts are asked of: the pipe's `sql:` source
/// connector (v4's `pipe.connector`), else the instance (no other source type answers them
/// in v4; the instance query then fails without `fetch:definition`, like before).
fn remote_instance(
    rt: &mut Runtime<'_>,
    session: &Session,
    inst: &Instance,
    sp: &SelectedPipe,
) -> Instance {
    let keys = sp.pipe.connector_keys();
    if keys.split(':').next() != Some("sql") || keys == inst.keys() {
        return inst.clone();
    }
    session
        .instance(keys, rt.config())
        .unwrap_or_else(|_| inst.clone())
}

/// `str(bound)` (`None` for an open bound).
pub fn bound_str(b: &Option<DateBound>) -> String {
    b.as_ref()
        .map(DateBound::py_str)
        .unwrap_or_else(|| "None".into())
}

fn chunk_label(b: &ChunkBounds) -> String {
    format!("{} - {}", bound_str(&b.0), bound_str(&b.1))
}

/// `Pipe.get_bound_interval()`.
fn get_bound_interval(sp: &SelectedPipe) -> Option<ChunkInterval> {
    let verify = sp.pipe.parameters().get("verify")?.as_map()?.clone();
    let suffixes = ["minutes", "hours", "days", "weeks", "years", "seconds"];
    let mut found: Option<(String, Value)> = None;
    for (key, val) in &verify {
        if !key.starts_with("bound_") {
            continue;
        }
        if suffixes.iter().any(|s| *key == format!("bound_{s}")) {
            found = Some((key.clone(), val.clone()));
            break;
        }
    }
    let (key, val) = found?;
    if val.is_null() {
        return None;
    }
    let dt_col = sp.pipe.column("datetime");
    let is_int = dt_col.as_ref().is_some_and(|c| {
        sp.pipe
            .parameters()
            .get("dtypes")
            .and_then(Value::as_map)
            .and_then(|d| d.get(c))
            .map(pyfmt::py_str)
            .unwrap_or_else(|| "datetime".into())
            .to_lowercase()
            .contains("int")
    });
    let n = match &val {
        Value::Int(i) => *i as f64,
        Value::Float(f) => *f,
        other => pyfmt::py_str(other).trim().parse().unwrap_or(0.0),
    };
    if dt_col.is_none() || is_int {
        return Some(ChunkInterval::Int(n as i64));
    }
    let unit_secs: f64 = match key.trim_start_matches("bound_") {
        "minutes" => 60.0,
        "hours" => 3600.0,
        "days" => 86400.0,
        "weeks" => 604_800.0,
        "years" => 365.0 * 86400.0,
        _ => 1.0,
    };
    Some(ChunkInterval::Delta(Duration::microseconds(
        (n * unit_secs * 1e6) as i64,
    )))
}

fn bound_lt(a: &DateBound, b: &DateBound) -> bool {
    match (a, b) {
        (DateBound::Int(x), DateBound::Int(y)) => x < y,
        (DateBound::Datetime(x), DateBound::Datetime(y)) => x.utc < y.utc,
        _ => false,
    }
}

/// `Pipe.get_bound_time()`.
fn get_bound_time(session: &Session, inst: &Instance, sp: &SelectedPipe) -> Option<DateBound> {
    let interval = get_bound_interval(sp)?;
    let newest = sync_time(session, inst, inst, sp, None, true, false)?;
    let bound_time = subtract_interval(newest.clone(), interval);
    let oldest = sync_time(session, inst, inst, sp, None, false, false)?;
    let max_days = static_config().pipes.max_bound_time_days;
    let extreme = match (&newest, &oldest) {
        (DateBound::Datetime(n), DateBound::Datetime(o)) => {
            n.utc - o.utc >= Duration::days(max_days)
        }
        _ => false,
    };
    if bound_lt(&oldest, &bound_time) || extreme {
        Some(bound_time)
    } else {
        None
    }
}

/// The `verify` / `deduplicate` keyword arguments from the action.
#[derive(Debug, Clone, Default)]
pub struct VerifyArgs {
    /// `bounded` (`--bounded` sets `True`; unset is `None`).
    pub bounded: Option<bool>,
    /// `chunk_interval` (`--chunk-minutes` etc.).
    pub chunk_interval: Option<ChunkIntervalArg>,
    /// `workers`.
    pub workers: Option<i64>,
    /// `batchsize`.
    pub batchsize: Option<i64>,
    /// `deduplicate`.
    pub deduplicate: bool,
    /// `check_rowcounts_only`.
    pub check_rowcounts_only: bool,
    /// `skip_chunks_with_greater_rowcounts`.
    pub skip_chunks_with_greater_rowcounts: bool,
}

impl VerifyArgs {
    /// From the parsed action arguments.
    pub fn from_args(args: &ArgsMap) -> VerifyArgs {
        let chunk_interval = match args.get("chunk_interval") {
            Some(PyValue::TimeDelta(td)) => Some(ChunkIntervalArg::Delta(
                Duration::days(td.days)
                    + Duration::seconds(td.seconds)
                    + Duration::microseconds(td.microseconds),
            )),
            Some(PyValue::Int(i)) => i64::try_from(*i).ok().map(ChunkIntervalArg::Int),
            _ => None,
        };
        VerifyArgs {
            bounded: if flag(args, "bounded") {
                Some(true)
            } else {
                None
            },
            chunk_interval,
            workers: int(args, "workers"),
            batchsize: int(args, "batchsize"),
            deduplicate: flag(args, "deduplicate"),
            check_rowcounts_only: flag(args, "check_rowcounts_only"),
            skip_chunks_with_greater_rowcounts: flag(args, "skip_chunks_with_greater_rowcounts"),
        }
    }
}

/// `Pipe.get_num_workers(workers)` (no connections are checked out between syncs).
fn get_num_workers(inst: &Instance, workers: Option<i64>) -> i64 {
    match inst {
        Instance::Sql(sql) => {
            if !sql.is_thread_safe() {
                return 1;
            }
            let pool = i64::from(sql.spec().pool_size);
            let w = workers.filter(|w| *w != 0).unwrap_or(pool).min(pool);
            w.max(1)
        }
        Instance::Other(_) => workers
            .filter(|w| *w != 0)
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get() as i64)
                    .unwrap_or(1)
            })
            .max(1),
    }
}

/// `extract_stats_from_message(message)`.
pub fn extract_stats(message: &str) -> [i64; 4] {
    let keys = ["inserted", "updated", "upserted", "checked"];
    let mut stats = [0i64; 4];
    for line in message.split('\n') {
        for (i, key) in keys.iter().enumerate() {
            let lower = line.to_lowercase();
            if !lower.contains(key) {
                continue;
            }
            let lower = lower.replace(',', "");
            let Some(text) = lower.split(&format!("{key} ")).nth(1) else {
                continue;
            };
            let digits: String = text.chars().take_while(char::is_ascii_digit).collect();
            if digits.is_empty() {
                continue;
            }
            if let Ok(n) = digits.parse::<i64>() {
                stats[i] += n;
            }
        }
    }
    stats
}

/// `get_chunks_success_message(chunk_success_tuples, header, check_rowcounts_only)`.
pub fn get_chunks_success_message(
    tuples: &[(ChunkBounds, SuccessTuple)],
    header: &str,
    check_rowcounts_only: bool,
) -> String {
    let mut totals = [0i64; 4];
    for (_, t) in tuples.iter().filter(|(_, t)| t.success) {
        let s = extract_stats(&t.message);
        for i in 0..4 {
            totals[i] += s[i];
        }
    }
    let fails: Vec<&(ChunkBounds, SuccessTuple)> =
        tuples.iter().filter(|(_, t)| !t.success).collect();
    let out_of_sync: Vec<&ChunkBounds> = if check_rowcounts_only {
        tuples
            .iter()
            .filter(|(_, t)| t.message.contains("out-of-sync"))
            .map(|(b, _)| b)
            .collect()
    } else {
        Vec::new()
    };
    let header = if header.is_empty() {
        String::new()
    } else {
        format!("{header}\n")
    };
    let mut items: Vec<String> = Vec::new();
    for (i, word) in ["inserted", "updated", "upserted", "checked"]
        .iter()
        .enumerate()
    {
        if totals[i] != 0 {
            items.push(format!("{word} {}", thousands(totals[i])));
        }
    }
    if items.is_empty() {
        items.push("synced 0".into());
    }
    let stats_msg = super::items_str_with(&items, false, false, "and");
    let n = tuples.len();
    let mut success_msg = format!(
        "Successfully {} {} chunk{}\n({stats_msg} rows in total).",
        if check_rowcounts_only {
            "checked"
        } else {
            "synced"
        },
        thousands(n as i64),
        super::plural(n)
    );
    if check_rowcounts_only {
        let k = out_of_sync.len();
        success_msg.push_str(&format!(
            "\n\nFound {k} chunk{} to be out-of-sync{}{}",
            super::plural(k),
            if k == 0 { "." } else { ":\n\n  " },
            out_of_sync
                .iter()
                .map(|b| chunk_label(b))
                .collect::<Vec<_>>()
                .join("\n  ")
        ));
    }
    let fail_msg = if fails.is_empty() {
        String::new()
    } else {
        format!(
            "\n\nFailed to sync {} chunk{}:\n{}",
            thousands(fails.len() as i64),
            super::plural(fails.len()),
            fails
                .iter()
                .map(|(b, t)| format!("{}\n{}\n", chunk_label(b), t.message))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };
    format!("{header}{success_msg}{fail_msg}")
}

fn pprint_tuple(rt: &mut Runtime<'_>, tup: &SuccessTuple) {
    print_tuple(rt, tup, TupleArgs::default());
}

/// `Pipe.deduplicate(begin, end, params)` (the instance method).
pub fn deduplicate_with(
    session: &Session,
    inst: &Instance,
    sp: &SelectedPipe,
    begin: Option<DateBound>,
    end: Option<DateBound>,
    params: Option<Map>,
) -> SuccessTuple {
    let q = DataQuery {
        begin,
        end,
        params,
        ..Default::default()
    };
    session.block_on(inst.conn().deduplicate_pipe(&sp.pipe, &q))
}

/// `pipe.deduplicate(**sync_kwargs)` (`deduplicate pipes`).
pub fn deduplicate_pipe(
    rt: &mut Runtime<'_>,
    session: &Session,
    inst: &Instance,
    sp: &mut SelectedPipe,
    args: &ArgsMap,
) -> SuccessTuple {
    hydrate(rt, inst, sp);
    let sa = SyncArgs::from_args(args);
    deduplicate_with(session, inst, sp, sa.begin, sa.end, sa.params)
}

/// `pipe.verify(**sync_kwargs)` (`verify pipes`, `sync pipes --verify`).
pub fn verify_pipe(
    rt: &mut Runtime<'_>,
    session: &Session,
    inst: &Instance,
    sp: &mut SelectedPipe,
    args: &ArgsMap,
    sync_args: &SyncArgs,
) -> SuccessTuple {
    let va = VerifyArgs::from_args(args);
    verify_pipe_with(rt, session, inst, sp, &va, sync_args)
}

/// `pipe.verify(begin, end, params, chunk_interval, bounded, deduplicate, workers, batchsize,
/// skip_chunks_with_greater_rowcounts, check_rowcounts_only, **kwargs)` with the arguments
/// already parsed: `va` holds the verify keywords, `sync_args` the bounds, `params` and the
/// keywords each chunk's `pipe.sync()` receives. The façade (`_core.verify_pipe`) calls this
/// with a [`Session`] holding the connectors the Python process built.
pub fn verify_pipe_with(
    rt: &mut Runtime<'_>,
    session: &Session,
    inst: &Instance,
    sp: &mut SelectedPipe,
    va: &VerifyArgs,
    sync_args: &SyncArgs,
) -> SuccessTuple {
    hydrate(rt, inst, sp);
    let source = remote_instance(rt, session, inst, sp);
    let source = &source;
    let defaults = super::pipes::defaults(rt);
    let max_chunks_syncs = rt
        .config()
        .get(&["pipes", "verify", "max_chunks_syncs"])
        .and_then(|v| pyfmt::py_int(&v))
        .unwrap_or(3);
    let workers = get_num_workers(inst, va.workers);
    let check_rowcounts = va.skip_chunks_with_greater_rowcounts || va.check_rowcounts_only;
    let params = sync_args.params.clone();
    let pipe_str = sp.pipe.to_string();

    let bound_time = if va.bounded != Some(false) {
        get_bound_time(session, inst, sp)
    } else {
        None
    };
    let bounded = va.bounded.unwrap_or(bound_time.is_some());
    let aware = dt_is_aware(session, inst, sp);
    let mut begin = sync_args.begin.clone().map(|b| coerce_bound(b, aware));
    let mut end = sync_args.end.clone().map(|b| coerce_bound(b, aware));
    if bounded && begin.is_none() {
        begin = bound_time
            .clone()
            .or_else(|| sync_time(session, inst, source, sp, None, false, false))
            .or_else(|| sync_time(session, inst, source, sp, None, false, true));
    }
    if bounded && end.is_none() {
        end = sync_time(session, inst, source, sp, None, true, false)
            .or_else(|| sync_time(session, inst, source, sp, None, true, true))
            .map(bump);
    }
    let cannot_determine_bounds = bounded && begin.is_none() && end.is_none();

    let chunk_sync_args = |b: Option<DateBound>, e: Option<DateBound>| SyncArgs {
        begin: b,
        end: e,
        params: params.clone(),
        ..sync_args.clone()
    };

    if cannot_determine_bounds && !va.check_rowcounts_only {
        warn(
            rt,
            &format!("Cannot determine sync bounds for {pipe_str}. Syncing instead..."),
        );
        let res = sync_pipe(
            rt,
            session,
            inst,
            sp,
            &chunk_sync_args(begin.clone(), end.clone()),
        );
        if !res.success {
            return res;
        }
        if va.deduplicate {
            return deduplicate_with(session, inst, sp, begin, end, params);
        }
        return res;
    }

    let chunk_interval = match sp.pipe.get_chunk_interval(va.chunk_interval, &defaults) {
        Ok(c) => c,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let oldest = sync_time(session, inst, source, sp, None, false, false);
    let newest = sync_time(session, inst, source, sp, None, true, false);
    let chunk_bounds = match get_chunk_bounds(
        begin.clone(),
        end.clone(),
        bounded,
        chunk_interval,
        true,
        oldest,
        newest,
    ) {
        Ok(c) => c,
        Err(e) => return SuccessTuple::fail(e),
    };

    if chunk_bounds.is_empty() {
        if va.deduplicate {
            return deduplicate_with(session, inst, sp, begin, end, params);
        }
        return SuccessTuple::ok(format!(
            "Could not determine chunks between '{}' and '{}'; nothing to do.",
            bound_str(&begin),
            bound_str(&end)
        ));
    }

    let first = &chunk_bounds[0];
    let last = &chunk_bounds[chunk_bounds.len() - 1];
    let begin_to_print = match &begin {
        Some(b) => b.py_str(),
        None => bound_str(if bounded { &first.0 } else { &first.1 }),
    };
    let end_to_print = match &end {
        Some(e) => e.py_str(),
        None => bound_str(if bounded { &last.1 } else { &last.0 }),
    };
    let message_header = format!("{begin_to_print} - {end_to_print}");
    let interval_text = match &chunk_interval {
        ChunkInterval::Delta(d) => interval_str(d.num_microseconds().unwrap_or(0) as f64 / 1e6),
        ChunkInterval::Int(i) => i.to_string(),
    };
    let n = chunk_bounds.len();
    info(
        rt,
        &format!(
            "Verifying {pipe_str}:\n    {} {n} chunk{} ({}bounded) of size '{interval_text}' between '{begin_to_print}' and '{end_to_print}'.\n",
            if va.check_rowcounts_only {
                "Checking"
            } else {
                "Syncing"
            },
            super::plural(n),
            if bounded { "" } else { "un" },
        ),
    );

    let mut all: Vec<(ChunkBounds, SuccessTuple)> = Vec::new();
    let process = |rt: &mut Runtime<'_>,
                   sp: &mut SelectedPipe,
                   bounds: &ChunkBounds,
                   all: &[(ChunkBounds, SuccessTuple)]|
     -> SuccessTuple {
        if let Some((_, t)) = all.iter().find(|(b, _)| b == bounds) {
            return t.clone();
        }
        let mut do_sync = true;
        let mut result = SuccessTuple::fail("Did not sync chunk.");
        if check_rowcounts {
            let existing = rowcount(session, inst, sp, bounds, None, false);
            let remote = rowcount(session, source, sp, bounds, None, true);
            let ex = existing.unwrap_or(0);
            let checked = format!(
                "checked {} row{} vs {} remote",
                thousands(ex),
                super::plural(ex as usize),
                thousands(remote.unwrap_or(0))
            );
            if let (Some(e), Some(r)) = (existing, remote)
                && e >= r
            {
                do_sync = false;
                result = SuccessTuple::ok(format!("Row-count is up-to-date ({checked})."));
            } else if va.check_rowcounts_only {
                do_sync = false;
                result = SuccessTuple::ok(format!("Row-counts are out-of-sync ({checked})."));
            }
        }
        let mut num_syncs = 0;
        while num_syncs < max_chunks_syncs {
            if do_sync {
                result = sync_pipe(
                    rt,
                    session,
                    inst,
                    sp,
                    &chunk_sync_args(bounds.0.clone(), bounds.1.clone()),
                );
            }
            if result.success {
                break;
            }
            num_syncs += 1;
            std::thread::sleep(std::time::Duration::from_secs(
                (num_syncs * num_syncs) as u64,
            ));
        }
        let mut msg = result.message.trim().to_string();
        if !msg.contains(" - ") {
            msg = format!(
                "Verified chunk for {pipe_str}:\n{}\n{msg}",
                chunk_label(bounds)
            );
        }
        let tup = SuccessTuple {
            success: result.success,
            message: msg,
        };
        pprint_tuple(rt, &tup);
        tup
    };

    let mut remaining: Vec<ChunkBounds> = chunk_bounds.clone();
    if n > 1 {
        let first_label = chunk_label(first);
        info(
            rt,
            &format!("Verifying first chunk for {pipe_str}:\n    {first_label}"),
        );
        let t = process(rt, sp, first, &all);
        if !t.success {
            return SuccessTuple::fail(format!(
                "\n{first_label}\nFailed to sync first chunk:\n{}",
                t.message
            ));
        }
        all.push((first.clone(), t));
        info(
            rt,
            &format!("Completed first chunk for {pipe_str}:\n    {first_label}\n"),
        );
        remaining.remove(0);
    }

    let batchsize = va.batchsize.filter(|b| *b > 0).unwrap_or(workers).max(1) as usize;
    let batches: Vec<Vec<ChunkBounds>> = remaining.chunks(batchsize).map(<[_]>::to_vec).collect();
    let num_batches = batches.len();

    let process_batch = |rt: &mut Runtime<'_>,
                         sp: &mut SelectedPipe,
                         batch: &[ChunkBounds],
                         all: &mut Vec<(ChunkBounds, SuccessTuple)>|
     -> SuccessTuple {
        let b_begin = batch[0].0.clone();
        let b_end = batch[batch.len() - 1].1.clone();
        let header = format!("{} - {}", bound_str(&b_begin), bound_str(&b_end));
        if va.check_rowcounts_only {
            info(
                rt,
                &format!("Checking row-counts for batch bounds:\n    {header}"),
            );
            let t = process(rt, sp, &(b_begin.clone(), b_end.clone()), all);
            pprint_tuple(rt, &t);
            if t.success && t.message.contains("up-to-date") {
                info(rt, "Entire batch is up-to-date.");
                return t;
            }
        }
        let mut batch_tuples: Vec<(ChunkBounds, SuccessTuple)> = Vec::new();
        for b in batch {
            let t = process(rt, sp, b, all);
            batch_tuples.push((b.clone(), t));
        }
        for (b, t) in &batch_tuples {
            upsert(all, b, t);
        }
        if batch_tuples.iter().all(|(_, t)| t.success) {
            let msg = get_chunks_success_message(&batch_tuples, &header, va.check_rowcounts_only);
            if va.deduplicate {
                let d = deduplicate_with(session, inst, sp, b_begin, b_end, params.clone());
                return SuccessTuple {
                    success: d.success,
                    message: format!("{msg}\n\n{}", d.message),
                };
            }
            return SuccessTuple::ok(msg);
        }
        // PARITY: `zip(batch_chunk_bounds, batch_bounds_success_bools)` zips the bounds with
        // the dict's keys (truthy tuples), so no chunk is retried and `all([])` reports the
        // batch as a success.
        let msg = format!(
            "{}\nRetried 0 chunks.",
            get_chunks_success_message(&batch_tuples, &header, va.check_rowcounts_only)
        );
        if va.deduplicate {
            let d = deduplicate_with(session, inst, sp, b_begin, b_end, params.clone());
            return SuccessTuple {
                success: d.success,
                message: format!("{msg}\n\n{}", d.message),
            };
        }
        SuccessTuple::ok(msg)
    };

    for (i, batch) in batches.iter().enumerate() {
        let counter = format!(
            "({}/{})",
            thousands(i as i64 + 1),
            thousands(num_batches as i64)
        );
        let b_begin = bound_str(&batch[0].0);
        let b_end = bound_str(&batch[batch.len() - 1].1);
        let batch_label = format!("batch {counter}:\n{b_begin} - {b_end}");
        let label_str = batch_label.replace(":\n", &format!(" for {pipe_str}...\n    "));
        info(rt, &format!("Verifying {label_str}\n"));
        let t = process_batch(rt, sp, batch, &mut all);
        let header = rt.with_formatter(|f| {
            f.make_header_with(&format!("Completed batch {counter}:"), "─", 0, false, 1)
        });
        pprint_tuple(
            rt,
            &SuccessTuple {
                success: t.success,
                message: format!("{header}\n{}", t.message),
            },
        );
        let mut t = t;
        if !t.success {
            info(rt, &format!("Retrying batch {counter}..."));
            let r = process_batch(rt, sp, batch, &mut all);
            let header = rt.with_formatter(|f| {
                f.make_header_with(&format!("batch {batch_label}"), "─", 0, false, 1)
            });
            pprint_tuple(
                rt,
                &SuccessTuple {
                    success: r.success,
                    message: format!("Retried {header}\n{}", r.message),
                },
            );
            t = r;
        }
        if !t.success {
            return SuccessTuple::fail(format!("Failed to verify {batch_label}:\n\n{}", t.message));
        }
    }

    SuccessTuple::ok(get_chunks_success_message(
        &all,
        &message_header,
        va.check_rowcounts_only,
    ))
}

fn upsert(all: &mut Vec<(ChunkBounds, SuccessTuple)>, b: &ChunkBounds, t: &SuccessTuple) {
    match all.iter_mut().find(|(x, _)| x == b) {
        Some(slot) => slot.1 = t.clone(),
        None => all.push((b.clone(), t.clone())),
    }
}
