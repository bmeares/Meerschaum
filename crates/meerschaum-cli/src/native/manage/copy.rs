//! `copy pipes <instance>` (`actions/copy.py`): copying registrations and, optionally, data to
//! another instance in incremental, backtracked chunks.

use meerschaum_connector::windows::{just_past, split_point};
use meerschaum_connector::{DataQuery, DateBound, InstanceConnector};
use meerschaum_connector_sql::instance::data::get_chunk_bounds;
use meerschaum_core::access::Locks;
use meerschaum_core::pipe::{ChunkInterval, DEFAULT_BACKTRACK_MINUTES, PipeDefaults};
use meerschaum_core::value::{Map, Value};
use meerschaum_core::{Pipe, SuccessTuple};
use meerschaum_kernel::Frame;
use meerschaum_kernel::value::DateTimeValue;

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::format::TupleArgs;
use crate::value::ArgsMap;

use super::register_pipe;
use crate::native::pipes::{SelectedPipe, build_pipe, hydrate_checked, selected_pipes};
use crate::native::show::bound;
use crate::native::{flag, info, plural, print_tuple, session, string, warn, words};

/// `copy pipes <instance>`: copy the selected pipes' registrations (and optionally data) to
/// another instance. Without a destination instance, v4 prompts for each new key.
pub fn copy_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let yes = flag(args, "yes");
    let noask = flag(args, "noask");
    let force = flag(args, "force");
    let Some(dest_keys) = action.first().cloned() else {
        return Ok(SuccessTuple::fail(
            "Copying pipes interactively is not available in the Rust CLI yet; pass the destination instance (`copy pipes <instance>`).",
        ));
    };
    let (src_inst, mut pipes) = selected_pipes(rt, args)?;
    let api_profile =
        (rt.env)(meerschaum_core::team_scope::api_execution::PROFILE_VAR).as_deref() == Some("1");
    if api_profile
        && meerschaum_server::execute_policy::bound_command_scopes(&words(args, "sysargs"))
            .is_some()
        && pipes.len() != 1
    {
        return Ok(SuccessTuple::fail("Copy requires one visible source pipe."));
    }
    let session = session(rt);
    let dest = session.instance(&dest_keys, rt.config())?;
    let auto_resolve = force || yes || noask;
    let mut sync_data = string(args, "sync_data");
    if sync_data.is_none()
        && (args.contains_key("begin") || args.contains_key("end") || args.contains_key("params"))
    {
        sync_data = Some("filter".into());
    }
    if !auto_resolve {
        return Ok(SuccessTuple::fail(
            "Copying pipes without `-y`, `-f` or `--noask` prompts for each conflict and is not available in the Rust CLI yet.",
        ));
    }
    let overwrite = if force { "overwrite" } else { "skip" };
    let batch_sync = sync_data.clone().unwrap_or_else(|| "nothing".into());
    let Some(mode) = CopyData::parse(&batch_sync) else {
        return Ok(SuccessTuple::fail(format!(
            "Unknown `--sync-data {batch_sync}` (expected nothing, backtrack, incremental, all or filter)."
        )));
    };
    // The filters `all` and `backtrack` would ignore: refused, never a silent copy of everything.
    let bounded = ["begin", "end"]
        .iter()
        .any(|key| string(args, key).is_some())
        || super::params(args).is_some_and(|p| !p.is_empty());
    if bounded && matches!(mode, CopyData::All | CopyData::Backtrack) {
        return Ok(SuccessTuple::fail(format!(
            "`--sync-data {batch_sync}` ignores --begin, --end and --params.\n\
             To bound the copy, use `--sync-data filter` (exactly those rows) or \
             `--sync-data incremental` (the new rows within them)."
        )));
    }
    let explicit = DataQuery {
        begin: bound(args, "begin"),
        end: bound(args, "end"),
        params: super::params(args),
        ..Default::default()
    };
    let default_backtrack_minutes = rt
        .config()
        .get(&["pipes", "parameters", "fetch", "backtrack_minutes"])
        .and_then(|v| v.as_i64())
        .unwrap_or(DEFAULT_BACKTRACK_MINUTES);
    let defaults = super::pipes::defaults(rt);
    // Each pipe's outcome, for the summary: copied, skipped (already on the destination) or
    // failed (with the reason). Any failure fails the action.
    let mut copied: Vec<String> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();
    let mut failed: Vec<(String, String)> = Vec::new();
    let n = pipes.len();
    for (i, sp) in pipes.iter_mut().enumerate() {
        // The source's parameters and locks are what the copy writes: a read that fails stops
        // this pipe before anything is written to the destination.
        if let Err(e) = hydrate_checked(rt, &src_inst, sp) {
            warn(
                rt,
                &format!("Failed to copy {} to '{}'.", sp.pipe, dest.keys()),
            );
            failed.push((sp.pipe.to_string(), format!("attributes: {e}")));
            continue;
        }
        info(rt, &format!("Copying {} ({} / {n})...", sp.pipe, i + 1));
        // A copy keeps the source's locks and never takes its access (owner, team,
        // visibility, edit): a new pipe gets the default access and an existing destination
        // keeps its own. The locks are set last, so a `sync` lock does not refuse the copy's own
        // data sync.
        let (params, source_locks) = super::access::copy_parameters(sp.pipe.raw_parameters());
        let mut new_pipe = build_pipe(
            sp.pipe.connector_keys(),
            sp.pipe.metric_key(),
            sp.pipe.location_key(),
            &dest.keys(),
            Some(params),
            &defaults,
        )?;
        // Whether the destination already has this pipe: a lookup that fails is this pipe's
        // failure, never "not registered" (the copy would otherwise register over it, or lose
        // an incremental copy's data-only mode).
        let exists = match session.block_on(dest.conn().get_pipe_id_checked(&new_pipe)) {
            Ok(id) => id.is_some(),
            Err(e) => {
                warn(
                    rt,
                    &format!("Failed to copy {} to '{}'.", sp.pipe, dest.keys()),
                );
                failed.push((
                    sp.pipe.to_string(),
                    format!("attributes: could not tell whether {new_pipe} is registered: {e}"),
                ));
                continue;
            }
        };
        let mut dest_locks = Locks::new();
        // An incremental copy into an existing pipe without `--force` copies data only (v4's
        // interactive "data only" answer): the destination keeps all of its attributes.
        let data_only = exists && !force && mode == CopyData::Incremental;
        if exists {
            let mut existing = SelectedPipe {
                pipe: new_pipe.clone(),
                id: None,
                hydrated: false,
            };
            // `hydrate` patches the pipe's own (copied) parameters over the stored ones: start
            // from the stored ones alone to read the destination's access and locks.
            existing.pipe.set_parameters(Map::new());
            // The destination's access and locks (or, data only, all of its attributes) are kept:
            // a read that fails stops this pipe before any edit or registration.
            if let Err(e) = hydrate_checked(rt, &dest, &mut existing) {
                warn(
                    rt,
                    &format!("Failed to copy {} to '{}'.", sp.pipe, dest.keys()),
                );
                failed.push((sp.pipe.to_string(), format!("attributes: {e}")));
                continue;
            }
            if data_only {
                new_pipe = existing.pipe;
            } else {
                let stored = existing.pipe.raw_parameters();
                dest_locks = Locks::from_parameters(stored);
                let mut params = new_pipe.raw_parameters().clone();
                for key in ["access", "locks"] {
                    if let Some(value) = stored.get(key) {
                        params.insert(key.to_string(), value.clone());
                    }
                }
                new_pipe.set_parameters(params);
            }
        }
        if data_only {
            // v4's message for its interactive "data only" answer (`actions/copy.py`).
            info(
                rt,
                &format!("Keeping existing attributes of {new_pipe}: will copy data only."),
            );
        } else if exists {
            if overwrite == "skip" {
                info(
                    rt,
                    &format!("Skipping {new_pipe} — leaving destination pipe unchanged."),
                );
                skipped.push(sp.pipe.to_string());
                continue;
            }
            info(
                rt,
                &format!(
                    "Updating attributes of existing {new_pipe} from {}...",
                    sp.pipe
                ),
            );
            let res = session.block_on(dest.conn().edit_pipe(&new_pipe, false));
            if !res.success {
                warn(
                    rt,
                    &format!("Failed to copy attributes to {new_pipe}:\n{}", res.message),
                );
                failed.push((sp.pipe.to_string(), res.message));
                continue;
            }
        } else {
            let res = register_pipe(rt, &dest, &mut new_pipe);
            if !res.success {
                warn(
                    rt,
                    &format!("Failed to register new {new_pipe}:\n{}", res.message),
                );
                failed.push((sp.pipe.to_string(), res.message));
                continue;
            }
        }
        print_tuple(
            rt,
            &SuccessTuple::ok(format!("Successfully copied {} to {new_pipe}.", sp.pipe)),
            TupleArgs::default(),
        );
        // Whether the source has a table to read: a probe that fails is this pipe's failure,
        // never "no rows to copy" (v4 took any failure for a missing table and reported the pipe
        // copied without its rows).
        let src_exists = if mode == CopyData::Nothing {
            false
        } else {
            match session.block_on(src_inst.conn().pipe_exists_checked(&sp.pipe)) {
                Ok(exists) => exists,
                Err(e) => {
                    warn(
                        rt,
                        &format!("Failed to copy data from {} to {new_pipe}.", sp.pipe),
                    );
                    failed.push((
                        sp.pipe.to_string(),
                        format!("data: could not tell whether {} has a table: {e}", sp.pipe),
                    ));
                    continue;
                }
            }
        };
        if src_exists && mode != CopyData::Nothing {
            // The rows to read: `None` when nothing is new (incremental), an error when the
            // window cannot be told (never a fall-back to the whole history).
            let query = match mode {
                CopyData::Incremental => {
                    let window = session.block_on(incremental_window(
                        dest.conn().as_ref(),
                        &new_pipe,
                        &explicit,
                        default_backtrack_minutes,
                        &defaults,
                    ));
                    match window {
                        Ok((query, note)) => {
                            info(rt, &note);
                            query
                        }
                        Err(reason) => {
                            warn(rt, &format!("Not copying data into {new_pipe}:\n{reason}"));
                            failed.push((sp.pipe.to_string(), format!("data: {reason}")));
                            continue;
                        }
                    }
                }
                CopyData::Filter => Some(explicit.clone()),
                _ => Some(DataQuery::default()),
            };
            let res = match query {
                None => SuccessTuple::ok(format!("No new rows to copy into {new_pipe}.")),
                Some(query) => {
                    info(rt, &format!("Fetching data from {}...", sp.pipe));
                    // An `api:` server answers at most `api:data:max_response_row_limit` rows per
                    // read and drops the rest silently: each read asks for that many explicitly
                    // (a server allowing fewer refuses it), and a read that comes back full is
                    // split, never taken for the whole chunk.
                    let cap = if api_profile || src_inst.conn().connector_keys().starts_with("api:")
                    {
                        rt.config()
                            .get(&["api", "data", "max_response_row_limit"])
                            .and_then(|v| v.as_i64())
                            .filter(|n| *n > 0)
                            .or(Some(DEFAULT_RESPONSE_ROW_LIMIT))
                    } else {
                        None
                    };
                    let sync_into = |df| {
                        let mut dest_sp = SelectedPipe {
                            pipe: new_pipe.clone(),
                            id: None,
                            hydrated: true,
                        };
                        let sargs = super::sync::SyncArgs {
                            check_existing: true,
                            chunksize: Some(-1),
                            ..Default::default()
                        };
                        super::sync::sync_frame_public(&session, &dest, &mut dest_sp, df, &sargs)
                    };
                    // Every mode but `backtrack` reads chunk by chunk, as v4 did
                    // (`get_data(as_iterator=True)`): never the whole history in one frame.
                    // A `backtrack` read from a source without a row limit stays one read, as in
                    // v4 (`get_backtrack_data`); from an `api:` source it is the same window,
                    // read in limited, splittable parts.
                    let windows = match (mode, cap) {
                        (CopyData::Backtrack, None) => Ok(vec![]),
                        (CopyData::Backtrack, Some(_)) => session.block_on(backtrack_windows(
                            src_inst.conn().as_ref(),
                            &sp.pipe,
                            default_backtrack_minutes,
                            &defaults,
                        )),
                        _ => session.block_on(copy_windows(
                            src_inst.conn().as_ref(),
                            &sp.pipe,
                            &query,
                            &defaults,
                        )),
                    };
                    match windows {
                        Err(reason) => SuccessTuple::fail(format!(
                            "Not copying data into {new_pipe}:\n{reason}"
                        )),
                        Ok(windows)
                            if windows.len() > 1 || (cap.is_some() && !windows.is_empty()) =>
                        {
                            if windows.len() > 1 {
                                info(
                                    rt,
                                    &format!(
                                        "Syncing into {new_pipe} in {} chunks...",
                                        windows.len()
                                    ),
                                );
                            }
                            let read = |window: &DataQuery| {
                                session.block_on(src_inst.conn().get_pipe_data(&sp.pipe, window))
                            };
                            copy_chunks(&sp.pipe, &new_pipe, windows, cap, read, sync_into)
                        }
                        Ok(windows) => {
                            let df = match windows.first() {
                                Some(window) => session
                                    .block_on(src_inst.conn().get_pipe_data(&sp.pipe, window)),
                                None => {
                                    let minutes = backtrack_minutes(
                                        &sp.pipe,
                                        default_backtrack_minutes,
                                        &defaults,
                                    );
                                    session.block_on(src_inst.conn().get_backtrack_data(
                                        &sp.pipe,
                                        minutes,
                                        &DataQuery::default(),
                                    ))
                                }
                            };
                            info(rt, &format!("Syncing into {new_pipe}..."));
                            match df {
                                Ok(Some(df)) => sync_into(df),
                                Ok(None) => SuccessTuple::fail(format!(
                                    "You passed `None` instead of data into `sync()` for {new_pipe}.\nOmit the DataFrame to infer fetching."
                                )),
                                Err(e) => SuccessTuple::fail(e.to_string()),
                            }
                        }
                    }
                }
            };
            print_tuple(rt, &res, TupleArgs::default());
            if !res.success {
                warn(
                    rt,
                    &format!("Failed to copy data from {} to {new_pipe}.", sp.pipe),
                );
                failed.push((sp.pipe.to_string(), format!("data: {}", res.message)));
                continue;
            }
        }
        // The source's locks, added to the destination's own (a copy never unlocks), with the
        // destination's `lock_pipe` (an `api:` destination asks its server).
        if !data_only && dest_locks.with(&source_locks).kinds() != dest_locks.kinds() {
            let res = session.block_on(dest.conn().lock_pipe(&new_pipe, &source_locks));
            if !res.success {
                warn(
                    rt,
                    &format!("Failed to copy the locks of {} to {new_pipe}.", sp.pipe),
                );
                failed.push((sp.pipe.to_string(), format!("locks: {}", res.message)));
                continue;
            }
        }
        copied.push(sp.pipe.to_string());
    }
    Ok(copy_summary(&dest.keys(), &copied, &skipped, &failed))
}

/// What `copy pipes --sync-data` copies from each source pipe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CopyData {
    /// No data (the default).
    Nothing,
    /// The source pipe's backtrack window before its own sync time.
    Backtrack,
    /// The rows from each destination pipe's sync time minus its backtrack window.
    Incremental,
    /// All of the source pipe's rows.
    All,
    /// The rows within `--begin`, `--end` and `--params`.
    Filter,
}

impl CopyData {
    pub(super) fn parse(choice: &str) -> Option<CopyData> {
        Some(match choice {
            "nothing" => CopyData::Nothing,
            "backtrack" => CopyData::Backtrack,
            "incremental" => CopyData::Incremental,
            "all" => CopyData::All,
            "filter" => CopyData::Filter,
            _ => return None,
        })
    }
}

/// The source rows an incremental copy reads for `dest_pipe`, and a line saying where they
/// start (`None`: `--end` is not after the window's start, so nothing is new).
///
/// The window starts at the destination pipe's sync time (within `--params`) minus the pipe's
/// backtrack window, or at `--begin` when that is later, and ends at `--end`. A destination
/// without a sync time is copied from `--begin` (or from the start) only when its connector
/// proves it has no rows (`pipe_has_rows_checked`); one with rows, or one whose rows cannot be
/// counted, is an error, so a failed watermark or count is never read as "copy everything".
pub(super) async fn incremental_window(
    dest: &dyn InstanceConnector,
    dest_pipe: &Pipe,
    explicit: &DataQuery,
    default_backtrack_minutes: i64,
    defaults: &PipeDefaults,
) -> Result<(Option<DataQuery>, String), String> {
    let sync_time = dest
        .get_sync_time(dest_pipe, explicit.params.as_ref(), true)
        .await
        .map_err(|e| format!("Failed to read the sync time of {dest_pipe}: {e}"))?;
    let watermark = match sync_time {
        Some(sync_time) => Some(backtrack_from(
            dest_pipe,
            sync_time,
            default_backtrack_minutes,
            defaults,
        )),
        None => {
            // Only a target proven missing or empty is filled; an unknown count never is.
            let has_rows = dest
                .pipe_has_rows_checked(dest_pipe, explicit.params.as_ref())
                .await
                .map_err(|e| {
                    format!(
                        "{dest_pipe} has no sync time, and whether it has rows cannot be \
                         determined ({e}), so an incremental copy would not know where to \
                         start.\nUse `--sync-data filter` with explicit bounds for this pipe."
                    )
                })?;
            if has_rows {
                return Err(format!(
                    "{dest_pipe} has rows but no sync time (no datetime column, or one without \
                     readable values), so an incremental copy cannot tell which rows are \
                     new.\nUse `--sync-data all` or `--sync-data filter` for this pipe."
                ));
            }
            None
        }
    };
    incremental_query(dest_pipe, watermark, explicit)
}

/// The query of an incremental copy from the destination's watermark (its sync time minus its
/// backtrack window, `None` for a destination without rows) and the explicit filters.
/// `--begin` only raises the watermark and `--end` caps it; `None` when the window is empty,
/// so the source is not read at all.
pub(super) fn incremental_query(
    dest_pipe: &Pipe,
    watermark: Option<DateBound>,
    explicit: &DataQuery,
) -> Result<(Option<DataQuery>, String), String> {
    let (begin, from_watermark) = match (watermark.clone(), explicit.begin.clone()) {
        (Some(watermark), Some(begin)) => {
            if bound_before(&watermark, &begin, dest_pipe)? {
                (Some(begin), false)
            } else {
                (Some(watermark), true)
            }
        }
        (Some(watermark), None) => (Some(watermark), true),
        (None, begin) => (begin, false),
    };
    let rows = match (&begin, &explicit.end) {
        (Some(begin), Some(end)) => format!("rows from {} until {}", begin.py_str(), end.py_str()),
        (Some(begin), None) => format!("rows from {} on", begin.py_str()),
        (None, Some(end)) => format!("rows until {}", end.py_str()),
        (None, None) => "all rows".to_string(),
    };
    let note = if watermark.is_none() {
        let matching = if explicit.params.is_some() {
            " matching rows"
        } else {
            " rows"
        };
        format!("{dest_pipe} has no{matching} yet: copying {rows}.")
    } else if from_watermark {
        format!("Copying {rows} into {dest_pipe} (its sync time minus its backtrack window).")
    } else {
        format!("Copying {rows} into {dest_pipe} (--begin is after its sync time).")
    };
    if let (Some(begin), Some(end)) = (&begin, &explicit.end)
        && !bound_before(begin, end, dest_pipe)?
    {
        return Ok((None, note));
    }
    let query = DataQuery {
        begin,
        end: explicit.end.clone(),
        params: explicit.params.clone(),
        ..Default::default()
    };
    Ok((Some(query), note))
}

/// `pipe.get_sync_time(apply_backtrack_interval=True)` from a sync time already read: the
/// pipe's `fetch.backtrack_minutes` (else the configured default) as a duration, or in axis
/// units on an integer axis (scaled by `precision`). Integer values decide the axis when the
/// dtypes do not say (v4 infers the dtype from the table).
pub(super) fn backtrack_from(
    pipe: &Pipe,
    sync_time: DateBound,
    default_backtrack_minutes: i64,
    defaults: &PipeDefaults,
) -> DateBound {
    let interval = match axis_pipe(pipe, &sync_time).get_backtrack_interval(
        true,
        default_backtrack_minutes,
        defaults,
    ) {
        Ok(interval) => interval,
        Err(_) => return sync_time,
    };
    match (&sync_time, &interval) {
        (DateBound::Datetime(dt), ChunkInterval::Delta(delta)) => {
            match dt.utc.checked_sub_signed(*delta) {
                Some(utc) => DateBound::Datetime(DateTimeValue {
                    utc,
                    tz: dt.tz.clone(),
                }),
                None => sync_time,
            }
        }
        (DateBound::Int(i), ChunkInterval::Int(width)) => DateBound::Int(i.saturating_sub(*width)),
        // PARITY: v4 cannot subtract a duration from an integer (or the reverse) and keeps the
        // sync time itself.
        _ => sync_time,
    }
}

/// `pipe` with an integer dtype for its datetime column when `sync_time` is an integer and no
/// dtype is configured for it (`Pipe::dtypes` would otherwise default it to `datetime`).
pub(super) fn axis_pipe(pipe: &Pipe, sync_time: &DateBound) -> Pipe {
    let mut axis = pipe.clone();
    if let (DateBound::Int(_), Some(dt_col)) = (sync_time, pipe.column("datetime"))
        && pipe
            .param("dtypes")
            .and_then(|dtypes| dtypes.get(&dt_col))
            .is_none_or(|dtype| !dtype.truthy())
    {
        let mut dtypes = Map::new();
        dtypes.insert(dt_col, Value::Str("int".into()));
        let mut patch = Map::new();
        patch.insert("dtypes".into(), Value::Map(dtypes));
        axis.update_parameters(patch);
    }
    axis
}

/// The `backtrack_minutes` a source pipe's `get_backtrack_data` takes: its own
/// `fetch.backtrack_minutes` (else the configured default), in axis units on an integer axis.
pub(super) fn backtrack_minutes(
    pipe: &Pipe,
    default_backtrack_minutes: i64,
    defaults: &PipeDefaults,
) -> i64 {
    match pipe.get_backtrack_interval(true, default_backtrack_minutes, defaults) {
        Ok(ChunkInterval::Int(width)) => width,
        Ok(ChunkInterval::Delta(delta)) => delta.num_minutes(),
        Err(_) => default_backtrack_minutes,
    }
}

/// `api:data:max_response_row_limit` when the config has none.
const DEFAULT_RESPONSE_ROW_LIMIT: i64 = 100_000;

/// The windows `copy pipes` reads one at a time (v4's `get_data(as_iterator=True)`): chunks of
/// the source's chunk interval (`verify.chunk_minutes`, ...) from the query's `begin` (else the
/// oldest value) to its `end` (else just past the newest), with the query's `params`; one window
/// with those resolved bounds when they fit in one chunk (so a limited read can still be split).
/// The query itself when the pipe has no datetime axis or no rows to bound. A sync time or
/// chunk interval that cannot be read is an error, never an unbounded read.
pub(super) async fn copy_windows(
    src: &dyn InstanceConnector,
    pipe: &Pipe,
    query: &DataQuery,
    defaults: &PipeDefaults,
) -> Result<Vec<DataQuery>, String> {
    if !pipe.columns().get("datetime").is_some_and(|v| !v.is_null()) {
        return Ok(vec![query.clone()]);
    }
    let params = query.params.as_ref();
    let sync_time = |newest: bool| async move {
        src.get_sync_time(pipe, params, newest)
            .await
            .map_err(|e| format!("Failed to read the sync time of {pipe}: {e}"))
    };
    let begin = match &query.begin {
        Some(begin) => Some(begin.clone()),
        None => sync_time(false).await?,
    };
    let end = match &query.end {
        Some(end) => Some(end.clone()),
        None => sync_time(true).await?.and_then(just_past),
    };
    let (Some(begin), Some(end)) = (begin, end) else {
        return Ok(vec![query.clone()]);
    };
    let interval = pipe
        .get_chunk_interval(None, defaults)
        .map_err(|e| format!("Failed to read the chunk interval of {pipe}: {e}"))?;
    let bounds = get_chunk_bounds(
        Some(begin.clone()),
        Some(end.clone()),
        false,
        interval,
        false,
        None,
        None,
    )
    .map_err(|e| format!("Failed to divide the rows of {pipe} into chunks: {e}"))?;
    if bounds.len() > 1 {
        return Ok(bounds
            .into_iter()
            .map(|(begin, end)| DataQuery {
                begin,
                end,
                ..query.clone()
            })
            .collect());
    }
    Ok(vec![DataQuery {
        begin: Some(begin),
        end: Some(end),
        ..query.clone()
    }])
}

/// The windows of a `backtrack` copy from a source with a row limit (an `api:` instance): the
/// rows `get_backtrack_data` reads (from the source pipe's newest value minus its own backtrack
/// window), as [`copy_windows`] divides them; all of them when the source has no sync time.
pub(super) async fn backtrack_windows(
    src: &dyn InstanceConnector,
    pipe: &Pipe,
    default_backtrack_minutes: i64,
    defaults: &PipeDefaults,
) -> Result<Vec<DataQuery>, String> {
    let newest = src
        .get_sync_time(pipe, None, true)
        .await
        .map_err(|e| format!("Failed to read the sync time of {pipe}: {e}"))?;
    let query = DataQuery {
        begin: newest
            .map(|newest| backtrack_from(pipe, newest, default_backtrack_minutes, defaults)),
        ..Default::default()
    };
    copy_windows(src, pipe, &query, defaults).await
}

/// Read and sync each window in order, skipping empty ones (v4 never yielded an empty chunk).
/// With `cap` (an `api:` source's row limit), each read asks for at most `cap` rows, and a read
/// that comes back with `cap` rows may have been cut: its window is split in two and read again,
/// and one that cannot be split fails the copy (never a silent loss). The first failure stops
/// the copy: the windows before it are synced, and an incremental copy resumes from them.
pub(super) fn copy_chunks(
    source: &Pipe,
    new_pipe: &Pipe,
    windows: Vec<DataQuery>,
    mut cap: Option<i64>,
    mut read: impl FnMut(&DataQuery) -> meerschaum_connector::ConnectorResult<Option<Frame>>,
    mut sync: impl FnMut(Frame) -> SuccessTuple,
) -> SuccessTuple {
    let label = |window: &DataQuery| {
        format!(
            "{} - {}",
            window
                .begin
                .as_ref()
                .map_or("None".into(), DateBound::py_str),
            window.end.as_ref().map_or("None".into(), DateBound::py_str)
        )
    };
    let mut queue: std::collections::VecDeque<DataQuery> = windows.into_iter().collect();
    let mut messages = Vec::new();
    let mut retries = meerschaum_connector::windows::ROW_LIMIT_RETRIES;
    while let Some(window) = queue.pop_front() {
        let synced = messages.len();
        let failed = |message: String| {
            SuccessTuple::fail(format!(
                "Failed to copy the rows {} into {new_pipe} ({synced} chunks synced before):\n{message}",
                label(&window),
            ))
        };
        let limited = DataQuery {
            limit: cap.or(window.limit),
            ..window.clone()
        };
        let df = match read(&limited) {
            Ok(Some(df)) => df,
            // With a row limit, a refused read (a server allowing fewer rows) answers nothing:
            // never taken for an empty chunk.
            Ok(None) if cap.is_some() => {
                return failed(format!(
                    "{source} answered no data (does its server allow reads of {} rows? \
                     `api:data:max_response_row_limit`)",
                    cap.unwrap_or_default()
                ));
            }
            Ok(None) => continue,
            Err(e) => {
                // A server that allows fewer rows names its own limit in its `413`: read with
                // that one (a few times at most).
                match (cap, meerschaum_connector::windows::refused_row_limit(&e)) {
                    (Some(asked), Some(allowed)) if allowed < asked && retries > 0 => {
                        cap = Some(allowed);
                        retries -= 1;
                        queue.push_front(window);
                        continue;
                    }
                    _ => return failed(e.to_string()),
                }
            }
        };
        if let Some(cap) = cap
            && i64::try_from(df.height()).unwrap_or(i64::MAX) >= cap
        {
            let middle = match (&window.begin, &window.end) {
                (Some(begin), Some(end)) => split_point(begin, end),
                _ => None,
            };
            match middle {
                Some(middle) => {
                    queue.push_front(DataQuery {
                        begin: Some(middle.clone()),
                        ..window.clone()
                    });
                    queue.push_front(DataQuery {
                        end: Some(middle),
                        ..window.clone()
                    });
                    continue;
                }
                None => {
                    return failed(format!(
                        "{source} answered {cap} rows, the most its server sends in one read \
                         (`api:data:max_response_row_limit`), and these rows cannot be read in \
                         smaller parts: the rest would be lost. Raise the server's limit, or \
                         copy from the instance it serves."
                    ));
                }
            }
        }
        if df.height() == 0 {
            continue;
        }
        let res = sync(df);
        if !res.success {
            return failed(res.message);
        }
        messages.push(format!("{}\n{}", label(&window), res.message));
    }
    if messages.is_empty() {
        return SuccessTuple::ok(format!("No rows to copy into {new_pipe}."));
    }
    // PARITY: v4's summary of a chunked sync (`Pipe.sync` of an iterator).
    SuccessTuple::ok(format!(
        "Synced {} chunk{} to {new_pipe}\n({} succeeded, 0 failed):\n\n{}",
        messages.len(),
        if messages.len() == 1 { "" } else { "s" },
        messages.len(),
        messages.join("\n\n")
    ))
}

/// `a < b` for two bounds on the same axis; an error for a datetime and an integer.
pub(super) fn bound_before(a: &DateBound, b: &DateBound, pipe: &Pipe) -> Result<bool, String> {
    match (a, b) {
        (DateBound::Datetime(x), DateBound::Datetime(y)) => Ok(x.utc < y.utc),
        (DateBound::Int(x), DateBound::Int(y)) => Ok(x < y),
        _ => Err(format!(
            "--begin / --end ({} and {}) do not match the datetime axis of {pipe}.",
            a.py_str(),
            b.py_str()
        )),
    }
}

/// The result of `copy pipes`: a failure when any pipe failed (or none was selected), with one
/// line per pipe that was not copied.
pub(super) fn copy_summary(
    dest: &str,
    copied: &[String],
    skipped: &[String],
    failed: &[(String, String)],
) -> SuccessTuple {
    let total = copied.len() + skipped.len() + failed.len();
    if total == 0 {
        return SuccessTuple::fail("No pipes were copied.");
    }
    if failed.is_empty() && skipped.is_empty() {
        // PARITY: v4's message when every pipe was copied.
        return SuccessTuple::ok(format!(
            "Copied {} pipe{}.",
            copied.len(),
            plural(copied.len())
        ));
    }
    let mut message = format!(
        "Copied {} of {total} pipe{} to '{dest}'",
        copied.len(),
        plural(total)
    );
    let mut parts = Vec::new();
    if !failed.is_empty() {
        parts.push(format!("{} failed", failed.len()));
    }
    if !skipped.is_empty() {
        parts.push(format!("{} skipped", skipped.len()));
    }
    message.push_str(&format!(" ({}).", parts.join(", ")));
    for (pipe, reason) in failed {
        let reason = reason.lines().next().unwrap_or_default();
        message.push_str(&format!("\n  - {pipe}: failed: {reason}"));
    }
    for pipe in skipped {
        message.push_str(&format!(
            "\n  - {pipe}: skipped (already on '{dest}'; pass --force to overwrite)"
        ));
    }
    SuccessTuple {
        success: failed.is_empty(),
        message,
    }
}
