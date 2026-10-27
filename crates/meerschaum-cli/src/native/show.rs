//! The instance-backed `show` subactions (`meerschaum/actions/show.py`): `pipes`, `connectors`,
//! `columns`, `rowcounts`, `data`, `users`, `sizes`, `partitions` and `tags`, plus `Pipe.show()`
//! and `pprint_pipe_columns`.

use meerschaum_connector::{DataQuery, DateBound, InstanceConnector, pipe_ops};
use meerschaum_core::SuccessTuple;
use meerschaum_core::pyfmt::{JsonStyle, json_dumps};
use meerschaum_core::value::{Map, Value};
use meerschaum_kernel::value::DateTimeValue;
use meerschaum_kernel::value::Value as KValue;

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::dataframe::{DisplayOptions, Fetched, format_dataframe_display};
use crate::format::progress::Progress;
use crate::format::rich::console::{Justify, Renderable};
use crate::format::rich::pretty::PyObj;
use crate::format::rich::table::{ASCII, Column, MINIMAL, Table};
use crate::format::rich::text::Text;
use crate::format::{Cell, Frame as TabFrame, OptionsArgs, TupleArgs, format_dataframe};
use crate::value::{ArgsMap, PyValue};

use super::pipes::{SelectedPipe, hydrate, pipe_keys, pipe_meta, pipe_state, selected_pipes};
use super::{Instance, flag, info, pprint, print_tuple, session, string, thousands, warn, words};

/// `json.dumps(value)`.
pub fn dumps(v: &Value) -> String {
    json_dumps(v, JsonStyle::Default, true)
}

/// `pipe.attributes` (the cached `get_pipes` attributes or the instance document).
pub fn pipe_attributes(rt: &mut Runtime<'_>, inst: &Instance, sp: &mut SelectedPipe) -> Map {
    hydrate(rt, inst, sp);
    let mut m = Map::new();
    m.insert(
        "connector_keys".into(),
        Value::Str(sp.pipe.connector_keys().into()),
    );
    m.insert("metric_key".into(), Value::Str(sp.pipe.metric_key().into()));
    m.insert(
        "location_key".into(),
        sp.pipe
            .location_key()
            .map(|l| Value::Str(l.into()))
            .unwrap_or(Value::Null),
    );
    m.insert(
        "parameters".into(),
        Value::Map(sp.pipe.raw_parameters().clone()),
    );
    if let Some(id) = &sp.id {
        m.insert("pipe_id".into(), id.to_value());
    }
    m
}

/// `Pipe.show(nopretty=...)`.
pub fn pipe_show(
    rt: &mut Runtime<'_>,
    inst: &Instance,
    sp: &mut SelectedPipe,
    nopretty: bool,
) -> ActionResult {
    let attrs = Value::Map(pipe_attributes(rt, inst, sp));
    if nopretty {
        rt.print(&format!("{}\n", dumps(&attrs)));
        return Ok(SuccessTuple::success());
    }
    let to_print = format!("Attributes for {}:", sp.pipe);
    let text = rt.with_formatter(|f| {
        if f.settings.ansi {
            let header = f.fill_ansi(&f.highlight_pipes(&f.make_header(&to_print)), "magenta");
            format!("{header}\n{}", super::rich_json::print_json(f, &attrs))
        } else {
            format!("{to_print}\n")
        }
    });
    rt.print(&text);
    Ok(SuccessTuple::success())
}

/// One line for each of `pipes` whose latest recorded sync failed (rc15 sync status), e.g.
/// `Pipe('sql:main', 'weather') failed its last 3 syncs (latest 2026-10-27 12:00:05 UTC): no
/// such table: raw`. Nothing when every latest sync succeeded or the statuses cannot be read.
fn failing_pipes_note(rt: &mut Runtime<'_>, inst: &Instance, pipes: &[SelectedPipe]) -> String {
    let Some(instance_keys) = pipes.first().map(|p| p.pipe.instance_keys().to_string()) else {
        return String::new();
    };
    let conn = inst.conn();
    let read = session(rt).block_on(async { conn.get_sync_statuses(&instance_keys).await });
    let statuses = match read {
        Ok(statuses) => statuses,
        Err(e) => {
            tracing::debug!("Cannot read the sync statuses on '{instance_keys}': {e}");
            return String::new();
        }
    };
    let failing: std::collections::HashMap<&str, _> = statuses
        .iter()
        .filter(|s| !s.success && s.last_attempt.is_some())
        .map(|s| (s.pipe_ref.as_str(), s))
        .collect();
    let mut lines = String::new();
    for sp in pipes {
        let Some(status) = failing.get(sp.pipe.pipe_ref().as_str()) else {
            continue;
        };
        let syncs = match status.consecutive_failures {
            0 | 1 => "its last sync".to_string(),
            n => format!("its last {n} syncs"),
        };
        let latest = status
            .last_failure
            .or(status.last_attempt)
            .map(|t| format!(" (latest {})", t.format("%Y-%m-%d %H:%M:%S UTC")))
            .unwrap_or_default();
        let mut reason: String = status
            .message
            .lines()
            .find(|l| !l.trim().is_empty())
            .unwrap_or_default()
            .trim()
            .chars()
            .take(160)
            .collect();
        if !reason.is_empty() {
            reason.insert_str(0, ": ");
        }
        lines.push_str(&format!("{} failed {syncs}{latest}{reason}\n", sp.pipe));
    }
    if lines.is_empty() {
        return lines;
    }
    rt.with_formatter(|f| {
        if f.settings.ansi {
            format!("\n{}", f.fill_ansi(&lines, "yellow"))
        } else {
            format!("\n{lines}")
        }
    })
}

/// `show pipes`. The pretty output ends with a line for each pipe whose latest sync failed.
pub fn show_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::ok("No pipes to show."));
    }
    if pipes.len() == 1 {
        let shown = pipe_show(rt, &inst, &mut pipes[0], nopretty);
        if !nopretty && shown.is_ok() {
            let note = failing_pipes_note(rt, &inst, &pipes);
            rt.print(&note);
        }
        return shown;
    }
    if !nopretty {
        let keys: Vec<_> = pipes.iter().map(|p| pipe_keys(&p.pipe)).collect();
        let text = rt.with_formatter(|f| f.pprint_pipes(&keys));
        rt.print(&text);
        let note = failing_pipes_note(rt, &inst, &pipes);
        rt.print(&note);
    } else {
        let mut out = String::new();
        for p in &pipes {
            out.push_str(&dumps(&Value::Map(pipe_state(&p.pipe))));
            out.push('\n');
        }
        rt.print(&out);
    }
    Ok(SuccessTuple::success())
}

/// `show connectors [type[:label]]`.
pub fn show_connectors(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    let conn_type = action
        .first()
        .map(|a| a.split(':').next().unwrap_or_default().to_string());
    let mut out = String::new();
    let session = session(rt);
    let text = rt.with_formatter(|f| {
        if !nopretty {
            let label = match &conn_type {
                Some(t) => format!("'{t}' "),
                None => String::new(),
            };
            out.push_str(&f.make_header(&format!("\nConfigured {label}Connectors:")));
            out.push('\n');
        }
        let mut keys = vec!["meerschaum", "connectors"];
        if let Some(t) = &conn_type {
            keys.push(t.as_str());
        }
        let cfg = f.config().get(&keys).unwrap_or(Value::Null);
        out.push_str(&f.pprint(&PyObj::from_value(&cfg), nopretty));
        if !nopretty && conn_type.is_none() {
            out.push_str(&f.make_header("\nActive connectors:"));
            out.push('\n');
            // `connectors`: `{type: {label: connector}}` of the connectors built so far.
            let mut active: Vec<(PyObj, PyObj)> = Vec::new();
            for (typ, label) in session.active() {
                let pos = active
                    .iter()
                    .position(|(k, _)| matches!(k, PyObj::Str(s) if *s == typ));
                let entry = PyObj::Other(format!("{typ}:{label}"));
                match pos {
                    Some(i) => {
                        if let PyObj::Dict(d) = &mut active[i].1 {
                            d.push((PyObj::Str(label), entry));
                        }
                    }
                    None => active.push((
                        PyObj::Str(typ.clone()),
                        PyObj::Dict(vec![(PyObj::Str(label), entry)]),
                    )),
                }
            }
            out.push_str(&f.pprint(&PyObj::Dict(active), nopretty));
        }
        if let Some(first) = action.first()
            && first.contains(':')
        {
            let (typ, label) = super::session::split_keys(first);
            let attrs = super::session::connector_attributes(f.config(), &typ, &label);
            if !attrs.is_empty() {
                if !nopretty {
                    out.push_str(
                        &f.make_header(&format!("\nAttributes for connector '{typ}:{label}':")),
                    );
                    out.push('\n');
                }
                out.push_str(&f.pprint(&PyObj::from_value(&Value::Map(attrs)), nopretty));
            }
        }
        std::mem::take(&mut out)
    });
    rt.print(&text);
    Ok(SuccessTuple::success())
}

fn query_from_args(args: &ArgsMap) -> DataQuery {
    DataQuery {
        begin: bound(args, "begin"),
        end: bound(args, "end"),
        params: super::params(args),
        ..Default::default()
    }
}

/// A `--begin` / `--end` argument as a bound.
pub fn bound(args: &ArgsMap, key: &str) -> Option<DateBound> {
    match args.get(key) {
        Some(PyValue::DateTime(dt)) => Some(DateBound::utc(*dt)),
        Some(PyValue::Int(i)) => i64::try_from(*i).ok().map(DateBound::Int),
        _ => None,
    }
}

/// `str(begin)` for a parsed bound (`2024-01-01 00:00:00+00:00`).
pub fn bound_str(args: &ArgsMap, key: &str) -> Option<String> {
    match args.get(key) {
        Some(PyValue::None) | None => None,
        Some(v) => Some(v.py_str()),
    }
}

/// `show rowcounts [remote]`.
pub fn show_rowcounts(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let remote = action.first().is_some_and(|a| a == "remote");
    let (inst, pipes) = selected_pipes(rt, args)?;
    let session = session(rt);
    let conn = inst.conn();
    let mut query = query_from_args(args);
    query.remote = remote;
    let conn = &conn;
    let query = &query;
    let progress = Progress::start(
        rt,
        args,
        if remote {
            "Counting remote rows"
        } else {
            "Counting rows"
        },
    );
    progress.set_total(pipes.len());
    let counter = progress.counter();
    let counter = &counter;
    let mut rcs = session.block_on(super::metadata::ordered(
        pipes.iter().collect(),
        super::metadata::concurrency(&inst, args),
        |p| async move {
            // PARITY: `Pipe.get_rowcount()` returns 0 when the connector returns `None`.
            let rc = conn
                .get_pipe_rowcount(&p.pipe, query)
                .await
                .ok()
                .map(|rc| rc.unwrap_or(0));
            counter.inc();
            (p.pipe.to_string(), rc)
        },
    ));
    drop(progress);
    // Sort descending (largest pipes first); `None` is the smallest. Python's sort is stable.
    rcs.sort_by(|a, b| {
        let ka = (a.1.is_some(), a.1.unwrap_or(0));
        let kb = (b.1.is_some(), b.1.unwrap_or(0));
        kb.cmp(&ka)
    });
    let msgs: Vec<String> = rcs
        .iter()
        .map(|(p, rc)| format!("{p}\n{}\n", rc.map(thousands).unwrap_or_default()))
        .collect();
    let header = if remote {
        "Remote row-counts:"
    } else {
        "Pipe row-counts:"
    };
    let opts = OptionsArgs {
        header: Some(header.into()),
        nopretty: flag(args, "nopretty"),
        ..OptionsArgs::default()
    };
    let text = rt.with_formatter(|f| f.print_options(&msgs, &opts));
    rt.print(&text);
    Ok(SuccessTuple::success())
}

/// A kernel frame as the `pandas` frame `format_dataframe` prints.
pub fn tab_frame(df: &meerschaum_kernel::Frame) -> TabFrame {
    TabFrame {
        columns: df
            .columns()
            .iter()
            .map(|c| {
                let cells = c
                    .data
                    .values()
                    .into_iter()
                    .map(|v| match v {
                        v if v.is_na() => Cell::Null,
                        KValue::Bool(b) => Cell::Bool(b),
                        KValue::Int(i) => Cell::Int(i),
                        KValue::Float(f) => Cell::Float(f),
                        KValue::Str(s) => Cell::Str(s),
                        other => Cell::Str(other.py_str()),
                    })
                    .collect();
                (c.name.clone(), cells)
            })
            .collect(),
    }
}

/// `float(x)` formatting as Python's `str(float)`.
fn py_float(f: f64) -> String {
    meerschaum_core::pyfmt::float_repr(f)
}

/// How `show data` reads each pipe.
#[derive(Clone)]
struct DataPlan {
    /// The window of the last N minutes (`show data 60`, and the 1440-minute default).
    backtrack: Option<f64>,
    begin: Option<DateBound>,
    end: Option<DateBound>,
    params: Option<Map>,
    /// Pretty mode with truncation: the limits (`None`: read every row).
    limits: Option<DisplayOptions>,
}

/// The rows a read returned: the kernel frame (whole reads, which `--nopretty` serializes) or
/// the cells of the head and tail of a limited read.
enum Rows {
    Whole(meerschaum_kernel::Frame),
    Cells(TabFrame),
}

impl Rows {
    fn cells(&self) -> TabFrame {
        match self {
            Rows::Whole(df) => tab_frame(df),
            Rows::Cells(cells) => cells.clone(),
        }
    }
}

/// One pipe's rows as `show data` prints them.
struct DataRead {
    frame: Rows,
    /// The true number of rows in the window.
    total: usize,
    fetched: Fetched,
}

/// `begin` of the backtrack window: the newest value of the axis minus `minutes`.
fn backtrack_begin(sync_time: DateBound, minutes: i64) -> DateBound {
    match sync_time {
        DateBound::Int(i) => DateBound::Int(i - minutes),
        DateBound::Datetime(dt) => DateBound::Datetime(DateTimeValue {
            utc: dt.utc - chrono::Duration::minutes(minutes),
            tz: dt.tz,
        }),
    }
}

/// The rows of `tail` appended to `head` by column name.
fn append_rows(head: TabFrame, tail: &TabFrame) -> TabFrame {
    TabFrame {
        columns: head
            .columns
            .into_iter()
            .map(|(name, mut cells)| {
                if let Some((_, more)) = tail.columns.iter().find(|(n, _)| *n == name) {
                    cells.extend(more.iter().cloned());
                }
                (name, cells)
            })
            .collect(),
    }
}

/// Whether `get_pipe_data` orders the rows by the pipe's datetime axis: the axis is the
/// `columns:datetime` column, else the datetime column v4's `guess_datetime` finds in the dtypes
/// (none when `dtypes:datetime` is null), it exists in the table, and the read emits an `ORDER BY`
/// at all (it needs at least one existing `columns` entry; v4 skips the clause otherwise, and a
/// "tail" of unordered rows is arbitrary).
fn orders_by_datetime(
    columns: &Map,
    dtypes_datetime_is_null: bool,
    dtypes: &indexmap::IndexMap<String, String>,
) -> bool {
    let named = |role: &str| {
        columns
            .get(role)
            .filter(|v| v.truthy())
            .map(meerschaum_core::pyfmt::py_str)
    };
    let axis = match named("datetime") {
        Some(column) => Some(column),
        None if dtypes_datetime_is_null => None,
        None => dtypes
            .iter()
            .find(|(_, t)| t.starts_with("datetime"))
            .map(|(c, _)| c.clone()),
    };
    let Some(axis) = axis else {
        return false;
    };
    dtypes.contains_key(&axis)
        && columns
            .values()
            .filter(|v| v.truthy())
            .any(|v| dtypes.contains_key(&meerschaum_core::pyfmt::py_str(v)))
}

/// Whether the pipe has a datetime axis the rows can be ordered by (see [`orders_by_datetime`]).
async fn has_datetime_axis(conn: &dyn InstanceConnector, pipe: &meerschaum_core::Pipe) -> bool {
    let Ok(dtypes) = pipe_ops::get_dtypes(conn, pipe, pipe.parameters(), true).await else {
        return false;
    };
    let null_datetime = matches!(pipe.dtypes().get("datetime"), Some(Value::Null));
    orders_by_datetime(&pipe.columns(), null_datetime, &dtypes)
}

/// The head and tail of a window that has more rows than the view shows, or `None` to read it
/// whole (a short window, an empty or missing table, or a connector that ignores `limit`).
async fn read_limited(
    conn: &dyn InstanceConnector,
    pipe: &meerschaum_core::Pipe,
    plan: &DataPlan,
    opts: &DisplayOptions,
) -> Option<DataRead> {
    let mut window = DataQuery {
        begin: plan.begin.clone(),
        end: plan.end.clone(),
        params: plan.params.clone(),
        ..Default::default()
    };
    if let Some(minutes) = plan.backtrack {
        // `Pipe.get_backtrack_data`: everything newer than the sync time minus the backtrack.
        let sync_time = conn
            .get_sync_time(pipe, plan.params.as_ref(), true)
            .await
            .ok()??;
        window.begin = Some(backtrack_begin(sync_time, minutes as i64));
        window.end = None;
    }
    let total = usize::try_from(conn.get_pipe_rowcount(pipe, &window).await.ok()??).ok()?;
    if !opts.truncates(total) {
        return None;
    }
    let (head_n, tail_n) = opts.head_tail();
    let ordered = has_datetime_axis(conn, pipe).await;
    // Without a datetime axis there is no cheap tail: the first rows and the count.
    let head_limit = if ordered { head_n } else { head_n + tail_n };
    let read = |limit: usize, order: &str| {
        let query = DataQuery {
            limit: Some(limit as i64),
            order: Some(order.into()),
            ..window.clone()
        };
        async move { conn.get_pipe_data(pipe, &query).await.ok().flatten() }
    };
    // The rows keep the orientation of an unlimited read: a backtrack window is newest first
    // (`Pipe.get_backtrack_data` orders descending), the rest oldest first.
    let (first, last) = if plan.backtrack.is_some() && ordered {
        ("desc", "asc")
    } else {
        ("asc", "desc")
    };
    let head = read(head_limit, first).await?;
    if head.height() > head_limit {
        return None;
    }
    let head = tab_frame(&head);
    if !ordered {
        return Some(DataRead {
            frame: Rows::Cells(head),
            total,
            fetched: Fetched::HeadOnly,
        });
    }
    let tail = read(tail_n, last).await?;
    if tail.height() > tail_n {
        return None;
    }
    // The tail was read from the far end: reverse it into the orientation of the head.
    let newest_last = tail.take(&(0..tail.height()).rev().collect::<Vec<_>>());
    let head_rows = head.num_rows();
    Some(DataRead {
        frame: Rows::Cells(append_rows(head, &tab_frame(&newest_last))),
        total,
        fetched: Fetched::HeadTail { head: head_rows },
    })
}

/// One pipe's rows for `show data`: limited in pretty mode when the window is long, else whole.
async fn read_pipe_data(
    conn: &dyn InstanceConnector,
    pipe: &meerschaum_core::Pipe,
    plan: &DataPlan,
) -> Option<DataRead> {
    if let Some(opts) = &plan.limits
        && let Some(read) = read_limited(conn, pipe, plan, opts).await
    {
        return Some(read);
    }
    let df = match plan.backtrack {
        Some(m) => {
            conn.get_backtrack_data(
                pipe,
                m as i64,
                &DataQuery {
                    params: plan.params.clone(),
                    ..Default::default()
                },
            )
            .await
        }
        None => {
            conn.get_pipe_data(
                pipe,
                &DataQuery {
                    begin: plan.begin.clone(),
                    end: plan.end.clone(),
                    params: plan.params.clone(),
                    ..Default::default()
                },
            )
            .await
        }
    };
    let df = df.ok().flatten()?;
    Some(DataRead {
        total: df.height(),
        frame: Rows::Whole(df),
        fetched: Fetched::All,
    })
}

/// The pipe with the parameters registered on the instance (`Pipe.attributes`), read inside the
/// concurrent per-pipe future.
async fn hydrated_pipe(conn: &dyn InstanceConnector, sp: &SelectedPipe) -> meerschaum_core::Pipe {
    let mut pipe = sp.pipe.clone();
    if sp.hydrated {
        return pipe;
    }
    if let Ok(attrs) = conn.get_pipe_attributes(&pipe).await
        && let Some(registered) = attrs.get("parameters").and_then(Value::as_map)
    {
        let mut params = registered.clone();
        for (k, v) in pipe.raw_parameters() {
            params.insert(k.clone(), v.clone());
        }
        pipe.set_parameters(params);
    }
    pipe
}

/// The terminal width the table has to fit: the terminal of stdout, else `$COLUMNS`; a pipe or a
/// file gets every column.
fn display_width(rt: &Runtime<'_>) -> Option<usize> {
    crate::format::terminal_width(1).or_else(|| {
        (rt.env)("COLUMNS")
            .and_then(|c| c.trim().parse::<usize>().ok())
            .filter(|w| *w > 0)
    })
}

/// `show data [minutes|all]`.
///
/// In pretty mode the output is truncated like a pandas frame (`formatting:dataframe:*`, see
/// `format::dataframe`) and only the rows shown are read; `--all` and `--nopretty` print
/// everything. The pipes are read concurrently (`--workers`) and printed in their order.
pub fn show_data(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    let begin = bound(args, "begin");
    let end = bound(args, "end");
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    let session = session(rt);
    let conn = inst.conn();
    // PARITY: `float(action[0])` is a float (`show data 60` prints `Last 60.0 minutes`), while the
    // default is the int `1440` (`Last 1440 minutes`) (`actions/show.py::_show_data`).
    let mut default_backtrack = false;
    let backtrack_minutes: Option<f64> =
        match action.first().and_then(|a| a.trim().parse::<f64>().ok()) {
            Some(m) => Some(m),
            None => {
                if begin.is_none()
                    && end.is_none()
                    && (action.is_empty() || action.first().is_some_and(|a| a != "all"))
                {
                    default_backtrack = true;
                    Some(1440.0)
                } else {
                    None
                }
            }
        };
    let limits = (!nopretty && !flag(args, "all")).then(|| {
        let width = display_width(rt);
        rt.with_formatter(|f| DisplayOptions::from_config(f.config(), width))
    });
    let plan = DataPlan {
        backtrack: backtrack_minutes,
        begin: begin.clone(),
        end: end.clone(),
        params: super::params(args),
        limits: limits.clone(),
    };
    let concurrency = super::metadata::concurrency(&inst, args);
    let progress = Progress::start(rt, args, "Reading data");
    progress.set_total(pipes.len());
    let counter = progress.counter();
    let (conn, plan, counter) = (&conn, &plan, &counter);
    // Whole tables are read a few pipes at a time so memory stays bounded; limited reads are
    // small and all go at once.
    let batch = if limits.is_some() {
        pipes.len().max(1)
    } else {
        concurrency.max(1)
    };
    for chunk in pipes.chunks_mut(batch) {
        let reads = session.block_on(super::metadata::ordered(
            chunk.iter().collect(),
            concurrency,
            |sp| async move {
                let pipe = hydrated_pipe(conn.as_ref(), sp).await;
                let read = read_pipe_data(conn.as_ref(), &pipe, plan).await;
                counter.inc();
                read
            },
        ));
        for (p, read) in chunk.iter().zip(reads) {
            let Some(read) = read else {
                warn(rt, &format!("Failed to fetch data for {}.", p.pipe));
                continue;
            };
            let info_msg = match backtrack_minutes {
                Some(m) => format!(
                    "Last {} minute{} of data for {}:",
                    if default_backtrack {
                        (m as i64).to_string()
                    } else {
                        py_float(m)
                    },
                    if m != 1.0 { "s" } else { "" },
                    p.pipe
                ),
                None => format!(
                    "Data for {}{}{}:",
                    p.pipe,
                    bound_str(args, "begin")
                        .map(|b| format!(" from {b}"))
                        .unwrap_or_default(),
                    bound_str(args, "end")
                        .map(|e| format!(" to {e}"))
                        .unwrap_or_default(),
                ),
            };
            if !nopretty {
                info(rt, &info_msg);
                let cells = read.frame.cells();
                let text = match &limits {
                    Some(opts) => format_dataframe_display(&cells, read.total, read.fetched, opts),
                    None => format_dataframe(&cells, None),
                };
                rt.print(&format!("{text}\n"));
            } else {
                rt.print(&format!("{}\n", dumps(&Value::Map(pipe_state(&p.pipe)))));
                let Rows::Whole(df) = &read.frame else {
                    unreachable!("--nopretty reads whole tables");
                };
                let json = meerschaum_connector_api::wire::to_json(
                    df,
                    None,
                    &meerschaum_connector_api::wire::ToJsonOptions {
                        columns_orient: true,
                        ..Default::default()
                    },
                );
                pprint(rt, &PyObj::Str(json), true);
            }
        }
    }
    drop(progress);
    Ok(SuccessTuple::success())
}

/// What `pprint_pipe_columns` prints about one pipe, read from the instance.
struct ColumnsInfo {
    pipe: meerschaum_core::Pipe,
    cols: Map,
    cols_types: indexmap::IndexMap<String, String>,
    dtypes: indexmap::IndexMap<String, String>,
}

/// The registered columns, the table's column types and the resolved dtypes of a pipe.
async fn read_columns(conn: &dyn InstanceConnector, sp: &SelectedPipe) -> ColumnsInfo {
    let pipe = hydrated_pipe(conn, sp).await;
    let exists = conn.pipe_exists(&pipe).await.unwrap_or(false);
    let cols: Map = if exists { pipe.columns() } else { Map::new() };
    let cols_types = if exists {
        conn.get_pipe_columns_types(&pipe).await.unwrap_or_default()
    } else {
        Default::default()
    };
    let dtypes = pipe_ops::get_dtypes(conn, &pipe, pipe.parameters(), true)
        .await
        .unwrap_or_default();
    ColumnsInfo {
        pipe,
        cols,
        cols_types,
        dtypes,
    }
}

/// `pprint_pipe_columns(pipe, nopretty)`.
pub fn pprint_pipe_columns(
    rt: &mut Runtime<'_>,
    inst: &Instance,
    sp: &mut SelectedPipe,
    nopretty: bool,
) {
    let session = session(rt);
    let conn = inst.conn();
    let info = session.block_on(read_columns(conn.as_ref(), sp));
    print_columns(rt, info, nopretty);
}

/// Print what [`read_columns`] returned.
fn print_columns(rt: &mut Runtime<'_>, info: ColumnsInfo, nopretty: bool) {
    let ColumnsInfo {
        pipe,
        cols,
        cols_types,
        dtypes,
    } = info;
    if nopretty {
        let types_map: Map = cols_types
            .iter()
            .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
            .collect();
        let dtypes_map: Map = dtypes
            .iter()
            .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
            .collect();
        rt.print(&format!(
            "{}\n{}\n{}\n{}\n",
            dumps(&Value::Map(pipe_state(&pipe))),
            dumps(&Value::Map(cols.clone())),
            dumps(&Value::Map(types_map)),
            dumps(&Value::Map(dtypes_map)),
        ));
        return;
    }
    let header = rt.with_formatter(|f| f.make_header(&format!("\nIndex Columns for {pipe}:")));
    let text = rt.with_formatter(|f| f.info(&header, false));
    rt.print(&text);
    if !cols.is_empty() {
        pprint(rt, &PyObj::from_value(&Value::Map(cols)), false);
        rt.print("\n");
    } else {
        print_tuple(
            rt,
            &SuccessTuple::fail(format!("No registered columns for {pipe}.")),
            TupleArgs {
                calm: true,
                ..TupleArgs::default()
            },
        );
    }
    if dtypes.is_empty() {
        print_tuple(
            rt,
            &SuccessTuple::fail(format!("No table columns for {pipe}. Does the pipe exist?")),
            TupleArgs {
                calm: true,
                ..TupleArgs::default()
            },
        );
        return;
    }
    let text = rt.with_formatter(|f| {
        let ansi = f.settings.ansi;
        let style = |s: &str| if ansi { s.to_string() } else { String::new() };
        let mut table = Table::new();
        table.box_chars = Some(if f.settings.unicode { MINIMAL } else { ASCII });
        table.add_column(Column {
            header: Text::new("Column", style("white").as_str()),
            ..Column::default()
        });
        table.add_column(Column {
            header: Text::new("DB Type", style("cyan").as_str()),
            justify: Justify::Right,
            ..Column::default()
        });
        table.add_column(Column {
            header: Text::new("PD Type", style("yellow").as_str()),
            justify: Justify::Left,
            ..Column::default()
        });
        for (c, t) in &dtypes {
            let db = cols_types.get(c).cloned().unwrap_or_else(|| "None".into());
            table.add_row(vec![
                Some(Box::new(Text::new(c, style("white").as_str())) as Box<dyn Renderable>),
                Some(Box::new(Text::new(&db, style("cyan").as_str()))),
                Some(Box::new(Text::new(t, style("yellow").as_str()))),
            ]);
        }
        f.console().print(&table)
    });
    rt.print(&text);
}

/// `show columns`.
pub fn show_columns(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let (inst, pipes) = selected_pipes(rt, args)?;
    let session = session(rt);
    let conn = inst.conn();
    let progress = Progress::start(rt, args, "Reading columns");
    progress.set_total(pipes.len());
    let counter = progress.counter();
    let (conn, counter) = (&conn, &counter);
    // Read concurrently (`--workers`), print in the order of the pipes.
    let infos = session.block_on(super::metadata::ordered(
        pipes.iter().collect(),
        super::metadata::concurrency(&inst, args),
        |sp| async move {
            let info = read_columns(conn.as_ref(), sp).await;
            counter.inc();
            info
        },
    ));
    for info in infos {
        print_columns(rt, info, nopretty);
    }
    drop(progress);
    Ok(SuccessTuple::success())
}

/// `show users`.
pub fn show_users(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let inst = super::get_instance(args, rt)?;
    let session = session(rt);
    let users = match session.block_on(inst.conn().get_users()) {
        Ok(u) => u,
        Err(_) => {
            return Ok(SuccessTuple::fail(format!(
                "Failed to get users from instance '{}'",
                string(args, "mrsm_instance").unwrap_or_else(|| "None".into())
            )));
        }
    };
    let opts = OptionsArgs {
        header: Some(format!("Registered users for instance '{}':", inst.keys())),
        ..OptionsArgs::default()
    };
    let text = rt.with_formatter(|f| f.print_options(&users, &opts));
    rt.print(&text);
    Ok(SuccessTuple::success())
}

/// `format_bytes(num_bytes)`.
pub fn format_bytes(n: Option<i64>) -> String {
    meerschaum_connector_sql::instance::values::format_bytes(n)
}

/// The physical table behind a pipe: the database (flavor, host, port, database; never the
/// credentials), the schema and the target, so pipes that share a `target` are recognized.
/// A non-SQL instance is identified by its connector keys.
fn table_identity(inst: &Instance, pipe: &meerschaum_core::Pipe) -> String {
    match inst.sql() {
        Some(sql) => {
            let spec = sql.spec();
            let (target, schema) = match sql.sql_pipe(pipe) {
                Ok(sp) => {
                    let schema = sql.get_pipe_schema(&sp);
                    (sp.target.clone(), schema)
                }
                Err(_) => (pipe.target(None).unwrap_or_default(), None),
            };
            format!(
                "{}|{}|{}|{}|{}|{}",
                spec.flavor,
                spec.host.as_deref().unwrap_or_default(),
                spec.port.map(|p| p.to_string()).unwrap_or_default(),
                spec.database.as_deref().unwrap_or_default(),
                schema.unwrap_or_default(),
                target
            )
        }
        None => format!("{}|{}", inst.keys(), pipe.target(None).unwrap_or_default()),
    }
}

/// A pipe's table identity and its size in bytes.
async fn read_size(inst: &Instance, sp: &SelectedPipe) -> (String, Option<i64>) {
    let conn = inst.conn();
    let pipe = hydrated_pipe(conn.as_ref(), sp).await;
    let size = conn.get_pipe_size(&pipe).await.unwrap_or(None);
    (table_identity(inst, &pipe), size)
}

/// One row of `show sizes`.
struct SizeRow {
    pipe: SelectedPipe,
    size: Option<i64>,
    /// How many of the selected pipes share this pipe's table (1: not shared).
    sharers: usize,
}

/// `show sizes`.
///
/// Pipes whose `target` is the same physical table (same database, schema and table) show the
/// same size, are marked `shared xN`, and the total counts the table once.
pub fn show_sizes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let (inst, pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes selected."));
    }
    let session = session(rt);
    let mut sized: Vec<(SelectedPipe, String, Option<i64>)> = Vec::new();
    let inst_ref = &inst;
    let concurrency = super::metadata::concurrency(&inst, args);
    let progress = Progress::start(rt, args, "Measuring pipe sizes");
    progress.set_total(pipes.len());
    let counter = progress.counter();
    let counter = &counter;
    let mut fetched = (concurrency > 1).then(|| {
        session
            .block_on(super::metadata::ordered(
                pipes.iter().collect(),
                concurrency,
                |p| async move {
                    let read = read_size(inst_ref, p).await;
                    counter.inc();
                    read
                },
            ))
            .into_iter()
    });
    for (index, p) in pipes.into_iter().enumerate() {
        if fetched.is_none() {
            progress.set_done(index);
        }
        if !nopretty {
            info(rt, &format!("Calculating size for {}...", p.pipe));
        }
        let (identity, size) = match fetched.as_mut() {
            Some(results) => results.next().expect("one size per pipe"),
            None => session.block_on(read_size(&inst, &p)),
        };
        if !nopretty {
            info(rt, &format!("Size for {}: {}", p.pipe, format_bytes(size)));
        }
        sized.push((p, identity, size));
    }
    drop(progress);
    let mut sharing: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for (_, identity, _) in &sized {
        *sharing.entry(identity.as_str()).or_default() += 1;
    }
    // Each physical table counts once in the total.
    let mut counted = std::collections::HashSet::new();
    let total: i64 = sized
        .iter()
        .filter(|(_, identity, _)| counted.insert(identity.as_str()))
        .filter_map(|(_, _, size)| *size)
        .sum();
    let sharers: Vec<usize> = sized
        .iter()
        .map(|(_, id, _)| sharing[id.as_str()])
        .collect();
    let mut sizes: Vec<SizeRow> = sized
        .into_iter()
        .zip(sharers)
        .map(|((pipe, _, size), sharers)| SizeRow {
            pipe,
            size,
            sharers,
        })
        .collect();
    sizes.sort_by(|a, b| {
        let ka = (a.size.is_some(), a.size.unwrap_or(0));
        let kb = (b.size.is_some(), b.size.unwrap_or(0));
        kb.cmp(&ka)
    });
    let shared = |n: usize| (n > 1).then(|| format!("shared x{n}"));
    if nopretty {
        let mut out = String::new();
        for row in &sizes {
            out.push_str(&format!("{}\t{}", row.pipe.pipe, format_bytes(row.size)));
            if let Some(note) = shared(row.sharers) {
                out.push('\t');
                out.push_str(&note);
            }
            out.push('\n');
        }
        rt.print(&out);
        return Ok(SuccessTuple::success());
    }
    let text = rt.with_formatter(|f| {
        let rows: Vec<(String, String, String, Option<String>)> = sizes
            .iter()
            .map(|row| {
                (
                    row.pipe.pipe.to_string(),
                    f.pipe_repr(&pipe_keys(&row.pipe.pipe)),
                    format_bytes(row.size),
                    shared(row.sharers),
                )
            })
            .collect();
        let name_w = rows
            .iter()
            .map(|r| r.0.chars().count())
            .chain(std::iter::once(4))
            .max()
            .unwrap_or(4);
        let total_str = format_bytes(Some(total));
        let size_w = rows
            .iter()
            .map(|r| r.2.chars().count())
            .chain([4, total_str.chars().count()])
            .max()
            .unwrap_or(4);
        let header = format!("    {:<name_w$}  {:>size_w$}", "Pipe", "Size");
        let sep = format!("    {}", "-".repeat(header.chars().count() - 4));
        let mut lines = vec![header, sep.clone()];
        for (plain, hl, s, note) in &rows {
            let pad = " ".repeat(name_w - plain.chars().count());
            let note = note.as_ref().map(|n| format!("  {n}")).unwrap_or_default();
            lines.push(format!("    {hl}{pad}  {s:>size_w$}{note}"));
        }
        lines.push(sep);
        lines.push(format!("    {:<name_w$}  {:>size_w$}", "Total", total_str));
        format!("\n{}\n\n", lines.join("\n"))
    });
    rt.print(&text);
    Ok(SuccessTuple::success())
}

/// `interval_str(timedelta)` for a whole number of minutes.
pub fn interval_minutes_str(minutes: i64) -> String {
    super::sync::interval_str(minutes as f64 * 60.0)
}

fn partition_interval_str(interval: Option<&Value>) -> String {
    match interval {
        Some(Value::Map(m)) if m.contains_key("minutes") => {
            interval_minutes_str(m.get("minutes").and_then(Value::as_i64).unwrap_or(0))
        }
        Some(Value::Map(m)) if m.contains_key("seconds") => {
            super::sync::interval_str(m.get("seconds").and_then(Value::as_f64).unwrap_or(0.0))
        }
        Some(Value::Int(i)) => format!("{} (int axis)", thousands(*i)),
        _ => "-".to_string(),
    }
}

async fn partition_metadata(inst: &Instance, pipe: &meerschaum_core::Pipe) -> (Map, Option<i64>) {
    let info = match inst.sql() {
        Some(sql) => sql.get_partition_info_typed(pipe).await,
        None => Map::new(),
    };
    let rows = if info.get("partitioned").is_some_and(Value::truthy)
        && info
            .get("count")
            .and_then(Value::as_i64)
            .is_some_and(|count| count != 0)
    {
        inst.conn()
            .get_pipe_rowcount(pipe, &DataQuery::default())
            .await
            .ok()
            .flatten()
    } else {
        None
    };
    (info, rows)
}

/// `show partitions`.
pub fn show_partitions(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let (inst, pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes selected."));
    }
    let session = session(rt);
    struct Rec {
        pipe: meerschaum_core::Pipe,
        partitioned: bool,
        count: Option<i64>,
        interval: String,
        rpp: Option<i64>,
    }
    let mut records: Vec<Rec> = Vec::new();
    let concurrency = super::metadata::concurrency(&inst, args);
    let progress = Progress::start(rt, args, "Inspecting partitions");
    progress.set_total(pipes.len());
    let counter = progress.counter();
    let counter = &counter;
    let inst_ref = &inst;
    let mut fetched = (concurrency > 1).then(|| {
        session
            .block_on(super::metadata::ordered(
                pipes.iter().collect(),
                concurrency,
                |p| async move {
                    let result = partition_metadata(inst_ref, &p.pipe).await;
                    counter.inc();
                    result
                },
            ))
            .into_iter()
    });
    for (index, p) in pipes.into_iter().enumerate() {
        if fetched.is_none() {
            progress.set_done(index);
        }
        if !nopretty {
            info(rt, &format!("Inspecting partitions for {}...", p.pipe));
        }
        let (pinfo, rows) = match fetched.as_mut() {
            Some(results) => results.next().expect("one partition result per pipe"),
            None => session.block_on(partition_metadata(&inst, &p.pipe)),
        };
        let partitioned = pinfo.get("partitioned").is_some_and(Value::truthy);
        let count = pinfo.get("count").and_then(Value::as_i64);
        let interval = partition_interval_str(pinfo.get("interval"));
        let mut rpp = None;
        if partitioned
            && let Some(c) = count.filter(|c| *c != 0)
            && let Some(rc) = rows
        {
            rpp = Some(py_round_div(rc, c));
        }
        records.push(Rec {
            pipe: p.pipe,
            partitioned,
            count,
            interval,
            rpp,
        });
    }
    drop(progress);
    records.sort_by(|a, b| {
        (b.partitioned, b.count.unwrap_or(0)).cmp(&(a.partitioned, a.count.unwrap_or(0)))
    });
    let count_str = |r: &Rec| r.count.map(thousands).unwrap_or_else(|| "-".into());
    let rpp_str = |r: &Rec| r.rpp.map(thousands).unwrap_or_else(|| "-".into());
    if nopretty {
        let mut out = String::new();
        for r in &records {
            out.push_str(&format!(
                "{}\t{}\t{}\t{}\n",
                r.pipe,
                count_str(r),
                r.interval,
                rpp_str(r)
            ));
        }
        rt.print(&out);
        return Ok(SuccessTuple::success());
    }
    let text = rt.with_formatter(|f| {
        let rows: Vec<(String, String, String, String, String)> = records
            .iter()
            .map(|r| {
                (
                    r.pipe.to_string(),
                    f.pipe_repr(&pipe_keys(&r.pipe)),
                    count_str(r),
                    r.interval.clone(),
                    rpp_str(r),
                )
            })
            .collect();
        #[allow(clippy::type_complexity)]
        let w = |f: &dyn Fn(&(String, String, String, String, String)) -> usize, min: usize| {
            rows.iter()
                .map(f)
                .chain(std::iter::once(min))
                .max()
                .unwrap_or(min)
        };
        let name_w = w(&|r| r.0.chars().count(), 4);
        let parts_w = w(&|r| r.2.chars().count(), 10);
        let interval_w = w(&|r| r.3.chars().count(), 8);
        let rpp_w = w(&|r| r.4.chars().count(), 9);
        let header = format!(
            "    {:<name_w$}  {:>parts_w$}  {:>interval_w$}  {:>rpp_w$}",
            "Pipe", "Partitions", "Interval", "Rows/Part"
        );
        let sep = format!("    {}", "-".repeat(header.chars().count() - 4));
        let mut lines = vec![header, sep];
        for (plain, hl, c, i, r) in &rows {
            let pad = " ".repeat(name_w - plain.chars().count());
            lines.push(format!(
                "    {hl}{pad}  {c:>parts_w$}  {i:>interval_w$}  {r:>rpp_w$}"
            ));
        }
        format!("\n{}\n\n", lines.join("\n"))
    });
    rt.print(&text);
    Ok(SuccessTuple::success())
}

/// Python's `round(a / b)` (half to even).
fn py_round_div(a: i64, b: i64) -> i64 {
    let x = a as f64 / b as f64;
    let r = x.round();
    if (x - x.trunc()).abs() == 0.5 {
        let t = x.trunc();
        if (t as i64) % 2 == 0 {
            t as i64
        } else {
            r as i64
        }
    } else {
        r as i64
    }
}

/// `show tags [tag ...]`.
pub fn show_tags(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    let mut tags = action.clone();
    tags.extend(words(args, "tags"));
    let inst = super::get_instance(args, rt)?;
    let mut sel = super::pipes::Selection::from_args(args);
    sel.tags = tags;
    let pipes = super::pipes::get_pipes(rt, &inst, &sel)?;
    // `as_tags_dict=True`: `{tag: [pipes]}` in first-seen order.
    let mut tags_pipes: Vec<(String, Vec<meerschaum_core::Pipe>)> = Vec::new();
    for p in &pipes {
        for tag in p.pipe.tags_list() {
            match tags_pipes.iter().position(|(t, _)| *t == tag) {
                Some(i) => tags_pipes[i].1.push(p.pipe.clone()),
                None => tags_pipes.push((tag, vec![p.pipe.clone()])),
            }
        }
    }
    if !action.is_empty() {
        tags_pipes.retain(|(t, _)| action.contains(t));
    }
    if tags_pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes have been tagged."));
    }
    if nopretty {
        let mut out = String::new();
        for (tag, ps) in &tags_pipes {
            out.push_str(tag);
            out.push('\n');
            for p in ps {
                out.push_str(&dumps(&Value::Map(pipe_meta(p))));
                out.push('\n');
            }
        }
        rt.print(&out);
        return Ok(SuccessTuple::success());
    }
    let mut sorted = tags_pipes.clone();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let text = rt.with_formatter(|f| {
        let groups: Vec<(String, Vec<crate::format::PipeKeys>)> = sorted
            .iter()
            .map(|(t, ps)| (t.clone(), ps.iter().map(pipe_keys).collect()))
            .collect();
        tags_columns(f, &groups)
    });
    rt.print(&text);
    Ok(SuccessTuple::success())
}

/// `Columns([Panel(Group(*pipe_texts), title=tag_text, title_align='left') ...], equal=True)`.
fn tags_columns(
    f: &crate::format::Formatter<'_>,
    groups: &[(String, Vec<crate::format::PipeKeys>)],
) -> String {
    use crate::format::rich::panel::{Columns, Panel};
    let unicode = f.settings.unicode;
    let ansi = f.settings.ansi;
    let prefix = if unicode {
        f.formatting_str(&["pipes", "unicode", "icons", "tag"])
    } else {
        String::new()
    };
    let tag_style = if ansi {
        f.formatting_str(&["pipes", "ansi", "styles", "tags"])
    } else {
        String::new()
    };
    let mut panels: Vec<Box<dyn Renderable>> = Vec::new();
    for (tag, keys) in groups {
        let title = Text::plain_text(&prefix).plus(&Text::new(tag, tag_style.as_str()));
        let texts: Vec<Box<dyn Renderable>> = keys
            .iter()
            .map(|k| Box::new(f.pipe_repr_text(k)) as Box<dyn Renderable>)
            .collect();
        let mut panel = Panel::new(Box::new(Group { renderables: texts }));
        panel.title = Some(title);
        panel.title_align = "left";
        panels.push(Box::new(panel));
    }
    f.console().print(&Columns {
        renderables: panels,
        equal: true,
    })
}

/// `rich.console.Group(*renderables)`.
pub struct Group<'a> {
    /// The renderables, rendered one after another.
    pub renderables: Vec<Box<dyn Renderable + 'a>>,
}

impl Renderable for Group<'_> {
    fn render(
        &self,
        console: &crate::format::rich::console::Console,
        options: &crate::format::rich::console::RenderOptions,
    ) -> Vec<crate::format::rich::segment::Segment> {
        let mut out = Vec::new();
        for r in &self.renderables {
            out.extend(console.render(r.as_ref(), options));
        }
        out
    }

    fn measure(
        &self,
        console: &crate::format::rich::console::Console,
        options: &crate::format::rich::console::RenderOptions,
    ) -> crate::format::rich::console::Measurement {
        let ms: Vec<_> = self
            .renderables
            .iter()
            .map(|r| crate::format::rich::console::Measurement::get(console, options, r.as_ref()))
            .collect();
        crate::format::rich::console::Measurement::new(
            ms.iter().map(|m| m.minimum).max().unwrap_or(0),
            ms.iter().map(|m| m.maximum).max().unwrap_or(0),
        )
    }
}

/// Fail with v4's error when the instance raised.
pub fn connector_error(e: impl std::fmt::Display) -> ActionError {
    ActionError::new("Exception", e.to_string())
}

/// `parse_repo_keys(repository)`: `meerschaum:repository` by default, `api:` when untyped.
pub fn repo_keys(args: &ArgsMap, rt: &mut Runtime<'_>) -> Result<String, ActionError> {
    let repo = super::string(args, "repository").unwrap_or_else(|| {
        rt.config()
            .get(&["meerschaum", "repository"])
            .map(|v| v.py_str())
            .unwrap_or_else(|| "api:mrsm".into())
    });
    let keys = if repo.contains(':') {
        repo
    } else {
        format!("api:{repo}")
    };
    if !keys.starts_with("api:") {
        return Err(ActionError::new(
            "ValueError",
            "Only APIConnectors may be treated as repositories.",
        ));
    }
    Ok(keys)
}

/// `show plugins all` / `show plugins <username>`: the plugins registered on the repository.
pub fn show_plugins_repo(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let keys = repo_keys(args, rt)?;
    let session = session(rt);
    let progress = Progress::start(rt, args, format!("Fetching plugins from '{keys}'"));
    let repo = session.instance(&keys, rt.config())?;
    let conn = repo.conn();
    let (names, header) = if action[0] == "all" {
        let names = session
            .block_on(conn.get_plugins(None, None))
            .map_err(connector_error)?;
        (
            names,
            format!("Available plugins from Meerschaum repository '{keys}':"),
        )
    } else {
        let username = &action[0];
        let user = meerschaum_core::users::User::new(username, Some(""));
        let user_id = session
            .block_on(conn.get_user_id(&user))
            .map_err(connector_error)?;
        let names = session
            .block_on(conn.get_plugins(user_id.as_ref(), None))
            .map_err(connector_error)?;
        (
            names,
            format!("Plugins from user '{username}' at Meerschaum repository '{keys}':"),
        )
    };
    drop(progress);
    let opts = OptionsArgs {
        header: Some(header),
        nopretty: flag(args, "nopretty"),
        ..OptionsArgs::default()
    };
    let text = rt.with_formatter(|f| f.print_options(&names, &opts));
    rt.print(&text);
    Ok(SuccessTuple::success())
}

#[cfg(test)]
mod partition_interval_tests {
    use super::*;

    #[test]
    fn datetime_partition_duration_and_integer_axis_have_distinct_labels() {
        let day = Value::Map(Map::from_iter([("seconds".into(), Value::Float(86400.0))]));
        assert_eq!(partition_interval_str(Some(&day)), "1 day");
        assert_eq!(
            partition_interval_str(Some(&Value::Int(1440))),
            "1,440 (int axis)"
        );
        assert_eq!(partition_interval_str(None), "-");
    }
}

#[cfg(test)]
mod limited_read_tests {
    use super::*;
    use meerschaum_connector_sql::uri::{ConnectionSpec, ConnectorAttributes};
    use meerschaum_connector_sql::{SqlConnector, flavor::Flavor};
    use std::sync::Arc;

    async fn connector(dir: &std::path::Path, rows: usize) -> Arc<SqlConnector> {
        let attrs = ConnectorAttributes {
            flavor: Some(Flavor::Sqlite),
            database: Some(dir.join("show.db").to_string_lossy().into_owned()),
            ..Default::default()
        };
        let sql = Arc::new(
            SqlConnector::connect("show", ConnectionSpec::from_attributes(&attrs).unwrap())
                .await
                .unwrap(),
        );
        sql.exec("CREATE TABLE show_t (id INTEGER PRIMARY KEY, dt TEXT, v REAL)")
            .await
            .unwrap();
        for i in 0..rows {
            sql.exec(&format!(
                "INSERT INTO show_t VALUES ({i}, datetime('2024-01-01', '+{i} hours'), {i}.5)"
            ))
            .await
            .unwrap();
        }
        sql
    }

    fn pipe(datetime: bool) -> meerschaum_core::Pipe {
        let mut columns = Map::new();
        columns.insert("primary".into(), Value::Str("id".into()));
        if datetime {
            columns.insert("datetime".into(), Value::Str("dt".into()));
        }
        let mut params = Map::new();
        params.insert("columns".into(), Value::Map(columns));
        params.insert("target".into(), Value::Str("show_t".into()));
        meerschaum_core::Pipe::builder("sql:show", "t")
            .parameters(params)
            .build()
            .unwrap()
    }

    fn plan(limits: Option<DisplayOptions>) -> DataPlan {
        DataPlan {
            backtrack: None,
            begin: None,
            end: None,
            params: None,
            limits,
        }
    }

    fn options() -> DisplayOptions {
        DisplayOptions {
            max_rows: 20,
            min_rows: 6,
            max_columns: None,
            max_colwidth: 50,
            width: None,
        }
    }

    fn first_column(read: &DataRead) -> Vec<String> {
        let cells = read.frame.cells();
        cells.columns[0]
            .1
            .iter()
            .map(|c| match c {
                Cell::Int(i) => i.to_string(),
                other => format!("{other:?}"),
            })
            .collect()
    }

    #[tokio::test]
    async fn a_long_window_reads_only_the_head_and_the_tail() {
        let dir = tempfile::tempdir().unwrap();
        let sql = connector(dir.path(), 100).await;
        sql.enable_query_log();
        let conn: &dyn InstanceConnector = sql.as_ref();
        let read = read_pipe_data(conn, &pipe(true), &plan(Some(options())))
            .await
            .expect("rows");
        assert_eq!(read.total, 100);
        assert_eq!(read.fetched, Fetched::HeadTail { head: 3 });
        assert_eq!(first_column(&read), ["0", "1", "2", "97", "98", "99"]);
        // No statement read the whole table: every `SELECT *`-style read carries a LIMIT.
        let reads: Vec<String> = sql
            .take_query_log()
            .into_iter()
            .filter(|q| q.contains("show_t") && !q.contains("COUNT") && !q.contains("count("))
            .filter(|q| q.trim_start().to_uppercase().starts_with("SELECT"))
            .filter(|q| !q.contains("pragma") && !q.contains("sqlite_master"))
            .collect();
        assert!(!reads.is_empty());
        assert!(
            reads.iter().all(|q| q.to_uppercase().contains("LIMIT")),
            "{reads:#?}"
        );
    }

    #[tokio::test]
    async fn a_backtrack_window_keeps_the_newest_first_orientation() {
        let dir = tempfile::tempdir().unwrap();
        let sql = connector(dir.path(), 100).await;
        let conn: &dyn InstanceConnector = sql.as_ref();
        let mut backtrack = plan(Some(options()));
        // 60 hours back from the newest row: 61 rows (the bound is inclusive).
        backtrack.backtrack = Some(3600.0);
        let read = read_pipe_data(conn, &pipe(true), &backtrack)
            .await
            .expect("rows");
        assert_eq!(read.total, 61);
        assert_eq!(first_column(&read), ["99", "98", "97", "41", "40", "39"]);
        // Short enough to read whole: the plain backtrack read, newest first as in v4.
        backtrack.backtrack = Some(600.0);
        let short = read_pipe_data(conn, &pipe(true), &backtrack)
            .await
            .expect("rows");
        assert_eq!((short.total, short.fetched), (11, Fetched::All));
        assert_eq!(first_column(&short)[..2], ["99", "98"]);
    }

    #[tokio::test]
    async fn a_pipe_without_a_datetime_reads_the_first_rows_only() {
        let dir = tempfile::tempdir().unwrap();
        let sql = connector(dir.path(), 100).await;
        let conn: &dyn InstanceConnector = sql.as_ref();
        // The table has a TEXT column named `dt`, not a datetime dtype, so it has no axis.
        let read = read_pipe_data(conn, &pipe(false), &plan(Some(options())))
            .await
            .expect("rows");
        assert_eq!(read.total, 100);
        assert_eq!(read.fetched, Fetched::HeadOnly);
        assert_eq!(first_column(&read), ["0", "1", "2", "3", "4", "5"]);
    }

    #[test]
    fn the_axis_is_columns_datetime_or_v4s_guess_and_needs_an_order_by() {
        let dtypes = |pairs: &[(&str, &str)]| -> indexmap::IndexMap<String, String> {
            pairs
                .iter()
                .map(|(c, t)| (c.to_string(), t.to_string()))
                .collect()
        };
        let cols = |pairs: &[(&str, &str)]| -> Map {
            pairs
                .iter()
                .map(|(r, c)| (r.to_string(), Value::Str(c.to_string())))
                .collect()
        };
        let table = dtypes(&[("id", "int"), ("ts", "datetime64[ns, UTC]"), ("v", "float")]);
        // `columns:datetime`, or the guessed datetime dtype next to another index column.
        assert!(orders_by_datetime(
            &cols(&[("datetime", "ts")]),
            false,
            &table
        ));
        assert!(orders_by_datetime(&cols(&[("id", "id")]), false, &table));
        // A datetime dtype alone: v4 emits no ORDER BY, so a "tail" would be arbitrary.
        assert!(!orders_by_datetime(&cols(&[]), false, &table));
        // `dtypes:datetime: null` switches the guess off; a missing column is no axis.
        assert!(!orders_by_datetime(&cols(&[("id", "id")]), true, &table));
        assert!(!orders_by_datetime(
            &cols(&[("datetime", "gone")]),
            false,
            &table
        ));
        assert!(!orders_by_datetime(
            &cols(&[("id", "id")]),
            false,
            &dtypes(&[("id", "int"), ("v", "float")])
        ));
    }

    #[tokio::test]
    async fn a_datetime_dtype_without_columns_prints_the_head_only() {
        let dir = tempfile::tempdir().unwrap();
        let sql = connector(dir.path(), 0).await;
        sql.exec("CREATE TABLE show_d (ts DATETIME, v REAL)")
            .await
            .unwrap();
        for i in 0..100 {
            // Out of time order: an unordered "tail" would be the last physical rows.
            sql.exec(&format!(
                "INSERT INTO show_d VALUES (datetime('2024-01-01', '+{} hours'), {i}.5)",
                (i * 37) % 100
            ))
            .await
            .unwrap();
        }
        let mut params = Map::new();
        params.insert("target".into(), Value::Str("show_d".into()));
        params.insert(
            "dtypes".into(),
            Value::Map(Map::from_iter([(
                "ts".into(),
                Value::Str("datetime".into()),
            )])),
        );
        let pipe = meerschaum_core::Pipe::builder("sql:show", "d")
            .parameters(params)
            .build()
            .unwrap();
        let conn: &dyn InstanceConnector = sql.as_ref();
        let read = read_pipe_data(conn, &pipe, &plan(Some(options())))
            .await
            .expect("rows");
        assert_eq!(read.total, 100);
        assert_eq!(read.fetched, Fetched::HeadOnly);
    }

    #[tokio::test]
    async fn short_windows_unlimited_reads_and_missing_tables_read_whole() {
        let dir = tempfile::tempdir().unwrap();
        let sql = connector(dir.path(), 15).await;
        let conn: &dyn InstanceConnector = sql.as_ref();
        let short = read_pipe_data(conn, &pipe(true), &plan(Some(options())))
            .await
            .expect("rows");
        assert_eq!((short.total, short.fetched), (15, Fetched::All));
        let all = read_pipe_data(conn, &pipe(true), &plan(None))
            .await
            .expect("rows");
        assert_eq!((all.total, all.fetched), (15, Fetched::All));
        let long_dir = tempfile::tempdir().unwrap();
        let long = connector(long_dir.path(), 40).await;
        let unlimited = read_pipe_data(long.as_ref(), &pipe(true), &plan(None))
            .await
            .expect("rows");
        assert_eq!((unlimited.total, unlimited.fetched), (40, Fetched::All));
        let missing = meerschaum_core::Pipe::builder("sql:show", "nothing")
            .build()
            .unwrap();
        assert!(
            read_pipe_data(conn, &missing, &plan(Some(options())))
                .await
                .is_none()
        );
    }

    #[test]
    fn the_backtrack_window_starts_before_the_newest_value() {
        assert_eq!(
            backtrack_begin(DateBound::Int(100), 30).py_str(),
            "70".to_string()
        );
        let day = chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let begin = backtrack_begin(DateBound::Datetime(DateTimeValue::naive(day)), 90);
        assert_eq!(begin.py_str(), "2024-01-01 22:30:00");
    }
}
