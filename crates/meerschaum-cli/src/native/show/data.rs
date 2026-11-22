//! `show data`: reading the newest rows of each pipe (the head and tail of a window, or a
//! backtrack from the sync time) and printing them.

use super::*;

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
pub(super) async fn hydrated_pipe(
    conn: &dyn InstanceConnector,
    sp: &SelectedPipe,
) -> meerschaum_core::Pipe {
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

#[cfg(test)]
mod limited_read_tests;
