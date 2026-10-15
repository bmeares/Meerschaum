//! The instance-backed `show` subactions (`meerschaum/actions/show.py`): `pipes`, `connectors`,
//! `columns`, `rowcounts`, `data`, `users`, `sizes`, `partitions` and `tags`, plus `Pipe.show()`
//! and `pprint_pipe_columns`.

use meerschaum_connector::{DataQuery, DateBound, pipe_ops};
use meerschaum_core::SuccessTuple;
use meerschaum_core::pyfmt::{JsonStyle, json_dumps};
use meerschaum_core::value::{Map, Value};
use meerschaum_kernel::value::Value as KValue;

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
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

/// `show pipes`.
pub fn show_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::ok("No pipes to show."));
    }
    if pipes.len() == 1 {
        return pipe_show(rt, &inst, &mut pipes[0], nopretty);
    }
    if !nopretty {
        let keys: Vec<_> = pipes.iter().map(|p| pipe_keys(&p.pipe)).collect();
        let text = rt.with_formatter(|f| f.pprint_pipes(&keys));
        rt.print(&text);
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
            (p.pipe.to_string(), rc)
        },
    ));
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

/// `show data [minutes|all]`.
pub fn show_data(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    let begin = bound(args, "begin");
    let end = bound(args, "end");
    let (inst, pipes) = selected_pipes(rt, args)?;
    let session = session(rt);
    let conn = inst.conn();
    let backtrack_minutes: Option<f64> =
        match action.first().and_then(|a| a.trim().parse::<f64>().ok()) {
            Some(m) => Some(m),
            None => {
                if begin.is_none()
                    && end.is_none()
                    && (action.is_empty() || action.first().is_some_and(|a| a != "all"))
                {
                    Some(1440.0)
                } else {
                    None
                }
            }
        };
    let params = super::params(args);
    for p in &pipes {
        let df = match backtrack_minutes {
            Some(m) => session.block_on(conn.get_backtrack_data(
                &p.pipe,
                m as i64,
                &DataQuery {
                    params: params.clone(),
                    ..Default::default()
                },
            )),
            None => session.block_on(conn.get_pipe_data(
                &p.pipe,
                &DataQuery {
                    begin: begin.clone(),
                    end: end.clone(),
                    params: params.clone(),
                    ..Default::default()
                },
            )),
        };
        let df = match df {
            Ok(Some(df)) => df,
            Ok(None) | Err(_) => {
                warn(rt, &format!("Failed to fetch data for {}.", p.pipe));
                continue;
            }
        };
        let info_msg = match backtrack_minutes {
            Some(m) => format!(
                "Last {} minute{} of data for {}:",
                py_float(m),
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
            let text = format_dataframe(&tab_frame(&df), None);
            rt.print(&format!("{text}\n"));
        } else {
            rt.print(&format!("{}\n", dumps(&Value::Map(pipe_state(&p.pipe)))));
            let json = meerschaum_api::wire::to_json(
                &df,
                None,
                &meerschaum_api::wire::ToJsonOptions {
                    columns_orient: true,
                    ..Default::default()
                },
            );
            pprint(rt, &PyObj::Str(json), true);
        }
    }
    Ok(SuccessTuple::success())
}

/// `pprint_pipe_columns(pipe, nopretty)`.
pub fn pprint_pipe_columns(
    rt: &mut Runtime<'_>,
    inst: &Instance,
    sp: &mut SelectedPipe,
    nopretty: bool,
) {
    hydrate(rt, inst, sp);
    let session = session(rt);
    let conn = inst.conn();
    let pipe = sp.pipe.clone();
    let exists = session.block_on(conn.pipe_exists(&pipe)).unwrap_or(false);
    let cols: Map = if exists { pipe.columns() } else { Map::new() };
    let cols_types = if exists {
        session
            .block_on(conn.get_pipe_columns_types(&pipe))
            .unwrap_or_default()
    } else {
        Default::default()
    };
    let dtypes = session
        .block_on(pipe_ops::get_dtypes(
            conn.as_ref(),
            &pipe,
            pipe.parameters(),
            true,
        ))
        .unwrap_or_default();
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
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    for sp in pipes.iter_mut() {
        pprint_pipe_columns(rt, &inst, sp, nopretty);
    }
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
    meerschaum_sql::instance::values::format_bytes(n)
}

/// `show sizes`.
pub fn show_sizes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let (inst, pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes selected."));
    }
    let session = session(rt);
    let conn = inst.conn();
    let mut sizes: Vec<(SelectedPipe, Option<i64>)> = Vec::new();
    let conn = &conn;
    let concurrency = super::metadata::concurrency(&inst, args);
    let mut fetched = (concurrency > 1).then(|| {
        session
            .block_on(super::metadata::ordered(
                pipes.iter().collect(),
                concurrency,
                |p| async move { conn.get_pipe_size(&p.pipe).await.unwrap_or(None) },
            ))
            .into_iter()
    });
    for p in pipes {
        if !nopretty {
            info(rt, &format!("Calculating size for {}...", p.pipe));
        }
        let size = match fetched.as_mut() {
            Some(results) => results.next().expect("one size per pipe"),
            None => session
                .block_on(conn.get_pipe_size(&p.pipe))
                .unwrap_or(None),
        };
        if !nopretty {
            info(rt, &format!("Size for {}: {}", p.pipe, format_bytes(size)));
        }
        sizes.push((p, size));
    }
    sizes.sort_by(|a, b| {
        let ka = (a.1.is_some(), a.1.unwrap_or(0));
        let kb = (b.1.is_some(), b.1.unwrap_or(0));
        kb.cmp(&ka)
    });
    let total: i64 = sizes.iter().filter_map(|(_, s)| *s).sum();
    if nopretty {
        let mut out = String::new();
        for (p, s) in &sizes {
            out.push_str(&format!("{}\t{}\n", p.pipe, format_bytes(*s)));
        }
        rt.print(&out);
        return Ok(SuccessTuple::success());
    }
    let text = rt.with_formatter(|f| {
        let rows: Vec<(String, String, String)> = sizes
            .iter()
            .map(|(p, s)| {
                (
                    p.pipe.to_string(),
                    f.pipe_repr(&pipe_keys(&p.pipe)),
                    format_bytes(*s),
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
        for (plain, hl, s) in &rows {
            let pad = " ".repeat(name_w - plain.chars().count());
            lines.push(format!("    {hl}{pad}  {s:>size_w$}"));
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

async fn partition_metadata(inst: &Instance, pipe: &meerschaum_core::Pipe) -> (Map, Option<i64>) {
    let info = match inst.sql() {
        Some(sql) => sql.get_partition_info(pipe).await,
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
    let mut fetched = (concurrency > 1).then(|| {
        session
            .block_on(super::metadata::ordered(
                pipes.iter().collect(),
                concurrency,
                |p| partition_metadata(&inst, &p.pipe),
            ))
            .into_iter()
    });
    for p in pipes {
        if !nopretty {
            info(rt, &format!("Inspecting partitions for {}...", p.pipe));
        }
        let (pinfo, rows) = match fetched.as_mut() {
            Some(results) => results.next().expect("one partition result per pipe"),
            None => session.block_on(partition_metadata(&inst, &p.pipe)),
        };
        let partitioned = pinfo.get("partitioned").is_some_and(Value::truthy);
        let count = pinfo.get("count").and_then(Value::as_i64);
        let interval = match pinfo.get("interval") {
            Some(Value::Map(m)) if m.contains_key("minutes") => {
                interval_minutes_str(m.get("minutes").and_then(Value::as_i64).unwrap_or(0))
            }
            Some(Value::Map(m)) if m.contains_key("seconds") => {
                super::sync::interval_str(m.get("seconds").and_then(Value::as_f64).unwrap_or(0.0))
            }
            Some(Value::Int(i)) => format!("{} (int axis)", thousands(*i)),
            _ => "-".to_string(),
        };
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
    let opts = OptionsArgs {
        header: Some(header),
        nopretty: flag(args, "nopretty"),
        ..OptionsArgs::default()
    };
    let text = rt.with_formatter(|f| f.print_options(&names, &opts));
    rt.print(&text);
    Ok(SuccessTuple::success())
}
