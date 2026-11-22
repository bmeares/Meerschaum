//! `show columns` and `pprint_pipe_columns`: each pipe's columns, types and indices.

use super::data::hydrated_pipe;
use super::*;

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
