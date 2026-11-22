//! `show sizes` and `show partitions`: the physical tables behind pipes, their sizes and their
//! partitions.

use super::data::hydrated_pipe;
use super::*;

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
