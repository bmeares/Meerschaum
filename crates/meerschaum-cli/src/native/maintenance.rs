//! The maintenance family: `vacuum pipes` (`actions/vacuum.py`), `analyze pipes`
//! (`actions/analyze.py`), `compress pipes` / `decompress pipes` (`actions/compress.py`,
//! `actions/decompress.py`) and `partition pipes` (`actions/partition.py`).

use meerschaum_core::SuccessTuple;
use meerschaum_core::pipe::chunk::{ChunkInterval, ChunkIntervalArg, total_seconds};

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::value::ArgsMap;

use super::pipes::{SelectedPipe, hydrate, selected_pipes};
use super::show::format_bytes;
use super::sync::interval_str;
use super::{Instance, flag, info, int, plural, session, thousands, warn, yes_no};

const PG_VACUUM_FAMILY: &[&str] = &[
    "postgresql",
    "postgis",
    "citus",
    "timescaledb",
    "timescaledb-ha",
];

fn flavor(inst: &Instance) -> Option<String> {
    inst.sql().map(|s| s.flavor().as_str().to_string())
}

fn signed_bytes(n: i64) -> String {
    if n >= 0 {
        format_bytes(Some(n))
    } else {
        format!("-{}", format_bytes(Some(-n)))
    }
}

/// The `Before / After / Saved` table of `vacuum` / `compress` / `decompress`.
fn size_table(
    rows: &mut [(i64, String, String, String, String)],
    before: i64,
    after: i64,
    label: &str,
    total_delta: i64,
) -> String {
    rows.sort_by_key(|r| std::cmp::Reverse(r.0));
    #[allow(clippy::type_complexity)]
    let w = |f: &dyn Fn(&(i64, String, String, String, String)) -> usize, min: usize| {
        rows.iter()
            .map(f)
            .chain(std::iter::once(min))
            .max()
            .unwrap_or(min)
    };
    let name_w = w(&|r| r.1.chars().count(), 4);
    let before_w = w(&|r| r.2.chars().count(), 6);
    let after_w = w(&|r| r.3.chars().count(), 5);
    let saved_w = w(&|r| r.4.chars().count(), label.len());
    let header = format!(
        "    {:<name_w$}  {:>before_w$}  {:>after_w$}  {:>saved_w$}",
        "Pipe", "Before", "After", label
    );
    let sep = format!("    {}", "-".repeat(header.chars().count() - 4));
    let mut lines = vec![header, sep.clone()];
    for (_, name, b, a, s) in rows.iter() {
        lines.push(format!(
            "    {name:<name_w$}  {b:>before_w$}  {a:>after_w$}  {s:>saved_w$}"
        ));
    }
    lines.push(sep);
    lines.push(format!(
        "    {:<name_w$}  {:>before_w$}  {:>after_w$}  {:>saved_w$}",
        "Total",
        format_bytes(Some(before)),
        format_bytes(Some(after)),
        signed_bytes(total_delta)
    ));
    format!("\n{}\n\n", lines.join("\n"))
}

fn confirm(
    rt: &mut Runtime<'_>,
    args: &ArgsMap,
    question: &str,
) -> Result<bool, crate::builtin::ActionError> {
    Ok(flag(args, "force") || yes_no(rt, question, "n", flag(args, "yes"), flag(args, "noask"))?)
}

fn pipe_list(pipes: &[SelectedPipe]) -> String {
    let mut s = String::new();
    for p in pipes {
        s.push_str(&format!("    - {}\n", p.pipe));
    }
    s.push('\n');
    s
}

fn get_size(rt: &mut Runtime<'_>, inst: &Instance, sp: &SelectedPipe) -> Option<i64> {
    let session = session(rt);
    match session.block_on(inst.conn().get_pipe_size(&sp.pipe)) {
        Ok(s) => s,
        Err(meerschaum_connector::ConnectorError::NotImplemented(_)) => None,
        Err(e) => {
            warn(rt, &format!("Failed to get the size of {}:\n{e}", sp.pipe));
            None
        }
    }
}

/// `vacuum pipes [--full]`.
pub fn vacuum_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let full = flag(args, "full");
    let nopretty = flag(args, "nopretty");
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes to vacuum."));
    }
    let fl = flavor(&inst);
    let is_pg = fl.as_deref().is_some_and(|f| PG_VACUUM_FAMILY.contains(&f));
    if !full && is_pg && !nopretty {
        info(
            rt,
            "Run `vacuum pipes --full` to reclaim space (needs exclusive lock).",
        );
    }
    let question = format!(
        "Are you sure you want to vacuum the target tables for these pipes?\n{}{}",
        if full {
            "    `VACUUM FULL` takes an exclusive lock and may take a while.\n\n"
        } else {
            "    This may take a while.\n\n"
        },
        pipe_list(&pipes)
    );
    if !confirm(rt, args, &question)? {
        return Ok(SuccessTuple::fail("No pipes were vacuumed."));
    }
    let session = session(rt);
    let show_stats = full || !is_pg;
    let (mut successes, mut fails) = (0usize, 0usize);
    let (mut total_before, mut total_after) = (0i64, 0i64);
    let mut rows = Vec::new();
    for sp in pipes.iter_mut() {
        hydrate(rt, &inst, sp);
        if !nopretty {
            info(rt, &format!("Vacuuming {}...", sp.pipe));
        }
        let before = if show_stats {
            get_size(rt, &inst, sp)
        } else {
            None
        };
        let res = session.block_on(inst.conn().vacuum_pipe(&sp.pipe, full));
        if res.success {
            successes += 1;
        } else {
            fails += 1;
            warn(rt, &res.message);
        }
        let after = if show_stats {
            get_size(rt, &inst, sp)
        } else {
            None
        };
        if let (Some(b), Some(a)) = (before, after) {
            total_before += b;
            total_after += a;
            let r = b - a;
            rows.push((
                r,
                sp.pipe.to_string(),
                format_bytes(Some(b)),
                format_bytes(Some(a)),
                signed_bytes(r),
            ));
        }
    }
    if !rows.is_empty() && !nopretty {
        let t = size_table(
            &mut rows,
            total_before,
            total_after,
            "Saved",
            total_before - total_after,
        );
        rt.print(&t);
    }
    Ok(SuccessTuple {
        success: successes > 0,
        message: format!(
            "Finished vacuuming {} pipe{}\n    ({successes} succeeded, {fails} failed).",
            pipes.len(),
            plural(pipes.len())
        ),
    })
}

/// `analyze pipes`.
pub fn analyze_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes to analyze."));
    }
    let question = format!(
        "Are you sure you want to analyze the target tables for these pipes?\n\n{}",
        pipe_list(&pipes)
    );
    if !confirm(rt, args, &question)? {
        return Ok(SuccessTuple::fail("No pipes were analyzed."));
    }
    let session = session(rt);
    let (mut successes, mut fails) = (0usize, 0usize);
    for sp in pipes.iter_mut() {
        hydrate(rt, &inst, sp);
        if !nopretty {
            info(rt, &format!("Analyzing {}...", sp.pipe));
        }
        let res = session.block_on(inst.conn().analyze_pipe(&sp.pipe));
        if res.success {
            successes += 1;
        } else {
            fails += 1;
            warn(rt, &res.message);
        }
    }
    Ok(SuccessTuple {
        success: successes > 0,
        message: format!(
            "Finished analyzing {} pipe{}\n    ({successes} succeeded, {fails} failed).",
            pipes.len(),
            plural(pipes.len())
        ),
    })
}

/// `compress pipes` (`decompress = false`) and `decompress pipes`.
fn compress_family(args: &ArgsMap, rt: &mut Runtime<'_>, decompress: bool) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let no_policy = flag(args, "no_policy");
    let (verb, verb_ing, verb_ed) = if decompress {
        ("decompress", "Decompressing", "decompressed")
    } else {
        ("compress", "Compressing", "compressed")
    };
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail(format!("No pipes to {verb}.")));
    }
    let question = format!(
        "Are you sure you want to {verb} the target tables for these pipes?\n{}{}",
        if decompress {
            "    This rewrites table data, increases disk usage, and may take a while.\n\n"
        } else {
            "    This rewrites table data and may take a while.\n\n"
        },
        pipe_list(&pipes)
    );
    if !confirm(rt, args, &question)? {
        return Ok(SuccessTuple::fail(format!("No pipes were {verb_ed}.")));
    }
    let session = session(rt);
    let (mut successes, mut fails) = (0usize, 0usize);
    let (mut total_before, mut total_after) = (0i64, 0i64);
    let mut rows = Vec::new();
    for sp in pipes.iter_mut() {
        hydrate(rt, &inst, sp);
        if !nopretty {
            info(rt, &format!("{verb_ing} {}...", sp.pipe));
        }
        let before = get_size(rt, &inst, sp);
        let res = if decompress {
            session.block_on(inst.conn().decompress_pipe(&sp.pipe, no_policy))
        } else {
            match inst.sql() {
                Some(sql) => session.block_on(sql.compress_pipe(&sp.pipe, no_policy)),
                None => session.block_on(inst.conn().compress_pipe(&sp.pipe)),
            }
        };
        if res.success {
            successes += 1;
        } else {
            fails += 1;
            warn(rt, &res.message);
        }
        let after = get_size(rt, &inst, sp);
        if let (Some(b), Some(a)) = (before, after) {
            total_before += b;
            total_after += a;
            let d = if decompress { a - b } else { b - a };
            rows.push((
                d,
                sp.pipe.to_string(),
                format_bytes(Some(b)),
                format_bytes(Some(a)),
                signed_bytes(d),
            ));
        }
    }
    if !rows.is_empty() && !nopretty {
        let (label, total) = if decompress {
            ("Added", total_after - total_before)
        } else {
            ("Saved", total_before - total_after)
        };
        let t = size_table(&mut rows, total_before, total_after, label, total);
        rt.print(&t);
    }
    Ok(SuccessTuple {
        success: successes > 0,
        message: format!(
            "Finished {} {} pipe{}\n    ({successes} succeeded, {fails} failed).",
            verb_ing.to_lowercase(),
            pipes.len(),
            plural(pipes.len())
        ),
    })
}

/// `compress pipes [--no-policy]`.
pub fn compress_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    compress_family(args, rt, false)
}

/// `decompress pipes [--no-policy]`.
pub fn decompress_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    compress_family(args, rt, true)
}

/// `interval_str(...)` of a chunk interval, or `f"{n:,}"` on an integer axis.
fn chunk_interval_display(iv: &ChunkInterval) -> String {
    match iv {
        ChunkInterval::Delta(d) => interval_str(total_seconds(*d)),
        ChunkInterval::Int(i) => thousands(*i),
    }
}

/// `partition pipes [--chunk-minutes N]`.
pub fn partition_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let chunk_minutes = int(args, "chunk_minutes");
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes to repartition."));
    }
    let width_str = match chunk_minutes {
        Some(m) => interval_str(m as f64 * 60.0),
        None => "each pipe's configured `verify.chunk_minutes`".to_string(),
    };
    let question = format!(
        "Repartition the target tables for these pipes to {width_str}?\n    Non-TimescaleDB tables are rebuilt (read, dropped, and re-synced), which may take a while and load each table into memory.\n\n{}",
        pipe_list(&pipes)
    );
    if !confirm(rt, args, &question)? {
        return Ok(SuccessTuple::fail("No pipes were repartitioned."));
    }
    let session = session(rt);
    let defaults = super::pipes::defaults(rt);
    let (mut successes, mut fails) = (0usize, 0usize);
    let mut total_partitions: i64 = 0;
    let mut rows: Vec<(String, String, String, String)> = Vec::new();
    for sp in pipes.iter_mut() {
        hydrate(rt, &inst, sp);
        if !nopretty {
            info(rt, &format!("Repartitioning {}...", sp.pipe));
        }
        let res = session.block_on(inst.conn().partition_pipe(&sp.pipe, chunk_minutes));
        if res.success {
            successes += 1;
        } else {
            fails += 1;
            warn(rt, &res.message);
        }
        let count = if res.success {
            inst.sql().and_then(|sql| {
                session
                    .block_on(sql.get_partition_info(&sp.pipe))
                    .get("count")
                    .and_then(meerschaum_core::Value::as_i64)
            })
        } else {
            None
        };
        if let Some(c) = count {
            total_partitions += c;
        }
        let width = if res.success {
            sp.pipe
                .get_chunk_interval(
                    chunk_minutes.map(|m| ChunkIntervalArg::Delta(chrono::TimeDelta::minutes(m))),
                    &defaults,
                )
                .map(|iv| chunk_interval_display(&iv))
                .unwrap_or_else(|_| "-".into())
        } else {
            "-".into()
        };
        rows.push((
            sp.pipe.to_string(),
            if res.success { "OK" } else { "FAILED" }.to_string(),
            width,
            count.map(thousands).unwrap_or_else(|| "-".into()),
        ));
    }
    if !rows.is_empty() && !nopretty {
        #[allow(clippy::type_complexity)]
        let w = |f: &dyn Fn(&(String, String, String, String)) -> usize, min: usize| {
            rows.iter()
                .map(f)
                .chain(std::iter::once(min))
                .max()
                .unwrap_or(min)
        };
        let name_w = w(&|r| r.0.chars().count(), 4);
        let result_w = w(&|r| r.1.chars().count(), 6);
        let width_w = w(&|r| r.2.chars().count(), 5);
        let parts_w = w(&|r| r.3.chars().count(), 10);
        let header = format!(
            "    {:<name_w$}  {:<result_w$}  {:>width_w$}  {:>parts_w$}",
            "Pipe", "Result", "Width", "Partitions"
        );
        let sep = format!("    {}", "-".repeat(header.chars().count() - 4));
        let mut lines = vec![header, sep.clone()];
        for (n, r, wd, p) in &rows {
            lines.push(format!(
                "    {n:<name_w$}  {r:<result_w$}  {wd:>width_w$}  {p:>parts_w$}"
            ));
        }
        lines.push(sep);
        lines.push(format!(
            "    {:<name_w$}  {:<result_w$}  {:>width_w$}  {:>parts_w$}",
            "Total",
            "",
            "",
            thousands(total_partitions)
        ));
        rt.print(&format!("\n{}\n\n", lines.join("\n")));
    }
    Ok(SuccessTuple {
        success: successes > 0,
        message: format!(
            "Repartitioned {successes} of {} pipe{}{} across {} partition{}.",
            pipes.len(),
            plural(pipes.len()),
            if fails > 0 {
                format!(" ({fails} failed)")
            } else {
                String::new()
            },
            thousands(total_partitions),
            if total_partitions != 1 { "s" } else { "" }
        ),
    })
}
