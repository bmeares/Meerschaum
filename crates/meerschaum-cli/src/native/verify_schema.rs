//! `verify schema` (`verify schemas`, `verify schemata`): compare each selected pipe's table
//! with the pipe's declared dtypes, print the plan, and apply it on confirmation.
//!
//! - `-y` / `--yes` (or `--force`) applies every change without asking.
//! - `--noask` (or `MRSM_NOASK`) applies the safe changes (added columns, widenings) and
//!   refuses the ones that need confirmation.
//! - `--dry-run` prints the SQL only and changes nothing.
//!
//! Columns are never dropped, and static pipes are included (their syncs never change the
//! table; this action is how their tables catch up with edited `dtypes`). The plans and
//! statements come from the instance ([`meerschaum_connector::InstanceConnector::get_pipe_schema_plan`]).

use meerschaum_connector::ConnectorError;
use meerschaum_connector::schema::{SchemaChange, SchemaPlan};
use meerschaum_core::SuccessTuple;

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::value::ArgsMap;

use super::pipes::{hydrate, selected_pipes};
use super::{check_noask, flag, info, plural, session, warn, yes_no};

/// What a plan table row shows in the "Change" column.
fn change_text(c: &SchemaChange) -> String {
    let mut text = c.kind.label().to_string();
    if c.blocked.is_some() {
        text.push_str(" (blocked)");
    } else if !c.is_safe() {
        text.push_str(" (needs confirmation)");
    }
    if let Some(note) = &c.note {
        text.push_str(&format!("; {note}"));
    }
    text
}

/// The plan as a table (`Column  Now  New  Change`), with its notes and blocked reasons.
pub fn plan_table(plan: &SchemaPlan) -> String {
    let rows: Vec<[String; 4]> = plan
        .changes
        .iter()
        .map(|c| {
            [
                c.column.clone(),
                c.current.clone().unwrap_or_else(|| "(missing)".into()),
                format!("{} ({})", c.target, c.dtype),
                change_text(c),
            ]
        })
        .collect();
    let headers = ["Column", "Now", "New", "Change"];
    let mut widths: [usize; 4] = headers.map(|h| h.chars().count());
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }
    let line = |cells: &[String; 4]| {
        let mut s = String::from("    ");
        for (i, cell) in cells.iter().enumerate() {
            if i == 3 {
                s.push_str(cell);
            } else {
                s.push_str(&format!("{cell:<w$}  ", w = widths[i]));
            }
        }
        s.trim_end().to_string()
    };
    let mut lines = vec![line(&headers.map(String::from))];
    lines.push(format!(
        "    {}",
        widths
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>()
            .join("  ")
    ));
    for row in &rows {
        lines.push(line(row));
    }
    for note in &plan.notes {
        lines.push(format!("    Note: {note}"));
    }
    if let Some(blocked) = &plan.blocked {
        for b in blocked.lines() {
            lines.push(format!("    {b}"));
        }
    }
    lines.join("\n")
}

/// The statements of a plan, each terminated with `;`.
pub fn plan_sql(plan: &SchemaPlan, safe_only: bool) -> String {
    let queries = if safe_only {
        &plan.safe_queries
    } else {
        &plan.queries
    };
    queries
        .iter()
        .map(|q| format!("{};", q.trim_end().trim_end_matches(';')))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The question asked before applying `plan`.
fn question(pipe: &str, plan: &SchemaPlan) -> String {
    let n = plan.changes.iter().filter(|c| c.is_applicable()).count();
    let rebuild = plan
        .changes
        .iter()
        .any(|c| c.note.as_deref() == Some("rebuilds the table"));
    format!(
        "Apply {n} change{} to the table of {pipe}?{}",
        plural(n),
        if rebuild {
            "\n    SQLite cannot change a column's type in place: the table is rebuilt."
        } else {
            ""
        }
    )
}

/// `verify schema`.
pub fn verify_schemas(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let dry_run = flag(args, "dry_run");
    let yes = flag(args, "yes") || flag(args, "force");
    let noask = check_noask(rt, flag(args, "noask"));
    let nopretty = flag(args, "nopretty");
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes to verify."));
    }
    let session = session(rt);
    let conn = inst.conn();
    let (mut matched, mut updated, mut declined, mut failed) = (0usize, 0usize, 0usize, 0usize);
    let mut pending = 0usize;
    let mut sql_blocks: Vec<String> = Vec::new();
    for sp in pipes.iter_mut() {
        hydrate(rt, &inst, sp);
        let pipe_str = sp.pipe.to_string();
        let plan = match session.block_on(conn.get_pipe_schema_plan(&sp.pipe)) {
            Ok(plan) => plan,
            Err(ConnectorError::NotImplemented(message)) => {
                return Ok(SuccessTuple::fail(message));
            }
            Err(e) => {
                failed += 1;
                warn(
                    rt,
                    &format!("Failed to check the table of {pipe_str}:\n{e}"),
                );
                continue;
            }
        };
        if dry_run {
            let body = if !plan.exists {
                format!("-- {pipe_str}: no table yet.")
            } else if plan.queries.is_empty() {
                format!("-- {pipe_str}: nothing to change.")
            } else {
                format!("-- {pipe_str}\n{}", plan_sql(&plan, false))
            };
            sql_blocks.push(body);
            if plan.is_empty() {
                matched += 1;
            }
            continue;
        }
        if !plan.exists || plan.is_empty() {
            matched += 1;
            if !nopretty {
                let why = if plan.exists {
                    "matches its dtypes"
                } else {
                    "does not exist yet"
                };
                info(rt, &format!("The table of {pipe_str} {why}."));
                for note in &plan.notes {
                    rt.print(&format!("    Note: {note}\n"));
                }
            }
            continue;
        }
        info(
            rt,
            &format!(
                "The table {} of {pipe_str} differs from its dtypes:",
                plan.table
            ),
        );
        rt.print(&format!("{}\n\n", plan_table(&plan)));
        let applicable: Vec<&SchemaChange> =
            plan.changes.iter().filter(|c| c.is_applicable()).collect();
        if applicable.is_empty() {
            failed += 1;
            warn(rt, &format!("No change can be applied to {pipe_str}."));
            continue;
        }
        let needs_confirmation = applicable.iter().filter(|c| !c.is_safe()).count();
        let safe_only = if yes {
            false
        } else if noask {
            true
        } else {
            let ok = yes_no(rt, &question(&pipe_str, &plan), "n", false, false)?;
            if !ok {
                declined += 1;
                info(rt, &format!("Left the table of {pipe_str} unchanged."));
                continue;
            }
            false
        };
        if safe_only && applicable.iter().all(|c| !c.is_safe()) {
            pending += 1;
            warn(
                rt,
                &format!(
                    "Not changing {pipe_str}: {needs_confirmation} change{} need{} confirmation \
                     (run `verify schema` without --noask, or with --yes).",
                    plural(needs_confirmation),
                    if needs_confirmation == 1 { "s" } else { "" }
                ),
            );
            continue;
        }
        let result = session.block_on(conn.apply_pipe_schema(&sp.pipe, &plan, safe_only));
        if !result.success {
            failed += 1;
            warn(rt, &result.message);
            continue;
        }
        info(rt, &result.message);
        if safe_only && needs_confirmation > 0 {
            pending += 1;
            warn(
                rt,
                "Run `verify schema` without --noask (or with --yes) to apply the rest.",
            );
        } else {
            updated += 1;
        }
    }
    if dry_run {
        rt.print(&format!("{}\n", sql_blocks.join("\n\n")));
        return Ok(SuccessTuple::ok(format!(
            "Printed the schema changes for {} pipe{} (nothing was changed).",
            pipes.len(),
            plural(pipes.len())
        )));
    }
    let n = pipes.len();
    let mut parts = Vec::new();
    if updated > 0 {
        parts.push(format!("{updated} updated"));
    }
    if matched > 0 {
        parts.push(format!("{matched} already matched"));
    }
    if declined > 0 {
        parts.push(format!("{declined} left unchanged"));
    }
    if pending > 0 {
        parts.push(format!(
            "{pending} need{} confirmation",
            if pending == 1 { "s" } else { "" }
        ));
    }
    if failed > 0 {
        parts.push(format!("{failed} failed"));
    }
    let message = format!(
        "Verified the tables of {n} pipe{}\n    ({}).",
        plural(n),
        parts.join(", ")
    );
    let success = failed == 0 && pending == 0 && !(declined > 0 && updated == 0 && matched == 0);
    Ok(SuccessTuple { success, message })
}
