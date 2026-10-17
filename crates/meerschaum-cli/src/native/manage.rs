//! Pipe management actions: `register pipes` (`actions/register.py`), `edit pipes`
//! (`actions/edit.py`, non-interactive with `--params`), `delete pipes` (`actions/delete.py`),
//! `drop pipes` (`actions/drop.py`), `clear pipes` (`actions/clear.py`), `tag pipes`
//! (`actions/tag.py`), `index pipes` (`actions/index.py`) and `copy pipes` (`actions/copy.py`,
//! the `copy pipes <instance>` path).

use meerschaum_connector::DataQuery;
use meerschaum_core::value::{Map, Value, apply_patch_maps};
use meerschaum_core::{Pipe, SuccessTuple};

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::format::rich::pretty::PyObj;
use crate::format::{OptionsArgs, TupleArgs};
use crate::value::ArgsMap;

use super::pipes::{SelectedPipe, Selection, build_pipe, explicit_pipes, hydrate, selected_pipes};
use super::show::{bound, bound_str};
use super::{
    Instance, flag, info, plural, pprint, print_tuple, session, string, thousands, warn, words,
    yes_no,
};

/// `Pipe.validate_indices()`: index roles may not be `json` columns.
pub fn validate_indices(pipe: &Pipe) -> SuccessTuple {
    let columns = pipe.columns();
    if columns.is_empty() {
        return SuccessTuple::success();
    }
    let dtypes = pipe.dtypes();
    let mut offenders: Vec<(String, String)> = columns
        .iter()
        .filter_map(|(ix, col)| {
            let col = col.as_str()?;
            let dt = dtypes.get(col).map(Value::py_str).unwrap_or_default();
            (dt.to_lowercase() == "json").then(|| (ix.clone(), col.to_string()))
        })
        .collect();
    if offenders.is_empty() {
        return SuccessTuple::success();
    }
    offenders.sort();
    let listing = offenders
        .iter()
        .map(|(ix, col)| format!("'{col}' (as the '{ix}' index)"))
        .collect::<Vec<_>>()
        .join(", ");
    SuccessTuple::fail(format!(
        "Cannot use a 'json' column as an index column: {listing}.\n    JSON values are dictionaries or lists, which cannot be sorted or deduplicated when syncing.\n    Either give the column a scalar dtype (e.g. 'str'), or move it out of `columns` and\n    declare it under `indices` if you only need a database index on it."
    ))
}

/// `Pipe.register()` for non-plugin connectors.
pub fn register_pipe(rt: &mut Runtime<'_>, inst: &Instance, pipe: &mut Pipe) -> SuccessTuple {
    if pipe.temporary {
        return SuccessTuple::fail(
            "Cannot register pipes created with `temporary=True` (read-only).",
        );
    }
    if pipe.raw_parameters().is_empty() {
        let cols = pipe.columns();
        let cols = if cols.is_empty() {
            let mut m = Map::new();
            m.insert("datetime".into(), Value::Null);
            m.insert("id".into(), Value::Null);
            m
        } else {
            cols
        };
        let mut params = Map::new();
        params.insert("columns".into(), Value::Map(cols));
        pipe.set_parameters(params);
    }
    let v = validate_indices(pipe);
    if !v.success {
        return v;
    }
    let session = session(rt);
    session.block_on(inst.conn().register_pipe(pipe))
}

/// `register pipes`.
pub fn register_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let mut sel = Selection::from_args(args);
    let tags = words(args, "tags");
    let mut params = sel.params.clone().unwrap_or_default();
    if !tags.is_empty() {
        params.insert(
            "tags".into(),
            Value::List(tags.iter().cloned().map(Value::Str).collect()),
        );
    }
    if sel.connector_keys.is_empty() || sel.metric_keys.is_empty() {
        warn(
            rt,
            "You must provide connector keys (-c) and metrics (-m) to register pipes.\n\nRun `bootstrap pipe` for an interactive guide that creates pipes.",
        );
        return Ok(SuccessTuple::fail("Missing connector keys or metrics"));
    }
    sel.params = (!params.is_empty()).then(|| params.clone());
    let inst = super::get_instance(args, rt)?;
    let pipes = explicit_pipes(rt, &inst, &sel)?;
    let mut success_pipes = 0usize;
    let mut failed_pipes = 0usize;
    for sp in pipes {
        let mut pipe = sp.pipe;
        if !params.is_empty() {
            let existing = pipe.raw_parameters().clone();
            pipe.set_parameters(apply_patch_maps(&params, &existing));
        }
        let res = register_pipe(rt, &inst, &mut pipe);
        if res.success {
            success_pipes += 1;
        } else {
            warn(rt, &res.message);
            failed_pipes += 1;
        }
    }
    let mut message = String::new();
    if success_pipes > 0 {
        message.push_str(&format!(
            "Successfully registered {success_pipes} pipe{}.",
            plural(success_pipes)
        ));
        if failed_pipes > 0 {
            message.push('\n');
        }
    }
    if failed_pipes > 0 {
        message.push_str(&format!(
            "Failed to register {failed_pipes} pipe{}.",
            plural(failed_pipes)
        ));
    }
    Ok(SuccessTuple {
        success: success_pipes > 0,
        message,
    })
}

fn pipes_options(pipes: &[SelectedPipe]) -> Vec<String> {
    pipes.iter().map(|p| p.pipe.to_string()).collect()
}

/// `edit pipes [definition]` (with `--params`; the editor path is interactive).
pub fn edit_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let yes = flag(args, "yes");
    let force = flag(args, "force");
    let noask = flag(args, "noask");
    let params = super::params(args);
    // The editor paths (`edit pipes`, `edit pipes definition`, `edit definition`).
    if action.first().is_some_and(|a| a == "definition") {
        return super::definition::edit_pipes(args, rt, true);
    }
    if params.as_ref().is_none_or(|p| p.is_empty()) {
        return super::definition::edit_pipes(args, rt, false);
    }
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes to edit."));
    }
    let opts = OptionsArgs {
        header: Some("Pipes to be edited:".into()),
        ..OptionsArgs::default()
    };
    let options = pipes_options(&pipes);
    let text = rt.with_formatter(|f| f.print_options(&options, &opts));
    rt.print(&text);

    let skip_prompts = yes || force || noask;
    if pipes.len() > 1 && !skip_prompts {
        let q = format!(
            "Press [Enter] to begin editing the above {} pipe{} or [CTRL-C] to cancel:",
            pipes.len(),
            plural(pipes.len())
        );
        if super::prompt(rt, &q, false, false).is_none() {
            return Ok(SuccessTuple::fail("No pipes changed."));
        }
    }
    let mut success = true;
    let mut msg = String::new();
    let session = session(rt);
    for sp in pipes.iter_mut() {
        if !skip_prompts {
            let q = format!("Press [Enter] to edit {} or [CTRL-C] to skip:", sp.pipe);
            match super::prompt(rt, &q, false, false) {
                None => continue,
                Some(t) if t == "pass" => continue,
                _ => {}
            }
        }
        hydrate(rt, &inst, sp);
        let Some(params) = &params else {
            continue;
        };
        info(
            rt,
            &format!("Will patch the following parameters into {}:", sp.pipe),
        );
        pprint(rt, &PyObj::from_value(&Value::Map(params.clone())), false);
        let patched = apply_patch_maps(sp.pipe.raw_parameters(), params);
        sp.pipe.set_parameters(patched);
        let v = validate_indices(&sp.pipe);
        let res = if !v.success {
            v
        } else if sp.pipe.temporary {
            SuccessTuple::fail("Cannot edit pipes created with `temporary=True` (read-only).")
        } else {
            session.block_on(inst.conn().edit_pipe(&sp.pipe, false))
        };
        success = success && res.success;
        if !res.success {
            msg.push_str(&format!("\n{}: {}", sp.pipe, res.message));
        }
    }
    let msg = if success {
        "Success".to_string()
    } else {
        msg.chars().skip(1).collect()
    };
    Ok(SuccessTuple {
        success,
        message: msg,
    })
}

/// The quoted table name `drop pipes` lists (`sql_item_name(target, flavor, schema)` on SQL).
fn drop_target(inst: &Instance, pipe: &Pipe) -> String {
    match inst.sql() {
        Some(sql) => match sql.sql_pipe(pipe) {
            Ok(sp) => sql.pipe_table_name(&sp),
            Err(_) => pipe.target(None).unwrap_or_default(),
        },
        None => pipe.target(None).unwrap_or_default(),
    }
}

/// `drop pipes`.
pub fn drop_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let yes = flag(args, "yes");
    let force = flag(args, "force");
    let noask = flag(args, "noask");
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes to drop."));
    }
    let mut question = "Are you sure you want to drop these tables?\n    Data will be lost and will need to be resynced.\n\n".to_string();
    let mut seen: Vec<String> = Vec::new();
    for sp in pipes.iter_mut() {
        hydrate(rt, &inst, sp);
        let target = drop_target(&inst, &sp.pipe);
        if seen.contains(&target) {
            continue;
        }
        question.push_str(&format!("    - {target}\n"));
        seen.push(target);
    }
    question.push('\n');
    let answer = force || yes_no(rt, &question, "n", yes, noask)?;
    if !answer {
        return Ok(SuccessTuple::fail("No pipes were dropped."));
    }
    let session = session(rt);
    let (mut successes, mut fails) = (0usize, 0usize);
    for sp in &pipes {
        let res = session.block_on(inst.conn().drop_pipe(&sp.pipe));
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
            "Finished dropping {} pipe{}\n    ({successes} succeeded, {fails} failed).",
            pipes.len(),
            plural(pipes.len())
        ),
    })
}

/// `delete pipes`: drop the tables (asking first), then delete the registrations.
pub fn delete_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let yes = flag(args, "yes");
    let force = flag(args, "force");
    let noask = flag(args, "noask");
    let (inst, pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes to delete."));
    }
    // `actions['drop'](['pipes'], ...)`: its result is discarded.
    let mut drop_args = args.clone();
    drop_args.shift_remove("action");
    let _ = drop_pipes(&drop_args, rt);

    let mut question =
        "Are you sure you want to delete these pipes? This can't be undone!\n".to_string();
    for p in &pipes {
        question.push_str(&format!("    - {}\n", p.pipe));
    }
    let answer = force || yes_no(rt, &question, "n", yes, noask)?;
    if !answer {
        return Ok(SuccessTuple::fail("No pipes deleted."));
    }
    let session = session(rt);
    let (mut successes, mut fails) = (0usize, 0usize);
    for sp in &pipes {
        // `pipe.delete(drop=False)`: delete the registration only.
        let res = if sp.pipe.temporary {
            SuccessTuple::fail("Cannot delete pipes created with `temporary=True` (read-only).")
        } else {
            session.block_on(inst.conn().delete_pipe(&sp.pipe))
        };
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
            "Finished deleting {} pipes.\n    ({successes} succeeded, {fails} failed)",
            pipes.len()
        ),
    })
}

/// `_ask_with_rowcounts(pipes, begin, end, yes, nopretty, noask)`.
fn ask_with_rowcounts(
    rt: &mut Runtime<'_>,
    inst: &Instance,
    pipes: &mut [SelectedPipe],
    args: &ArgsMap,
) -> Result<bool, crate::builtin::ActionError> {
    let begin = bound(args, "begin");
    let end = bound(args, "end");
    let session = session(rt);
    for sp in pipes.iter_mut() {
        hydrate(rt, inst, sp);
        let cols = sp.pipe.columns();
        let has_dt = cols.get("datetime").is_some_and(|v| !v.is_null());
        if (begin.is_some() || end.is_some()) && !has_dt {
            match sp.pipe.guess_datetime() {
                None => warn(
                    rt,
                    &format!(
                        "No datetime could be determined for {}!\n    THIS WILL DELETE THE ENTIRE TABLE!",
                        sp.pipe
                    ),
                ),
                Some(dt) => warn(
                    rt,
                    &format!(
                        "A datetime wasn't specified for {}.\n    Using column \"{dt}\" for datetime bounds...",
                        sp.pipe
                    ),
                ),
            }
        }
    }
    let query = DataQuery {
        begin: begin.clone(),
        end: end.clone(),
        ..Default::default()
    };
    let mut total: i64 = 0;
    let mut options = Vec::new();
    for sp in pipes.iter() {
        let rc = session
            .block_on(inst.conn().get_pipe_rowcount(&sp.pipe, &query))
            .ok()
            .flatten()
            .unwrap_or(0);
        total += rc;
        options.push(format!("{}\n{}\n", sp.pipe, thousands(rc)));
    }
    let opts = OptionsArgs {
        header: Some("Number of Rows to be Deleted".into()),
        ..OptionsArgs::default()
    };
    let text = rt.with_formatter(|f| f.print_options(&options, &opts));
    rt.print(&text);
    let mut question = format!(
        "Are you sure you want to delete {} rows across {} pipe{} in the following range?\n",
        thousands(total),
        pipes.len(),
        plural(pipes.len())
    );
    let begin_s = bound_str(args, "begin");
    let end_s = bound_str(args, "end");
    let range_text = if begin_s.is_some() || end_s.is_some() {
        let mut r = String::new();
        if let Some(b) = &begin_s {
            r.push_str(&format!("\n    Newer than (>=) {b}"));
        }
        if let Some(e) = &end_s {
            r.push_str(&format!(
                "\n    Older than (<){} {e}",
                if begin_s.is_some() { " " } else { "" }
            ));
        }
        r
    } else {
        "\n    Unbounded (delete all rows)!".to_string()
    };
    question.push_str(&range_text);
    question.push_str("\n\n");
    yes_no(rt, &question, "n", flag(args, "yes"), flag(args, "noask"))
}

/// `clear pipes`.
pub fn clear_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if !flag(args, "force") && !ask_with_rowcounts(rt, &inst, &mut pipes, args)? {
        return Ok(SuccessTuple::fail("No rows were deleted."));
    }
    let session = session(rt);
    let query = DataQuery {
        begin: bound(args, "begin"),
        end: bound(args, "end"),
        params: super::params(args),
        ..Default::default()
    };
    let (mut successes, mut fails) = (0usize, 0usize);
    for sp in &pipes {
        let res = session.block_on(inst.conn().clear_pipe(&sp.pipe, &query));
        print_tuple(rt, &res, TupleArgs::default());
        if res.success {
            successes += 1;
        } else {
            fails += 1;
        }
    }
    Ok(SuccessTuple {
        success: successes > 0,
        message: format!(
            "Finished clearing {} pipe{}\n    ({successes} succeeded, {fails} failed).",
            pipes.len(),
            plural(pipes.len())
        ),
    })
}

/// `separate_negation_values(values)`.
fn separate_negation(values: &[String]) -> (Vec<String>, Vec<String>) {
    let prefix = &meerschaum_core::static_config()
        .system
        .fetch_pipes_keys
        .negation_prefix;
    let mut inc = Vec::new();
    let mut exc = Vec::new();
    for v in values {
        match v.strip_prefix(prefix.as_str()) {
            Some(rest) => exc.push(rest.to_string()),
            None => inc.push(v.clone()),
        }
    }
    (inc, exc)
}

/// `tag pipes [tag ...] [_tag ...]`.
pub fn tag_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    if action.is_empty() {
        return Ok(SuccessTuple::fail("No tags were provided."));
    }
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail(
            "No pipes were found with the given keys.",
        ));
    }
    let success_msg = format!(
        "Updated tags for {} pipe{}.",
        pipes.len(),
        plural(pipes.len())
    );
    let (add, remove) = separate_negation(&action);
    let mut edited: Vec<Pipe> = Vec::new();
    for sp in pipes.iter_mut() {
        hydrate(rt, &inst, sp);
        let mut tags = sp.pipe.tags_list();
        let existing: Vec<String> = tags.clone();
        let mut was_edited = false;
        for t in &add {
            if !existing.contains(t) {
                tags.push(t.clone());
                was_edited = true;
            }
        }
        for t in &remove {
            if existing.contains(t) {
                if let Some(i) = tags.iter().position(|x| x == t) {
                    tags.remove(i);
                }
                was_edited = true;
            }
        }
        if was_edited {
            let _ = sp.pipe.set_tags(tags);
            edited.push(sp.pipe.clone());
        }
    }
    if edited.is_empty() {
        return Ok(SuccessTuple::ok(success_msg));
    }
    let session = session(rt);
    let mut failed: Vec<String> = Vec::new();
    for pipe in &edited {
        let res = session.block_on(inst.conn().edit_pipe(pipe, false));
        if !res.success {
            warn(
                rt,
                &format!("Failed to update tags for {pipe}:\n{}", res.message),
            );
            failed.push(res.message);
        }
    }
    if failed.is_empty() {
        return Ok(SuccessTuple::ok(success_msg));
    }
    Ok(SuccessTuple::fail(format!(
        "Failed to update tags for {} pipe{}:\n{}",
        failed.len(),
        plural(failed.len()),
        failed.join("\n")
    )))
}

/// `index pipes [columns ...]`.
pub fn index_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes to index."));
    }
    let session = session(rt);
    let (mut successes, mut fails) = (0usize, 0usize);
    let n = pipes.len();
    for sp in pipes.iter_mut() {
        hydrate(rt, &inst, sp);
        if !nopretty {
            info(rt, &format!("Creating indices for {}...", sp.pipe));
        }
        let cols = (!action.is_empty()).then_some(action.as_slice());
        let res = match inst.sql() {
            Some(sql) => session.block_on(sql.create_pipe_indices(&sp.pipe, cols)),
            None => session.block_on(inst.conn().create_pipe_indices(&sp.pipe)),
        };
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
            "Finished indexing {n} pipe{}\n    ({successes} succeeded, {fails} failed).",
            plural(n)
        ),
    })
}

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
    let defaults = super::pipes::defaults(rt);
    let mut successes: i64 = 0;
    let n = pipes.len();
    for (i, sp) in pipes.iter_mut().enumerate() {
        hydrate(rt, &src_inst, sp);
        info(rt, &format!("Copying {} ({} / {n})...", sp.pipe, i + 1));
        let mut new_pipe = build_pipe(
            sp.pipe.connector_keys(),
            sp.pipe.metric_key(),
            sp.pipe.location_key(),
            &dest.keys(),
            Some(sp.pipe.raw_parameters().clone()),
            &defaults,
        )?;
        let exists = session
            .block_on(dest.conn().get_pipe_id(&new_pipe))
            .ok()
            .flatten()
            .is_some();
        if exists {
            if overwrite == "skip" {
                info(
                    rt,
                    &format!("Skipping {new_pipe} — leaving destination pipe unchanged."),
                );
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
                continue;
            }
        } else {
            let res = register_pipe(rt, &dest, &mut new_pipe);
            if !res.success {
                warn(
                    rt,
                    &format!("Failed to register new {new_pipe}:\n{}", res.message),
                );
                continue;
            }
        }
        successes += 1;
        print_tuple(
            rt,
            &SuccessTuple::ok(format!("Successfully copied {} to {new_pipe}.", sp.pipe)),
            TupleArgs::default(),
        );
        let src_exists = session
            .block_on(src_inst.conn().pipe_exists(&sp.pipe))
            .unwrap_or(false);
        if src_exists && batch_sync != "nothing" {
            info(rt, &format!("Fetching data from {}...", sp.pipe));
            let query = DataQuery {
                begin: bound(args, "begin"),
                end: bound(args, "end"),
                params: super::params(args),
                ..Default::default()
            };
            let df = match batch_sync.as_str() {
                "backtrack" => session.block_on(src_inst.conn().get_backtrack_data(
                    &sp.pipe,
                    meerschaum_sql::pipe::DEFAULT_BACKTRACK_MINUTES,
                    &DataQuery::default(),
                )),
                "filter" => session.block_on(src_inst.conn().get_pipe_data(&sp.pipe, &query)),
                _ => session.block_on(
                    src_inst
                        .conn()
                        .get_pipe_data(&sp.pipe, &DataQuery::default()),
                ),
            };
            info(rt, &format!("Syncing into {new_pipe}..."));
            let res = match df {
                Ok(Some(df)) => {
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
                }
                Ok(None) => SuccessTuple::fail(format!(
                    "You passed `None` instead of data into `sync()` for {new_pipe}.\nOmit the DataFrame to infer fetching."
                )),
                Err(e) => SuccessTuple::fail(e.to_string()),
            };
            print_tuple(rt, &res, TupleArgs::default());
            if !res.success {
                warn(
                    rt,
                    &format!("Failed to copy data from {} to {new_pipe}.", sp.pipe),
                );
                successes -= 1;
            }
        }
    }
    let msg = if successes == 0 {
        "No pipes were copied.".to_string()
    } else {
        format!("Copied {successes} pipe{}.", plural(successes as usize))
    };
    Ok(SuccessTuple {
        success: successes > 0,
        message: msg,
    })
}

/// `register plugins [plugins]`: the checks before the upload, then the archive build and
/// upload (`plugin_admin::register_uploads`).
pub fn register_plugins(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let _repo = super::show::repo_keys(args, rt)?;
    let action = words(args, "action");
    if action.is_empty() || action == [""] {
        return Ok(SuccessTuple::fail("No plugins to register."));
    }
    let installed = crate::builtin::plugin_names(&rt.config().paths().plugins_dirs);
    let mut to_register = Vec::new();
    for p in &action {
        if installed.contains(p) {
            if !to_register.contains(p) {
                to_register.push(p.clone());
            }
        } else {
            warn(
                rt,
                &format!("Plugin '{p}' is not installed and cannot be registered. Ignoring..."),
            );
        }
    }
    if to_register.is_empty() {
        return Ok(SuccessTuple::fail(
            "Finished registering 0 plugins\n    (0 succeeded, 0 failed).",
        ));
    }
    super::plugin_admin::register_uploads(args, rt, &to_register)
}
