//! Pipe management actions: `register pipes` (`actions/register.py`), `edit pipes`
//! (`actions/edit.py`, non-interactive with `--params`), `delete pipes` (`actions/delete.py`),
//! `drop pipes` (`actions/drop.py`), `clear pipes` (`actions/clear.py`), `tag pipes`
//! (`actions/tag.py`), `index pipes` (`actions/index.py`) and `copy pipes` (`actions/copy.py`,
//! the `copy pipes <instance>` path).

use meerschaum_connector::windows::{just_past, split_point};
use meerschaum_connector::{DataQuery, DateBound, InstanceConnector};
use meerschaum_connector_sql::instance::data::get_chunk_bounds;
use meerschaum_core::access::Locks;
use meerschaum_core::pipe::{ChunkInterval, DEFAULT_BACKTRACK_MINUTES, PipeDefaults};
use meerschaum_core::value::{Map, Value, apply_patch_maps};
use meerschaum_core::{Pipe, SuccessTuple};
use meerschaum_kernel::Frame;
use meerschaum_kernel::value::DateTimeValue;
use meerschaum_plugin::NativePlugins;

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::format::progress::Progress;
use crate::format::rich::pretty::PyObj;
use crate::format::{OptionsArgs, TupleArgs};
use crate::value::ArgsMap;

use super::pipes::{SelectedPipe, Selection, build_pipe, explicit_pipes, hydrate, selected_pipes};
use super::show::{bound, bound_str};
use super::{
    Instance, flag, info, plural, pprint, print_tuple, session, string, thousands, warn, words,
    yes_no,
};

/// `Pipe.validate_indices()` (the implementation is `meerschaum_core::Pipe::validate_indices`).
pub fn validate_indices(pipe: &Pipe) -> SuccessTuple {
    pipe.validate_indices()
}

/// The connector half of `Pipe.register()`: a `plugin:` or custom connector's `register(pipe)`
/// returns parameters, which become the base the pipe's own parameters are patched onto
/// (`self.parameters = apply_patch_to_config(params, self.parameters)`), so what was given on the
/// command line or already set on the pipe wins. A connector without `register()` changes
/// nothing; one that raises is reported by the worker and treated as `None`; a return value
/// that is not a dict is a warning and changes nothing. Native hooks run in-process; a Python
/// connector the worker cannot build is skipped (v4 swallows the error of `Pipe.connector`).
fn apply_connector_register(rt: &mut Runtime<'_>, pipe: &mut Pipe) {
    let keys = pipe.connector_keys().to_string();
    let session = session(rt);
    match ask_connector_register(&session, rt.config(), pipe) {
        None => {}
        Some(Ok(outcome)) if outcome.valid => {
            let merged = apply_patch_maps(&outcome.parameters, pipe.raw_parameters());
            pipe.set_parameters(merged);
        }
        Some(Ok(outcome)) => warn(
            rt,
            &format!(
                "Invalid parameters returned from `register()` in connector {keys}:\n{}",
                outcome.repr
            ),
        ),
        Some(Err(e)) => warn(rt, &format!("`register()` failed for {pipe}: {e}")),
    }
}

/// Ask the `plugin:` or custom connector of `pipe` for its `register(pipe)` parameters through
/// its native library or the Python worker. `None`: nothing was asked (a built-in connector, a
/// connector the worker cannot build, or one without `register()`).
pub(crate) fn ask_connector_register(
    session: &super::Session,
    config: &meerschaum_core::config::Config,
    pipe: &Pipe,
) -> Option<Result<meerschaum_worker::connector::RegisterOutcome, String>> {
    if !super::pipes::is_python_connector(pipe.connector_keys()) {
        return None;
    }
    let natives = match session.native_plugins(config) {
        Ok(natives) => natives,
        Err(error) => return Some(Err(error.message)),
    };
    if owns_connector(&natives, pipe.connector_keys()) {
        // Native ownership is exclusive, including plugins without a register hook.
        if !manifest_may_register(session, config, pipe.connector_keys()) {
            return None;
        }
        // The hook may make host calls (`host::plugin_config()`, `host::pipe(...)`), with the
        // trust of this process, as a Python plugin's `register` has.
        let host = crate::native_plugins::local_host(crate::native_plugins::HostEnv::new(
            session.clone(),
            crate::native_plugins::host_config(config),
            natives.clone(),
            Some(pipe.instance_keys().to_string()),
        ));
        return native_connector_register_with_host(&natives, config, pipe, Some(host));
    }
    if !session.worker_enabled() {
        return None;
    }
    if !manifest_may_register(session, config, pipe.connector_keys()) {
        return None;
    }
    let pool = session.pool(config);
    let connector = session
        .block_on(meerschaum_worker::PythonConnector::connect(
            pipe.connector_keys(),
            pool,
        ))
        .ok()?;
    if !connector.info().has_register {
        return None;
    }
    Some(
        session
            .block_on(connector.register(pipe))
            .map_err(|e| e.to_string()),
    )
}

/// Whether a native plugin owns the connector of `keys` (`plugin:<native plugin>` or a type
/// declared by a native plugin's `#[make_connector]`).
pub fn owns_connector(natives: &NativePlugins, keys: &str) -> bool {
    let (typ, label) = super::session::split_keys(keys);
    if typ == "plugin" {
        natives.contains(&label)
    } else {
        natives.connector_plugin(&typ).is_some()
    }
}

/// The `register(pipe)` parameters of a connector a native plugin owns, called in-process.
/// `None`: not native-owned, or the plugin defines no `register`.
pub fn native_connector_register(
    natives: &NativePlugins,
    config: &meerschaum_core::config::Config,
    pipe: &Pipe,
) -> Option<Result<meerschaum_worker::connector::RegisterOutcome, String>> {
    native_connector_register_with_host(natives, config, pipe, None)
}

/// [`native_connector_register`] whose hook makes its host calls through `host`.
pub fn native_connector_register_with_host(
    natives: &NativePlugins,
    config: &meerschaum_core::config::Config,
    pipe: &Pipe,
    host: Option<std::sync::Arc<dyn meerschaum_plugin::HostServices>>,
) -> Option<Result<meerschaum_worker::connector::RegisterOutcome, String>> {
    if !owns_connector(natives, pipe.connector_keys()) {
        return None;
    }
    let (typ, label) = super::session::split_keys(pipe.connector_keys());
    let connector = if typ == "plugin" {
        natives
            .plugin(&label)
            .map(|plugin| plugin.source(serde_json::Value::Null))
    } else {
        let connectors = config
            .get(&["meerschaum", "connectors"])
            .map(|v| v.to_map())
            .unwrap_or_default();
        let attributes = crate::native_plugins::attributes(&connectors, &typ, &label);
        natives.connector(&typ, &label, serde_json::json!(attributes))?
    };
    (|| {
        let mut connector = connector.map_err(|e| e.to_string())?;
        if let Some(host) = host {
            connector = connector.with_host_services(host);
        }
        let reply = match connector.call(
            "register",
            serde_json::json!({
                "pipe": {"keys": pipe.keys(), "parameters": pipe.raw_parameters(),
                         "temporary": pipe.temporary}
            }),
            None,
        ) {
            Ok(reply) => reply,
            Err(meerschaum_connector::ConnectorError::NotImplemented(_)) => {
                return Ok(None);
            }
            Err(e) => return Err(e.to_string()),
        };
        Ok(Some(meerschaum_worker::connector::RegisterOutcome {
            parameters: serde_json::from_value(reply.value.clone()).unwrap_or_default(),
            valid: reply.value.is_null() || reply.value.is_object(),
            defined: true,
            repr: reply.value.to_string(),
        }))
    })()
    .transpose()
}

/// Whether the plugin manifest leaves open that the connector of `keys` implements
/// `register()`, so that a Python worker must be asked. It answers `false` (no worker) for a
/// `plugin:` connector whose plugin imported cleanly and defines no `register` function, and
/// for a connector type that no plugin defines while every plugin imported (v4's
/// `Pipe.connector` then falls back to the keys string and `Pipe.register()` skips the hook).
/// Without this check every `register pipes` of such a pipe started a worker (one per pipe
/// in `compose up`, which registers each pipe with its own `mrsm` command).
fn manifest_may_register(
    session: &super::Session,
    config: &meerschaum_core::config::Config,
    keys: &str,
) -> bool {
    let Ok(manifest) = session.manifest(config) else {
        return true;
    };
    let (typ, label) = super::session::split_keys(keys);
    if typ == "plugin" {
        return match manifest.plugins.get(&label) {
            Some(plugin) if plugin.import_error.is_none() => {
                plugin.data_functions.iter().any(|f| f == "register")
            }
            _ => true,
        };
    }
    match manifest.connector_types.get(&typ) {
        Some(connector_type) => connector_type.methods.iter().any(|m| m == "register"),
        None => {
            !manifest.load_errors.is_empty()
                || manifest.plugins.values().any(|p| p.import_error.is_some())
        }
    }
}

/// `Pipe.register()`.
pub fn register_pipe(rt: &mut Runtime<'_>, inst: &Instance, pipe: &mut Pipe) -> SuccessTuple {
    if pipe.temporary {
        return SuccessTuple::fail(
            "Cannot register pipes created with `temporary=True` (read-only).",
        );
    }
    apply_connector_register(rt, pipe);
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
    let progress = Progress::start(rt, args, "Registering pipes");
    let inst = super::get_instance(args, rt)?;
    let pipes = explicit_pipes(rt, &inst, &sel)?;
    progress.set_total(pipes.len());
    let mut success_pipes = 0usize;
    let mut failed_pipes = 0usize;
    for sp in pipes {
        progress.inc();
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
    // `edit pipes access` (rc15 wave 2, AC10): owner, team, visibility and edit mode.
    if action.first().is_some_and(|a| a == "access") {
        return super::access::edit_pipes_access(args, rt);
    }
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
    let progress = Progress::start(rt, args, "Reading pipe registrations");
    progress.set_total(pipes.len());
    for sp in pipes.iter_mut() {
        hydrate(rt, &inst, sp);
        progress.inc();
        let target = drop_target(&inst, &sp.pipe);
        if seen.contains(&target) {
            continue;
        }
        question.push_str(&format!("    - {target}\n"));
        seen.push(target);
    }
    question.push('\n');
    drop(progress);
    let answer = force || yes_no(rt, &question, "n", yes, noask)?;
    if !answer {
        return Ok(SuccessTuple::fail("No pipes were dropped."));
    }
    let session = session(rt);
    let (mut successes, mut fails) = (0usize, 0usize);
    let progress = Progress::start(rt, args, "Dropping tables");
    progress.set_total(pipes.len());
    for sp in &pipes {
        let res = session.block_on(inst.conn().drop_pipe(&sp.pipe));
        progress.inc();
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
    // Locks (rc15 AC5): deleting drops the tables first, so a `drop` or a `delete` lock refuses
    // it before anything changes (one locked pipe stops the command; `-y` and `--force` never
    // get past a lock).
    let checks = session(rt);
    let conn = inst.conn();
    let refusals: Vec<String> = pipes
        .iter()
        .filter_map(|sp| {
            checks.block_on(meerschaum_connector::lock_refusal(
                conn.as_ref(),
                &sp.pipe,
                meerschaum_core::access::LockOp::DeleteAndDrop,
            ))
        })
        .map(|refused| refused.message)
        .collect();
    if !refusals.is_empty() {
        return Ok(SuccessTuple::fail(refusals.join("\n")));
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
    let progress = Progress::start(rt, args, "Deleting pipes");
    progress.set_total(pipes.len());
    for sp in &pipes {
        progress.inc();
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
    let progress = Progress::start(rt, args, "Reading pipe registrations");
    progress.set_total(pipes.len());
    for sp in pipes.iter_mut() {
        hydrate(rt, inst, sp);
        progress.inc();
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
    progress.set_message("Counting the rows to delete");
    progress.set_total(pipes.len());
    progress.set_done(0);
    for sp in pipes.iter() {
        let rc = session
            .block_on(inst.conn().get_pipe_rowcount(&sp.pipe, &query))
            .ok()
            .flatten()
            .unwrap_or(0);
        progress.inc();
        total += rc;
        options.push(format!("{}\n{}\n", sp.pipe, thousands(rc)));
    }
    drop(progress);
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
    let progress = Progress::start(rt, args, "Clearing pipes");
    progress.set_total(pipes.len());
    for (index, sp) in pipes.iter().enumerate() {
        progress.set_done(index);
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
        hydrate(rt, &src_inst, sp);
        info(rt, &format!("Copying {} ({} / {n})...", sp.pipe, i + 1));
        // AC4: a copy keeps the source's locks and never takes its access (owner, team,
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
        let exists = session
            .block_on(dest.conn().get_pipe_id(&new_pipe))
            .ok()
            .flatten()
            .is_some();
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
            hydrate(rt, &dest, &mut existing);
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
        let src_exists = session
            .block_on(src_inst.conn().pipe_exists(&sp.pipe))
            .unwrap_or(false);
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
                    let cap = if src_inst.conn().connector_keys().starts_with("api:") {
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
enum CopyData {
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
    fn parse(choice: &str) -> Option<CopyData> {
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
async fn incremental_window(
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
fn incremental_query(
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
fn backtrack_from(
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
fn axis_pipe(pipe: &Pipe, sync_time: &DateBound) -> Pipe {
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
fn backtrack_minutes(pipe: &Pipe, default_backtrack_minutes: i64, defaults: &PipeDefaults) -> i64 {
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
async fn copy_windows(
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
async fn backtrack_windows(
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
fn copy_chunks(
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
fn bound_before(a: &DateBound, b: &DateBound, pipe: &Pipe) -> Result<bool, String> {
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
fn copy_summary(
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

#[cfg(test)]
mod copy_tests {
    use super::{
        CopyData, DataQuery, DateBound, PipeDefaults, axis_pipe, backtrack_from, backtrack_minutes,
        backtrack_windows, bound_before, copy_chunks, copy_summary, copy_windows,
        incremental_query, incremental_window, just_past, split_point,
    };
    use chrono::{TimeZone, Utc};
    use meerschaum_core::{Pipe, Value};

    fn pipe(params: &str) -> Pipe {
        Pipe::builder("sql:src", "temp")
            .parameters(Value::from_json_str(params).unwrap().to_map())
            .build()
            .unwrap()
    }

    fn at(hour: u32) -> DateBound {
        DateBound::utc(Utc.with_ymd_and_hms(2024, 1, 2, hour, 0, 0).unwrap())
    }

    #[test]
    fn the_modes_are_v4_choices() {
        assert_eq!(CopyData::parse("incremental"), Some(CopyData::Incremental));
        assert_eq!(CopyData::parse("all"), Some(CopyData::All));
        assert_eq!(CopyData::parse("everything"), None);
    }

    #[test]
    fn the_watermark_is_the_sync_time_minus_the_pipes_backtrack() {
        let d = PipeDefaults::default();
        let dt = pipe(r#"{"columns": {"datetime": "ts"}}"#);
        assert_eq!(
            backtrack_from(&dt, at(12), 1440, &d).py_str(),
            "2024-01-01 12:00:00+00:00"
        );
        let own = pipe(r#"{"columns": {"datetime": "ts"}, "fetch": {"backtrack_minutes": 120}}"#);
        assert_eq!(
            backtrack_from(&own, at(12), 1440, &d).py_str(),
            "2024-01-02 10:00:00+00:00"
        );
        // An integer axis in milliseconds: one minute is 60,000 units.
        let ms = pipe(
            r#"{"columns": {"datetime": "t"}, "dtypes": {"t": "int"}, "precision": "ms",
                "fetch": {"backtrack_minutes": 1}}"#,
        );
        let big = 1_700_000_000_123_456_789_i64;
        assert!(matches!(
            backtrack_from(&ms, DateBound::Int(big), 1440, &d),
            DateBound::Int(v) if v == big - 60_000
        ));
        // Integer values without a dtype: the axis is an integer one, in raw minutes.
        assert!(matches!(
            backtrack_from(&dt, DateBound::Int(10_000), 1440, &d),
            DateBound::Int(8_560)
        ));
        assert!(axis_pipe(&dt, &DateBound::Int(1)).is_int_axis());
        assert!(!axis_pipe(&dt, &at(1)).is_int_axis());
        // PARITY: a datetime sync time on a pipe whose dtype says int keeps the sync time.
        let int_dtype = pipe(r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "int"}}"#);
        assert_eq!(
            backtrack_from(&int_dtype, at(12), 1440, &d).py_str(),
            at(12).py_str()
        );
    }

    #[test]
    fn backtrack_reads_the_source_pipes_own_window() {
        let d = PipeDefaults::default();
        let own = pipe(r#"{"columns": {"datetime": "ts"}, "fetch": {"backtrack_minutes": 120}}"#);
        assert_eq!(backtrack_minutes(&own, 1440, &d), 120);
        assert_eq!(
            backtrack_minutes(&pipe(r#"{"columns": {"datetime": "ts"}}"#), 1440, &d),
            1440
        );
        let ms = pipe(
            r#"{"columns": {"datetime": "t"}, "dtypes": {"t": "int"}, "precision": "ms",
                "fetch": {"backtrack_minutes": 2}}"#,
        );
        assert_eq!(backtrack_minutes(&ms, 1440, &d), 120_000);
    }

    #[test]
    fn begin_only_raises_the_watermark_and_an_empty_window_reads_nothing() {
        let p = pipe(r#"{"columns": {"datetime": "ts"}}"#);
        let q = |begin: Option<DateBound>, end: Option<DateBound>| DataQuery {
            begin,
            end,
            ..Default::default()
        };
        let begin_of = |(query, _): (Option<DataQuery>, String)| query.unwrap().begin.unwrap();
        let (query, note) = incremental_query(&p, Some(at(10)), &q(Some(at(12)), None)).unwrap();
        assert_eq!(query.unwrap().begin.unwrap().py_str(), at(12).py_str());
        assert!(note.contains("--begin"), "{note}");
        let raised = incremental_query(&p, Some(at(12)), &q(Some(at(10)), None)).unwrap();
        assert_eq!(begin_of(raised).py_str(), at(12).py_str());
        // `--end` at or before the window's start: no query, so the source is never read.
        assert!(
            incremental_query(&p, Some(at(12)), &q(None, Some(at(12))))
                .unwrap()
                .0
                .is_none()
        );
        assert!(
            incremental_query(&p, None, &q(Some(at(5)), Some(at(4))))
                .unwrap()
                .0
                .is_none()
        );
        // No rows yet: everything (or from `--begin`).
        let (all, note) = incremental_query(&p, None, &q(None, None)).unwrap();
        assert!(all.unwrap().begin.is_none());
        assert!(
            note.ends_with("has no rows yet: copying all rows."),
            "{note}"
        );
        // Integer watermark, datetime `--begin`: refused, never guessed.
        assert!(incremental_query(&p, Some(DateBound::Int(5)), &q(Some(at(1)), None)).is_err());
    }

    /// A destination (or source) whose row count and sync times are whatever the test says.
    struct Destination {
        rows: Result<Option<i64>, String>,
        /// The oldest and the newest value of the datetime axis.
        sync_times: Result<(Option<DateBound>, Option<DateBound>), String>,
    }

    #[async_trait::async_trait]
    impl meerschaum_connector::Connector for Destination {
        fn connector_type(&self) -> &str {
            "fake"
        }

        fn label(&self) -> &str {
            "destination"
        }

        async fn test_connection(&self) -> meerschaum_connector::ConnectorResult<bool> {
            Ok(true)
        }
    }

    #[async_trait::async_trait]
    impl meerschaum_connector::InstanceConnector for Destination {
        async fn register_pipe(&self, _: &Pipe) -> meerschaum_core::SuccessTuple {
            meerschaum_core::SuccessTuple::success()
        }

        async fn get_pipe_attributes(
            &self,
            _: &Pipe,
        ) -> meerschaum_connector::ConnectorResult<meerschaum_core::Map> {
            Ok(meerschaum_core::Map::new())
        }

        async fn get_pipe_id(
            &self,
            _: &Pipe,
        ) -> meerschaum_connector::ConnectorResult<Option<meerschaum_connector::PipeId>> {
            Ok(None)
        }

        async fn fetch_pipes_keys(
            &self,
            _: &meerschaum_connector::PipesKeysQuery,
        ) -> meerschaum_connector::ConnectorResult<meerschaum_connector::PipesKeys> {
            Ok(meerschaum_connector::PipesKeys::List(Vec::new()))
        }

        async fn pipe_exists(&self, _: &Pipe) -> meerschaum_connector::ConnectorResult<bool> {
            Ok(true)
        }

        async fn drop_pipe(&self, _: &Pipe) -> meerschaum_core::SuccessTuple {
            meerschaum_core::SuccessTuple::success()
        }

        async fn sync_pipe(
            &self,
            _: &Pipe,
            _: meerschaum_kernel::Frame,
            _: &meerschaum_connector::SyncOptions,
        ) -> meerschaum_core::SuccessTuple {
            panic!("an incremental window never writes")
        }

        async fn get_pipe_data(
            &self,
            _: &Pipe,
            _: &DataQuery,
        ) -> meerschaum_connector::ConnectorResult<Option<meerschaum_kernel::Frame>> {
            panic!("an incremental window never reads rows")
        }

        async fn get_sync_time(
            &self,
            _: &Pipe,
            _: Option<&meerschaum_core::Map>,
            newest: bool,
        ) -> meerschaum_connector::ConnectorResult<Option<DateBound>> {
            match &self.sync_times {
                Ok((oldest, newest_value)) => Ok(if newest {
                    newest_value.clone()
                } else {
                    oldest.clone()
                }),
                Err(e) => Err(meerschaum_connector::ConnectorError::Backend(e.clone())),
            }
        }

        async fn get_pipe_rowcount(
            &self,
            _: &Pipe,
            _: &DataQuery,
        ) -> meerschaum_connector::ConnectorResult<Option<i64>> {
            self.rows
                .clone()
                .map_err(meerschaum_connector::ConnectorError::Backend)
        }

        async fn get_pipe_columns_types(
            &self,
            _: &Pipe,
        ) -> meerschaum_connector::ConnectorResult<indexmap::IndexMap<String, String>> {
            Ok(indexmap::IndexMap::new())
        }
    }

    fn window(rows: Result<Option<i64>, String>) -> Result<(Option<DataQuery>, String), String> {
        let p = pipe(r#"{"columns": {"datetime": "ts"}}"#);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(incremental_window(
            &Destination {
                rows,
                sync_times: Ok((None, None)),
            },
            &p,
            &DataQuery::default(),
            1440,
            &PipeDefaults::default(),
        ))
    }

    #[test]
    fn only_a_destination_proven_empty_is_filled() {
        // A count that cannot be determined is not an empty destination: refused with no
        // query, so the source is never read.
        let unknown = window(Ok(None)).unwrap_err();
        assert!(unknown.contains("cannot be determined"), "{unknown}");
        let failed = window(Err("connection reset".into())).unwrap_err();
        assert!(failed.contains("connection reset"), "{failed}");
        let negative = window(Ok(Some(-1))).unwrap_err();
        assert!(negative.contains("Invalid row count"), "{negative}");
        let rows = window(Ok(Some(3))).unwrap_err();
        assert!(rows.contains("has rows but no sync time"), "{rows}");
        // Counted and empty: the first fill.
        let (query, note) = window(Ok(Some(0))).unwrap();
        assert!(query.unwrap().begin.is_none());
        assert!(
            note.ends_with("has no rows yet: copying all rows."),
            "{note}"
        );
    }

    #[test]
    fn bounds_compare_only_on_the_same_axis() {
        let p = pipe(r#"{"columns": {"datetime": "ts"}}"#);
        assert_eq!(bound_before(&at(1), &at(2), &p), Ok(true));
        assert_eq!(bound_before(&at(2), &at(2), &p), Ok(false));
        assert_eq!(
            bound_before(&DateBound::Int(1), &DateBound::Int(2), &p),
            Ok(true)
        );
        assert!(bound_before(&at(1), &DateBound::Int(2), &p).is_err());
    }

    #[test]
    fn any_failed_pipe_fails_the_copy() {
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let ok = copy_summary("sql:dest", &s(&["a", "b"]), &[], &[]);
        assert!(ok.success);
        assert_eq!(ok.message, "Copied 2 pipes.");
        let partial = copy_summary(
            "sql:dest",
            &s(&["a"]),
            &s(&["c"]),
            &[("b".to_string(), "table is locked\nmore".to_string())],
        );
        assert!(!partial.success);
        assert_eq!(
            partial.message,
            "Copied 1 of 3 pipes to 'sql:dest' (1 failed, 1 skipped).\n  - b: failed: table is locked\n  - c: skipped (already on 'sql:dest'; pass --force to overwrite)"
        );
        // Pipes already on the destination are not a failure.
        let skipped = copy_summary("sql:dest", &[], &s(&["a"]), &[]);
        assert!(skipped.success);
        assert_eq!(
            skipped.message,
            "Copied 0 of 1 pipe to 'sql:dest' (1 skipped).\n  - a: skipped (already on 'sql:dest'; pass --force to overwrite)"
        );
        assert!(!copy_summary("sql:dest", &[], &[], &[]).success);
    }

    /// A source on an integer axis (`axis[i]` is row `i`'s value) behind a server that answers at
    /// most `limit` rows per read, silently, as `/data` does.
    fn capped_reader(
        axis: Vec<i64>,
    ) -> impl FnMut(&DataQuery) -> meerschaum_connector::ConnectorResult<Option<meerschaum_kernel::Frame>>
    {
        use meerschaum_kernel::value::Value as KValue;
        move |window: &DataQuery| {
            let inside = |v: i64| {
                window
                    .begin
                    .as_ref()
                    .is_none_or(|b| matches!(b, DateBound::Int(b) if v >= *b))
                    && window
                        .end
                        .as_ref()
                        .is_none_or(|e| matches!(e, DateBound::Int(e) if v < *e))
            };
            let docs: Vec<indexmap::IndexMap<String, KValue>> = axis
                .iter()
                .copied()
                .filter(|v| inside(*v))
                .take(window.limit.map_or(usize::MAX, |l| l as usize))
                .map(|v| [("n".to_string(), KValue::Int(v))].into_iter().collect())
                .collect();
            Ok(Some(
                meerschaum_kernel::docs::frame_from_docs(&docs).unwrap(),
            ))
        }
    }

    fn int_window(begin: Option<i64>, end: Option<i64>) -> DataQuery {
        DataQuery {
            begin: begin.map(DateBound::Int),
            end: end.map(DateBound::Int),
            ..Default::default()
        }
    }

    #[test]
    fn a_full_read_is_split_until_nothing_is_cut() {
        let pipe = pipe(r#"{"columns": {"datetime": "n"}, "dtypes": {"n": "int"}}"#);
        let mut synced = 0usize;
        let res = copy_chunks(
            &pipe,
            &pipe,
            vec![
                int_window(Some(0), Some(10)),
                int_window(Some(10), Some(20)),
            ],
            Some(3),
            capped_reader((0..20).collect()),
            |df| {
                synced += df.height();
                meerschaum_core::SuccessTuple::success()
            },
        );
        assert!(res.success, "{}", res.message);
        assert_eq!(synced, 20, "rows past the server's limit were lost");
        // Without the limit the same windows read everything in two reads.
        let mut whole = 0usize;
        let res = copy_chunks(
            &pipe,
            &pipe,
            vec![
                int_window(Some(0), Some(10)),
                int_window(Some(10), Some(20)),
            ],
            None,
            capped_reader((0..20).collect()),
            |df| {
                whole += df.height();
                meerschaum_core::SuccessTuple::success()
            },
        );
        assert!(res.success && whole == 20 && res.message.contains("Synced 2 chunks"));
    }

    #[test]
    fn a_full_read_that_cannot_be_split_fails_the_copy() {
        let pipe = pipe(r#"{"columns": {"datetime": "n"}, "dtypes": {"n": "int"}}"#);
        // Five rows at one axis value: no smaller window holds fewer.
        for window in [int_window(Some(4), Some(5)), int_window(None, None)] {
            let res = copy_chunks(
                &pipe,
                &pipe,
                vec![window],
                Some(3),
                capped_reader(vec![4; 5]),
                |_| meerschaum_core::SuccessTuple::success(),
            );
            assert!(!res.success);
            assert!(
                res.message.contains("the rest would be lost"),
                "{}",
                res.message
            );
        }
        // A read the server refused (nothing) is never an empty chunk.
        let res = copy_chunks(
            &pipe,
            &pipe,
            vec![int_window(Some(0), Some(10))],
            Some(3),
            |_: &DataQuery| Ok(None),
            |_| meerschaum_core::SuccessTuple::success(),
        );
        assert!(!res.success, "{}", res.message);
    }

    #[test]
    fn split_points_and_the_end_past_the_newest_value() {
        assert_eq!(
            split_point(&DateBound::Int(0), &DateBound::Int(10)),
            Some(DateBound::Int(5))
        );
        assert_eq!(split_point(&DateBound::Int(4), &DateBound::Int(5)), None);
        let at = |m: u32, s: u32| {
            DateBound::Datetime(meerschaum_kernel::value::DateTimeValue {
                utc: Utc
                    .with_ymd_and_hms(2024, 1, 1, 0, m, s)
                    .unwrap()
                    .naive_utc(),
                tz: None,
            })
        };
        assert_eq!(just_past(at(5, 30)), Some(at(6, 0)));
        assert_eq!(just_past(DateBound::Int(7)), Some(DateBound::Int(8)));
        assert_eq!(split_point(&at(0, 0), &at(2, 0)), Some(at(1, 0)));
    }

    fn source(sync_times: Result<(Option<DateBound>, Option<DateBound>), String>) -> Destination {
        Destination {
            rows: Ok(None),
            sync_times,
        }
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    #[test]
    fn windows_carry_resolved_bounds_and_an_unreadable_bound_is_an_error() {
        let dated = pipe(r#"{"columns": {"datetime": "n"}, "dtypes": {"n": "int"}}"#);
        let defaults = PipeDefaults::default();
        let all = DataQuery::default();
        // One chunk: its resolved bounds (so a limited read can split it), not an open query.
        let windows = block_on(copy_windows(
            &source(Ok((Some(DateBound::Int(0)), Some(DateBound::Int(19))))),
            &dated,
            &all,
            &defaults,
        ))
        .unwrap();
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].begin, Some(DateBound::Int(0)));
        assert_eq!(windows[0].end, Some(DateBound::Int(20)));
        // A sync time that cannot be read: an error, never the whole history.
        let unreadable = block_on(copy_windows(
            &source(Err("the source is down".into())),
            &dated,
            &all,
            &defaults,
        ));
        assert!(unreadable.unwrap_err().contains("the source is down"));
        // Explicit bounds need no sync time; a pipe without an axis is one read, as given.
        let bounded = DataQuery {
            begin: Some(DateBound::Int(3)),
            end: Some(DateBound::Int(7)),
            ..Default::default()
        };
        let windows = block_on(copy_windows(
            &source(Err("unused".into())),
            &dated,
            &bounded,
            &defaults,
        ))
        .unwrap();
        assert_eq!(
            (windows[0].begin.clone(), windows[0].end.clone()),
            (Some(DateBound::Int(3)), Some(DateBound::Int(7)))
        );
        // The newest value is the axis's largest: the window stays open at its end (no end past
        // it exists), never an end that cuts off that row.
        let windows = block_on(copy_windows(
            &source(Ok((
                Some(DateBound::Int(0)),
                Some(DateBound::Int(i64::MAX)),
            ))),
            &dated,
            &all,
            &defaults,
        ))
        .unwrap();
        assert_eq!(windows.len(), 1);
        assert!(windows[0].end.is_none(), "{windows:?}");
        let flat = pipe(r#"{"columns": {"id": "station"}}"#);
        let windows = block_on(copy_windows(
            &source(Err("unused".into())),
            &flat,
            &all,
            &defaults,
        ))
        .unwrap();
        assert_eq!(windows.len(), 1);
        assert!(windows[0].begin.is_none() && windows[0].end.is_none());
    }

    #[test]
    fn a_limited_backtrack_copy_is_split_never_cut() {
        let p = pipe(
            r#"{"columns": {"datetime": "n"}, "dtypes": {"n": "int"}, "fetch": {"backtrack_minutes": 10}}"#,
        );
        let defaults = PipeDefaults::default();
        let src = source(Ok((Some(DateBound::Int(0)), Some(DateBound::Int(39)))));
        let windows = block_on(backtrack_windows(&src, &p, 1440, &defaults)).unwrap();
        let Some(DateBound::Int(begin)) = windows[0].begin.clone() else {
            panic!("{windows:?}");
        };
        assert!(
            begin > 0 && begin < 39,
            "the backtrack window starts at {begin}"
        );
        assert_eq!(windows.last().unwrap().end, Some(DateBound::Int(40)));
        let mut synced = 0usize;
        let res = copy_chunks(
            &p,
            &p,
            windows,
            Some(3),
            capped_reader((0..40).collect()),
            |df| {
                synced += df.height();
                meerschaum_core::SuccessTuple::success()
            },
        );
        assert!(res.success, "{}", res.message);
        assert_eq!(
            synced,
            (40 - begin) as usize,
            "rows past the limit were lost"
        );
        // No sync time to read: an error, not the server's first rows.
        assert!(
            block_on(backtrack_windows(
                &source(Err("down".into())),
                &p,
                1440,
                &defaults
            ))
            .is_err()
        );
    }
}
