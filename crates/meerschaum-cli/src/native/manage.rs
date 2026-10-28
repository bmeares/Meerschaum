//! Pipe management actions: `register pipes` (`actions/register.py`), `edit pipes`
//! (`actions/edit.py`, non-interactive with `--params`), `delete pipes` (`actions/delete.py`),
//! `drop pipes` (`actions/drop.py`), `clear pipes` (`actions/clear.py`), `tag pipes`
//! (`actions/tag.py`), `index pipes` (`actions/index.py`) and `copy pipes` (`actions/copy.py`,
//! the `copy pipes <instance>` path).

use meerschaum_connector::DataQuery;
use meerschaum_core::access::Locks;
use meerschaum_core::value::{Map, Value, apply_patch_maps};
use meerschaum_core::{Pipe, SuccessTuple};
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
        if exists {
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
                    meerschaum_connector_sql::pipe::DEFAULT_BACKTRACK_MINUTES,
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
                failed.push((sp.pipe.to_string(), format!("data: {}", res.message)));
                continue;
            }
        }
        // The source's locks, added to the destination's own (a copy never unlocks), with the
        // destination's `lock_pipe` (an `api:` destination asks its server).
        if dest_locks.with(&source_locks).kinds() != dest_locks.kinds() {
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
    use super::copy_summary;

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
}
