//! The actions that used to fall through to the Python worker's `core_action`
//! (`meerschaum/actions/*.py`), wave 15 group A, and the dispatch that routes them:
//!
//! - [`run`]: the entry point `dispatch::run_action` calls for any function name [`handles`]
//!   accepts: `login`, `os`, `sql`, `stack`, `api`, `bootstrap`, the connector, token, cache and
//!   index actions, `show schedules|targets|tokens`, `start connectors|gui` and the main
//!   actions' `choose_subaction`.
//! - [`choose_subaction`]: v4's fallback for a main action (`drop`, `show`, ...) whose word did
//!   not resolve to a subaction function (an unknown word, an alias like `drop index`).

use meerschaum_core::SuccessTuple;
use meerschaum_core::value::Value;

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::rich::console::{Group, Overflow, Renderable};
use crate::format::rich::table::{Column, HEAVY_HEAD, Table};
use crate::format::{OptionsArgs, TupleArgs};
use crate::value::{ArgsMap, PyValue};

use super::pipes::{hydrate, pipe_keys, pipe_meta, pipe_str, selected_pipes};
use super::show::dumps;
use super::{flag, info, plural, print_tuple, session, warn, words, yes_no};

/// Every function name [`run`] implements.
const FUNCTIONS: &[&str] = &[
    "login",
    "os",
    "sql",
    "stack",
    "api",
    "_api_start",
    "_bootstrap_connectors",
    "_bootstrap_pipes",
    "_bootstrap_jobs",
    "_clear_cache",
    "_delete_cache",
    "_delete_config",
    "_delete_connectors",
    "_copy_connectors",
    "_register_connectors",
    "_register_tokens",
    "_edit_tokens",
    "_edit_definition",
    "_show_tokens",
    "_show_schedules",
    "_show_targets",
    "_start_connectors",
    "_start_gui",
    "_show_daemons",
    "_start_daemons",
    "_stop_daemons",
    "_restart_daemons",
    "_drop_indices",
    "_flush_indices",
    "_flush_pipes",
];

/// The main actions that dispatch through `choose_subaction` and their options (`name`,
/// `function`), including v4's aliases.
pub const MAINS: &[(&str, &[(&str, &str)])] = &[
    ("analyze", &[("pipes", "_analyze_pipes")]),
    (
        "attach",
        &[("jobs", "_attach_jobs"), ("logs", "_attach_logs")],
    ),
    (
        "bootstrap",
        &[
            ("pipes", "_bootstrap_pipes"),
            ("connectors", "_bootstrap_connectors"),
            ("plugins", "_bootstrap_plugins"),
            ("jobs", "_bootstrap_jobs"),
        ],
    ),
    (
        "clear",
        &[("pipes", "_clear_pipes"), ("cache", "_clear_cache")],
    ),
    ("compress", &[("pipes", "_compress_pipes")]),
    (
        "copy",
        &[("pipes", "_copy_pipes"), ("connectors", "_copy_connectors")],
    ),
    ("decompress", &[("pipes", "_decompress_pipes")]),
    ("deduplicate", &[("pipes", "_deduplicate_pipes")]),
    (
        "delete",
        &[
            ("config", "_delete_config"),
            ("pipes", "_delete_pipes"),
            ("plugins", "_delete_plugins"),
            ("users", "_delete_users"),
            ("connectors", "_delete_connectors"),
            ("jobs", "_delete_jobs"),
            ("venvs", "_delete_venvs"),
            ("cache", "_delete_cache"),
        ],
    ),
    (
        "drop",
        &[
            ("pipes", "_drop_pipes"),
            ("indices", "_drop_indices"),
            ("index", "_drop_indices"),
            ("indexes", "_drop_indices"),
        ],
    ),
    (
        "edit",
        &[
            ("config", "_edit_config"),
            ("pipes", "_edit_pipes"),
            ("definition", "_edit_definition"),
            ("users", "_edit_users"),
            ("plugins", "_edit_plugins"),
            ("jobs", "_edit_jobs"),
            ("tokens", "_edit_tokens"),
        ],
    ),
    (
        "flush",
        &[
            ("pipes", "_flush_pipes"),
            ("indices", "_flush_indices"),
            ("index", "_flush_indices"),
            ("indexes", "_flush_indices"),
        ],
    ),
    ("index", &[("pipes", "_index_pipes")]),
    (
        "install",
        &[
            ("plugins", "_install_plugins"),
            ("packages", "_install_packages"),
            ("required", "_install_required"),
            ("drivers", "_install_drivers"),
        ],
    ),
    ("partition", &[("pipes", "_partition_pipes")]),
    ("pause", &[("jobs", "_pause_jobs")]),
    ("plan", &[("pipes", "_plan_pipes")]),
    ("prune", &[("venvs", "_prune_venvs")]),
    (
        "register",
        &[
            ("pipes", "_register_pipes"),
            ("plugins", "_register_plugins"),
            ("users", "_register_users"),
            ("connectors", "_register_connectors"),
            ("tokens", "_register_tokens"),
        ],
    ),
    (
        "restart",
        &[("jobs", "_restart_jobs"), ("daemons", "_restart_daemons")],
    ),
    ("setup", &[("plugins", "_setup_plugins")]),
    (
        "show",
        &[
            ("actions", "_show_actions"),
            ("pipes", "_show_pipes"),
            ("config", "_show_config"),
            ("environment", "_show_environment"),
            ("version", "_show_version"),
            ("connectors", "_show_connectors"),
            ("arguments", "_show_arguments"),
            ("data", "_show_data"),
            ("columns", "_show_columns"),
            ("rowcounts", "_show_rowcounts"),
            ("sizes", "_show_sizes"),
            ("partitions", "_show_partitions"),
            ("plugins", "_show_plugins"),
            ("packages", "_show_packages"),
            ("help", "_show_help"),
            ("users", "_show_users"),
            ("jobs", "_show_jobs"),
            ("logs", "_show_logs"),
            ("tags", "_show_tags"),
            ("targets", "_show_targets"),
            ("schedules", "_show_schedules"),
            ("venvs", "_show_venvs"),
            ("tokens", "_show_tokens"),
            ("daemons", "_show_daemons"),
            ("graph", "_show_graph"),
            ("runs", "_show_runs"),
            ("drivers", "_show_drivers"),
        ],
    ),
    (
        "start",
        &[
            ("api", "_start_api"),
            ("jobs", "_start_jobs"),
            ("gui", "_start_gui"),
            ("webterm", "_start_webterm"),
            ("connectors", "_start_connectors"),
            ("pipeline", "_start_pipeline"),
            ("daemons", "_start_daemons"),
            ("mcp", "_start_mcp"),
        ],
    ),
    (
        "stop",
        &[("jobs", "_stop_jobs"), ("daemons", "_stop_daemons")],
    ),
    ("sync", &[("pipes", "_sync_pipes")]),
    ("tag", &[("pipes", "_tag_pipes")]),
    ("vacuum", &[("pipes", "_vacuum_pipes")]),
];

/// Whether [`run`] implements the function (`dispatch::run_action` calls it before the
/// worker fallback).
pub fn handles(function: &str) -> bool {
    FUNCTIONS.contains(&function) || MAINS.iter().any(|(main, _)| *main == function)
}

/// Run the function (`None` when it is not one of [`handles`]).
pub fn run(function: &str, args: &ArgsMap, rt: &mut Runtime<'_>) -> Option<ActionResult> {
    Some(match function {
        "login" => super::login::login(args, rt),
        "os" => super::os_sql_stack::os(args, rt),
        "sql" => super::os_sql_stack::sql(args, rt),
        "stack" => super::os_sql_stack::stack(args, rt),
        "api" => api(args, rt),
        "_api_start" => api_start(args, rt),
        "_bootstrap_connectors" => super::bootstrap::bootstrap_connectors(args, rt),
        "_bootstrap_pipes" => super::bootstrap::bootstrap_pipes(args, rt),
        "_bootstrap_jobs" => super::bootstrap::bootstrap_jobs(args, rt),
        "_clear_cache" | "_delete_cache" => super::cache::delete_cache(args, rt),
        "_delete_config" => super::connectors::delete_config(args, rt),
        "_delete_connectors" => super::connectors::delete_connectors(args, rt),
        "_copy_connectors" => super::connectors::copy_connectors(args, rt),
        "_register_connectors" => super::connectors::register_connectors(args, rt),
        "_register_tokens" => super::tokens::register_tokens(args, rt),
        "_edit_tokens" => super::tokens::edit_tokens(args, rt),
        "_edit_definition" => super::definition::edit_definition(args, rt),
        "_show_tokens" => super::tokens::show_tokens(args, rt),
        "_show_schedules" => show_schedules(args, rt),
        "_show_targets" => show_targets(args, rt),
        "_start_connectors" => super::connectors::start_connectors(args, rt),
        "_start_gui" => super::gui::start_gui(args, rt),
        "_show_daemons" => show_daemons(),
        "_start_daemons" | "_restart_daemons" => start_daemons(),
        "_stop_daemons" => stop_daemons(rt),
        "_drop_indices" => drop_indices(args, rt),
        "_flush_indices" => flush_indices(args, rt),
        "_flush_pipes" => flush_pipes(args, rt),
        main if MAINS.iter().any(|(m, _)| *m == main) => choose_subaction(main, args, rt),
        _ => return None,
    })
}

/// `choose_subaction(action, options)` for a main action function: run the option the first
/// word names (a `+s` plural is accepted), else list the options and fail.
pub fn choose_subaction(main: &str, args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let options = MAINS
        .iter()
        .find(|(m, _)| *m == main)
        .map(|(_, o)| *o)
        .unwrap_or(&[]);
    let mut action = words(args, "action");
    if action.is_empty() {
        action.push(String::new());
    }
    let choice = action[0].clone();
    let find = |word: &str| options.iter().find(|(name, _)| *name == word);
    let found = find(&choice).or_else(|| find(&format!("{choice}s")));
    let Some((_, function)) = found else {
        warn(rt, &format!("Cannot {main} '{choice}'. Choose one:"));
        let mut names: Vec<&str> = options.iter().map(|(n, _)| *n).collect();
        names.sort_unstable();
        let mut text = String::new();
        for option in names {
            text.push_str(&format!("  - {main} {option}\n"));
        }
        rt.print(&text);
        return Ok(SuccessTuple::fail(format!("Invalid choice '{choice}'.")));
    };
    let mut sub_args = args.clone();
    sub_args.insert("action".into(), PyValue::str_list(&action[1..]));
    if let Some(result) = run(function, &sub_args, rt) {
        return result;
    }
    if let Some(result) = super::run(function, &sub_args, rt) {
        return result;
    }
    Err(ActionError::new(
        "NotImplementedError",
        format!("`{main} {choice}` is not available."),
    ))
}

// --- api --------------------------------------------------------------------------------------

/// `api [start | label] <command ...>`: send commands to an API instance (or start the server).
pub fn api(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let mut action = words(args, "action");
    if action.is_empty() {
        let doc = crate::action_docs::docstring("api").unwrap_or_default();
        info(rt, doc);
        return Ok(SuccessTuple::fail(
            "Please provide a command to execute (see above).",
        ));
    }
    if matches!(action[0].as_str(), "start" | "boot" | "init") {
        return api_start(args, rt);
    }
    let sysargs = words(args, "sysargs");
    let api_configs = rt
        .config()
        .get(&["meerschaum", "connectors", "api"])
        .map(|v| v.to_map())
        .unwrap_or_default();
    let mut api_label = "main".to_string();
    let mut args_to_send = sysargs.clone();
    if args_to_send.iter().any(|a| a == "api") && !args_to_send.is_empty() {
        args_to_send.remove(0);
    }
    if api_configs.contains_key(&action[0]) {
        api_label = action.remove(0);
        if args_to_send.len() > 1 {
            args_to_send.remove(0);
        }
    }
    let mut kw = args.clone();
    kw.shift_remove("mrsm_instance");
    kw.insert("action".into(), PyValue::str_list(&action));
    kw.insert("sysargs".into(), PyValue::str_list(&args_to_send));
    kw.insert("yes".into(), PyValue::Bool(true));
    let keys = format!("api:{api_label}");
    let (typ, label) = super::session::split_keys(&keys);
    let attrs = super::session::connector_attributes(rt.config(), &typ, &label);
    let connector = meerschaum_connector_api::APIConnector::new(Some(&label), attrs)
        .map_err(|e| ActionError::new("InvalidAttributesError", e.to_string()))?;
    let connector_keys = format!("api:{label}");
    match args.get("mrsm_instance") {
        Some(PyValue::None) | None => {}
        Some(instance) if instance.py_str() == connector_keys => {
            warn(
                rt,
                &format!(
                    "Cannot send Meerschaum instance keys '{}' to itself. Removing from arguments...",
                    instance.py_str()
                ),
            );
        }
        Some(instance) => {
            kw.insert("mrsm_instance".into(), PyValue::Str(instance.py_str()));
        }
    }
    let sysargs = crate::sysargs::parse_dict_to_sysargs(&kw)
        .map_err(|e| ActionError::new(&e.class, e.message))?;
    let sysargs = crate::sysargs::remove_api_executor_keys(&sysargs);
    let sess = session(rt);
    let icons = rt.with_formatter(|f| {
        let charset = f.charset();
        ["success", "failure", "success_calm", "failure_calm"]
            .iter()
            .map(|kind| f.formatting_str(&[kind, charset, "icon"]))
            .collect::<Vec<_>>()
    });
    let result = run_streaming(
        rt,
        &sess,
        TupleTailFilter::new(icons),
        move |on_line| async move { connector.do_action(&sysargs, None, on_line).await },
    );
    Ok(result)
}

/// Holds back the streamed lines that are the job's own final `print_tuple` (v4's job log ends
/// with `mrsm.pprint(result)`) so the client, which prints the result itself, shows it once.
///
/// v4's client never sees that line: it stops reading the log when the job's status turns
/// `stopped`, which happens before the daemon prints it. Here the log is complete, so the block
/// that starts with a result icon (and continues on indented lines) is released unless it is
/// exactly the final result.
#[derive(Default)]
pub(crate) struct TupleTailFilter {
    icons: Vec<String>,
    held: Vec<String>,
}

fn strip_ansi(text: &str) -> String {
    use std::sync::OnceLock;
    static ANSI: OnceLock<regex::Regex> = OnceLock::new();
    ANSI.get_or_init(|| regex::Regex::new("\x1b\\[[0-9;]*[A-Za-z]").expect("regex"))
        .replace_all(text, "")
        .into_owned()
}

impl TupleTailFilter {
    /// A filter for the tuples `print_tuple` writes with these icons (success, failure).
    pub(crate) fn new(icons: Vec<String>) -> Self {
        Self {
            icons,
            held: Vec::new(),
        }
    }

    /// The lines to print now for `line`.
    pub(crate) fn feed(&mut self, chunk: String) -> Vec<String> {
        // A websocket frame may carry several lines.
        let mut out = Vec::new();
        for line in chunk.split_inclusive('\n') {
            out.extend(self.feed_line(line.to_string()));
        }
        out
    }

    fn feed_line(&mut self, line: String) -> Vec<String> {
        let plain = strip_ansi(&line);
        let starts_result = self
            .icons
            .iter()
            .any(|icon| !icon.is_empty() && plain.trim_start().starts_with(icon.as_str()));
        if starts_result {
            let out = std::mem::take(&mut self.held);
            self.held.push(line);
            return out;
        }
        if !self.held.is_empty() {
            let continuation = plain.trim().is_empty() || plain.starts_with(char::is_whitespace);
            if continuation {
                self.held.push(line);
                return Vec::new();
            }
            let mut out = std::mem::take(&mut self.held);
            out.push(line);
            return out;
        }
        vec![line]
    }

    /// The held lines to print once the job is done, unless they are exactly `expected` (the text
    /// `print_tuple` makes of the final result).
    pub(crate) fn finish(&mut self, expected: &str) -> Vec<String> {
        let held = std::mem::take(&mut self.held);
        let normalize = |lines: Vec<String>| -> Vec<String> {
            let mut lines: Vec<String> = lines
                .iter()
                .map(|l| strip_ansi(l).trim_end().to_string())
                .collect();
            while lines.last().is_some_and(String::is_empty) {
                lines.pop();
            }
            while lines.first().is_some_and(String::is_empty) {
                lines.remove(0);
            }
            lines
        };
        let expected_lines: Vec<String> =
            expected.split_inclusive('\n').map(String::from).collect();
        if normalize(held.clone()) == normalize(expected_lines) {
            Vec::new()
        } else {
            held
        }
    }
}

/// Run a remote action on the session's runtime and print each line it reports as it arrives (not
/// when the job ends): the task sends the lines over a channel that closes with the task. The
/// job's own final result block is dropped (see [`TupleTailFilter`]).
fn run_streaming<F, Fut>(
    rt: &mut Runtime<'_>,
    sess: &super::Session,
    mut filter: TupleTailFilter,
    make: F,
) -> SuccessTuple
where
    F: FnOnce(Box<dyn FnMut(String) + Send + 'static>) -> Fut,
    Fut: std::future::Future<Output = SuccessTuple> + Send + 'static,
{
    let (sender, receiver) = std::sync::mpsc::channel::<String>();
    let task = sess.handle().spawn(make(Box::new(move |line| {
        let _ = sender.send(line);
    })));
    while let Ok(line) = receiver.recv() {
        for out in filter.feed(line) {
            rt.print(&out);
        }
    }
    let result = sess
        .block_on(task)
        .unwrap_or_else(|e| SuccessTuple::fail(format!("The remote action was interrupted: {e}")));
    let expected = rt.with_formatter(|f| f.print_tuple(&result, TupleArgs::default()));
    for out in filter.finish(&expected) {
        rt.print(&out);
    }
    result
}

/// `_api_start(action, port, ...)`: start the API server. A second word that is an integer is
/// the port when `--port` is not given.
pub fn api_start(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let mut args = args.clone();
    let action = words(&args, "action");
    if args.get("port").is_none_or(|p| matches!(p, PyValue::None))
        && action.len() > 1
        && action[1].trim().parse::<i64>().is_ok()
    {
        args.insert(
            "port".into(),
            PyValue::Int(action[1].trim().parse::<i64>().unwrap_or(0) as i128),
        );
    }
    crate::server::start_api(&args, rt)
}

// --- the CLI daemons (`_internal/cli/workers.py`) -----------------------------------------------

/// The CLI daemons kept a Python interpreter warm between commands (`experimental:cli_daemon`).
/// `mrsm` is a native binary: there is nothing to keep warm, so none ever runs.
const NO_CLI_DAEMONS: &str =
    "The CLI daemons are not used in Meerschaum v5: actions run natively in `mrsm`.";

/// `show daemons`: with no workers v4 answers `No CLI daemons are running.`.
fn show_daemons() -> ActionResult {
    Ok(SuccessTuple::ok("No CLI daemons are running."))
}

/// `start daemons` / `restart daemons` (which is `start daemons --force`): v4 spawns the workers
/// after its lock and restart prompts, none of which can apply here.
fn start_daemons() -> ActionResult {
    Ok(SuccessTuple::fail(NO_CLI_DAEMONS))
}

/// `stop daemons`: with no workers v4 only recreates the (empty) CLI resources directory.
fn stop_daemons(rt: &mut Runtime<'_>) -> ActionResult {
    let path = rt.config().paths().path("CLI_RESOURCES_PATH");
    let cleaned = (|| -> std::io::Result<()> {
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
        }
        std::fs::create_dir_all(&path)
    })();
    Ok(match cleaned {
        Ok(()) => SuccessTuple::ok("Success"),
        Err(e) => SuccessTuple::fail(format!("Failed to clean up CLI resources directory.\n{e}")),
    })
}

// --- show schedules / targets -----------------------------------------------------------------

/// `show schedules <schedule> [count]`.
pub fn show_schedules(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    if action.is_empty() {
        return Ok(SuccessTuple::fail("Provide a schedule to be parsed."));
    }
    let schedule = &action[0];
    let default_count = 5i64;
    let count = action
        .get(1)
        .map(|s| s.trim().parse::<i64>().unwrap_or(default_count))
        .unwrap_or(default_count);
    let now = chrono::Utc::now().fixed_offset();
    let mut trigger = match meerschaum_jobs::parse_schedule(schedule, now) {
        Ok(t) => t,
        Err(meerschaum_jobs::schedule::ScheduleError::Frequency(message)) => {
            return Ok(SuccessTuple::fail(message));
        }
        Err(e) => return Err(ActionError::new("Exception", e.to_string())),
    };
    let mut next: Vec<String> = Vec::new();
    for _ in 0..count.max(0) {
        match trigger.next() {
            Some(dt) => next.push(meerschaum_jobs::schedule::isoformat(&dt).replacen('T', " ", 1)),
            None => break,
        }
    }
    let opts = OptionsArgs {
        num_cols: Some(1),
        nopretty,
        header: Some(format!(
            "Next {} timestamps for schedule '{schedule}':",
            count.min(next.len() as i64)
        )),
        ..OptionsArgs::default()
    };
    let text = rt.with_formatter(|f| f.print_options(&next, &opts));
    rt.print(&text);
    Ok(SuccessTuple::ok("Success"))
}

/// One `show targets` row: `(schema, target)` and the pipes writing to it.
type TargetRow = ((Option<String>, String), Vec<meerschaum_core::Pipe>);

/// `show targets [targets ...]`.
pub fn show_targets(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    let connector_schema = inst.sql().and_then(|s| s.schema().map(str::to_string));
    let flavor = inst.sql().map(|s| s.flavor().to_string());
    // `{(schema, target): [pipes]}` in first-seen order.
    let mut targets: Vec<TargetRow> = Vec::new();
    for sp in pipes.iter_mut() {
        hydrate(rt, &inst, sp);
        let raw = sp.pipe.raw_parameters().clone();
        let truthy = |k: &str| raw.get(k).filter(|v| v.truthy()).map(Value::py_str);
        let schema = truthy("schema").or_else(|| connector_schema.clone());
        let explicit = truthy("target")
            .or_else(|| truthy("target_name"))
            .or_else(|| truthy("target_table"))
            .or_else(|| truthy("target_table_name"));
        let target = match explicit {
            Some(t) => t,
            None => {
                let legacy = sp.pipe.target_legacy();
                match &flavor {
                    Some(f) => {
                        meerschaum_core::pipe::truncate_item_name(&legacy, f).unwrap_or(legacy)
                    }
                    None => legacy,
                }
            }
        };
        let key = (schema, target);
        match targets.iter().position(|(k, _)| *k == key) {
            Some(i) => targets[i].1.push(sp.pipe.clone()),
            None => targets.push((key, vec![sp.pipe.clone()])),
        }
    }
    if !action.is_empty() {
        targets.retain(|((_, target), _)| action.contains(target));
    }
    if targets.is_empty() {
        return Ok(SuccessTuple::fail("No targets to show."));
    }
    if nopretty {
        let mut out = String::new();
        for ((schema, target), pipes) in &targets {
            match schema.as_deref().filter(|s| !s.is_empty()) {
                Some(s) => out.push_str(&format!("{s}.{target}\n")),
                None => out.push_str(&format!("{target}\n")),
            }
            for pipe in pipes {
                out.push_str(&format!("{}\n", dumps(&Value::Map(pipe_meta(pipe)))));
            }
        }
        rt.print(&out);
        return Ok(SuccessTuple::ok("Success"));
    }
    targets.sort_by(|a, b| {
        let key =
            |t: &((Option<String>, String), _)| (t.0.0.clone().unwrap_or_default(), t.0.1.clone());
        key(a).cmp(&key(b))
    });
    let text = rt.with_formatter(|f| {
        let mut table = Table::new();
        table.box_chars = Some(HEAVY_HEAD);
        table.show_lines = true;
        table.header_style = crate::format::rich::style::StyleRef::Name("bold".into());
        for header in ["Schema", "Target", "Pipes"] {
            table.add_column(Column {
                header: crate::format::rich::text::Text::plain_text(header),
                overflow: Overflow::Fold,
                ..Column::default()
            });
        }
        for ((schema, target), pipes) in &targets {
            let texts: Vec<Box<dyn Renderable>> = pipes
                .iter()
                .map(|p| Box::new(f.pipe_repr_text(&pipe_keys(p))) as Box<dyn Renderable>)
                .collect();
            table.add_row(vec![
                Some(Box::new(crate::format::rich::text::Text::plain_text(
                    schema.as_deref().unwrap_or(""),
                )) as Box<dyn Renderable>),
                Some(Box::new(crate::format::rich::text::Text::plain_text(
                    target,
                ))),
                Some(Box::new(Group { renderables: texts })),
            ]);
        }
        f.console().print(&table)
    });
    rt.print(&text);
    Ok(SuccessTuple::ok("Success"))
}

// --- drop / flush -----------------------------------------------------------------------------

/// `drop indices`.
pub fn drop_indices(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let (yes, force, noask) = (flag(args, "yes"), flag(args, "force"), flag(args, "noask"));
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes to drop."));
    }
    let sess = session(rt);
    let mut question = "Are you sure you want to drop these indices?\n".to_string();
    for sp in pipes.iter_mut() {
        hydrate(rt, &inst, sp);
        let indices = sess
            .block_on(inst.conn().get_pipe_index_names(&sp.pipe))
            .unwrap_or_default();
        if indices.is_empty() {
            continue;
        }
        question.push_str(&format!("\n{}\n", pipe_str(&sp.pipe)));
        for (_, name) in &indices {
            question.push_str(&format!("    - {}\n", name.py_str()));
        }
    }
    question.push('\n');
    let answer = force || yes_no(rt, &question, "n", yes, noask)?;
    if !answer {
        return Ok(SuccessTuple::fail("No pipes were dropped."));
    }
    let (mut successes, mut fails) = (0usize, 0usize);
    for sp in &pipes {
        let res = sess.block_on(inst.conn().drop_pipe_indices(&sp.pipe));
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
            "Finished dropping indices for {} pipe{}\n    ({successes} succeeded, {fails} failed).",
            pipes.len(),
            plural(pipes.len())
        ),
    })
}

/// `flush indices`: `drop indices`, then `index pipes`.
pub fn flush_indices(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let dropped = drop_indices(args, rt)?;
    if !dropped.success {
        return Ok(dropped);
    }
    super::manage::index_pipes(args, rt)
}

/// `flush pipes`: `drop pipes` (or `clear pipes` with `--begin`, `--end` or `--params`), then
/// `sync pipes`.
pub fn flush_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let present = |key: &str| args.get(key).is_some_and(|v| !matches!(v, PyValue::None));
    let bounded =
        present("begin") || present("end") || args.get("params").is_some_and(|p| p.is_truthy());
    let first = if bounded {
        super::manage::clear_pipes(args, rt)?
    } else {
        super::manage::drop_pipes(args, rt)?
    };
    if !first.success {
        return Ok(first);
    }
    super::sync::sync_pipes(args, rt)
}

/// `edit config <keys> --rollback`: restore keys from their most recent backups.
pub fn rollback_config(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let noask = flag(args, "noask");
    let mut action = words(args, "action");
    if action.is_empty() {
        let keys = crate::builtin::possible_config_keys(rt);
        let backed_up: Vec<String> = keys
            .into_iter()
            .filter(|k| {
                !rt.config()
                    .with_state(|s| s.list_config_backups(k))
                    .is_empty()
            })
            .collect();
        if backed_up.is_empty() {
            return Ok(SuccessTuple::fail("No configuration backups exist yet."));
        }
        let first = backed_up[0].clone();
        let chosen = super::ask::choose(
            rt,
            "Choose a configuration file to roll back:",
            &backed_up,
            Some(&first),
            true,
        )?;
        action = vec![chosen.unwrap_or(first)];
    }
    let (mut successes, mut fails) = (0usize, 0usize);
    for key in &action {
        let backups = rt.config().with_state(|s| s.list_config_backups(key));
        let Some(latest) = backups.first().cloned() else {
            warn(rt, &format!("No configuration backups found for '{key}'."));
            fails += 1;
            continue;
        };
        let name = latest
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        if !noask
            && !yes_no(
                rt,
                &format!(
                    "Restore '{key}' from backup '{name}'? The current file will itself be backed up first."
                ),
                "y",
                false,
                noask,
            )?
        {
            info(rt, &format!("Skipped rolling back '{key}'."));
            continue;
        }
        let (success, message) = rt
            .config()
            .with_state(|s| s.restore_config_backup(key, Some(&latest), 0));
        print_tuple(rt, &SuccessTuple { success, message }, TupleArgs::default());
        if success {
            successes += 1;
        } else {
            fails += 1;
        }
    }
    Ok(if fails > 0 && successes == 0 {
        SuccessTuple::fail(format!("Failed to roll back {fails} config key(s)."))
    } else if fails > 0 {
        SuccessTuple::ok(format!(
            "Rolled back {successes} config key(s); {fails} failed."
        ))
    } else if successes == 0 {
        SuccessTuple::fail("No configuration keys were rolled back.")
    } else {
        SuccessTuple::ok(format!("Rolled back {successes} config key(s)."))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    /// A stdout that records each write.
    struct Stamped(Arc<Mutex<Vec<(Instant, String)>>>);

    impl std::io::Write for Stamped {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap()
                .push((Instant::now(), String::from_utf8_lossy(buf).into_owned()));
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn the_jobs_own_result_block_is_dropped_only_when_it_is_the_result() {
        let icons = vec!["\u{1f389}".to_string(), "\u{1f4a2}".to_string()];
        let mut filter = TupleTailFilter::new(icons.clone());
        let mut printed = Vec::new();
        for line in ["hi\n", "\n", " \u{1f389} done\n", "    \n"] {
            printed.extend(filter.feed(line.to_string()));
        }
        // Held until the end, then dropped: it is the final result.
        assert_eq!(printed, ["hi\n", "\n"]);
        assert!(filter.finish(" \u{1f389} done\n    \n").is_empty());

        // An icon line in the middle of the output is released by the next block.
        let mut filter = TupleTailFilter::new(icons.clone());
        let mut printed = Vec::new();
        for line in [
            " \u{1f389} step one\n",
            "plain\n",
            " \u{1f4a2} bad\n",
            "    detail\n",
        ] {
            printed.extend(filter.feed(line.to_string()));
        }
        assert_eq!(printed, [" \u{1f389} step one\n", "plain\n"]);
        // The final result is something else: the held block is shown.
        let rest = filter.finish(" \u{1f4a2} other\n");
        assert_eq!(rest, [" \u{1f4a2} bad\n", "    detail\n"]);

        // ANSI styling does not matter for the comparison.
        let mut filter = TupleTailFilter::new(icons);
        assert!(
            filter
                .feed("\x1b[1;92m \u{1f389} ok\x1b[0m\n".into())
                .is_empty()
        );
        assert!(filter.finish(" \u{1f389} ok\n").is_empty());
    }

    #[test]
    fn remote_output_is_printed_as_it_arrives() {
        let writes = Arc::new(Mutex::new(Vec::new()));
        let mut out = Stamped(writes.clone());
        let mut rt = Runtime::system(&mut out);
        let sess = super::super::Session::new();
        // The job reports a line, then does not go on until the line has been printed: output held
        // back until the job ends would never get there (the wait gives up after `PATIENCE` and the
        // job records it), so no timing of the machine decides the result.
        const PATIENCE: Duration = Duration::from_secs(20);
        let seen = writes.clone();
        let result = run_streaming(
            &mut rt,
            &sess,
            TupleTailFilter::default(),
            |mut on_line| async move {
                let mut printed_while_running = 0;
                for i in 0..3 {
                    on_line(format!("line {i}\n"));
                    let deadline = Instant::now() + PATIENCE;
                    while seen.lock().unwrap().len() <= i && Instant::now() < deadline {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    if seen.lock().unwrap().len() > i {
                        printed_while_running += 1;
                    }
                }
                SuccessTuple::ok(format!("done {printed_while_running}"))
            },
        );
        assert_eq!(result, SuccessTuple::ok("done 3"));
        let writes = writes.lock().unwrap();
        let lines: Vec<&str> = writes.iter().map(|(_, text)| text.as_str()).collect();
        assert_eq!(lines, ["line 0\n", "line 1\n", "line 2\n"]);
    }
}
