//! The native instance and job actions (milestone 5.0.0b1, `docs/design/v5.0.0.md` §4.3):
//! `show`, `register`, `edit`, `delete`, `drop`, `clear`, `sync`, `verify`, `deduplicate`,
//! `index`, the maintenance family, `tag`, `copy` and the job actions, ported from
//! `meerschaum/actions/*.py` (v4.1.4) over the `InstanceConnector` trait and `meerschaum-jobs`.
//!
//! - [`session`]: the Tokio runtime and the instance / source connectors.
//! - [`compose_instance`]: the instance connector `mrsm compose` reads pipe registrations from.
//! - [`pipes`]: `get_pipes` and the pipe helpers the actions share.
//! - [`sync`]: the sync seam (`Pipe.sync` for `sql:` source pipes; the engine replaces it).
//! - [`show`], [`manage`], [`users`], [`maintenance`], [`jobs`]: the actions.
//! - [`rich_json`]: `rich.json.JSON` (pipe attributes in `show pipes`).

pub mod compose_instance;
pub mod engine_views;
pub mod jobs;
pub mod maintenance;
pub mod manage;
pub mod pipes;
pub mod rich_json;
pub mod session;
pub mod show;
pub mod sync;
pub mod users;
pub mod venvs;
pub mod verify;

use meerschaum_core::value::{Map, Value};
use meerschaum_core::{SuccessTuple, static_config};

use crate::builtin::ActionError;
use crate::dispatch::Runtime;
use crate::format::TupleArgs;
use crate::format::rich::pretty::PyObj;
use crate::value::{ArgsMap, PyValue};

pub use session::{Instance, Session};
/// The engine registry builder (native connectors, worker pool, native plugins), for hosts
/// such as `meerschaum-py`: `meerschaum_cli::native::engine_registry(..)`, or
/// `registry_with(.., RegistryOptions { overrides, .. })` with the host's own connectors.
pub use sync::engine::{
    EngineRegistry, RegistryOptions, build as build_engine, registry as engine_registry,
    registry_with,
};

use crate::builtin::ActionResult;

/// Run a native instance or job action by its v4 function name (`None` when the action is not
/// native).
pub fn run(function: &str, args: &ArgsMap, rt: &mut Runtime<'_>) -> Option<ActionResult> {
    Some(match function {
        "_show_pipes" => show::show_pipes(args, rt),
        "_show_connectors" => show::show_connectors(args, rt),
        "_show_columns" => show::show_columns(args, rt),
        "_show_rowcounts" => show::show_rowcounts(args, rt),
        "_show_data" => show::show_data(args, rt),
        "_show_users" => show::show_users(args, rt),
        "_show_sizes" => show::show_sizes(args, rt),
        "_show_partitions" => show::show_partitions(args, rt),
        "_show_tags" => show::show_tags(args, rt),
        "_show_venvs" => venvs::show_venvs(args, rt),
        "_prune_venvs" => venvs::prune_venvs(args, rt),
        "_upgrade_meerschaum" | "_upgrade_packages" | "_upgrade_plugins" => {
            venvs::upgrade(function, args, rt)
        }
        "_show_jobs" => jobs::show_jobs(args, rt),
        "_show_logs" | "_attach_logs" => jobs::show_logs(args, rt),
        "_register_pipes" => manage::register_pipes(args, rt),
        "_register_users" => users::register_users(args, rt),
        "_register_plugins" => manage::register_plugins(args, rt),
        "_edit_pipes" => manage::edit_pipes(args, rt),
        "_edit_users" => users::edit_users(args, rt),
        "_delete_pipes" => manage::delete_pipes(args, rt),
        "_delete_users" => users::delete_users(args, rt),
        "_delete_jobs" => jobs::delete_jobs(args, rt),
        "_drop_pipes" => manage::drop_pipes(args, rt),
        "_clear_pipes" => manage::clear_pipes(args, rt),
        "_tag_pipes" => manage::tag_pipes(args, rt),
        "_index_pipes" => manage::index_pipes(args, rt),
        "_copy_pipes" => manage::copy_pipes(args, rt),
        "_sync_pipes" => sync::sync_pipes(args, rt),
        "_plan_pipes" => engine_views::plan_pipes(args, rt),
        "_show_graph" => engine_views::show_graph(args, rt),
        "_show_runs" => engine_views::show_runs(args, rt),
        "_verify_pipes" => sync::sync_pipes_with(args, rt, sync::LapMethod::Verify),
        "_verify_rowcounts" => {
            let mut a = args.clone();
            a.insert("check_rowcounts_only".into(), PyValue::Bool(true));
            sync::sync_pipes_with(&a, rt, sync::LapMethod::Verify)
        }
        "_deduplicate_pipes" => sync::sync_pipes_with(args, rt, sync::LapMethod::Deduplicate),
        "_vacuum_pipes" => maintenance::vacuum_pipes(args, rt),
        "_analyze_pipes" => maintenance::analyze_pipes(args, rt),
        "_compress_pipes" => maintenance::compress_pipes(args, rt),
        "_decompress_pipes" => maintenance::decompress_pipes(args, rt),
        "_partition_pipes" => maintenance::partition_pipes(args, rt),
        "_start_jobs" => jobs::start_jobs(args, rt),
        "_stop_jobs" => jobs::stop_jobs(args, rt),
        "_pause_jobs" => jobs::pause_jobs(args, rt),
        "_restart_jobs" => jobs::restart_jobs(args, rt),
        "_attach_jobs" => jobs::attach_jobs(args, rt),
        _ => return None,
    })
}

/// Whether a flag is set.
pub fn flag(args: &ArgsMap, key: &str) -> bool {
    args.get(key).is_some_and(PyValue::is_truthy)
}

/// A string argument (`None` when unset).
pub fn string(args: &ArgsMap, key: &str) -> Option<String> {
    match args.get(key) {
        None | Some(PyValue::None) => None,
        Some(v) => Some(v.py_str()),
    }
}

/// A list of strings.
pub fn words(args: &ArgsMap, key: &str) -> Vec<String> {
    args.get(key)
        .and_then(PyValue::as_list)
        .map(|l| l.iter().map(PyValue::py_str).collect())
        .unwrap_or_default()
}

/// An integer argument.
pub fn int(args: &ArgsMap, key: &str) -> Option<i64> {
    match args.get(key) {
        Some(PyValue::Int(i)) => i64::try_from(*i).ok(),
        Some(PyValue::Float(f)) => Some(*f as i64),
        Some(PyValue::Str(s)) => s.trim().parse().ok(),
        _ => None,
    }
}

/// A float argument.
pub fn float(args: &ArgsMap, key: &str) -> Option<f64> {
    match args.get(key) {
        Some(PyValue::Int(i)) => Some(*i as f64),
        Some(PyValue::Float(f)) => Some(*f),
        Some(PyValue::Str(s)) => s.trim().parse().ok(),
        _ => None,
    }
}

/// A parsed argument as a core [`Value`] (JSON-like; datetimes become ISO strings).
pub fn py_to_value(v: &PyValue) -> Value {
    match v {
        PyValue::None => Value::Null,
        PyValue::Bool(b) => Value::Bool(*b),
        PyValue::Int(i) => i64::try_from(*i)
            .map(Value::Int)
            .unwrap_or_else(|_| Value::Str(i.to_string())),
        PyValue::Float(f) => Value::Float(*f),
        PyValue::Str(s) => Value::Str(s.clone()),
        PyValue::List(l) | PyValue::Tuple(l) | PyValue::Set(l) => {
            Value::List(l.iter().map(py_to_value).collect())
        }
        PyValue::Dict(d) => Value::Map(
            d.iter()
                .map(|(k, v)| (k.py_str(), py_to_value(v)))
                .collect(),
        ),
        other => Value::Str(other.py_str()),
    }
}

/// `params` as a core map (`None` when unset).
pub fn params(args: &ArgsMap) -> Option<Map> {
    match args.get("params") {
        Some(PyValue::Dict(_)) => args.get("params").map(py_to_value).map(|v| v.to_map()),
        _ => None,
    }
}

/// The session (built on first use).
pub fn session(rt: &mut Runtime<'_>) -> Session {
    rt.native.get_or_insert_with(Session::new).clone()
}

/// `mrsm_instance`, else `meerschaum:instance`.
pub fn instance_keys(args: &ArgsMap, rt: &mut Runtime<'_>) -> String {
    string(args, "mrsm_instance").unwrap_or_else(|| {
        rt.config()
            .get(&["meerschaum", "instance"])
            .map(|v| v.py_str())
            .unwrap_or_else(|| "sql:main".into())
    })
}

/// `parse_instance_keys(mrsm_instance)`.
pub fn get_instance(args: &ArgsMap, rt: &mut Runtime<'_>) -> Result<Instance, ActionError> {
    let keys = instance_keys(args, rt);
    let session = session(rt);
    session.instance(&keys, rt.config())
}

/// `print_tuple(tup, **kw)` to stdout.
pub fn print_tuple(rt: &mut Runtime<'_>, tup: &SuccessTuple, args: TupleArgs) {
    let text = rt.with_formatter(|f| f.print_tuple(tup, args));
    rt.print(&text);
}

/// `info(msg)` (stdout).
pub fn info(rt: &mut Runtime<'_>, msg: &str) {
    let text = rt.with_formatter(|f| f.info(msg, true));
    rt.print(&text);
}

/// `warn(msg, stack=False)` (stderr).
pub fn warn(rt: &mut Runtime<'_>, msg: &str) {
    let text = rt.with_formatter(|f| f.warn(msg));
    rt.eprint(&text);
}

/// `pprint(obj, nopretty=...)`.
pub fn pprint(rt: &mut Runtime<'_>, obj: &PyObj, nopretty: bool) {
    let text = rt.with_formatter(|f| f.pprint(obj, nopretty));
    rt.print(&text);
}

/// `check_noask(noask)`: `MRSM_NOASK` flips `noask` on.
pub fn check_noask(rt: &Runtime<'_>, noask: bool) -> bool {
    if noask {
        return true;
    }
    let var = &static_config().environment.noask;
    (rt.env)(var).is_some_and(|v| matches!(v.to_lowercase().as_str(), "1" | "true"))
}

/// `prompt(question, icon=..., noask=...)`: v4 prints nothing and returns `''` when `noask`.
pub fn prompt(rt: &mut Runtime<'_>, question: &str, icon: bool, noask: bool) -> Option<String> {
    if check_noask(rt, noask) {
        return Some(String::new());
    }
    let q = rt.with_formatter(|f| {
        let mut lines = question.splitn(2, '\n');
        let first = lines.next().unwrap_or_default().to_string();
        let other = lines.next().unwrap_or_default().to_string();
        let (first, other) = if f.settings.ansi {
            let style = f.formatting_str(&["question", "ansi", "rich", "style"]);
            (
                f.fill_ansi(&f.highlight_pipes(&first), &style),
                f.highlight_pipes(&other),
            )
        } else {
            (first, other)
        };
        let icon_str = f.formatting_str(&["question", f.charset(), "icon"]);
        let mut q = if icon && !icon_str.is_empty() {
            format!(" {icon_str} ")
        } else {
            String::new()
        };
        q.push_str(&first);
        if !other.is_empty() {
            q.push('\n');
            q.push_str(&other);
        }
        if !crate::format::remove_ansi(&q).ends_with(' ') {
            q.push(' ');
        }
        q
    });
    rt.host.read_line(&q)
}

/// `yes_no(question, default=..., yes=..., noask=...)`: closed input raises `EOFError` like
/// `prompt_toolkit.prompt`.
pub fn yes_no(
    rt: &mut Runtime<'_>,
    question: &str,
    default: &str,
    yes: bool,
    noask: bool,
) -> Result<bool, ActionError> {
    let default = if yes { "y" } else { default };
    let noask = yes || check_noask(rt, noask);
    let ending = format!(
        " [{}]",
        ["y", "n"]
            .iter()
            .map(|o| if *o == default {
                o.to_uppercase()
            } else {
                o.to_string()
            })
            .collect::<Vec<_>>()
            .join("/")
    );
    loop {
        let answer = match prompt(rt, &format!("{question}{ending}"), true, noask) {
            Some(a) => a,
            None => {
                rt.print("\n");
                return Err(ActionError::new("EOFError", ""));
            }
        };
        let answer = if answer.is_empty() {
            default.to_string()
        } else {
            answer
        };
        let lower = answer.to_lowercase();
        if lower == "y" || lower == "n" {
            return Ok(lower == "y");
        }
        warn(rt, "Please enter a valid reponse.");
    }
}

/// `"s"` unless `n == 1`.
pub fn plural(n: usize) -> &'static str {
    if n != 1 { "s" } else { "" }
}

/// `f"{n:,}"`.
pub fn thousands(n: i64) -> String {
    let s = n.unsigned_abs().to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 { format!("-{out}") } else { out }
}

/// `items_str(items, quotes=..., and_=..., and_str=...)` (`meerschaum/utils/misc.py`).
pub fn items_str_with(items: &[String], quotes: bool, and_: bool, and_str: &str) -> String {
    if items.is_empty() {
        return String::new();
    }
    let q = if quotes { "'" } else { "" };
    let a = if and_ { and_str } else { "" };
    if items.len() == 1 {
        return format!("{q}{}{q}", items[0]);
    }
    if items.len() == 2 {
        return format!("{q}{}{q} {a} {q}{}{q}", items[0], items[1]);
    }
    let sep = format!("{q}, {q}");
    let mut out = format!("{q}{}{q}", items[..items.len() - 1].join(&sep));
    out.push(',');
    out.push(' ');
    out.push_str(a);
    if and_ {
        out.push(' ');
    }
    out.push_str(&format!("{q}{}{q}", items[items.len() - 1]));
    out
}

/// `items_str(items)`.
pub fn items_str(items: &[String]) -> String {
    items_str_with(items, true, true, "and")
}
