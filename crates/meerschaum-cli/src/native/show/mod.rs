//! The instance-backed `show` subactions (`meerschaum/actions/show.py`): `pipes`, `connectors`,
//! `columns`, `rowcounts`, `data`, `users`, `sizes`, `partitions` and `tags`, plus `Pipe.show()`
//! and `pprint_pipe_columns`.

use meerschaum_connector::{DataQuery, DateBound, InstanceConnector, pipe_ops};
use meerschaum_core::SuccessTuple;
use meerschaum_core::pyfmt::{JsonStyle, json_dumps, thousands};
use meerschaum_core::value::{Map, Value};
use meerschaum_kernel::value::DateTimeValue;
use meerschaum_kernel::value::Value as KValue;

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::dataframe::{DisplayOptions, Fetched, format_dataframe_display};
use crate::format::progress::Progress;
use crate::format::rich::console::{Justify, Renderable};
use crate::format::rich::pretty::PyObj;
use crate::format::rich::table::{ASCII, Column, MINIMAL, Table};
use crate::format::rich::text::Text;
use crate::format::{Cell, Frame as TabFrame, OptionsArgs, TupleArgs, format_dataframe};
use crate::value::{ArgsMap, PyValue};

use super::pipes::{SelectedPipe, hydrate, pipe_keys, pipe_meta, pipe_state, selected_pipes};
use super::{Instance, flag, info, pprint, print_tuple, session, warn, words};
use super::{metadata, params, sync};

mod columns;
mod data;
mod storage;

pub use columns::{pprint_pipe_columns, show_columns};
pub use data::show_data;
pub use storage::{format_bytes, interval_minutes_str, show_partitions, show_sizes};

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

/// One line for each of `pipes` whose latest recorded sync failed, e.g.
/// `Pipe('sql:main', 'weather') failed its last 3 syncs (latest 2026-10-27 12:00:05 UTC): no
/// such table: raw`. Nothing when every latest sync succeeded or the statuses cannot be read.
fn failing_pipes_note(rt: &mut Runtime<'_>, inst: &Instance, pipes: &[SelectedPipe]) -> String {
    let Some(instance_keys) = pipes.first().map(|p| p.pipe.instance_keys().to_string()) else {
        return String::new();
    };
    let conn = inst.conn();
    let read = session(rt).block_on(async { conn.get_sync_statuses(&instance_keys).await });
    let statuses = match read {
        Ok(statuses) => statuses,
        Err(e) => {
            tracing::debug!("Cannot read the sync statuses on '{instance_keys}': {e}");
            return String::new();
        }
    };
    let failing: std::collections::HashMap<&str, _> = statuses
        .iter()
        .filter(|s| !s.success && s.last_attempt.is_some())
        .map(|s| (s.pipe_ref.as_str(), s))
        .collect();
    let mut lines = String::new();
    for sp in pipes {
        let Some(status) = failing.get(sp.pipe.pipe_ref().as_str()) else {
            continue;
        };
        let syncs = match status.consecutive_failures {
            0 | 1 => "its last sync".to_string(),
            n => format!("its last {n} syncs"),
        };
        let latest = status
            .last_failure
            .or(status.last_attempt)
            .map(|t| format!(" (latest {})", t.format("%Y-%m-%d %H:%M:%S UTC")))
            .unwrap_or_default();
        let mut reason: String = status
            .message
            .lines()
            .find(|l| !l.trim().is_empty())
            .unwrap_or_default()
            .trim()
            .chars()
            .take(160)
            .collect();
        if !reason.is_empty() {
            reason.insert_str(0, ": ");
        }
        lines.push_str(&format!("{} failed {syncs}{latest}{reason}\n", sp.pipe));
    }
    if lines.is_empty() {
        return lines;
    }
    rt.with_formatter(|f| {
        if f.settings.ansi {
            format!("\n{}", f.fill_ansi(&lines, "yellow"))
        } else {
            format!("\n{lines}")
        }
    })
}

/// `show pipes`. The pretty output ends with a line for each pipe whose latest sync failed.
pub fn show_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::ok("No pipes to show."));
    }
    if pipes.len() == 1 {
        let shown = pipe_show(rt, &inst, &mut pipes[0], nopretty);
        if !nopretty && shown.is_ok() {
            let note = failing_pipes_note(rt, &inst, &pipes);
            rt.print(&note);
        }
        return shown;
    }
    if !nopretty {
        let keys: Vec<_> = pipes.iter().map(|p| pipe_keys(&p.pipe)).collect();
        let text = rt.with_formatter(|f| f.pprint_pipes(&keys));
        rt.print(&text);
        let note = failing_pipes_note(rt, &inst, &pipes);
        rt.print(&note);
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
        // FX2: every secret masked by the console's Connectors page rules.
        let cfg = super::connector_mask::mask_connectors_config(&cfg, conn_type.as_deref());
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
            let attrs = super::connector_mask::mask_attributes(&attrs, Some(&typ), None);
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
    let progress = Progress::start(
        rt,
        args,
        if remote {
            "Counting remote rows"
        } else {
            "Counting rows"
        },
    );
    progress.set_total(pipes.len());
    let counter = progress.counter();
    let counter = &counter;
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
            counter.inc();
            (p.pipe.to_string(), rc)
        },
    ));
    drop(progress);
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

// `show users` is `super::users::show_users` (a table of users, roles and teams).

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
    let progress = Progress::start(rt, args, format!("Fetching plugins from '{keys}'"));
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
    drop(progress);
    let opts = OptionsArgs {
        header: Some(header),
        nopretty: flag(args, "nopretty"),
        ..OptionsArgs::default()
    };
    let text = rt.with_formatter(|f| f.print_options(&names, &opts));
    rt.print(&text);
    Ok(SuccessTuple::success())
}
