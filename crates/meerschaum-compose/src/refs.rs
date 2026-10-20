//! References that do not resolve on this host: connectors, instances, plugins and `MRSM{}`
//! symlinks.
//!
//! `compose explain` lists every one as a lint finding and keeps going. `compose up`, `run` and
//! the passthrough (`sync pipes`, ...) refuse to start on a missing connector or plugin and name
//! it and where it is referenced, instead of failing later with whatever the connector raised.

use meerschaum_core::pyfmt::py_str;
use meerschaum_core::value::{Map, Value};

use crate::lint::LintWarning;
use crate::pipes::ProjectConfig;
use crate::project::Project;

/// The finding codes `up` / `run` refuse to start on: a connector or instance the host does not
/// define. A missing plugin is only reported (by `explain`): whether a plugin is available
/// cannot be told from the plugins directories alone (entry-point plugins, connectors a plugin
/// registers), and a pipe on a plugin that is not there fails with its own message when it
/// syncs.
pub const BLOCKING_CODES: &[&str] = &["unresolved-connector"];

/// Connector types whose labels are defined in the configuration; the other types come from
/// plugins (which can create their own labels), so an unknown label of those is not an error.
const CONFIGURED_TYPES: &[&str] = &["sql", "api", "valkey"];

/// A defined pipe's source: where in the document it is (`pipes[1]`) and its keyword
/// arguments.
#[derive(Debug, Clone)]
pub struct PipeSource {
    /// `pipes[1]` or `sync.pipes[0]`.
    pub label: String,
    /// The pipe's keyword arguments (with the project's defaults filled in).
    pub meta: Map,
}

fn split_keys(keys: &str) -> (&str, Option<&str>) {
    match keys.split_once(':') {
        Some((t, l)) => (t, Some(l)),
        None => (keys, None),
    }
}

/// Whether a configured-type connector (`sql:foo`) resolves in the project's configuration.
fn connector_defined(pc: &ProjectConfig, keys: &str) -> bool {
    let (typ, label) = split_keys(keys);
    if !CONFIGURED_TYPES.contains(&typ) {
        return true;
    }
    let Some(label) = label else {
        return true;
    };
    pc.get(&["meerschaum", "connectors", typ, label])
        .is_some_and(|v| !v.is_null())
}

fn listed_plugins(project: &Project) -> Vec<String> {
    match project.config.get("plugins") {
        Some(Value::List(items)) => items
            .iter()
            .map(|p| crate::model::PluginSpec::parse(&py_str(p)).name)
            .collect(),
        _ => Vec::new(),
    }
}

/// Every unresolved reference of a project.
///
/// `installed` are the plugins in the project's plugins directories.
pub fn findings(
    project: &Project,
    pc: &ProjectConfig,
    pipes: &[PipeSource],
    installed: &[String],
) -> Vec<LintWarning> {
    let mut out = Vec::new();

    for r in &project.unresolved {
        match r.kind {
            "connector" => out.push(LintWarning::warning(
                "unresolved-connector",
                r.location.clone(),
                format!(
                    "connector '{}' is not defined on this host (referenced by {})",
                    r.name, r.location
                ),
            )),
            _ => out.push(LintWarning::warning(
                "unresolved-symlink",
                r.location.clone(),
                format!(
                    "MRSM{{{}}} does not resolve on this host (referenced by {})",
                    r.name, r.location
                ),
            )),
        }
    }

    let listed = listed_plugins(project);
    let mut pending: Vec<String> = Vec::new();
    if let Some(Value::List(items)) = project.config.get("plugins") {
        for (i, item) in items.iter().enumerate() {
            let spec = crate::model::PluginSpec::parse(&py_str(item));
            if installed.contains(&spec.name) || pending.contains(&spec.name) {
                continue;
            }
            pending.push(spec.name.clone());
            out.push(LintWarning::info(
                "pending-plugin",
                format!("plugins[{i}]"),
                format!(
                    "plugin '{}' is not installed; `compose up` would install it from {}.",
                    spec.name,
                    spec.repository
                        .as_deref()
                        .map_or_else(|| "the default repository".to_string(), str::to_string)
                ),
            ));
        }
    }

    for source in pipes {
        let meta = &source.meta;
        let connector = meta.get("connector").map(py_str).unwrap_or_default();
        let (typ, label) = split_keys(&connector);
        if typ == "plugin" {
            if let Some(name) = label
                && !installed.iter().any(|p| p == name)
                && !listed.iter().any(|p| p == name)
            {
                let location = format!("{}.connector", source.label);
                out.push(LintWarning::warning(
                    "missing-plugin",
                    location.clone(),
                    format!(
                        "plugin '{name}' is not in the plugins directories and not listed under `plugins:` (referenced by {location})"
                    ),
                ));
            }
        } else if !connector.is_empty() && !connector_defined(pc, &connector) {
            let location = format!("{}.connector", source.label);
            out.push(LintWarning::warning(
                "unresolved-connector",
                location.clone(),
                format!(
                    "connector '{connector}' is not defined on this host (referenced by {location})"
                ),
            ));
        }
        let instance = meta
            .get("instance")
            .filter(|v| v.truthy())
            .or_else(|| meta.get("mrsm_instance").filter(|v| v.truthy()))
            .map(py_str);
        if let Some(instance) = instance
            && !connector_defined(pc, &instance)
        {
            let location = if meta.contains_key("instance") || meta.contains_key("mrsm_instance") {
                format!("{}.instance", source.label)
            } else {
                "config.meerschaum.instance".to_string()
            };
            out.push(LintWarning::warning(
                "unresolved-connector",
                location.clone(),
                format!(
                    "instance '{instance}' is not defined on this host (referenced by {location})"
                ),
            ));
        }
    }
    out
}

/// The findings `compose up`, `run` and the passthrough refuse to start on, as one message.
pub fn blocking_message(findings: &[LintWarning]) -> Option<String> {
    let blocking: Vec<&LintWarning> = findings
        .iter()
        .filter(|f| BLOCKING_CODES.contains(&f.code))
        .collect();
    if blocking.is_empty() {
        return None;
    }
    // Several references to one missing connector are reported once per reference.
    let mut lines: Vec<String> = blocking.iter().map(|f| f.message.clone()).collect();
    lines.dedup();
    Some(lines.join("\n"))
}
