//! The compose lint (§6.3): `mrsm compose explain` reports likely mistakes as warnings. The lint
//! never changes behavior; it only reads the compose document and the defined pipes.
//!
//! Checks:
//! - unknown top-level keys (compose ignores them);
//! - pipes nested under `sync:` (loaded, but almost always a mistake);
//! - `plugins_dir` entries that do not exist;
//! - misspelled pipe keywords and parameter keys (`dtype:` for `dtypes:`, `tag:` for `tags:`,
//!   `column:`, `index:`, `parameter:`) and unknown pipe keywords (`Pipe()` raises on them);
//! - string-typed `tags` (a pipe's `tags:` must be a list; `parameters: tags:` as a string is
//!   silently replaced by the project tag);
//! - `{{ Pipe() }}` references to pipes that are neither defined in the project nor registered
//!   on their instance, and templates that cannot be parsed.

use std::fmt;
use std::path::{Path, PathBuf};

use meerschaum_core::pipe::{PIPE_INIT_PARAMS, Pipe, PipeDefaults};
use meerschaum_core::pyfmt::py_str;
use meerschaum_core::template::{self, TemplateTarget};
use meerschaum_core::value::{Map, Value};

use crate::ops::InstanceOps;
use crate::project::{COMPOSE_KEYS, resolve_against};

/// How serious a finding is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Severity {
    /// Likely a mistake.
    #[default]
    Warning,
    /// Information (an override, something `up` would install).
    Info,
}

/// One lint finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LintWarning {
    /// A stable identifier for the check (`unknown-key`, `nested-pipes`, `missing-plugins-dir`,
    /// `misspelled-key`, `unknown-pipe-keyword`, `string-tags`, `unresolved-reference`,
    /// `invalid-template`, `unresolved-connector`, `unresolved-symlink`, `missing-plugin`,
    /// `pending-plugin`, `unreachable-instance`, `include-*`, `override-*`).
    pub code: &'static str,
    /// Warning or info.
    pub severity: Severity,
    /// Where the finding is (`pipes[1].parameters.dtype`).
    pub location: String,
    /// The message.
    pub message: String,
}

impl fmt::Display for LintWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.location, self.message)
    }
}

/// Misspellings of pipe keywords and parameter keys, and the canonical key.
pub const MISSPELLINGS: &[(&str, &str)] = &[
    ("dtype", "dtypes"),
    ("tag", "tags"),
    ("column", "columns"),
    ("index", "indices"),
    ("parameter", "parameters"),
    ("params", "parameters"),
];

/// The top-level keys v5 adds: `include` and `parent` (read from the document, not kept in the
/// resolved configuration).
pub const V5_KEYS: &[&str] = &["include", "parent"];

/// Keys accepted at the top level of a compose file (the `COMPOSE_KEYS` and [`V5_KEYS`]).
pub fn is_known_top_level_key(key: &str) -> bool {
    COMPOSE_KEYS.contains(&key) || V5_KEYS.contains(&key)
}

impl LintWarning {
    /// A warning finding.
    pub fn warning(
        code: &'static str,
        location: impl Into<String>,
        message: impl Into<String>,
    ) -> LintWarning {
        LintWarning {
            code,
            severity: Severity::Warning,
            location: location.into(),
            message: message.into(),
        }
    }

    /// An informational finding.
    pub fn info(
        code: &'static str,
        location: impl Into<String>,
        message: impl Into<String>,
    ) -> LintWarning {
        LintWarning {
            code,
            severity: Severity::Info,
            location: location.into(),
            message: message.into(),
        }
    }
}

fn misspelling(key: &str) -> Option<&'static str> {
    MISSPELLINGS
        .iter()
        .find(|(wrong, _)| *wrong == key)
        .map(|(_, right)| *right)
}

/// The inputs to [`lint`].
pub struct LintInput<'a> {
    /// The compose document (YAML after environment substitution).
    pub document: &'a Map,
    /// The compose file (relative `plugins_dir` entries resolve against its directory).
    pub compose_file: &'a Path,
    /// The project's pipe defaults (default instance).
    pub defaults: &'a PipeDefaults,
    /// The project's default instance keys for pipes without one.
    pub default_instance: &'a str,
    /// Pipes the other projects of the tree define: a `{{ Pipe() }}` reference to one of them
    /// resolves.
    pub also_defined: &'a [crate::project::PipeKey],
}

fn pipe_label(section: &str, ix: usize, meta: &Map) -> String {
    let ck = meta.get("connector").map(py_str).unwrap_or_default();
    let mk = meta.get("metric").map(py_str).unwrap_or_default();
    format!("{section}[{ix}] ({ck}, {mk})")
}

pub(crate) fn pipe_entries(document: &Map) -> Vec<(String, usize, Map)> {
    let mut out = Vec::new();
    if let Some(Value::List(items)) = document
        .get("sync")
        .and_then(Value::as_map)
        .and_then(|s| s.get("pipes"))
    {
        for (i, item) in items.iter().enumerate() {
            if let Value::Map(m) = item {
                out.push(("sync.pipes".to_string(), i, m.clone()));
            }
        }
    }
    if let Some(Value::List(items)) = document.get("pipes") {
        for (i, item) in items.iter().enumerate() {
            if let Value::Map(m) = item {
                out.push(("pipes".to_string(), i, m.clone()));
            }
        }
    }
    out
}

fn construct(meta: &Map, defaults: &PipeDefaults, default_instance: &str) -> Option<Pipe> {
    let mut kwargs: Map = meta
        .iter()
        .filter(|(k, _)| PIPE_INIT_PARAMS.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if !kwargs.get("instance").is_some_and(Value::truthy)
        && !kwargs.get("mrsm_instance").is_some_and(Value::truthy)
    {
        kwargs.insert("instance".into(), Value::Str(default_instance.to_string()));
    }
    if !matches!(kwargs.get("tags"), None | Some(Value::List(_))) {
        kwargs.shift_remove("tags");
    }
    Pipe::from_call(&[], &kwargs, defaults).ok()
}

fn walk_strings<'a>(v: &'a Value, path: String, out: &mut Vec<(String, &'a str)>) {
    match v {
        Value::Str(s) => out.push((path, s)),
        Value::Map(m) => {
            for (k, x) in m {
                walk_strings(x, format!("{path}.{k}"), out);
            }
        }
        Value::List(l) => {
            for (i, x) in l.iter().enumerate() {
                walk_strings(x, format!("{path}[{i}]"), out);
            }
        }
        _ => {}
    }
}

/// Run the lint. `instance` answers whether a referenced pipe is registered.
pub fn lint(input: &LintInput, instance: &mut dyn InstanceOps) -> Vec<LintWarning> {
    let mut out = Vec::new();
    let doc = input.document;

    for key in doc.keys() {
        if !is_known_top_level_key(key) {
            out.push(LintWarning {
                severity: Severity::Warning,
                code: "unknown-key",
                location: key.clone(),
                message: format!(
                    "Unknown top-level key '{key}' is ignored by compose (expected one of: {}).",
                    COMPOSE_KEYS.join(", ")
                ),
            });
        }
    }

    if let Some(Value::List(nested)) = doc
        .get("sync")
        .and_then(Value::as_map)
        .and_then(|s| s.get("pipes"))
        && !nested.is_empty()
    {
        out.push(LintWarning {
            severity: Severity::Warning,
            code: "nested-pipes",
            location: "sync.pipes".into(),
            message: format!(
                "{} pipe{} nested under `sync:`; they are loaded (before the top-level `pipes:`), \
                     but `pipes:` belongs at the top level.",
                nested.len(),
                if nested.len() == 1 { " is" } else { "s are" }
            ),
        });
    }

    let base = input
        .compose_file
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let entries: Vec<Value> = match doc.get("plugins_dir") {
        Some(Value::List(l)) => l.clone(),
        Some(v) => vec![v.clone()],
        None => Vec::new(),
    };
    for (i, entry) in entries.iter().enumerate() {
        let Value::Str(s) = entry else { continue };
        if s.is_empty() {
            continue;
        }
        let path = resolve_against(&base, Path::new(s));
        if !path.exists() {
            out.push(LintWarning {
                severity: Severity::Warning,
                code: "missing-plugins-dir",
                location: if matches!(doc.get("plugins_dir"), Some(Value::List(_))) {
                    format!("plugins_dir[{i}]")
                } else {
                    "plugins_dir".into()
                },
                message: format!(
                    "The plugins directory '{s}' does not exist ({}).",
                    path.display()
                ),
            });
        }
    }

    let entries = pipe_entries(doc);
    let defined: Vec<Pipe> = entries
        .iter()
        .filter_map(|(_, _, m)| construct(m, input.defaults, input.default_instance))
        .collect();

    for (section, ix, meta) in &entries {
        let label = pipe_label(section, *ix, meta);
        for key in meta.keys() {
            if let Some(right) = misspelling(key) {
                out.push(LintWarning {
                    severity: Severity::Warning,
                    code: "misspelled-key",
                    location: format!("{label}.{key}"),
                    message: format!(
                        "'{key}' is not a pipe keyword; did you mean '{right}'? (`Pipe()` will raise.)"
                    ),
                });
            } else if !PIPE_INIT_PARAMS.contains(&key.as_str()) {
                out.push(LintWarning {
                    severity: Severity::Warning,
                    code: "unknown-pipe-keyword",
                    location: format!("{label}.{key}"),
                    message: format!("'{key}' is not a pipe keyword (`Pipe()` will raise)."),
                });
            }
        }
        if let Some(tags) = meta.get("tags")
            && !matches!(tags, Value::List(_))
        {
            out.push(LintWarning {
                severity: Severity::Warning,
                code: "string-tags",
                location: format!("{label}.tags"),
                message: format!(
                    "`tags` must be a list (got {}); compose fails to add the project tag.",
                    crate::project::py_type_name(tags)
                ),
            });
        }
        if let Some(Value::Map(params)) = meta.get("parameters") {
            for key in params.keys() {
                if let Some(right) = misspelling(key) {
                    out.push(LintWarning {
                        severity: Severity::Warning,
                        code: "misspelled-key",
                        location: format!("{label}.parameters.{key}"),
                        message: format!(
                            "Unknown parameter '{key}'; did you mean '{right}'? (It is ignored.)"
                        ),
                    });
                }
            }
            if let Some(tags) = params.get("tags")
                && !matches!(tags, Value::List(_))
            {
                out.push(LintWarning {
                        severity: Severity::Warning,
                        code: "string-tags",
                        location: format!("{label}.parameters.tags"),
                        message: format!(
                            "`parameters.tags` must be a list (got {}); compose replaces it with the project tag.",
                            crate::project::py_type_name(tags)
                        ),
                    });
            }
        }

        // `{{ Pipe() }}` references.
        let Some(self_pipe) = construct(meta, input.defaults, input.default_instance) else {
            continue;
        };
        let mut strings = Vec::new();
        for (k, v) in meta {
            walk_strings(v, format!("{label}.{k}"), &mut strings);
        }
        for (path, text) in strings {
            for t in template::parse(text, false).templates {
                if let TemplateTarget::Pipe { call: Err(e), .. } = &t.target {
                    out.push(LintWarning {
                        severity: Severity::Warning,
                        code: "invalid-template",
                        location: path.clone(),
                        message: format!("Cannot parse the template {}: {e}", t.raw),
                    });
                }
                if let Err(e) = &t.chain {
                    out.push(LintWarning {
                        severity: Severity::Warning,
                        code: "invalid-template",
                        location: path.clone(),
                        message: format!("Cannot parse the template {}: {e}", t.raw),
                    });
                }
            }
            for r in template::referenced_pipes(text, Some(&self_pipe), input.defaults) {
                let is_defined = defined.contains(&r.pipe)
                    || input.also_defined.contains(&crate::tree::pipe_key(&r.pipe));
                if is_defined || instance.get_id(&r.pipe).is_some() {
                    continue;
                }
                out.push(LintWarning {
                    severity: Severity::Warning,
                    code: "unresolved-reference",
                    location: path.clone(),
                    message: format!(
                        "{} references {}, which is not defined in this project or registered on '{}'.",
                        self_pipe,
                        r.pipe,
                        r.pipe.instance_keys()
                    ),
                });
            }
        }
    }
    out
}
