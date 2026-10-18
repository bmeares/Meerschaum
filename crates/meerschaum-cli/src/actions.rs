//! The action registry and the v4 resolution rules (`meerschaum/actions/__init__.py`:
//! `get_action`, `get_subactions`, `get_main_action_name`; `remove_leading_action` in
//! `meerschaum/_internal/arguments/_parse_arguments.py`).
//!
//! Actions resolve by the Python naming convention: `sync pipes` → the `sync` module's
//! `_sync_pipes` function, `show pipe` → `_show_pipes` (a trailing `s` is tried), and unknown
//! subactions fall back to the main action, which receives the remaining words.

use crate::spec::keys;

/// A subaction (`_<action>_<sub>` function in v4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubactionDef {
    /// The subaction key (e.g. `pipes`).
    pub name: &'static str,
    /// The v4 function name (e.g. `_sync_pipes`).
    pub function: &'static str,
    /// Whether the v4 function accepts `executor_keys`.
    pub accepts_executor_keys: bool,
}

impl SubactionDef {
    /// The function's docstring (`__doc__`), ported from `meerschaum/actions/*.py`.
    pub fn doc(&self) -> Option<&'static str> {
        crate::action_docs::docstring(self.function)
    }
}

/// A top-level action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActionDef {
    /// The action name (e.g. `sync`).
    pub name: &'static str,
    /// The v4 function name.
    pub function: &'static str,
    /// Whether the v4 function accepts `executor_keys`.
    pub accepts_executor_keys: bool,
    /// The subactions, keyed as `get_subactions` returns them.
    pub subactions: &'static [SubactionDef],
}

impl ActionDef {
    /// The action function's docstring (`__doc__`), ported from `meerschaum/actions/*.py`.
    pub fn doc(&self) -> Option<&'static str> {
        crate::action_docs::docstring(self.function)
    }
}

/// Actions that exist only in v5. They resolve, have help and complete like any core action, but
/// `show actions` and the unknown-action help table keep v4's listing (the parity fixtures are
/// recorded from v4), and the registry parity test ignores them.
pub const V5_ONLY_ACTIONS: &[&str] = &["prune", "plan"];

/// Subactions that exist only in v5 (`(action, subaction)`). They resolve and run like any
/// core subaction, and complete once a prefix is typed, but the empty `show <TAB>` list and the
/// registry parity test keep v4's contents.
pub const V5_ONLY_SUBACTIONS: &[(&str, &str)] = &[
    ("show", "graph"),
    ("show", "runs"),
    ("show", "drivers"),
    ("install", "drivers"),
    ("uninstall", "drivers"),
];

/// Whether `(action, subaction)` is v5-only.
pub fn is_v5_only_subaction(action: &str, subaction: &str) -> bool {
    V5_ONLY_SUBACTIONS.contains(&(action, subaction))
}

const fn sub(name: &'static str, function: &'static str) -> SubactionDef {
    SubactionDef {
        name,
        function,
        accepts_executor_keys: false,
    }
}

const fn sub_e(name: &'static str, function: &'static str) -> SubactionDef {
    SubactionDef {
        name,
        function,
        accepts_executor_keys: true,
    }
}

const fn action(name: &'static str, subactions: &'static [SubactionDef]) -> ActionDef {
    ActionDef {
        name,
        function: name,
        accepts_executor_keys: false,
        subactions,
    }
}

const fn action_e(name: &'static str, subactions: &'static [SubactionDef]) -> ActionDef {
    ActionDef {
        name,
        function: name,
        accepts_executor_keys: true,
        subactions,
    }
}

/// The core actions, in the order v4's `meerschaum.actions.actions` lists them.
/// Kept in sync with `tests/fixtures/actions.json` by the `actions_registry_matches_python` test.
pub static CORE_ACTIONS: &[ActionDef] = &[
    action_e(
        "attach",
        &[sub_e("jobs", "_attach_jobs"), sub("logs", "_attach_logs")],
    ),
    action(
        "clear",
        &[sub("cache", "_clear_cache"), sub("pipes", "_clear_pipes")],
    ),
    action("deduplicate", &[sub("pipes", "_deduplicate_pipes")]),
    action("login", &[]),
    action("os", &[]),
    action("pause", &[sub_e("jobs", "_pause_jobs")]),
    action(
        "register",
        &[
            sub("connectors", "_register_connectors"),
            sub("pipes", "_register_pipes"),
            sub("plugins", "_register_plugins"),
            sub("tokens", "_register_tokens"),
            sub("users", "_register_users"),
        ],
    ),
    action("reload", &[]),
    action_e(
        "restart",
        &[
            sub("daemons", "_restart_daemons"),
            sub_e("jobs", "_restart_jobs"),
        ],
    ),
    action("setup", &[sub("plugins", "_setup_plugins")]),
    action("sh", &[]),
    action("tag", &[sub("pipes", "_tag_pipes")]),
    action(
        "verify",
        &[
            sub("packages", "_verify_packages"),
            sub("pipes", "_verify_pipes"),
            sub("plugins", "_verify_plugins"),
            sub("rowcounts", "_verify_rowcounts"),
            sub("venvs", "_verify_venvs"),
        ],
    ),
    action(
        "install",
        &[
            sub("drivers", "_install_drivers"),
            sub("packages", "_install_packages"),
            sub("plugins", "_install_plugins"),
            sub("required", "_install_required"),
        ],
    ),
    action("sync", &[sub("pipes", "_sync_pipes")]),
    action("api", &[sub("start", "_api_start")]),
    action(
        "bootstrap",
        &[
            sub("connectors", "_bootstrap_connectors"),
            sub_e("jobs", "_bootstrap_jobs"),
            sub("pipes", "_bootstrap_pipes"),
            sub("plugins", "_bootstrap_plugins"),
        ],
    ),
    action(
        "drop",
        &[sub("indices", "_drop_indices"), sub("pipes", "_drop_pipes")],
    ),
    action(
        "flush",
        &[
            sub("indices", "_flush_indices"),
            sub("pipes", "_flush_pipes"),
        ],
    ),
    action("stack", &[]),
    action(
        "stop",
        &[sub("daemons", "_stop_daemons"), sub_e("jobs", "_stop_jobs")],
    ),
    action(
        "uninstall",
        &[
            sub("drivers", "_uninstall_drivers"),
            sub("packages", "_uninstall_packages"),
            sub("plugins", "_uninstall_plugins"),
        ],
    ),
    action("vacuum", &[sub("pipes", "_vacuum_pipes")]),
    // v5 only (D20): not in v4's registry.
    action("prune", &[sub("venvs", "_prune_venvs")]),
    action("plan", &[sub("pipes", "_plan_pipes")]),
    action(
        "show",
        &[
            sub("actions", "_show_actions"),
            sub("arguments", "_show_arguments"),
            sub("columns", "_show_columns"),
            sub("config", "_show_config"),
            sub("connectors", "_show_connectors"),
            sub("daemons", "_show_daemons"),
            sub("data", "_show_data"),
            sub("drivers", "_show_drivers"),
            sub("environment", "_show_environment"),
            sub("graph", "_show_graph"),
            sub("help", "_show_help"),
            sub_e("jobs", "_show_jobs"),
            sub_e("logs", "_show_logs"),
            sub("packages", "_show_packages"),
            sub("partitions", "_show_partitions"),
            sub("pipes", "_show_pipes"),
            sub("plugins", "_show_plugins"),
            sub("rowcounts", "_show_rowcounts"),
            sub("runs", "_show_runs"),
            sub("schedules", "_show_schedules"),
            sub("sizes", "_show_sizes"),
            sub("tags", "_show_tags"),
            sub("targets", "_show_targets"),
            sub("tokens", "_show_tokens"),
            sub("users", "_show_users"),
            sub("venvs", "_show_venvs"),
            sub("version", "_show_version"),
        ],
    ),
    action(
        "delete",
        &[
            sub("cache", "_delete_cache"),
            sub("config", "_delete_config"),
            sub("connectors", "_delete_connectors"),
            sub_e("jobs", "_delete_jobs"),
            sub("pipes", "_delete_pipes"),
            sub("plugins", "_delete_plugins"),
            sub("users", "_delete_users"),
            sub("venvs", "_delete_venvs"),
        ],
    ),
    action("partition", &[sub("pipes", "_partition_pipes")]),
    action(
        "start",
        &[
            sub("api", "_start_api"),
            sub("connectors", "_start_connectors"),
            sub("daemons", "_start_daemons"),
            sub("gui", "_start_gui"),
            sub_e("jobs", "_start_jobs"),
            sub("mcp", "_start_mcp"),
            sub("pipeline", "_start_pipeline"),
            sub("webterm", "_start_webterm"),
            sub("worker", "_start_worker"),
        ],
    ),
    action("analyze", &[sub("pipes", "_analyze_pipes")]),
    action("compress", &[sub("pipes", "_compress_pipes")]),
    action(
        "copy",
        &[
            sub("connectors", "_copy_connectors"),
            sub("pipes", "_copy_pipes"),
        ],
    ),
    action("decompress", &[sub("pipes", "_decompress_pipes")]),
    action(
        "edit",
        &[
            sub("config", "_edit_config"),
            sub("definition", "_edit_definition"),
            sub_e("jobs", "_edit_jobs"),
            sub("pipes", "_edit_pipes"),
            sub("plugins", "_edit_plugins"),
            sub("tokens", "_edit_tokens"),
            sub("users", "_edit_users"),
        ],
    ),
    action("index", &[sub("pipes", "_index_pipes")]),
    action("sql", &[]),
    action_e("python", &[]),
    action(
        "upgrade",
        &[
            sub("meerschaum", "_upgrade_meerschaum"),
            sub("packages", "_upgrade_packages"),
            sub("plugins", "_upgrade_plugins"),
        ],
    ),
    action("compose", &[]),
];

/// A resolved action function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resolved {
    /// The main action.
    pub action: &'static ActionDef,
    /// The subaction, if the words resolved to one.
    pub sub: Option<&'static SubactionDef>,
}

impl Resolved {
    /// The v4 function name (`function.__name__`).
    pub fn function_name(&self) -> &'static str {
        self.sub.map(|s| s.function).unwrap_or(self.action.function)
    }

    /// Whether the resolved function accepts `executor_keys`.
    pub fn accepts_executor_keys(&self) -> bool {
        self.sub
            .map(|s| s.accepts_executor_keys)
            .unwrap_or(self.action.accepts_executor_keys)
    }

    /// A display name such as `sync pipes`.
    pub fn display_name(&self) -> String {
        match self.sub {
            Some(s) => format!("{} {}", self.action.name, s.name),
            None => self.action.name.to_string(),
        }
    }
}

fn find(actions: &'static [ActionDef], name: &str) -> Option<&'static ActionDef> {
    actions.iter().find(|a| a.name == name)
}

/// `get_subactions(action)`.
pub fn get_subactions(actions: &'static [ActionDef], name: &str) -> &'static [SubactionDef] {
    find(actions, name).map(|a| a.subactions).unwrap_or(&[])
}

/// `get_action(action)`.
pub fn get_action<S: AsRef<str>>(action: &[S], actions: &'static [ActionDef]) -> Option<Resolved> {
    if !action.iter().any(|a| !a.as_ref().is_empty()) {
        return None;
    }
    if action.len() == 1 {
        return find(actions, action[0].as_ref()).map(|a| Resolved {
            action: a,
            sub: None,
        });
    }
    // A custom action may have underscores in its name (e.g. a plugin's `foo_bar`).
    let joined = action
        .iter()
        .map(|a| a.as_ref())
        .collect::<Vec<_>>()
        .join("_");
    // PARITY: v4 sorts `(len(leftovers), function)` tuples, which raises TypeError on a length
    // tie; the first minimal candidate is used here.
    let mut best: Option<(usize, &'static ActionDef)> = None;
    for a in actions.iter().filter(|a| a.name.contains('_')) {
        if joined.starts_with(a.name) {
            let leftovers = joined.replace(a.name, "").chars().count();
            if best.is_none_or(|(len, _)| leftovers < len) {
                best = Some((leftovers, a));
            }
        }
    }
    if let Some((_, a)) = best {
        return Some(Resolved {
            action: a,
            sub: None,
        });
    }
    let main = find(actions, action[0].as_ref())?;
    let word = action[1].as_ref();
    let sub = main.subactions.iter().find(|s| s.name == word).or_else(|| {
        main.subactions
            .iter()
            .find(|s| s.name == format!("{word}s"))
    });
    Some(Resolved { action: main, sub })
}

/// `get_main_action_name(action)`.
pub fn get_main_action_name<S: AsRef<str>>(
    action: &[S],
    actions: &'static [ActionDef],
) -> Option<&'static str> {
    let resolved = get_action(action, actions)?;
    let clean = resolved.function_name().trim_start_matches('_');
    if let Some(a) = find(actions, clean) {
        return Some(a.name);
    }
    let words: Vec<&str> = clean.split('_').collect();
    if let Some(a) = find(actions, words[0]) {
        return Some(a.name);
    }
    if let Some(second) = words.get(1)
        && let Some(a) = find(actions, second)
    {
        return Some(a.name);
    }
    None
}

/// Python `s[start:]` with a possibly negative start, by code points.
fn py_slice_from(s: &str, start: i64) -> String {
    let chars: Vec<char> = s.chars().collect();
    let n = chars.len() as i64;
    let start = if start < 0 {
        (n + start).max(0)
    } else {
        start.min(n)
    };
    chars[start as usize..].iter().collect()
}

/// `remove_leading_action(action)`: strip the words that name the action function.
pub fn remove_leading_action<S: AsRef<str>>(
    action: &[S],
    actions: &'static [ActionDef],
) -> Vec<String> {
    let original: Vec<String> = action.iter().map(|a| a.as_ref().to_string()).collect();
    let Some(resolved) = get_action(&original, actions) else {
        return original;
    };
    let Some(main_name) = get_main_action_name(&original, actions) else {
        return vec![];
    };
    let standin = keys::UNDERSCORE_STANDIN;
    let mut words: Vec<String> = original.iter().map(|a| a.replace('_', standin)).collect();

    let mut action_name = resolved.function_name().trim_start_matches('_').to_string();
    let main_ix = action_name
        .find(main_name)
        .map(|i| action_name[..i].chars().count() as i64)
        .unwrap_or(-1);
    action_name = py_slice_from(&action_name, main_ix);

    let sub_parts: Vec<String> = action_name
        .replace(main_name, "")
        .trim_start_matches('_')
        .split('_')
        .map(str::to_string)
        .collect();
    let sub_name = sub_parts.first().cloned().unwrap_or_default();
    if !sub_name.is_empty() && sub_name.ends_with('s') && !original[1].ends_with('s') {
        words[1].push('s');
    }
    let action_str = words.join("_");
    if !action_str.replace(standin, "_").starts_with(&action_name) {
        // v4 warns: "Unable to parse '{action_str}' for action '{action_name}'."
        return original;
    }
    let rest = py_slice_from(&action_str, action_name.chars().count() as i64 + 1);
    let mut parsed: Vec<String> = rest.split('_').map(|a| a.replace(standin, "_")).collect();
    if parsed.first().is_some_and(|p| p.is_empty()) && !original[0].is_empty() {
        parsed.remove(0);
    }
    parsed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolution() {
        let r = get_action(&["show", "pipe"], CORE_ACTIONS).unwrap();
        assert_eq!(r.function_name(), "_show_pipes");
        assert_eq!(
            remove_leading_action(&["show", "pipes", "a_b"], CORE_ACTIONS),
            vec!["a_b"]
        );
        assert_eq!(
            remove_leading_action(&["show", "foo", "bar"], CORE_ACTIONS),
            vec!["foo", "bar"]
        );
        assert_eq!(
            remove_leading_action(&["foo", "bar"], CORE_ACTIONS),
            vec!["foo", "bar"]
        );
        assert!(get_action(&[""], CORE_ACTIONS).is_none());
    }
}
