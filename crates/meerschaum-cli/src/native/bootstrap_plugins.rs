//! `bootstrap plugins`, the interactive plugin scaffold, ported from
//! `meerschaum/actions/bootstrap.py::_bootstrap_plugins` and `meerschaum/plugins/bootstrap.py`.
//!
//! The text the wizard writes is `bootstrap_templates.json`, generated from v4's
//! `FEATURE_LINES` / `IMPORTS_LINES` / `FEATURE_CHOICES` by
//! `scripts/rust/gen_bootstrap_templates.py`; `str.format` is reproduced by [`py_format`].
//!
//! # Differences from v4
//!
//! - Asking for an action name with `MRSM_NOASK` set ends with an error instead of looping
//!   forever on the empty answer.
//! - A failing `uninstall plugin <name> -f` of an existing plugin is reported with its own
//!   message and ends the wizard, as v4's returned tuple does.

use std::path::PathBuf;
use std::sync::OnceLock;

use meerschaum_core::SuccessTuple;
use serde_json::Value as Json;

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::{OptionsArgs, TupleArgs};
use crate::value::{ArgsMap, PyValue};

use super::ask::{Ask, ask, choose, eof};
use super::lifecycle::plugin_installed;
use super::plugin_admin::edit_file;
use super::{check_noask, flag, info, print_tuple, warn, words, yes_no};

const TEMPLATES: &str = include_str!("bootstrap_templates.json");

fn templates() -> &'static Json {
    static PARSED: OnceLock<Json> = OnceLock::new();
    PARSED.get_or_init(|| serde_json::from_str(TEMPLATES).expect("bootstrap_templates.json"))
}

fn line(group: &str, key: &str) -> &'static str {
    templates()[group][key].as_str().unwrap_or_default()
}

/// The `(index, description)` features offered.
fn feature_choices() -> Vec<(String, String)> {
    templates()["feature_choices"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    (
                        item[0].as_str().unwrap_or_default().to_string(),
                        item[1].as_str().unwrap_or_default().to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// `template.format(**labels)`: `{name}` is replaced, `{{` and `}}` are literal braces.
pub fn py_format(template: &str, labels: &[(&str, &str)]) -> String {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                out.push('{');
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                out.push('}');
            }
            '{' => {
                let mut name = String::new();
                for next in chars.by_ref() {
                    if next == '}' {
                        break;
                    }
                    name.push(next);
                }
                match labels.iter().find(|(k, _)| *k == name) {
                    Some((_, value)) => out.push_str(value),
                    None => out.push_str(&format!("{{{name}}}")),
                }
            }
            other => out.push(other),
        }
    }
    out
}

/// `bootstrap plugins [names]`: create plugin source files from a few answers.
pub fn bootstrap_plugins(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let dirs = rt.config().paths().plugins_dirs.clone();
    for dir in &dirs {
        if !dir.exists() {
            let create = yes_no(
                rt,
                &format!("Create path '{}'?", dir.display()),
                "y",
                false,
                false,
            )?;
            if !create {
                return Ok(SuccessTuple::fail(
                    "Create missing plugins paths before bootstrapping.",
                ));
            }
            if let Err(e) = std::fs::create_dir_all(dir) {
                return Err(ActionError::new("OSError", e.to_string()));
            }
        }
    }
    let mut action = words(args, "action");
    if action.is_empty() {
        action = vec![ask(rt, "Enter the name of your new plugin:", &Ask::new())?];
    }
    for name in action {
        let result = bootstrap_plugin(args, rt, &name)?;
        if !result.success {
            return Ok(result);
        }
    }
    Ok(SuccessTuple::success())
}

fn clear_screen(rt: &mut Runtime<'_>, args: &ArgsMap) {
    crate::shell::clear_screen(rt, flag(args, "debug"));
}

/// `bootstrap_plugin(plugin_name)`.
fn bootstrap_plugin(args: &ArgsMap, rt: &mut Runtime<'_>, plugin_name: &str) -> ActionResult {
    // The name becomes `<plugins>/<name>.py` or `<plugins>/<name>.rs`.
    if let Err(message) = super::plugin_files::validate_plugin_name(plugin_name) {
        return Ok(SuccessTuple::fail(message));
    }
    let plugins_dir = plugins_dir_path(rt)?;
    clear_screen(rt, args);
    info(
        rt,
        "Answer the questions below to pick out features.\n    \
         See the Writing Plugins guide for documentation:\n    \
         https://meerschaum.io/reference/plugins/writing-plugins/ for documentation.\n",
    );

    if plugin_installed(rt, args, plugin_name) {
        warn(rt, &format!("Plugin '{plugin_name}' is already installed!"));
        let uninstall = yes_no(
            rt,
            &format!("Do you want to first uninstall '{plugin_name}'?"),
            "n",
            false,
            false,
        )?;
        if !uninstall {
            return Ok(SuccessTuple::fail(format!(
                "Plugin '{plugin_name}' already exists."
            )));
        }
        let mut uninstall_args = ArgsMap::new();
        uninstall_args.insert("action".into(), PyValue::str_list(&[plugin_name]));
        uninstall_args.insert("force".into(), PyValue::Bool(true));
        let result = super::plugin_admin::uninstall_plugins(&uninstall_args, rt)?;
        if !result.success {
            return Ok(result);
        }
        clear_screen(rt, args);
    }

    let language = choose(
        rt,
        "Which language would you like to use for your plugin?",
        &["Python".into(), "Rust".into()],
        Some("Python"),
        true,
    )?;
    let rust = language.as_deref() == Some("Rust");
    if rust && let Err(message) = rust_plugin_name(plugin_name) {
        return Ok(SuccessTuple::fail(message));
    }
    clear_screen(rt, args);
    let package = choose_packaging(rt, plugin_name, rust)?;
    clear_screen(rt, args);
    let features = choose_features(rt)?;
    clear_screen(rt, args);

    let mut action_name = String::new();
    if features.iter().any(|f| f == "action") {
        action_name = get_action_name(rt)?;
        clear_screen(rt, args);
    }
    let action_spaces = action_name.replace('_', " ");

    let mut dt_col = None;
    if features.iter().any(|f| f == "fetch") {
        dt_col = get_dt_col_name(rt);
        clear_screen(rt, args);
    }
    let dt_col_name = quoted_dt_col_name(dt_col.as_deref());

    let capitalized = {
        let first = plugin_name
            .to_lowercase()
            .split(['-', '_'])
            .next()
            .unwrap_or_default()
            .to_string();
        let mut chars = first.chars();
        chars
            .next()
            .map(|c| c.to_uppercase().collect::<String>() + chars.as_str())
            .unwrap_or_default()
    };
    let lower = plugin_name.to_lowercase();
    let labels: Vec<(&str, &str)> = vec![
        ("plugin_name", plugin_name),
        ("plugin_name_capitalized", &capitalized),
        ("plugin_name_lower", &lower),
        ("action_name", &action_name),
        ("action_spaces", &action_spaces),
        ("dt_col_name", &dt_col_name),
    ];
    let python_body = || {
        let has = |feature: &str| features.iter().any(|f| f == feature);
        let imports = |key: &str| py_format(line("imports_lines", key), &labels);
        let feature = |key: &str| py_format(line("feature_lines", key), &labels);

        let mut body = String::new();
        body.push_str(&feature("header"));
        body.push_str(&imports("stdlib"));
        body.push_str(&imports("default"));
        if has("connector") && !has("instance-connector") {
            body.push_str(&imports("connector"));
        }
        if has("instance-connector") {
            body.push_str(&imports("instance-connector"));
        }
        if has("action") {
            body.push_str(&imports("action"));
        }
        if has("api") && has("web") {
            body.push_str(&imports("api+web"));
        } else if has("api") {
            body.push_str(&imports("api"));
        } else if has("web") {
            body.push_str(&imports("web"));
        }
        body.push('\n');
        body.push_str(&feature("default"));
        body.push_str(&feature("setup"));
        if has("fetch") {
            body.push_str(&feature("register"));
            body.push_str(&feature("fetch"));
        }
        if has("connector") && !has("instance-connector") {
            body.push_str(&feature("connector"));
        }
        if has("instance-connector") {
            body.push_str(&feature("instance-connector"));
        }
        if has("action") {
            body.push_str(&feature("action"));
        }
        if has("api") {
            body.push_str(&feature("api"));
        }
        if has("web") {
            body.push_str(&feature("web"));
        }
        body.trim_end().to_string()
    };
    let scaffold = Scaffold {
        name: plugin_name,
        features: &features,
        action: &action_name,
        dt_col: dt_col.as_deref(),
    };
    let files: Vec<(PathBuf, String)> = match (rust, package) {
        (false, false) => vec![(format!("{plugin_name}.py").into(), python_body())],
        (false, true) => vec![(
            PathBuf::from(plugin_name).join("__init__.py"),
            python_body(),
        )],
        (true, false) => vec![(
            format!("{plugin_name}.rs").into(),
            rust_scaffold(&scaffold)?.trim_end().to_string(),
        )],
        (true, true) => rust_package(&scaffold)?
            .into_iter()
            .map(|(path, text)| (PathBuf::from(plugin_name).join(path), text))
            .collect(),
    };
    if package {
        let directory = plugins_dir.join(plugin_name);
        if directory.exists() {
            return Ok(SuccessTuple::fail(format!(
                "Directory '{}' already exists.",
                directory.display()
            )));
        }
    }
    for (relative, text) in &files {
        let path = plugins_dir.join(relative);
        let written = match path.parent() {
            Some(parent) => std::fs::create_dir_all(parent),
            None => Ok(()),
        }
        .and_then(|_| std::fs::write(&path, text));
        if let Err(e) = written {
            return Ok(SuccessTuple::fail(format!(
                "Failed to write file '{}':\n{e}",
                path.display()
            )));
        }
    }
    // The file to edit: the module or the crate's `src/lib.rs` (the last file written).
    let plugin_path = plugins_dir.join(&files[if rust && package { files.len() - 1 } else { 0 }].0);
    let created = if package {
        plugins_dir.join(plugin_name)
    } else {
        plugin_path.clone()
    };

    clear_screen(rt, args);
    print_tuple(
        rt,
        &SuccessTuple::ok(format!(
            "Successfully created {} '{}'.",
            if package { "package" } else { "file" },
            created.display()
        )),
        TupleArgs::default(),
    );
    let opened = ask(
        rt,
        &format!("Press [Enter] to edit plugin '{plugin_name}', [CTRL+C] to skip."),
        &Ask::new().no_icon(),
    );
    if opened.is_err() {
        return Ok(SuccessTuple::success());
    }
    edit_file(rt, &plugin_path)?;
    Ok(SuccessTuple::success())
}

/// `_get_plugins_dir_path()`.
fn plugins_dir_path(rt: &mut Runtime<'_>) -> Result<PathBuf, ActionError> {
    let dirs = rt.config().paths().plugins_dirs.clone();
    match dirs.len() {
        0 => Err(ActionError::new(
            "OSError",
            "No plugin dir path could be found.",
        )),
        1 => Ok(dirs[0].clone()),
        _ => {
            let shown: Vec<String> = dirs
                .iter()
                .map(|d| d.to_string_lossy().into_owned())
                .collect();
            let chosen = choose(
                rt,
                "In which directory do you want to write your plugin?",
                &shown,
                Some(&shown[0]),
                true,
            )?;
            Ok(PathBuf::from(chosen.unwrap_or_else(|| shown[0].clone())))
        }
    }
}

/// `choose(..., FEATURE_CHOICES.items(), default='fetch', multiple=True, as_indices=True)`:
/// the keys of the features picked, by number or key, separated by commas.
fn choose_features(rt: &mut Runtime<'_>) -> Result<Vec<String>, ActionError> {
    let choices = feature_choices();
    let question = "Which of the following features would you like to add to your plugin?\n    Enter your choices, separated by ','.\n";
    let default = "fetch";
    let noask = check_noask(rt, false);
    let digits = choices.len().to_string().len();
    let options: Vec<String> = choices
        .iter()
        .enumerate()
        .map(|(i, (_, text))| {
            let ix = (i + 1).to_string();
            format!("  {ix}. {}{text}\n", " ".repeat(digits - ix.len()))
        })
        .collect();
    let default_number = choices
        .iter()
        .position(|(key, _)| key == default)
        .map(|i| (i + 1).to_string())
        .unwrap_or_default();
    loop {
        let text = rt.with_formatter(|f| {
            f.print_options(
                &options,
                &OptionsArgs {
                    header: Some(String::new()),
                    ..OptionsArgs::default()
                },
            )
        });
        rt.print(&text);
        let mut prompt = Ask::new().noask(noask).default(default_number.clone());
        prompt.default_label = Some(default.to_string());
        let mut answer = ask(rt, question, &prompt)?;
        if answer.is_empty() {
            answer = default.to_string();
        }
        let answers: Vec<String> = answer
            .split(',')
            .map(|a| a.trim().to_string())
            .filter(|a| !a.is_empty())
            .collect();
        let mut picked: Vec<String> = Vec::new();
        for item in &answers {
            let key = choices
                .iter()
                .find(|(_, text)| text == item)
                .map(|(key, _)| key.clone())
                .or_else(|| {
                    item.parse::<usize>()
                        .ok()
                        .filter(|n| *n >= 1 && n.to_string() == *item)
                        .and_then(|n| choices.get(n - 1))
                        .map(|(key, _)| key.clone())
                })
                .unwrap_or_else(|| item.clone());
            if choices.iter().any(|(k, _)| *k == key) {
                picked.push(key);
            }
        }
        if picked.len() != answers.len() {
            warn(rt, "Please pick a valid choice.");
            if noask {
                return Err(eof());
            }
            continue;
        }
        if !picked.is_empty() {
            return Ok(picked);
        }
    }
}

/// `_get_action_name()`: subactions are separated by spaces, joined with underscores.
fn get_action_name(rt: &mut Runtime<'_>) -> Result<String, ActionError> {
    loop {
        let answer = ask(
            rt,
            "What is name of your action?\n    (separate subactions with spaces, e.g. `sing song`):",
            &Ask::new(),
        )?
        .replace(' ', "_");
        if !answer.is_empty() {
            return Ok(answer);
        }
        warn(rt, "Please enter an action.");
        if check_noask(rt, false) {
            return Err(eof());
        }
    }
}

/// `_get_quoted_dt_col_name()`'s question: the datetime column, `None` to skip.
fn get_dt_col_name(rt: &mut Runtime<'_>) -> Option<String> {
    ask(
        rt,
        "Enter the datetime column name ([CTRL+C] to skip):",
        &Ask::new(),
    )
    .ok()
    .filter(|name| !name.is_empty())
}

/// The datetime column as a Python string literal (`None` to skip).
fn quoted_dt_col_name(name: Option<&str>) -> String {
    match name {
        None => "None".into(),
        Some(name) if name.contains(['"', '\'']) => format!("\"\"\"{name}\"\"\""),
        Some(name) => format!("\"{name}\""),
    }
}

/// Single-file module (default) or a package directory: `<name>/__init__.py` for Python, a
/// Cargo crate (`plugin.toml`, `Cargo.toml`, `src/lib.rs`) for Rust.
fn choose_packaging(rt: &mut Runtime<'_>, name: &str, rust: bool) -> Result<bool, ActionError> {
    let (single, package) = if rust {
        (
            format!("Single file ({name}.rs)"),
            format!("Package ({name}/Cargo.toml)"),
        )
    } else {
        (
            format!("Single file ({name}.py)"),
            format!("Package ({name}/__init__.py)"),
        )
    };
    let choice = choose(
        rt,
        "How would you like to package your plugin?",
        &[single.clone(), package.clone()],
        Some(&single),
        true,
    )?;
    Ok(choice.as_deref() == Some(package.as_str()))
}

/// A Rust plugin's name is its crate, connector type and page path: letters, digits, `_` and
/// `-`, starting with a letter.
fn rust_plugin_name(name: &str) -> Result<(), String> {
    let valid = name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if valid {
        Ok(())
    } else {
        Err(format!(
            "Rust plugin names must start with a letter and contain only letters, digits, '_' or '-' (got '{name}')."
        ))
    }
}

/// Rust keywords: an action named after one is spelled as a raw identifier (`fn r#type`).
const RUST_KEYWORDS: &[&str] = &[
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "do", "dyn",
    "else", "enum", "extern", "false", "final", "fn", "for", "gen", "if", "impl", "in", "let",
    "loop", "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref", "return",
    "static", "struct", "trait", "true", "try", "type", "typeof", "unsafe", "unsized", "use",
    "virtual", "where", "while", "yield",
];

/// The Rust function an action is named after: `#[make_action] fn <action>` is `mrsm <action>`.
fn rust_action_ident(action: &str) -> Result<String, ActionError> {
    let valid = action
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && action
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !matches!(action, "_" | "self" | "super" | "crate");
    if !valid {
        return Err(ActionError::new(
            "ValueError",
            "Rust action names must start with a letter and contain only letters, digits, or underscores.",
        ));
    }
    if matches!(action, "fetch" | "sync" | "register" | "setup") {
        return Err(ActionError::new(
            "ValueError",
            format!("'{action}' is a plugin function name; choose another action name."),
        ));
    }
    Ok(if RUST_KEYWORDS.contains(&action) {
        format!("r#{action}")
    } else {
        action.to_string()
    })
}

/// What the Rust scaffolds are generated from.
struct Scaffold<'a> {
    name: &'a str,
    features: &'a [String],
    action: &'a str,
    dt_col: Option<&'a str>,
}

impl Scaffold<'_> {
    fn has(&self, feature: &str) -> bool {
        self.features.iter().any(|f| f == feature)
    }

    /// The connector type `#[make_connector]` defines (the plugin name with `-` as `_`).
    fn connector_type(&self) -> Result<String, ActionError> {
        let connector = self.name.replace('-', "_");
        if !connector
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
            || ["plugin", "sql", "api", "valkey"].contains(&connector.as_str())
        {
            return Err(ActionError::new(
                "ValueError",
                "Choose a plugin name usable as a connector type (letters, digits, underscores; no built-in connector names).",
            ));
        }
        Ok(connector)
    }

    /// One example row (with the datetime column when one was given).
    fn example_row(&self, extra: &str) -> String {
        match self.dt_col {
            Some(dt) => {
                format!("json!({{{dt:?}: \"2025-01-01T00:00:00+00:00\", \"id\": 1, {extra}}})")
            }
            None => format!("json!({{\"id\": 1, {extra}}})"),
        }
    }

    /// The default parameters `register` returns.
    fn register_value(&self) -> String {
        match self.dt_col {
            Some(dt) => format!("json!({{\"columns\": {{\"datetime\": {dt:?}, \"id\": \"id\"}}}})"),
            None => "json!({\"columns\": {\"id\": \"id\"}})".into(),
        }
    }
}

/// The instance methods a single-file instance connector module answers, with stubs to fill in
/// (the same set as the Python template's `InstanceConnector`).
const RUST_INSTANCE_METHODS: &str = r#"    /// Insert the pipe's keys and parameters into your pipes table.
    fn register_pipe(_pipe: &Pipe) -> SuccessTuple {
        // TODO store `_pipe.connector_keys()`, `_pipe.metric_key()`, `_pipe.location_key()`
        // and `_pipe.parameters()`.
        SuccessTuple::success()
    }

    /// The pipe's stored attributes (keys and `parameters`).
    fn get_pipe_attributes(_pipe: &Pipe) -> Value {
        // TODO read the pipe's row from your pipes table.
        json!({})
    }

    /// The pipe's ID, if it is registered.
    fn get_pipe_id(_pipe: &Pipe) -> Option<i64> {
        // TODO look up the ID mapped to this pipe.
        None
    }

    /// Persist the pipe's in-memory parameters.
    fn edit_pipe(_pipe: &Pipe) -> SuccessTuple {
        // TODO update the pipe's row with `_pipe.parameters()`.
        SuccessTuple::success()
    }

    /// Delete the pipe's registration.
    fn delete_pipe(_pipe: &Pipe) -> SuccessTuple {
        // TODO delete the pipe's row from your pipes table.
        SuccessTuple::success()
    }

    /// The registered pipes' keys that match `query` (`connector_keys`, `metric_keys`,
    /// `location_keys`, `tags`; a `_` prefix negates a value).
    fn fetch_pipes_keys(_query: &Value) -> Value {
        // TODO return `[[connector_keys, metric_key, location_key], ...]`.
        json!([])
    }

    /// Whether the pipe's target table exists.
    fn pipe_exists(_pipe: &Pipe) -> bool {
        // TODO check whether `_pipe.target()` exists.
        false
    }

    /// Drop the pipe's target table.
    fn drop_pipe(_pipe: &Pipe) -> SuccessTuple {
        // TODO drop `_pipe.target()`.
        SuccessTuple::success()
    }

    /// Upsert new rows into the pipe's target table.
    fn sync_pipe(_pipe: &Pipe, data: Vec<Value>) -> (bool, String) {
        // TODO write `data` to `_pipe.target()`.
        (true, format!("Received {} rows.", data.len()))
    }

    /// Delete the rows within `begin`, `end` and `params`.
    fn clear_pipe(_pipe: &Pipe, _begin: Option<Bound>, _end: Option<Bound>, _params: &Value) -> SuccessTuple {
        // TODO delete the matching rows.
        SuccessTuple::success()
    }

    /// The pipe's rows within `begin`, `end` and `params`.
    fn get_pipe_data(_pipe: &Pipe, _begin: Option<Bound>, _end: Option<Bound>, _params: &Value) -> Vec<Value> {
        // TODO read the matching rows from `_pipe.target()`.
        vec![]
    }

    /// The newest (or oldest) value of the datetime axis.
    fn get_sync_time(_pipe: &Pipe, _newest: bool, _params: &Value) -> Option<String> {
        // TODO return the largest datetime as an ISO timestamp (the smallest if not `_newest`).
        None
    }

    /// The target table's columns and their types.
    fn get_pipe_columns_types(_pipe: &Pipe) -> Value {
        // TODO map each column to its type, e.g. `{"id": "int"}`.
        json!({})
    }

    /// How many rows the target table has within `begin`, `end` and `params`.
    fn get_pipe_rowcount(_pipe: &Pipe, _begin: Option<Bound>, _end: Option<Bound>, _params: &Value) -> Option<i64> {
        // TODO count the matching rows.
        Some(0)
    }
"#;

/// Single-file shorthand: no manifest, SDK imports, or ABI boilerplate.
fn rust_scaffold(scaffold: &Scaffold<'_>) -> Result<String, ActionError> {
    let name = scaffold.name;
    let mut body = format!(
        "//! Implement the plugin '{name}'.\n//!\n//! See the Rust Plugins guide for more information:\n//! https://meerschaum.io/reference/plugins/rust-plugins/\n\nconst VERSION: &str = \"0.0.1\";\n\n/// Executed during installation and `mrsm setup plugin {name}`.\nfn setup() -> (bool, String) {{\n    (true, \"Success\".into())\n}}\n\n"
    );
    if scaffold.has("fetch") {
        body.push_str(&format!(
            "/// Return the default parameters for a new pipe.\nfn register() -> Value {{\n    {}\n}}\n\n/// Return the new rows (each `json!` object is one row).\nfn fetch(_pipe: &Pipe, _begin: Option<Bound>, _end: Option<Bound>) -> Vec<Value> {{\n    vec![{}]\n}}\n\n",
            scaffold.register_value(),
            scaffold.example_row("\"value\": 42"),
        ));
    }
    if scaffold.has("connector") || scaffold.has("instance-connector") {
        let connector = scaffold.connector_type()?;
        let instance = scaffold.has("instance-connector");
        body.push_str(&format!(
            "/// Implement '{connector}' connectors.\n#[make_connector(name = {connector:?}{})]\nmod {connector}_connector {{\n    /// Return or yield rows.\n    fn fetch(connector: &Connector, _pipe: &Pipe, _begin: Option<Bound>, _end: Option<Bound>) -> Vec<Value> {{\n        vec![json!({{\"id\": 1, \"source\": connector.label()}})]\n    }}\n",
            if instance { ", instance" } else { "" },
        ));
        if instance {
            body.push('\n');
            body.push_str(RUST_INSTANCE_METHODS);
        }
        body.push_str("}\n\n");
    }
    if scaffold.has("action") {
        let function = rust_action_ident(scaffold.action)?;
        let spaces = scaffold.action.replace('_', " ");
        body.push_str(&format!("/// Run `mrsm {spaces}` to trigger.\n#[make_action]\nfn {function}(action: Vec<String>) -> (bool, String) {{\n    (true, format!(\"Hello, {{}}!\", action.join(\", \")))\n}}\n\n"));
    }
    if scaffold.has("api") {
        body.push_str(&format!("/// Add a new endpoint to the API at `/{name}`.\n#[api_route(GET, \"/{name}\")]\nfn get_my_endpoint(request: &RouteRequest) -> Value {{\n    json!({{\"message\": \"Hello from plugin '{name}'!\", \"user\": request.principal}})\n}}\n\n"));
    }
    if scaffold.has("web") {
        body.push_str(&format!("/// A new page of the web console at `/dash/{name}`.\n#[web_page({name:?}, login_required = false)]\nfn page_layout(page: &PageRequest) -> String {{\n    format!(\n        \"<h1>Hello from plugin '{name}', {{}}!</h1>\",\n        page.username.as_deref().unwrap_or(\"guest\")\n    )\n}}\n\n"));
    }
    Ok(body)
}

/// The SDK requirement of a generated Cargo plugin: this release exactly while it is a
/// pre-release, else its minor series.
fn sdk_requirement() -> String {
    let version = env!("CARGO_PKG_VERSION");
    if version.contains('-') {
        format!("={version}")
    } else {
        version
            .rsplitn(2, '.')
            .last()
            .unwrap_or(version)
            .to_string()
    }
}

/// `UpperCamelCase` for the plugin's struct.
fn camel_case(name: &str) -> String {
    name.split(['-', '_'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|c| c.to_ascii_uppercase().to_string() + chars.as_str())
                .unwrap_or_default()
        })
        .collect()
}

/// A Cargo plugin crate on the full SDK: `(relative path, contents)` of `plugin.toml`,
/// `Cargo.toml` and `src/lib.rs` (last).
fn rust_package(scaffold: &Scaffold<'_>) -> Result<Vec<(String, String)>, ActionError> {
    let name = scaffold.name;
    let has = |feature: &str| scaffold.has(feature);
    let plugin_struct = camel_case(name);
    let plugin_toml = format!("name = {name:?}\nversion = \"0.0.1\"\n");
    let cargo_toml = format!(
        "[package]\nname = {name:?}\nversion = \"0.0.1\"\nedition = \"2024\"\n\n# Built on its own, even inside another Cargo workspace.\n[workspace]\n\n[lib]\ncrate-type = [\"cdylib\"]\n\n[dependencies]\nmeerschaum-plugin = \"{}\"\n\n[profile.release]\n# Required: the SDK catches panics at the plugin boundary.\npanic = \"unwind\"\n",
        sdk_requirement()
    );

    let mut imports = vec!["prelude::*"];
    if has("fetch") || has("connector") || has("instance-connector") {
        imports.push("sdk::FetchOutput");
    }
    if has("api") {
        imports.push("api::{RouteRequest, RouteResponse, RouteSpec}");
    }

    // What the plugin provides, declared with `Manifest`: the SDK writes the manifest document,
    // with the package's name and version.
    let mut declarations = Vec::new();
    if has("fetch") {
        declarations.extend([".fetch()".to_string(), ".register()".to_string()]);
    }
    let connector = if has("connector") || has("instance-connector") {
        Some(scaffold.connector_type()?)
    } else {
        None
    };
    if let Some(connector) = &connector {
        let instance = has("instance-connector");
        let methods = if instance {
            "\"fetch\", \"register_pipe\", \"get_pipe_attributes\", \"get_pipe_id\", \"edit_pipe\", \"delete_pipe\", \"fetch_pipes_keys\", \"pipe_exists\", \"drop_pipe\", \"sync_pipe\", \"clear_pipe\", \"get_pipe_data\", \"get_sync_time\", \"get_pipe_columns_types\", \"get_pipe_rowcount\""
        } else {
            "\"fetch\""
        };
        let kind = if instance { ".instance()" } else { "" };
        declarations.push(format!(
            ".connector(ConnectorType::new({connector:?}, [{methods}]){kind})"
        ));
    }
    let action = if has("action") {
        rust_action_ident(scaffold.action)?;
        let spaces = scaffold.action.replace('_', " ");
        declarations.push(format!(
            ".action({:?}, \"Run `mrsm {spaces}` to trigger.\")",
            scaffold.action
        ));
        Some(scaffold.action)
    } else {
        None
    };
    if has("api") {
        declarations.push(format!(
            ".api_route(RouteSpec::new(\"get_my_endpoint\", \"GET\", \"/{name}\"))"
        ));
    }
    if has("web") {
        declarations.push(format!(".web_page({name:?}, \"page_layout\", false)"));
    }
    let declarations = declarations
        .iter()
        .map(|line| format!("            {line}\n"))
        .collect::<String>();

    let mut arms = Vec::new();
    arms.push(
        "            // Executed during installation and `mrsm setup plugin <name>`.\n            \"setup\" => Ok(Reply::json(json!([true, \"Success\"]))),".to_string(),
    );
    if has("fetch") {
        arms.push(format!(
            "            // The default parameters for a new pipe.\n            \"register\" => Ok(Reply::json({})),",
            scaffold.register_value()
        ));
    }
    if has("fetch") || connector.is_some() {
        arms.push("            \"fetch\" => self.fetch(request).map(Reply::data),".to_string());
    }
    if has("instance-connector") {
        arms.push(r#"            // Instance methods: the pipe is `request.pipe()?`, bounds `request.bound("begin")?`,
            // other arguments `request.field("params")`. Fill in the TODOs.
            "register_pipe" | "edit_pipe" | "delete_pipe" | "drop_pipe" | "clear_pipe" => {
                let _pipe = request.pipe()?;
                // TODO store, update or remove the pipe or its rows.
                Ok(Reply::json(json!([true, "Success"])))
            }
            "sync_pipe" => {
                let rows = data.map(|frame| frame.height()).unwrap_or(0);
                // TODO write `data` to the pipe's target table.
                Ok(Reply::json(json!([true, format!("Received {rows} rows.")])))
            }
            // TODO return the matching rows.
            "get_pipe_data" => Ok(Reply::data(None)),
            // TODO return `[[connector_keys, metric_key, location_key], ...]`.
            "fetch_pipes_keys" => Ok(Reply::json(json!([]))),
            "pipe_exists" => Ok(Reply::json(json!(false))),
            "get_pipe_attributes" | "get_pipe_columns_types" => Ok(Reply::json(json!({}))),
            "get_pipe_id" | "get_sync_time" => Ok(Reply::json(Value::Null)),
            "get_pipe_rowcount" => Ok(Reply::json(json!(0))),"#
            .to_string());
    }
    if let Some(action) = action {
        let spaces = action.replace('_', " ");
        arms.push(format!(
            "            // `mrsm {spaces}`: the words after the action are `context.kwargs.action`.\n            \"action\" if request.context[\"name\"] == {action:?} => {{\n                let words: Vec<&str> = request.context[\"kwargs\"][\"action\"]\n                    .as_array()\n                    .map(|words| words.iter().filter_map(Value::as_str).collect())\n                    .unwrap_or_default();\n                Ok(Reply::json(json!([true, format!(\"Hello, {{}}!\", words.join(\", \"))])))\n            }}"
        ));
    }
    if has("api") {
        arms.push("            \"api_route\" => meerschaum_plugin::sdk::call_server_extension(self, request),".to_string());
    }
    if has("web") {
        arms.push(format!(
            "            // The page's HTML (`context.username` is `null` for a guest).\n            \"page\" => Ok(Reply::json(json!({{\n                \"html\": format!(\n                    \"<h1>Hello from plugin '{name}', {{}}!</h1>\",\n                    request.context[\"username\"].as_str().unwrap_or(\"guest\")\n                )\n            }}))),"
        ));
    }

    let mut methods = String::new();
    if has("fetch") || connector.is_some() {
        let source_row = if let Some(connector) = &connector {
            format!(
                "        if request.connector_type == {connector:?} {{\n            // A `{connector}:<label>` connector; its attributes are in `request.config`.\n            return vec![json!({{\"id\": 1, \"source\": request.label}})].into_frame();\n        }}\n"
            )
        } else {
            String::new()
        };
        let plugin_rows = if has("fetch") {
            format!(
                "        // `plugin:{name}` pipes: fetch the rows between these bounds.\n        let _begin = request.bound(\"begin\")?;\n        let _end = request.bound(\"end\")?;\n        vec![{}].into_frame()\n",
                scaffold.example_row("\"value\": 42")
            )
        } else {
            "        Ok(None)\n".to_string()
        };
        methods.push_str(&format!(
            "\n    /// Return the new rows (each `json!` object is one row).\n    fn fetch(&self, request: &Request) -> Result<Option<Frame>> {{\n{source_row}{plugin_rows}    }}\n"
        ));
    }
    if has("api") {
        methods.push_str(&format!(
            "\n    /// Serve the routes declared under `api_routes`.\n    fn api_route(&self, request: &RouteRequest) -> Result<RouteResponse> {{\n        match request.name.as_str() {{\n            \"get_my_endpoint\" => Ok(RouteResponse::json(&json!({{\n                \"message\": \"Hello from plugin '{name}'!\",\n                \"user\": request.principal,\n            }}))),\n            other => Err(Error::Unsupported(format!(\"unknown route {{other}}\"))),\n        }}\n    }}\n"
        ));
    }
    let data_param = if has("instance-connector") {
        "data"
    } else {
        "_data"
    };
    let lib = format!(
        r#"//! Implement the plugin '{name}'.
//!
//! See the Rust Plugins guide for more information:
//! https://meerschaum.io/reference/plugins/rust-plugins/advanced/

use meerschaum_plugin::{{
    {imports},
}};

#[derive(Default)]
struct {plugin_struct};

impl Plugin for {plugin_struct} {{
    /// What this plugin provides (read without loading the library). Its name and version
    /// are the package's, from Cargo.toml (keep plugin.toml the same).
    fn manifest(&self) -> Value {{
        Manifest::new()
{declarations}            .into()
    }}
{methods}
    fn call(&self, request: &Request, {data_param}: Option<Frame>) -> Result<Reply> {{
        match request.method.as_str() {{
{arms}
            other => Err(Error::Unsupported(format!("{{other}} is not implemented"))),
        }}
    }}
}}

meerschaum_plugin::export_plugin!({plugin_struct});
"#,
        imports = imports.join(",\n    "),
        arms = arms.join("\n"),
    );
    Ok(vec![
        ("plugin.toml".into(), plugin_toml),
        ("Cargo.toml".into(), cargo_toml),
        ("src/lib.rs".into(), lib),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_like_python() {
        let labels = [("plugin_name", "demo")];
        assert_eq!(
            py_format("{{'a': {plugin_name}}} }}", &labels),
            "{'a': demo} }"
        );
        assert_eq!(py_format("no braces", &labels), "no braces");
    }

    #[test]
    fn every_template_formats() {
        let labels = [
            ("plugin_name", "demo"),
            ("plugin_name_capitalized", "Demo"),
            ("plugin_name_lower", "demo"),
            ("action_name", "sing_song"),
            ("action_spaces", "sing song"),
            ("dt_col_name", "\"dt\""),
        ];
        let lines = templates()["feature_lines"].as_object().unwrap();
        for (key, text) in lines {
            let out = py_format(text.as_str().unwrap(), &labels);
            assert!(!out.contains("{plugin_name"), "{key}: {out}");
        }
        assert_eq!(feature_choices().len(), 6);
    }

    const ALL_FEATURES: [&str; 6] = [
        "fetch",
        "connector",
        "instance-connector",
        "action",
        "api",
        "web",
    ];

    fn scaffold_with(
        features: &[&str],
        action: &str,
        dt_col: Option<&str>,
    ) -> Result<String, ActionError> {
        let features: Vec<String> = features.iter().map(|f| f.to_string()).collect();
        rust_scaffold(&Scaffold {
            name: "demo",
            features: &features,
            action,
            dt_col,
        })
    }

    fn scaffold(action: &str) -> Result<String, ActionError> {
        scaffold_with(&["fetch", "action", "connector"], action, None)
    }

    #[test]
    fn rust_offers_every_python_feature() {
        assert_eq!(
            feature_choices()
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>(),
            ALL_FEATURES
        );
    }

    #[test]
    fn rust_actions_are_named_after_their_function() {
        let body = scaffold("sing_song").unwrap();
        assert!(
            body.contains("/// Run `mrsm sing song` to trigger.\n#[make_action]\nfn sing_song(action: Vec<String>)"),
            "{body}"
        );
        assert!(!body.contains("#[make_action("), "{body}");
        assert!(body.contains("const VERSION: &str = \"0.0.1\";"), "{body}");
        assert!(scaffold("type").unwrap().contains("fn r#type("));
        for invalid in ["", "1st", "sing-song", "self", "fetch", "setup"] {
            assert!(scaffold(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn rust_scaffolds_generate_valid_plugins() {
        let dir = tempfile::tempdir().unwrap();
        let mut cases: Vec<(Vec<&str>, Option<&str>)> = ALL_FEATURES
            .iter()
            .map(|feature| (vec![*feature], None))
            .collect();
        cases.push((ALL_FEATURES.to_vec(), Some("timestamp")));
        cases.push((vec!["fetch", "connector"], Some("dt")));
        for (i, (features, dt_col)) in cases.into_iter().enumerate() {
            let body = scaffold_with(&features, "sing_song", dt_col).unwrap();
            let file = dir.path().join("demo.rs");
            std::fs::write(&file, &body).unwrap();
            let generated = dir.path().join(i.to_string());
            meerschaum_plugin::source::prepare_single_file(&file, "demo", &generated)
                .unwrap_or_else(|e| panic!("{features:?}: {e}\n{body}"));
            let lib = std::fs::read_to_string(generated.join("lib.rs")).unwrap();
            assert!(
                lib.contains(r#"const VERSION: &'static str = "0.0.1";"#),
                "{lib}"
            );
            if features.contains(&"action") {
                assert!(lib.contains(r#"\"name\":\"sing_song\""#), "{lib}");
            }
        }
    }

    #[test]
    fn rust_packages_are_cargo_plugin_crates() {
        let features: Vec<String> = ALL_FEATURES.iter().map(|f| f.to_string()).collect();
        let files = rust_package(&Scaffold {
            name: "my-demo",
            features: &features,
            action: "sing_song",
            dt_col: Some("ts"),
        })
        .unwrap();
        let paths: Vec<&str> = files.iter().map(|(p, _)| p.as_str()).collect();
        assert_eq!(paths, ["plugin.toml", "Cargo.toml", "src/lib.rs"]);
        assert!(files[0].1.contains("name = \"my-demo\""));
        assert!(files[1].1.contains("crate-type = [\"cdylib\"]"));
        assert!(files[1].1.contains("panic = \"unwind\""));
        assert!(
            files[1]
                .1
                .contains(&format!("meerschaum-plugin = \"{}\"", sdk_requirement()))
        );
        let lib = &files[2].1;
        assert!(lib.contains("struct MyDemo;"), "{lib}");
        assert!(
            lib.contains("meerschaum_plugin::export_plugin!(MyDemo);"),
            "{lib}"
        );
        for needle in [
            "Manifest::new()",
            ".fetch()",
            ".connector(ConnectorType::new(",
            ".instance())",
            ".action(\"sing_song\", \"Run `mrsm sing song` to trigger.\")",
            ".api_route(RouteSpec::new(\"get_my_endpoint\", \"GET\", \"/my-demo\"))",
            ".web_page(\"my-demo\", \"page_layout\", false)",
            "\"sync_pipe\" =>",
            "\"page\" =>",
        ] {
            assert!(lib.contains(needle), "{needle}\n{lib}");
        }
        // No hand-written manifest: the SDK names and versions the plugin from the package.
        for absent in [
            "meerschaum_version",
            "const NAME",
            "const VERSION",
            "schema_version",
        ] {
            assert!(!lib.contains(absent), "{absent}\n{lib}");
        }
    }

    #[test]
    fn packaging_names_and_versions() {
        assert!(rust_plugin_name("my-plugin_2").is_ok());
        for invalid in ["2nd", "my plugin", "a.b", ""] {
            assert!(rust_plugin_name(invalid).is_err(), "{invalid}");
        }
        assert_eq!(camel_case("my-demo_plugin"), "MyDemoPlugin");
        let requirement = sdk_requirement();
        assert!(requirement.starts_with('=') || !requirement.contains('-'));
        assert_eq!(quoted_dt_col_name(None), "None");
        assert_eq!(quoted_dt_col_name(Some("ts")), "\"ts\"");
    }
}

#[cfg(test)]
mod dump_tmp {
    use super::*;
    #[test]
    #[ignore]
    fn dump() {
        let out = std::path::PathBuf::from(std::env::var("DUMP_DIR").unwrap());
        let all = [
            "fetch",
            "connector",
            "instance-connector",
            "action",
            "api",
            "web",
        ];
        let mut cases: Vec<(String, Vec<&str>, Option<&str>)> = all
            .iter()
            .map(|f| (f.replace('-', "_"), vec![*f], None))
            .collect();
        cases.push(("all".into(), all.to_vec(), Some("ts")));
        for (label, features, dt) in cases {
            let features: Vec<String> = features.iter().map(|f| f.to_string()).collect();
            let name = format!("demo_{label}");
            let s = Scaffold {
                name: &name,
                features: &features,
                action: "sing_song",
                dt_col: dt,
            };
            let single = out.join("single");
            std::fs::create_dir_all(&single).unwrap();
            let file = single.join(format!("{name}.rs"));
            std::fs::write(&file, rust_scaffold(&s).unwrap()).unwrap();
            meerschaum_plugin::source::prepare_single_file(
                &file,
                &name,
                &out.join("gen").join(&name),
            )
            .unwrap();
            for (path, text) in rust_package(&s).unwrap() {
                let p = out.join("pkg").join(&name).join(path);
                std::fs::create_dir_all(p.parent().unwrap()).unwrap();
                std::fs::write(p, text).unwrap();
            }
        }
    }
}
