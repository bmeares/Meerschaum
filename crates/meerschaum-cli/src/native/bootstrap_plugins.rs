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
    // The name becomes `<plugins>/<name>.py`.
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

    let features = choose_features(rt)?;
    clear_screen(rt, args);

    let mut action_name = String::new();
    if features.iter().any(|f| f == "action") {
        action_name = get_action_name(rt)?;
        clear_screen(rt, args);
    }
    let action_spaces = action_name.replace('_', " ");

    let mut dt_col_name = "None".to_string();
    if features.iter().any(|f| f == "fetch") {
        dt_col_name = get_quoted_dt_col_name(rt);
        clear_screen(rt, args);
    }

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

    let plugin_path = plugins_dir.join(format!("{plugin_name}.py"));
    if let Err(e) = std::fs::write(&plugin_path, body.trim_end()) {
        return Ok(SuccessTuple::fail(format!(
            "Failed to write file '{}':\n{e}",
            plugin_path.display()
        )));
    }

    clear_screen(rt, args);
    print_tuple(
        rt,
        &SuccessTuple::ok(format!(
            "Successfully created file '{}'.",
            plugin_path.display()
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

/// `_get_quoted_dt_col_name()`: the answer as a Python string literal (`None` to skip).
fn get_quoted_dt_col_name(rt: &mut Runtime<'_>) -> String {
    match ask(
        rt,
        "Enter the datetime column name ([CTRL+C] to skip):",
        &Ask::new(),
    ) {
        Err(_) => "None".into(),
        Ok(name) if name.contains(['"', '\'']) => format!("\"\"\"{name}\"\"\""),
        Ok(name) => format!("\"{name}\""),
    }
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
}
