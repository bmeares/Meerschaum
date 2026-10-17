//! `edit definition` and the interactive forms of `edit pipes` (`actions/edit.py::_edit_pipes`,
//! `Pipe.edit(interactive=True)`, `Pipe.edit_definition()`): open a pipe's SQL definition or its
//! parameters in `$EDITOR` and persist the result.

use meerschaum_core::value::{Map, Value, apply_patch_maps};
use meerschaum_core::{Pipe, SuccessTuple};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::OptionsArgs;
use crate::format::rich::pretty::PyObj;
use crate::value::ArgsMap;

use super::ask::{Ask, ask, eof, is_interrupt};
use super::manage::validate_indices;
use super::pipes::{hydrate, selected_pipes};
use super::{Instance, flag, info, plural, pprint, session, warn, yes_no};

/// `edit definition`: `_edit_pipes(['definition'])`.
pub fn edit_definition(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    edit_pipes(args, rt, true)
}

/// `edit_file(path)`: open the file in the editor (`native::editor`); a failure to start it is
/// the action's result.
fn edit_file(rt: &mut Runtime<'_>, path: &std::path::Path) -> Result<(), SuccessTuple> {
    super::editor::edit_file(rt, path)
        .map(|_| ())
        .map_err(SuccessTuple::fail)
}

/// `textwrap.dedent(text)`.
fn dedent(text: &str) -> String {
    let lines: Vec<&str> = text.split('\n').collect();
    let blank = |l: &str| l.trim().is_empty();
    let margin = lines
        .iter()
        .filter(|l| !blank(l))
        .map(|l| &l[..l.len() - l.trim_start_matches([' ', '\t']).len()])
        .fold(None::<&str>, |acc, indent| match acc {
            None => Some(indent),
            Some(m) => {
                let common = m
                    .chars()
                    .zip(indent.chars())
                    .take_while(|(a, b)| a == b)
                    .count();
                Some(&m[..common])
            }
        })
        .unwrap_or("");
    lines
        .iter()
        .map(|l| {
            if blank(l) {
                ""
            } else {
                l.strip_prefix(margin).unwrap_or(l)
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The body of `_edit_pipes` for the editor paths: `edit pipes` without `--params`, and
/// `edit pipes definition`.
pub fn edit_pipes(args: &ArgsMap, rt: &mut Runtime<'_>, definition: bool) -> ActionResult {
    let (yes, force, noask) = (flag(args, "yes"), flag(args, "force"), flag(args, "noask"));
    let params = super::params(args).filter(|p| !p.is_empty());
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes to edit."));
    }
    let options: Vec<String> = pipes.iter().map(|p| p.pipe.to_string()).collect();
    let text = rt.with_formatter(|f| {
        f.print_options(
            &options,
            &OptionsArgs {
                header: Some("Pipes to be edited:".into()),
                ..OptionsArgs::default()
            },
        )
    });
    rt.print(&text);
    let skip_prompts = yes || force || noask;
    if pipes.len() > 1 && !skip_prompts {
        let q = format!(
            "Press [Enter] to begin editing the above {} pipe{} or [CTRL-C] to cancel:",
            pipes.len(),
            plural(pipes.len())
        );
        match ask(rt, &q, &Ask::new().no_icon()) {
            Err(e) if is_interrupt(&e) => return Ok(SuccessTuple::fail("No pipes changed.")),
            Err(e) => return Err(e),
            Ok(_) => {}
        }
    }
    let sess = session(rt);
    let mut success = true;
    let mut msg = String::new();
    for sp in pipes.iter_mut() {
        if !skip_prompts {
            let q = format!("Press [Enter] to edit {} or [CTRL-C] to skip:", sp.pipe);
            match ask(rt, &q, &Ask::new().no_icon()) {
                Err(e) if is_interrupt(&e) => continue,
                Err(e) => return Err(e),
                Ok(t) if t == "pass" => continue,
                Ok(_) => {}
            }
        }
        hydrate(rt, &inst, sp);
        if let Some(params) = &params {
            info(
                rt,
                &format!("Will patch the following parameters into {}:", sp.pipe),
            );
            pprint(rt, &PyObj::from_value(&Value::Map(params.clone())), false);
            let patched = apply_patch_maps(sp.pipe.raw_parameters(), params);
            sp.pipe.set_parameters(patched);
        }
        let res = if definition {
            edit_pipe_definition(rt, &inst, &mut sp.pipe, args)?
        } else {
            edit_pipe_yaml(rt, &inst, &mut sp.pipe, params.is_none())?
        };
        let _ = &sess;
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

/// `Pipe.edit(interactive=...)`.
fn edit_pipe_yaml(
    rt: &mut Runtime<'_>,
    inst: &Instance,
    pipe: &mut Pipe,
    interactive: bool,
) -> Result<SuccessTuple, ActionError> {
    if pipe.temporary {
        return Ok(SuccessTuple::fail(
            "Cannot edit pipes created with `temporary=True` (read-only).",
        ));
    }
    let sess = session(rt);
    if !interactive {
        let v = validate_indices(pipe);
        if !v.success {
            return Ok(v);
        }
        return Ok(sess.block_on(inst.conn().edit_pipe(pipe, false)));
    }
    let dir = rt.config().paths().path("PIPES_CACHE_RESOURCES_PATH");
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join(format!("{pipe}.yaml"));
    let edit_text = format!("Edit the parameters for {pipe}");
    let top = "#".repeat(edit_text.chars().count() + 4);
    let header = format!("{top}\n# {edit_text} #\n{top}\n\n");
    let defaults = rt
        .config()
        .get(&["pipes", "parameters"])
        .map(|v| v.to_map())
        .unwrap_or_default();
    let parameters = apply_patch_maps(&defaults, pipe.raw_parameters());
    let dumped = meerschaum_core::yaml::dump(&Value::Map(parameters))
        .map_err(|e| ActionError::new("YAMLError", e.0))?;
    std::fs::write(&path, format!("{header}{dumped}"))
        .map_err(|e| ActionError::new("OSError", e.to_string()))?;
    let file_parameters = loop {
        if let Err(failure) = edit_file(rt, &path) {
            return Ok(failure);
        }
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        match meerschaum_core::yaml::load(&text) {
            Ok(v) => break v,
            Err(e) => {
                warn(
                    rt,
                    &format!("Invalid format defined for '{pipe}':\n\n{}", e.0),
                );
                rt.host
                    .read_line(&format!(
                        "Press [Enter] to correct the configuration for '{pipe}': "
                    ))
                    .ok_or_else(eof)?;
            }
        }
    };
    pipe.set_parameters(match file_parameters {
        Value::Map(m) => m,
        _ => Map::new(),
    });
    Ok(sess.block_on(inst.conn().edit_pipe(pipe, false)))
}

/// `Pipe.edit_definition()`.
fn edit_pipe_definition(
    rt: &mut Runtime<'_>,
    inst: &Instance,
    pipe: &mut Pipe,
    args: &ArgsMap,
) -> Result<SuccessTuple, ActionError> {
    if pipe.temporary {
        return Ok(SuccessTuple::fail(
            "Cannot edit pipes created with `temporary=True` (read-only).",
        ));
    }
    let source_type = pipe
        .connector_keys()
        .split(':')
        .next()
        .unwrap_or_default()
        .to_string();
    if source_type != "sql" && source_type != "api" {
        return edit_pipe_yaml(rt, inst, pipe, true);
    }
    let (yes, force, noask) = (flag(args, "yes"), flag(args, "force"), flag(args, "noask"));
    let mut parameters = pipe.raw_parameters().clone();
    let mut fetch = parameters
        .get("fetch")
        .map(|v| v.to_map())
        .unwrap_or_default();
    let dir = rt.config().paths().path("PIPES_CACHE_RESOURCES_PATH");
    let _ = std::fs::create_dir_all(&dir);
    if source_type == "api" {
        info(
            rt,
            &format!(
                "Please enter the keys of the source pipe from '{}'.\nType 'None' for None, or empty when there is no default. Press [CTRL+C] to skip.",
                pipe.connector_keys()
            ),
        );
        let mut keys = Map::new();
        for k in ["connector_keys", "metric_key", "location_key"] {
            let current = fetch.get(k).filter(|v| !v.is_null()).map(Value::py_str);
            let mut options = Ask::new();
            if let Some(c) = current {
                options = options.default(c);
            }
            let label = {
                let mut chars = k.chars();
                let first = chars.next().map(|c| c.to_uppercase().to_string());
                format!("{}{}", first.unwrap_or_default(), chars.as_str()).replace('_', " ")
            };
            let answer = match ask(rt, &format!("{label}:"), &options) {
                Ok(a) => a,
                Err(e) if !is_interrupt(&e) => return Err(e),
                Err(_) => {
                    let existing = fetch.get(k).cloned().unwrap_or(Value::Null);
                    keys.insert(k.to_string(), existing);
                    continue;
                }
            };
            let value = if matches!(answer.as_str(), "" | "None" | "'None'" | "[None]") {
                Value::Null
            } else {
                Value::Str(answer)
            };
            keys.insert(k.to_string(), value);
        }
        fetch = apply_patch_maps(&fetch, &keys);
        info(
            rt,
            "You may optionally specify additional filter parameters as JSON.",
        );
        rt.print(
            "  Parameters are translated into a 'WHERE x AND y' clause, and lists are IN clauses.\n  For example, the following JSON would correspond to 'WHERE x = 1 AND y IN (2, 3)':\n{\n  \"x\": 1,\n  \"y\": [\n    2,\n    3\n  ]\n}\n",
        );
        if force
            || yes_no(
                rt,
                "Would you like to add additional filter parameters?",
                "y",
                yes,
                noask,
            )?
        {
            let path = dir.join(format!("{pipe}.json"));
            let current = fetch
                .get("params")
                .cloned()
                .unwrap_or(Value::Map(Map::new()));
            let text = meerschaum_core::pyfmt::json_dumps(
                &current,
                meerschaum_core::pyfmt::JsonStyle::Indent(2),
                true,
            );
            if let Err(e) = std::fs::write(&path, text) {
                return Ok(SuccessTuple::fail(format!(
                    "Failed writing file '{}':\n{e}",
                    path.display()
                )));
            }
            let params: Option<Value> = loop {
                if let Err(failure) = edit_file(rt, &path) {
                    return Ok(failure);
                }
                let text = std::fs::read_to_string(&path).unwrap_or_default();
                match Value::from_json_str(&text) {
                    Ok(v) => break Some(v),
                    Err(e) => {
                        warn(rt, &format!("Failed to read parameters JSON:\n{e}"));
                        if force
                            || yes_no(
                                rt,
                                "Would you like to try again?\n  If not, the parameters JSON file will be ignored.",
                                "y",
                                yes,
                                noask,
                            )?
                        {
                            continue;
                        }
                        break None;
                    }
                }
            };
            if let Some(p) = params {
                fetch.insert("params".into(), p);
            }
        }
        parameters.insert("fetch".into(), Value::Map(fetch));
        pipe.set_parameters(parameters);
    } else {
        let path = dir.join(format!("{pipe}.sql"));
        let definition = fetch
            .get("definition")
            .filter(|v| !v.is_null())
            .map(Value::py_str)
            .unwrap_or_default();
        let definition = dedent(&definition).trim_start().to_string();
        if let Err(e) = std::fs::write(&path, &definition) {
            return Ok(SuccessTuple::fail(format!(
                "Failed writing file '{}':\n{e}",
                path.display()
            )));
        }
        if let Err(failure) = edit_file(rt, &path) {
            return Ok(failure);
        }
        let file_definition = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(e) => {
                return Ok(SuccessTuple::fail(format!(
                    "Failed reading file '{}':\n{e}",
                    path.display()
                )));
            }
        };
        if definition == file_definition {
            return Ok(SuccessTuple::fail(format!(
                "No changes made to definition for {pipe}."
            )));
        }
        if !file_definition.contains(' ') {
            return Ok(SuccessTuple::fail(format!(
                "Invalid SQL definition for {pipe}."
            )));
        }
        fetch.insert("definition".into(), Value::Str(file_definition));
        parameters.insert("fetch".into(), Value::Map(fetch));
        pipe.set_parameters(parameters);
    }
    edit_pipe_yaml(rt, inst, pipe, false)
}
