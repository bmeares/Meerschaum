//! `choose_subaction` for the actions whose words did not name a subaction (`uninstall`,
//! `verify`, `upgrade`, `delete` without or with an unknown one), and `start worker`, ported from
//! `meerschaum/actions/__init__.py::choose_subaction` and `meerschaum/actions/start.py`.
//!
//! # `start worker`
//!
//! v4's worker is the experimental CLI daemon (`system.experimental.cli_daemon`): an internal
//! entry point `mrsm start worker <index>` that a client feeds actions through a named pipe.
//! v5 has no CLI daemon (`docs/design/v5.0.0.md`, "No CLI daemon": actions run natively in
//! `mrsm`), so the argument checks are v4's and a valid index is refused with that explanation.

use meerschaum_core::SuccessTuple;

use crate::actions::{CORE_ACTIONS, get_subactions};
use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::value::{ArgsMap, PyValue};

use super::{warn, words};

/// Aliases `choose_subaction`'s option tables carry beyond the subaction names: `(action, alias,
/// function)`.
const ALIASES: &[(&str, &str, &str)] = &[("upgrade", "mrsm", "_upgrade_meerschaum")];

/// `choose_subaction(action, options)` for the registry's subactions of `parent`: run the chosen
/// one (a trailing `s` is optional) with the remaining words, or list the choices.
pub fn choose_subaction(parent: &str, args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let mut options: Vec<(String, String)> = get_subactions(CORE_ACTIONS, parent)
        .iter()
        .map(|s| (s.name.to_string(), s.function.to_string()))
        .collect();
    for (action, alias, function) in ALIASES {
        if *action == parent {
            options.push(((*alias).to_string(), (*function).to_string()));
        }
    }
    let mut action = words(args, "action");
    if action.is_empty() {
        action = vec![String::new()];
    }
    let choice = action[0].clone();
    let chosen = options
        .iter()
        .find(|(name, _)| *name == choice)
        .or_else(|| {
            let plural = format!("{choice}s");
            options.iter().find(|(name, _)| *name == plural)
        });
    let Some((name, function)) = chosen else {
        warn(rt, &format!("Cannot {parent} '{choice}'. Choose one:"));
        let mut names: Vec<&String> = options.iter().map(|(name, _)| name).collect();
        names.sort();
        for name in names {
            rt.print(&format!("  - {parent} {name}\n"));
        }
        return Ok(SuccessTuple::fail(format!("Invalid choice '{choice}'.")));
    };
    let mut sub_args = args.clone();
    sub_args.insert("action".into(), PyValue::str_list(&action[1..]));
    match super::run(function, &sub_args, rt) {
        Some(result) => result,
        // Every subaction of these parents is native; a registry entry without a handler is a
        // bug, never a Python fallback.
        None => Ok(SuccessTuple::fail(format!(
            "`{parent} {name}` has no native handler."
        ))),
    }
}

/// `start worker <index>`.
pub fn start_worker(args: &ArgsMap, _rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let Some(index) = action.first() else {
        return Ok(SuccessTuple::fail("No worker index is provided."));
    };
    if !index.trim().parse::<f64>().is_ok_and(|n| n.fract() == 0.0) {
        return Ok(SuccessTuple::fail("Invalid worker index."));
    }
    Ok(SuccessTuple::fail(
        "The CLI worker is not used in Meerschaum v5: actions run natively in `mrsm`.",
    ))
}
