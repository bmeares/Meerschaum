//! JSON schema of the argument surface (`mrsm --print-arg-schema`).
//!
//! The Python `parse_arguments` (kept in the SDK for plugins that build sysargs) reads this
//! instead of declaring its own parser (§4.3). Field names follow `argparse.Action` attributes
//! (`dest`, `option_strings`, `nargs`, `type`, `default`, `const`, `choices`, `required`, `help`,
//! `metavar`) so the Python side can rebuild an equivalent `ArgumentParser`; clap-side names
//! (`long`, `short`, `aliases`, `short_aliases`, `num_args`) are included for Rust consumers.

use serde_json::{Value, json};

use crate::command::{command, option_strings};
use crate::spec::{self, Action, Group, Nargs};

/// Version of the schema layout.
pub const SCHEMA_VERSION: u32 = 1;

fn nargs_json(nargs: Nargs) -> Value {
    match nargs {
        Nargs::Single => Value::Null,
        Nargs::ZeroOrMore => json!("*"),
        Nargs::OneOrMore => json!("+"),
        Nargs::Remainder => json!("..."),
        Nargs::Exactly(n) => json!(n),
    }
}

/// The argument schema as JSON.
pub fn arg_schema() -> Value {
    let cmd = command();
    let mut arguments = Vec::new();
    for arg in cmd.get_arguments() {
        let Some(spec) = spec::by_dest(arg.get_id().as_str()) else {
            continue;
        };
        let range = arg.get_num_args();
        let num_args = range.map(|r| {
            json!({
                "min": r.min_values(),
                "max": if r.max_values() == usize::MAX { Value::Null } else { json!(r.max_values()) },
            })
        });
        let choices: Option<Vec<String>> = {
            let pv = arg.get_possible_values();
            if pv.is_empty() {
                None
            } else {
                Some(pv.iter().map(|p| p.get_name().to_string()).collect())
            }
        };
        arguments.push(json!({
            "dest": spec.dest,
            "positional": arg.is_positional(),
            "option_strings": option_strings(arg),
            "long": arg.get_long(),
            "short": arg.get_short().map(|c| c.to_string()),
            "aliases": arg.get_visible_aliases().unwrap_or_default(),
            "short_aliases": arg.get_visible_short_aliases().unwrap_or_default().iter().map(|c| c.to_string()).collect::<Vec<_>>(),
            "action": match spec.action { Action::Store => "store", Action::StoreTrue => "store_true" },
            "action_class": spec.action.class_name(),
            "nargs": nargs_json(spec.nargs),
            "num_args": num_args,
            "type": spec.arg_type.name(),
            "default": match spec.action { Action::StoreTrue => json!(false), Action::Store => Value::Null },
            "const": match spec.action { Action::StoreTrue => json!(true), Action::Store => Value::Null },
            "choices": choices,
            "required": arg.is_required_set(),
            "help": arg.get_help().map(|h| h.to_string()),
            "metavar": Value::Null,
            "value_name": arg.get_value_names().and_then(|v| v.first().map(|s| s.to_string())),
            "group": arg.get_help_heading(),
        }));
    }
    // Additive engine arguments are parsed before the frozen v4 argument table.
    for (dest, flag, help) in [
        (
            "run_id",
            "--run-id",
            "Unique execution id for structured run correlation.",
        ),
        (
            "resume",
            "--resume",
            "Resume a prior run under a new execution id.",
        ),
    ] {
        arguments.push(
            json!({"dest":dest,"positional":false,"option_strings":[flag],
            "long":flag.trim_start_matches('-'),"short":null,"aliases":[],"short_aliases":[],
            "action":"store","action_class":"_StoreAction","nargs":null,
            "num_args":{"min":1,"max":1},"type":"str","default":null,"const":null,
            "choices":null,"required":false,"help":help,"metavar":null,"value_name":"RUN_ID",
            "group":"Syncing Engine"}),
        );
    }
    json!({
        "schema_version": SCHEMA_VERSION,
        "mrsm_version": env!("CARGO_PKG_VERSION"),
        "prog": spec::PROG,
        "usage": spec::USAGE,
        "description": spec::DESCRIPTION,
        "groups": Group::ALL.iter().map(|g| g.title()).collect::<Vec<_>>(),
        "keys": {
            "sub_decorators": [spec::keys::SUB_BEGIN, spec::keys::SUB_END],
            "underscore_standin": spec::keys::UNDERSCORE_STANDIN,
            "failure_key": spec::keys::FAILURE_KEY,
            "and_key": spec::keys::AND,
            "escaped_and_key": spec::keys::ESCAPED_AND,
            "pipeline_key": spec::keys::PIPELINE,
            "escaped_pipeline_key": spec::keys::ESCAPED_PIPELINE,
        },
        "arguments": arguments,
    })
}
