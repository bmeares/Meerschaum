//! The clap [`Command`] for `mrsm`, built from the argument table.
//!
//! The command is the introspectable form of the v4 argument surface: every option string
//! (primary long/short plus visible aliases), arity, possible values, help text and heading.
//! `argparse` semantics that clap cannot express (see `crate::argparse`) are applied by the
//! parsing engine; the JSON schema (`crate::schema`) and `--help` (`crate::help`) read this
//! command back.

use clap::builder::PossibleValuesParser;
use clap::{Arg, ArgAction, Command};

use crate::spec::{self, Action, ArgSpec, Nargs};

fn build_arg(spec: &ArgSpec) -> Arg {
    let mut arg = Arg::new(spec.dest)
        .help(spec.help)
        .help_heading(spec.group.title());
    if spec.is_positional() {
        return arg
            .num_args(0..)
            .action(ArgAction::Append)
            .value_name(spec.dest);
    }
    let shorts: Vec<char> = spec
        .option_strings
        .iter()
        .filter(|o| !o.starts_with("--"))
        .filter_map(|o| o.chars().nth(1))
        .collect();
    let longs: Vec<&'static str> = spec
        .option_strings
        .iter()
        .filter_map(|o| o.strip_prefix("--"))
        .collect();
    if let Some((first, rest)) = shorts.split_first() {
        arg = arg.short(*first);
        if !rest.is_empty() {
            arg = arg.visible_short_aliases(rest.to_vec());
        }
    }
    if let Some((first, rest)) = longs.split_first() {
        arg = arg.long(*first);
        if !rest.is_empty() {
            arg = arg.visible_aliases(rest.to_vec());
        }
    }
    arg = match (spec.action, spec.nargs) {
        (Action::StoreTrue, _) => arg.action(ArgAction::SetTrue),
        (Action::Store, Nargs::Single) => arg.action(ArgAction::Set).num_args(1),
        (Action::Store, Nargs::OneOrMore) => arg.action(ArgAction::Set).num_args(1..),
        (Action::Store, Nargs::ZeroOrMore) => arg.action(ArgAction::Set).num_args(0..),
        (Action::Store, Nargs::Remainder) => arg
            .action(ArgAction::Set)
            .num_args(0..)
            .allow_hyphen_values(true),
        (Action::Store, Nargs::Exactly(n)) => arg.action(ArgAction::Set).num_args(n),
    };
    if spec.action == Action::Store {
        arg = arg.value_name(spec.dest.to_uppercase());
    }
    if let Some(choices) = spec.choices {
        arg = arg.value_parser(PossibleValuesParser::new(choices.iter().copied()));
    }
    arg
}

/// The `mrsm` clap command.
pub fn command() -> Command {
    let mut cmd = Command::new(spec::PROG)
        .about(spec::DESCRIPTION)
        .override_usage(spec::USAGE)
        .version(env!("CARGO_PKG_VERSION"))
        .disable_help_flag(true)
        .disable_version_flag(true)
        .disable_help_subcommand(true);
    for spec in spec::ARGUMENTS {
        cmd = cmd.arg(build_arg(spec));
    }
    cmd
}

/// Every option string of a clap argument (`-c`, `-C`, `--connector-keys`), in the declared
/// v4 order. clap stores shorts, longs and aliases separately, so the order comes from the table.
pub fn option_strings(arg: &Arg) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    if let Some(s) = arg.get_short() {
        found.push(format!("-{s}"));
    }
    for s in arg.get_visible_short_aliases().unwrap_or_default() {
        found.push(format!("-{s}"));
    }
    if let Some(l) = arg.get_long() {
        found.push(format!("--{l}"));
    }
    for l in arg.get_visible_aliases().unwrap_or_default() {
        found.push(format!("--{l}"));
    }
    match spec::by_dest(arg.get_id().as_str()) {
        Some(spec) => {
            let mut ordered: Vec<String> =
                spec.option_strings.iter().map(|s| s.to_string()).collect();
            ordered.retain(|o| found.contains(o));
            ordered
        }
        None => found,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_is_valid_and_mirrors_table() {
        let cmd = command();
        cmd.clone().debug_assert();
        for spec in spec::ARGUMENTS {
            let arg = cmd
                .get_arguments()
                .find(|a| a.get_id() == spec.dest)
                .expect("arg present");
            let expected: Vec<String> = spec.option_strings.iter().map(|s| s.to_string()).collect();
            assert_eq!(option_strings(arg), expected, "{}", spec.dest);
            assert_eq!(
                arg.get_help().map(|h| h.to_string()).as_deref(),
                Some(spec.help)
            );
        }
    }
}
