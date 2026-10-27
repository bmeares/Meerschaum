//! Help: what `mrsm help`, `mrsm -h`, `show help` and the shell's `help` print, and the help of
//! one action (`mrsm sync pipes -h`, `help sync pipes`).
//!
//! The global help starts with the actions in groups (Create, Sync, Run, Inspect, Maintain,
//! Configure, then the plugins' actions and their subcommands), each with the first line of its
//! docstring, then the options in `argparse`'s layout: a port of CPython 3.14
//! `argparse.HelpFormatter` (and the parts of `textwrap` it uses) over the clap [`Command`] built
//! from the argument table, plus the flags parsed ahead of that table ([`EXTRA_OPTIONS`], the
//! "Pipeline options" among them) and the plugins' `add_plugin_argument` flags.

use clap::Command;

use meerschaum_core::pyfmt::dedent;
use meerschaum_worker::manifest::{ArgumentManifest, Manifest};

use crate::actions::{ActionDef, CORE_ACTIONS};
use crate::spec::{self, Group};

/// The help width: `$COLUMNS`, else the width of the terminal on stdout, else 80.
pub fn terminal_columns() -> usize {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|c| c.trim().parse::<usize>().ok())
        .filter(|c| *c > 0)
        .or_else(|| crate::format::terminal_width(1))
        .unwrap_or(80)
}

fn is_ws(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\x0b' | '\x0c' | '\r')
}

fn is_w(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn is_letter(c: char) -> bool {
    c == '_' || c.is_alphabetic()
}

fn is_word_punct(c: char) -> bool {
    is_w(c) || "!\"'&.,?".contains(c)
}

/// `TextWrapper.wordsep_re.split` (with `break_on_hyphens=True`), empty chunks dropped.
fn split_chunks(text: &str) -> Vec<String> {
    let c: Vec<char> = text.chars().collect();
    let n = c.len();
    let at = |i: usize| c.get(i).copied();
    let mut chunks = Vec::new();
    let mut p = 0usize;
    while p < n {
        // Any whitespace.
        if is_ws(c[p]) {
            let start = p;
            while p < n && is_ws(c[p]) {
                p += 1;
            }
            chunks.push(c[start..p].iter().collect());
            continue;
        }
        // An em-dash between words.
        if c[p] == '-' && p > 0 && is_word_punct(c[p - 1]) {
            let k = c[p..].iter().take_while(|x| **x == '-').count();
            if k >= 2 && at(p + k).is_some_and(is_w) {
                chunks.push(c[p..p + k].iter().collect());
                p += k;
                continue;
            }
        }
        // A word, possibly hyphenated (lazy `\S+?`).
        let mut end = n;
        for j in p + 1..=n {
            if j < n && c[j] == '-' {
                let behind = (j >= 2 && is_letter(c[j - 2]) && is_letter(c[j - 1]))
                    || (j >= 3 && is_letter(c[j - 3]) && c[j - 2] == '-' && is_letter(c[j - 1]));
                let ahead = at(j + 1).is_some_and(is_letter)
                    && (at(j + 2).is_some_and(is_letter)
                        || (at(j + 2) == Some('-') && at(j + 3).is_some_and(is_letter)));
                if behind && ahead {
                    end = j + 1;
                    break;
                }
            }
            if j == n || is_ws(c[j]) {
                end = j;
                break;
            }
            if is_word_punct(c[j - 1]) && c[j] == '-' {
                let k = c[j..].iter().take_while(|x| **x == '-').count();
                if k >= 2 && at(j + k).is_some_and(is_w) {
                    end = j;
                    break;
                }
            }
        }
        chunks.push(c[p..end].iter().collect());
        p = end;
    }
    chunks
}

/// `textwrap.wrap(text, width)` with the default `TextWrapper` options.
pub fn wrap(text: &str, width: usize) -> Vec<String> {
    let text: String = text
        .chars()
        .map(|ch| if is_ws(ch) { ' ' } else { ch })
        .collect();
    let mut chunks: Vec<String> = split_chunks(&text);
    chunks.reverse();
    let len = |s: &String| s.chars().count();
    let mut lines: Vec<String> = Vec::new();
    while !chunks.is_empty() {
        let mut cur_line: Vec<String> = Vec::new();
        let mut cur_len = 0usize;
        if chunks.last().is_some_and(|c| c.trim().is_empty()) && !lines.is_empty() {
            chunks.pop();
        }
        while let Some(last) = chunks.last() {
            let l = len(last);
            if cur_len + l <= width {
                cur_len += l;
                cur_line.push(chunks.pop().unwrap_or_default());
            } else {
                break;
            }
        }
        if let Some(last) = chunks.last()
            && len(last) > width
        {
            // `_handle_long_word`
            let space_left = if width < 1 { 1 } else { width - cur_len };
            let chunk: Vec<char> = last.chars().collect();
            let mut end = space_left;
            if chunk.len() > space_left {
                let hyphen = chunk[..space_left.min(chunk.len())]
                    .iter()
                    .rposition(|x| *x == '-');
                if let Some(h) = hyphen
                    && h > 0
                    && chunk[..h].iter().any(|x| *x != '-')
                {
                    end = h + 1;
                }
            }
            let end = end.min(chunk.len());
            cur_line.push(chunk[..end].iter().collect());
            let rest: String = chunk[end..].iter().collect();
            let ix = chunks.len() - 1;
            chunks[ix] = rest;
        }
        if cur_line.last().is_some_and(|c| c.trim().is_empty()) {
            cur_line.pop();
        }
        if !cur_line.is_empty() {
            lines.push(cur_line.concat());
        }
    }
    lines
}

/// `_whitespace_matcher.sub(' ', text).strip()`.
fn normalize_ws(text: &str) -> String {
    text.split(is_ws)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// One entry of a two-column listing: what to type, and what it does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelpRow {
    /// The action words or the option strings (with their metavariables).
    pub invocation: String,
    /// The one-line description (wrapped to the terminal).
    pub help: String,
}

impl HelpRow {
    fn new(invocation: impl Into<String>, help: impl Into<String>) -> HelpRow {
        HelpRow {
            invocation: invocation.into(),
            help: help.into(),
        }
    }
}

/// A titled group of rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelpGroup {
    /// The heading (without the colon).
    pub title: String,
    /// The rows, in order.
    pub rows: Vec<HelpRow>,
}

/// The help position `argparse` uses for rows with these invocations: two columns past the
/// longest one (plus the indent), at most 24.
fn help_position<'a>(invocations: impl Iterator<Item = &'a str>, width: usize) -> usize {
    let max_help_position = 24usize.min(width.saturating_sub(20).max(4));
    let indent = 2usize;
    let longest = invocations
        .map(|i| i.chars().count() + indent)
        .max()
        .unwrap_or(0);
    (longest + 2).min(max_help_position)
}

/// `HelpFormatter._format_action` for each row: the invocation, then the wrapped help aligned at
/// `help_position` (on the next line when the invocation is too long).
fn format_rows(rows: &[HelpRow], width: usize, help_position: usize) -> String {
    let indent = 2usize;
    let help_width = width.saturating_sub(help_position).max(11);
    let action_width = help_position.saturating_sub(indent + 2);
    let mut items = String::new();
    for row in rows {
        let header = &row.invocation;
        if row.help.trim().is_empty() {
            items.push_str(&format!("{}{header}\n", " ".repeat(indent)));
            continue;
        }
        let indent_first = if header.chars().count() <= action_width {
            items.push_str(&format!("{}{header:<action_width$}  ", " ".repeat(indent)));
            0
        } else {
            items.push_str(&format!("{}{header}\n", " ".repeat(indent)));
            help_position
        };
        let lines = wrap(&normalize_ws(&row.help), help_width);
        if let Some((first, rest)) = lines.split_first() {
            items.push_str(&format!("{}{first}\n", " ".repeat(indent_first)));
            for line in rest {
                items.push_str(&format!("{}{line}\n", " ".repeat(help_position)));
            }
        }
    }
    items
}

/// Groups as `\n<title>:\n<rows>`, every group's rows aligned at one help position (as
/// `argparse` aligns a parser's actions).
fn format_groups(groups: &[HelpGroup], width: usize) -> String {
    let position = help_position(
        groups
            .iter()
            .flat_map(|g| g.rows.iter().map(|r| r.invocation.as_str())),
        width,
    );
    let mut out = String::new();
    for group in groups {
        if group.rows.is_empty() {
            continue;
        }
        out.push_str(&format!(
            "\n{}:\n{}\n",
            group.title,
            format_rows(&group.rows, width, position)
        ));
    }
    out
}

/// `_long_break_matcher.sub('\n\n', help)`, then `strip('\n') + '\n'`.
fn collapse_blank_lines(text: &str) -> String {
    let mut collapsed = String::with_capacity(text.len());
    let mut newlines = 0usize;
    for ch in text.chars() {
        if ch == '\n' {
            newlines += 1;
            continue;
        }
        if newlines > 0 {
            collapsed.push_str(if newlines >= 2 { "\n\n" } else { "\n" });
            newlines = 0;
        }
        collapsed.push(ch);
    }
    format!("{}\n", collapsed.trim_matches('\n'))
}

/// The first line of a docstring: an action's one-line description.
pub fn summary(doc: Option<&str>) -> String {
    doc.unwrap_or_default()
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or_default()
        .to_string()
}

/// The groups of the action list, in order, with the core actions each lists. A listed core
/// action missing here goes to "Other" (so a new action is never hidden).
pub const ACTION_GROUPS: &[(&str, &[&str])] = &[
    ("Create", &["bootstrap", "register", "copy"]),
    ("Sync", &["sync", "plan", "verify", "watch"]),
    (
        "Run",
        &[
            "start", "stop", "pause", "restart", "attach", "compose", "api", "stack",
        ],
    ),
    ("Inspect", &["show", "sql", "python", "doctor", "help"]),
    (
        "Maintain",
        &[
            "clear",
            "drop",
            "delete",
            "deduplicate",
            "flush",
            "index",
            "partition",
            "compress",
            "decompress",
            "vacuum",
            "analyze",
            "prune",
            "backup",
            "restore",
            "upgrade",
        ],
    ),
    (
        "Configure",
        &[
            "edit",
            "tag",
            "setup",
            "install",
            "uninstall",
            "login",
            "reload",
        ],
    ),
];

/// The shell's own commands, listed in the shell's `help` (`exit` has no docstring).
const SHELL_ROWS: &[&str] = &["instance", "repo", "executor", "debug"];

fn core_action(name: &str) -> Option<&'static ActionDef> {
    CORE_ACTIONS.iter().find(|a| a.name == name)
}

/// The core actions by group (hidden commands are left out).
pub fn core_action_groups() -> Vec<HelpGroup> {
    let listed = crate::builtin::listed_actions();
    let mut groups: Vec<HelpGroup> = Vec::new();
    let mut placed: Vec<&str> = Vec::new();
    for (title, names) in ACTION_GROUPS {
        let rows: Vec<HelpRow> = names
            .iter()
            .filter(|n| listed.iter().any(|l| l == *n))
            .filter_map(|n| core_action(n))
            .map(|a| HelpRow::new(a.name, summary(a.doc())))
            .collect();
        placed.extend(names.iter().copied());
        groups.push(HelpGroup {
            title: (*title).to_string(),
            rows,
        });
    }
    let mut other: Vec<HelpRow> = listed
        .iter()
        .filter(|n| !placed.contains(&n.as_str()) && !n.starts_with('_'))
        .filter_map(|n| core_action(n))
        .map(|a| HelpRow::new(a.name, summary(a.doc())))
        .collect();
    other.sort_by(|a, b| a.invocation.cmp(&b.invocation));
    groups.push(HelpGroup {
        title: "Other".into(),
        rows: other,
    });
    groups
}

/// The plugins' actions and their subcommands, one row each, sorted by their words (so
/// `weather wait` follows `weather`). An action's words are `@make_action(name='weather wait')`'s,
/// else its name split on `_` (`weather_wait` runs as `mrsm weather wait`).
pub fn plugin_action_rows(manifest: &Manifest) -> Vec<HelpRow> {
    let mut rows: Vec<HelpRow> = Vec::new();
    for plugin in manifest.plugins.values() {
        for action in &plugin.actions {
            let words = action.action_words().join(" ");
            if words.trim().is_empty() {
                continue;
            }
            rows.push(HelpRow::new(words.clone(), summary(action.doc.as_deref())));
            for sub in &action.subactions {
                rows.push(HelpRow::new(
                    format!("{words} {}", sub.name),
                    summary(sub.doc.as_deref()),
                ));
            }
        }
    }
    rows.sort_by(|a, b| a.invocation.cmp(&b.invocation));
    rows.dedup_by(|a, b| a.invocation == b.invocation);
    rows
}

/// One flag parsed outside the argument table (`parse::ENGINE_OPTIONS`), as `mrsm -h` lists it.
#[derive(Debug, Clone, Copy)]
pub struct ExtraOption {
    /// The `dest` in `parse::ENGINE_OPTIONS`.
    pub dest: &'static str,
    /// The flag and its metavariable.
    pub invocation: &'static str,
    /// The help text.
    pub help: &'static str,
    /// The options group it is listed in.
    pub group: &'static str,
}

/// The title of the group of the graph-run flags.
pub const PIPELINE_OPTIONS: &str = "Pipeline options";

/// Every flag of `parse::ENGINE_OPTIONS`, with its help (a test keeps the two lists equal).
pub const EXTRA_OPTIONS: &[ExtraOption] = &[
    ExtraOption {
        dest: "parents",
        invocation: "--parents [N]",
        help: "Also sync the selected pipes' ancestors first, in dependency order. N limits the \
               generations (`--parents 1`: the direct parents).",
        group: PIPELINE_OPTIONS,
    },
    ExtraOption {
        dest: "children",
        invocation: "--children [N]",
        help: "Also sync the selected pipes' descendants afterwards, in dependency order. N \
               limits the generations.",
        group: PIPELINE_OPTIONS,
    },
    ExtraOption {
        dest: "resume",
        invocation: "--resume RUN_ID",
        help: "Re-run only the steps of the run RUN_ID that failed or never ran (see \
               `mrsm show runs`).",
        group: PIPELINE_OPTIONS,
    },
    ExtraOption {
        dest: "run_id",
        invocation: "--run-id RUN_ID",
        help: "Record the run under this ID (letters, numbers, dots, colons, underscores or \
               hyphens).",
        group: PIPELINE_OPTIONS,
    },
    ExtraOption {
        dest: "chunk_pipelining",
        invocation: "--chunk-pipelining",
        help: "Let a backfilling child start each chunk as soon as the parent chunks it depends \
               on are done (`system:engine:chunk_pipelining`).",
        group: PIPELINE_OPTIONS,
    },
    ExtraOption {
        dest: "no_stagger",
        invocation: "--no-stagger",
        help: "Start jobs at once, without the startup gate (random delay, busy-host wait and \
               start slots, `jobs:startup:*`).",
        group: "Jobs options",
    },
    ExtraOption {
        dest: "startup_jitter",
        invocation: "--startup-jitter DURATION",
        help: "Bound a new job's random startup delay, e.g. `30s`.",
        group: "Jobs options",
    },
    ExtraOption {
        dest: "jitter",
        invocation: "--jitter DURATION",
        help: "With `-s`, shift every run of the schedule by a fixed per-job offset of up to \
               DURATION, e.g. `30s`.",
        group: "Jobs options",
    },
    ExtraOption {
        dest: "all",
        invocation: "--all",
        help: "With `show data`, print every row, column and cell.",
        group: "Miscellaneous options",
    },
    ExtraOption {
        dest: "last",
        invocation: "--last N",
        help: "With `show runs`, list only the N most recent runs.",
        group: "Miscellaneous options",
    },
];

/// One argument as the help formatter sees it, read back from the clap command.
struct HelpArg {
    option_strings: Vec<String>,
    dest: String,
    takes_values: bool,
    nargs: spec::Nargs,
    choices: Option<Vec<String>>,
    help: String,
    group: String,
}

fn help_args(cmd: &Command) -> Vec<HelpArg> {
    cmd.get_arguments()
        .filter_map(|arg| {
            let spec = spec::by_dest(arg.get_id().as_str())?;
            Some(HelpArg {
                option_strings: crate::command::option_strings(arg),
                dest: spec.dest.to_string(),
                takes_values: spec.nargs != spec::Nargs::Exactly(0),
                nargs: spec.nargs,
                choices: {
                    let pv = arg.get_possible_values();
                    if pv.is_empty() {
                        None
                    } else {
                        Some(pv.iter().map(|v| v.get_name().to_string()).collect())
                    }
                },
                help: arg.get_help().map(|h| h.to_string()).unwrap_or_default(),
                group: arg.get_help_heading().unwrap_or("options").to_string(),
            })
        })
        .collect()
}

/// `_format_args` for a metavariable and `nargs`.
fn format_nargs(metavar: &str, nargs: spec::Nargs) -> String {
    match nargs {
        spec::Nargs::Single => metavar.to_string(),
        spec::Nargs::OneOrMore => format!("{metavar} [{metavar} ...]"),
        spec::Nargs::ZeroOrMore => format!("[{metavar} ...]"),
        spec::Nargs::Remainder => "...".into(),
        spec::Nargs::Exactly(n) => vec![metavar.to_string(); n].join(" "),
    }
}

fn invocation(a: &HelpArg) -> String {
    if a.option_strings.is_empty() {
        return a.dest.clone();
    }
    let opts = a.option_strings.join(", ");
    if !a.takes_values {
        return opts;
    }
    let metavar = match &a.choices {
        Some(c) => format!("{{{}}}", c.join(",")),
        None => a.dest.to_uppercase(),
    };
    format!("{opts} {}", format_nargs(&metavar, a.nargs))
}

/// A plugin's `add_plugin_argument` flag as `argparse` would list it.
fn plugin_argument_row(argument: &ArgumentManifest) -> HelpRow {
    let opts = argument.option_strings.join(", ");
    let takes_values = !matches!(
        argument.action.as_str(),
        "_StoreTrueAction"
            | "_StoreFalseAction"
            | "_StoreConstAction"
            | "_CountAction"
            | "_HelpAction"
            | "_VersionAction"
            | "BooleanOptionalAction"
            | "_AppendConstAction"
    );
    let invocation = if !takes_values || opts.is_empty() {
        opts
    } else {
        let metavar = match &argument.metavar {
            serde_json::Value::String(m) => m.clone(),
            _ => match argument.choices.as_array() {
                Some(choices) if !choices.is_empty() => format!(
                    "{{{}}}",
                    choices
                        .iter()
                        .map(|c| c
                            .as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| c.to_string()))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                _ => argument.dest.clone().unwrap_or_default().to_uppercase(),
            },
        };
        let nargs = match &argument.nargs {
            serde_json::Value::String(n) if n == "+" => spec::Nargs::OneOrMore,
            serde_json::Value::String(n) if n == "*" => spec::Nargs::ZeroOrMore,
            serde_json::Value::String(n) if n == "..." || n == "A..." => spec::Nargs::Remainder,
            serde_json::Value::String(n) if n == "?" => {
                return HelpRow::new(
                    format!("{opts} [{metavar}]"),
                    argument.help.clone().unwrap_or_default(),
                );
            }
            serde_json::Value::Number(n) => {
                spec::Nargs::Exactly(n.as_u64().unwrap_or(1).try_into().unwrap_or(1))
            }
            _ => spec::Nargs::Single,
        };
        format!("{opts} {}", format_nargs(&metavar, nargs))
    };
    let help = argument
        .help
        .clone()
        .filter(|h| h != "==SUPPRESS==")
        .unwrap_or_default();
    HelpRow::new(invocation, help)
}

/// The options groups: the argument table's (in `argparse` order), with the flags parsed ahead
/// of it ([`EXTRA_OPTIONS`]; "Pipeline options" follows "Sync options"), then each plugin's
/// `add_plugin_argument` flags (`Plugin '<name>' options`).
pub fn option_groups(manifest: Option<&Manifest>) -> Vec<HelpGroup> {
    let cmd = crate::command::command();
    let args = help_args(&cmd);
    let extra_rows = |title: &str| -> Vec<HelpRow> {
        EXTRA_OPTIONS
            .iter()
            .filter(|o| o.group == title)
            .map(|o| HelpRow::new(o.invocation, o.help))
            .collect()
    };
    let mut groups: Vec<HelpGroup> = Vec::new();
    for group in Group::ALL {
        let title = group.title();
        let mut rows: Vec<HelpRow> = args
            .iter()
            .filter(|a| a.group == title)
            .map(|a| HelpRow::new(invocation(a), a.help.clone()))
            .collect();
        rows.extend(extra_rows(title));
        groups.push(HelpGroup {
            title: title.to_string(),
            rows,
        });
        if group == Group::Sync {
            groups.push(HelpGroup {
                title: PIPELINE_OPTIONS.to_string(),
                rows: extra_rows(PIPELINE_OPTIONS),
            });
        }
    }
    if let Some(manifest) = manifest {
        for plugin in manifest.plugins.values() {
            let rows: Vec<HelpRow> = plugin
                .arguments
                .iter()
                .filter(|a| !a.option_strings.is_empty())
                .filter(|a| a.help.as_deref() != Some("==SUPPRESS=="))
                .map(plugin_argument_row)
                .collect();
            groups.push(HelpGroup {
                title: format!("Plugin '{}' options", plugin.name),
                rows,
            });
        }
    }
    groups
}

/// The global help: the usage line, the description, the action groups (with the plugins'
/// actions and, in the shell, the shell's commands), a pointer to per-action help, then every
/// option.
pub fn format_global_help(columns: usize, manifest: Option<&Manifest>, shell: bool) -> String {
    let width = columns.saturating_sub(2).max(1);
    let mut out = String::new();
    out.push_str(&format!("usage: {}\n\n", spec::USAGE));
    out.push_str(&wrap(&normalize_ws(spec::DESCRIPTION), width.max(11)).join("\n"));
    out.push_str("\n\n");

    let mut action_groups = core_action_groups();
    if shell {
        let mut rows: Vec<HelpRow> = SHELL_ROWS
            .iter()
            .map(|name| HelpRow::new(*name, summary(Some(crate::shell::shell_command_doc(name)))))
            .collect();
        rows.push(HelpRow::new("exit", "Leave the shell (or press Ctrl+D)."));
        action_groups.push(HelpGroup {
            title: "Shell".into(),
            rows,
        });
    }
    if let Some(manifest) = manifest {
        action_groups.push(HelpGroup {
            title: "Plugins".into(),
            rows: plugin_action_rows(manifest),
        });
    }
    out.push_str(&format_groups(&action_groups, width));
    let pointer = if shell {
        "Type `help <action>` for an action's details, e.g. `help sync pipes`."
    } else {
        "Run `mrsm <action> -h` for an action's details, e.g. `mrsm sync pipes -h`."
    };
    out.push_str(&format!("\n{}\n", wrap(pointer, width.max(11)).join("\n")));
    out.push_str(&format_groups(&option_groups(manifest), width));
    collapse_blank_lines(&out)
}

/// [`format_global_help`] without plugins, outside the shell.
pub fn format_help(columns: usize) -> String {
    format_global_help(columns, None, false)
}

/// A main action's docstring with its `Options:` line listing the subactions it has now
/// (v4's `choices_docstring`, recomputed so that additive subactions are listed too).
fn with_subaction_choices(doc: &str, action: &ActionDef) -> String {
    let marker = format!("\n    Options:\n        `{} [", action.name);
    let base = match doc.find(&marker) {
        Some(i) => doc[..i].trim_end().to_string(),
        None => doc.trim_end().to_string(),
    };
    if action.subactions.is_empty() {
        return format!("{base}\n");
    }
    let mut names: Vec<&str> = action.subactions.iter().map(|s| s.name).collect();
    names.sort_unstable();
    names.dedup();
    format!(
        "{base}\n\n    Options:\n        `{} [{}]`",
        action.name,
        names.join(", ")
    )
}

/// The help text `mrsm <action> [<subaction>] --help` prints for a core action: the dedented
/// docstring of the subaction function if the second word names one (a trailing `s` is tried),
/// else of the main action (with its current subactions), followed by `print`'s newline. `None`
/// when the first word is not a core action.
pub fn format_action_help(words: &[String]) -> Option<String> {
    let main = CORE_ACTIONS
        .iter()
        .find(|a| Some(a.name) == words.first().map(String::as_str))?;
    if let Some(word) = words.get(1) {
        let sub = main.subactions.iter().find(|s| s.name == word).or_else(|| {
            main.subactions
                .iter()
                .find(|s| s.name == format!("{word}s"))
        });
        if let Some(sub) = sub {
            return Some(format!("{}\n", dedent(sub.doc().unwrap_or_default())));
        }
    }
    let doc = match main.doc() {
        Some(doc) => with_subaction_choices(doc, main),
        None => format!("No help available for '{}'.", main.name),
    };
    Some(format!("{}\n", dedent(&doc)))
}

/// The subcommands of the plugin action (or family of actions) run by `prefix`: the action's
/// own subactions and every action whose words extend `prefix` (`weather wait` under `weather`).
fn plugin_subcommand_rows(prefix: &[String], manifest: &Manifest) -> Vec<HelpRow> {
    let mut rows: Vec<HelpRow> = Vec::new();
    for action in manifest.plugins.values().flat_map(|p| &p.actions) {
        let words = action.action_words();
        if words == prefix {
            rows.extend(
                action
                    .subactions
                    .iter()
                    .map(|s| HelpRow::new(s.name.clone(), summary(s.doc.as_deref()))),
            );
        } else if words.len() > prefix.len() && words.starts_with(prefix) {
            rows.push(HelpRow::new(
                words[prefix.len()..].join(" "),
                summary(action.doc.as_deref()),
            ));
        }
    }
    rows.retain(|r| !r.invocation.is_empty());
    rows.sort_by(|a, b| a.invocation.cmp(&b.invocation));
    rows.dedup_by(|a, b| a.invocation == b.invocation);
    rows
}

fn subcommands_block(title: &str, rows: &[HelpRow], columns: usize) -> String {
    let width = columns.saturating_sub(2).max(1);
    format_groups(
        &[HelpGroup {
            title: title.to_string(),
            rows: rows.to_vec(),
        }],
        width,
    )
}

/// What `mrsm <words> -h` prints, from the core actions and the plugins' actions: a plugin
/// action's docstring and its subcommands, the subcommands of a plugin family (`mrsm weather -h`
/// lists `weather_wait` as `wait`), or a core action's help with the plugins' subactions of it.
/// `None` when nothing has that name.
pub fn action_help(
    words: &[String],
    manifest: Option<&Manifest>,
    columns: usize,
) -> Option<String> {
    let words: Vec<String> = words.iter().filter(|w| !w.is_empty()).cloned().collect();
    let first = words.first()?;
    if let Some(manifest) = manifest
        && let Some(name) = crate::python::action_name(&words, manifest)
        && let Some(action) = manifest
            .plugins
            .values()
            .flat_map(|p| &p.actions)
            .find(|a| a.name == name)
    {
        let mut text = format!("{}\n", action.doc.as_deref().unwrap_or_default());
        let action_words = action.action_words();
        let rows = plugin_subcommand_rows(&action_words, manifest);
        if !rows.is_empty() {
            text.push_str(&subcommands_block(
                &format!("Subcommands of `mrsm {}`", action_words.join(" ")),
                &rows,
                columns,
            ));
        }
        return Some(text);
    }
    if let Some(text) = format_action_help(&words) {
        // A core action without a subaction word: add the plugins' subactions of it.
        let is_main_help = words.len() == 1
            || !core_action(first).is_some_and(|main| {
                main.subactions
                    .iter()
                    .any(|s| s.name == words[1] || s.name == format!("{}s", words[1]))
            });
        if let Some(manifest) = manifest
            && is_main_help
        {
            let rows = plugin_subcommand_rows(std::slice::from_ref(first), manifest);
            if !rows.is_empty() {
                return Some(format!(
                    "{text}{}",
                    subcommands_block(
                        &format!("Plugin subcommands of `mrsm {first}`"),
                        &rows,
                        columns
                    )
                ));
            }
        }
        return Some(text);
    }
    let manifest = manifest?;
    let rows = plugin_subcommand_rows(&words, manifest);
    if rows.is_empty() {
        return None;
    }
    Some(subcommands_block(
        &format!("Subcommands of `mrsm {}`", words.join(" ")),
        &rows,
        columns,
    ))
}

/// Print the help for `words` as `mrsm <words> -h`, `mrsm help <words>` and `help <words>` do:
/// the global help (with the shell's commands inside the shell) when `words` is empty, else the
/// action's help; an unknown action prints `show actions`. Plugins come from the cached manifest
/// when the caller set the session to it (`--help` starts no Python worker).
pub fn print_help(
    words: &[String],
    args: &crate::value::ArgsMap,
    rt: &mut crate::dispatch::Runtime<'_>,
) {
    let manifest = crate::python::manifest(rt).ok();
    let columns = rt.columns;
    if words.is_empty() {
        let text = format_global_help(columns, manifest.as_deref(), rt.in_shell);
        // `print(parser.format_help())`.
        rt.print(&format!("{text}\n"));
    } else {
        match action_help(words, manifest.as_deref(), columns) {
            Some(text) => rt.print(&text),
            None => {
                // `actions['show'](['actions'], **_args)`.
                let _ = crate::builtin::show_actions(args, rt);
            }
        }
    }
    if let Some(hint) = stale_plugins_hint(rt) {
        let text = rt.with_formatter(|f| f.info(hint, true));
        rt.print(&text);
    }
}

/// What help says when it read the plugins' actions from the cache (`mrsm -h` starts no Python)
/// and the plugins changed since they were cached.
pub const STALE_PLUGINS_HINT: &str = "The plugins changed since their actions were last loaded, \
     so this help may miss new actions and subcommands: run `mrsm reload`, then ask again.";

/// [`STALE_PLUGINS_HINT`] when help reads the cached plugin manifest and it no longer matches
/// the plugin trees ([`plugins_cache_is_current`]); `None` otherwise (in the shell, plugins are
/// validated before each command).
pub fn stale_plugins_hint(rt: &mut crate::dispatch::Runtime<'_>) -> Option<&'static str> {
    if !crate::native::session(rt).cached_manifest_only() {
        return None;
    }
    let paths = rt.config().paths();
    (!plugins_cache_is_current(&paths.root_dir, &paths.plugins_dirs)).then_some(STALE_PLUGINS_HINT)
}

/// Whether the plugin manifests cached under `root` (Python and native) describe the plugins in
/// `plugins_dirs` now: one fingerprint walk of each tree, starting nothing (no Python worker, no
/// build, no library loaded). No plugins and no cache count as current.
pub fn plugins_cache_is_current(
    root: &std::path::Path,
    plugins_dirs: &[std::path::PathBuf],
) -> bool {
    use meerschaum_worker::manifest::{discover_plugins, manifest_path, stale_plugins};
    let python = match Manifest::read(&manifest_path(root)).ok().flatten() {
        Some(cached) => !stale_plugins(&cached, plugins_dirs).0,
        None => discover_plugins(plugins_dirs).is_empty(),
    };
    python
        && meerschaum_plugin::discovery::manifest_cache_is_current(
            plugins_dirs,
            &crate::native_plugins::cache_path(root),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_worker::manifest::{ActionManifest, PluginManifest, SubactionManifest};

    #[test]
    fn chunks_split_hyphenated_words() {
        assert_eq!(
            split_chunks("are sub-arguments"),
            vec!["are", " ", "sub-", "arguments"]
        );
        assert_eq!(split_chunks("foo--bar"), vec!["foo", "--", "bar"]);
        assert_eq!(wrap("aaa bbb ccc", 7), vec!["aaa bbb", "ccc"]);
    }

    fn plugin_manifest() -> Manifest {
        let action = |name: &str, doc: &str| ActionManifest {
            name: name.into(),
            doc: Some(doc.into()),
            ..ActionManifest::default()
        };
        let mut manifest = Manifest::default();
        manifest.plugins.insert(
            "weather".into(),
            PluginManifest {
                name: "weather".into(),
                actions: vec![
                    action(
                        "weather_wait",
                        "Wait until the service answers.\n\nMore text.",
                    ),
                    action("weather_status", "Show the service's status."),
                    ActionManifest {
                        subactions: vec![SubactionManifest {
                            name: "world".into(),
                            doc: Some("Say hello to the world.".into()),
                        }],
                        ..action("hello", "Say hello.")
                    },
                    action("show_widgets", "Show the widgets."),
                    // `@make_action(name='weather wait_for')`: the words keep their underscore.
                    ActionManifest {
                        words: vec!["weather".into(), "wait_for".into()],
                        ..action("weather_wait_for", "Wait for a condition.")
                    },
                ],
                arguments: vec![ArgumentManifest {
                    option_strings: vec!["--weather-timeout".into()],
                    dest: Some("weather_timeout".into()),
                    action: "_StoreAction".into(),
                    help: Some("Seconds to wait for the service.".into()),
                    ..ArgumentManifest::default()
                }],
                ..PluginManifest::default()
            },
        );
        manifest
    }

    #[test]
    fn global_help_groups_the_actions_before_the_options() {
        let text = format_help(100);
        assert!(
            text.starts_with("usage: mrsm [actions] {options}\n"),
            "{text}"
        );
        let create = text.find("\nCreate:\n").expect("Create group");
        let sync = text.find("\nSync:\n").expect("Sync group");
        let run = text.find("\nRun:\n").expect("Run group");
        let options = text.find("\nActions options:\n").expect("options");
        assert!(create < sync && sync < run && run < options, "{text}");
        assert!(text.contains("  bootstrap "), "{text}");
        assert!(!text.contains("\nShell:\n"), "{text}");
        assert!(!text.contains("v5 additions"), "{text}");
        assert!(
            !text.contains("\nOther:\n"),
            "every core action has a group: {text}"
        );
        // Every listed core action has a row with its docstring's first line.
        for name in crate::builtin::listed_actions() {
            let action = core_action(&name).unwrap();
            let line = text
                .lines()
                .find(|l| l.trim_start().starts_with(&format!("{name} ")) || l.trim() == name)
                .unwrap_or_else(|| panic!("{name} is not listed:\n{text}"));
            let description = summary(action.doc());
            let first_words: Vec<&str> = description.split_whitespace().take(2).collect();
            assert!(
                line.contains(first_words[0]),
                "{name}: {line:?} lacks its summary {first_words:?}"
            );
        }
    }

    #[test]
    fn pipeline_options_follow_the_sync_options() {
        let text = format_help(100);
        let sync = text.find("\nSync options:\n").unwrap();
        let pipeline = text.find("\nPipeline options:\n").unwrap();
        let api = text.find("\nAPI options:\n").unwrap();
        assert!(sync < pipeline && pipeline < api, "{text}");
        for flag in [
            "--parents [N]",
            "--children [N]",
            "--resume RUN_ID",
            "--run-id RUN_ID",
            "--chunk-pipelining",
        ] {
            let at = text.find(flag).unwrap_or_else(|| panic!("{flag}: {text}"));
            assert!(at > pipeline && at < api, "{flag} is in its group");
        }
        for flag in [
            "--no-stagger",
            "--startup-jitter DURATION",
            "--all",
            "--last N",
        ] {
            assert!(text.contains(flag), "{flag}");
        }
        for flag in ["--allow-protected", "--public", "--replace"] {
            assert!(text.contains(flag), "{flag}");
        }
    }

    #[test]
    fn every_engine_option_has_help() {
        for option in crate::parse::ENGINE_OPTIONS {
            let extra = EXTRA_OPTIONS
                .iter()
                .find(|e| e.dest == option.dest)
                .unwrap_or_else(|| panic!("{} has no help in EXTRA_OPTIONS", option.flag));
            assert!(extra.invocation.starts_with(option.flag));
        }
        assert_eq!(EXTRA_OPTIONS.len(), crate::parse::ENGINE_OPTIONS.len());
    }

    #[test]
    fn stale_option_help_is_gone() {
        let text = format_help(200);
        for stale in [
            "red-engine",
            "Gunicorn process manager",
            "pandasgui",
            "Meerschaum CLI daemon",
            "open browser windows",
            "\"connectors\" : \"main\"",
            "defaults answers",
            "``",
        ] {
            assert!(!text.contains(stale), "{stale:?} in:\n{text}");
        }
        assert!(
            spec::by_dest("chunk_hours")
                .unwrap()
                .help
                .contains("in hours")
        );
        // The `--params` example is valid JSON.
        let help = spec::by_dest("params").unwrap().help;
        let example = help.split('\'').nth(1).expect("a quoted JSON example");
        assert!(
            serde_json::from_str::<serde_json::Value>(example).is_ok(),
            "{example}"
        );
    }

    #[test]
    fn plugin_actions_and_subcommands_are_listed() {
        let manifest = plugin_manifest();
        let text = format_global_help(100, Some(&manifest), false);
        let plugins = text.find("\nPlugins:\n").expect("plugins group");
        let options = text.find("\nActions options:\n").unwrap();
        assert!(plugins < options);
        let section = &text[plugins..options];
        for row in [
            "weather status",
            "weather wait",
            "weather wait_for",
            "hello",
            "hello world",
            "show widgets",
        ] {
            assert!(section.contains(&format!("  {row} ")), "{row}: {section}");
        }
        assert!(section.contains("Wait until the service answers."));
        assert!(!section.contains("More text."));
        assert!(
            text.contains("\nPlugin 'weather' options:\n  --weather-timeout WEATHER_TIMEOUT"),
            "{text}"
        );
    }

    #[test]
    fn plugin_help_lists_subcommands() {
        let manifest = plugin_manifest();
        let words = |w: &[&str]| w.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let family = action_help(&words(&["weather"]), Some(&manifest), 80).unwrap();
        assert!(
            family.contains("Subcommands of `mrsm weather`:"),
            "{family}"
        );
        assert!(
            family.contains("  status  ") && family.contains("  wait  "),
            "{family}"
        );
        assert!(family.contains("  wait_for  "), "{family}");
        let wait = action_help(&words(&["weather", "wait"]), Some(&manifest), 80).unwrap();
        assert!(
            wait.starts_with("Wait until the service answers."),
            "{wait}"
        );
        let hello = action_help(&words(&["hello"]), Some(&manifest), 80).unwrap();
        assert!(hello.starts_with("Say hello.\n"), "{hello}");
        assert!(
            hello.contains("Subcommands of `mrsm hello`:\n  world"),
            "{hello}"
        );
        let show = action_help(&words(&["show"]), Some(&manifest), 80).unwrap();
        assert!(
            show.contains("Plugin subcommands of `mrsm show`:\n  widgets"),
            "{show}"
        );
        let pipes = action_help(&words(&["show", "pipes"]), Some(&manifest), 80).unwrap();
        assert!(!pipes.contains("Plugin subcommands"), "{pipes}");
        assert!(action_help(&words(&["nothing"]), Some(&manifest), 80).is_none());
    }

    #[test]
    fn main_action_help_lists_every_subaction() {
        let show = format_action_help(&["show".to_string()]).unwrap();
        assert!(
            show.contains("`show [actions, arguments, columns, completions, config"),
            "{show}"
        );
        assert!(show.contains("graph") && show.contains("runs") && show.contains("drivers"));
        let sync = format_action_help(&["sync".to_string()]).unwrap();
        assert!(
            sync.contains("`sync [pipeline, pipelines, pipes]`"),
            "{sync}"
        );
        assert!(!sync.contains("``"), "{sync}");
        for action in CORE_ACTIONS {
            let text = format_action_help(&[action.name.to_string()]).unwrap();
            assert!(!text.contains("v5 additions"), "{}", action.name);
            assert!(!text.contains("(v5)"), "{}: {text}", action.name);
            for sub in action.subactions {
                let text =
                    format_action_help(&[action.name.to_string(), sub.name.to_string()]).unwrap();
                assert!(
                    !text.contains("(v5)"),
                    "{} {}: {text}",
                    action.name,
                    sub.name
                );
            }
        }
    }

    #[test]
    fn sync_pipes_help_has_the_graph_flags_in_its_text() {
        let text = format_action_help(&["sync".to_string(), "pipes".to_string()]).unwrap();
        assert!(
            text.starts_with("\nFetch and sync new data for pipes."),
            "{text}"
        );
        for flag in [
            "--parents",
            "--children",
            "--resume",
            "--run-id",
            "--chunk-pipelining",
            "--replace",
        ] {
            assert!(text.contains(flag), "{flag}: {text}");
        }
    }
}
