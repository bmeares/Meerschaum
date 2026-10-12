//! `--help` rendering: a port of CPython 3.14 `argparse.HelpFormatter` (and the parts of
//! `textwrap` it uses) over the clap [`Command`](clap::Command) built from the argument table,
//! so `mrsm --help` matches v4's `parser.format_help()` byte for byte at the same width.

use clap::Command;

use crate::spec::{self, Group};

/// The terminal width `argparse` would use: `$COLUMNS`, else 80 (`shutil.get_terminal_size`'s
/// fallback; the terminal itself is not queried).
pub fn terminal_columns() -> usize {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|c| c.trim().parse::<usize>().ok())
        .filter(|c| *c > 0)
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
    let args = match a.nargs {
        spec::Nargs::Single => metavar,
        spec::Nargs::OneOrMore => format!("{metavar} [{metavar} ...]"),
        spec::Nargs::ZeroOrMore => format!("[{metavar} ...]"),
        spec::Nargs::Remainder => "...".into(),
        spec::Nargs::Exactly(n) => vec![metavar; n].join(" "),
    };
    format!("{opts} {args}")
}

/// `parser.format_help()` at a given terminal width (columns).
pub fn format_help(columns: usize) -> String {
    let cmd = crate::command::command();
    let width = columns.saturating_sub(2).max(1);
    let max_help_position = 24usize.min(width.saturating_sub(20).max(4));
    let indent = 2usize;
    let args = help_args(&cmd);
    let action_max_length = args
        .iter()
        .map(|a| invocation(a).chars().count() + indent)
        .max()
        .unwrap_or(0);

    let mut out = String::new();
    out.push_str(&format!("usage: {}\n\n", spec::USAGE));
    let desc_width = width.max(11);
    out.push_str(&wrap(&normalize_ws(spec::DESCRIPTION), desc_width).join("\n"));
    out.push_str("\n\n");

    for group in Group::ALL {
        let mut items = String::new();
        for a in args.iter().filter(|a| a.group == group.title()) {
            let help_position = (action_max_length + 2).min(max_help_position);
            let help_width = width.saturating_sub(help_position).max(11);
            let action_width = help_position - indent - 2;
            let header = invocation(a);
            if a.help.is_empty() {
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
            let lines = wrap(&normalize_ws(&a.help), help_width);
            if let Some((first, rest)) = lines.split_first() {
                items.push_str(&format!("{}{first}\n", " ".repeat(indent_first)));
                for line in rest {
                    items.push_str(&format!("{}{line}\n", " ".repeat(help_position)));
                }
            }
        }
        if items.is_empty() {
            continue;
        }
        out.push_str(&format!("\n{}:\n{items}\n", group.title()));
    }

    // `_long_break_matcher.sub('\n\n', help)`, then `strip('\n') + '\n'`.
    let mut collapsed = String::with_capacity(out.len());
    let mut newlines = 0usize;
    for ch in out.chars() {
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

/// Help for `mrsm <action...> --help`. v4 prints the action's docstring; the Rust port prints
/// the action's usage line and the shared options until the action itself is ported.
pub fn format_action_help(words: &[String], columns: usize) -> String {
    let global = format_help(columns);
    let body = global
        .split_once("\n\n")
        .map(|(_, rest)| rest)
        .unwrap_or(&global);
    format!(
        "usage: mrsm {} {{options}}\n\nThe `{}` action is not implemented in Rust yet; its options are shared:\n\n{}",
        words.join(" "),
        words.join(" "),
        body
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_split_hyphenated_words() {
        assert_eq!(
            split_chunks("are sub-arguments"),
            vec!["are", " ", "sub-", "arguments"]
        );
        assert_eq!(split_chunks("foo--bar"), vec!["foo", "--", "bar"]);
        assert_eq!(wrap("aaa bbb ccc", 7), vec!["aaa bbb", "ccc"]);
    }
}
