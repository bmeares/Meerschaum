//! Ports of `parse_arguments`, `parse_line` and `parse_synonyms`
//! (`meerschaum/_internal/arguments/_parse_arguments.py`).

use std::sync::LazyLock;

use crate::argparse::Parser;
use crate::shlex;
use crate::spec::{ARGUMENTS, keys};
use crate::types::DateContext;
use crate::value::{ArgsMap, PyException, PyTimeDelta, PyValue};

/// The process-wide parser over [`ARGUMENTS`].
pub static PARSER: LazyLock<Parser> = LazyLock::new(|| Parser::new(ARGUMENTS));

/// `_rewrite_python_code_flag`: `python -c CODE` → `python CODE --noask`.
pub fn rewrite_python_code_flag(sysargs: &[String]) -> Vec<String> {
    if sysargs.first().map(String::as_str) != Some("python") {
        return sysargs.to_vec();
    }
    let Some(ix) = sysargs.iter().position(|a| a == "-c") else {
        return sysargs.to_vec();
    };
    if ix + 1 >= sysargs.len() {
        return sysargs.to_vec();
    }
    let code = sysargs[ix + 1].clone();
    let mut rest: Vec<String> = sysargs[1..ix].to_vec();
    rest.extend_from_slice(&sysargs[ix + 2..]);
    let has_noask = rest.iter().any(|a| a == "--noask");
    let mut out = vec!["python".to_string(), code];
    out.extend(rest);
    if !has_noask {
        out.push("--noask".into());
    }
    out
}

/// Whether `word` names a Python file for `mrsm python <word>`: a `.py` / `.pyw` name, or an
/// existing file (relative to the working directory).
pub fn is_python_script(word: &str) -> bool {
    if word.is_empty() || word.starts_with('-') || word.contains('\n') {
        return false;
    }
    let lower = word.to_lowercase();
    ((lower.ends_with(".py") || lower.ends_with(".pyw")) && !word.contains(char::is_whitespace))
        || std::path::Path::new(word).is_file()
}

/// The positional words of `sysargs` with their indices, in order, as `parse_known_args` sees
/// them: option words, the values options take and `[decorated]` sub-arguments are skipped, and
/// the walk stops at `--` and `-A` (everything after them is not an action word).
fn positional_words(sysargs: &[String]) -> impl Iterator<Item = (usize, &str)> {
    use crate::argparse::OptionArity;
    let mut i = 0;
    std::iter::from_fn(move || {
        while i < sysargs.len() {
            let word = sysargs[i].as_str();
            if word == "--" {
                i = sysargs.len();
                return None;
            }
            if word.starts_with(keys::SUB_BEGIN)
                && let Some(close) = sysargs[i..].iter().position(|w| w.ends_with(keys::SUB_END))
            {
                // A `[decorated]` sub-argument (possibly several words).
                i += close + 1;
                continue;
            }
            match PARSER.option_arity(word) {
                None => {
                    i += 1;
                    return Some((i - 1, word));
                }
                Some(OptionArity::Rest) => {
                    i = sysargs.len();
                    return None;
                }
                Some(OptionArity::Values(n)) => i += 1 + n,
                Some(OptionArity::Greedy) => {
                    i += 1;
                    while i < sysargs.len() && PARSER.option_arity(&sysargs[i]).is_none() {
                        i += 1;
                    }
                }
            }
        }
        None
    })
}

/// `[options] python [options] <file> [args ...]`: the indices of `python` and of `<file>` in
/// `sysargs` (the first two action words). Meerschaum reads the words up to the file (`mrsm
/// python --venv x script.py`); every word after it belongs to the script, unchanged and in
/// order, as with `python script.py ...`: none is read as a Meerschaum flag (`--dry-run`, `-y`,
/// `-d`, `--loop`, `-h`), sub-argument or chain flag.
pub fn python_script_index(sysargs: &[String]) -> Option<(usize, usize)> {
    let mut positionals = positional_words(sysargs);
    let (python, word) = positionals.next()?;
    if word != "python" {
        return None;
    }
    let (script, word) = positionals.next()?;
    is_python_script(word).then_some((python, script))
}

/// Split `python [options] <file> [args ...]` into the words Meerschaum parses and the script's
/// words ([`python_script_index`]); any other line is all Meerschaum's. The file follows
/// `python` in the words to parse: `parse_known_args` keeps only the action words that come
/// before the first option, so `python --venv x script.py` would lose the file.
pub fn split_python_script_args(sysargs: &[String]) -> (Vec<String>, Vec<String>) {
    match python_script_index(sysargs) {
        Some((python, script)) => {
            let mut head = sysargs[..=python].to_vec();
            head.push(sysargs[script].clone());
            head.extend(sysargs[python + 1..script].iter().cloned());
            (head, sysargs[script + 1..].to_vec())
        }
        None => (sysargs.to_vec(), Vec::new()),
    }
}

/// The words of `sysargs` Meerschaum reads its own flags from: all of them, except that the
/// words after the file of `python <file> ...` are the script's ([`python_script_index`]).
pub fn meerschaum_words(sysargs: &[String]) -> &[String] {
    match python_script_index(sysargs) {
        Some((_, script)) => &sysargs[..=script],
        None => sysargs,
    }
}

/// Pull `[decorated]` sub-arguments out of `sysargs`: returns (sub_arguments, filtered_sysargs).
///
/// PARITY: any word wrapped in brackets is a sub-argument, so `-l [None]` loses its value and
/// `[None]` becomes the sub-argument `None`.
fn extract_decorated_sub_args(sysargs: &[String]) -> (Vec<String>, Vec<String>) {
    let (begin, end) = (keys::SUB_BEGIN, keys::SUB_END);
    let mut sub_arguments = Vec::new();
    let mut sub_arg_indices = Vec::new();
    let mut found_begin = false;
    let mut found_end = false;
    for (i, word) in sysargs.iter().enumerate() {
        let mut is_sub_arg = false;
        if !found_begin {
            found_begin = word.starts_with(begin);
            found_end = word.ends_with(end);
        }
        if found_begin {
            // Check whether the sub-arg is ever closed.
            for a in &sysargs[i..] {
                if a.ends_with(end) {
                    is_sub_arg = true;
                    found_begin = false;
                }
            }
        } else if found_end {
            for a in &sysargs[..i] {
                if a.starts_with(begin) {
                    is_sub_arg = true;
                    found_begin = false;
                }
            }
        }
        if is_sub_arg {
            let mut sa = word.as_str();
            if let Some(stripped) = sa.strip_prefix(begin) {
                sa = stripped;
            }
            if let Some(stripped) = sa.strip_suffix(end) {
                sa = stripped;
            }
            sub_arguments.push(sa.to_string());
            sub_arg_indices.push(i);
        }
    }
    let filtered = sysargs
        .iter()
        .enumerate()
        .filter(|(i, _)| !sub_arg_indices.contains(i))
        .map(|(_, w)| w.clone())
        .collect();
    (sub_arguments, filtered)
}

/// Compose options that are given back to non-`compose` actions as sub-arguments.
const COMPOSE_OPTIONS: &[(&str, &[&str])] = &[
    ("file", &["--file", "--compose-file"]),
    ("env_file", &["--env-file"]),
    ("dry", &["--dry"]),
    ("drop", &["--drop", "-v", "--volumes"]),
    ("presync", &["--presync"]),
    ("no_jobs", &["--no-jobs"]),
    ("isolated", &["--isolated"]),
];

/// `parse_arguments(sysargs)`: parse sysargs into the standard arguments dictionary.
///
/// Argument errors do not fail: they are stored under `_argparse_exception` with the leading
/// action words, as in v4. The `Err` case is an exception escaping `parse_arguments` itself
/// (e.g. `OverflowError` from `timedelta(days=--chunk-days)`).
pub fn parse_arguments(sysargs: &[String], ctx: &DateContext) -> Result<ArgsMap, PyException> {
    let full_sysargs = rewrite_python_code_flag(sysargs);
    // `python file.py [args ...]`: the words after the file are the script's, untouched; they
    // follow the file in `action` (and the sysargs) and are never parsed as flags.
    let (sysargs, script_args) = split_python_script_args(&full_sysargs);
    let (sub_arguments, filtered_sysargs) = extract_decorated_sub_args(&sysargs);
    let (filtered_sysargs, engine_args) = engine_arguments(&filtered_sysargs)?;

    let mut args: ArgsMap = match PARSER.parse_known_args(&filtered_sysargs, ctx) {
        Ok((ns, _unknown)) => ns.into_iter().collect(),
        Err(err) => {
            let mut action = Vec::new();
            for a in &filtered_sysargs {
                if a.starts_with('-') {
                    break;
                }
                action.push(a.clone());
            }
            action.extend(script_args.iter().cloned());
            let mut m = ArgsMap::new();
            m.insert("action".into(), PyValue::str_list(&action));
            m.insert("sysargs".into(), PyValue::str_list(&full_sysargs));
            m.insert("text".into(), PyValue::str(shlex::join(&full_sysargs)));
            m.insert(
                keys::FAILURE_KEY.into(),
                PyValue::Exception(err.into_exception()),
            );
            m
        }
    };
    if !script_args.is_empty()
        && !args.contains_key(keys::FAILURE_KEY)
        && let Some(PyValue::List(action)) = args.get_mut("action")
    {
        action.extend(script_args.iter().map(PyValue::str));
    }

    args.extend(engine_args);

    // PARITY: native Compose keeps its flags registered for every action; v4 gives them back to
    // other actions as sub-arguments instead of stealing `-v` / `--file` (`_parse_arguments.py`).
    let first_action = args
        .get("action")
        .and_then(PyValue::as_list)
        .filter(|l| !l.is_empty())
        .map(|l| l[0].clone())
        .unwrap_or(PyValue::None);
    if first_action.as_str() != Some("compose") {
        let mut restored: Vec<String> = Vec::new();
        for (dest, option_strings) in COMPOSE_OPTIONS {
            if !args.get(*dest).is_some_and(PyValue::is_truthy) {
                continue;
            }
            let takes_value = *dest == "file" || *dest == "env_file";
            for (i, arg) in filtered_sysargs.iter().enumerate() {
                if option_strings.contains(&arg.as_str()) {
                    restored.push(arg.clone());
                    if takes_value && i + 1 < filtered_sysargs.len() {
                        restored.push(filtered_sysargs[i + 1].clone());
                    }
                } else if takes_value
                    && option_strings
                        .iter()
                        .any(|o| arg.starts_with(&format!("{o}=")))
                {
                    restored.push(arg.clone());
                }
            }
            args.shift_remove(*dest);
        }
        if !restored.is_empty() {
            let mut sub = match args.get("sub_args") {
                Some(PyValue::List(l)) => l.clone(),
                _ => Vec::new(),
            };
            sub.extend(restored.into_iter().map(PyValue::Str));
            args.insert("sub_args".into(), PyValue::List(sub));
        }
    }

    // Drop `False` flags.
    args.retain(|_, v| !matches!(v, PyValue::Bool(false)));

    args.insert("sysargs".into(), PyValue::str_list(&full_sysargs));
    let mut filtered_sysargs = filtered_sysargs;
    filtered_sysargs.extend(script_args);
    args.insert(
        "filtered_sysargs".into(),
        PyValue::str_list(&filtered_sysargs),
    );

    // Merge `-A` sub-arguments with the decorated ones.
    if !args.contains_key("sub_args") {
        args.insert("sub_args".into(), PyValue::List(vec![]));
    }
    if matches!(args.get("sub_args"), Some(PyValue::None)) {
        args.insert("sub_args".into(), PyValue::List(vec![]));
    }
    let mut all_sub: Vec<String> = match args.get("sub_args") {
        Some(PyValue::List(l)) => l.iter().map(PyValue::py_str).collect(),
        _ => Vec::new(),
    };
    all_sub.extend(sub_arguments);
    let mut parsed_sub: Vec<String> = Vec::new();
    for sub_arg in all_sub {
        if sub_arg.contains(' ') {
            // PARITY: `str.split(' ')`, so repeated spaces yield empty strings.
            parsed_sub.extend(sub_arg.split(' ').map(str::to_string));
        } else {
            parsed_sub.push(sub_arg);
        }
    }
    if parsed_sub.len() == 1 && parsed_sub[0].is_empty() {
        parsed_sub.clear();
    }
    args.insert("sub_args".into(), PyValue::str_list(&parsed_sub));

    // Remove `None` values and turn the string 'None' into `None`.
    // PARITY: this applies to every key, including `text` and `name` (`--name None`).
    args.retain(|_, v| !matches!(v, PyValue::None));
    for v in args.values_mut() {
        if v.as_str() == Some("None") {
            *v = PyValue::None;
        }
    }

    if let Some(PyValue::List(lks)) = args.get_mut("location_keys") {
        for lk in lks.iter_mut() {
            if matches!(lk.as_str(), Some("[None]") | Some("None")) {
                *lk = PyValue::None;
            }
        }
    }

    parse_synonyms(args)
}

/// `parse_synonyms`.
pub fn parse_synonyms(mut args: ArgsMap) -> Result<ArgsMap, PyException> {
    let truthy = |args: &ArgsMap, k: &str| args.get(k).is_some_and(PyValue::is_truthy);
    if truthy(&args, "async") {
        args.insert("unblock".into(), PyValue::Bool(true));
    }
    if truthy(&args, "mrsm_instance") {
        let v = args["mrsm_instance"].clone();
        args.insert("instance".into(), v);
    }
    if truthy(&args, "skip_check_existing") {
        args.insert("check_existing".into(), PyValue::Bool(false));
    }
    if truthy(&args, "skip_enforce_dtypes") {
        args.insert("enforce_dtypes".into(), PyValue::Bool(false));
    }
    if matches!(
        args.get("venv").and_then(PyValue::as_str),
        Some("None") | Some("[None]")
    ) {
        args.insert("venv".into(), PyValue::None);
    }
    let chunk = [
        ("chunk_minutes", 60_000_000i128),
        ("chunk_hours", 3_600_000_000),
        ("chunk_days", 86_400_000_000),
    ];
    for (key, unit_micros) in chunk {
        match args.get(key) {
            Some(PyValue::Int(n)) => {
                let total = n.checked_mul(unit_micros).ok_or_else(|| {
                    PyException::new("OverflowError", "Python int too large to convert to C int")
                })?;
                let td = PyTimeDelta::from_microseconds(total)?;
                args.insert("chunk_interval".into(), PyValue::TimeDelta(td));
                break;
            }
            Some(PyValue::BigInt(_)) => {
                return Err(PyException::new(
                    "OverflowError",
                    "Python int too large to convert to C int",
                ));
            }
            _ => {}
        }
    }
    Ok(args)
}

/// `parse_line(line)`: `shlex.split` then `parse_arguments`; any failure yields
/// `{'action': [], 'text': line}`.
pub fn parse_line(line: &str, ctx: &DateContext) -> ArgsMap {
    let fallback = || {
        let mut m = ArgsMap::new();
        m.insert("action".into(), PyValue::List(vec![]));
        m.insert("text".into(), PyValue::str(line));
        m
    };
    match shlex::split(line) {
        Ok(sysargs) => parse_arguments(&sysargs, ctx).unwrap_or_else(|_| fallback()),
        Err(_) => fallback(),
    }
}

/// One v5-only option parsed by `engine_arguments` (outside the frozen v4 [`ARGUMENTS`]).
#[derive(Debug, Clone, Copy)]
pub struct EngineOption {
    /// The key in the arguments dictionary.
    pub dest: &'static str,
    /// The long flag, e.g. `--parents`.
    pub flag: &'static str,
}

/// Every v5-only option `engine_arguments` accepts. It is the single list both directions read:
/// `engine_arguments` stores a flag only under its `dest` here, and
/// [`engine_options_to_sysargs`] (used by `parse_dict_to_sysargs`) writes each `dest` back, so a new
/// flag cannot be dropped when the shell rebuilds a command line.
pub const ENGINE_OPTIONS: &[EngineOption] = &[
    EngineOption {
        dest: "parents",
        flag: "--parents",
    },
    EngineOption {
        dest: "children",
        flag: "--children",
    },
    EngineOption {
        dest: "chunk_pipelining",
        flag: "--chunk-pipelining",
    },
    EngineOption {
        dest: "no_stagger",
        flag: "--no-stagger",
    },
    EngineOption {
        dest: "startup_jitter",
        flag: "--startup-jitter",
    },
    EngineOption {
        dest: "jitter",
        flag: "--jitter",
    },
    EngineOption {
        dest: "all",
        flag: "--all",
    },
    EngineOption {
        dest: "last",
        flag: "--last",
    },
    EngineOption {
        dest: "resume",
        flag: "--resume",
    },
    EngineOption {
        dest: "run_id",
        flag: "--run-id",
    },
];

/// The command-line words for the [`ENGINE_OPTIONS`] present in `args` (the reverse of
/// `engine_arguments`). Values are attached (`--children=2`) so they never read as positionals.
pub fn engine_options_to_sysargs(args: &ArgsMap) -> Vec<String> {
    let mut out = Vec::new();
    for opt in ENGINE_OPTIONS {
        match args.get(opt.dest) {
            None | Some(PyValue::None) | Some(PyValue::Bool(false)) => {}
            Some(PyValue::Bool(true)) => out.push(opt.flag.to_string()),
            Some(value) => out.push(format!("{}={}", opt.flag, value.py_str())),
        }
    }
    out
}

/// v5-only graph options are separate from the frozen v4 argument schema.
fn engine_arguments(sysargs: &[String]) -> Result<(Vec<String>, ArgsMap), PyException> {
    let mut rest = Vec::new();
    let mut args = ArgsMap::new();
    let mut i = 0;
    while i < sysargs.len() {
        let token = &sysargs[i];
        if token == "--" || token == "-A" {
            rest.extend_from_slice(&sysargs[i..]);
            break;
        }
        let (flag, attached) = token
            .split_once('=')
            .map_or((token.as_str(), None), |(a, b)| (a, Some(b)));
        let Some(opt) = ENGINE_OPTIONS.iter().find(|o| o.flag == flag) else {
            rest.push(token.clone());
            i += 1;
            continue;
        };
        let value = match opt.dest {
            "parents" | "children" => {
                let value = attached.or_else(|| {
                    sysargs
                        .get(i + 1)
                        .map(String::as_str)
                        .filter(|s| s.parse::<i128>().is_ok())
                });
                match value {
                    None => Some(PyValue::Bool(true)),
                    Some(s) => {
                        let n = s.parse::<i128>().ok().filter(|n| *n >= 0).ok_or_else(|| {
                            PyException::new(
                                "InvalidArgumentException",
                                format!("{flag} requires a non-negative depth"),
                            )
                        })?;
                        if attached.is_none() {
                            i += 1;
                        }
                        Some(PyValue::Int(n))
                    }
                }
            }
            // Wave 18 (D23): start jobs without the startup jitter, load wait and slots.
            "chunk_pipelining" | "no_stagger" if attached.is_none() => Some(PyValue::Bool(true)),
            // Wave 18 (D23): `--startup-jitter 30s` bounds a job's startup delay, `--jitter 30s`
            // its schedule's fixed offset (scoped to a schedule so plugin flags named `--jitter`
            // still parse).
            "startup_jitter" | "jitter"
                if opt.dest == "startup_jitter" || has_schedule_flag(sysargs) =>
            {
                let value = attached
                    .or_else(|| sysargs.get(i + 1).map(String::as_str))
                    .filter(|v| meerschaum_jobs::schedule::parse_jitter_seconds(v).is_some())
                    .ok_or_else(|| {
                        PyException::new(
                            "InvalidArgumentException",
                            format!("{flag} requires a duration like 30s, 5m or 1h"),
                        )
                    })?;
                if attached.is_none() {
                    i += 1;
                }
                Some(PyValue::Str(value.to_owned()))
            }
            // `show data --all` (v5-only): print every row and column untruncated.
            "all" if attached.is_none() && is_show_data(sysargs) => Some(PyValue::Bool(true)),
            // `show runs --last N` (v5-only; scoped so plugin flags named `--last` still parse).
            "last" if is_show_runs(sysargs) => {
                let value = attached
                    .or_else(|| sysargs.get(i + 1).map(String::as_str))
                    .and_then(|s| s.parse::<i128>().ok())
                    .filter(|n| *n >= 0)
                    .ok_or_else(|| {
                        PyException::new(
                            "InvalidArgumentException",
                            "--last requires a non-negative integer",
                        )
                    })?;
                if attached.is_none() {
                    i += 1;
                }
                Some(PyValue::Int(value))
            }
            "resume" | "run_id" => {
                let value = attached
                    .or_else(|| {
                        sysargs
                            .get(i + 1)
                            .map(String::as_str)
                            .filter(|s| !s.starts_with('-'))
                    })
                    .ok_or_else(|| {
                        PyException::new(
                            "InvalidArgumentException",
                            format!("{flag} requires a run ID"),
                        )
                    })?;
                if opt.dest == "run_id"
                    && (value.is_empty()
                        || value.len() > 200
                        || !value
                            .bytes()
                            .all(|c| c.is_ascii_alphanumeric() || b"_.:-".contains(&c)))
                {
                    return Err(PyException::new(
                        "InvalidArgumentException",
                        "Invalid --run-id; use letters, numbers, dots, colons, underscores or hyphens.",
                    ));
                }
                if attached.is_none() {
                    i += 1;
                }
                Some(PyValue::Str(value.to_owned()))
            }
            // Out of scope (e.g. a plugin's own `--all`): leave it for the v4 parser.
            _ => None,
        };
        match value {
            Some(value) => {
                args.insert(opt.dest.into(), value);
            }
            None => rest.push(token.clone()),
        }
        i += 1;
    }
    Ok((rest, args))
}

/// Whether the arguments carry a schedule (`-s`, `--schedule`, `--cron`).
fn has_schedule_flag(sysargs: &[String]) -> bool {
    sysargs
        .iter()
        .take_while(|a| *a != "--" && *a != "-A")
        .any(|a| {
            a == "-s"
                || a == "--cron"
                || a == "--schedule"
                || a.starts_with("--schedule=")
                || a.starts_with("--cron=")
        })
}

/// Whether the command is `show data` (the two words may follow global options such as
/// `-i sql:main`).
fn is_show_data(sysargs: &[String]) -> bool {
    sysargs
        .iter()
        .take_while(|s| s.as_str() != "--" && s.as_str() != "+")
        .map(String::as_str)
        .collect::<Vec<_>>()
        .windows(2)
        .any(|w| w == ["show", "data"])
}

/// Whether the words name `show run` / `show runs`.
fn is_show_runs(sysargs: &[String]) -> bool {
    let words: Vec<&str> = sysargs
        .iter()
        .map(String::as_str)
        .take_while(|s| !s.starts_with('-'))
        .collect();
    words.first() == Some(&"show") && matches!(words.get(1), Some(&"run" | &"runs"))
}

#[cfg(test)]
mod engine_argument_tests {
    use super::*;

    #[test]
    fn all_is_a_show_data_flag_only() {
        let words = ["show", "data", "60", "-m", "x", "--all"].map(str::to_owned);
        let (rest, args) = engine_arguments(&words).unwrap();
        assert_eq!(rest, ["show", "data", "60", "-m", "x"]);
        assert_eq!(args["all"], PyValue::Bool(true));
        // Global options may come first.
        let leading = ["-i", "sql:x", "show", "data", "--all"].map(str::to_owned);
        assert_eq!(
            engine_arguments(&leading).unwrap().0,
            ["-i", "sql:x", "show", "data"]
        );
        // Other actions keep `--all` for plugins and their own parsing.
        let other = ["sync", "pipes", "--all"].map(str::to_owned);
        let (rest, args) = engine_arguments(&other).unwrap();
        assert_eq!(rest, other);
        assert!(!args.contains_key("all"));
    }

    #[test]
    fn graph_depths_and_resume_are_preserved() {
        let words = [
            "sync",
            "pipes",
            "--parents",
            "--children=2",
            "--resume",
            "run-1",
            "-w",
            "1",
        ]
        .map(str::to_owned);
        let (rest, args) = engine_arguments(&words).unwrap();
        assert_eq!(rest, ["sync", "pipes", "-w", "1"]);
        assert_eq!(args["parents"], PyValue::Bool(true));
        assert_eq!(args["children"], PyValue::Int(2));
        assert_eq!(args["resume"], PyValue::Str("run-1".into()));
        assert!(engine_arguments(&["--parents=-1".into()]).is_err());
        assert!(engine_arguments(&["--resume".into()]).is_err());
    }

    #[test]
    fn chunk_pipelining_and_last() {
        let words = ["show", "runs", "--last", "3", "--chunk-pipelining"].map(str::to_owned);
        let (rest, args) = engine_arguments(&words).unwrap();
        assert_eq!(rest, ["show", "runs"]);
        assert_eq!(args["last"], PyValue::Int(3));
        assert_eq!(args["chunk_pipelining"], PyValue::Bool(true));
        // `--last` belongs to a plugin outside `show runs`.
        let words = ["sync", "pipes", "--last", "3"].map(str::to_owned);
        let (rest, args) = engine_arguments(&words).unwrap();
        assert_eq!(rest, words);
        assert!(!args.contains_key("last"));
        assert!(engine_arguments(&["show".into(), "runs".into(), "--last".into()]).is_err());
    }

    #[test]
    fn job_stagger_and_jitter_flags() {
        let words = [
            "start",
            "jobs",
            "--no-stagger",
            "--startup-jitter",
            "5s",
            "a",
        ]
        .map(str::to_owned);
        let (rest, args) = engine_arguments(&words).unwrap();
        assert_eq!(rest, ["start", "jobs", "a"]);
        assert_eq!(args["no_stagger"], PyValue::Bool(true));
        assert_eq!(args["startup_jitter"], PyValue::Str("5s".into()));

        // `--jitter` is the schedule's, so it needs one; otherwise it belongs to a plugin.
        let words = ["sync", "pipes", "-s", "daily", "--jitter=30s", "-d"].map(str::to_owned);
        let (rest, args) = engine_arguments(&words).unwrap();
        assert_eq!(rest, ["sync", "pipes", "-s", "daily", "-d"]);
        assert_eq!(args["jitter"], PyValue::Str("30s".into()));
        let words = ["myplugin", "--jitter", "3"].map(str::to_owned);
        let (rest, args) = engine_arguments(&words).unwrap();
        assert_eq!(rest, words);
        assert!(!args.contains_key("jitter"));

        // A value which is not a duration is an error, not a silent default.
        let bad = ["sync", "pipes", "-s", "daily", "--jitter", "soon"].map(str::to_owned);
        assert!(engine_arguments(&bad).is_err());
        assert!(engine_arguments(&["--startup-jitter".into()]).is_err());
    }
}

#[cfg(test)]
mod python_script_tests {
    use super::*;

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn list(args: &ArgsMap, key: &str) -> Vec<String> {
        match args.get(key) {
            Some(PyValue::List(items)) => items.iter().map(PyValue::py_str).collect(),
            other => panic!("{key}: {other:?}"),
        }
    }

    #[test]
    fn the_words_after_the_file_are_the_scripts() {
        let ctx = DateContext::system();
        let script_words = [
            "--dry-run",
            "-y",
            "-i",
            "sql:other",
            "--begin",
            "2024-01-01",
            "-d",
            "[--z]",
            "-A",
            "a b",
        ];
        // Options may come before `python` and between `python` and the file.
        let mut words = v(&[
            "--debug", "python", "--venv", "tools", "-i", "sql:x", "s.py",
        ]);
        words.extend(v(&script_words));
        assert_eq!(python_script_index(&words), Some((1, 6)));
        let args = parse_arguments(&words, &ctx).unwrap();
        let mut action = v(&["python", "s.py"]);
        action.extend(v(&script_words));
        assert_eq!(list(&args, "action"), action);
        // Only the words before the file are Meerschaum's.
        assert_eq!(args["venv"], PyValue::Str("tools".into()));
        assert_eq!(args["mrsm_instance"], PyValue::Str("sql:x".into()));
        assert_eq!(args["debug"], PyValue::Bool(true));
        for key in ["dry_run", "yes", "begin", "daemon"] {
            assert!(!args.contains_key(key), "{key} was parsed: {args:?}");
        }
        assert!(list(&args, "sub_args").is_empty());
        assert_eq!(list(&args, "sysargs"), words);
        assert!(list(&args, "filtered_sysargs").ends_with(&v(&script_words)));
        assert_eq!(meerschaum_words(&words), &words[..=6]);
    }

    #[test]
    fn only_a_python_file_splits_the_line() {
        // Code, `-A`, other actions and a missing file name keep parsing flags.
        assert_eq!(python_script_index(&v(&["python", "print(1)", "-y"])), None);
        assert_eq!(
            python_script_index(&v(&["python", "--venv=tools", "s.py", "-y"])),
            Some((0, 2))
        );
        assert_eq!(
            python_script_index(&v(&["python", "-isql:x", "[-u]", "s.pyw", "-y"])),
            Some((0, 3))
        );
        assert_eq!(
            python_script_index(&v(&["python", "-A", "s.py", "-y"])),
            None
        );
        assert_eq!(python_script_index(&v(&["python", "-c", "s.py"])), None);
        assert_eq!(python_script_index(&v(&["sync", "s.py", "-y"])), None);
        assert_eq!(python_script_index(&v(&["python"])), None);
        // `-m` takes words up to the next option.
        assert_eq!(
            python_script_index(&v(&["python", "-m", "a", "b", "--debug", "s.py"])),
            Some((0, 5))
        );
        // An existing file without the extension is a script too.
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("run_me");
        std::fs::write(&script, "print(1)\n").unwrap();
        let path = script.display().to_string();
        assert_eq!(
            python_script_index(&v(&["python", &path, "--x"])),
            Some((0, 1))
        );
        assert!(!is_python_script("print(1); x.py"));
        assert!(!is_python_script("-x.py"));
        // `-h` after the file is the script's.
        assert_eq!(
            meerschaum_words(&v(&["python", "s.py", "-h"])),
            &v(&["python", "s.py"])[..]
        );
    }

    /// The shell and jobs rebuild the line from the dictionary: the script's words come back
    /// last and unchanged, after Meerschaum's flags (and the interpreter's sub-arguments).
    #[test]
    fn the_script_words_survive_the_round_trip() {
        let ctx = DateContext::system();
        let first =
            parse_arguments(&v(&["python", "s.py", "--x", "a b", "-i", "y"]), &ctx).unwrap();
        let mut kwargs = first.clone();
        kwargs.shift_remove("sysargs");
        kwargs.shift_remove("filtered_sysargs");
        kwargs.insert("mrsm_instance".into(), PyValue::Str("sql:shell".into()));
        kwargs.insert("debug".into(), PyValue::Bool(true));
        let rebuilt = crate::sysargs::parse_dict_to_sysargs(&kwargs).unwrap();
        assert_eq!(rebuilt.first().map(String::as_str), Some("python"));
        assert_eq!(
            &rebuilt[rebuilt.len() - 5..],
            &v(&["s.py", "--x", "a b", "-i", "y"])[..]
        );
        let second = parse_arguments(&rebuilt, &ctx).unwrap();
        assert_eq!(list(&second, "action"), list(&first, "action"));
        assert_eq!(second["mrsm_instance"], PyValue::Str("sql:shell".into()));
        assert_eq!(second["debug"], PyValue::Bool(true));

        // The interpreter's sub-arguments (`python [-u] s.py`) stay before the file.
        let with_sub = parse_arguments(&v(&["python", "[-u]", "s.py", "--x"]), &ctx).unwrap();
        assert_eq!(list(&with_sub, "sub_args"), v(&["-u"]));
        assert_eq!(list(&with_sub, "action"), v(&["python", "s.py", "--x"]));
        let mut kwargs = with_sub.clone();
        kwargs.shift_remove("sysargs");
        kwargs.shift_remove("filtered_sysargs");
        let rebuilt = crate::sysargs::parse_dict_to_sysargs(&kwargs).unwrap();
        assert_eq!(rebuilt, v(&["python", "[-u]", "s.py", "--x"]));
        let again = parse_arguments(&rebuilt, &ctx).unwrap();
        assert_eq!(list(&again, "sub_args"), v(&["-u"]));
        assert_eq!(list(&again, "action"), list(&with_sub, "action"));
        // Any other action keeps v4's shape.
        let other = parse_arguments(&v(&["show", "pipes", "-A", "x"]), &ctx).unwrap();
        assert_eq!(
            crate::sysargs::parse_dict_to_sysargs(&other).unwrap()[..2],
            v(&["show", "pipes"])[..]
        );
    }
}
