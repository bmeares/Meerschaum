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
    let sysargs = rewrite_python_code_flag(sysargs);
    let (sub_arguments, filtered_sysargs) = extract_decorated_sub_args(&sysargs);

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
            let mut m = ArgsMap::new();
            m.insert("action".into(), PyValue::str_list(&action));
            m.insert("sysargs".into(), PyValue::str_list(&sysargs));
            m.insert("text".into(), PyValue::str(shlex::join(&sysargs)));
            m.insert(
                keys::FAILURE_KEY.into(),
                PyValue::Exception(err.into_exception()),
            );
            m
        }
    };

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

    args.insert("sysargs".into(), PyValue::str_list(&sysargs));
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
