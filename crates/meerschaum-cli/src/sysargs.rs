//! Sysargs-level helpers: `+` chaining, `:` pipeline flags, API executor keys, the
//! `start pipeline` rendition, and the arguments-dict → sysargs round trip
//! (`meerschaum/_internal/arguments/_parse_arguments.py`).

use crate::shlex;
use crate::spec::{ARGUMENTS, keys};
use crate::value::{ArgsMap, PyException, PyValue};

/// `split_pipeline_sysargs`: split at the last `:` into (sysargs, pipeline flags).
pub fn split_pipeline_sysargs(sysargs: &[String]) -> (Vec<String>, Vec<String>) {
    let Some(ix) = sysargs.iter().rposition(|a| a == keys::PIPELINE) else {
        return (sysargs.to_vec(), vec![]);
    };
    let after = sysargs[ix + 1..].to_vec();
    // PARITY: every earlier `:` is dropped too, not only the last one (`split_pipeline_sysargs`).
    let before = sysargs[..ix]
        .iter()
        .filter(|a| *a != keys::PIPELINE)
        .cloned()
        .collect();
    (before, after)
}

/// `split_chained_sysargs`: split at `+` into separate sysargs lists.
///
/// The `Err` case mirrors the `IndexError` v4 raises when the input is only `+` keys.
pub fn split_chained_sysargs(sysargs: &[String]) -> Result<Vec<Vec<String>>, PyException> {
    if sysargs.is_empty() || !sysargs.iter().any(|a| a == keys::AND) {
        return Ok(vec![sysargs.to_vec()]);
    }
    // Coalesce consecutive joiners into one.
    let mut coalesced: Vec<String> = Vec::new();
    for arg in sysargs {
        if arg == keys::AND && coalesced.last().map(String::as_str) == Some(keys::AND) {
            continue;
        }
        coalesced.push(arg.clone());
    }
    // Remove any joiners from the ends.
    if coalesced.first().map(String::as_str) == Some(keys::AND) {
        coalesced.remove(0);
    }
    // PARITY: `coalesce_args[-1]` raises IndexError for `['+']` (`split_chained_sysargs`).
    let last = coalesced
        .last()
        .ok_or_else(|| PyException::new("IndexError", "list index out of range"))?;
    if last == keys::AND {
        coalesced.pop();
    }
    let mut chained = Vec::new();
    let mut current = Vec::new();
    for arg in coalesced {
        if arg != keys::AND {
            current.push(arg);
        } else {
            chained.push(std::mem::take(&mut current));
        }
    }
    chained.push(current);
    Ok(chained)
}

/// `sysargs_has_api_executor_keys`.
pub fn sysargs_has_api_executor_keys(sysargs: &[String]) -> bool {
    for (i, arg) in sysargs.iter().enumerate() {
        if arg != "-e" && arg != "--executor-keys" {
            continue;
        }
        let Some(keys) = sysargs.get(i + 1) else {
            return false;
        };
        if keys.starts_with("api:") {
            return true;
        }
    }
    false
}

/// `remove_api_executor_keys`.
pub fn remove_api_executor_keys(sysargs: &[String]) -> Vec<String> {
    if !sysargs_has_api_executor_keys(sysargs) {
        return sysargs.to_vec();
    }
    let mut skip = Vec::new();
    for (i, arg) in sysargs.iter().enumerate() {
        if arg == "-e" || arg == "--executor-keys" {
            skip.push(i);
            skip.push(i + 1);
        }
    }
    sysargs
        .iter()
        .enumerate()
        .filter(|(i, _)| !skip.contains(i))
        .map(|(_, a)| a.clone())
        .collect()
}

/// `get_pipeline_sysargs`: `start pipeline <pipeline flags> -P {"sub_args_line": ..., "patch_args": ...}`.
pub fn get_pipeline_sysargs(
    sysargs: &[String],
    pipeline_args: &[String],
    patch_args: Option<&ArgsMap>,
) -> Vec<String> {
    let patch = match patch_args {
        Some(m) => PyValue::Dict(
            m.iter()
                .map(|(k, v)| (PyValue::str(k), v.clone()))
                .collect(),
        ),
        None => PyValue::None,
    };
    let params = PyValue::Dict(vec![
        (
            PyValue::str("sub_args_line"),
            PyValue::str(shlex::join(sysargs)),
        ),
        (PyValue::str("patch_args"), patch),
    ]);
    let mut out = vec!["start".to_string(), "pipeline".to_string()];
    out.extend(pipeline_args.iter().cloned());
    out.push("-P".into());
    out.push(params.json_dumps_compact().unwrap_or_else(|_| "{}".into()));
    out
}

/// `compress_pipeline_sysargs`: the condensed `<sysargs> : <pipeline flags>` rendition.
pub fn compress_pipeline_sysargs(pipeline_sysargs: &[String]) -> Result<Vec<String>, PyException> {
    if pipeline_sysargs.len() < 2
        || pipeline_sysargs[0] != "start"
        || pipeline_sysargs[1] != "pipeline"
    {
        return Ok(pipeline_sysargs.to_vec());
    }
    let Some(params_ix) = pipeline_sysargs.iter().position(|a| a == "-P") else {
        return Ok(pipeline_sysargs.to_vec());
    };
    let pipeline_args = &pipeline_sysargs[2..params_ix];
    let params_str = pipeline_sysargs.last().cloned().unwrap_or_default();
    let Ok(PyValue::Dict(params)) = crate::pyjson::loads(&params_str) else {
        // PARITY: a JSON value that is not an object makes `.get` raise in v4.
        return match crate::pyjson::loads(&params_str) {
            Ok(other) => Err(PyException::new(
                "AttributeError",
                format!("'{}' object has no attribute 'get'", other.type_name()),
            )),
            Err(_) => Ok(pipeline_sysargs.to_vec()),
        };
    };
    let line = params
        .iter()
        .find(|(k, _)| k.as_str() == Some("sub_args_line"))
        .map(|(_, v)| v.clone())
        .unwrap_or(PyValue::None);
    if !line.is_truthy() {
        return Ok(pipeline_sysargs.to_vec());
    }
    let PyValue::Str(line) = line else {
        return Err(PyException::new(
            "AttributeError",
            "sub_args_line is not a string",
        ));
    };
    let mut out = shlex::split(&line).map_err(|e| PyException::new("ValueError", e))?;
    out.push(keys::PIPELINE.into());
    out.extend(pipeline_args.iter().cloned());
    Ok(out)
}

/// `parse_dict_to_sysargs`: revert an arguments dictionary back to a command line.
pub fn parse_dict_to_sysargs(args: &ArgsMap) -> Result<Vec<String>, PyException> {
    let mut sysargs: Vec<String> = Vec::new();
    if let Some(PyValue::List(action)) = args.get("action") {
        sysargs.extend(action.iter().map(PyValue::py_str));
    }
    let allow_none = ["location_keys", "begin", "end", "executor_keys"];

    for spec in ARGUMENTS {
        let a = spec.dest;
        if a == "action" {
            continue;
        }
        let Some(value) = args.get(a) else {
            continue;
        };
        let flag = spec.option_strings[0].to_string();
        match value {
            PyValue::Bool(true) => sysargs.push(flag),
            PyValue::Bool(false) => {}
            PyValue::List(items) | PyValue::Tuple(items) => {
                if items.is_empty() {
                    continue;
                }
                let is_blank = items.len() == 1 && items[0].as_str() == Some("");
                if a == "sub_args" && !is_blank {
                    let strs: Vec<String> = items.iter().map(PyValue::py_str).collect();
                    sysargs.push("-A".into());
                    sysargs.push(shlex::join(&strs));
                } else {
                    sysargs.push(flag);
                    sysargs.extend(items.iter().map(PyValue::py_str));
                }
            }
            PyValue::Dict(items) => {
                if !items.is_empty() {
                    sysargs.push(flag);
                    sysargs.push(value.json_dumps_compact()?);
                }
            }
            _ if (a == "begin" || a == "end") && args.contains_key("sysargs") => {
                // Preserve the original datetime strings.
                let original: Vec<String> = match &args["sysargs"] {
                    PyValue::List(l) => l.iter().map(PyValue::py_str).collect(),
                    _ => Vec::new(),
                };
                // PARITY: `list.index` raises ValueError when the flag was spelled `--begin=...`
                // or abbreviated (`parse_dict_to_sysargs`).
                let ix = original.iter().position(|s| *s == flag).ok_or_else(|| {
                    PyException::new("ValueError", "list.index(x): x not in list")
                })?;
                let val = original
                    .get(ix + 1)
                    .cloned()
                    .unwrap_or_else(|| value.py_str());
                sysargs.push(flag);
                sysargs.push(val);
            }
            PyValue::None if allow_none.contains(&a) => {
                sysargs.push(flag);
                sysargs.push("None".into());
            }
            PyValue::None => {}
            other => {
                sysargs.push(flag);
                sysargs.push(other.py_str());
            }
        }
    }
    Ok(sysargs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn chaining() {
        assert_eq!(
            split_chained_sysargs(&v(&["+", "show", "pipes", "+", "+", "x", "+"])).unwrap(),
            vec![v(&["show", "pipes"]), v(&["x"])]
        );
        assert!(split_chained_sysargs(&v(&["+"])).is_err());
        assert_eq!(
            split_pipeline_sysargs(&v(&["a", ":", "b", ":", "-d"])),
            (v(&["a", "b"]), v(&["-d"]))
        );
    }
}
