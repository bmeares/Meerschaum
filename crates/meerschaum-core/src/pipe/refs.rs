//! Parsing pipe references from parameters: `get_pipe_from_value` and `get_pipe_from_string`
//! (`meerschaum/utils/pipes.py`), used by `parents`, `children` and `references`.

use std::sync::LazyLock;

use regex::Regex;

use super::{Pipe, PipeDefaults, PipeError};
use crate::pyfmt::parse_arguments_str;
use crate::value::{Map, Value};

static PIPE_CALL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:mrsm\.)?Pipe\((.*?)\)").expect("valid regex"));

const INSTANCE_KWARGS: [&str; 3] = ["instance", "mrsm_instance", "instance_keys"];

fn with_parent_instance(kwargs: &mut Map, parent: Option<&Pipe>) {
    if let Some(parent) = parent
        && !INSTANCE_KWARGS.iter().any(|k| kwargs.contains_key(*k))
    {
        kwargs.insert(
            "instance".into(),
            Value::Str(parent.instance_keys().to_string()),
        );
    }
}

/// Port of `get_pipe_from_string`: build the pipe from the first `Pipe(...)` /
/// `mrsm.Pipe(...)` call found in the string. Returns `None` if there is no call or it fails
/// (Python warns). Without an explicit instance, the parent's instance is used.
pub fn pipe_from_string(s: &str, parent: Option<&Pipe>, defaults: &PipeDefaults) -> Option<Pipe> {
    let caps = PIPE_CALL_RE.captures(s)?;
    let (args, mut kwargs) = parse_arguments_str(&caps[1]).ok()?;
    with_parent_instance(&mut kwargs, parent);
    Pipe::from_call(&args, &kwargs, defaults).ok()
}

/// Port of `get_pipe_from_value`: a `Pipe(...)` string, or a dict of constructor keyword
/// arguments (including the `keys()` form: `connector_keys`, `metric_key`, `location_key`,
/// `instance_keys`).
///
/// Returns `Ok(None)` where Python warns and returns `None`, and an error for other value types
/// (Python raises `ValueError("Expecting kwargs for a Pipe.")`).
pub fn pipe_from_value(
    value: &Value,
    parent: Option<&Pipe>,
    defaults: &PipeDefaults,
) -> Result<Option<Pipe>, PipeError> {
    match value {
        Value::Str(s) => Ok(pipe_from_string(s, parent, defaults)),
        Value::Map(m) => {
            let mut kwargs = m.clone();
            with_parent_instance(&mut kwargs, parent);
            Ok(Pipe::from_call(&[], &kwargs, defaults).ok())
        }
        _ => Err(PipeError::ValueError("Expecting kwargs for a Pipe.".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refs() {
        let d = PipeDefaults::default();
        let parent = Pipe::builder("sql:main", "x")
            .instance("sql:local")
            .build()
            .unwrap();
        let p = pipe_from_string("Pipe('plugin:foo', 'bar')", Some(&parent), &d).unwrap();
        assert_eq!(p.instance_keys(), "sql:local");
        let p = pipe_from_string(
            "mrsm.Pipe(\"a\", \"b\", instance=\"sql:other\")",
            Some(&parent),
            &d,
        )
        .unwrap();
        assert_eq!(p.instance_keys(), "sql:other");
        assert!(pipe_from_string("not a pipe", None, &d).is_none());
        let v = Value::from_json_str(
            r#"{"connector_keys": "a", "metric_key": "b", "location_key": null, "instance_keys": "sql:x"}"#,
        )
        .unwrap();
        let p = pipe_from_value(&v, Some(&parent), &d).unwrap().unwrap();
        assert_eq!(p.to_string(), "Pipe('a', 'b', instance='sql:x')");
        assert!(pipe_from_value(&Value::Int(1), None, &d).is_err());
    }
}
