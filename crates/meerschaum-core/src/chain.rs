//! Action chains: several commands run as one (`sync pipes -c a + sync pipes -c b`), with flags
//! that apply to the whole chain after a `:` (`... : --loop --min-seconds 60`).
//!
//! A chain that runs later (a background job, a schedule, a remote API) is written as one command
//! line under the internal action [`CHAIN_ACTION`]:
//!
//! ```text
//! chain <chain flags> -P '{"sub_args_line": "<a + b>", "patch_args": <dict or null>}'
//! ```
//!
//! The action is never listed in help or completion; the dispatcher only runs it with that
//! payload. Meerschaum 4.1 (and 5.0 before rc15) wrote the same payload under `start pipeline`
//! ([`LEGACY_CHAIN_ACTION`]). That form keeps loading: jobs on disk and older API servers carry it,
//! and every reader here understands both. `start pipeline NAME` now starts a saved Pipeline's job.

use crate::value::Value;

/// The internal action a serialized chain runs under.
pub const CHAIN_ACTION: &str = "chain";

/// The action words Meerschaum 4.1 serialized chains under.
pub const LEGACY_CHAIN_ACTION: [&str; 2] = ["start", "pipeline"];

/// The payload key holding the chained command line.
pub const LINE_KEY: &str = "sub_args_line";

/// The payload key holding the arguments patched into every step.
pub const PATCH_KEY: &str = "patch_args";

/// A chain read back from its serialized form.
#[derive(Debug, Clone, PartialEq)]
pub struct SerializedChain {
    /// The chained command line (`show version + show arguments`).
    pub line: String,
    /// The flags for the whole chain (what followed `:`), in order.
    pub flags: Vec<String>,
    /// The arguments patched into every step (`patch_args`), when given.
    pub patch: Option<Value>,
    /// Written by Meerschaum 4.1 (`start pipeline ... -P ...`).
    pub legacy: bool,
}

/// How many leading words name the chain action in `sysargs` (`chain`: 1, `start pipeline`: 2).
pub fn action_len(sysargs: &[String]) -> Option<usize> {
    if sysargs.first().map(String::as_str) == Some(CHAIN_ACTION) {
        return Some(1);
    }
    if sysargs.len() >= 2
        && sysargs[0] == LEGACY_CHAIN_ACTION[0]
        && sysargs[1] == LEGACY_CHAIN_ACTION[1]
    {
        return Some(2);
    }
    None
}

/// Whether a `params` value carries a chain (a mapping with a non-empty `sub_args_line`).
pub fn params_carry_chain(params: &Value) -> bool {
    params
        .get(LINE_KEY)
        .is_some_and(|line| line.truthy() && line.as_str().is_some())
}

/// Read a serialized chain (either form). `None` when `sysargs` is not one: another command, no
/// `-P`, a payload that is not JSON or carries no `sub_args_line`.
///
/// PARITY: like v4's `compress_pipeline_sysargs`, the flags are the words between the action and
/// the first `-P`, and the payload is the last word.
pub fn parse(sysargs: &[String]) -> Option<SerializedChain> {
    let start = action_len(sysargs)?;
    let params_ix = sysargs.iter().position(|a| a == "-P")?;
    if params_ix < start {
        return None;
    }
    let payload = Value::from_json_str(sysargs.last()?).ok()?;
    if !params_carry_chain(&payload) {
        return None;
    }
    let line = payload.get(LINE_KEY)?.as_str()?.to_string();
    let patch = payload.get(PATCH_KEY).filter(|p| !p.is_null()).cloned();
    Some(SerializedChain {
        line,
        flags: sysargs[start..params_ix].to_vec(),
        patch,
        legacy: start == 2,
    })
}

/// A serialized chain back in the form it was typed: `<chained line> : <flags>`. Any other
/// command is returned as it is.
pub fn compress(sysargs: &[String]) -> Vec<String> {
    let Some(chain) = parse(sysargs) else {
        return sysargs.to_vec();
    };
    let Ok(mut out) = crate::shlex::split(&chain.line) else {
        return sysargs.to_vec();
    };
    out.push(":".into());
    out.extend(chain.flags);
    out
}

/// A serialized chain in the form every Meerschaum version runs (`start pipeline ...`), for an API
/// server of unknown version. Any other command is returned as it is.
pub fn to_legacy(sysargs: &[String]) -> Vec<String> {
    if action_len(sysargs) != Some(1) || parse(sysargs).is_none() {
        return sysargs.to_vec();
    }
    LEGACY_CHAIN_ACTION
        .iter()
        .map(|s| s.to_string())
        .chain(sysargs[1..].iter().cloned())
        .collect()
}

/// A serialized chain in the current form (`chain ...`). Any other command is returned as it is.
pub fn to_current(sysargs: &[String]) -> Vec<String> {
    if action_len(sysargs) != Some(2) || parse(sysargs).is_none() {
        return sysargs.to_vec();
    }
    std::iter::once(CHAIN_ACTION.to_string())
        .chain(sysargs[2..].iter().cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    const PAYLOAD: &str = r#"{"sub_args_line":"show version + show arguments","patch_args":null}"#;

    #[test]
    fn both_forms_parse_and_compress_alike() {
        let current = v(&["chain", "--loop", "--min-seconds", "5", "-P", PAYLOAD]);
        let legacy = v(&[
            "start",
            "pipeline",
            "--loop",
            "--min-seconds",
            "5",
            "-P",
            PAYLOAD,
        ]);
        let expected = v(&[
            "show",
            "version",
            "+",
            "show",
            "arguments",
            ":",
            "--loop",
            "--min-seconds",
            "5",
        ]);
        assert_eq!(compress(&current), expected);
        assert_eq!(compress(&legacy), expected);
        let parsed = parse(&legacy).unwrap();
        assert!(parsed.legacy);
        assert_eq!(parsed.flags, v(&["--loop", "--min-seconds", "5"]));
        assert_eq!(parsed.patch, None);
        assert!(!parse(&current).unwrap().legacy);
        assert_eq!(to_legacy(&current), legacy);
        assert_eq!(to_current(&legacy), current);
        assert_eq!(to_legacy(&legacy), legacy);
        assert_eq!(to_current(&current), current);
    }

    #[test]
    fn other_commands_are_left_alone() {
        for sysargs in [
            v(&["sync", "pipes", "--loop"]),
            v(&["start", "pipeline", "daily"]),
            v(&["chain"]),
            v(&["chain", "-P", "not json"]),
            v(&["chain", "-P", r#"{"sub_args_line": ""}"#]),
            v(&["start", "pipeline", "-P", "[1]"]),
        ] {
            assert_eq!(compress(&sysargs), sysargs);
            assert_eq!(to_legacy(&sysargs), sysargs);
            assert!(parse(&sysargs).is_none(), "{sysargs:?}");
        }
        let patched = v(&[
            "chain",
            "-P",
            r#"{"sub_args_line":"show version","patch_args":{"nopretty":true}}"#,
        ]);
        let parsed = parse(&patched).unwrap();
        assert_eq!(
            parsed.patch.and_then(|p| p.get("nopretty").cloned()),
            Some(Value::Bool(true))
        );
    }
}
