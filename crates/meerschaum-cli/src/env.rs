//! `MRSM_*` environment variables that change how parsed arguments behave.
//!
//! v4 does not write these into the arguments dict at parse time; each consumer checks the
//! environment (`meerschaum.utils.prompt.check_noask`, the `MRSM_NONINTERACTIVE` checks in the
//! actions, `meerschaum.config.environment.get_daemon_env_vars` in `entry`). The Rust dispatcher
//! applies them once, after parsing and before an action runs, via [`apply_env_arguments`];
//! `parse_arguments` itself stays environment-free so its output matches v4 key for key.

use crate::value::{ArgsMap, PyValue};

/// `STATIC_CONFIG['environment']['noask']`
pub const NOASK: &str = "MRSM_NOASK";
/// `STATIC_CONFIG['environment']['noninteractive']`
pub const NONINTERACTIVE: &str = "MRSM_NONINTERACTIVE";
/// `STATIC_CONFIG['environment']['daemon_id']`
pub const DAEMON_ID: &str = "MRSM_DAEMON_ID";
/// The systemd job variables (`get_daemon_env_vars`).
pub const DAEMON_ENV_VARS: &[&str] = &[
    "MRSM_SYSTEMD_LOG_PATH",
    "MRSM_SYSTEMD_RESULT_PATH",
    "MRSM_SYSTEMD_DELETE_JOB",
    "MRSM_SYSTEMD_STDIN_PATH",
    DAEMON_ID,
];

/// `check_noask(noask)`: `MRSM_NOASK` set to `1`/`true` (any case) flips `noask` on.
pub fn check_noask(noask: bool, get: impl Fn(&str) -> Option<String>) -> bool {
    if noask {
        return true;
    }
    matches!(
        get(NOASK)
            .unwrap_or_else(|| "false".into())
            .to_lowercase()
            .as_str(),
        "1" | "true"
    )
}

/// The actions' `MRSM_NONINTERACTIVE` check (`1`, `true` or `yes`, any case).
pub fn noninteractive(get: impl Fn(&str) -> Option<String>) -> bool {
    matches!(
        get(NONINTERACTIVE).map(|v| v.to_lowercase()).as_deref(),
        Some("1" | "true" | "yes")
    )
}

/// `get_daemon_env_vars()`: the job-related variables present in the environment.
pub fn daemon_env_vars(get: impl Fn(&str) -> Option<String>) -> Vec<(String, String)> {
    DAEMON_ENV_VARS
        .iter()
        .filter_map(|k| get(k).map(|v| (k.to_string(), v)))
        .collect()
}

/// Merge the environment into parsed arguments before an action runs: `noask` follows
/// [`check_noask`] and `noninteractive` is recorded when `MRSM_NONINTERACTIVE` is truthy.
pub fn apply_env_arguments(args: &mut ArgsMap, get: impl Fn(&str) -> Option<String>) {
    let noask = args.get("noask").is_some_and(PyValue::is_truthy);
    if !noask && check_noask(false, &get) {
        args.insert("noask".into(), PyValue::Bool(true));
    }
    if noninteractive(&get) {
        args.insert("noninteractive".into(), PyValue::Bool(true));
    }
}

/// Read a variable from the process environment.
pub fn process_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noask_from_env() {
        let env = |k: &str| (k == NOASK).then(|| "TRUE".to_string());
        assert!(check_noask(false, env));
        let none = |_: &str| None;
        assert!(!check_noask(false, none));
        let mut args = ArgsMap::new();
        apply_env_arguments(&mut args, |k: &str| (k == NOASK).then(|| "1".to_string()));
        assert_eq!(args.get("noask"), Some(&PyValue::Bool(true)));
        assert!(noninteractive(
            |k: &str| (k == NONINTERACTIVE).then(|| "yes".to_string())
        ));
    }
}
