//! Shared validation for saved Pipeline definitions and Compose declarations.

/// Options a saved Pipeline may supply without changing its identity or membership.
pub const EXECUTION_DEFAULTS: &[&str] = &[
    "begin",
    "end",
    "workers",
    "retries",
    "min_seconds",
    "chunksize",
    "check_existing",
    "enforce_dtypes",
    "force",
    "params",
    "yes",
];

/// Validate the instance-local name accepted by the SDK and CLI.
pub fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || name.len() > 128
        || !name.as_bytes()[0].is_ascii_alphanumeric()
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
    {
        return Err("Use 1–128 letters, numbers, dots, underscores or hyphens, starting with a letter or number.".into());
    }
    Ok(())
}
