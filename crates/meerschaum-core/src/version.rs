//! Shared PEP 440 version comparison for plugin registries and CLI upgrade checks.

/// Compare valid PEP 440 versions, including epochs, pre/dev/post releases and local versions.
/// Invalid versions compare false; callers needing an error can use [`parse_version`].
pub fn version_ge(a: &str, b: &str) -> bool {
    match (parse_version(a), parse_version(b)) {
        (Ok(a), Ok(b)) => a >= b,
        _ => false,
    }
}

/// Parse a version using the same ordering rules as Python's `packaging.version.Version`.
pub fn parse_version(value: &str) -> Result<pep440_rs::Version, pep440_rs::VersionParseError> {
    value.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pep440_ordering() {
        for (a, b) in [
            ("1.0", "1.0.0"),
            ("1!0", "9999"),
            ("1.0.post1", "1.0"),
            ("1.0rc1", "1.0b9"),
            ("1.0a1", "1.0.dev9"),
            ("1.0+1", "1.0+abc"),
            ("1.0.post1", "1.0.post1.dev1"),
            ("1.0a1", "1.0a1.dev1"),
            ("v1.0", "1.0"),
        ] {
            assert!(version_ge(a, b), "{a} >= {b}");
        }
        assert!(!version_ge("1.0rc1", "1.0"));
        assert!(!version_ge("invalid", "1.0"));
    }
}
