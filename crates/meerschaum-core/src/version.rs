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

/// Whether `version` satisfies a PEP 440 specifier set such as `>=2.0.0` or `>=1.2,<2`.
/// Returns `None` when either side does not parse, so callers can tell "unknown" from "no".
pub fn version_satisfies(version: &str, specifiers: &str) -> Option<bool> {
    let version = parse_version(version).ok()?;
    let specifiers: pep440_rs::VersionSpecifiers = specifiers.trim().parse().ok()?;
    Some(specifiers.contains(&version))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn specifier_matching() {
        assert_eq!(version_satisfies("3.0.2", ">=3.0.2"), Some(true));
        assert_eq!(version_satisfies("3.0.1", ">=3.0.2"), Some(false));
        assert_eq!(version_satisfies("1.5", ">=1.2,<2"), Some(true));
        assert_eq!(version_satisfies("2.0.0rc1", ">=2.0.0rc1"), Some(true));
        assert_eq!(version_satisfies("2.0.0-rc1", ">=2.0.0"), Some(false));
        assert_eq!(version_satisfies("invalid", ">=1.0"), None);
        assert_eq!(version_satisfies("1.0", "not a specifier"), None);
    }

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
