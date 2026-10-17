//! `_core.__version__` and `_core.build_info()`.

use pyo3::prelude::*;
use pyo3::types::PyDict;

/// The Cargo package version (workspace version, e.g. `5.0.0-a1`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Compile-time facts about this build of the extension, recorded by `build.rs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildInfo {
    pub version: &'static str,
    pub version_pep440: String,
    pub rustc: &'static str,
    pub profile: &'static str,
    pub opt_level: &'static str,
    pub debug_assertions: bool,
    pub target: &'static str,
    pub host: &'static str,
    pub features: Vec<&'static str>,
    pub python_abi: &'static str,
    pub python_implementation: &'static str,
    pub static_config: bool,
}

/// Collect the build facts recorded by `build.rs`.
pub fn build_info() -> BuildInfo {
    let features = env!("MRSM_BUILD_FEATURES");
    BuildInfo {
        version: VERSION,
        version_pep440: pep440(VERSION),
        rustc: env!("MRSM_BUILD_RUSTC"),
        profile: env!("MRSM_BUILD_PROFILE"),
        opt_level: env!("MRSM_BUILD_OPT_LEVEL"),
        debug_assertions: cfg!(debug_assertions),
        target: env!("MRSM_BUILD_TARGET"),
        host: env!("MRSM_BUILD_HOST"),
        features: features.split(',').filter(|f| !f.is_empty()).collect(),
        python_abi: env!("MRSM_BUILD_PYTHON_ABI"),
        python_implementation: env!("MRSM_BUILD_PYTHON_IMPLEMENTATION"),
        static_config: crate::static_config::STATIC_TOML.is_some(),
    }
}

/// Convert a Cargo (semver) version into the PEP 440 form maturin writes into the wheel
/// metadata: `5.0.0-a1` -> `5.0.0a1`, `5.0.0-alpha.1` -> `5.0.0a1`, `5.0.0-rc.2` -> `5.0.0rc2`,
/// `5.0.0-dev3` -> `5.0.0.dev3`. Unrecognized pre-release tags are returned unchanged.
pub fn pep440(version: &str) -> String {
    let Some((release, pre)) = version.split_once('-') else {
        return version.to_string();
    };
    let pre = pre.replace('.', "");
    let (tag, num) = pre.split_at(pre.find(|c: char| c.is_ascii_digit()).unwrap_or(pre.len()));
    let num = if num.is_empty() { "0" } else { num };
    match tag {
        "a" | "alpha" => format!("{release}a{num}"),
        "b" | "beta" => format!("{release}b{num}"),
        "rc" | "c" => format!("{release}rc{num}"),
        "dev" => format!("{release}.dev{num}"),
        _ => version.to_string(),
    }
}

/// `_core.build_info() -> dict`: toolchain, profile, target, features and Python ABI.
#[pyfunction(name = "build_info")]
pub fn py_build_info(py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
    let info = build_info();
    let dict = PyDict::new(py);
    dict.set_item("version", info.version)?;
    dict.set_item("version_pep440", info.version_pep440)?;
    dict.set_item("rustc", info.rustc)?;
    dict.set_item("profile", info.profile)?;
    dict.set_item("opt_level", info.opt_level)?;
    dict.set_item("debug_assertions", info.debug_assertions)?;
    dict.set_item("target", info.target)?;
    dict.set_item("host", info.host)?;
    dict.set_item("features", info.features)?;
    dict.set_item("python_abi", info.python_abi)?;
    dict.set_item("python_implementation", info.python_implementation)?;
    dict.set_item("static_config", info.static_config)?;
    Ok(dict)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pep440_conversions() {
        assert_eq!(pep440("5.0.0"), "5.0.0");
        assert_eq!(pep440("5.0.0-a1"), "5.0.0a1");
        assert_eq!(pep440("5.0.0-alpha.1"), "5.0.0a1");
        assert_eq!(pep440("5.0.0-beta.2"), "5.0.0b2");
        assert_eq!(pep440("5.0.0-rc.3"), "5.0.0rc3");
        assert_eq!(pep440("5.0.0-dev4"), "5.0.0.dev4");
        assert_eq!(pep440("5.0.0-a"), "5.0.0a0");
        assert_eq!(pep440("5.0.0-weird"), "5.0.0-weird");
    }

    #[test]
    fn build_info_is_populated() {
        let info = build_info();
        assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
        assert!(info.rustc.starts_with("rustc "), "{}", info.rustc);
        assert!(!info.profile.is_empty());
        assert!(!info.target.is_empty());
        assert!(
            info.python_abi.starts_with("abi3-py3"),
            "{}",
            info.python_abi
        );
        assert_eq!(info.features.contains(&"polars"), cfg!(feature = "polars"));
    }
}
