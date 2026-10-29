//! Build script for `meerschaum-py`.
//!
//! Records toolchain, profile, target and feature information for `_core.build_info()`.
//! (`static.toml` is embedded by `meerschaum-core`, which this crate depends on.)

use pyo3_build_config::PythonAbiKind;
use std::env;
use std::process::Command;

fn main() {
    println!("cargo::rerun-if-changed=build.rs");

    record_build_info();
}

fn record_build_info() {
    let rustc = env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let rustc_version = Command::new(&rustc)
        .arg("--version")
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let mut features: Vec<String> = env::vars_os()
        .filter_map(|(key, _)| {
            key.into_string()
                .ok()?
                .strip_prefix("CARGO_FEATURE_")
                .map(|feat| feat.to_lowercase().replace('_', "-"))
        })
        .collect();
    features.sort();

    // The ABI the extension targets: `abi3-py311` for the stable-ABI wheels (the minimum version
    // the `pyo3/abi3-py3XX` feature asks for, not the interpreter's), `abi3t-py3XX` for the
    // free-threaded stable ABI, `cp3XX` / `cp3XXt` for a version-specific build.
    let pyo3_config = pyo3_build_config::get();
    let target_abi = pyo3_config.target_abi();
    let version = target_abi.version();
    let python_abi = match target_abi.kind() {
        PythonAbiKind::Stable(stable) => {
            format!("{stable}-py{}{}", version.major, version.minor)
        }
        PythonAbiKind::VersionSpecific(_) => format!(
            "cp{}{}{}",
            version.major,
            version.minor,
            if target_abi.kind().is_free_threaded() {
                "t"
            } else {
                ""
            }
        ),
    };

    let vars = [
        ("MRSM_BUILD_RUSTC", rustc_version),
        (
            "MRSM_BUILD_PROFILE",
            env::var("PROFILE").unwrap_or_default(),
        ),
        (
            "MRSM_BUILD_OPT_LEVEL",
            env::var("OPT_LEVEL").unwrap_or_default(),
        ),
        ("MRSM_BUILD_DEBUG", env::var("DEBUG").unwrap_or_default()),
        ("MRSM_BUILD_TARGET", env::var("TARGET").unwrap_or_default()),
        ("MRSM_BUILD_HOST", env::var("HOST").unwrap_or_default()),
        ("MRSM_BUILD_FEATURES", features.join(",")),
        ("MRSM_BUILD_PYTHON_ABI", python_abi),
        (
            "MRSM_BUILD_PYTHON_IMPLEMENTATION",
            pyo3_config.implementation().to_string(),
        ),
    ];
    for (key, value) in vars {
        println!("cargo::rustc-env={key}={value}");
    }
}
