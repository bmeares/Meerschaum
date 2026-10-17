//! Build script for `meerschaum-py`.
//!
//! Records toolchain, profile, target and feature information for `_core.build_info()`.
//! (`static.toml` is embedded by `meerschaum-core`, which this crate depends on.)

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

    let pyo3_config = pyo3_build_config::get();
    let python_abi = if pyo3_config.abi3 {
        format!(
            "abi3-py{}{}",
            pyo3_config.version.major, pyo3_config.version.minor
        )
    } else {
        format!(
            "cp{}{}",
            pyo3_config.version.major, pyo3_config.version.minor
        )
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
            pyo3_config.implementation.to_string(),
        ),
    ];
    for (key, value) in vars {
        println!("cargo::rustc-env={key}={value}");
    }
}
