//! Build script for `meerschaum-py`.
//!
//! 1. Embeds `crates/meerschaum-core/static.toml` (D15) when it exists. The file is owned by
//!    `meerschaum-core` and may not exist yet this wave, so its absence is not an error: the
//!    `mrsm_static_toml` cfg is simply left unset and `_core.static_config()` returns `None`.
//! 2. Records toolchain, profile, target and feature information for `_core.build_info()`.

use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo::rustc-check-cfg=cfg(mrsm_static_toml)");
    println!("cargo::rerun-if-changed=build.rs");

    embed_static_toml();
    record_build_info();
}

fn embed_static_toml() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let core_dir = manifest_dir.join("..").join("meerschaum-core");
    let static_toml = core_dir.join("static.toml");

    if static_toml.is_file() {
        let path = static_toml
            .canonicalize()
            .unwrap_or_else(|_| static_toml.clone());
        println!("cargo::rustc-cfg=mrsm_static_toml");
        println!("cargo::rustc-env=MRSM_STATIC_TOML_PATH={}", path.display());
        println!("cargo::rerun-if-changed={}", path.display());
    } else if core_dir.is_dir() {
        // Watch the directory so the build script reruns once `static.toml` is created.
        println!("cargo::rerun-if-changed={}", core_dir.display());
    }
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

    let mut features: Vec<String> = env::vars()
        .filter_map(|(key, _)| {
            key.strip_prefix("CARGO_FEATURE_")
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
