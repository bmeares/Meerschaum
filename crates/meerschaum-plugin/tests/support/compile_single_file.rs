use std::{path::PathBuf, process::Command};

pub fn compile(candidate: &meerschaum_plugin::discovery::Candidate) {
    let generated = candidate
        .build
        .as_ref()
        .unwrap()
        .manifest_path
        .parent()
        .unwrap();
    // Compile the generated runtime against the serialization crates already built by Cargo.
    // Legacy fixtures still link the full SDK to exercise the original Arrow ABI.
    // The real Cargo build and shared dependency cache are covered in single_file.rs.
    let deps = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let lightweight = std::fs::read_to_string(generated.join("lib.rs"))
        .unwrap()
        .contains("extern crate self");
    let dependency = if lightweight {
        "serde_json"
    } else {
        "meerschaum_plugin"
    };
    let sdk: PathBuf = std::fs::read_dir(&deps)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(&format!("lib{dependency}-"))
                && p.extension().is_some_and(|e| e == "rlib")
        })
        .max_by_key(|p| p.metadata().unwrap().modified().unwrap())
        .unwrap();
    let serde = std::fs::read_dir(&deps)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("libserde-")
                && p.extension().is_some_and(|e| e == "rlib")
        })
        .max_by_key(|p| p.metadata().unwrap().modified().unwrap())
        .unwrap();
    std::fs::create_dir_all(candidate.library.parent().unwrap()).unwrap();
    let output = Command::new("rustc")
        .args([
            "--edition=2024",
            "--crate-type=cdylib",
            "--crate-name=fixture",
        ])
        .arg(generated.join("lib.rs"))
        .arg("--extern")
        .arg(format!("{dependency}={}", sdk.display()))
        .arg("--extern")
        .arg(format!("serde={}", serde.display()))
        .arg("-L")
        .arg(format!("dependency={}", deps.display()))
        .arg("-o")
        .arg(&candidate.library)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
