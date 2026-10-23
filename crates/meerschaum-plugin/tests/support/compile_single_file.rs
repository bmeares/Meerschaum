use std::{path::PathBuf, process::Command};

pub fn compile(candidate: &meerschaum_plugin::discovery::Candidate) {
    let generated = candidate
        .build
        .as_ref()
        .unwrap()
        .manifest_path
        .parent()
        .unwrap();
    // Compile against the SDK Cargo just built, reusing the workspace's dependencies.
    // This exercises the exact generated source without downloading a published SDK or
    // building another copy of Polars in a scratch target directory.
    let deps = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let sdk: PathBuf = std::fs::read_dir(&deps)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("libmeerschaum_plugin-")
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
        .arg(format!("meerschaum_plugin={}", sdk.display()))
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
