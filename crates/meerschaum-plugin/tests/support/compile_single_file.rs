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
    // Several builds may leave rlibs of the same crate (other features, other serde_core
    // versions) in the shared deps directory: try the newest pairs until one links.
    let rlibs = |prefix: &str| -> Vec<PathBuf> {
        let mut found: Vec<PathBuf> = std::fs::read_dir(&deps)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(&format!("lib{prefix}-"))
                    && p.extension().is_some_and(|e| e == "rlib")
            })
            .collect();
        found.sort_by_key(|p| std::cmp::Reverse(p.metadata().unwrap().modified().unwrap()));
        found
    };
    std::fs::create_dir_all(candidate.library.parent().unwrap()).unwrap();
    let mut first_error = None;
    for sdk in rlibs(dependency).iter().take(4) {
        for serde in rlibs("serde").iter().take(4) {
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
            if output.status.success() {
                return;
            }
            first_error.get_or_insert(String::from_utf8_lossy(&output.stderr).into_owned());
        }
    }
    panic!("{}", first_error.unwrap_or_default());
}
