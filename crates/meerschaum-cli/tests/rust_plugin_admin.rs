//! Rust sources use the same administration actions and repository protocol as Python.
#[path = "../../meerschaum-plugin/tests/support/compile_single_file.rs"]
mod compile_single_file;
mod plugin_lifecycle_support;

use plugin_lifecycle_support::{Home, Repo, install_native_demo};
use std::io::Read;

const NO_PYTHON: (&str, &str) = ("MRSM_PYTHON", "/nonexistent/mrsm-test-python");

fn prepare(home: &Home) {
    let builds = home.root.join("cache/native_build");
    let found = meerschaum_plugin::discovery::discover_report_in(
        std::slice::from_ref(&home.plugins),
        &builds,
    );
    assert!(found.skipped.is_empty(), "{:?}", found.skipped);
    for candidate in found.candidates {
        if candidate.build.is_none() {
            continue;
        }
        compile_single_file::compile(&candidate);
        std::fs::write(
            builds.join(&candidate.name).join("mrsm-source-fingerprint"),
            &candidate.fingerprint,
        )
        .unwrap();
    }
}

fn uploaded_entries(repo: &Repo, name: &str) -> Vec<(String, String)> {
    let upload = repo.upload(name).expect("uploaded archive");
    let start = upload.windows(2).position(|w| w == [0x1f, 0x8b]).unwrap();
    let decoder = flate2::read::GzDecoder::new(&upload[start..]);
    let mut archive = tar::Archive::new(decoder);
    archive
        .entries()
        .unwrap()
        .map(|entry| {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().to_string_lossy().into_owned();
            let mut source = String::new();
            entry.read_to_string(&mut source).unwrap();
            (path, source)
        })
        .collect()
}

#[test]
fn bootstrap_rust_generates_runnable_fetch_action_and_connector() {
    let home = Home::new();
    // Retry an invalid language, choose Rust, then select features.
    let run = home.run_input(
        &["bootstrap", "plugins", "readings"],
        "unknown\n2\nfetch,connector,action\nsay hello\n\n",
        &[("EDITOR", "true"), NO_PYTHON],
    );
    assert!(run.success, "{}", run.text());
    assert!(
        run.text().contains("Which language would you like to use"),
        "{}",
        run.text()
    );
    assert!(
        run.text().contains("Please pick a valid choice."),
        "{}",
        run.text()
    );
    let source = std::fs::read_to_string(home.plugins.join("readings.rs")).unwrap();
    assert!(
        source.contains("#[make_action(name = \"say_hello\")]"),
        "{source}"
    );
    assert!(
        source.contains("#[make_connector(name = \"readings\")]"),
        "{source}"
    );
    assert!(!home.plugins.join("readings.py").exists());
    prepare(&home);
    let run = home.run_with(&["say", "hello", "Alice"], &[NO_PYTHON]);
    assert!(
        run.success && run.text().contains("Hello, Alice!"),
        "{}",
        run.text()
    );
    let run = home.run_with(
        &[
            "register",
            "pipes",
            "-c",
            "readings:office",
            "-m",
            "readings",
            "-i",
            "sql:local",
            "--noask",
        ],
        &[NO_PYTHON],
    );
    assert!(run.success, "{}", run.text());
}

#[test]
#[cfg(unix)]
fn edit_rust_selects_native_source_over_shadowed_python() {
    let home = Home::new();
    home.write_plugin("same.rs", "#[make_action]\nfn greet() {}\n");
    home.write_plugin(
        "same.py",
        "raise RuntimeError('shadowed Python must not load')\n",
    );
    prepare(&home);
    let receipt = home.root.join("edited.rs");
    let receipt_path = receipt.to_string_lossy();
    // The editor copies its source argument into an owned receipt, without changing it.
    let editor = r#"sh -c 'cp "$1" "$MRSM_TEST_EDITOR_RECEIPT"' --"#;
    let run = home.run_input(
        &["edit", "plugins", "same"],
        "\n",
        &[
            ("EDITOR", editor),
            ("MRSM_TEST_EDITOR_RECEIPT", receipt_path.as_ref()),
            NO_PYTHON,
        ],
    );
    assert!(run.success, "{}", run.text());
    assert_eq!(
        std::fs::read(&receipt).unwrap(),
        std::fs::read(home.plugins.join("same.rs")).unwrap()
    );
    assert!(!home.root.join("same.py").exists());

    install_native_demo(&home.plugins);
    let run = home.run_input(
        &["edit", "plugins", "native_demo"],
        "\n",
        &[("EDITOR", editor), NO_PYTHON],
    );
    assert!(
        !run.success && run.text().contains("no editable Rust source"),
        "{}",
        run.text()
    );
    home.write_plugin("native_demo/Cargo.toml", "[package]\nname = 'native_demo'\nversion = '0.1.0'\n[lib]\ncrate-type = ['cdylib']\npath = 'custom.rs'\n");
    home.write_plugin(
        "native_demo/custom.rs",
        "//! An editable packaged plugin.\n",
    );
    install_native_demo(&home.plugins);
    let run = home.run_input(
        &["edit", "plugins", "native_demo"],
        "\n",
        &[
            ("EDITOR", editor),
            ("MRSM_TEST_EDITOR_RECEIPT", receipt_path.as_ref()),
            NO_PYTHON,
        ],
    );
    assert!(run.success, "{}", run.text());
    assert_eq!(
        std::fs::read_to_string(&receipt).unwrap(),
        "//! An editable packaged plugin.\n"
    );
}

#[test]
fn register_rust_uploads_sources_and_keeps_dependency_metadata() {
    let home = Home::new();
    let repo = Repo::start();
    let source = "//! A Rust source plugin.\n//!\n//! ```cargo\n//! [dependencies]\n//! serde_json = \"1\"\n//! ```\n#[make_action]\nfn greet() {}\n";
    home.write_plugin("same.rs", source);
    home.write_plugin("same.py", "__version__ = '99.0.0'\n");
    prepare(&home);
    let (key, value) = repo.connector("loop");
    let env = [(key.as_str(), value.as_str()), NO_PYTHON];
    let run = home.run_with(&["register", "plugins", "same", "-r", "api:loop"], &env);
    assert!(run.success, "{}", run.text());
    assert_eq!(
        uploaded_entries(&repo, "same"),
        vec![("same.rs".into(), source.into())]
    );
    let upload = repo.upload("same").unwrap();
    let body = String::from_utf8_lossy(&upload);
    assert!(body.contains("A+Rust+source+plugin."), "{body}");
    assert!(!body.contains("99.0.0"));

    // A shipped native library plus editable Cargo sources also remains a native archive.
    install_native_demo(&home.plugins);
    home.write_plugin("native_demo/Cargo.toml", "[package]\nname = 'native_demo'\nversion = '0.1.0'\n[lib]\ncrate-type = ['cdylib']\npath = 'custom.rs'\n");
    home.write_plugin("native_demo/custom.rs", "//! A packaged plugin.\n");
    home.write_plugin("native_demo/Cargo.lock", "# lockfile\n");
    home.write_plugin("native_demo/.cargo/config.toml", "[net]\noffline = true\n");
    home.write_plugin("native_demo/target/large-file", "excluded");
    home.write_plugin("native_demo/.git/config", "excluded");
    home.write_plugin("native_demo/.gitignore", "scratch.txt\n");
    home.write_plugin("native_demo/scratch.txt", "excluded");
    home.write_plugin("native_demo/.env", "excluded");
    home.write_plugin("native_demo/assets/message.txt", "included");
    #[cfg(unix)]
    std::os::unix::fs::symlink(
        home.plugins.join("same.rs"),
        home.plugins.join("native_demo/link.rs"),
    )
    .unwrap();
    // The shipped library is newer than the sources, so no build is needed.
    install_native_demo(&home.plugins);
    let run = home.run_with(
        &["register", "plugins", "native_demo", "-r", "api:loop"],
        &env,
    );
    assert!(run.success, "{}", run.text());
    // Read paths only: the real shipped library is binary.
    let upload = repo.upload("native_demo").unwrap();
    let start = upload.windows(2).position(|w| w == [0x1f, 0x8b]).unwrap();
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(&upload[start..]));
    let paths: Vec<_> = archive
        .entries()
        .unwrap()
        .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
        .collect();
    for required in [
        "plugin.toml",
        "Cargo.toml",
        "Cargo.lock",
        "custom.rs",
        ".cargo/config.toml",
        "assets/message.txt",
    ] {
        assert!(
            paths.contains(&format!("native_demo/{required}")),
            "{paths:?}"
        );
    }
    assert!(
        !paths.iter().any(|p| p.contains("/target/")
            || p.contains("/.git/")
            || p.ends_with("/scratch.txt")
            || p.ends_with("/.env")
            || p.ends_with("/link.rs")),
        "{paths:?}"
    );

    let gzip = &upload[start..];
    let mut decoder = flate2::bufread::GzDecoder::new(gzip);
    decoder.read_to_end(&mut Vec::new()).unwrap();
    let compressed_len = gzip.len() - decoder.get_ref().len();
    repo.publish("native_demo", gzip[..compressed_len].to_vec());
    let receiver = Home::new();
    let run = receiver.run_with(
        &["install", "plugins", "native_demo", "-r", "api:loop"],
        &env,
    );
    assert!(run.success, "{}", run.text());
    assert_eq!(
        std::fs::read_to_string(receiver.plugins.join("native_demo/custom.rs")).unwrap(),
        "//! A packaged plugin.\n"
    );
    assert!(
        receiver
            .plugins
            .join("native_demo/.cargo/config.toml")
            .exists()
    );
    let run = receiver.run_with(&["show", "plugins"], &[NO_PYTHON]);
    assert!(
        run.success && run.text().contains("native_demo"),
        "{}",
        run.text()
    );
}
