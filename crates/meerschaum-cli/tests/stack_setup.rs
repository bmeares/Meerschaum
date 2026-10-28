//! Wave 17c (K1): `mrsm stack` writes the database setup script before it runs Docker Compose,
//! because the compose file mounts it as a FILE
//! (`<root>/db/initdb/create-extensions.sql:/docker-entrypoint-initdb.d/100_meerschaum.sql`):
//! a bind-mount source that does not exist would be created as a directory.
//!
//! `docker` is hidden from `PATH`, so the action stops at "Failed to connect to the Docker
//! engine" after writing its files: no Docker command ever runs.
#![cfg(unix)]

use std::process::{Command, Stdio};

use meerschaum_core::config::sync::CREATE_EXTENSIONS_TEXT;

fn mrsm_stack(root: &std::path::Path, plugins: &std::path::Path) -> (i32, String) {
    mrsm(root, plugins, &["stack", "config"])
}

/// `mrsm <args>` against `root` with no `docker` (nor anything else) on `PATH` and no Python.
fn mrsm(root: &std::path::Path, plugins: &std::path::Path, args: &[&str]) -> (i32, String) {
    let empty_path = root.join("no-bin");
    std::fs::create_dir_all(&empty_path).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
        .env_clear()
        .env("PATH", &empty_path)
        .env("HOME", root)
        .env("MRSM_ROOT_DIR", root)
        .env("MRSM_PLUGINS_DIR", plugins)
        .env("MRSM_PYTHON", root.join("no-python"))
        .args(args)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.code().unwrap_or(-1), text)
}

#[test]
fn stack_writes_the_setup_file_it_mounts_before_running_compose() {
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let (code, text) = mrsm_stack(root.path(), plugins.path());
    assert_ne!(code, 0, "{text}");
    assert!(
        text.contains("Failed to connect to the Docker engine."),
        "{text}"
    );

    let setup = root
        .path()
        .join("db")
        .join("initdb")
        .join("create-extensions.sql");
    assert_eq!(
        std::fs::read_to_string(&setup).unwrap(),
        CREATE_EXTENSIONS_TEXT
    );
    let compose =
        std::fs::read_to_string(root.path().join("stack").join("docker-compose.yaml")).unwrap();
    let mount = format!(
        "- {}:/docker-entrypoint-initdb.d/100_meerschaum.sql:z,ro\n",
        setup.to_string_lossy()
    );
    assert!(compose.contains(&mount), "{compose}");
    assert!(
        compose.contains("- pg_isready -h 127.0.0.1 -p 5432 -d $POSTGRES_DB -U $POSTGRES_USER\n"),
        "{compose}"
    );
    assert!(compose.contains("start_period: 300s\n"), "{compose}");

    // A missing file is written again before the next compose command; a customized one is kept.
    std::fs::remove_file(&setup).unwrap();
    let (_, text) = mrsm_stack(root.path(), plugins.path());
    assert_eq!(
        std::fs::read_to_string(&setup).unwrap(),
        CREATE_EXTENSIONS_TEXT,
        "{text}"
    );
    std::fs::write(&setup, "-- mine\n").unwrap();
    mrsm_stack(root.path(), plugins.path());
    assert_eq!(std::fs::read_to_string(&setup).unwrap(), "-- mine\n");
}

/// `mrsm stack` (twice) on an upgraded v4.1.4 root: v4's `meerschaum.yaml`, `stack.yaml` and
/// `system.json` (recorded by the core crate's `tests/fixtures/gen_v4_root.py`, with v4's own
/// result of `mrsm stack config` on them). The files stay byte-identical, and the compose `db`
/// service is the one v4 generates: the v4 directory mount and healthcheck, no `start_period`, and
/// no key the user removed comes back (`edited`: `retries` deleted). v4's `system.json` lacks the
/// v5-only `system:engine` defaults, whose lookups used to deep-merge every default into the
/// loaded config (PARITY.md W17K1-006). No parity overlay.
#[test]
fn stack_keeps_the_compose_section_of_a_v4_stack_yaml() {
    fn db_service(compose: &str) -> String {
        let start = compose.find("\n  db:\n").expect("a db service");
        let end = compose[start + 1..]
            .find("\n  api:\n")
            .map(|i| start + 1 + i)
            .expect("an api service after db");
        compose[start..end].to_string()
    }
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../meerschaum-core/tests/fixtures/v4_root.json");
    let fixture: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(fixture).unwrap()).unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let root_text = root.path().to_string_lossy().into_owned();
        let config_dir = root.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let before = case["before"]["config_files"].as_object().unwrap();
        for (file, text) in before {
            let text = text.as_str().unwrap().replace("{ROOT}", &root_text);
            std::fs::write(config_dir.join(file), text).unwrap();
        }
        let compose_path = root.path().join("stack").join("docker-compose.yaml");
        std::fs::create_dir_all(compose_path.parent().unwrap()).unwrap();
        std::fs::write(
            &compose_path,
            case["before"]["compose"]
                .as_str()
                .unwrap()
                .replace("{ROOT}", &root_text),
        )
        .unwrap();

        let mut text = String::new();
        for _ in 0..2 {
            text = mrsm_stack(root.path(), plugins.path()).1;
        }
        let after = case["after"]["config_files"].as_object().unwrap();
        for file in before.keys() {
            let want = after[file].as_str().unwrap().replace("{ROOT}", &root_text);
            let got = std::fs::read_to_string(config_dir.join(file)).unwrap();
            // Q54 pins upgraded roots to pandas once; every pre-existing byte is preserved.
            let want = if file == "meerschaum.yaml" {
                format!(
                    "{want}{}dataframe: pandas\n",
                    if want.ends_with('\n') { "" } else { "\n" }
                )
            } else {
                want
            };
            assert_eq!(got, want, "case {name}: {file}\n{text}");
        }
        let compose = std::fs::read_to_string(&compose_path).unwrap();
        let want = case["after"]["compose"]
            .as_str()
            .unwrap()
            .replace("{ROOT}", &root_text);
        assert_eq!(db_service(&compose), db_service(&want), "case {name}");
        assert!(
            compose.contains(&format!(
                "- {root_text}/db/initdb:/docker-entrypoint-initdb.d:z,ro\n"
            )),
            "{compose}"
        );
        assert!(!compose.contains("start_period"), "case {name}: {compose}");
        // The setup script is still written (the directory mount runs it on a fresh volume).
        assert_eq!(
            std::fs::read_to_string(root.path().join("db/initdb/create-extensions.sql")).unwrap(),
            CREATE_EXTENSIONS_TEXT
        );
    }
}

/// The typical commands v4.1.4 ran on the same upgraded root (`fixtures/v4_root.json`,
/// `commands`): v5 running them leaves every config file v4 wrote byte-identical to what v4
/// leaves. (v5 may write files v4 does not, e.g. `pipes.json`.)
#[test]
fn typical_commands_leave_an_upgraded_v4_root_as_v4_does() {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../meerschaum-core/tests/fixtures/v4_root.json");
    let fixture: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(fixture).unwrap()).unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let root_text = root.path().to_string_lossy().into_owned();
        let config_dir = root.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        // Start from the root as v4 left it after its own `stack config` runs.
        for (file, text) in case["after"]["config_files"].as_object().unwrap() {
            let text = text.as_str().unwrap().replace("{ROOT}", &root_text);
            std::fs::write(config_dir.join(file), text).unwrap();
        }
        let mut log = String::new();
        for command in case["commands"].as_array().unwrap() {
            let words: Vec<&str> = command.as_str().unwrap().split(' ').collect();
            let (code, text) = mrsm(root.path(), plugins.path(), &words);
            log.push_str(&format!("$ mrsm {} -> {code}\n{text}\n", words.join(" ")));
        }
        for (file, text) in case["after_commands"]["config_files"].as_object().unwrap() {
            let want = text.as_str().unwrap().replace("{ROOT}", &root_text);
            let got = std::fs::read_to_string(config_dir.join(file)).unwrap();
            let want = if file == "meerschaum.yaml" {
                format!(
                    "{want}{}dataframe: pandas\n",
                    if want.ends_with('\n') { "" } else { "\n" }
                )
            } else {
                want
            };
            assert_eq!(got, want, "case {name}: {file}\n{log}");
        }
    }
}
