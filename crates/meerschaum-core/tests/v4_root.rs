//! An upgraded v4.1.4 root (`fixtures/v4_root.json`, recorded by `fixtures/gen_v4_root.py`):
//! v4's `meerschaum.yaml`, `stack.yaml` and `system.json`, and what v4's own `mrsm stack` does
//! with them. v5's commands must leave the files as they are and generate the `db` service v4
//! generates: no v5 default may be filled into a key the user's files have.
//!
//! The trigger this guards against (PARITY.md W17K1-006): v4's `system.json` lacks the v5-only
//! `system:engine` defaults, and most v5 commands look those up. The registry's fallback for a
//! missing key deep-merges EVERY default into every loaded key (v4's `get_config` does the same,
//! but v4 never looks these keys up), which filled `start_period` (and a `retries` the user had
//! deleted) into `stack.yaml`'s compose section and recreated the user's `db` container.
//! No parity overlay is applied here.

use meerschaum_core::Value;
use meerschaum_core::config::paths::posix;
use meerschaum_core::config::{Config, ConfigOptions, GetOptions};

fn fixture() -> serde_json::Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("v4_root.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn registry(root: &std::path::Path) -> Config {
    let env = vec![("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned())];
    let mut opts = ConfigOptions::with_env(env);
    opts.allow_writes = true;
    Config::new(opts).unwrap()
}

/// The `db` service block of a generated compose file.
fn db_service(compose: &str) -> String {
    let start = compose.find("\n  db:\n").expect("a db service");
    let end = compose[start + 1..]
        .find("\n  api:\n")
        .map(|i| start + 1 + i)
        .expect("an api service after db");
    compose[start..end].to_string()
}

/// What v5 commands look up besides the stack files: the engine and worker settings (v5-only
/// keys) and the usual v4 keys.
fn typical_v5_lookups(config: &Config) {
    let opts = GetOptions {
        patch: true,
        substitute: true,
        write_missing: true,
        warn: false,
    };
    for keys in [
        &["system", "engine", "python_workers"][..],
        &["system", "engine", "workers"],
        &["system", "engine", "limits", "connectors"],
        &["system", "engine", "implicit_edges"],
        &["meerschaum", "instance"],
        &["meerschaum", "connectors", "sql", "main"],
        &["stack", "project_name"],
    ] {
        config.get_with(keys, opts);
    }
}

#[test]
fn an_upgraded_v4_root_keeps_its_files_and_its_db_service() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 2);
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap().join("root");
        std::fs::create_dir_all(&root).unwrap();
        let root_text = posix(&root);
        let config = registry(&root);
        let paths = config.paths();
        std::fs::create_dir_all(&paths.config_dir).unwrap();
        let before = case["before"]["config_files"].as_object().unwrap();
        for (file, text) in before {
            let text = text.as_str().unwrap().replace("{ROOT}", &root_text);
            std::fs::write(paths.config_dir.join(file), text).unwrap();
        }
        let compose_path = paths.path("STACK_COMPOSE_PATH");
        std::fs::create_dir_all(compose_path.parent().unwrap()).unwrap();
        let before_compose = case["before"]["compose"]
            .as_str()
            .unwrap()
            .replace("{ROOT}", &root_text);
        std::fs::write(&compose_path, &before_compose).unwrap();

        // Two `mrsm stack` runs' worth of work in one process (lookups, then the stack files).
        for _ in 0..2 {
            typical_v5_lookups(&config);
            config
                .with_state(|s| s.sync_files(&["stack"]))
                .expect("sync");
        }

        // The user's files are byte-identical to what v4 leaves (v4 may add other files).
        let after = case["after"]["config_files"].as_object().unwrap();
        for file in before.keys() {
            let want = after[file].as_str().unwrap().replace("{ROOT}", &root_text);
            let got = std::fs::read_to_string(paths.config_dir.join(file)).unwrap();
            assert_eq!(got, want, "case {name}: {file}");
        }
        // The `db` service v4 generates from the same `stack.yaml`.
        let compose = std::fs::read_to_string(&compose_path).unwrap();
        let want_compose = case["after"]["compose"]
            .as_str()
            .unwrap()
            .replace("{ROOT}", &root_text);
        assert_eq!(
            db_service(&compose),
            db_service(&want_compose),
            "case {name}"
        );
        assert!(!compose.contains("start_period"), "case {name}: {compose}");
        assert!(!compose.contains("100_meerschaum.sql"), "case {name}");
        if name == "edited" {
            assert_eq!(
                db_service(&compose).matches("retries:").count(),
                0,
                "a removed key came back: {compose}"
            );
        }

        // In memory too: every loaded section is what its file says (nothing merged in), apart
        // from the v5-only `system:engine` defaults, which are never stored.
        let raw = GetOptions {
            patch: false,
            substitute: false,
            write_missing: false,
            warn: false,
        };
        for (key, file) in [
            ("stack", "stack.yaml"),
            ("meerschaum", "meerschaum.yaml"),
            ("system", "system.json"),
        ] {
            let (_, in_memory) = config.get_with(&[key], raw);
            let from_file = config.with_state(|s| {
                s.read_config(
                    Some(&paths.config_dir),
                    Some(&[key.to_string()]),
                    false,
                    true,
                    false,
                )
                .map(|(c, _)| c.get(key).cloned())
            });
            let from_file: Option<Value> = from_file.ok().flatten();
            assert_eq!(
                Some(in_memory),
                from_file,
                "case {name}: in-memory '{key}' ({file})"
            );
        }
    }
}
