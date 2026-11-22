//! Real interpreter cache-substitution regression using only an owned synthetic module.
//! The actual native Metadata/Factory process fixture separately proves kernel/source/key
//! admission; this verifies the same startup flags used by that native interpreter launch.
use super::*;
use std::path::PathBuf;
use std::process::{Command, Stdio};

fn python() -> Command {
    let mut command = Command::new("/usr/bin/python3");
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .stdin(Stdio::null())
        .args(["-I", "-S"]);
    command
}

fn output(mut command: Command) -> Vec<u8> {
    let output = command
        .output()
        .expect("The fixed fixture interpreter is unavailable.");
    assert!(
        output.status.success(),
        "Fixture interpreter refused: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.len() <= 4096 && output.stderr.len() <= 4096);
    output.stdout
}

#[test]
fn constructor_selected_empty_prefix_ignores_actual_poisoned_source_tree_bytecode() {
    let root = tempfile::tempdir().unwrap();
    let sources = root.path().join("synthetic-sdk");
    let generation = root.path().join("generation");
    let prefix = generation.join("sdk-pycache");
    std::fs::create_dir_all(&sources).unwrap();
    std::fs::create_dir_all(&prefix).unwrap();
    let mut prepare = python();
    prepare
        .args([
            "-c",
            r#"
import importlib.util, marshal, pathlib, py_compile, sys
source = pathlib.Path(sys.argv[1]) / 'attested_sdk_probe.py'
source.write_text("VALUE = 'source'\n")
cache = pathlib.Path(importlib.util.cache_from_source(str(source)))
py_compile.compile(str(source), cfile=str(cache), doraise=True)
# Preserve the legitimate source timestamp/size header while replacing executable code.
header = cache.read_bytes()[:16]
cache.write_bytes(header + marshal.dumps(compile("VALUE = 'poison'\n", str(source), 'exec')))
print(str(cache))
"#,
        ])
        .arg(&sources);
    let cache = PathBuf::from(String::from_utf8(output(prepare)).unwrap().trim());
    let poisoned = std::fs::read(&cache).unwrap();
    let script = "import sys; sys.path.insert(0,sys.argv[1]); import attested_sdk_probe; print(attested_sdk_probe.VALUE)";
    let mut ordinary = python();
    ordinary.args(["-c", script]).arg(&sources);
    assert_eq!(
        output(ordinary),
        b"poison\n",
        "The control must prove a usable poisoned cache."
    );

    let mut attested = python();
    configure_sdk_cache(&mut attested, true, Some(&prefix)).unwrap();
    // The ignored environment cannot override the sealed command-line selection.
    attested
        .env("PYTHONPYCACHEPREFIX", &sources)
        .args(["-c", script])
        .arg(&sources);
    assert_eq!(output(attested), b"source\n");
    assert_eq!(
        std::fs::read(&cache).unwrap(),
        poisoned,
        "The native path must not repair or mutate source/owner caches."
    );
    assert_eq!(
        std::fs::read_dir(prefix).unwrap().count(),
        0,
        "The per-generation cache stays empty."
    );
}

#[test]
fn absent_prefix_keeps_ordinary_command_and_unsealed_or_relative_selection_refuses() {
    let mut command = python();
    let before = command
        .get_args()
        .map(|value| value.to_os_string())
        .collect::<Vec<_>>();
    configure_sdk_cache(&mut command, false, None).unwrap();
    assert_eq!(
        command
            .get_args()
            .map(|value| value.to_os_string())
            .collect::<Vec<_>>(),
        before
    );
    assert!(configure_sdk_cache(&mut command, false, Some(Path::new("/owned/prefix"))).is_err());
    assert!(configure_sdk_cache(&mut command, true, Some(Path::new("relative"))).is_err());
}
