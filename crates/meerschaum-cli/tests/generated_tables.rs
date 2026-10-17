//! `native/packages_table.rs` and `native/bootstrap_templates.json` are generated from the Python
//! sources (`scripts/rust/gen_packages_table.py`, `scripts/rust/gen_bootstrap_templates.py`).
//! Both generators read the sources as text (no `meerschaum` import, no built extension), so
//! this check is cheap: it fails when a generated file is stale. Fails without `python3`; set `MRSM_TEST_ALLOW_NO_PYTHON=1` to skip it
//! (with a message on stderr) on a machine that has none.

use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    root.canonicalize().unwrap_or(root)
}

fn check(generator: &str, generated: &str) {
    let root = repo_root();
    let output = match Command::new("python3")
        .arg("-I")
        .arg(root.join("scripts/rust").join(generator))
        .output()
    {
        Ok(output) => output,
        Err(e) => {
            if std::env::var_os("MRSM_TEST_ALLOW_NO_PYTHON").is_some() {
                eprintln!("SKIPPED {generator}: python3 is unavailable ({e})");
                return;
            }
            panic!(
                "python3 is required to check {generated} against {generator} ({e}); \
                 set MRSM_TEST_ALLOW_NO_PYTHON=1 to skip this check"
            );
        }
    };
    assert!(
        output.status.success(),
        "{generator} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let path = root
        .join("crates/meerschaum-cli/src/native")
        .join(generated);
    let current = std::fs::read(&path).unwrap();
    assert!(
        current == output.stdout,
        "{generated} is stale: regenerate it with\n    python3 scripts/rust/{generator} > crates/meerschaum-cli/src/native/{generated}"
    );
}

#[test]
fn packages_table_matches_its_generator() {
    check("gen_packages_table.py", "packages_table.rs");
}

#[test]
fn bootstrap_templates_match_their_generator() {
    check("gen_bootstrap_templates.py", "bootstrap_templates.json");
}
