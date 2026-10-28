//! The current Python worker's interpreter; `.venv-ref` belongs to released-v4 comparisons.

use std::path::{Path, PathBuf};
use std::process::Command;

pub fn current(override_env: &str) -> Option<PathBuf> {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let root = tempfile::tempdir().unwrap();
    std::env::var_os(override_env)
        .map(PathBuf::from)
        .into_iter()
        .chain([
            repo.join(".venv/bin/python"),
            meerschaum_worker::worker::default_python(),
        ])
        .find_map(|python| {
            let output = Command::new(python)
                .args(["-c", "import sys; print(sys.executable)"])
                .env_clear()
                .env("PATH", std::env::var_os("PATH").unwrap_or_default())
                .env("MRSM_ROOT_DIR", root.path())
                .current_dir(&repo)
                .output()
                .ok()?;
            if !output.status.success() {
                return None;
            }
            let python = PathBuf::from(String::from_utf8(output.stdout).ok()?.trim());
            python.is_file().then_some(python)
        })
}
