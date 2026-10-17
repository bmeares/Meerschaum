//! Shared helpers for the integration tests.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use meerschaum_core::value::Value;
use meerschaum_jobs::{JobsContext, Launcher};

/// The crate's fixture directory.
pub fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

/// Load a JSON fixture.
pub fn load_fixture(name: &str) -> Value {
    let text = std::fs::read_to_string(fixtures_dir().join(name)).expect("fixture");
    Value::from_json_str(&text).expect("valid JSON")
}

/// The repository root.
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("repo root")
        .to_path_buf()
}

/// A context rooted at `root` whose jobs run `/bin/sh <sysargs>` through the crate's supervisor.
pub fn sh_context(root: &Path) -> JobsContext {
    let mut ctx = JobsContext::for_root(root);
    ctx.paths.systemd_user_dir = root
        .join("userhome")
        .join(".config")
        .join("systemd")
        .join("user");
    ctx.with_launcher(Launcher {
        supervisor: vec![env!("CARGO_BIN_EXE_mrsm-job-supervisor").to_string()],
        program: vec!["/bin/sh".to_string()],
    })
}

/// Poll `f` until it returns true or `secs` elapse.
pub fn wait_for(secs: f64, mut f: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs_f64(secs);
    while std::time::Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    false
}

/// Replace `YYYY-MM-DD HH:MM | ` prefixes with `2000-01-01 00:00 | `.
pub fn normalize_log_timestamps(text: &str) -> String {
    let re = regex::Regex::new(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2} \| ").unwrap();
    re.replace_all(text, "2000-01-01 00:00 | ").to_string()
}
