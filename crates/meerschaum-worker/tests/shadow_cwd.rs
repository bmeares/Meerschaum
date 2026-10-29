//! A worker runs in the user's working directory, which must never shadow the package: a project
//! with its own `meerschaum/` (or `meerschaum.py`) would otherwise replace the installed one,
//! because `python -c` and `python -m` put the working directory first on `sys.path`. The worker
//! starts with `-P`, as the `mrsm` console script always behaved (v4: `sys.path[0]` is the scripts
//! directory). Plugins are imported through the plugins directory, never the working one.

mod common;

use std::process::Command;

use meerschaum_worker::Worker;

use common::{TestEnv, python};

fn shadow_dir() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("mrsm-wk-shadow-")
        .tempdir()
        .unwrap();
    let package = dir.path().join("meerschaum");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::write(
        package.join("__init__.py"),
        "raise RuntimeError('SHADOWED: the working directory replaced the package')\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("meerschaum.py"),
        "raise RuntimeError('SHADOWED')\n",
    )
    .unwrap();
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shadowing_package_in_the_working_directory_is_not_imported_by_a_worker() {
    require_python!();
    let env = TestEnv::new();
    let shadow = shadow_dir();

    // The control: without `-P` the shadow wins over `PYTHONPATH`, so the test means something.
    let mut control = Command::new(python());
    control
        .args(["-c", "import meerschaum"])
        .current_dir(shadow.path());
    for (k, v) in env.env() {
        control.env(k, v);
    }
    let out = control.output().expect("python runs");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("SHADOWED"),
        "the shadowing package was not picked up without -P:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let mut opts = env.worker_options();
    opts.current_dir = Some(shadow.path().to_path_buf());
    let worker = Worker::spawn(&opts).await.expect(
        "a worker started in a directory with a shadowing `meerschaum/` imports the real one",
    );
    assert!(!worker.hello().meerschaum.is_empty());
    worker.shutdown(std::time::Duration::from_secs(5)).await;
}
