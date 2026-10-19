//! The API host starts behind the version guard (`meerschaum_core::python_guard`): the launcher
//! runs `python -c <guard + runpy>` with the expected version and protocol in its environment, and
//! a host that refuses to start (exit code 3) is not restarted and not waited for.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use meerschaum_core::python_guard;
use meerschaum_server::host::{
    HostLauncher, HostManifest, HostSupervisor, PythonHostLauncher, SupervisorOptions,
};

/// A fake `python` that records its arguments and the guard's environment, then exits `code`.
fn fake_python(dir: &Path, code: i32) -> PathBuf {
    let script = dir.join("fake-python");
    let tmp = dir.join("fake-python.tmp");
    std::fs::write(
        &tmp,
        format!(
            "#!/bin/sh\nprintf '%s\\0' \"$@\" > '{args}'\nprintf '%s|%s|%s|%s' \"$MRSM_WORKER_EXPECT_VERSION\" \
             \"$MRSM_WORKER_EXPECT_PROTOCOL\" \"$MRSM_GUARD_WARN\" \"$MRSM_GUARD_RETRY_HINT\" > '{env}'\nexit {code}\n",
            args = dir.join("args").display(),
            env = dir.join("env").display(),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::rename(&tmp, &script).unwrap();
    script
}

#[tokio::test]
async fn the_host_is_launched_behind_the_guard() {
    let dir = tempfile::tempdir().unwrap();
    let launcher = PythonHostLauncher {
        python: fake_python(dir.path(), 0),
        env: Vec::new(),
    };
    let socket = dir.path().join("host.sock");
    let mut process = launcher.launch(&socket).await.unwrap();
    assert_eq!(process.wait().await.unwrap(), Some(0));
    let args = std::fs::read_to_string(dir.path().join("args")).unwrap();
    let args: Vec<&str> = args.split('\0').filter(|a| !a.is_empty()).collect();
    assert_eq!(args[0], "-P");
    assert_eq!(args[1], "-c");
    // The program: the guard, its call, then the module as `-m` would run it.
    let program = args[2];
    assert!(program.contains("def _mrsm_guard()"));
    assert!(program.contains("runpy.run_module('meerschaum.api.host'"));
    assert!(program.find("_mrsm_guard()\nimport runpy").is_some());
    assert!(!args.contains(&"-m"), "{args:?}");
    assert_eq!(args[args.len() - 2], "--socket");
    assert_eq!(args[args.len() - 1], socket.display().to_string());
    let env = std::fs::read_to_string(dir.path().join("env")).unwrap();
    let fields: Vec<&str> = env.splitn(4, '|').collect();
    assert_eq!(fields[0], python_guard::expected_version());
    assert_eq!(fields[1], python_guard::WORKER_PROTOCOL_VERSION.to_string());
    assert_eq!(fields[2], "1");
    // A running server cannot be told to `reload` from the CLI: its message says what to do.
    assert!(
        fields[3].contains("restart `mrsm start api`"),
        "{}",
        fields[3]
    );
    assert!(!fields[3].contains("mrsm reload"));
}

#[tokio::test]
async fn a_refused_host_is_neither_restarted_nor_waited_for() {
    let dir = tempfile::tempdir().unwrap();
    let launcher = PythonHostLauncher {
        python: fake_python(dir.path(), python_guard::REFUSED_EXIT_CODE),
        env: Vec::new(),
    };
    let manifest: HostManifest = serde_json::from_str(r#"{"api_plugins": ["portal"]}"#).unwrap();
    let supervisor = HostSupervisor::new(
        &manifest,
        Arc::new(launcher),
        dir.path().join("host.sock"),
        SupervisorOptions {
            // A long wait: a refusal must end it at once.
            ready_timeout: Duration::from_secs(60),
            backoff_initial: Duration::from_millis(50),
            backoff_max: Duration::from_millis(100),
        },
    );
    let started = Instant::now();
    let result = supervisor.ensure_started().await;
    assert!(result.is_err());
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "waited {:?}",
        started.elapsed()
    );
    // Give a (wrong) restart loop time to show itself.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(supervisor.starts(), 1, "the refused host was restarted");
    assert!(!supervisor.is_running().await);
}

fn supervisor_for(dir: &Path, code: i32, ready_timeout: Duration) -> Arc<HostSupervisor> {
    let launcher = PythonHostLauncher {
        python: fake_python(dir, code),
        env: Vec::new(),
    };
    let manifest: HostManifest = serde_json::from_str(r#"{"api_plugins": ["portal"]}"#).unwrap();
    HostSupervisor::new(
        &manifest,
        Arc::new(launcher),
        dir.join("host.sock"),
        SupervisorOptions {
            ready_timeout,
            backoff_initial: Duration::from_millis(50),
            backoff_max: Duration::from_millis(200),
        },
    )
}

/// Exit code 3 is uvicorn's `STARTUP_FAILURE` (a plugin's startup hook raised, the socket could not
/// be bound), not the guard's refusal code: the host is restarted with backoff, as for any crash.
#[tokio::test]
async fn a_uvicorn_startup_failure_is_restarted_with_backoff_and_not_a_refusal() {
    let dir = tempfile::tempdir().unwrap();
    assert_ne!(python_guard::REFUSED_EXIT_CODE, 3);
    let supervisor = supervisor_for(dir.path(), 3, Duration::from_millis(300));
    // Nothing ever listens: the wait ends with the ready timeout, not with a refusal.
    let started = Instant::now();
    assert!(supervisor.ensure_started().await.is_err());
    assert!(
        started.elapsed() >= Duration::from_millis(250),
        "{:?}",
        started.elapsed()
    );
    // Let the supervisor restart it a few times (each restart waits out the ready timeout).
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let launched = supervisor.starts();
    assert!(
        launched >= 3,
        "the host was restarted only {launched} time(s)"
    );
    // Backoff and the ready wait bound the restarts.
    assert!(launched <= 15, "no backoff: {launched} launches in 1.8 s");
    assert!(supervisor.is_running().await, "still supervised");
    // A later request does not launch a second supervisor on top of the running one.
    let before = supervisor.starts();
    let _ = tokio::time::timeout(Duration::from_millis(100), supervisor.ensure_started()).await;
    assert!(supervisor.starts() <= before + 1);
}

/// A guard refusal is remembered: later requests fail at once without launching a process, until
/// the delay passes or the plugin manifest changes.
#[tokio::test]
async fn a_guard_refusal_is_remembered_until_the_manifest_changes() {
    let dir = tempfile::tempdir().unwrap();
    let supervisor = supervisor_for(
        dir.path(),
        python_guard::REFUSED_EXIT_CODE,
        Duration::from_secs(60),
    );
    assert!(supervisor.ensure_started().await.is_err());
    assert_eq!(supervisor.starts(), 1);
    // Ten requests: no launch, and each answers at once.
    let started = Instant::now();
    for _ in 0..10 {
        let err = supervisor.ensure_started().await.unwrap_err();
        assert!(err.to_string().contains("refused to start"), "{err}");
    }
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(supervisor.starts(), 1, "a refused host was launched again");
    // A manifest change (a fixed install, other plugins) clears the memory.
    let changed: HostManifest =
        serde_json::from_str(r#"{"api_plugins": ["portal", "other"]}"#).unwrap();
    assert!(supervisor.set_manifest(&changed).await);
    assert!(supervisor.ensure_started().await.is_err());
    assert_eq!(
        supervisor.starts(),
        2,
        "the refusal outlived the manifest change"
    );
    // `clear_refusal` (a reload) does too.
    supervisor.clear_refusal();
    assert!(supervisor.ensure_started().await.is_err());
    assert_eq!(supervisor.starts(), 3);
}
