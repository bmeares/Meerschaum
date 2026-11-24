//! API profile confinement is installed before Python site/manifest probes.

use std::collections::BTreeSet;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use meerschaum_cli::executor::profiles::{Inputs, Prepared, prepare};
use meerschaum_core::team_scope::sandbox::{self, Mode};
use meerschaum_core::{Map, Value};

fn run(prepared: &Prepared, args: &[&str], probe: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    command
        .env_clear()
        .envs(prepared.environment.iter().cloned())
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .args(args)
        .stdin(Stdio::null());
    if probe {
        command.env(meerschaum_cli::team::execution::PROBE_ENV, "1");
    }
    crate::common::process::output_with_timeout(&mut command, Duration::from_secs(20))
}

#[test]
fn api_profile_without_host_runtime_never_executes_an_interpreter_wrapper() {
    if !Path::new("/usr/bin/python3").is_file() {
        eprintln!("API profile probe requires the system's isolated stdlib Python.");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let directory = root.join("execution-contexts/one");
    let marker = root.join("outside-marker");
    let secret = root.join("server-secret");
    std::fs::write(&secret, "synthetic-server-secret").unwrap();
    let sibling = root.join("execution-contexts/two");
    std::fs::create_dir_all(&sibling).unwrap();
    std::fs::write(
        sibling.join("credential.json"),
        "synthetic-other-launch-token",
    )
    .unwrap();
    let socket_path = root.join("outside.sock");
    let _listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let interpreter = root.join("python-probe");
    let script = format!(
        r#"#!/bin/sh
exec /usr/bin/python3 -I -S -c '
import json, os, socket
result = {{}}
for name, path, mode in [("outside_write", {marker:?}, "w"), ("server_secret", {secret:?}, "r"), ("sibling_token", {sibling:?}, "r"), ("own_token", os.path.join(os.path.dirname(os.environ["MRSM_CONFIG_FILE"]), "credential.json"), "r")]:
    try:
        with open(path, mode) as f:
            if mode == "w": f.write("unsafe")
            else: f.read()
        result[name] = True
    except OSError: result[name] = False
try:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect({socket_path:?})
    result["unix_socket"] = True
except OSError: result["unix_socket"] = False
try:
    os.kill({parent}, 0)
    result["outside_signal"] = True
except OSError: result["outside_signal"] = False
with open(os.path.join(os.environ["MRSM_ROOT_DIR"], "tmp", "outcome.json"), "w") as f: json.dump(result, f)
print("5.1.0 {capability}")
'
"#,
        marker = marker.to_string_lossy(),
        secret = secret.to_string_lossy(),
        sibling = sibling.join("credential.json").to_string_lossy(),
        parent = std::process::id(),
        socket_path = socket_path.to_string_lossy(),
        capability = meerschaum_worker::worker::TEAM_SCOPE_CAPABILITY
    );
    std::fs::write(&interpreter, script).unwrap();
    std::fs::set_permissions(&interpreter, std::fs::Permissions::from_mode(0o700)).unwrap();
    let reads = vec![
        std::fs::canonicalize(env!("CARGO_BIN_EXE_mrsm-cli")).unwrap(),
        interpreter.clone(),
    ];
    let env = vec![
        ("PATH".into(), "/usr/bin:/bin".into()),
        (
            "MRSM_PYTHON".into(),
            interpreter.to_string_lossy().into_owned(),
        ),
    ];
    let prepared = prepare(&Inputs {
        metadata: false,
        server_root: &root,
        directory: &directory,
        base: &Map::new(),
        logical_instances: &BTreeSet::from(["sql:main".into()]),
        default_instance: "sql:main",
        callback_url: "https://main.example",
        read_exec: &reads,
        job_dir: None,
        environment: &env,
    })
    .unwrap();
    let outcome = prepared.directory.join("state/tmp/outcome.json");
    meerschaum_core::team_scope::write_private(&prepared.credential_file, "synthetic-own-token")
        .unwrap();
    let output = run(&prepared, &[], true);
    assert!(
        !marker.exists(),
        "an interpreter ran before mandatory confinement"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let probe: meerschaum_server::team_execution::Probe = stdout
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str(line).ok())
        .unwrap_or_else(|| {
            panic!(
                "missing active sandbox report: {stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            )
        });
    if let Err(reason) = probe.sandbox.require_sockets_verdict() {
        // Unsupported kernels/runtime policies refuse before invoking even this test interpreter.
        assert!(
            !outcome.exists(),
            "Python ran despite failed mandatory proof: {reason}"
        );
        assert!(
            !run(&prepared, &["show", "version", "--nopretty"], false)
                .status
                .success()
        );
        return;
    }
    // Filesystem rules alone do not authorize an interpreter or wrapper. The prepared
    // runtime positive is covered by executor_api_profile; this generation has no Runtime.
    assert_eq!(
        probe.python_error.as_deref(),
        Some("The team has no host-prepared native runtime."),
        "unexpected probe: {stdout}\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(!outcome.exists(), "an unprepared interpreter wrapper ran");
}

#[test]
fn an_api_marker_cannot_accept_a_missing_or_weaker_spec() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let directory = root.join("execution-contexts/one");
    let mut prepared = prepare(&Inputs {
        metadata: false,
        server_root: &root,
        directory: &directory,
        base: &Map::new(),
        logical_instances: &BTreeSet::from(["sql:main".into()]),
        default_instance: "sql:main",
        callback_url: "https://main.example",
        read_exec: &[],
        job_dir: None,
        environment: &[],
    })
    .unwrap();
    let spec_file = prepared.directory.join(sandbox::SPEC_FILE);
    prepared.spec.mode = Mode::Auto;
    std::fs::write(&spec_file, serde_json::to_vec(&prepared.spec).unwrap()).unwrap();
    let output = run(&prepared, &["show", "version", "--nopretty"], false);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("mandatory"));
    std::fs::remove_file(spec_file).unwrap();
    assert!(!run(&prepared, &["show", "version"], false).status.success());
    prepared
        .environment
        .retain(|(key, _)| key != sandbox::SPEC_VAR);
    assert!(!run(&prepared, &["show", "version"], false).status.success());
    // Configuration contains a path, not the contents of the credential file.
    let config = Value::from_json_str(
        &std::fs::read_to_string(prepared.directory.join("config.json")).unwrap(),
    )
    .unwrap();
    assert!(config.get("stack").is_none());
}
