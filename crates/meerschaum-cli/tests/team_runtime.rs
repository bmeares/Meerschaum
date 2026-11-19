//! Interpreter wrappers and site hooks first run inside the native team's kernel boundary.
#![cfg(target_os = "linux")]

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use meerschaum_core::team_scope::sandbox::{self, Access, Mode, Rule, Spec, runtime};
use serde_json::json;

fn output(mut command: Command) -> std::process::Output {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    runtime::sanitize_command(&mut command);
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("The owned native interpreter fixture exceeded its deadline.");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.wait_with_output().unwrap()
}

#[test]
fn wrappers_site_hooks_and_source_roots_cannot_run_before_confinement_or_expose_adjacent_secrets() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let _ = meerschaum_core::self_exe::register_native_current_executable();
    let parent = std::env::var_os("MRSM_ROOT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../test_root"));
    std::fs::create_dir_all(&parent).unwrap();
    let root = tempfile::Builder::new()
        .prefix("team-runtime-")
        .tempdir_in(parent)
        .unwrap();
    let root = root.path().canonicalize().unwrap();
    let server = root.join("server");
    let work = root.join("work");
    let sources = root.join("sources");
    let venv = root.join("admin-venv");
    for dir in [
        &server,
        &work,
        &sources,
        &venv.join("bin"),
        &sources.join("fixture_package"),
    ] {
        std::fs::create_dir_all(dir).unwrap();
    }
    let canary = server.join("canary");
    let outside = server.join("early-execution-marker");
    let dotenv = sources.join(".env");
    std::fs::write(&canary, "private-host-canary").unwrap();
    std::fs::write(&dotenv, "private-adjacent-environment").unwrap();
    std::fs::write(sources.join("fixture_package/__init__.py"), "VALUE = 42\n").unwrap();
    let selected = std::env::var_os("MRSM_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/usr/bin/python3"));
    let base = std::fs::canonicalize(selected).unwrap();
    let version = base.file_name().unwrap().to_string_lossy();
    let version = version
        .strip_prefix("python")
        .filter(|v| {
            !v.is_empty()
                && v.bytes()
                    .all(|b| b.is_ascii_digit() || b == b'.' || b == b't')
        })
        .expect("The owned test Python must use a versioned CPython executable.");
    let site = venv.join(format!("lib/python{version}/site-packages"));
    std::fs::create_dir_all(&site).unwrap();
    std::fs::write(
        venv.join("pyvenv.cfg"),
        format!(
            "home = {}\ninclude-system-site-packages = false\n",
            base.parent().unwrap().display()
        ),
    )
    .unwrap();
    let python = venv.join("bin/python3");
    symlink(&base, &python).unwrap();
    let site_marker = work.join("site-confined");
    let hook = format!(
        "try:\n open({}, 'w').write('unsafe')\nexcept PermissionError:\n open({}, 'w').write('confined')\n",
        json!(outside.display().to_string()),
        json!(site_marker.display().to_string())
    );
    std::fs::write(
        site.join("fixture.pth"),
        format!("import builtins; exec({})\n", json!(hook)),
    )
    .unwrap();
    let malicious = work.join("malicious-python");
    std::fs::write(
        &malicious,
        format!(
            "#!/bin/sh\nprintf unsafe > '{}'\nexec '{}' \"$@\"\n",
            outside.display(),
            base.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&malicious, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut spec = Spec {
        mode: Mode::Require,
        team: "RuntimeFixture".into(),
        canary,
        protected: vec![server.clone(), sources.clone()],
        rules: sandbox::SYSTEM_RULES
            .iter()
            .map(|(path, access, why)| Rule::new(*path, *access, why))
            .collect(),
        ..Spec::default()
    };
    spec.rules.push(Rule::new(
        &work,
        Access::ReadWrite,
        "owned runtime test work",
    ));
    let environment = vec![
        ("PATH".into(), "/usr/bin:/bin".into()),
        ("MRSM_PYTHON".into(), python.display().to_string()),
        ("PYTHONPATH".into(), sources.display().to_string()),
    ];
    let mut prepared = runtime::Runtime::prepare(&mut spec, &environment).unwrap();
    // This test process is a trusted native harness; the actual child is the Cargo-built CLI.
    let binary = std::fs::canonicalize(env!("CARGO_BIN_EXE_mrsm-cli")).unwrap();
    prepared.launcher = binary.clone();
    for (key, value) in &mut prepared.environment {
        if key == "MRSM_BIN" {
            *value = binary.display().to_string();
        }
    }
    spec.rules
        .push(Rule::new(&binary, Access::ReadExec, "owned actual CLI"));
    spec.runtime = Some(prepared);
    let spec_file = root.join("sandbox.json");
    spec.rules
        .push(Rule::new(&spec_file, Access::Read, "owned runtime spec"));
    assert!(
        !spec.rules.iter().any(|rule| dotenv.starts_with(&rule.path)),
        "A source import root must not grant adjacent .env files."
    );
    std::fs::write(&spec_file, serde_json::to_vec(&spec).unwrap()).unwrap();
    let command = || {
        let mut command = Command::new(&binary);
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &work)
            .env(sandbox::SPEC_VAR, &spec_file)
            .env("MRSM_PYTHON", &malicious)
            .env("PYTHONPATH", &sources)
            .env("LD_PRELOAD", work.join("never-load.so"));
        command
    };
    let mut probe = command();
    probe.args(["show", "version", "--nopretty"]);
    let _ = output(probe);
    assert!(
        !outside.exists(),
        "The CLI ran an interpreter wrapper before installing its sandbox."
    );
    let script = format!(
        "import fixture_package, json, sys\ntry:\n open({}).read()\nexcept PermissionError:\n denied = True\nelse:\n denied = False\nprint(json.dumps({{'value': fixture_package.VALUE, 'denied': denied, 'prefix': sys.prefix}}))",
        json!(dotenv.display().to_string())
    );
    let mut confined = command();
    confined.args([runtime::PYTHON_CHILD, "python3", "-P", "-c", &script]);
    let result = output(confined);
    if !sandbox::kernel_support().available() {
        assert!(!result.status.success());
        assert!(!site_marker.exists());
        return;
    }
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let actual: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(actual["value"], 42);
    assert_eq!(actual["denied"], true);
    assert_eq!(actual["prefix"], venv.display().to_string());
    assert_eq!(std::fs::read_to_string(&site_marker).unwrap(), "confined");
    assert!(!outside.exists());
    // Even an explicitly selected malicious wrapper runs after the same kernel boundary.
    let mut wrapper = command();
    wrapper.args([
        runtime::PYTHON_CHILD,
        malicious.to_str().unwrap(),
        "-c",
        "print('confined-wrapper')",
    ]);
    assert!(output(wrapper).status.success());
    assert!(!outside.exists());
    // A user's original sysargs may look like an internal supervisor invocation. Only the
    // host's consumed lifecycle marker exempts the real parent; argv alone cannot skip proof.
    let attack_context = meerschaum_jobs::JobsContext::for_root(work.join("supervisor-argv"));
    let mut attack_job = meerschaum_jobs::Job::new(
        "argv-probe",
        Some(vec!["show".into(), "version".into()]),
        meerschaum_jobs::JobOptions::default(),
        &attack_context,
    )
    .unwrap();
    assert!(attack_job.daemon().setup(true).success);
    let mut attack = attack_job.daemon().supervisor_options(false);
    attack.command = vec![
        malicious.display().to_string(),
        "-c".into(),
        "print('confined-supervisor-child')".into(),
    ];
    attack.cwd = work.clone();
    attack.startup = None;
    let mut supervisor_argv = command();
    supervisor_argv
        .arg(meerschaum_jobs::supervisor::SUPERVISOR_ARG)
        .args(attack.to_args());
    let supervised = output(supervisor_argv);
    assert!(
        supervised.status.success(),
        "The confined supervisor fixture could not run its owned child: {}",
        String::from_utf8_lossy(&supervised.stderr)
    );
    assert!(
        !outside.exists(),
        "A user-selected supervisor argv ran its child before the kernel boundary."
    );
    // Trusted host program/supervisor overrides may not put an interpreter or wrapper ahead
    // of marked A/B jobs. The native test host models admission, never supervises a real job.
    let host = meerschaum_core::self_exe::native_executable().unwrap();
    let mut model = spec.clone();
    model.runtime.as_mut().unwrap().launcher = host.clone();
    let model_file = root.join("model-sandbox.json");
    std::fs::write(&model_file, serde_json::to_vec(&model).unwrap()).unwrap();
    for supervisor_override in [false, true] {
        let mut launcher = meerschaum_jobs::Launcher {
            program: vec![host.display().to_string()],
            supervisor: vec![
                host.display().to_string(),
                meerschaum_jobs::supervisor::SUPERVISOR_ARG.into(),
            ],
        };
        if supervisor_override {
            launcher.supervisor[0] = malicious.display().to_string();
        } else {
            launcher.program[0] = malicious.display().to_string();
        }
        let context = meerschaum_jobs::JobsContext::for_root(root.join("model-jobs"))
            .with_launcher(launcher)
            .with_launch_environment(vec![(
                sandbox::SPEC_VAR.into(),
                model_file.display().to_string(),
            )]);
        let mut job = meerschaum_jobs::Job::new(
            if supervisor_override {
                "bad-supervisor"
            } else {
                "bad-program"
            },
            Some(vec!["show".into(), "version".into()]),
            meerschaum_jobs::JobOptions::default(),
            &context,
        )
        .unwrap();
        let admission = job.start();
        assert!(
            !admission.success && admission.message.contains("canonical native"),
            "{admission}"
        );
        assert!(!outside.exists());
    }
    let context = meerschaum_jobs::JobsContext::for_root(root.join("systemd-model"));
    let executor = meerschaum_jobs::systemd::SystemdExecutor::new(context);
    let unit = executor.get_service_file_text(
        "marked",
        &["show".into(), "version".into()],
        host.to_str().unwrap(),
        &[
            (sandbox::SPEC_VAR.into(), model_file.display().to_string()),
            ("MRSM_TEAM_SCOPE".into(), "RuntimeFixture".into()),
            ("MRSM_TEAM_FINGERPRINT".into(), "host-proof".into()),
        ],
    );
    assert!(unit.contains("Environment=MRSM_TEAM_SANDBOX="));
    assert!(unit.contains("Environment=MRSM_TEAM_FINGERPRINT=host-proof"));
    let removed = unit
        .lines()
        .find_map(|line| line.strip_prefix("UnsetEnvironment="))
        .unwrap();
    for name in runtime::BOOTSTRAP_UNSET {
        assert!(
            removed.split_whitespace().any(|value| value == *name),
            "Missing {name} from scoped systemd loader sanitation."
        );
    }
    let mut unmarked = Command::new(&binary);
    unmarked.env_clear().args([
        runtime::PYTHON_CHILD,
        malicious.to_str().unwrap(),
        "-c",
        "pass",
    ]);
    assert!(
        !output(unmarked).status.success(),
        "The internal native entry must refuse unmarked callers."
    );
    assert!(!outside.exists());
}

#[test]
fn sdk_embedding_refuses_before_native_probe_or_interpreter_wrapper_execution() {
    if std::env::var("MRSM_TEST_TEAM_RUNTIME_SDK").as_deref() != Ok("1") {
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let parent = std::env::var_os("MRSM_ROOT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../test_root"));
    std::fs::create_dir_all(&parent).unwrap();
    let root = tempfile::Builder::new()
        .prefix("team-runtime-sdk-")
        .tempdir_in(parent)
        .unwrap();
    let marker = root.path().join("unsafe-sdk-probe");
    let wrapper = root.path().join("malicious-python");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nprintf unsafe > '{}'\nexit 1\n",
            marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../meerschaum/_core");
    let extension = std::fs::read_dir(directory)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("_native")
                && path.extension().is_some_and(|ext| ext == "so")
        })
        .expect("Build the private SDK extension before opting into its bootstrap acceptance.");
    let script = format!(
        r#"
import importlib.util, json, os
spec = importlib.util.spec_from_file_location('_native', {extension})
native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native)
os.environ['MRSM_PYTHON'] = {wrapper}
assert native.main(['start', 'executor', '--host', '127.0.0.1', '--port', '0', '--nopretty', '-P', '{{"role":"proxy"}}']) != 0, 'A wheel-only executor falsely reached runtime startup'
env = {{'PATH': '/usr/bin:/bin', 'MRSM_PYTHON': {wrapper}, 'MRSM_CONFIG': json.dumps({{'system': {{'engine': {{'python_team_sandbox': 'require_sockets'}}}}}})}}
try:
    native.access_team_process_env({root}, 'Wetlands', ['sql:main'], env, sandbox=True)
except RuntimeError as error:
    assert 'native' in str(error).lower() or 'embedding' in str(error).lower(), str(error)
    print(json.dumps({{'refused': True}}))
else:
    raise SystemExit('Python embedding adopted a native launcher or started its interpreter before confinement')
"#,
        extension = json!(extension.display().to_string()),
        wrapper = json!(wrapper.display().to_string()),
        root = json!(root.path().display().to_string())
    );
    let python = std::env::var_os("MRSM_PYTHON").unwrap_or_else(|| "/usr/bin/python3".into());
    let mut command = Command::new(python);
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", root.path())
        .env("MRSM_ROOT_DIR", root.path())
        .args(["-I", "-S", "-c", &script]);
    let result = output(command);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&result.stdout).unwrap()["refused"],
        true
    );
    assert!(
        !marker.exists(),
        "An SDK probe executed a wrapper before rejecting its missing native provenance."
    );
}
