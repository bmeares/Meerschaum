//! Actual native and pre-site SDK transport selection under mutable configuration.
#![cfg(target_os = "linux")]

use meerschaum_cli::executor_profiles::{Inputs, prepare_with_runtime};
use meerschaum_core::{
    Map, Value,
    team_scope::{
        api_execution,
        sandbox::{self, Access, Rule, runtime},
        write_private,
    },
};
use serde_json::json;
use std::{
    collections::BTreeSet,
    io::{Read, Write},
    net::TcpListener,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

struct Endpoint {
    url: String,
    graphs: Arc<AtomicUsize>,
    metadata: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Endpoint {
    fn new(refuse_graph: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let graphs = Arc::new(AtomicUsize::new(0));
        let metadata = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (observed_graphs, observed_metadata, stopped) =
            (graphs.clone(), metadata.clone(), stop.clone());
        let thread = std::thread::spawn(move || {
            while !stopped.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(value) => value,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) => panic!("owned instruction listener: {error}"),
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                let mut chunk = [0u8; 2048];
                let deadline = Instant::now() + Duration::from_secs(2);
                while request.len() < 16 * 1024
                    && !request.windows(4).any(|bytes| bytes == b"\r\n\r\n")
                    && Instant::now() < deadline
                {
                    stream
                        .set_read_timeout(Some(
                            deadline
                                .saturating_duration_since(Instant::now())
                                .max(Duration::from_millis(1)),
                        ))
                        .unwrap();
                    match stream.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(length) => request.extend_from_slice(&chunk[..length]),
                    }
                }
                let head = String::from_utf8_lossy(&request);
                let path = head
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("")
                    .split('?')
                    .next()
                    .unwrap_or("")
                    .to_string();
                let length = head
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map_or(Some(0), |(_, value)| value.trim().parse::<usize>().ok());
                let expected = request
                    .windows(4)
                    .position(|bytes| bytes == b"\r\n\r\n")
                    .and_then(|offset| offset.checked_add(4)?.checked_add(length?))
                    .filter(|length| *length <= 16 * 1024);
                if let Some(expected) = expected {
                    while request.len() < expected && Instant::now() < deadline {
                        stream
                            .set_read_timeout(Some(
                                deadline
                                    .saturating_duration_since(Instant::now())
                                    .max(Duration::from_millis(1)),
                            ))
                            .unwrap();
                        let remaining = (expected - request.len()).min(chunk.len());
                        match stream.read(&mut chunk[..remaining]) {
                            Ok(0) | Err(_) => break,
                            Ok(length) => request.extend_from_slice(&chunk[..length]),
                        }
                    }
                }
                // Consume the complete bounded POST before close; unread request data can
                // otherwise reset the connection and discard a valid response on regression.
                let complete = expected.is_some_and(|expected| request.len() >= expected);
                let (status, body) = if !complete {
                    (
                        400,
                        json!({"detail":"Incomplete or oversized owned fixture request."}),
                    )
                } else if path == meerschaum_connector_api::client::GRAPH_PLAN_PATH {
                    observed_graphs.fetch_add(1, Ordering::SeqCst);
                    if refuse_graph {
                        (
                            403,
                            json!({"detail":"Exact original Main refused its graph."}),
                        )
                    } else {
                        (
                            200,
                            json!({"success":true,"message":"Wrong mutable callback must never be used","instruction":null}),
                        )
                    }
                } else {
                    observed_metadata.fetch_add(1, Ordering::SeqCst);
                    let value = if path == "/pipes/keys" {
                        json!({"1":["sql:main","fixture",null]})
                    } else if path.ends_with("/attributes") {
                        json!({"pipe_id":1,"parameters":{"columns":{"datetime":"id"},"dtypes":{"id":"int64"}}})
                    } else if path.ends_with("/parameters") {
                        json!({"columns":{"datetime":"id"},"dtypes":{"id":"int64"}})
                    } else if path.ends_with("/id") {
                        json!(1)
                    } else {
                        json!({})
                    };
                    (200, value)
                };
                let body = body.to_string();
                let reason = match status {
                    200 => "OK",
                    403 => "Forbidden",
                    _ => "Bad Request",
                };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        Self {
            url,
            graphs,
            metadata,
            stop,
            thread: Some(thread),
        }
    }
}
impl Drop for Endpoint {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}
fn output(mut command: Command) -> std::process::Output {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove("TMUX")
        .env_remove("TMUX_PANE");
    runtime::sanitize_command(&mut command);
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("The owned graph transport child exceeded its deadline.");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.wait_with_output().unwrap()
}

#[test]
#[ignore = "Requires the current private SDK build and an explicitly selected trusted Python runtime; parent runs this native+SDK acceptance gate serially."]
fn native_and_pre_site_sdk_keep_the_original_graph_callback_after_configuration_mutation() {
    use std::os::unix::fs::symlink;
    let _ = meerschaum_core::self_exe::register_native_current_executable();
    let parent = std::env::var_os("MRSM_ROOT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../test_root"));
    std::fs::create_dir_all(&parent).unwrap();
    let root = tempfile::Builder::new()
        .prefix("immutable-graph-transport-")
        .tempdir_in(parent)
        .unwrap();
    let root = root.path().canonicalize().unwrap();
    let original = Endpoint::new(true);
    let replacement = Endpoint::new(false);
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    assert!(
        std::fs::read_dir(repository.join("meerschaum/_core"))
            .unwrap()
            .flatten()
            .any(
                |entry| entry.file_name().to_string_lossy().starts_with("_native")
                    && entry
                        .path()
                        .extension()
                        .is_some_and(|extension| extension == "so")
            ),
        "Build the current private SDK before this acceptance gate."
    );
    let selected = std::env::var_os("MRSM_PYTHON")
        .expect("Select the trusted private SDK Python with MRSM_PYTHON.");
    let base = std::fs::canonicalize(selected).unwrap();
    let basename = base.file_name().unwrap().to_string_lossy();
    let version = basename
        .strip_prefix("python")
        .filter(|version| {
            !version.is_empty()
                && version
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'.' | b't'))
        })
        .expect("The trusted Python must have a versioned CPython executable.");
    let venv = root.join("admin-venv");
    let site = venv.join(format!("lib/python{version}/site-packages"));
    std::fs::create_dir_all(&site).unwrap();
    std::fs::create_dir_all(venv.join("bin")).unwrap();
    let python = venv.join("bin/python3");
    symlink(&base, &python).unwrap();
    std::fs::write(
        venv.join("pyvenv.cfg"),
        format!(
            "home = {}\ninclude-system-site-packages = false\n",
            base.parent().unwrap().display()
        ),
    )
    .unwrap();
    let server = root.join("server");
    std::fs::create_dir_all(&server).unwrap();
    let directory = server.join("execution-contexts/fixture");
    let environment = vec![
        ("PATH".into(), "/usr/bin:/bin".into()),
        ("MRSM_PYTHON".into(), python.display().to_string()),
        ("PYTHONPATH".into(), repository.display().to_string()),
    ];
    let logical = BTreeSet::from(["sql:main".to_string()]);
    let mut prepared = prepare_with_runtime(&Inputs {
        metadata: false,
        server_root: &server,
        directory: &directory,
        base: &Map::new(),
        logical_instances: &logical,
        default_instance: "sql:main",
        callback_url: &original.url,
        read_exec: &[],
        job_dir: None,
        environment: &environment,
    })
    .unwrap();
    let launcher = std::fs::canonicalize(env!("CARGO_BIN_EXE_mrsm-cli")).unwrap();
    prepared.spec.runtime.as_mut().unwrap().launcher = launcher.clone();
    prepared.environment.retain(|(key, _)| key != "MRSM_BIN");
    prepared
        .environment
        .push(("MRSM_BIN".into(), launcher.to_string_lossy().into_owned()));
    prepared.spec.rules.push(Rule::new(
        &launcher,
        Access::ReadExec,
        "actual native fixture launcher",
    ));
    let spec = prepared.directory.join(sandbox::SPEC_FILE);
    write_private(&spec, &serde_json::to_string(&prepared.spec).unwrap()).unwrap();
    write_private(&prepared.credential_file,&json!({"token":"synthetic-transport-fixture","expires_at":chrono::Utc::now().timestamp()+120}).to_string()).unwrap();
    let poison = prepared.directory.join("state/changed-config.json");
    let changed = api_execution::configuration(
        &Map::new(),
        &logical,
        "sql:main",
        &replacement.url,
        &prepared.credential_file,
    )
    .unwrap();
    write_private(&poison, &Value::Map(changed).to_json_string()).unwrap();
    let mut command = Command::new(&launcher);
    command
        .env_clear()
        .envs(prepared.environment.iter().cloned())
        .env("MRSM_CONFIG_FILE", &poison)
        .args(["plan", "pipes", "--nopretty"]);
    let native = output(command);
    assert!(
        !native.status.success(),
        "A mutable callback replaced Main's explicit refusal."
    );
    assert!(
        original.graphs.load(Ordering::SeqCst) > 0,
        "Native construction never requested Main: {}\n{}",
        String::from_utf8_lossy(&native.stdout),
        String::from_utf8_lossy(&native.stderr)
    );
    assert_eq!(replacement.graphs.load(Ordering::SeqCst), 0);
    assert!(
        replacement.metadata.load(Ordering::SeqCst) > 0,
        "The mutable native configuration was not exercised."
    );
    let before = original.graphs.load(Ordering::SeqCst);
    let marker = prepared.directory.join("state/site-ran");
    // The host-mapped SDK is loaded and captures its own Core state before this .pth runs.
    let hook = format!(
        "import json, os, sys\nos.environ['MRSM_CONFIG_FILE']={poison}\nos.environ['MRSM_TEAM_SANDBOX']={poison}\nos.environ.pop('MRSM_API_EXECUTION',None)\nnative=sys.modules['meerschaum._core._native']\nnative.config_set(json.load(open({poison})))\nassert json.loads(native.team_sandbox_enforce([]))['enforced']\nopen({marker},'w').write('after-capture')\n",
        poison = json!(poison.display().to_string()),
        marker = json!(marker.display().to_string())
    );
    std::fs::write(
        site.join("mutation.pth"),
        format!("import builtins; exec({})\n", json!(hook)),
    )
    .unwrap();
    let script = r#"import json, sys
native=sys.modules['meerschaum._core._native']
try:
 native.engine_plan(json.dumps({'instance':'sql:main','targets':[['sql:main','fixture',None,'sql:main']],'flat':True}))
except RuntimeError as error:
 assert 'Main refused its canonical graph instructions' in str(error), str(error)
 print('immutable-sdk-transport')
else:
 raise AssertionError('A mutated SDK callback selected local planning')
"#;
    let mut command = Command::new(&launcher);
    command
        .env_clear()
        .envs(prepared.environment.iter().cloned())
        .args([
            runtime::PYTHON_CHILD,
            python.to_str().unwrap(),
            "-c",
            script,
        ]);
    let sdk = output(command);
    assert!(
        sdk.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&sdk.stdout),
        String::from_utf8_lossy(&sdk.stderr)
    );
    assert_eq!(std::fs::read_to_string(marker).unwrap(), "after-capture");
    assert!(original.graphs.load(Ordering::SeqCst) > before);
    assert_eq!(
        replacement.graphs.load(Ordering::SeqCst),
        0,
        "A .pth/runtime Config change selected graph authority."
    );
}
