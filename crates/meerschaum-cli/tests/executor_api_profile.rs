//! Signed API-only jobs execute plugins and callbacks only inside their private boundary.
#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::authentication::{Request, RequestAuthentication};
use meerschaum_connector_api::client::{Body, Method, RequestOptions};
use meerschaum_connector_api::traits::{ConnectorError, ConnectorResult};
use meerschaum_core::{Map, Value};
use meerschaum_jobs::process::{self, ProcState, Sig};
use meerschaum_server::executor::control::{Actor, Authority};
use meerschaum_server::executor::environment::{JOB_CONTEXT_PROPERTY, Profile, Target, Update};
use meerschaum_server::executor::gateway::{CONFIGURATION_PATH, CONTEXTS_PATH, Configuration};
use serde_json::json;
use uuid::Uuid;

struct Signer {
    authority: Authority,
    revision: String,
}

fn only_generation(directory: &Path) -> PathBuf {
    let generations = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("generation-")
        })
        .collect::<Vec<_>>();
    assert_eq!(generations.len(), 1, "{generations:?}");
    generations.into_iter().next().unwrap()
}
impl RequestAuthentication for Signer {
    fn authorization(&self, request: Request<'_>) -> ConnectorResult<String> {
        self.authority
            .issue(
                Actor {
                    owner: "operator".into(),
                    username: Some("operator".into()),
                    permissions: Default::default(),
                    administrator: false,
                },
                meerschaum_server::executor::control::Request {
                    method: request.method,
                    path: request.path,
                    body: request.body.ok_or_else(|| {
                        ConnectorError::Invalid("Unbuffered fixture request.".into())
                    })?,
                },
                &self.revision,
                chrono::Utc::now().timestamp(),
            )
            .map(|token| format!("Bearer {token}"))
            .map_err(|_| ConnectorError::Invalid("Could not sign fixture request.".into()))
    }
}

struct Subreaper(Option<rustix::process::Pid>);
impl Subreaper {
    fn install() -> Self {
        let old = rustix::process::child_subreaper().unwrap();
        rustix::process::set_child_subreaper(rustix::process::Pid::from_raw(1)).unwrap();
        Self(old)
    }
}
impl Drop for Subreaper {
    fn drop(&mut self) {
        let _ = rustix::process::set_child_subreaper(self.0);
    }
}

/// Exact fixture PIDs are remembered with their births; cleanup never signals a reused PID.
/// Subreaping lets this test reap its own detached supervisors rather than leaving zombies.
struct Fixture {
    api: Child,
    bystander: Child,
    root: PathBuf,
    names: Vec<String>,
    owned: BTreeMap<i32, chrono::NaiveDateTime>,
}
impl Fixture {
    fn remember(&mut self, pid: i32) {
        if let Some(birth) = process::process_start_time(pid) {
            self.owned.entry(pid).or_insert(birth);
        }
    }
    fn snapshot(&mut self) {
        for name in self.names.clone() {
            if let Ok(pid) =
                std::fs::read_to_string(self.root.join("jobs").join(&name).join("process.pid"))
                && let Ok(pid) = pid.trim().parse::<i32>()
            {
                self.remember(pid);
            }
        }
        for (pid, args) in process::process_cmdlines() {
            let owns_root = args.windows(2).any(|pair| {
                pair[0] == "--jobs-dir" && Path::new(&pair[1]) == self.root.join("jobs")
            });
            let owns_name = args
                .windows(2)
                .any(|pair| pair[0] == "--daemon-id" && self.names.contains(&pair[1]));
            if owns_root
                && owns_name
                && args
                    .iter()
                    .any(|arg| arg == meerschaum_jobs::supervisor::SUPERVISOR_ARG)
            {
                self.remember(pid);
            }
        }
        let mut pending: Vec<_> = self.owned.keys().copied().collect();
        let mut scanned = std::collections::BTreeSet::new();
        while let Some(parent) = pending.pop() {
            if !scanned.insert(parent) {
                continue;
            }
            for pid in process::children(parent).unwrap_or_default() {
                self.remember(pid);
                pending.push(pid);
            }
        }
    }
    fn reap(&self) {
        for pid in self.owned.keys() {
            if let Some(pid) = rustix::process::Pid::from_raw(*pid) {
                let _ = rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::NOHANG);
            }
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.api.kill();
        let _ = self.api.wait();
        self.snapshot();
        for (&pid, &birth) in &self.owned {
            if process::process_start_time(pid) == Some(birth)
                && matches!(
                    process::proc_state(pid),
                    ProcState::Alive | ProcState::Stopped
                )
            {
                let _ = process::send_signal(pid, Sig::Kill);
            }
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            self.reap();
            if self
                .owned
                .keys()
                .all(|pid| process::proc_state(*pid) == ProcState::Gone)
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = self.bystander.kill();
        let _ = self.bystander.wait();
    }
}
fn core(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

async fn status(api: &APIConnector, name: &str, wanted: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let observed = api.get_job_status(name).await.unwrap();
        if observed == wanted {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{name}: expected {wanted}, observed {observed}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

struct CallbackTask(tokio::task::JoinHandle<()>);
impl Drop for CallbackTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn context(
    api: &APIConnector,
    update: &Update,
) -> meerschaum_connector_api::client::ApiResponse {
    api.make_request(
        Method::Put,
        &format!("{CONTEXTS_PATH}{}", update.id),
        RequestOptions {
            body: Body::Json(serde_json::to_string(update).unwrap()),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

fn diagnostics(root: &Path, name: &str, state: &Path) -> String {
    let mut paths = vec![
        root.join("launcher.log"),
        root.join("daemon_errors.log"),
        root.join("jobs").join(name).join("properties.json"),
        root.join("jobs").join(name).join(".stop.json"),
        state.join("early-proof.json"),
        state.join("plugin-imported"),
    ];
    if let Ok(entries) = std::fs::read_dir(root.join("logs")) {
        paths.extend(
            entries
                .flatten()
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(&format!("{name}.log"))
                })
                .take(16)
                .map(|entry| entry.path()),
        );
    }
    let mut evidence = String::new();
    for path in paths {
        let Ok(metadata) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !metadata.file_type().is_file() || metadata.len() > 128 * 1024 {
            continue;
        }
        let Ok(mut value) = std::fs::read_to_string(&path) else {
            continue;
        };
        if let Ok(mut json) = serde_json::from_str::<serde_json::Value>(&value) {
            meerschaum_server::redact::redact_value(None, &mut json);
            value = json.to_string();
        }
        value = meerschaum_server::redact::mask_uri_passwords(&value);
        for secret in [
            "synthetic-native-team-password",
            "synthetic-plugin-secret",
            "first-api-token",
            "second-api-token",
            "persisted-native-password",
        ] {
            value = value.replace(secret, "[fixture credential redacted]");
        }
        evidence.push_str(&format!(
            "\n{}:\n{value}\n",
            path.strip_prefix(root).unwrap_or(&path).display()
        ));
    }
    evidence
}

async fn wait_file(path: &Path, api: &APIConnector, root: &Path, name: &str) {
    let began = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(25);
    while !path.is_file() {
        let stopped = began.elapsed() > Duration::from_secs(2)
            && api.get_job_status(name).await.ok().as_deref() == Some("stopped");
        assert!(
            !stopped && Instant::now() < deadline,
            "Missing {}\n{}",
            path.display(),
            diagnostics(root, name, path.parent().unwrap())
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_api_jobs_use_rotating_logical_callbacks_without_team_credentials_or_early_execution()
 {
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::symlink;
    let _subreaper = Subreaper::install();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("server");
    let control = temp.path().join("factory-control");
    std::fs::create_dir_all(&control).unwrap();
    let venv = temp.path().join("admin-runtime");
    std::fs::create_dir_all(root.join("plugins")).unwrap();
    std::fs::create_dir_all(root.join("config")).unwrap();
    std::fs::create_dir_all(root.join("venvs/shared")).unwrap();
    std::fs::create_dir_all(venv.join("bin")).unwrap();
    let server_secret = root.join("config/server.secret");
    let team_secret = root.join("venvs/shared/team.secret");
    let outside_secret = temp.path().join("outside.secret");
    for path in [&server_secret, &team_secret, &outside_secret] {
        std::fs::write(path, "synthetic-never-granted-secret").unwrap();
    }
    let inherited_file = std::fs::File::open(&server_secret).unwrap();
    let (inherited_stream, mut stream_peer) = std::os::unix::net::UnixStream::pair().unwrap();
    stream_peer
        .write_all(b"synthetic-main-connected-channel")
        .unwrap();
    let file_fd = inherited_file.as_raw_fd();
    let stream_fd = inherited_stream.as_raw_fd();
    rustix::io::fcntl_setfd(&inherited_file, rustix::io::FdFlags::empty()).unwrap();
    rustix::io::fcntl_setfd(&inherited_stream, rustix::io::FdFlags::empty()).unwrap();
    let stream_identity = std::fs::read_link(format!("/proc/self/fd/{stream_fd}")).unwrap();
    let socket = root.join("privileged.sock");
    let _unix = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let selected = std::env::var_os("MRSM_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/usr/bin/python3"));
    let real = std::fs::canonicalize(&selected).unwrap();
    let filename = real.file_name().unwrap().to_string_lossy();
    let version = filename
        .strip_prefix("python")
        .filter(|v| {
            !v.is_empty()
                && v.bytes()
                    .all(|b| b.is_ascii_digit() || b == b'.' || b == b't')
        })
        .expect("The fixture needs versioned CPython.");
    let site = venv.join(format!("lib/python{version}/site-packages"));
    std::fs::create_dir_all(&site).unwrap();
    std::fs::write(
        venv.join("pyvenv.cfg"),
        format!(
            "home = {}\ninclude-system-site-packages = false\n",
            real.parent().unwrap().display()
        ),
    )
    .unwrap();
    let python = venv.join("bin/python3");
    symlink(real, &python).unwrap();

    let requests = Arc::new(Mutex::new(Vec::<(String, String, Vec<u8>)>::new()));
    let archive = Arc::new(Mutex::new(Vec::<u8>::new()));
    let source_archive = archive.clone();
    let observed = requests.clone();
    let callback = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let callback_url = format!("http://{}", callback.local_addr().unwrap());
    let routes = axum::Router::new().fallback(move |request: axum::extract::Request| {
        let observed = observed.clone();
        let source_archive = source_archive.clone();
        async move {
            use axum::response::IntoResponse;
            let path = request.uri().to_string();
            let auth = request
                .headers()
                .get("authorization")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("")
                .to_string();
            if path == "/plugins/api_probe" {
                if auth != "Mrsm-Plugin exact-api-probe-source" {
                    return axum::http::StatusCode::FORBIDDEN.into_response();
                }
                return source_archive.lock().unwrap().clone().into_response();
            }
            let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .to_vec();
            observed.lock().unwrap().push((path, auth, body));
            axum::Json(json!([{"value":7}])).into_response()
        }
    });
    let _callback = CallbackTask(tokio::spawn(async move {
        axum::serve(callback, routes).await.unwrap();
    }));

    let team_id = Uuid::new_v4();
    let generation = Uuid::new_v4();
    let (authority, _) = Authority::generate(Uuid::new_v4(), team_id, generation).unwrap();
    let bootstrap = control.join("bootstrap.json");
    std::fs::write(
        &bootstrap,
        serde_json::to_vec(&authority.verifier()).unwrap(),
    )
    .unwrap();
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    let log_path = control.join("launcher.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", &root)
        .env("MRSM_ROOT_DIR", &root)
        .env("MRSM_PLUGINS_DIR", root.join("plugins"))
        .env("MRSM_EXECUTOR_BOOTSTRAP", bootstrap)
        .env("MRSM_PYTHON", python)
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .env("DB_PASSWORD", "synthetic-native-team-password")
        .env(
            "PYTHONPATH",
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .canonicalize()
                .unwrap(),
        )
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .args([
            "start",
            "executor",
            "--host",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--control-root",
            control.to_str().unwrap(),
        ]);
    drop(reservation);
    let mut api_process = command.spawn().unwrap();
    // Only this trusted executor Main inherits the deliberate capabilities. Later fixture
    // subprocesses retain their normal CLOEXEC defaults.
    rustix::io::fcntl_setfd(&inherited_file, rustix::io::FdFlags::CLOEXEC).unwrap();
    rustix::io::fcntl_setfd(&inherited_stream, rustix::io::FdFlags::CLOEXEC).unwrap();
    let bystander = match Command::new("/bin/sleep")
        .env_clear()
        .arg("300")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            let _ = api_process.kill();
            let _ = api_process.wait();
            panic!("{error}");
        }
    };
    let mut fixture = Fixture {
        api: api_process,
        bystander,
        root: control.clone(),
        names: Vec::new(),
        owned: BTreeMap::new(),
    };
    let deadline = Instant::now() + Duration::from_secs(15);
    while tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .is_err()
    {
        assert!(
            fixture.api.try_wait().unwrap().is_none() && Instant::now() < deadline,
            "{}",
            std::fs::read_to_string(&log_path).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let sibling = control.join("execution-contexts/other/credential.json");
    let api_pid = fixture.api.id();
    assert_eq!(
        std::fs::read_link(format!("/proc/{api_pid}/fd/{file_fd}")).unwrap(),
        server_secret,
        "The trusted API did not inherit the intended control file."
    );
    assert_eq!(
        std::fs::read_link(format!("/proc/{api_pid}/fd/{stream_fd}")).unwrap(),
        stream_identity,
        "The trusted API did not inherit the intended connected stream."
    );
    let hook = format!(
        r#"
import errno, json, os, pathlib, socket
state = pathlib.Path(os.environ['MRSM_ROOT_DIR'])
proof = {{}}
try:
    proof['inherited_file'] = os.pread({file_fd}, 128, 0) != b'synthetic-never-granted-secret'
except OSError:
    proof['inherited_file'] = True
try:
    inherited = socket.socket(fileno={stream_fd})
    try:
        proof['inherited_stream'] = inherited.recv(128, socket.MSG_DONTWAIT | socket.MSG_PEEK) != b'synthetic-main-connected-channel'
    except OSError:
        proof['inherited_stream'] = True
    finally:
        inherited.detach()
except OSError:
    proof['inherited_stream'] = True
for name, path in [('server', {server}), ('team', {team}), ('outside', {outside}), ('sibling', {sibling}), ('proc', '/proc/{pid}/environ')]:
    try:
        open(path).read()
        proof[name] = False
    except OSError as e:
        proof[name] = e.errno in (errno.EACCES, errno.EPERM)
try:
    os.kill({pid}, 0)
    proof['signal'] = False
except OSError as e:
    proof['signal'] = e.errno == errno.EPERM
try:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect({socket})
    proof['unix'] = False
except OSError as e:
    proof['unix'] = e.errno in (errno.EACCES, errno.EPERM, errno.EAFNOSUPPORT)
try:
    open({marker}, 'w').write('unsafe')
    proof['outside_write'] = False
except OSError as e:
    proof['outside_write'] = e.errno in (errno.EACCES, errno.EPERM)
try:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(('127.0.0.1', 0))
        listener.listen()
    proof['no_inbound_listener'] = False
except OSError as e:
    proof['no_inbound_listener'] = e.errno in (errno.EACCES, errno.EPERM)
proof['private_state'] = state.name == '.mrsm_team_work'
proof['no_team_env'] = 'DB_PASSWORD' not in os.environ
(state / 'early-proof.json').write_text(json.dumps(proof))
assert all(proof.values()), proof
"#,
        server = json!(server_secret),
        team = json!(team_secret),
        outside = json!(outside_secret),
        sibling = json!(sibling),
        pid = api_pid,
        socket = json!(socket),
        marker = json!(root.join("outside-write")),
        file_fd = file_fd,
        stream_fd = stream_fd
    );
    std::fs::write(
        site.join("boundary.pth"),
        format!("import builtins; exec({})\n", json!(hook)),
    )
    .unwrap();
    let plugin = r#"
import json, os, pathlib, time
import meerschaum as mrsm
from meerschaum.plugins import make_action
state = pathlib.Path(os.environ['MRSM_ROOT_DIR'])
assert all(json.loads((state / 'early-proof.json').read_text()).values())
(state / 'plugin-imported').write_text('confined')
@make_action
def profile_probe(action=None, **kw):
    assert action == ['api', 'value'], action
    instance = mrsm.get_connector('sql:team')
    assert str(instance) == 'sql:team' and instance.type == 'sql' and instance.label == 'team'
    assert str(mrsm.get_connector('sql:executor')) == 'sql:executor'
    try:
        instance.engine
    except RuntimeError:
        pass
    else:
        raise AssertionError('API launch constructed a native SQL engine')
    pipe = mrsm.Pipe('sql:team', 'metric', instance=instance, parameters={'sql':'SELECT must_not_send_local_sql'})
    assert instance.get_pipe_docs(pipe) == [{'value':7}]
    (state / 'first-callback').write_text('ready')
    deadline = time.monotonic() + 20
    while not (state / 'rotate-ready').exists():
        assert time.monotonic() < deadline, 'The fixture did not rotate the callback'
        time.sleep(.02)
    assert instance.get_pipe_docs(pipe) == [{'value':7}]
    (state / 'finished').write_text('success')
    return True, 'API logical callbacks completed.'
"#;
    std::fs::write(root.join("plugins/api_probe.py"), plugin).unwrap();
    let mut archive_builder = tar::Builder::new(flate2::write::GzEncoder::new(
        Vec::new(),
        flate2::Compression::fast(),
    ));
    let mut header = tar::Header::new_gnu();
    header.set_size(plugin.len() as u64);
    header.set_mode(0o440);
    header.set_cksum();
    archive_builder
        .append_data(&mut header, "api_probe.py", plugin.as_bytes())
        .unwrap();
    let archive_bytes = archive_builder.into_inner().unwrap().finish().unwrap();
    let archive_sha256 = meerschaum_server::executor::plugins::sha256(&archive_bytes);
    *archive.lock().unwrap() = archive_bytes;
    let source_catalog = meerschaum_server::executor::plugins::Catalog {
        placement: Default::default(),
        schema: meerschaum_server::executor::plugins::SCHEMA,
        release: env!("CARGO_PKG_VERSION").into(),
        team_id,
        generation,
        plugins: BTreeMap::from([(
            "api_probe".into(),
            meerschaum_server::executor::plugins::Plugin {
                archive_sha256,
                kind: meerschaum_server::executor::plugins::SourceKind::PythonModule,
                metadata: None,
            },
        )]),
    };
    let source_digest =
        meerschaum_server::executor::plugins::sha256(&serde_json::to_vec(&source_catalog).unwrap());
    let configuration = Configuration {
        catalog_sha256: None,
        team_id,
        generation,
        team_name: "ApiFixture".into(),
        plugins: ["api_probe".into()].into_iter().collect(),
        config: core(json!({
            "meerschaum":{"instance":"sql:executor","api_instance":"sql:executor","connectors":{"sql":{
                "executor":{"flavor":"sqlite","database":root.join("executor.db")},
                "team":{"flavor":"sqlite","database":root.join("must-not-use-team.db"),"password":"synthetic-native-team-password"}
            }}}, "plugins":{"secret":"synthetic-plugin-secret"},
            "system":{"packages":{"auto_install":false}},"api":{"no_dash":true}
        })),
    };
    let api = APIConnector::new(
        Some("api-profile-fixture"),
        core(json!({"host":"127.0.0.1","port":port})),
    )
    .unwrap()
    .with_request_authentication(
        Arc::new(Signer {
            authority,
            revision: configuration.fingerprint().unwrap(),
        }),
        1024 * 1024,
    )
    .unwrap();
    let applied = api
        .make_request(
            Method::Put,
            CONFIGURATION_PATH,
            RequestOptions {
                body: Body::Json(serde_json::to_string(&configuration).unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(applied.status, 200, "{}", applied.text());
    let stage = api.make_request(Method::Post,
        meerschaum_server::executor::gateway::PLUGIN_DELIVERY_PATH,
        RequestOptions {body:Body::Json(json!({"catalog":source_catalog, "repository_origin":callback_url,
            "credentials":{"api_probe":{"authorization":"Mrsm-Plugin exact-api-probe-source", "expires_at":chrono::Utc::now().timestamp()+30}}}).to_string()),
            ..Default::default()}).await.unwrap();
    assert_eq!(stage.status, 200, "{}", stage.text());
    assert!(
        !root.join("early-proof.json").exists(),
        "Credentialed control started Python."
    );
    assert!(!root.join("plugin-imported").exists());
    std::fs::create_dir_all(sibling.parent().unwrap()).unwrap();
    std::fs::write(&sibling, "synthetic-other-context-token").unwrap();

    let native_database_before = std::fs::metadata(root.join("must-not-use-team.db"))
        .ok()
        .map(|m| (m.len(), m.modified().unwrap()));
    let make_update = |id, name: String| Update {
        metadata_deadline: None,
        discovery_catalog_sha256: Some(source_digest.clone()),
        extension: None,
        registration: None,
        id,
        revision: 1,
        owner: "operator".into(),
        owner_id: Some("synthetic-operator-id".into()),
        profile: Profile::Api,
        target: Target::Job { name },
        callback_url: callback_url.clone(),
        credential: meerschaum_connector_api::models::ExecutionCredential {
            token: "first-api-token".into(),
            expires_at: chrono::Utc::now().timestamp() + 30,
        },
    };
    let native_id = Uuid::new_v4();
    let native_name = format!("mrsm-{}", native_id.simple());
    fixture.names.push(native_name.clone());
    let mut native_update = make_update(native_id, native_name.clone());
    native_update.discovery_catalog_sha256 = None;
    let delivered = context(&api, &native_update).await;
    assert_eq!(delivered.status, 200, "{}", delivered.text());
    let mut properties = Map::new();
    properties.insert(JOB_CONTEXT_PROPERTY.into(), native_id.to_string().into());
    let created = api
        .create_job(
            &native_name,
            &["show".into(), "version".into()],
            Some(properties),
        )
        .await;
    assert!(created.success, "{}", created.message);
    status(&api, &native_name, "stopped").await;
    let native_properties: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(
            control
                .join("jobs")
                .join(&native_name)
                .join("properties.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(native_properties["result"][0], true, "{native_properties}");
    let native_generation = only_generation(
        &control
            .join("execution-contexts")
            .join(native_id.simple().to_string()),
    );
    assert!(
        !native_generation
            .join(".mrsm_team_work/plugins/api_probe.py")
            .exists(),
        "An empty launch selector inherited the global Team plugin view."
    );
    fixture.snapshot();

    let id = Uuid::new_v4();
    let name = format!("mrsm-{}", id.simple());
    fixture.names.push(name.clone());
    let mut update = make_update(id, name.clone());
    let delivered = context(&api, &update).await;
    assert_eq!(delivered.status, 200, "{}", delivered.text());
    let mut changed_selector = update.clone();
    changed_selector.discovery_catalog_sha256 = Some("b".repeat(64));
    let refused_selector = context(&api, &changed_selector).await;
    assert_eq!(
        refused_selector.status,
        503,
        "A changed immutable selector must not request a source pull: {}",
        refused_selector.text()
    );
    let launch_directory = control
        .join("execution-contexts")
        .join(id.simple().to_string());
    let directory = only_generation(&launch_directory);
    let state = directory.join(".mrsm_team_work");
    let config_text = std::fs::read_to_string(directory.join("config.json")).unwrap();
    for secret in [
        "synthetic-native-team-password",
        "synthetic-plugin-secret",
        "must-not-use-team.db",
    ] {
        assert!(!config_text.contains(secret), "API config leaked {secret}");
    }
    let config: serde_json::Value = serde_json::from_str(&config_text).unwrap();
    assert_eq!(
        config["meerschaum"]["connectors"]["sql"]["team"]["mrsm_api_alias"]["logical_keys"],
        "sql:team"
    );
    let mut properties = core(
        json!({"env":{"DB_PASSWORD":"persisted-native-password","MRSM_CONFIG_FILE":root.join("credentialed-config.json")}}),
    );
    properties.insert(JOB_CONTEXT_PROPERTY.into(), id.to_string().into());
    let created = api
        .create_job(
            &name,
            &[
                "profile".into(),
                "probe".into(),
                "api".into(),
                "value".into(),
            ],
            Some(properties),
        )
        .await;
    assert!(created.success, "{}", created.message);
    fixture.snapshot();
    assert_eq!(
        api.get_job(&name).await.unwrap().sysargs,
        ["profile", "probe", "api", "value"],
        "Signed canonical transport changed the admitted plugin command."
    );
    wait_file(&state.join("first-callback"), &api, &control, &name).await;
    fixture.snapshot();
    update.credential.token = "second-api-token".into();
    update.credential.expires_at = chrono::Utc::now().timestamp() + 30;
    let rotated = context(&api, &update).await;
    assert_eq!(rotated.status, 200, "{}", rotated.text());
    std::fs::write(state.join("rotate-ready"), "rotated").unwrap();
    wait_file(&state.join("finished"), &api, &control, &name).await;
    status(&api, &name, "stopped").await;
    let metadata =
        std::fs::read_to_string(control.join("jobs").join(&name).join("properties.json")).unwrap();
    let completed: serde_json::Value = serde_json::from_str(&metadata).unwrap();
    assert_eq!(completed["result"][0], true, "{completed}");
    for secret in [
        "synthetic-native-team-password",
        "synthetic-plugin-secret",
        "first-api-token",
        "second-api-token",
    ] {
        assert!(!metadata.contains(secret), "Job metadata leaked {secret}");
    }
    let records = requests.lock().unwrap().clone();
    assert_eq!(records.len(), 2, "{records:?}");
    assert_eq!(records[0].1, "Bearer first-api-token");
    assert_eq!(records[1].1, "Bearer second-api-token");
    for (path, _, body) in records {
        assert!(
            path.contains("instance_keys=sql%3Ateam") || path.contains("instance_keys=sql:team"),
            "{path}"
        );
        assert!(!path.contains("must_not_send_local_sql"));
        assert!(!String::from_utf8_lossy(&body).contains("must_not_send_local_sql"));
    }
    let proof: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(state.join("early-proof.json")).unwrap())
            .unwrap();
    assert!(
        proof
            .as_object()
            .unwrap()
            .values()
            .all(|value| value == true),
        "{proof}"
    );
    assert_eq!(
        std::fs::read_to_string(state.join("plugin-imported")).unwrap(),
        "confined"
    );
    assert!(!root.join("outside-write").exists());
    assert_eq!(
        std::fs::metadata(root.join("must-not-use-team.db"))
            .ok()
            .map(|m| (m.len(), m.modified().unwrap())),
        native_database_before,
        "API launch changed the control runtime's native Team database."
    );

    // Reusing an existing definition still needs its current signed context. Revocation stops
    // a running native loop and tombstones the exact revision against replay.
    let loop_id = Uuid::new_v4();
    let loop_name = format!("mrsm-{}", loop_id.simple());
    fixture.names.push(loop_name.clone());
    let mut loop_update = make_update(loop_id, loop_name.clone());
    let delivered = context(&api, &loop_update).await;
    assert_eq!(delivered.status, 200, "{}", delivered.text());
    let mut properties = Map::new();
    properties.insert(JOB_CONTEXT_PROPERTY.into(), loop_id.to_string().into());
    let created = api
        .create_job(
            &loop_name,
            &[
                "show".into(),
                "version".into(),
                "--loop".into(),
                "--min-seconds".into(),
                "30".into(),
            ],
            Some(properties),
        )
        .await;
    assert!(created.success, "{}", created.message);
    status(&api, &loop_name, "running").await;
    fixture.snapshot();
    assert!(api.pause_job(&loop_name).await.success);
    status(&api, &loop_name, "paused").await;
    let paused_pid = api.get_job(&loop_name).await.unwrap().daemon.pid;
    let loop_directory = control
        .join("execution-contexts")
        .join(loop_id.simple().to_string());
    let old_generation = only_generation(&loop_directory);
    // Model writes the old confined process is permitted to make. Preparing revision 2
    // must never walk this ancestor or its symlinked initializer, even while it is paused.
    let hostile = root.join("host-write-sentinel");
    std::fs::create_dir(&hostile).unwrap();
    std::fs::create_dir(hostile.join("plugins")).unwrap();
    let empty = hostile.join("empty.py");
    std::fs::write(&empty, "").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&empty, std::fs::Permissions::from_mode(0o440)).unwrap();
    std::os::unix::fs::symlink(&empty, hostile.join("plugins/__init__.py")).unwrap();
    let old_internal = old_generation.join(".mrsm_team_work/.internal");
    std::fs::rename(&old_internal, old_internal.with_file_name(".internal-old")).unwrap();
    std::os::unix::fs::symlink(&hostile, &old_internal).unwrap();
    std::fs::set_permissions(&hostile, std::fs::Permissions::from_mode(0o750)).unwrap();
    loop_update.credential.expires_at = chrono::Utc::now().timestamp() + 30;
    let renewed = context(&api, &loop_update).await;
    assert_eq!(renewed.status, 200, "{}", renewed.text());
    assert_eq!(only_generation(&loop_directory), old_generation);
    // A privileged control sender can rotate a context while a process is paused, but the
    // old kernel grants cannot change. Neither API start nor direct resume may SIGCONT it.
    loop_update.revision = 2;
    loop_update.credential.token = "paused-rotation-token".into();
    loop_update.credential.expires_at = chrono::Utc::now().timestamp() + 30;
    let delivered = context(&api, &loop_update).await;
    assert_eq!(delivered.status, 200, "{}", delivered.text());
    let entries = std::fs::read_dir(&loop_directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    let generations = entries
        .iter()
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("generation-")
        })
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(generations.len(), 2, "{generations:?}");
    for name in ["identity.json", "job-lifecycle-1.json"] {
        let path = loop_directory.join(name);
        let metadata = std::fs::symlink_metadata(&path).unwrap();
        assert!(
            metadata.is_file() && !metadata.is_symlink(),
            "Unsealed lifecycle descriptor: {path:?}"
        );
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }
    let expected = generations
        .iter()
        .cloned()
        .chain([
            loop_directory.join("identity.json"),
            loop_directory.join("job-lifecycle-1.json"),
        ])
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        entries
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        expected,
        "Unexpected context entries."
    );
    let new_generation = generations
        .iter()
        .find(|path| **path != old_generation)
        .unwrap();
    assert!(
        std::fs::symlink_metadata(&old_internal)
            .unwrap()
            .is_symlink()
    );
    assert_eq!(std::fs::read_to_string(&empty).unwrap(), "");
    assert_eq!(
        std::fs::metadata(&empty).unwrap().permissions().mode() & 0o777,
        0o440
    );
    assert_eq!(
        std::fs::metadata(&hostile).unwrap().permissions().mode() & 0o777,
        0o750
    );
    assert!(
        !std::fs::symlink_metadata(
            new_generation.join(".mrsm_team_work/.internal/plugins/__init__.py")
        )
        .unwrap()
        .is_symlink()
    );
    let old_spec: meerschaum_core::team_scope::sandbox::Spec =
        serde_json::from_slice(&std::fs::read(old_generation.join(".mrsm_sandbox.json")).unwrap())
            .unwrap();
    let new_spec: meerschaum_core::team_scope::sandbox::Spec =
        serde_json::from_slice(&std::fs::read(new_generation.join(".mrsm_sandbox.json")).unwrap())
            .unwrap();
    for (spec, other) in [
        (&old_spec, new_generation.as_path()),
        (&new_spec, old_generation.as_path()),
    ] {
        assert!(
            !spec.rules.iter().any(|rule| other.starts_with(&rule.path)),
            "A generation can read another generation: {spec:?}"
        );
    }
    let resumed = api.start_job(&loop_name).await;
    assert!(
        !resumed.success && resumed.message.contains("Stop it, then start it"),
        "{resumed}"
    );
    let mut direct = meerschaum_jobs::Job::open(
        &loop_name,
        &meerschaum_jobs::JobsContext::for_root(&control),
    )
    .unwrap();
    let resumed = direct.resume(None);
    assert!(
        !resumed.success && resumed.message.contains("Stop it, then start it"),
        "{resumed}"
    );
    status(&api, &loop_name, "paused").await;
    assert_eq!(
        api.get_job(&loop_name).await.unwrap().daemon.pid,
        paused_pid
    );
    let retired = api
        .make_request(
            Method::Delete,
            &format!("{CONTEXTS_PATH}{loop_id}"),
            RequestOptions {
                body: Body::Json(json!({"revision":2}).to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(retired.status, 200, "{}", retired.text());
    status(&api, &loop_name, "stopped").await;
    assert!(
        !control
            .join("execution-contexts")
            .join(loop_id.simple().to_string())
            .exists()
    );
    assert_ne!(context(&api, &loop_update).await.status, 200);
    let denied = api.start_job(&loop_name).await;
    assert!(
        !denied.success,
        "A retired API job restarted without fresh authority."
    );
    fixture.snapshot();
}
