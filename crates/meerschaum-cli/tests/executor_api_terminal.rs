//! Signed API-only mrsm terminals keep their isolated context across reconnects and windows.
#![cfg(target_os = "linux")]
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::authentication::{Request, RequestAuthentication};
use meerschaum_connector_api::client::{Body, Method, RequestOptions, SocketMessage};
use meerschaum_connector_api::traits::{ConnectorError, ConnectorResult};
use meerschaum_core::{Map, Value};
use meerschaum_server::executor::control::{Actor, Authority};
use meerschaum_server::executor::environment::{Profile, Target, Update};
use meerschaum_server::executor::gateway::{
    CONFIGURATION_PATH, CONTEXTS_PATH, Configuration, TERMINALS_PATH,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;
struct Signer {
    authority: Authority,
    revision: String,
}
impl RequestAuthentication for Signer {
    fn authorization(&self, request: Request<'_>) -> ConnectorResult<String> {
        self.authority
            .issue(
                Actor {
                    owner: "operator".into(),
                    username: Some("operator".into()),
                    permissions: [
                        "webterm:use".into(),
                        "actions:execute".into(),
                        "jobs:execute".into(),
                    ]
                    .into_iter()
                    .collect(),
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

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = meerschaum_jobs::process::send_signal(
            self.0.id() as i32,
            meerschaum_jobs::process::Sig::Term,
        );
        let deadline = Instant::now() + Duration::from_secs(8);
        while Instant::now() < deadline {
            if self.0.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
struct Task(tokio::task::JoinHandle<()>);
impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn core(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}
fn archive(name: &str, source: &str) -> Vec<u8> {
    let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
        Vec::new(),
        flate2::Compression::fast(),
    ));
    let mut header = tar::Header::new_gnu();
    header.set_size(source.len() as u64);
    header.set_mode(0o440);
    header.set_cksum();
    builder
        .append_data(&mut header, format!("{name}.py"), source.as_bytes())
        .unwrap();
    builder.into_inner().unwrap().finish().unwrap()
}
async fn request(api: &APIConnector, method: Method, path: &str, body: serde_json::Value) -> u16 {
    let response = api
        .make_request(
            method,
            path,
            RequestOptions {
                body: Body::Json(body.to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(response.status, 200, "{}", response.text());
    response.status
}
fn guardians(directory: &Path) -> std::collections::BTreeSet<std::ffi::OsString> {
    std::fs::read_dir(directory)
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.unwrap();
            let text = entry.file_name().to_string_lossy().into_owned();
            text.strip_prefix("terminal-")
                .and_then(|text| text.strip_suffix(".json"))
                .and_then(|pid| pid.parse::<i32>().ok())
                .filter(|pid| *pid > 0)
                .map(|_| entry.file_name())
        })
        .collect()
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
async fn wait_guardians(
    directory: &Path,
    count: usize,
) -> std::collections::BTreeMap<std::ffi::OsString, [meerschaum_jobs::supervisor::LifecycleOwner; 2]>
{
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let current = guardians(directory);
        if current.len() == count {
            return guardian_identities(directory);
        }
        assert!(
            Instant::now() < deadline,
            "Expected {count} owned pane guardians, found {current:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn guardian_identities(
    directory: &Path,
) -> std::collections::BTreeMap<std::ffi::OsString, [meerschaum_jobs::supervisor::LifecycleOwner; 2]>
{
    guardians(directory)
        .into_iter()
        .map(|filename| {
            let descriptor: serde_json::Value =
                serde_json::from_slice(&std::fs::read(directory.join(&filename)).unwrap()).unwrap();
            let identities = ["guardian", "child"].map(|key| {
                let identity =
                    serde_json::from_value::<meerschaum_jobs::supervisor::LifecycleOwner>(
                        descriptor[key].clone(),
                    )
                    .unwrap();
                assert!(
                    identity.alive(),
                    "The terminal process is not alive: {identity:?}"
                );
                identity
            });
            (filename, identities)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_api_console_uses_current_callback_policy_and_reconnects_only_to_its_restricted_guarded_panes()
 {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("server");
    let control = temp.path().join("factory-control");
    std::fs::create_dir_all(&control).unwrap();
    let venv = temp.path().join("fixture-runtime");
    std::fs::create_dir_all(root.join("plugins")).unwrap();
    std::fs::create_dir_all(venv.join("bin")).unwrap();
    let secret = root.join("server.secret");
    let outside = root.join("outside-marker");
    std::fs::write(&secret, "synthetic-never-granted").unwrap();
    let socket_path = root.join("host.sock");
    let _unix = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let python = std::env::var_os("MRSM_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|| "/usr/bin/python3".into());
    let real = std::fs::canonicalize(python).unwrap();
    let version = real.file_name().unwrap().to_string_lossy();
    let version = version
        .strip_prefix("python")
        .filter(|value| {
            !value.is_empty()
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || byte == b'.' || byte == b't')
        })
        .unwrap();
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
    let plugin = r#"
import json,os,pathlib
from meerschaum.plugins import make_action
state=pathlib.Path(os.environ['MRSM_ROOT_DIR'])
assert os.environ.get('MRSM_API_EXECUTION')=='1'
assert all(json.loads((state/'early-proof.json').read_text()).values())
@make_action
def terminal_probe(action=None,**kw):
    (state/'terminal-plugin-positive').write_text('confined')
    return True,'Confined terminal plugin completed.'
"#;
    std::fs::write(root.join("plugins/terminal_probe.py"), plugin).unwrap();
    let archives = Arc::new(BTreeMap::from([
        (
            "terminal_probe".to_string(),
            archive("terminal_probe", plugin),
        ),
        (
            "ungranted_probe".to_string(),
            archive(
                "ungranted_probe",
                "raise AssertionError('Ungrantable source must never import')\n",
            ),
        ),
    ]));
    let seen = Arc::new(Mutex::new(Vec::<(String, Vec<String>)>::new()));
    let observed = seen.clone();
    let sources = archives.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let callback_url = format!("http://{}", listener.local_addr().unwrap());
    let routes=axum::Router::new().fallback(move |request:axum::extract::Request| {
        let observed=observed.clone(); let sources=sources.clone(); async move {
            use axum::response::IntoResponse;
            if let Some(name)=request.uri().path().strip_prefix("/plugins/") {
                if request.headers().get("authorization").and_then(|value|value.to_str().ok())!=Some("Mrsm-Plugin exact-terminal-source") {
                    return axum::http::StatusCode::FORBIDDEN.into_response();
                }
                return sources.get(name).cloned().map(IntoResponse::into_response)
                    .unwrap_or_else(||axum::http::StatusCode::NOT_FOUND.into_response());
            }
            if request.uri().path() != "/execute/validate" {
                return axum::Json(json!({"success":false,"message":"This fixture exposes only terminal command validation."})).into_response();
            }
            let token=request.headers().get("authorization").and_then(|value|value.to_str().ok()).unwrap_or("").to_string();
            let bytes=axum::body::to_bytes(request.into_body(),1024*1024).await.unwrap();
            let body:serde_json::Value=serde_json::from_slice(&bytes).unwrap();
            let args=body["sysargs"].as_array().unwrap().iter().map(|value|value.as_str().unwrap().to_string()).collect::<Vec<_>>();
            let allowed=matches!(args.iter().map(String::as_str).collect::<Vec<_>>().as_slice(),["show","version"]|["terminal","probe"]);
            observed.lock().unwrap().push((token,args));
            axum::Json(json!({"success":allowed,"message":if allowed {"Allowed confined action."} else {"Fixture policy refused this system command; nothing was run."}})).into_response()
        }
    });
    let _callback = Task(tokio::spawn(async move {
        axum::serve(listener, routes).await.unwrap();
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
        .env("MRSM_PYTHON", python)
        .env("MRSM_EXECUTOR_BOOTSTRAP", bootstrap)
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env("MRSM_NO_UPDATE_CHECK", "1")
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
    let mut process = Process(command.spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(15);
    while tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .is_err()
    {
        assert!(
            process.0.try_wait().unwrap().is_none() && Instant::now() < deadline,
            "{}",
            std::fs::read_to_string(&log_path).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let hook = format!(
        r#"
import errno,json,os,pathlib,socket
state=pathlib.Path(os.environ['MRSM_ROOT_DIR'])
proof={{}}
for name,path in [('secret',{secret}),('proc','/proc/{pid}/environ')]:
    try: open(path).read(); proof[name]=False
    except OSError as e: proof[name]=e.errno in (errno.EACCES,errno.EPERM)
try: open({outside},'w').write('unsafe'); proof['outside']=False
except OSError as e: proof['outside']=e.errno in (errno.EACCES,errno.EPERM)
try: os.kill({pid},0); proof['signal']=False
except OSError as e: proof['signal']=e.errno==errno.EPERM
try:
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as client: client.connect({socket})
    proof['unix']=False
except OSError as e: proof['unix']=e.errno in (errno.EACCES,errno.EPERM,errno.EAFNOSUPPORT)
proof['private_state']=state.name=='state'
(state/'early-proof.json').write_text(json.dumps(proof))
assert all(proof.values()),proof
"#,
        secret = json!(secret),
        outside = json!(outside),
        pid = process.0.id(),
        socket = json!(socket_path)
    );
    std::fs::write(
        site.join("terminal-boundary.pth"),
        format!("import builtins; exec({}, {{}})\n", json!(hook)),
    )
    .unwrap();
    let configuration = Configuration {
        catalog_sha256: None,
        team_id,
        generation,
        team_name: "ApiTerminalFixture".into(),
        plugins: ["terminal_probe".into()].into_iter().collect(),
        config: core(
            json!({"meerschaum":{"instance":"sql:executor","api_instance":"sql:executor","connectors":{"sql":{"executor":{"flavor":"sqlite","database":root.join("executor.db"),"password":"synthetic-team-password"}}}},"api":{"no_dash":true},"system":{"packages":{"auto_install":false}}}),
        ),
    };
    let api = APIConnector::new(
        Some("terminal-fixture"),
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
    request(
        &api,
        Method::Put,
        CONFIGURATION_PATH,
        serde_json::to_value(configuration).unwrap(),
    )
    .await;
    let mut digests = BTreeMap::new();
    for (name, bytes) in archives.iter() {
        let catalog = meerschaum_server::executor::plugins::Catalog {
            placement: Default::default(),
            schema: meerschaum_server::executor::plugins::SCHEMA,
            release: env!("CARGO_PKG_VERSION").into(),
            team_id,
            generation,
            plugins: BTreeMap::from([(
                name.clone(),
                meerschaum_server::executor::plugins::Plugin {
                    archive_sha256: meerschaum_server::executor::plugins::sha256(bytes),
                    kind: meerschaum_server::executor::plugins::SourceKind::PythonModule,
                    metadata: None,
                },
            )]),
        };
        digests.insert(
            name.clone(),
            meerschaum_server::executor::plugins::sha256(&serde_json::to_vec(&catalog).unwrap()),
        );
        let credentials = BTreeMap::from([(
            name.clone(),
            json!({"authorization":"Mrsm-Plugin exact-terminal-source","expires_at":chrono::Utc::now().timestamp()+30}),
        )]);
        request(
            &api,
            Method::Post,
            meerschaum_server::executor::gateway::PLUGIN_DELIVERY_PATH,
            json!({"catalog":catalog,"repository_origin":callback_url,"credentials":credentials}),
        )
        .await;
    }
    let id = Uuid::new_v4();
    request(
        &api,
        Method::Put,
        &format!("{TERMINALS_PATH}{id}"),
        json!({"id":id,"owner":"operator","owner_id":"registered-operator"}),
    )
    .await;
    let mut update = Update {
        metadata_deadline: None,
        extension: None,
        discovery_catalog_sha256: Some(digests["terminal_probe"].clone()),
        registration: None,
        id,
        revision: 1,
        owner: "operator".into(),
        owner_id: Some("registered-operator".into()),
        profile: Profile::Api,
        target: Target::Terminal,
        callback_url: callback_url.clone(),
        credential: meerschaum_connector_api::models::ExecutionCredential {
            token: "first-terminal-token".into(),
            expires_at: chrono::Utc::now().timestamp() + 30,
        },
    };
    request(
        &api,
        Method::Put,
        &format!("{CONTEXTS_PATH}{id}"),
        serde_json::to_value(&update).unwrap(),
    )
    .await;
    let mut changed = update.clone();
    changed.discovery_catalog_sha256 = Some(digests["ungranted_probe"].clone());
    let changed_response = api
        .make_request(
            Method::Put,
            &format!("{CONTEXTS_PATH}{id}"),
            RequestOptions {
                body: Body::Json(serde_json::to_string(&changed).unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        changed_response.status,
        503,
        "A live terminal changed its immutable selector: {}",
        changed_response.text()
    );
    let ungranted_id = Uuid::new_v4();
    request(
        &api,
        Method::Put,
        &format!("{TERMINALS_PATH}{ungranted_id}"),
        json!({"id":ungranted_id,"owner":"operator","owner_id":"registered-operator"}),
    )
    .await;
    changed.id = ungranted_id;
    let ungranted_response = api
        .make_request(
            Method::Put,
            &format!("{CONTEXTS_PATH}{ungranted_id}"),
            RequestOptions {
                body: Body::Json(serde_json::to_string(&changed).unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        ungranted_response.status,
        503,
        "A staged source outside the Team grant became a terminal: {}",
        ungranted_response.text()
    );
    assert!(
        !control
            .join("execution-contexts")
            .join(ungranted_id.simple().to_string())
            .exists()
    );
    request(
        &api,
        Method::Delete,
        &format!("{TERMINALS_PATH}{ungranted_id}"),
        json!(null),
    )
    .await;
    let launch_directory = control
        .join("execution-contexts")
        .join(id.simple().to_string());
    let directory = only_generation(&launch_directory);
    let state = directory.join("state");
    assert!(
        !std::fs::read_to_string(directory.join("config.json"))
            .unwrap()
            .contains("synthetic-team-password")
    );
    let mut terminal = api
        .open_authenticated_socket(&format!("/websocket/{id}"))
        .await
        .unwrap();
    let mut output = String::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !output.contains("mrsm") {
        let message = tokio::time::timeout(Duration::from_secs(2), terminal.next()).await;
        if let Ok(Ok(Some(SocketMessage::Text(raw)))) = message {
            let wire: serde_json::Value = serde_json::from_str(&raw).unwrap();
            if wire[0] == "stdout" {
                output.push_str(wire[1].as_str().unwrap());
            }
            assert_ne!(
                wire[0],
                "disconnect",
                "{output}\n{}",
                std::fs::read_to_string(&log_path).unwrap()
            );
        }
        assert!(
            Instant::now() < deadline,
            "No interactive API shell: {output}\n{}",
            std::fs::read_to_string(&log_path).unwrap()
        );
    }
    let first_guard = wait_guardians(&directory, 1).await;
    terminal
        .send(SocketMessage::Text(
            json!(["stdin", "show version\r"]).to_string(),
        ))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let message = tokio::time::timeout(Duration::from_millis(250), terminal.next()).await;
        if let Ok(Ok(Some(SocketMessage::Text(raw)))) = message {
            let wire: serde_json::Value = serde_json::from_str(&raw).unwrap();
            if wire[0] == "stdout" {
                output.push_str(wire[1].as_str().unwrap());
            }
        }
        if seen.lock().unwrap().iter().any(|(token, args)| {
            token == "Bearer first-terminal-token" && args == &["show", "version"]
        }) && output.contains(env!("CARGO_PKG_VERSION"))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "No native action result through bounded policy: {output}"
        );
    }
    terminal
        .send(SocketMessage::Text(
            json!(["stdin", "terminal probe\r"]).to_string(),
        ))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !state.join("terminal-plugin-positive").is_file() {
        if let Ok(Ok(Some(SocketMessage::Text(raw)))) =
            tokio::time::timeout(Duration::from_millis(250), terminal.next()).await
        {
            let wire: serde_json::Value = serde_json::from_str(&raw).unwrap();
            if wire[0] == "stdout" {
                output.push_str(wire[1].as_str().unwrap());
            }
        }
        assert!(
            Instant::now() < deadline,
            "Granted terminal plugin never ran: {output}"
        );
    }
    assert_eq!(
        std::fs::read_to_string(state.join("terminal-plugin-positive")).unwrap(),
        "confined"
    );
    assert!(!state.join("plugins/ungranted_probe.py").exists());
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .any(|(token, args)| token == "Bearer first-terminal-token"
                && args == &["terminal", "probe"])
    );
    let denied = format!("sh touch {}\r", outside.display());
    terminal
        .send(SocketMessage::Text(json!(["stdin", denied]).to_string()))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !output.contains("Fixture policy refused") {
        if let Ok(Ok(Some(SocketMessage::Text(raw)))) =
            tokio::time::timeout(Duration::from_millis(250), terminal.next()).await
        {
            let wire: serde_json::Value = serde_json::from_str(&raw).unwrap();
            if wire[0] == "stdout" {
                output.push_str(wire[1].as_str().unwrap());
            }
        }
        assert!(Instant::now() < deadline, "No API shell refusal: {output}");
    }
    assert!(!outside.exists());
    let requests_before = seen.lock().unwrap().len();
    output.clear();
    terminal
        .send(SocketMessage::Text(
            json!(["stdin", "show version --_mrsm-confined-python\r"]).to_string(),
        ))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !output.contains("not user commands") {
        if let Ok(Ok(Some(SocketMessage::Text(raw)))) =
            tokio::time::timeout(Duration::from_millis(250), terminal.next()).await
        {
            let wire: serde_json::Value = serde_json::from_str(&raw).unwrap();
            if wire[0] == "stdout" {
                output.push_str(wire[1].as_str().unwrap());
            }
        }
        assert!(
            Instant::now() < deadline,
            "Raw internal flag was not refused: {output}"
        );
    }
    assert_eq!(
        seen.lock().unwrap().len(),
        requests_before,
        "The parser hid an internal flag before policy authorization."
    );
    terminal.close().await;
    update.credential.token = "second-terminal-token".into();
    update.credential.expires_at = chrono::Utc::now().timestamp() + 30;
    request(
        &api,
        Method::Put,
        &format!("{CONTEXTS_PATH}{id}"),
        serde_json::to_value(&update).unwrap(),
    )
    .await;
    let mut terminal = api
        .open_authenticated_socket(&format!("/websocket/{id}"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        guardian_identities(&directory),
        first_guard,
        "Reconnect started another pane process."
    );
    terminal
        .send(SocketMessage::Text(
            json!(["stdin", "show version\r"]).to_string(),
        ))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !seen.lock().unwrap().iter().any(|(token, args)| {
        token == "Bearer second-terminal-token" && args == &["show", "version"]
    }) {
        let _ = tokio::time::timeout(Duration::from_millis(100), terminal.next()).await;
        assert!(
            Instant::now() < deadline,
            "Reconnected shell did not read rotated policy credential."
        );
    }
    terminal
        .send(SocketMessage::Text(json!(["new_window"]).to_string()))
        .await
        .unwrap();
    wait_guardians(&directory, 2).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last_readiness = serde_json::Value::Null;
    loop {
        terminal
            .send(SocketMessage::Text(
                json!(["shell_state", "readiness"]).to_string(),
            ))
            .await
            .unwrap();
        if let Ok(Ok(Some(SocketMessage::Text(raw)))) =
            tokio::time::timeout(Duration::from_millis(200), terminal.next()).await
        {
            let wire: serde_json::Value = serde_json::from_str(&raw).unwrap();
            if wire[0] == "shell_state" && wire[1] == "readiness" {
                last_readiness = wire[2].clone();
                if last_readiness.get("idle").is_some() {
                    break;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "Guardian child readiness unavailable: {last_readiness}; guardian descriptors: {:?}; shell-state entries: {:?}; output: {output}; API log: {}",
            guardians(&directory),
            std::fs::read_dir(state.join("shell-state"))
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>(),
            std::fs::read_to_string(&log_path).unwrap()
        );
    }
    let proof: serde_json::Value =
        serde_json::from_slice(&std::fs::read(state.join("early-proof.json")).unwrap()).unwrap();
    assert!(
        proof
            .as_object()
            .unwrap()
            .values()
            .all(|value| value == true),
        "{proof}"
    );
    let owned = guardians(&directory)
        .into_iter()
        .flat_map(|filename| {
            let descriptor: serde_json::Value =
                serde_json::from_slice(&std::fs::read(directory.join(filename)).unwrap()).unwrap();
            ["guardian", "child"].map(|key| {
                serde_json::from_value::<meerschaum_jobs::supervisor::LifecycleOwner>(
                    descriptor[key].clone(),
                )
                .unwrap()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(owned.len(), 4);
    assert!(owned.iter().all(|identity| identity.alive()));
    request(
        &api,
        Method::Delete,
        &format!("{CONTEXTS_PATH}{id}"),
        json!({"revision":1}),
    )
    .await;
    assert!(!directory.exists());
    assert!(!launch_directory.exists());
    let deadline = Instant::now() + Duration::from_secs(5);
    while owned.iter().any(|identity| identity.alive()) {
        assert!(
            Instant::now() < deadline,
            "Revoked terminal processes remain alive: {owned:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let refused = api
        .open_authenticated_socket(&format!("/websocket/{id}"))
        .await;
    if let Ok(mut refused) = refused {
        // Webterm preserves v4's accept-then-refuse websocket behavior. A successful HTTP
        // upgrade grants no shell: require its explicit disconnect and actual close.
        let mut disconnected = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match tokio::time::timeout(Duration::from_millis(250), refused.next()).await {
                Ok(Ok(Some(SocketMessage::Text(raw)))) => {
                    let wire: serde_json::Value = serde_json::from_str(&raw).unwrap();
                    disconnected |= wire[0] == "disconnect" && wire[1] == 1;
                }
                Ok(Ok(None)) => break,
                Ok(Err(error)) => {
                    panic!("Revoked socket failed before its refusal completed: {error}")
                }
                _ => {}
            }
            assert!(
                Instant::now() < deadline,
                "Revoked terminal websocket stayed open."
            );
        }
        assert!(disconnected, "Revoked terminal never sent its refusal.");
    }
    assert!(
        !launch_directory.exists(),
        "A revoked connection recreated its context."
    );
    assert!(owned.iter().all(|identity| !identity.alive()));
    terminal.close().await;
}
