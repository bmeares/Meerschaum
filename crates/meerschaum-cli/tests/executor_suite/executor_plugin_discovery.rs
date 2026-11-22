//! Real Main archive pulls and a canonical, credential-free native metadata producer.

#[path = "../common/metadata_payload.rs"]
mod metadata_payload;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use meerschaum_cli::executor::client::Controller;
use meerschaum_cli::executor::jobs::Catalog;
use meerschaum_cli::executor::registry::Registry;
use meerschaum_connector_api::client::{Body, Method, RequestOptions};
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Value};
use meerschaum_server::executor::{callbacks, context, environment};
use serde_json::{Value as Json, json};
use uuid::Uuid;

fn map(value: Json) -> Map {
    serde_json::from_value::<Value>(value).unwrap().to_map()
}
struct Task(tokio::task::JoinHandle<()>);
impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct Executor {
    child: Child,
    root: PathBuf,
}
impl Drop for Executor {
    fn drop(&mut self) {
        use meerschaum_jobs::process;
        let mut owned = BTreeMap::new();
        let mut parents = vec![self.child.id() as i32];
        // Only this private root's native detached supervisors are included.
        for (pid, args) in process::process_cmdlines() {
            if args.windows(2).any(|pair| {
                pair[0] == "--jobs-dir" && Path::new(&pair[1]) == self.root.join("jobs")
            }) && args
                .iter()
                .any(|arg| arg == meerschaum_jobs::supervisor::SUPERVISOR_ARG)
            {
                parents.push(pid);
            }
        }
        while let Some(pid) = parents.pop() {
            if owned.contains_key(&pid) {
                continue;
            }
            if let Some(birth) = process::process_start_time(pid) {
                owned.insert(pid, birth);
                parents.extend(process::children(pid).unwrap_or_default());
            }
        }
        let _ = process::send_signal(self.child.id() as i32, process::Sig::Int);
        let until = Instant::now() + Duration::from_secs(3);
        while Instant::now() < until && self.child.try_wait().ok().flatten().is_none() {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        for (pid, birth) in owned {
            if process::process_start_time(pid) == Some(birth)
                && matches!(
                    process::proc_state(pid),
                    process::ProcState::Alive | process::ProcState::Stopped
                )
            {
                let _ = process::send_signal(pid, process::Sig::Kill);
            }
            if let Some(pid) = rustix::process::Pid::from_raw(pid) {
                let _ = rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::NOHANG);
            }
        }
    }
}
struct Subreaper(Option<rustix::process::Pid>);
impl Subreaper {
    fn new() -> Self {
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
fn archive(path: &Path, name: &str, source: &str) {
    std::fs::create_dir_all(path).unwrap();
    let file = std::fs::File::create(path.join(format!("{name}.tar.gz"))).unwrap();
    let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
    let mut tar = tar::Builder::new(encoder);
    let mut header = tar::Header::new_gnu();
    header.set_size(source.len() as u64);
    header.set_mode(0o400);
    header.set_cksum();
    tar.append_data(&mut header, format!("{name}.py"), source.as_bytes())
        .unwrap();
    tar.into_inner().unwrap().finish().unwrap();
}
fn diagnostics(root: &Path) -> String {
    let mut text = String::new();
    let mut paths = vec![root.join("launcher.log"), root.join("daemon_errors.log")];
    for directory in [root.join("logs"), root.join("jobs")] {
        if let Ok(entries) = std::fs::read_dir(directory) {
            for entry in entries.flatten().take(12) {
                if entry.path().is_dir() {
                    paths.push(entry.path().join("properties.json"));
                } else {
                    paths.push(entry.path());
                }
            }
        }
    }
    for path in paths {
        if let Ok(metadata) = std::fs::symlink_metadata(&path)
            && metadata.is_file()
            && metadata.len() < 128 * 1024
            && let Ok(value) = std::fs::read_to_string(&path)
        {
            let value = meerschaum_server::redact::mask_uri_passwords(&value);
            text.push_str(&format!(
                "\n{}: {}",
                path.strip_prefix(root).unwrap_or(&path).display(),
                value
            ));
        }
    }
    text
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_metadata_has_no_main_credentials_or_network_and_cannot_accept_jwt_rotation() {
    use std::os::unix::fs::symlink;
    let _subreaper = Subreaper::new();
    let temp = tempfile::tempdir().unwrap();
    let main = temp.path().join("main");
    let remote = temp.path().join("remote");
    let control = temp.path().join("factory-control");
    std::fs::create_dir(&control).unwrap();
    let venv = temp.path().join("interpreter");
    for root in [&main, &remote] {
        std::fs::create_dir_all(root.join("plugins")).unwrap();
    }
    std::fs::write(main.join("main.secret"), "main-callback-and-db-secret").unwrap();
    std::fs::create_dir_all(control.join("config")).unwrap();
    std::fs::write(
        control.join("config/server.secret"),
        "executor-server-secret",
    )
    .unwrap();
    let real = std::fs::canonicalize(
        std::env::var_os("MRSM_PYTHON")
            .map(PathBuf::from)
            .unwrap_or_else(|| "/usr/bin/python3".into()),
    )
    .unwrap();
    let filename = real.file_name().unwrap().to_string_lossy();
    let version = filename
        .strip_prefix("python")
        .filter(|value| {
            !value.is_empty()
                && value
                    .bytes()
                    .all(|c| c.is_ascii_digit() || c == b'.' || c == b't')
        })
        .unwrap();
    let site = venv.join(format!("lib/python{version}/site-packages"));
    std::fs::create_dir_all(&site).unwrap();
    std::fs::create_dir_all(venv.join("bin")).unwrap();
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
    let probe = format!(
        r#"
import errno, json, os, pathlib, socket, stat
state=pathlib.Path(os.environ['MRSM_ROOT_DIR'])
config=json.loads(pathlib.Path(os.environ['MRSM_CONFIG_FILE']).read_text())
proof={{}}
for name,path in [('main',{main}),('server',{server}),('proc',{proc})]:
    try: open(path).read(); proof[name]=False
    except OSError as error: proof[name]=error.errno in (errno.EACCES,errno.EPERM)
serialized=json.dumps(config)
proof['no_data']=all(value not in serialized for value in ['sql:main','sql:team','main-callback-and-db-secret','main-plugin-secret','executor-server-secret','proxy-password'])
proof['private_root']=state.name=='.mrsm_team_work'
proof['no_owner_input']=stat.S_ISCHR(os.fstat(0).st_mode) and not os.isatty(0) and os.read(0,1)==b''
proof['noninteractive']=os.environ.get('MRSM_NONINTERACTIVE')=='1'
proof['no_credential_env']=all(key not in os.environ for key in ['HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','AWS_SECRET_ACCESS_KEY','DB_PASSWORD','AUTHORIZATION'])
callback=config['meerschaum']['connectors']['api']['main_callback']
credential=json.loads(pathlib.Path(callback['credential_file']).read_text())
proof['no_callback']=credential['token']=='mrsm-metadata-no-authority' and callback['uri']=='http://metadata.invalid'
for name,family,kind in [('tcp',socket.AF_INET,socket.SOCK_STREAM),('udp',socket.AF_INET6,socket.SOCK_DGRAM),('unix',socket.AF_UNIX,socket.SOCK_STREAM)]:
    try: socket.socket(family,kind); proof[name]=False
    except OSError as error: proof[name]=error.errno in (errno.EPERM,errno.EACCES,errno.EAFNOSUPPORT)
try: pathlib.Path({outside_marker}).write_text('unsafe'); proof['outside_write']=False
except OSError as error: proof['outside_write']=error.errno in (errno.EACCES,errno.EPERM)
(state/'early-proof.json').write_text(json.dumps(proof))
assert all(proof.values()),proof
"#,
        main = json!(main.join("main.secret")),
        proc = json!(format!("/proc/{}/environ", std::process::id())),
        server = json!(control.join("config/server.secret")),
        outside_marker = json!(remote.join("control-imported"))
    );
    std::fs::write(
        site.join("metadata-boundary.pth"),
        format!("import builtins; exec({}, {{}})\n", json!(probe)),
    )
    .unwrap();
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    let snapshot = map(
        json!({"meerschaum":{"instance":"sql:main","api_instance":"sql:main","connectors":{"sql":{"main":{"flavor":"sqlite","database":main.join("main.db"),"password":"main-callback-and-db-secret"},"team":{"flavor":"sqlite","database":remote.join("team.db"),"password":"main-callback-and-db-secret"}},"api":{"executor":{"host":"127.0.0.1","port":port}}}},"plugins":{"manifest_probe":{"secret":"main-plugin-secret"}},"api":{"no_dash":true},"system":{"packages":{"auto_install":false}}}),
    );
    let options = ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), main.display().to_string()),
        (
            "MRSM_PLUGINS_DIR".into(),
            main.join("plugins").display().to_string(),
        ),
    ]);
    let config = Config::new(options.clone()).unwrap();
    config.set_config(snapshot.clone());
    let session = meerschaum_cli::native::Session::with_runtime(tokio::runtime::Handle::current());
    session.set_python_worker(false);
    let instance =
        tokio::task::spawn_blocking(move || session.instance("sql:main", &config).unwrap().conn())
            .await
            .unwrap();
    let mut user = User::new("metadata_operator", Some("synthetic-metadata-pass"));
    user.attributes = Some(map(
        json!({"role":"operator","primary_team":"DiscoveryTeam"}),
    ));
    assert!(instance.register_user(&mut user).await.success);
    let mut team = Team::new("DiscoveryTeam");
    team.attributes = map(
        json!({"isolated":true,"plugins_write":true,"plugins":["manifest_probe"],"instance":"sql:team","connectors":["sql:team"],"execution":{"mode":"api","connector":"api:executor"}}),
    );
    team.members
        .insert("metadata_operator".into(), TeamRole::Member);
    let registered = instance.register_team(&mut team).await;
    assert!(registered.success, "{registered}");
    let desired = Registry::new(&main).reconcile(&[team.clone()]).unwrap();
    let bootstrap = control.join("bootstrap.json");
    std::fs::write(
        &bootstrap,
        serde_json::to_vec(&desired.teams.values().next().unwrap().verifier).unwrap(),
    )
    .unwrap();
    let team_id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
    let log = std::fs::File::create(control.join("launcher.log")).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", &remote)
        .env("MRSM_ROOT_DIR", &remote)
        .env("MRSM_PLUGINS_DIR", remote.join("plugins"))
        .env("MRSM_EXECUTOR_BOOTSTRAP", bootstrap)
        .env("MRSM_PYTHON", python)
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .env(
            "PYTHONPATH",
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .canonicalize()
                .unwrap(),
        )
        .env("HTTP_PROXY", "http://proxy-user:proxy-password@127.0.0.1:1")
        .env(
            "HTTPS_PROXY",
            "http://proxy-user:proxy-password@127.0.0.1:1",
        )
        .env(
            "ALL_PROXY",
            "socks5://proxy-user:proxy-password@127.0.0.1:1",
        )
        .env("AWS_SECRET_ACCESS_KEY", "cloud-private-secret")
        .env("DB_PASSWORD", "main-callback-and-db-secret")
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
    let mut executor = Executor {
        child: command.spawn().unwrap(),
        root: control.clone(),
    };
    let until = Instant::now() + Duration::from_secs(15);
    while tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .is_err()
    {
        assert!(
            executor.child.try_wait().unwrap().is_none() && Instant::now() < until,
            "{}",
            diagnostics(&control)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let mut server = meerschaum_server::ServerConfig::for_root("sql:main", &main);
    server.actions_non_admin = true;
    server.execution_callback_url = Some(origin.clone());
    server.connectors = snapshot
        .get("meerschaum")
        .unwrap()
        .get("connectors")
        .unwrap()
        .to_map();
    let archives = server.plugins_archives_path.clone();
    archive(
        &archives,
        "manifest_probe",
        r#"
import json,os,pathlib,time
from meerschaum.plugins import make_action
state=pathlib.Path(os.environ['MRSM_ROOT_DIR'])
assert all(json.loads((state/'early-proof.json').read_text()).values())
(state/'plugin-ready').write_text('credential-free')
deadline=time.monotonic()+15
while not (state/'release-discovery').exists():
    assert time.monotonic()<deadline,'Fixture did not release discovery'
    time.sleep(.02)
@make_action
def catalog_probe(action=None,**kw):
    '''Credential-free catalog action.'''
    return True,'This action was discovered, not invoked.'
"#,
    );
    let controller = Arc::new(Controller::new(
        main.clone(),
        instance.clone(),
        options,
        snapshot,
    ));
    let state = meerschaum_server::AppState::builder(server, instance.clone())
        .build()
        .unwrap();
    controller.bind_state(&state).unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let app = meerschaum_server::app(state.clone()).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let recorded = recorded.clone();
            async move {
                recorded.lock().unwrap().push((
                    request.method().to_string(),
                    request.uri().path().to_string(),
                ));
                next.run(request).await
            }
        },
    ));
    let _server = Task(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    }));
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let login: Json = client
        .post(format!("{origin}/login"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("username=metadata_operator&password=synthetic-metadata-pass")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "authorization",
        format!("Bearer {}", login["access_token"].as_str().unwrap())
            .parse()
            .unwrap(),
    );
    let mut caller =
        meerschaum_server::access::require_fresh(&state, &headers, &["actions:execute"])
            .await
            .unwrap();
    caller
        .principal
        .permissions
        .retain(|scope| scope == "actions:execute");
    assert!(!caller.has("jobs:execute"));
    let caller = Arc::new(caller);
    requests.lock().unwrap().clear();
    let task_controller = controller.clone();
    let task_caller = caller.clone();
    let discovery = tokio::spawn(async move {
        context::scope(
            Some(task_caller),
            context::action_request_scope(
                task_controller
                    .discover_plugin_catalog(team_id, BTreeSet::from(["manifest_probe".into()])),
            ),
        )
        .await
    });
    let until = Instant::now() + Duration::from_secs(25);
    let (record, work, generation) = 'outer: loop {
        let records = Catalog::new(&main).records().unwrap();
        if let Some(record) = records.values().find(|record| record.discovery.is_some()) {
            let id = Uuid::parse_str(record.remote_name.strip_prefix("mrsm-").unwrap()).unwrap();
            let directory = control
                .join("execution-contexts")
                .join(id.simple().to_string());
            if let Ok(entries) = std::fs::read_dir(directory) {
                for entry in entries.flatten() {
                    if entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with("generation-")
                    {
                        let work = entry.path().join(".mrsm_team_work");
                        if work.join("plugin-ready").exists() {
                            break 'outer (record.clone(), work, entry.path());
                        }
                    }
                }
            }
        }
        if discovery.is_finished() {
            let outcome = discovery
                .await
                .map(|result| result.map(|catalog| catalog.complete()));
            panic!(
                "Metadata producer ended before its confined ready marker: {outcome:?}\n{}",
                diagnostics(&control)
            );
        }
        assert!(
            Instant::now() < until,
            "Metadata never reached confined discovery. Main canonical records: {}\n{}",
            serde_json::to_string(&Catalog::new(&main).records().unwrap().values().map(|record|serde_json::json!({"name":record.name,"remote_name":record.remote_name,"profile":record.profile,"revision":record.context_revision,"suspended":record.callback_suspended})).collect::<Vec<_>>()).unwrap(),
            diagnostics(&control)
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(record.profile, environment::Profile::Metadata);
    assert!(!remote.join("control-imported").exists());
    let proof: Json =
        serde_json::from_slice(&std::fs::read(work.join("early-proof.json")).unwrap()).unwrap();
    assert!(
        proof
            .as_object()
            .unwrap()
            .values()
            .all(|value| value == true),
        "{proof}"
    );
    let credential_path = generation.join("credential.json");
    let before = std::fs::read_to_string(&credential_path).unwrap();
    assert!(before.contains(environment::METADATA_TOKEN));
    let launch = Uuid::parse_str(record.remote_name.strip_prefix("mrsm-").unwrap()).unwrap();
    let real_credential = callbacks::credential(&state, record.callback.unwrap(), launch)
        .await
        .unwrap();
    assert!(!before.contains(&real_credential.token));
    let api = context::scope(Some(caller.clone()), controller.prepare_id(team_id))
        .await
        .unwrap();
    let update = environment::Update {
        metadata_deadline: record
            .discovery
            .as_ref()
            .map(|discovery| discovery.deadline),
        extension: None,
        registration: None,
        discovery_catalog_sha256: Some(
            record.discovery.as_ref().unwrap().catalog_sha256().unwrap(),
        ),
        id: launch,
        revision: record.context_revision,
        owner: "metadata_operator".into(),
        owner_id: instance
            .get_user_id(&user)
            .await
            .unwrap()
            .map(|value| value.py_str()),
        profile: environment::Profile::Metadata,
        target: environment::Target::Job {
            name: record.remote_name.clone(),
        },
        callback_url: environment::METADATA_ORIGIN.into(),
        credential: real_credential,
    };
    let refused = api
        .make_request(
            Method::Put,
            &format!(
                "{}{launch}",
                meerschaum_server::executor::gateway::CONTEXTS_PATH
            ),
            RequestOptions {
                body: Body::Json(serde_json::to_string(&update).unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(refused.status, 403);
    assert_eq!(std::fs::read_to_string(&credential_path).unwrap(), before);
    std::fs::write(work.join("release-discovery"), "go").unwrap();
    let result = discovery
        .await
        .unwrap()
        .unwrap_or_else(|error| panic!("{error}\n{}", diagnostics(&control)));
    assert!(result.complete());
    assert!(
        result
            .action_names()
            .unwrap()
            .iter()
            .any(|name| name == "catalog_probe")
    );
    assert_eq!(result.catalog().team_id, team_id);
    assert_eq!(
        result.catalog().generation,
        desired.teams.values().next().unwrap().verifier.generation
    );
    assert!(!remote.join("control-imported").exists());
    assert!(
        requests
            .lock()
            .unwrap()
            .iter()
            .all(|(method, path)| method == "GET" && path == "/plugins/manifest_probe"),
        "{:?}",
        requests.lock().unwrap()
    );
    let cached = context::scope(
        Some(caller.clone()),
        context::action_request_scope(
            controller.discover_plugin_catalog(team_id, BTreeSet::from(["manifest_probe".into()])),
        ),
    )
    .await
    .unwrap();
    assert_eq!(cached.catalog(), result.catalog());
    // Each changed immutable source digest gets a separate Main-created Metadata launch.
    // Its real result file and canonical HTTP response must preserve declarations whose
    // JSON string envelope exceeds the ordinary jobs limit; cache reads keep the decoded cap.
    for non_bmp in [false, true] {
        let (source, action_count) = metadata_payload::source(non_bmp);
        archive(&archives, "manifest_probe", &source);
        let large = context::scope(
            Some(caller.clone()),
            context::action_request_scope(
                controller
                    .discover_plugin_catalog(team_id, BTreeSet::from(["manifest_probe".into()])),
            ),
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "Large metadata (non-BMP={non_bmp}) failed: {error}\n{}",
                diagnostics(&control)
            )
        });
        let encoded = large.encode().unwrap();
        assert!(large.complete());
        assert_eq!(large.action_names().unwrap().len(), action_count);
        assert!(
            encoded.len() > meerschaum_server::executor::plugins::MAX_CATALOG_BYTES * 95 / 100,
            "The actual near-boundary catalog was only {} bytes.",
            encoded.len()
        );
        assert!(encoded.len() <= meerschaum_server::executor::plugins::MAX_CATALOG_BYTES);
        let envelope = serde_json::to_vec(&(true, String::from_utf8(encoded).unwrap())).unwrap();
        assert!(
            envelope.len() > meerschaum_jobs::supervisor::ResultProtocol::Ordinary.envelope_limit()
        );
        let pulls = requests.lock().unwrap().len();
        let reused = context::scope(
            Some(caller.clone()),
            context::action_request_scope(
                controller
                    .discover_plugin_catalog(team_id, BTreeSet::from(["manifest_probe".into()])),
            ),
        )
        .await
        .unwrap();
        assert_eq!(reused.catalog(), large.catalog());
        assert_eq!(
            requests.lock().unwrap().len(),
            pulls,
            "A verified cache hit unexpectedly pulled or relaunched its source."
        );
    }
    team.attributes
        .insert("plugins".into(), Value::List(Vec::new()));
    assert!(instance.edit_team(&team).await.success);
    assert!(
        context::scope(
            Some(caller),
            context::action_request_scope(
                controller
                    .discover_plugin_catalog(team_id, BTreeSet::from(["manifest_probe".into()]))
            )
        )
        .await
        .is_err(),
        "Cached metadata bypassed the original source's current plugin ceiling."
    );
}
