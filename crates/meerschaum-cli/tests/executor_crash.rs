//! SIGKILL of an owned executor cannot leave its marked running or paused commands alive.
#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_sigkill_stops_running_and_paused_marked_jobs_without_touching_an_unrelated_pid() {
    let _subreaper = Subreaper::install();
    let root = tempfile::tempdir().unwrap();
    let control = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("plugins")).unwrap();
    let team_id = Uuid::new_v4();
    let generation = Uuid::new_v4();
    let (authority, _) = Authority::generate(Uuid::new_v4(), team_id, generation).unwrap();
    let bootstrap = control.path().join("bootstrap.json");
    std::fs::write(
        &bootstrap,
        serde_json::to_vec(&authority.verifier()).unwrap(),
    )
    .unwrap();
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    let log_path = root.path().join("launcher.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", root.path())
        .env("MRSM_ROOT_DIR", root.path())
        .env("MRSM_PLUGINS_DIR", root.path().join("plugins"))
        .env("MRSM_EXECUTOR_BOOTSTRAP", bootstrap)
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .env(
            "PYTHONPATH",
            env!("CARGO_MANIFEST_DIR").to_string() + "/../..",
        )
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    if let Some(python) = std::env::var_os("MRSM_PYTHON") {
        command.env("MRSM_PYTHON", python);
    }
    command.args([
        "start",
        "executor",
        "--host",
        "127.0.0.1",
        "--port",
        &port.to_string(),
        "--control-root",
        control.path().to_str().unwrap(),
    ]);
    drop(reservation);
    let mut api_process = command.spawn().unwrap();
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
            panic!("Could not start the owned bystander: {error}");
        }
    };
    let mut fixture = Fixture {
        api: api_process,
        bystander,
        root: control.path().into(),
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
    let configuration = Configuration {
        catalog_sha256: None,
        team_id,
        generation,
        team_name: "CrashFixture".into(),
        plugins: Default::default(),
        config: core(json!({
            "meerschaum":{"instance":"sql:executor","api_instance":"sql:executor","connectors":{"sql":{"executor":{"flavor":"sqlite","database":root.path().join("executor.db")}}}},
            "system":{"packages":{"auto_install":false}},"api":{"no_dash":true}
        })),
    };
    let api = APIConnector::new(
        Some("crash-fixture"),
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
    let credential_expires = chrono::Utc::now().timestamp() + 30;
    for paused in [false, true] {
        let id = Uuid::new_v4();
        let name = format!("mrsm-{}", id.simple());
        fixture.names.push(name.clone());
        let update = Update {
            metadata_deadline: None,
            discovery_catalog_sha256: None,
            extension: None,
            registration: None,
            id,
            revision: 1,
            owner: "operator".into(),
            owner_id: Some("synthetic-operator-id".into()),
            profile: Profile::Team,
            target: Target::Job { name: name.clone() },
            callback_url: "http://127.0.0.1:9".into(),
            credential: meerschaum_connector_api::models::ExecutionCredential {
                token: "synthetic-bounded-crash-credential".into(),
                expires_at: credential_expires,
            },
        };
        let delivered = api
            .make_request(
                Method::Put,
                &format!("{CONTEXTS_PATH}{id}"),
                RequestOptions {
                    body: Body::Json(serde_json::to_string(&update).unwrap()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(delivered.status, 200, "{}", delivered.text());
        let mut properties = core(
            json!({"lifecycle_owner":{"pid":fixture.bystander.id(),"started_at":"2000-01-01T00:00:00"},"env":{"MRSM_LIFECYCLE_OWNER_PID":fixture.bystander.id().to_string()}}),
        );
        properties.insert(JOB_CONTEXT_PROPERTY.into(), id.to_string().into());
        let created = api
            .create_job(
                &name,
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
        fixture.snapshot();
        status(&api, &name, "running").await;
        if paused {
            assert!(api.pause_job(&name).await.success);
            status(&api, &name, "paused").await;
        }
        fixture.snapshot();
    }
    assert!(
        fixture.owned.len() >= 4,
        "Expected both worker/supervisor pairs and any owned descendants."
    );
    let mut guards = 0;
    for (pid, args) in process::process_cmdlines() {
        if !fixture.owned.contains_key(&pid) {
            continue;
        }
        if let Some(encoded) = args
            .windows(2)
            .find(|pair| pair[0] == "--_mrsm-lifecycle-owner")
            .map(|pair| &pair[1])
        {
            guards += 1;
            let owner: serde_json::Value = serde_json::from_str(encoded).unwrap();
            assert_eq!(owner["pid"], fixture.api.id());
            assert_ne!(owner["pid"], fixture.bystander.id());
        }
    }
    assert_eq!(
        guards, 2,
        "Both supervisors must carry an immutable host-selected owner."
    );
    assert!(
        chrono::Utc::now().timestamp() < credential_expires,
        "This must prove owner death before credential expiry."
    );
    let crash = Instant::now();
    fixture.api.kill().unwrap();
    let exit = fixture.api.wait().unwrap();
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&exit),
        Some(9)
    );
    loop {
        fixture.reap();
        if fixture
            .owned
            .keys()
            .all(|pid| process::proc_state(*pid) == ProcState::Gone)
        {
            break;
        }
        assert!(
            crash.elapsed() < Duration::from_secs(4),
            "Owned jobs survived their control API: {:?}",
            fixture
                .owned
                .keys()
                .map(|pid| (*pid, process::proc_state(*pid)))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        fixture.bystander.try_wait().unwrap().is_none(),
        "An unrelated fixture PID was signalled."
    );
    let jobs = meerschaum_jobs::JobsContext::for_root(control.path());
    for name in &fixture.names {
        let mut job = meerschaum_jobs::Job::open(name, &jobs).unwrap();
        assert_eq!(job.status(), meerschaum_jobs::Status::Stopped);
        assert!(
            job.stop_time().is_some(),
            "A crash must not trigger automatic arbitrary-command replay."
        );
        assert!(!job.result().success);
        assert!(job.result().message.contains("runtime owner ended"));
    }
}
