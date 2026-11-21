//! Controlled rebuild recovery against the real standalone executor and its own SQLite root.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::authentication::{Request, RequestAuthentication};
use meerschaum_connector_api::client::{Body, Method, RequestOptions};
use meerschaum_connector_api::traits::{ConnectorError, ConnectorResult};
use meerschaum_core::{Map, Value};
use meerschaum_server::executor::control::{Actor, Authority};
use meerschaum_server::executor::environment::{JOB_CONTEXT_PROPERTY, Profile, Target, Update};
use meerschaum_server::executor::gateway::{
    CONFIGURATION_PATH, CONTEXTS_PATH, Configuration, REVOKE_PATH,
};
use serde_json::json;
use uuid::Uuid;

struct Signer {
    authority: Authority,
    revision: Mutex<String>,
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
                &self.revision.lock().unwrap(),
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
        let deadline = Instant::now() + Duration::from_secs(5);
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

fn core(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

async fn configure(api: &APIConnector, signer: &Signer, configuration: &Configuration) -> u16 {
    *signer.revision.lock().unwrap() = configuration.fingerprint().unwrap();
    api.make_request(
        Method::Put,
        CONFIGURATION_PATH,
        RequestOptions {
            body: Body::Json(serde_json::to_string(configuration).unwrap()),
            timeout: Some(Duration::from_secs(20)),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .status
}

async fn context(api: &APIConnector, id: Uuid, revision: u64) {
    let update = Update {
        metadata_deadline: None,
        discovery_catalog_sha256: None,
        extension: None,
        registration: None,
        id,
        revision,
        owner: "operator".into(),
        owner_id: Some("synthetic-operator-id".into()),
        profile: Profile::Team,
        target: Target::Job {
            name: format!("mrsm-{}", id.simple()),
        },
        callback_url: "http://127.0.0.1:9".into(),
        // This fixture proves fresh signed DELIVERY gates recovery. Main-side source validation
        // and JWT issuance are separately tested; these commands never call their callback API.
        credential: meerschaum_connector_api::models::ExecutionCredential {
            token: "synthetic-bounded-recovery-credential".into(),
            expires_at: chrono::Utc::now().timestamp() + 30,
        },
    };
    let response = api
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
    assert_eq!(response.status, 200, "{}", response.text());
}

async fn status(api: &APIConnector, name: &str, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let observed = api.get_job_status(name).await.unwrap();
        if observed == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{name}: expected {expected}, observed {observed}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn pending(root: &Path, name: &str) -> Option<serde_json::Value> {
    let stored: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.join("executor-recovery/pending.json")).unwrap(),
    )
    .unwrap();
    stored.get(name).cloned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controlled_rebuild_requires_fresh_context_survives_failed_build_and_never_replays_completed_or_paused_work()
 {
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
    let mut process = Process(command.spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(15);
    while tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .is_err()
    {
        assert!(
            process.0.try_wait().unwrap().is_none() && Instant::now() < deadline,
            "Executor failed to start: {}",
            std::fs::read_to_string(&log_path).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut configuration = Configuration {
        catalog_sha256: None,
        team_id,
        generation,
        team_name: "Recovery fixture".into(),
        plugins: Default::default(),
        config: core(json!({
            "meerschaum": {"instance": "sql:executor", "api_instance": "sql:executor", "connectors": {
                "sql": {"executor": {"flavor": "sqlite", "database": root.path().join("executor.db")}}
            }},
            "system": {"packages": {"auto_install": false}}, "api": {"no_dash": true},
            "formatting": {"ansi": false}
        })),
    };
    let signer = Arc::new(Signer {
        authority,
        revision: Mutex::new(configuration.fingerprint().unwrap()),
    });
    let api = APIConnector::new(
        Some("recovery"),
        core(json!({"host":"127.0.0.1", "port":port})),
    )
    .unwrap()
    .with_request_authentication(signer.clone(), 1024 * 1024)
    .unwrap();
    assert_eq!(configure(&api, &signer, &configuration).await, 200);
    let completed_id = Uuid::new_v4();
    let active_id = Uuid::new_v4();
    let completed = format!("mrsm-{}", completed_id.simple());
    let active = format!("mrsm-{}", active_id.simple());
    for (id, name, args) in [
        (
            completed_id,
            &completed,
            vec!["show".into(), "version".into()],
        ),
        (
            active_id,
            &active,
            vec![
                "show".into(),
                "version".into(),
                "--loop".into(),
                "--min-seconds".into(),
                "30".into(),
            ],
        ),
    ] {
        context(&api, id, 1).await;
        let mut properties = Map::new();
        properties.insert(JOB_CONTEXT_PROPERTY.into(), id.to_string().into());
        let result = api.create_job(name, &args, Some(properties)).await;
        assert!(result.success, "{}", result.message);
    }
    status(&api, &completed, "stopped").await;
    status(&api, &active, "running").await;
    let once_logs = api.get_logs(&completed).await.unwrap();

    configuration
        .config
        .insert("formatting".into(), Value::Map(core(json!({"ansi":true}))));
    assert_eq!(configure(&api, &signer, &configuration).await, 200);
    status(&api, &active, "stopped").await;
    assert_eq!(pending(control.path(), &active).unwrap()["revision"], 1);
    assert_eq!(pending(control.path(), &active).unwrap()["stopped"], true);
    assert!(pending(control.path(), &completed).is_none());
    assert!(
        !control
            .path()
            .join("execution-contexts")
            .join(active_id.simple().to_string())
            .exists()
    );
    context(&api, completed_id, 1).await;
    status(&api, &completed, "stopped").await;
    assert_eq!(api.get_logs(&completed).await.unwrap(), once_logs);
    context(&api, active_id, 1).await;
    status(&api, &active, "running").await;
    assert!(pending(control.path(), &active).is_none());

    // A partial failed build clears its runtime and credentials; retry retains captured intent.
    let mut broken = configuration.clone();
    broken.config.insert(
        "meerschaum".into(),
        Value::Map(core(
            json!({"instance":"sql:missing", "api_instance":"sql:missing"}),
        )),
    );
    assert_eq!(configure(&api, &signer, &broken).await, 503);
    assert_eq!(pending(control.path(), &active).unwrap()["revision"], 1);
    assert!(
        !control
            .path()
            .join("execution-contexts")
            .join(active_id.simple().to_string())
            .exists()
    );
    assert_eq!(configure(&api, &signer, &configuration).await, 200);
    status(&api, &active, "stopped").await;
    context(&api, active_id, 1).await;
    status(&api, &active, "running").await;

    // New revision belongs to explicit main start; its PUT alone must never double-start.
    configuration
        .config
        .insert("formatting".into(), Value::Map(core(json!({"ansi":false}))));
    assert_eq!(configure(&api, &signer, &configuration).await, 200);
    context(&api, active_id, 2).await;
    status(&api, &active, "stopped").await;
    assert!(pending(control.path(), &active).is_none());
    assert!(api.start_job(&active).await.success);
    status(&api, &active, "running").await;

    // A paused command may not execute briefly just to restore its paused presentation.
    assert!(api.pause_job(&active).await.success);
    status(&api, &active, "paused").await;
    configuration
        .config
        .insert("formatting".into(), Value::Map(core(json!({"ansi":true}))));
    assert_eq!(configure(&api, &signer, &configuration).await, 200);
    assert_eq!(pending(control.path(), &active).unwrap()["paused"], true);
    context(&api, active_id, 2).await;
    status(&api, &active, "stopped").await;
    assert!(api.start_job(&active).await.success);
    status(&api, &active, "running").await;

    // Retirement after clear cancels captured intent; a late delivery cannot replay it.
    configuration
        .config
        .insert("formatting".into(), Value::Map(core(json!({"ansi":false}))));
    assert_eq!(configure(&api, &signer, &configuration).await, 200);
    let retired = api
        .make_request(
            Method::Delete,
            &format!("{CONTEXTS_PATH}{active_id}"),
            RequestOptions {
                body: Body::Json(json!({"revision":2}).to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(retired.status, 200, "{}", retired.text());
    context(&api, active_id, 2).await;
    status(&api, &active, "stopped").await;
    assert!(pending(control.path(), &active).is_none());
    assert!(api.delete_job(&completed).await.success);
    assert!(api.start_job(&active).await.success);
    status(&api, &active, "running").await;
    let revoked = api
        .make_request(Method::Post, REVOKE_PATH, RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(revoked.status, 200, "{}", revoked.text());
    assert!(
        pending(control.path(), &active).is_none(),
        "Ordinary revocation created a rebuild intent."
    );
    let jobs = meerschaum_jobs::JobsContext::for_root(control.path());
    assert_eq!(
        meerschaum_jobs::Job::open(&active, &jobs).unwrap().status(),
        meerschaum_jobs::Status::Stopped
    );
    drop(process);
}
