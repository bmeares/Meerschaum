//! A fresh executor API cannot erase uncertain process-tree cleanup provenance.
#![cfg(target_os = "linux")]

use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::authentication::{Request, RequestAuthentication};
use meerschaum_connector_api::client::{Body, Method, RequestOptions};
use meerschaum_connector_api::traits::{ConnectorError, ConnectorResult};
use meerschaum_core::{Map, Value};
use meerschaum_jobs::process::{self, ProcState, Sig};
use meerschaum_jobs::supervisor::LifecycleOwner;
use meerschaum_server::executor::control::{Actor, Authority};
use meerschaum_server::executor::environment::{self, Profile, Target, Update};
use meerschaum_server::executor::gateway::{CONFIGURATION_PATH, CONTEXTS_PATH, Configuration};
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

fn map(value: Json) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}
struct Signer {
    authority: Arc<Authority>,
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
                        ConnectorError::Invalid("Fixture request is not buffered".into())
                    })?,
                },
                &self.revision,
                chrono::Utc::now().timestamp(),
            )
            .map(|token| format!("Bearer {token}"))
            .map_err(|_| ConnectorError::Invalid("Could not sign fixture request".into()))
    }
}
struct Subreaper(Option<rustix::process::Pid>);
impl Subreaper {
    fn install() -> Self {
        let previous = rustix::process::child_subreaper().unwrap();
        rustix::process::set_child_subreaper(rustix::process::Pid::from_raw(1)).unwrap();
        Self(previous)
    }
}
impl Drop for Subreaper {
    fn drop(&mut self) {
        let _ = rustix::process::set_child_subreaper(self.0);
    }
}

struct Executor {
    child: Option<Child>,
    root: PathBuf,
    control_root: PathBuf,
    owned: BTreeMap<i32, LifecycleOwner>,
    bystander: Child,
}
impl Executor {
    fn kill_api(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    fn remember(&mut self, record: &Json) {
        for field in ["supervisor", "worker"] {
            if let Ok(owner) = serde_json::from_value::<LifecycleOwner>(record[field].clone()) {
                let pid = i32::try_from(record[field]["pid"].as_i64().unwrap()).unwrap();
                self.owned.insert(pid, owner);
            }
        }
    }
    fn stop_owned(&self) {
        for (pid, owner) in &self.owned {
            if owner.alive() {
                let _ = process::send_signal(*pid, Sig::Kill);
            }
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            for pid in self.owned.keys() {
                if let Some(pid) = rustix::process::Pid::from_raw(*pid) {
                    let _ =
                        rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::NOHANG);
                }
            }
            if self.owned.values().all(|owner| !owner.alive()) {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    async fn start(
        &mut self,
        authority: Arc<Authority>,
        configuration: &Configuration,
    ) -> APIConnector {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = reservation.local_addr().unwrap().port();
        std::fs::create_dir_all(&self.control_root).unwrap();
        let bootstrap = self.control_root.join("bootstrap.json");
        std::fs::write(
            &bootstrap,
            serde_json::to_vec(&authority.verifier()).unwrap(),
        )
        .unwrap();
        let log = self.control_root.join(format!("launcher-{port}.log"));
        let writer = std::fs::File::create(&log).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env(
                "PATH",
                std::env::var_os("PATH").unwrap_or_else(|| "/usr/bin:/bin".into()),
            )
            .env("HOME", &self.root)
            .env("MRSM_ROOT_DIR", &self.root)
            .env("MRSM_PLUGINS_DIR", self.root.join("plugins"))
            .env("MRSM_EXECUTOR_BOOTSTRAP", bootstrap)
            .env("MRSM_NO_AUTO_INSTALL", "1")
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env(
                "PYTHONPATH",
                std::env::var_os("PYTHONPATH")
                    .unwrap_or_else(|| (env!("CARGO_MANIFEST_DIR").to_string() + "/../..").into()),
            )
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .stdin(Stdio::null())
            .stdout(writer.try_clone().unwrap())
            .stderr(writer)
            .args(["start", "executor", "--host", "127.0.0.1", "--port"])
            .arg(port.to_string())
            .arg("--control-root")
            .arg(&self.control_root);
        if let Some(python) = std::env::var_os("MRSM_PYTHON") {
            command.env("MRSM_PYTHON", python);
        }
        drop(reservation);
        self.child = Some(command.spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(15);
        while tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_err()
        {
            assert!(
                self.child.as_mut().unwrap().try_wait().unwrap().is_none()
                    && Instant::now() < deadline,
                "{}",
                std::fs::read_to_string(&log).unwrap()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        APIConnector::new(
            Some("persisted-proof"),
            map(json!({"host":"127.0.0.1","port":port})),
        )
        .unwrap()
        .with_request_authentication(
            Arc::new(Signer {
                authority,
                revision: configuration.fingerprint().unwrap(),
            }),
            1024 * 1024,
        )
        .unwrap()
    }
}
impl Drop for Executor {
    fn drop(&mut self) {
        self.kill_api();
        self.stop_owned();
        let _ = self.bystander.kill();
        let _ = self.bystander.wait();
    }
}

async fn configure(api: &APIConnector, configuration: &Configuration) -> u16 {
    api.make_request(
        Method::Put,
        CONFIGURATION_PATH,
        RequestOptions {
            body: Body::Json(serde_json::to_string(configuration).unwrap()),
            timeout: Some(Duration::from_secs(12)),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .status
}

async fn deliver(api: &APIConnector, id: Uuid) -> Update {
    let update = Update {
        metadata_deadline: None,
        extension: None,
        registration: None,
        discovery_catalog_sha256: None,
        id,
        revision: 1,
        owner: "operator".into(),
        owner_id: Some("synthetic-operator-id".into()),
        profile: Profile::Api,
        target: Target::Job {
            name: format!("mrsm-{}", id.simple()),
        },
        callback_url: "http://127.0.0.1:9".into(),
        credential: meerschaum_connector_api::models::ExecutionCredential {
            token: "synthetic-short-proof-callback".into(),
            expires_at: chrono::Utc::now().timestamp() + 30,
        },
    };
    let response = api
        .make_request(
            Method::Put,
            &format!("{CONTEXTS_PATH}{id}"),
            RequestOptions {
                body: Body::Json(serde_json::to_string(&update).unwrap()),
                timeout: Some(Duration::from_secs(8)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(response.status, 200, "{}", response.text());
    update
}

async fn lifecycle(root: &Path, id: Uuid, complete: bool) -> (Vec<u8>, Json) {
    let context = root
        .join("execution-contexts")
        .join(id.simple().to_string());
    let path = context.join("job-lifecycle-1.json");
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        if let Ok(bytes) = std::fs::read(&path) {
            let record: Json = serde_json::from_slice(&bytes).unwrap();
            if record["supervisor"].is_object()
                && record["worker"].is_object()
                && record["cleanup_complete"] == complete
            {
                return (
                    std::fs::read(context.join("identity.json")).unwrap(),
                    record,
                );
            }
        }
        assert!(
            Instant::now() < deadline,
            "The actual supervisor never produced its expected lifecycle descriptor"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_executor_preserves_uncertain_cleanup_and_restarts_only_completed_generations() {
    let _subreaper = Subreaper::install();
    for abnormal in [true, false] {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("executor");
        let control = temporary.path().join("factory-control");
        std::fs::create_dir_all(root.join("plugins")).unwrap();
        let team_id = Uuid::new_v4();
        let generation = Uuid::new_v4();
        let (authority, _) = Authority::generate(Uuid::new_v4(), team_id, generation).unwrap();
        let authority = Arc::new(authority);
        let configuration = Configuration {
            catalog_sha256: None,
            team_id,
            generation,
            team_name: "ProofFixture".into(),
            plugins: Default::default(),
            config: map(json!({
            "meerschaum":{"instance":"sql:executor","api_instance":"sql:executor","connectors":{"sql":{"executor":{"flavor":"sqlite","database":root.join("executor.db")}}}},
            "system":{"packages":{"auto_install":false}},"api":{"no_dash":true}})),
        };
        let bystander = Command::new("/bin/sleep")
            .arg("300")
            .env_clear()
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut executor = Executor {
            child: None,
            root: root.clone(),
            control_root: control.clone(),
            owned: BTreeMap::new(),
            bystander,
        };
        let api = executor.start(authority.clone(), &configuration).await;
        assert_eq!(configure(&api, &configuration).await, 200);
        let id = Uuid::new_v4();
        let name = format!("mrsm-{}", id.simple());
        let _ = deliver(&api, id).await;
        let mut words = vec!["show".into(), "version".into()];
        if abnormal {
            words.extend(["--loop".into(), "--min-seconds".into(), "30".into()]);
        }
        let created = api
            .create_job(
                &name,
                &words,
                Some(Map::from_iter([(
                    environment::JOB_CONTEXT_PROPERTY.into(),
                    id.to_string().into(),
                )])),
            )
            .await;
        assert!(created.success, "{}", created.message);
        let (before, record) = lifecycle(&control, id, !abnormal).await;
        executor.remember(&record);
        if abnormal {
            let supervisor: LifecycleOwner =
                serde_json::from_value(record["supervisor"].clone()).unwrap();
            assert!(supervisor.alive());
            process::send_signal(
                i32::try_from(record["supervisor"]["pid"].as_i64().unwrap()).unwrap(),
                Sig::Kill,
            )
            .unwrap();
            executor.stop_owned();
            assert!(!record["cleanup_complete"].as_bool().unwrap());
        }
        executor.kill_api();
        let api = executor.start(authority, &configuration).await;
        let status = configure(&api, &configuration).await;
        let directory = control
            .join("execution-contexts")
            .join(id.simple().to_string());
        if abnormal {
            assert_ne!(
                status, 200,
                "A fresh Factory erased uncertain cleanup evidence"
            );
            assert_eq!(
                std::fs::read(directory.join("identity.json")).unwrap(),
                before,
                "A fresh empty memory map rewrote attempted provenance"
            );
            let preserved: Json = serde_json::from_slice(
                &std::fs::read(directory.join("job-lifecycle-1.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(preserved["cleanup_complete"], false);
            assert!(
                !control
                    .join("executor-retired-jobs")
                    .join(format!("{}.json", id.simple()))
                    .exists()
            );
        } else {
            assert_eq!(
                status, 200,
                "A completed generation could not establish controlled rebuild proof"
            );
            assert!(!directory.exists());
            let retired: Json = serde_json::from_slice(
                &std::fs::read(
                    control
                        .join("executor-retired-jobs")
                        .join(format!("{}.json", id.simple())),
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(retired["cleared_for_rebuild"], true);
            let _ = deliver(&api, id).await;
            assert_eq!(
                api.get_job_status(&name).await.unwrap(),
                "stopped",
                "A fresh Factory replayed work before explicit admission"
            );
            assert!(api.start_job(&name).await.success);
            let (_, current) = lifecycle(&control, id, true).await;
            executor.remember(&current);
        }
        assert!(
            matches!(
                process::proc_state(executor.bystander.id() as i32),
                ProcState::Alive
            ),
            "Owned cleanup touched a bystander"
        );
    }
}
