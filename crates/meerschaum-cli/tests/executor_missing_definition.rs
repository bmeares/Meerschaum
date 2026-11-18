//! Explicit missing-definition recovery preserves main-owned commands and original registration.
#![cfg(unix)]

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use meerschaum_cli::executor_client::Controller;
use meerschaum_cli::executor_jobs::{Catalog, DelegatingJobs};
use meerschaum_cli::executor_registry::Registry;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Value};
use meerschaum_server::backends::NoJobs;
use serde_json::json;

struct Actions(Arc<Controller>);
#[async_trait::async_trait]
impl meerschaum_server::backends::ActionsBackend for Actions {
    fn list(&self) -> Vec<String> {
        vec!["show".into()]
    }
    async fn execute(&self, _: &str, _: Map) -> meerschaum_core::SuccessTuple {
        meerschaum_core::SuccessTuple::fail("Fixture accepts tracked jobs only.")
    }
    async fn executor_ready(&self, team: &Team) -> Result<(), String> {
        self.0.prepare(team).await.map(|_| ())
    }
}

struct TeamInstance(Arc<dyn meerschaum_connector::InstanceConnector>);
impl meerschaum_server::backends::InstanceResolver for TeamInstance {
    fn resolve(&self, keys: &str) -> Option<Arc<dyn meerschaum_connector::InstanceConnector>> {
        (keys == "sql:team").then(|| self.0.clone())
    }
}

struct Task(tokio::task::JoinHandle<()>);
impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
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

fn grant_files(root: &Path) -> std::collections::BTreeSet<std::ffi::OsString> {
    std::fs::read_dir(root.join("executors/callbacks"))
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.unwrap();
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "json")
                .then(|| entry.file_name())
        })
        .collect()
}

fn inactive(root: &Path, record: &meerschaum_cli::executor_jobs::Record) {
    assert!(record.callback_suspended);
    let proof: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            root.join("executors/callbacks")
                .join(format!("{}.json", record.callback.unwrap())),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(proof["lease_until"], 0);
}

fn stored_command(root: &Path, name: &str, command: &[String]) {
    // A private fixture simulates a retained definition after command policy changed. The
    // remote definition keeps its original safe command, so admission must use main's record.
    let path = root.join("executors/catalog.json");
    let mut records: std::collections::BTreeMap<String, meerschaum_cli::executor_jobs::Record> =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    records.get_mut(name).unwrap().sysargs = command.to_vec();
    meerschaum_core::team_scope::write_private(&path, &serde_json::to_string(&records).unwrap())
        .unwrap();
}

async fn login(client: &reqwest::Client, origin: &str, username: &str) -> String {
    let response = client
        .post(format!("{origin}/login"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!(
            "username={username}&password=synthetic-fixture-pass"
        ))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let body: serde_json::Value = response.json().await.unwrap();
    body["access_token"].as_str().unwrap().into()
}

async fn tuple(response: reqwest::Response, expected: bool) {
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(
        body.get(0).and_then(serde_json::Value::as_bool),
        Some(expected),
        "{body}"
    );
}

async fn absent(api: &meerschaum_connector_api::APIConnector, name: &str) {
    assert!(api.delete_job(name).await.success);
    assert!(!api.try_get_job_exists(name).await.unwrap());
}

async fn status(api: &meerschaum_connector_api::APIConnector, name: &str, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let observed = api.get_job_status(name).await.unwrap();
        if observed == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "Expected {name} {expected}, found {observed}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn pending(root: &Path, name: &str) -> Option<serde_json::Value> {
    let bytes = match std::fs::read(root.join("executor-recovery/pending.json")) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => panic!("Could not read own fixture recovery intent: {error}"),
    };
    serde_json::from_slice::<serde_json::Value>(&bytes)
        .unwrap()
        .get(name)
        .cloned()
}

fn began(root: &Path, name: &str) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(
        &std::fs::read(root.join("jobs").join(name).join("properties.json")).ok()?,
    )
    .ok()?
    .get("process")?
    .get("began")?
    .as_str()
    .map(str::to_string)
}

async fn running_identity(
    root: &Path,
    name: &str,
    previous_began: Option<&str>,
) -> (i32, chrono::NaiveDateTime, String) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let pid = std::fs::read_to_string(root.join("jobs").join(name).join("process.pid"))
            .ok()
            .and_then(|text| text.trim().parse::<i32>().ok());
        if let Some(pid) = pid
            && let Some(birth) = meerschaum_jobs::process::process_start_time(pid)
            && let Some(began) = began(root, name)
            && previous_began != Some(began.as_str())
            && meerschaum_jobs::process::proc_state(pid)
                == meerschaum_jobs::process::ProcState::Alive
        {
            return (pid, birth, began);
        }
        assert!(
            Instant::now() < deadline,
            "The fixture command never established a live process identity."
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn launch(root: &Path, control: &Path, bootstrap: &Path, port: u16) -> Process {
    let log = std::fs::File::create(root.join("launcher.log")).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", root)
        .env("MRSM_ROOT_DIR", root)
        .env("MRSM_PLUGINS_DIR", root.join("plugins"))
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
    if let Some(temp) = std::env::var_os("TMPDIR") {
        command.env("TMPDIR", temp);
    }
    command.args([
        "start",
        "executor",
        "--host",
        "127.0.0.1",
        "--port",
        &port.to_string(),
        "--control-root",
        control.to_str().unwrap(),
    ]);
    Process(command.spawn().unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_start_recreates_only_the_canonical_command_under_its_original_live_source() {
    let main_root = tempfile::tempdir().unwrap();
    let executor_root = tempfile::tempdir().unwrap();
    let control = tempfile::tempdir().unwrap();
    for root in [main_root.path(), executor_root.path()] {
        std::fs::create_dir(root.join("plugins")).unwrap();
    }
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    let snapshot = core(
        json!({"meerschaum": {"instance":"sql:main", "api_instance":"sql:main", "connectors": {
        "sql": {
            "main":{"flavor":"sqlite", "database":main_root.path().join("main.db")},
            "team":{"flavor":"sqlite", "database":executor_root.path().join("team.db")}
        }, "api":{"executor":{"host":"127.0.0.1", "port":port}}
    }}, "api":{"no_dash":true}}),
    );
    let options = ConfigOptions::with_env(vec![
        (
            "MRSM_ROOT_DIR".into(),
            main_root.path().display().to_string(),
        ),
        (
            "MRSM_PLUGINS_DIR".into(),
            main_root.path().join("plugins").display().to_string(),
        ),
    ]);
    let config = Config::new(options.clone()).unwrap();
    config.set_config(snapshot.clone());
    let session = meerschaum_cli::native::Session::with_runtime(tokio::runtime::Handle::current());
    session.set_python_worker(false);
    let (instance, team_instance) = tokio::task::spawn_blocking(move || {
        (
            session.instance("sql:main", &config).unwrap().conn(),
            session.instance("sql:team", &config).unwrap().conn(),
        )
    })
    .await
    .unwrap();
    let mut team = Team::new("RecoveryOperators");
    team.attributes = core(
        json!({"isolated":true, "plugins_write":true, "instance":"sql:team", "connectors":["sql:team"],
        "execution":{"mode":"api", "connector":"api:executor"}}),
    );
    let registered = instance.register_team(&mut team).await;
    assert!(registered.success, "{}", registered.message);
    let desired = Registry::new(main_root.path())
        .reconcile(&[team.clone()])
        .unwrap();
    let bootstrap = control.path().join("bootstrap.json");
    std::fs::write(
        &bootstrap,
        serde_json::to_vec(&desired.teams.values().next().unwrap().verifier).unwrap(),
    )
    .unwrap();
    let mut original = User::new("original_operator", Some("synthetic-fixture-pass"));
    original.attributes = Some(core(json!({"role":"operator", "primary_team":team.name})));
    assert!(instance.register_user(&mut original).await.success);
    let original_id = instance.get_user_id(&original).await.unwrap().unwrap();
    let mut transport = User::new("transport_operator", Some("synthetic-fixture-pass"));
    transport.attributes = original.attributes.clone();
    assert!(instance.register_user(&mut transport).await.success);
    let mut admin = User::new("administrator", Some("synthetic-fixture-pass"));
    admin.user_type = Some("admin".into());
    assert!(instance.register_user(&mut admin).await.success);
    for username in ["original_operator", "transport_operator"] {
        team.members.insert(username.into(), TeamRole::Member);
    }
    let edited = instance.edit_team(&team).await;
    assert!(edited.success, "{}", edited.message);
    drop(reservation);
    let mut process = launch(executor_root.path(), control.path(), &bootstrap, port);
    let deadline = Instant::now() + Duration::from_secs(15);
    while tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .is_err()
    {
        assert!(
            process.0.try_wait().unwrap().is_none() && Instant::now() < deadline,
            "{}",
            std::fs::read_to_string(executor_root.path().join("launcher.log")).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let controller = Arc::new(Controller::new(
        main_root.path().into(),
        instance.clone(),
        options,
        snapshot.clone(),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = format!("http://{address}");
    let mut config = meerschaum_server::ServerConfig::for_root("sql:main", main_root.path());
    config.actions_non_admin = true;
    config.execution_callback_url = Some(origin.clone());
    config.allow_multiple_instances = true;
    config.allowed_instance_keys = vec!["sql:main".into(), "sql:team".into()];
    config.connectors = snapshot
        .get("meerschaum")
        .unwrap()
        .get("connectors")
        .unwrap()
        .to_map();
    let state = meerschaum_server::AppState::builder(config, instance.clone())
        .resolver(Arc::new(TeamInstance(team_instance.clone())))
        .actions(Arc::new(Actions(controller.clone())))
        .jobs(Arc::new(DelegatingJobs::new(
            main_root.path(),
            Arc::new(NoJobs),
            controller.clone(),
        )))
        .build()
        .unwrap();
    controller.bind_state(&state).unwrap();
    let router = meerschaum_server::app(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let original_token = login(&client, &origin, &original.username).await;
    let transport_token = login(&client, &origin, &transport.username).await;
    let admin_token = login(&client, &origin, &admin.username).await;
    let sysargs = vec!["show".to_string(), "version".to_string()];
    for (name, token) in [
        ("missing-original", &original_token),
        ("existing-original", &original_token),
        ("missing-transport", &transport_token),
    ] {
        tuple(
            client
                .post(format!("{origin}/jobs/{name}"))
                .bearer_auth(token)
                .json(
                    &json!({"sysargs":sysargs, "properties":{"synthetic_tag":"retain-canonical"}}),
                )
                .send()
                .await
                .unwrap(),
            true,
        )
        .await;
    }
    let catalog = Catalog::new(main_root.path());
    let before = catalog.get("missing-original").unwrap().unwrap();
    let api = controller.prepare(&team).await.unwrap();
    absent(&api, &before.remote_name).await;
    tuple(
        client
            .post(format!("{origin}/jobs/missing-original/start"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap(),
        true,
    )
    .await;
    let after = catalog.get(&before.name).unwrap().unwrap();
    assert_eq!(after.remote_name, before.remote_name);
    assert_eq!(after.sysargs, sysargs);
    assert_eq!(after.properties, before.properties);
    assert_eq!(after.owner_binding, before.owner_binding);
    assert_eq!(after.context_revision, before.context_revision + 1);
    assert_ne!(after.callback, before.callback);
    assert!(!main_root.path().join("jobs").join(&before.name).exists());
    let remote = controller.prepare(&team).await.unwrap();
    let metadata = remote.get_job_metadata(&after.remote_name).await.unwrap();
    assert_eq!(
        metadata.get("sysargs").unwrap().as_list().unwrap(),
        &sysargs
            .iter()
            .map(|s| Value::Str(s.clone()))
            .collect::<Vec<_>>()
    );
    let props = remote.get_job_properties(&after.remote_name).await.unwrap();
    assert_eq!(
        props.get("owner").and_then(Value::as_str),
        Some(original.username.as_str())
    );
    assert_eq!(
        props.get("synthetic_tag").and_then(Value::as_str),
        Some("retain-canonical")
    );
    assert!(!props.contains_key(meerschaum_core::job_access::OWNER_BINDING_KEY));
    assert!(!props.contains_key(meerschaum_core::job_access::TEAM_ID_KEY));

    // Existing remote definitions must reauthorize the stored command too. A failed check
    // keeps exactly the original inactive proof and never mints or delivers a replacement.
    tuple(
        client
            .post(format!("{origin}/jobs/existing-original/stop"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap(),
        true,
    )
    .await;
    let suspended = catalog.get("existing-original").unwrap().unwrap();
    inactive(main_root.path(), &suspended);
    assert!(
        remote
            .try_get_job_exists(&suspended.remote_name)
            .await
            .unwrap()
    );
    let files = grant_files(main_root.path());
    let mut denied = sysargs.clone();
    denied.push("__mrsm_job_supervisor__".into());
    stored_command(main_root.path(), &suspended.name, &denied);
    tuple(
        client
            .post(format!("{origin}/jobs/existing-original/start"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap(),
        false,
    )
    .await;
    let refused = catalog.get(&suspended.name).unwrap().unwrap();
    assert_eq!(refused.callback, suspended.callback);
    assert_eq!(refused.context_revision, suspended.context_revision);
    assert_eq!(grant_files(main_root.path()), files);
    inactive(main_root.path(), &refused);
    assert!(
        remote
            .try_get_job_exists(&refused.remote_name)
            .await
            .unwrap()
    );
    assert_eq!(
        remote.get_job_status(&refused.remote_name).await.unwrap(),
        "stopped"
    );
    stored_command(main_root.path(), &suspended.name, &sysargs);
    tuple(
        client
            .post(format!("{origin}/jobs/existing-original/start"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap(),
        true,
    )
    .await;

    tuple(
        client
            .post(format!("{origin}/jobs/existing-original/stop"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap(),
        true,
    )
    .await;
    let before_code_revoke = catalog.get(&suspended.name).unwrap().unwrap();
    let files = grant_files(main_root.path());
    team.attributes.insert("plugins_write".into(), false.into());
    let edited = instance.edit_team(&team).await;
    assert!(edited.success, "{}", edited.message);
    tuple(
        client
            .post(format!("{origin}/jobs/existing-original/start"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap(),
        false,
    )
    .await;
    let after_code_revoke = catalog.get(&suspended.name).unwrap().unwrap();
    assert_eq!(after_code_revoke.callback, before_code_revoke.callback);
    assert_eq!(
        after_code_revoke.context_revision,
        before_code_revoke.context_revision
    );
    assert_eq!(grant_files(main_root.path()), files);
    inactive(main_root.path(), &after_code_revoke);
    let revoked_remote = controller.prepare(&team).await.unwrap();
    assert!(
        revoked_remote
            .try_get_job_exists(&after_code_revoke.remote_name)
            .await
            .unwrap()
    );
    assert_eq!(
        revoked_remote
            .get_job_status(&after_code_revoke.remote_name)
            .await
            .unwrap(),
        "stopped"
    );
    // Restoring Team code rights permits a later explicit start under the same original
    // source; no Jobs-purpose grant is converted into Command data callback authority.
    team.attributes.insert("plugins_write".into(), true.into());
    let edited = instance.edit_team(&team).await;
    assert!(edited.success, "{}", edited.message);
    tuple(
        client
            .post(format!("{origin}/jobs/existing-original/start"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap(),
        true,
    )
    .await;
    let restarted = catalog.get(&suspended.name).unwrap().unwrap();
    let proof: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            main_root
                .path()
                .join("executors/callbacks")
                .join(format!("{}.json", restarted.callback.unwrap())),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(proof["purpose"], "jobs");
    assert!(
        !proof["permissions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|permission| permission == "pipes:read")
    );

    // A controlled rebuild must recheck the stored command before same-revision delivery,
    // even when its Jobs-only source grant remains valid and a fresh admin can still run it.
    let pipe = meerschaum_core::Pipe::builder("manual:recovery", "guarded")
        .instance("sql:team")
        .parameters(core(
            json!({"access":{"owner":original.username,"team":team.name,"visibility":"private"}}),
        ))
        .build()
        .unwrap();
    let registered = team_instance.register_pipe(&pipe).await;
    assert!(registered.success, "{}", registered.message);
    let command = vec![
        "show",
        "pipes",
        "-i",
        "sql:team",
        "-c",
        "manual:recovery",
        "-m",
        "guarded",
        "--loop",
        "--min-seconds",
        "30",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();
    tuple(
        client
            .post(format!("{origin}/jobs/rebuild-source-scope"))
            .bearer_auth(&original_token)
            .json(&json!({"sysargs":command,"properties":{}}))
            .send()
            .await
            .unwrap(),
        true,
    )
    .await;
    let rebuilding = catalog.get("rebuild-source-scope").unwrap().unwrap();
    let active_api = controller.prepare(&team).await.unwrap();
    status(&active_api, &rebuilding.remote_name, "running").await;
    let old_process = running_identity(control.path(), &rebuilding.remote_name, None).await;
    let files = grant_files(main_root.path());
    let original_attributes = original.attributes.clone();
    let mut scoped_edit = original.clone();
    scoped_edit.password.clear();
    let scopes = meerschaum_core::access::Role::Operator
        .permissions()
        .into_iter()
        .filter(|scope| scope != "pipes:read")
        .map(Value::Str)
        .collect();
    scoped_edit
        .attributes
        .as_mut()
        .unwrap()
        .insert("scopes".into(), Value::List(scopes));
    let edited = instance.edit_user(&scoped_edit).await;
    assert!(edited.success, "{}", edited.message);
    let launch_id =
        uuid::Uuid::parse_str(rebuilding.remote_name.strip_prefix("mrsm-").unwrap()).unwrap();
    let source = meerschaum_server::executor::callbacks::load_identity(
        &state,
        rebuilding.callback.unwrap(),
        launch_id,
    )
    .await
    .unwrap();
    assert!(source.source_access().has("actions:execute"));
    assert!(!source.source_access().has("pipes:read"));

    // A harmless granted-source-set change forces a real rebuild. It captures the actually
    // running command, stops it and waits for fresh same-revision Main authority.
    team.attributes.insert(
        "plugins".into(),
        Value::List(vec!["rebuild_scope_marker".into()]),
    );
    let edited = instance.edit_team(&team).await;
    assert!(edited.success, "{}", edited.message);
    let rebuilt_api = controller.prepare(&team).await.unwrap();
    status(&rebuilt_api, &rebuilding.remote_name, "stopped").await;
    assert_eq!(
        pending(control.path(), &rebuilding.remote_name).unwrap()["revision"],
        rebuilding.context_revision
    );
    assert!(
        !control
            .path()
            .join("execution-contexts")
            .join(launch_id.simple().to_string())
            .exists()
    );
    let maintenance = Task(tokio::spawn(controller.clone().maintain()));
    let deadline = Instant::now() + Duration::from_secs(20);
    let withheld = loop {
        let current = catalog.get(&rebuilding.name).unwrap().unwrap();
        if current.callback_suspended && pending(control.path(), &rebuilding.remote_name).is_none()
        {
            break current;
        }
        assert!(
            Instant::now() < deadline,
            "Maintenance did not withhold recovery after original pipe scope loss."
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    inactive(main_root.path(), &withheld);
    assert_eq!(withheld.callback, rebuilding.callback);
    assert_eq!(withheld.context_revision, rebuilding.context_revision);
    assert_eq!(withheld.owner_binding, rebuilding.owner_binding);
    assert_eq!(withheld.sysargs, command);
    assert_eq!(grant_files(main_root.path()), files);
    assert!(pending(control.path(), &rebuilding.remote_name).is_none());
    assert_eq!(
        began(control.path(), &rebuilding.remote_name).as_deref(),
        Some(old_process.2.as_str())
    );
    assert_eq!(
        rebuilt_api
            .get_job_status(&rebuilding.remote_name)
            .await
            .unwrap(),
        "stopped"
    );

    // Restoring the source's data scope does not replay a stopped command in maintenance.
    // The retained original proof remains inactive until a separately explicit start.
    scoped_edit.attributes = original_attributes;
    let edited = instance.edit_user(&scoped_edit).await;
    assert!(edited.success, "{}", edited.message);
    let observe_until = Instant::now() + Duration::from_secs(16);
    while Instant::now() < observe_until {
        assert_eq!(
            rebuilt_api
                .get_job_status(&rebuilding.remote_name)
                .await
                .unwrap(),
            "stopped"
        );
        assert_eq!(
            catalog.get(&rebuilding.name).unwrap().unwrap().callback,
            rebuilding.callback
        );
        assert_eq!(
            began(control.path(), &rebuilding.remote_name).as_deref(),
            Some(old_process.2.as_str())
        );
        assert!(
            meerschaum_jobs::process::process_start_time(old_process.0) != Some(old_process.1)
                || meerschaum_jobs::process::proc_state(old_process.0)
                    != meerschaum_jobs::process::ProcState::Alive
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(grant_files(main_root.path()), files);
    tuple(
        client
            .post(format!("{origin}/jobs/{}/start", rebuilding.name))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap(),
        true,
    )
    .await;
    let explicitly_restarted = catalog.get(&rebuilding.name).unwrap().unwrap();
    assert_eq!(explicitly_restarted.remote_name, rebuilding.remote_name);
    assert_eq!(explicitly_restarted.owner_binding, rebuilding.owner_binding);
    assert_eq!(explicitly_restarted.sysargs, command);
    assert_eq!(
        explicitly_restarted.context_revision,
        rebuilding.context_revision + 1
    );
    assert_ne!(explicitly_restarted.callback, rebuilding.callback);
    status(&rebuilt_api, &rebuilding.remote_name, "running").await;
    let new_process = running_identity(
        control.path(),
        &rebuilding.remote_name,
        Some(&old_process.2),
    )
    .await;
    assert_ne!(
        (new_process.0, new_process.1),
        (old_process.0, old_process.1)
    );
    assert_ne!(new_process.2, old_process.2);
    tuple(
        client
            .post(format!("{origin}/jobs/{}/stop", rebuilding.name))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap(),
        true,
    )
    .await;
    drop(maintenance);

    let remote = controller.prepare(&team).await.unwrap();
    absent(&remote, &after.remote_name).await;
    original.password = "rotated-synthetic-pass".into();
    original.password_hash = None;
    assert!(instance.edit_user(&original).await.success);
    tuple(
        client
            .post(format!("{origin}/jobs/missing-original/start"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap(),
        false,
    )
    .await;
    assert!(!remote.try_get_job_exists(&after.remote_name).await.unwrap());
    assert!(instance.delete_user(&original).await.success);
    let mut replacement = User::new(&original.username, Some("synthetic-fixture-pass"));
    replacement.attributes = original.attributes.clone();
    assert!(instance.register_user(&mut replacement).await.success);
    assert_ne!(
        instance.get_user_id(&replacement).await.unwrap().unwrap(),
        original_id
    );
    // Restore the same team membership too: a reused username still cannot inherit the source.
    assert!(instance.edit_team(&team).await.success);
    tuple(
        client
            .post(format!("{origin}/jobs/missing-original/start"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap(),
        false,
    )
    .await;
    assert!(!remote.try_get_job_exists(&after.remote_name).await.unwrap());

    // A real owned transport outage must fail rather than recreate on main or reserve a new name.
    let transport_record = catalog.get("missing-transport").unwrap().unwrap();
    absent(&remote, &transport_record.remote_name).await;
    drop(process);
    let unavailable = client
        .post(format!("{origin}/jobs/missing-transport/start"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        unavailable.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        catalog
            .get(&transport_record.name)
            .unwrap()
            .unwrap()
            .remote_name,
        transport_record.remote_name
    );
    assert!(
        !control
            .path()
            .join("jobs")
            .join(&transport_record.remote_name)
            .exists()
    );
    assert!(
        !main_root
            .path()
            .join("jobs")
            .join(&transport_record.name)
            .exists()
    );
    server.abort();
}
