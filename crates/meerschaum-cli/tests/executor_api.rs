//! Real native executor process: signed setup, delegated jobs, logs and private placement.
//! Every process and database belongs to this fixture; no host installation or service is used.
#![cfg(unix)]

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use meerschaum_cli::executor_client::Controller;
use meerschaum_cli::executor_jobs::{Catalog, DelegatingJobs};
use meerschaum_cli::executor_registry::Registry;
use meerschaum_core::access::Team;
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::{Map, Value};
use meerschaum_server::backends::{CreateJob, JobsBackend, LogEvent, NoJobs};
use serde_json::json;

struct MainTerminals(Arc<Controller>);
#[async_trait::async_trait]
impl meerschaum_server::backends::ActionsBackend for MainTerminals {
    fn list(&self) -> Vec<String> {
        vec!["show".into()]
    }
    async fn execute(&self, _: &str, _: Map) -> meerschaum_core::SuccessTuple {
        meerschaum_core::SuccessTuple::fail("This fixture only hosts executor terminals.")
    }
    async fn executor_ready(&self, team: &Team) -> Result<(), String> {
        self.0.prepare(team).await.map(|_| ())
    }
    async fn executor_terminal(
        &self,
        team: &Team,
        caller: &meerschaum_server::access::Access,
    ) -> Result<meerschaum_server::webterm::remote::Terminal, String> {
        self.0.terminal(team, caller).await
    }
    async fn revoke_executor_terminal(&self, id: uuid::Uuid, owner: &str) {
        meerschaum_server::executor::context::scope(None, self.0.revoke_terminal(id, owner)).await;
    }
}

fn team_generation(context: &Path) -> std::path::PathBuf {
    std::fs::read_dir(context)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.join(meerschaum_core::team_scope::source_namespace::FILE)
                .is_file()
        })
        .expect("The owned Team context must have a sealed generation.")
}

async fn console_terminal_round_trip(
    controller: Arc<Controller>,
    instance: Arc<dyn meerschaum_connector::InstanceConnector>,
    team: &mut Team,
    root: &Path,
    executor_root: &Path,
) {
    use futures_util::{SinkExt, StreamExt};
    use meerschaum_web::sessions::SessionStore;
    use tokio_tungstenite::tungstenite::Message;
    let mut user =
        meerschaum_core::users::User::new("console_operator", Some("synthetic-console-pass"));
    user.attributes = Some(core(json!({"role": "operator", "primary_team": team.name})));
    assert!(instance.register_user(&mut user).await.success);
    team.members.insert(
        user.username.clone(),
        meerschaum_core::access::TeamRole::Member,
    );
    assert!(instance.edit_team(team).await.success);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut config = meerschaum_server::ServerConfig::for_root("sql:main", root);
    config.execution_callback_url = Some(format!("http://{address}"));
    config.no_dash = false;
    config.no_webterm = false;
    config.actions_non_admin = true;
    let jobs = Arc::new(DelegatingJobs::new(
        root,
        Arc::new(NoJobs),
        controller.clone(),
    ));
    let state = meerschaum_server::AppState::builder(config, instance.clone())
        .actions(Arc::new(MainTerminals(controller.clone())))
        .jobs(jobs.clone())
        .build()
        .unwrap();
    controller.bind_state(&state).unwrap();
    let maintenance = tokio::spawn(controller.clone().maintain());
    let sessions = Arc::new(meerschaum_web::sessions::InstanceSessions::new(
        instance.clone(),
    ));
    let shell =
        meerschaum_server::webterm::ShellCommand::new("/nonexistent/must-not-start-on-main");
    let web = meerschaum_web::WebConfig {
        sessions: sessions.clone(),
        webterm: Some(meerschaum_web::WebtermSettings::new(shell)),
        pages: Vec::new(),
    };
    let router = meerschaum_web::app(state.clone(), web).unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    // The actual password login issues the binding that the Python console persists in its
    // shared session store. This exercises that store/terminal path without a synthetic Access.
    let login = client
        .post(format!("http://{address}/login"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("username=console_operator&password=synthetic-console-pass")
        .send()
        .await
        .unwrap();
    assert!(
        login.status().is_success(),
        "{}",
        login.text().await.unwrap()
    );
    let login: serde_json::Value = login.json().await.unwrap();
    let claims = meerschaum_server::auth::jwt_decode(
        login["access_token"].as_str().unwrap(),
        state.secret(),
        chrono::Utc::now(),
    )
    .unwrap();
    let session_id = uuid::Uuid::new_v4().to_string();
    sessions
        .set(
            &session_id,
            json!({"username": user.username, "credential": claims["mrsm_credential"]}),
            300,
        )
        .await
        .unwrap();
    assert_eq!(
        client
            .get(format!("http://{address}/webterm/{session_id}"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let socket_url = format!("ws://{address}/websocket/{session_id}");
    let (mut socket, _) = tokio_tungstenite::connect_async(&socket_url).await.unwrap();
    socket
        .send(Message::Text(
            json!(["stdin", "sh printf 'TPR_CONSOLE_''EXECUTOR_OK\\n'\r"])
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let mut output = String::new();
    tokio::time::timeout(Duration::from_secs(20), async {
        while !output.contains("TPR_CONSOLE_EXECUTOR_OK") {
            let Some(Ok(Message::Text(raw))) = socket.next().await else {
                panic!("Main terminal closed: {output:?}");
            };
            let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
            if value[0] == "stdout" {
                output.push_str(value[1].as_str().unwrap());
            }
            assert!(output.len() < 256 * 1024);
        }
    })
    .await
    .unwrap();
    let catalog_path = root.join("executors/terminals.json");
    let catalog = std::fs::read_to_string(&catalog_path).unwrap();
    assert!(
        !catalog.contains(&session_id),
        "Console session UUID crossed the executor boundary."
    );
    let records: serde_json::Value = serde_json::from_str(&catalog).unwrap();
    let terminal_id = records[0]["grant"]["id"].as_str().unwrap().to_string();
    assert!(records[0]["callback"].is_string());
    let callback_id = records[0]["callback"].as_str().unwrap().to_string();
    socket
        .send(Message::Text(
            json!([
                "stdin",
                "show version -d --name terminal-callback-child --yes --noask\r"
            ])
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(25);
    let child = loop {
        if let Some(record) = Catalog::new(root).get("terminal-callback-child").unwrap() {
            break record;
        }
        assert!(
            Instant::now() < deadline,
            "Terminal did not create a main-owned job: {output:?}"
        );
        tokio::select! {
            message = socket.next() => {
                let Some(Ok(Message::Text(raw))) = message else { panic!("Terminal callback failed: {output:?}"); };
                let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
                if value[0] == "stdout" { output.push_str(value[1].as_str().unwrap()); }
            },
            _ = tokio::time::sleep(Duration::from_millis(25)) => {},
        }
    };
    let ownership =
        meerschaum_core::job_access::JobAccess::from_properties(&child.name, &child.properties);
    assert!(
        child.callback.is_some(),
        "A canonical authenticated child needs its own authority."
    );
    assert_eq!(ownership.owner.as_deref(), Some(user.username.as_str()));
    assert_eq!(ownership.team.as_deref(), Some(team.name.as_str()));
    assert!(!root.join("jobs/terminal-callback-child").exists());
    assert!(
        !executor_root.join("jobs/terminal-callback-child").exists(),
        "The public child name was created locally in the executor."
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    while !executor_root.join("jobs").join(&child.remote_name).exists() {
        assert!(
            Instant::now() < deadline,
            "The canonical child was not delivered to its executor."
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(jobs.delete(&child.name).await.success);
    socket.close(None).await.unwrap();
    // A fresh connection uses the same main-owned owner/team identity.
    let (mut socket, _) = tokio_tungstenite::connect_async(&socket_url).await.unwrap();
    socket
        .send(Message::Text(
            json!(["shell_state", "console-probe"]).to_string().into(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let Some(Ok(Message::Text(raw))) = socket.next().await else {
                panic!("Reconnect failed");
            };
            let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
            if value[0] == "shell_state" && value[1] == "console-probe" {
                break;
            }
        }
    })
    .await
    .unwrap();
    let records: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&catalog_path).unwrap()).unwrap();
    assert_eq!(records.as_array().unwrap().len(), 1);
    assert_eq!(records[0]["grant"]["id"], terminal_id);
    let context_dir = executor_root.join("execution-contexts").join(
        uuid::Uuid::parse_str(&terminal_id)
            .unwrap()
            .simple()
            .to_string(),
    );
    let callback: meerschaum_connector_api::models::ExecutionCredential = serde_json::from_slice(
        &std::fs::read(team_generation(&context_dir).join("credential.json")).unwrap(),
    )
    .unwrap();
    let archive: serde_json::Value = client
        .post(format!("http://{address}/jobs/execute-private-archive"))
        .bearer_auth(&callback.token)
        .json(&json!({"sysargs": ["show", "version"], "properties": {}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        archive[0], true,
        "Private history admission failed: {archive}"
    );
    // The first delegated job starts another CLI job through its own callback context.
    let nested_command = format!(
        "exec {} show version --loop --min-seconds 30 -d --name nested-callback-child --yes --noask",
        meerschaum_cli::shlex::join(&[env!("CARGO_BIN_EXE_mrsm-cli").to_string()])
    );
    let response: serde_json::Value = client
        .post(format!("http://{address}/jobs/nesting-parent"))
        .bearer_auth(&callback.token)
        .json(&json!({"sysargs": ["sh", "bash", "--", "-c", nested_command], "properties": {}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        response[0], true,
        "Nested parent admission failed: {response}"
    );
    let deadline = Instant::now() + Duration::from_secs(25);
    let nested = loop {
        if let Some(record) = Catalog::new(root).get("nested-callback-child").unwrap()
            && executor_root
                .join("jobs")
                .join(&record.remote_name)
                .join("properties.json")
                .exists()
        {
            break record;
        }
        assert!(
            Instant::now() < deadline,
            "A delegated job did not route its child to main: {:?}",
            jobs.logs("nesting-parent").await
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(nested.callback.is_some());
    let nested_ownership =
        meerschaum_core::job_access::JobAccess::from_properties(&nested.name, &nested.properties);
    assert_eq!(
        nested_ownership.owner.as_deref(),
        Some(user.username.as_str())
    );
    assert_eq!(nested_ownership.team.as_deref(), Some(team.name.as_str()));
    assert!(!executor_root.join("jobs/nested-callback-child").exists());
    assert!(jobs.delete("nesting-parent").await.success);
    let nested_dir = executor_root.join("execution-contexts").join(
        uuid::Uuid::parse_str(nested.remote_name.trim_start_matches("mrsm-"))
            .unwrap()
            .simple()
            .to_string(),
    );
    let nested_callback: meerschaum_connector_api::models::ExecutionCredential =
        serde_json::from_slice(
            &std::fs::read(team_generation(&nested_dir).join("credential.json")).unwrap(),
        )
        .unwrap();
    assert_eq!(
        client
            .get(format!("http://{address}/jobs"))
            .bearer_auth(&nested_callback.token)
            .send()
            .await
            .unwrap()
            .status(),
        200,
        "A separately accepted child's authority depended on its completed parent."
    );
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let rotated: meerschaum_connector_api::models::ExecutionCredential =
            serde_json::from_slice(
                &std::fs::read(team_generation(&nested_dir).join("credential.json")).unwrap(),
            )
            .unwrap();
        if rotated.expires_at > nested_callback.expires_at {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "The canonical child callback was not rotated."
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // A main-side administrator restarts the stored definition using its ORIGINAL source.
    assert!(jobs.stop(&nested.name).await.success);
    assert!(!nested_dir.exists());
    assert!(jobs.start(&nested.name).await.success);
    let restarted = Catalog::new(root).get(&nested.name).unwrap().unwrap();
    assert_eq!(restarted.context_revision, nested.context_revision + 1);
    assert_ne!(restarted.callback, nested.callback);
    assert_eq!(restarted.properties, nested.properties);
    assert_eq!(
        client
            .get(format!("http://{address}/jobs"))
            .bearer_auth(&nested_callback.token)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if jobs
            .try_get(&nested.name)
            .await
            .unwrap()
            .is_some_and(|metadata| metadata["status"] == "running")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "The original job did not restart under its new revision."
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(jobs.pause(&nested.name).await.success);
    // Membership loss stops an open browser connection and removes its canonical lease.
    team.members.shift_remove(&user.username);
    assert!(instance.edit_team(team).await.success);
    tokio::time::timeout(Duration::from_secs(25), async {
        while let Some(message) = socket.next().await {
            if matches!(message, Ok(Message::Close(_)) | Err(_)) {
                break;
            }
        }
    })
    .await
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let records: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&catalog_path).unwrap()).unwrap();
        if records == json!([]) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Revocation retained a canonical terminal lease."
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        client
            .get(format!("http://{address}/jobs"))
            .bearer_auth(&callback.token)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while context_dir.exists()
        || root
            .join("executors/callbacks")
            .join(format!("{callback_id}.json"))
            .exists()
    {
        assert!(
            Instant::now() < deadline,
            "Terminal revocation retained callback authority or its credential file."
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let deadline = Instant::now() + Duration::from_secs(25);
    while nested_dir.exists() {
        assert!(
            Instant::now() < deadline,
            "Revocation retained a paused job's callback context."
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let metadata = jobs.try_get(&nested.name).await.unwrap().unwrap();
    assert_eq!(
        metadata["status"], "stopped",
        "Source loss did not stop a paused job."
    );
    assert!(jobs.delete(&nested.name).await.success);
    // Account history follows the registered ID. Password changes and leaving a team keep
    // one's own history; a newly registered account with the same name cannot inherit it.
    let original_id = instance.get_user_id(&user).await.unwrap().unwrap();
    assert!(
        instance
            .edit_user(&meerschaum_core::users::User::new(
                &user.username,
                Some("updated-console-pass")
            ))
            .await
            .success
    );
    let login: serde_json::Value = client
        .post(format!("http://{address}/login"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("username=console_operator&password=updated-console-pass")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let current_bearer = login["access_token"].as_str().unwrap();
    let history = client
        .get(format!("http://{address}/jobs/execute-private-archive"))
        .bearer_auth(current_bearer)
        .send()
        .await
        .unwrap();
    assert_eq!(history.status(), 200);
    assert!(!history.text().await.unwrap().contains("owner_binding"));
    assert!(
        instance
            .delete_user(&meerschaum_core::users::User::new(&user.username, None))
            .await
            .success
    );
    let mut replacement =
        meerschaum_core::users::User::new(&user.username, Some("replacement-console-pass"));
    replacement.attributes = Some(core(json!({"role":"operator", "primary_team":team.name})));
    assert!(instance.register_user(&mut replacement).await.success);
    assert_ne!(
        instance.get_user_id(&replacement).await.unwrap().unwrap(),
        original_id
    );
    team.members.insert(
        user.username.clone(),
        meerschaum_core::access::TeamRole::Member,
    );
    assert!(instance.edit_team(team).await.success);
    let login: serde_json::Value = client
        .post(format!("http://{address}/login"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("username=console_operator&password=replacement-console-pass")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let replacement_bearer = login["access_token"].as_str().unwrap();
    assert_eq!(
        client
            .get(format!("http://{address}/jobs/execute-private-archive"))
            .bearer_auth(replacement_bearer)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let listing = client
        .get(format!("http://{address}/jobs"))
        .bearer_auth(replacement_bearer)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!listing.contains("execute-private-archive"));
    assert!(jobs.delete("execute-private-archive").await.success);
    maintenance.abort();
    server.abort();
}

async fn terminal_client(
    controller: &Controller,
    team: &Team,
    name: &str,
) -> Arc<meerschaum_connector_api::APIConnector> {
    let mut caller = meerschaum_server::access::Access::everything();
    caller.no_auth = false;
    caller.auth = Some(meerschaum_server::auth::Principal::User(
        meerschaum_core::users::User::new(name, None),
    ));
    caller.principal = meerschaum_core::access::Principal::new(
        name,
        false,
        meerschaum_core::access::Role::Operator.permissions(),
        vec![(team.name.clone(), meerschaum_core::access::TeamRole::Member)],
    );
    caller.primary_team = team.name.clone();
    caller.team_records = vec![team.clone()];
    meerschaum_server::executor::context::scope(Some(Arc::new(caller)), controller.prepare(team))
        .await
        .unwrap()
}

async fn terminal_output(
    socket: &mut meerschaum_connector_api::client::AuthenticatedSocket,
    needle: &str,
) {
    use meerschaum_connector_api::client::SocketMessage;
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut output = String::new();
        loop {
            let message = socket.next().await.unwrap().unwrap_or_else(|| {
                panic!("The executor terminal closed before {needle:?}. Received {output:?}")
            });
            if let SocketMessage::Text(text) = message {
                let message: serde_json::Value = serde_json::from_str(&text).unwrap();
                if message[0] == "stdout" {
                    output.push_str(message[1].as_str().unwrap());
                }
                assert!(
                    output.len() < 256 * 1024,
                    "The executor terminal flooded its output."
                );
                if output.contains(needle) {
                    break;
                }
            }
        }
    })
    .await
    .expect("The executor terminal did not execute its command.");
}

async fn terminal_round_trip(controller: &Controller, team: &Team, root: &Path) {
    use meerschaum_connector_api::client::{Body, Method, RequestOptions, SocketMessage};
    use meerschaum_server::executor::gateway::TERMINALS_PATH;
    let api = terminal_client(controller, team, "operator").await;
    let grant = meerschaum_server::executor::terminal::TerminalGrant {
        id: uuid::Uuid::new_v4(),
        owner: "operator".into(),
        owner_id: "registered-fixture-operator".into(),
    };
    let path = format!("{TERMINALS_PATH}{}", grant.id);
    let response = api
        .make_request(
            Method::Put,
            &path,
            RequestOptions {
                body: Body::Json(serde_json::to_string(&grant).unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    let context = meerschaum_server::executor::environment::Update {
        metadata_deadline: None,
        discovery_catalog_sha256: None,
        extension: None,
        registration: None,
        id: grant.id,
        revision: 1,
        owner: grant.owner.clone(),
        owner_id: Some(grant.owner_id.clone()),
        profile: meerschaum_server::executor::environment::Profile::Team,
        target: meerschaum_server::executor::environment::Target::Terminal,
        callback_url: "http://127.0.0.1:1".into(),
        credential: meerschaum_connector_api::models::ExecutionCredential {
            token: "synthetic-transport-fixture-no-live-authority".into(),
            expires_at: chrono::Utc::now().timestamp() + 30,
        },
    };
    let response = api
        .make_request(
            Method::Put,
            &format!(
                "{}{}",
                meerschaum_server::executor::gateway::CONTEXTS_PATH,
                grant.id
            ),
            RequestOptions {
                body: Body::Json(serde_json::to_string(&context).unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    let socket_path = format!("/websocket/{}", grant.id);
    let bob = terminal_client(controller, team, "bob").await;
    let mut refused = bob.open_authenticated_socket(&socket_path).await.unwrap();
    terminal_output(&mut refused, "Not logged in").await;
    refused.close().await;
    let mut socket = api.open_authenticated_socket(&socket_path).await.unwrap();
    // The terminal echoes input. Assemble the marker in sh so seeing it proves execution.
    socket
        .send(SocketMessage::Text(
            json!(["stdin", "sh printf 'TPR_EXECUTOR_''TERMINAL_OK\\n'\r"]).to_string(),
        ))
        .await
        .unwrap();
    terminal_output(&mut socket, "TPR_EXECUTOR_TERMINAL_OK").await;
    let config = Config::new(ConfigOptions::with_env(vec![(
        "MRSM_ROOT_DIR".into(),
        root.display().to_string(),
    )]))
    .unwrap();
    let server_config = meerschaum_server::ServerConfig::from_config(&config, &Default::default());
    let base_socket = meerschaum_server::webterm::tmux_socket_path(&server_config).unwrap();
    let tmux_socket = meerschaum_server::webterm::team::team_socket_path(
        &base_socket,
        team.id.as_deref().unwrap(),
    )
    .unwrap();
    let options = Command::new("tmux")
        .arg("-S")
        .arg(&tmux_socket)
        .args(["show-options", "-gv", "default-command"])
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output()
        .unwrap();
    assert!(options.status.success());
    assert_eq!(String::from_utf8(options.stdout).unwrap().trim(), "exit 0");
    let window_count = || {
        let output = Command::new("tmux")
            .arg("-S")
            .arg(&tmux_socket)
            .args(["list-windows", "-a", "-F", "#{window_id}"])
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output()
            .unwrap();
        if output.status.success() {
            String::from_utf8(output.stdout).unwrap().lines().count()
        } else {
            0
        }
    };
    assert_eq!(
        window_count(),
        1,
        "The executor terminal must use its private tmux server."
    );
    socket
        .send(SocketMessage::Text(json!(["new_window"]).to_string()))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while window_count() < 2 {
        assert!(
            Instant::now() < deadline,
            "The executor did not create its second tmux window."
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    socket.close().await;
    let mut reconnected = api.open_authenticated_socket(&socket_path).await.unwrap();
    reconnected
        .send(SocketMessage::Text(
            json!(["stdin", "sh printf 'TPR_EXECUTOR_''RECONNECTED\\n'\r"]).to_string(),
        ))
        .await
        .unwrap();
    terminal_output(&mut reconnected, "TPR_EXECUTOR_RECONNECTED").await;
    assert_eq!(
        window_count(),
        2,
        "Reconnecting lost the executor's tmux windows."
    );
    let response = api
        .make_request(Method::Delete, &path, RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(
        window_count(),
        0,
        "Revoking a terminal left its tmux windows running."
    );
    reconnected.close().await;
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

async fn primary(
    root: &Path,
    snapshot: Map,
) -> (
    ConfigOptions,
    Arc<dyn meerschaum_connector::InstanceConnector>,
) {
    let options = ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.display().to_string()),
        (
            "MRSM_PLUGINS_DIR".into(),
            root.join("plugins").display().to_string(),
        ),
    ]);
    let config = Config::new(options.clone()).unwrap();
    config.set_config(snapshot);
    let session = meerschaum_cli::native::Session::with_runtime(tokio::runtime::Handle::current());
    let instance =
        tokio::task::spawn_blocking(move || session.instance("sql:main", &config).unwrap().conn())
            .await
            .unwrap();
    (options, instance)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_executor_runs_and_streams_jobs_without_creating_host_jobs_or_trusting_remote_owners()
 {
    let main_root = tempfile::tempdir().unwrap();
    let executor_root = tempfile::tempdir().unwrap();
    let control_root = tempfile::tempdir().unwrap();
    for root in [main_root.path(), executor_root.path()] {
        std::fs::create_dir(root.join("plugins")).unwrap();
    }
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    let snapshot = core(
        json!({"meerschaum": {"instance": "sql:main", "api_instance": "sql:main", "connectors": {
        "sql": {"main": {"flavor": "sqlite", "database": main_root.path().join("main.db").display().to_string(), "password": "main-private-synthetic"},
            "team": {"flavor": "sqlite", "database": executor_root.path().join("team.db").display().to_string()}},
        "api": {"executor": {"host": "127.0.0.1", "port": port}}}}, "api": {"no_dash": true}}),
    );
    let (options, instance) = primary(main_root.path(), snapshot.clone()).await;
    let mut team = Team::new("Operators");
    team.attributes = core(
        json!({"isolated": true, "plugins_write": false, "instance": "sql:team", "connectors": ["sql:team"],
        "execution": {"mode": "api", "connector": "api:executor"}}),
    );
    let registered = instance.register_team(&mut team).await;
    assert!(registered.success, "{}", registered.message);
    let desired = Registry::new(main_root.path())
        .reconcile(&[team.clone()])
        .unwrap();
    let verifier = desired.teams.values().next().unwrap().verifier.clone();
    let bootstrap = control_root.path().join("bootstrap.json");
    std::fs::write(&bootstrap, serde_json::to_vec(&verifier).unwrap()).unwrap();
    let log_path = control_root.path().join("launcher.log");
    // A persisted plugin may be malicious or have stale grants. Bootstrap cannot import it
    // before the main API authenticates and supplies the current configuration.
    let import_marker = executor_root.path().join("unexpected-import");
    std::fs::write(
        executor_root.path().join("plugins/bootstrap_probe.py"),
        format!(
            "from pathlib import Path\nPath({:?}).write_text('imported')\n",
            import_marker.display().to_string()
        ),
    )
    .unwrap();
    let log = std::fs::File::create(&log_path).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("TMPDIR", std::env::temp_dir())
        .env("HOME", executor_root.path())
        .env("MRSM_ROOT_DIR", executor_root.path())
        .env("MRSM_PLUGINS_DIR", executor_root.path().join("plugins"))
        .env("MRSM_EXECUTOR_BOOTSTRAP", &bootstrap)
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
        control_root.path().to_str().unwrap(),
    ]);
    drop(reservation);
    let mut process = Process(command.spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            break;
        }
        if process.0.try_wait().unwrap().is_some() || Instant::now() >= deadline {
            panic!(
                "Executor did not start: {}",
                std::fs::read_to_string(&log_path).unwrap()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !import_marker.exists(),
        "Executor bootstrap imported persisted team code."
    );
    let main_controller = || {
        Arc::new(Controller::new(
            main_root.path().into(),
            instance.clone(),
            options.clone(),
            snapshot.clone(),
        ))
    };
    let controller = main_controller();
    let console_controller = main_controller();
    let mut no_auth_config =
        meerschaum_server::ServerConfig::for_root("sql:main", main_root.path());
    no_auth_config.no_auth = true;
    let no_auth_state = meerschaum_server::AppState::builder(no_auth_config, instance.clone())
        .build()
        .unwrap();
    controller.bind_state(&no_auth_state).unwrap();
    let metadata_session =
        meerschaum_cli::native::Session::with_runtime(tokio::runtime::Handle::current());
    metadata_session.set_python_worker(false);
    let metadata_config = Config::new(options).unwrap();
    metadata_config.set_config(snapshot);
    let metadata_copy = metadata_session.clone();
    let team_instance = tokio::task::spawn_blocking(move || {
        metadata_copy
            .instance("sql:team", &metadata_config)
            .unwrap()
            .conn()
    })
    .await
    .unwrap();
    let mut unrelated = Team::new("Unrelated");
    let registered = instance.register_team(&mut unrelated).await;
    assert!(registered.success, "{}", registered.message);
    let forged = meerschaum_core::Pipe::builder("sql:team", "forged_placement")
        .instance("sql:team")
        .parameters(core(json!({"access": {"team": unrelated.name}})))
        .build()
        .unwrap();
    assert!(team_instance.register_pipe(&forged).await.success);
    let forged_command =
        ["sync", "pipes", "-i", "sql:team", "-m", "forged_placement"].map(str::to_owned);
    assert!(
        controller
            .command_team(&forged_command, &metadata_session)
            .await
            .unwrap_err()
            .contains("different executors")
    );
    assert!(team_instance.delete_pipe(&forged).await.success);
    for (metric, owner_team) in [
        ("remote_resource", Some(team.name.as_str())),
        ("host_resource", None),
    ] {
        let mut parameters = core(json!({"access": {"team": owner_team}}));
        if owner_team.is_none() {
            parameters.insert("parents".into(), Value::List(vec![Value::Map(core(json!({
                "connector_keys": "sql:main", "metric_key": "remote_resource", "instance_keys": "sql:main"
            })))]));
        }
        let pipe = meerschaum_core::Pipe::builder("sql:main", metric)
            .instance("sql:main")
            .parameters(parameters)
            .build()
            .unwrap();
        assert!(instance.register_pipe(&pipe).await.success);
    }
    let command = |tail: &[&str]| {
        ["sync", "pipes", "-i", "sql:main"]
            .into_iter()
            .chain(tail.iter().copied())
            .map(str::to_owned)
            .collect::<Vec<_>>()
    };
    // No caller/primary team: resource ownership still chooses the team executor, as for
    // an administrator. A mixed bulk request must fail before any local or remote sync starts.
    assert_eq!(
        controller
            .command_team(&command(&["-m", "remote_resource"]), &metadata_session)
            .await
            .unwrap()
            .unwrap()
            .id,
        team.id
    );
    assert!(
        controller
            .command_team(&command(&["-m", "host_resource"]), &metadata_session)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        controller
            .command_team(&command(&[]), &metadata_session)
            .await
            .unwrap_err()
            .contains("different executors")
    );
    assert!(
        controller
            .command_team(
                &command(&["-m", "host_resource", "--parents"]),
                &metadata_session
            )
            .await
            .unwrap_err()
            .contains("different executors")
    );
    let pipeline = meerschaum_core::Pipe::builder("mrsm", "pipeline")
        .location(Some("mixed"))
        .instance("sql:main")
        .parameters(core(json!({"pipeline": {"name": "mixed", "targets": [
            "sql:main|sql:main|host_resource|None", "sql:main|sql:main|remote_resource|None"
        ]}})))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&pipeline).await.success);
    let pipeline_command = ["sync", "pipelines", "mixed", "-i", "sql:main"].map(str::to_owned);
    assert!(
        controller
            .command_team(&pipeline_command, &metadata_session)
            .await
            .unwrap_err()
            .contains("different executors")
    );
    for words in [
        vec!["show", "pipelines", "mixed"],
        vec!["copy", "pipelines", "mixed", "new_name"],
    ] {
        let command = words
            .into_iter()
            .chain(["-i", "sql:main"])
            .map(str::to_owned)
            .collect::<Vec<_>>();
        assert!(
            controller
                .command_team(&command, &metadata_session)
                .await
                .unwrap()
                .is_none()
        );
    }
    let explicit = meerschaum_core::Pipe::builder("mrsm", "pipeline")
        .location(Some("explicit_only"))
        .instance("sql:main")
        .parameters(core(
            json!({"pipeline": {"name": "explicit_only", "parents": true,
            "targets": ["sql:main|sql:main|host_resource|None"]}}),
        ))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&explicit).await.success);
    let destructive = ["drop", "pipelines", "explicit_only", "-i", "sql:main"].map(str::to_owned);
    assert!(
        controller
            .command_team(&destructive, &metadata_session)
            .await
            .unwrap()
            .is_none()
    );
    let planning = ["plan", "pipelines", "explicit_only", "-i", "sql:main"].map(str::to_owned);
    assert!(
        controller
            .command_team(&planning, &metadata_session)
            .await
            .unwrap_err()
            .contains("different executors")
    );
    let delegated = DelegatingJobs::new(main_root.path(), Arc::new(NoJobs), controller.clone());
    let mut properties = Map::new();
    meerschaum_core::job_access::JobAccess::stamp(
        &mut properties,
        Some("operator"),
        Some(&team.name),
    );
    let created = delegated
        .create(
            "delegated-job",
            vec!["show".into(), "version".into()],
            Some(properties),
        )
        .await;
    match created {
        CreateJob::Started(result) => assert!(result.success, "{}", result.message),
        other => panic!("{other:?}"),
    }
    let metadata = delegated.try_get("delegated-job").await.unwrap().unwrap();
    assert_eq!(metadata["daemon"]["properties"]["owner"], "operator");
    assert_eq!(metadata["daemon"]["properties"]["team"], "Operators");
    assert_eq!(metadata["daemon"]["pid"], serde_json::Value::Null);
    assert!(!main_root.path().join("jobs/delegated-job").exists());
    let record = Catalog::new(main_root.path())
        .get("delegated-job")
        .unwrap()
        .unwrap();
    assert!(
        control_root
            .path()
            .join("jobs")
            .join(&record.remote_name)
            .is_dir()
    );
    // The signed no-auth placement has no callback. Its ordinary job still uses
    // the exact native Team boundary and the durable Team working directory.
    assert!(record.callback.is_none());
    let legacy = control_root
        .path()
        .join("jobs")
        .join(&record.remote_name)
        .join("factory/execution-contexts");
    let generation = std::fs::read_dir(legacy)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let descriptor =
        team_generation(&generation).join(meerschaum_core::team_scope::source_namespace::FILE);
    let launch = meerschaum_core::team_scope::source_namespace::Launch::read(&descriptor).unwrap();
    assert_eq!(launch.namespace.root, executor_root.path());
    assert_eq!(
        launch.sandbox.mode,
        meerschaum_core::team_scope::sandbox::Mode::RequireSockets
    );
    assert!(!descriptor.with_file_name("credential.json").exists());
    let api = controller.prepare(&team).await.unwrap();
    assert!(
        !import_marker.exists(),
        "An ungranted persisted plugin was imported."
    );
    let remote_properties = api.get_job_properties(&record.remote_name).await.unwrap();
    // The fixture has no public caller, so its signed audit identity is `server`. Main-owned
    // metadata must retain `operator`, even when the executor returns another owner.
    assert_eq!(
        remote_properties.get("owner").and_then(Value::as_str),
        Some("server")
    );
    let mut monitor = delegated.monitor_logs("delegated-job").await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(event) = monitor.events.recv().await {
            if let LogEvent::Stopped(result) = event {
                return result;
            }
        }
        panic!(
            "Delegated logs closed without a result: {:?}",
            delegated.logs("delegated-job").await
        );
    })
    .await
    .unwrap();
    assert!(result.success, "{}", result.message);
    assert!(delegated.delete("delegated-job").await.success);
    assert!(
        Catalog::new(main_root.path())
            .get("delegated-job")
            .unwrap()
            .is_none()
    );
    let (result, output) = controller
        .run(&team, vec!["show".into(), "version".into()])
        .await
        .unwrap();
    assert!(result.success, "{}", result.message);
    assert!(output.contains(env!("CARGO_PKG_VERSION")), "{output}");
    assert!(
        Catalog::new(main_root.path())
            .records()
            .unwrap()
            .values()
            .any(|r| r.completed_at.is_some()
                && r.properties
                    .get("mrsm_execution")
                    .and_then(Value::as_map)
                    .and_then(|e| e.get("origin"))
                    .and_then(Value::as_str)
                    == Some("executor_action"))
    );
    let captured = Catalog::new(main_root.path())
        .records()
        .unwrap()
        .into_values()
        .find(|r| {
            r.properties
                .get("mrsm_execution")
                .and_then(Value::as_map)
                .and_then(|e| e.get("origin"))
                .and_then(Value::as_str)
                == Some("executor_action")
        })
        .unwrap();
    assert!(captured.properties.get("owner").is_some_and(Value::is_null));
    team.attributes.insert("plugins_write".into(), true.into());
    assert!(instance.edit_team(&team).await.success);
    terminal_round_trip(&controller, &team, control_root.path()).await;
    console_terminal_round_trip(
        console_controller,
        instance.clone(),
        &mut team,
        main_root.path(),
        control_root.path(),
    )
    .await;
    team.attributes.insert("plugins_write".into(), false.into());
    assert!(instance.edit_team(&team).await.success);
    controller.prepare(&team).await.unwrap();
    assert!(
        !import_marker.exists(),
        "The terminal imported an ungranted persisted plugin."
    );
    team.attributes.insert(
        "plugins".into(),
        Value::List(vec!["bootstrap_probe".into()]),
    );
    assert!(instance.edit_team(&team).await.success);
    let plugin_api = controller.prepare(&team).await.unwrap();
    assert!(
        !import_marker.exists(),
        "The credentialed control runtime imported a granted plugin before confinement."
    );
    assert_eq!(
        plugin_api
            .get("/actions", Default::default())
            .await
            .unwrap()
            .status,
        503,
        "Granted plugin discovery must report unavailable until its confined manifest is ready."
    );
    team.attributes
        .insert("plugins".into(), Value::List(Vec::new()));
    assert!(instance.edit_team(&team).await.success);
    controller.prepare(&team).await.unwrap();
    assert!(
        !import_marker.exists(),
        "Reconfiguration imported a revoked plugin."
    );
    let mut properties = Map::new();
    meerschaum_core::job_access::JobAccess::stamp(
        &mut properties,
        Some("operator"),
        Some(&team.name),
    );
    let created = delegated
        .create(
            "long-running",
            vec![
                "show".into(),
                "version".into(),
                "--loop".into(),
                "--min-seconds".into(),
                "30".into(),
            ],
            Some(properties),
        )
        .await;
    assert!(matches!(created, CreateJob::Started(result) if result.success));
    let record = Catalog::new(main_root.path())
        .get("long-running")
        .unwrap()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if delegated
            .try_get("long-running")
            .await
            .unwrap()
            .is_some_and(|m| m["status"] == "running")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "The delegated loop did not start."
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    team.attributes
        .insert("connectors".into(), Value::List(Vec::new()));
    team.attributes.shift_remove("instance");
    assert!(instance.edit_team(&team).await.success);
    assert!(controller.prepare(&team).await.is_err());
    let context = meerschaum_jobs::JobsContext::for_root(control_root.path());
    let mut stopped = meerschaum_jobs::Job::open(&record.remote_name, &context).unwrap();
    assert!(
        !stopped.is_running(),
        "A configuration revocation left its old job running."
    );
    assert!(!delegated.start("long-running").await.success);
    drop(process);
}
