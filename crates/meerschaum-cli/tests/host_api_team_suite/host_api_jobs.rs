//! Real native Main API: canonical host jobs use each original source's API-only context.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use meerschaum_cli::executor::jobs::{Catalog, JobPlacement};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Value};
use serde_json::{Value as Json, json};

const PASSWORD: &str = "synthetic-host-original-pass";

fn map(value: Json) -> Map {
    serde_json::from_value::<Value>(value).unwrap().to_map()
}

struct Server {
    child: Child,
    stderr: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .status();
        let deadline = Instant::now() + Duration::from_secs(8);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(25));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn response(request: reqwest::RequestBuilder) -> (u16, Json) {
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let text = response.text().await.unwrap();
    (
        status,
        serde_json::from_str(&text).unwrap_or_else(|_| json!(text)),
    )
}

async fn login(client: &reqwest::Client, origin: &str, name: &str, password: &str) -> String {
    let (status, body) = response(
        client
            .post(format!("{origin}/login"))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(format!("username={name}&password={password}")),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    body["access_token"].as_str().unwrap().to_string()
}

async fn create(client: &reqwest::Client, origin: &str, token: &str, name: &str) {
    let (status, body) = response(
        client
            .post(format!("{origin}/jobs/{name}"))
            .bearer_auth(token)
            .json(&json!({"sysargs":["show","version"], "properties":{
                "owner":"admin", "team":"Other",
                "_mrsm_owner_binding":{"kind":"user","username":"admin","user_id":999},
                "_mrsm_team_id":"00000000-0000-4000-8000-000000000001",
                "env":{"AWS_SECRET_ACCESS_KEY":"caller-forged", "MRSM_JOB_PROGRAM":"/bin/sh"},
                "mrsm_execution":{"kind":"temporary_execution"}
            }})),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body[0], true, "{body}");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (status, body) = response(
            client
                .get(format!("{origin}/jobs/{name}"))
                .bearer_auth(token),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["executor"]["placement"], "host_api", "{body}");
        assert_eq!(body["executor"]["profile"], "api", "{body}");
        assert!(!body.to_string().contains("_mrsm_owner_binding"));
        assert!(!body.to_string().contains("_mrsm_team_id"));
        if body["status"] == "stopped" && body["result"][0] == true {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "Host job never completed: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn assert_context(root: &Path, name: &str, owner: &str, everyone: bool) -> String {
    let record = Catalog::new(root).get(name).unwrap().unwrap();
    assert_eq!(record.placement, JobPlacement::HostApi);
    assert_eq!(record.team_id.is_nil(), everyone);
    assert!(record.callback.is_some());
    assert!(record.owner_binding.is_some());
    assert_eq!(record.properties["owner"], Value::Str(owner.into()));
    let launch = record.remote_name.strip_prefix("mrsm-").unwrap();
    let directory = root.join("execution-contexts").join(launch);
    let properties: Json = serde_json::from_slice(
        &std::fs::read(
            root.join("jobs")
                .join(&record.remote_name)
                .join("properties.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let config_path = PathBuf::from(properties["env"]["MRSM_CONFIG_FILE"].as_str().unwrap());
    let spec_path = PathBuf::from(
        properties["env"][meerschaum_core::team_scope::sandbox::SPEC_VAR]
            .as_str()
            .unwrap(),
    );
    assert!(config_path.starts_with(&directory));
    assert!(spec_path.starts_with(&directory));
    let config = std::fs::read_to_string(config_path).unwrap();
    for forbidden in [
        "synthetic-main-db-secret",
        "hidden_source",
        "AWS_SECRET",
        "caller-forged",
    ] {
        assert!(!config.contains(forbidden), "{forbidden}: {config}");
    }
    assert!(config.contains("mrsm_api_alias"), "{config}");
    assert!(!root.join("jobs").join(name).exists());
    assert!(root.join("jobs").join(&record.remote_name).is_dir());
    let spec: Json = serde_json::from_slice(&std::fs::read(spec_path).unwrap()).unwrap();
    assert_eq!(spec["mode"], "require_sockets");
    assert!(spec["runtime"].is_object());
    record.remote_name
}

fn inactive_generation(
    root: &Path,
    revision: u64,
    generation: &meerschaum_jobs::supervisor::JobGeneration,
) -> bool {
    if generation.inactive().unwrap_or(false) {
        return true;
    }
    let stopped = root
        .join("executor-retired-jobs")
        .join(format!("{}.json", generation.context));
    std::fs::read(stopped)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Json>(&bytes).ok())
        .is_some_and(|proof| {
            proof["revision"] == revision
                && proof["generation"] == serde_json::to_value(generation).unwrap()
        })
}

async fn restore_missing_api_definition(
    client: &reqwest::Client,
    origin: &str,
    token: &str,
    root: &Path,
    name: &str,
) {
    let previous = Catalog::new(root).get(name).unwrap().unwrap();
    let context = root
        .join("execution-contexts")
        .join(previous.remote_name.strip_prefix("mrsm-").unwrap());
    let proof: Json =
        serde_json::from_slice(&std::fs::read(context.join("identity.json")).unwrap()).unwrap();
    let generation: meerschaum_jobs::supervisor::JobGeneration =
        serde_json::from_value(proof["generation"].clone()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !inactive_generation(root, previous.context_revision, &generation) {
        assert!(
            Instant::now() < deadline,
            "The original API job never produced a complete lifecycle proof."
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    std::fs::remove_dir_all(root.join("jobs").join(&previous.remote_name)).unwrap();
    let (status, restored) = response(
        client
            .post(format!("{origin}/jobs/{name}/start"))
            .bearer_auth(token),
    )
    .await;
    assert_eq!(status, 200, "{restored}");
    assert_eq!(
        restored[0], true,
        "A stopped API definition with original source and complete lifecycle proof was not restored: {restored}"
    );
    let current = Catalog::new(root).get(name).unwrap().unwrap();
    assert_eq!(current.remote_name, previous.remote_name);
    assert_eq!(current.context_revision, previous.context_revision + 1);
    assert_eq!(current.owner_binding, previous.owner_binding);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (status, metadata) = response(
            client
                .get(format!("{origin}/jobs/{name}"))
                .bearer_auth(token),
        )
        .await;
        assert_eq!(status, 200, "{metadata}");
        if metadata["status"] == "stopped" && metadata["result"][0] == true {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "The restored API definition did not finish: {metadata}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn rapid_generations(
    client: &reqwest::Client,
    origin: &str,
    token: &str,
    root: &Path,
    name: &str,
) {
    let mut paths = std::collections::BTreeSet::new();
    for lap in 0..66 {
        let previous = Catalog::new(root).get(name).unwrap().unwrap();
        let (status, started) = response(
            client
                .post(format!("{origin}/jobs/{name}/start"))
                .bearer_auth(token),
        )
        .await;
        assert_eq!(status, 200, "Rapid generation {lap}: {started}");
        assert_eq!(started[0], true, "Rapid generation {lap}: {started}");
        let current = Catalog::new(root).get(name).unwrap().unwrap();
        assert_eq!(current.remote_name, previous.remote_name);
        assert_eq!(current.context_revision, previous.context_revision + 1);
        assert_ne!(current.callback, previous.callback);
        let context = root
            .join("execution-contexts")
            .join(current.remote_name.strip_prefix("mrsm-").unwrap());
        let proof: Json =
            serde_json::from_slice(&std::fs::read(context.join("identity.json")).unwrap()).unwrap();
        let generation: meerschaum_jobs::supervisor::JobGeneration =
            serde_json::from_value(proof["generation"].clone()).unwrap();
        assert!(
            paths.insert(generation.directory.clone()),
            "A retired generation was reused."
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let (status, metadata) = response(
                client
                    .get(format!("{origin}/jobs/{name}"))
                    .bearer_auth(token),
            )
            .await;
            assert_eq!(status, 200, "{metadata}");
            if metadata["status"] == "stopped"
                && metadata["result"][0] == true
                && inactive_generation(root, current.context_revision, &generation)
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "Rapid generation {lap} did not complete with a sealed stopped proof: {metadata}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let staged = std::fs::read_dir(&context)
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|entry| {
                        entry
                            .file_name()
                            .to_string_lossy()
                            .starts_with("generation-")
                    })
                    .count()
            })
            .unwrap_or(0);
        assert!(
            staged <= 1,
            "Completed generations accumulated under one canonical job: {staged}"
        );
    }
    assert_eq!(paths.len(), 66);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_host_api_jobs_use_canonical_placement_and_original_registered_history() {
    let parent = std::env::var_os("MRSM_ROOT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../test_root"));
    std::fs::create_dir_all(&parent).unwrap();
    let parent = parent.canonicalize().unwrap_or(parent);
    let root = tempfile::Builder::new()
        .prefix("host-api-jobs-")
        .tempdir_in(parent)
        .unwrap();
    let plugins = root.path().join("plugins");
    std::fs::create_dir_all(&plugins).unwrap();
    let db = root.path().join("main.db");
    let instance = meerschaum_connector_sql::SqlConnector::from_uri(
        "main",
        &format!("sqlite:///{}", db.display()),
    )
    .await
    .unwrap();
    for (name, primary, admin) in [
        ("admin", "Everyone", true),
        ("alice", "Everyone", false),
        ("team_operator", "HostTeam", false),
        ("manager", "HostTeam", false),
    ] {
        let mut user = User::new(name, Some(PASSWORD));
        user.user_type = Some(if admin { "admin" } else { "regular" }.into());
        user.attributes = Some(map(json!({"role":"operator", "primary_team":primary})));
        assert!(instance.register_user(&mut user).await.success);
    }
    let mut team = Team::new("HostTeam");
    team.members
        .insert("team_operator".into(), TeamRole::Member);
    team.members.insert("manager".into(), TeamRole::Manager);
    assert!(instance.register_team(&mut team).await.success);
    let original_team_id = team.id.clone();
    let original_alice_id = instance
        .get_user_id(&User::new("alice", None))
        .await
        .unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let origin = format!("http://127.0.0.1:{port}");
    let patch = json!({
        "meerschaum":{"instance":"sql:main", "executor":"local", "connectors":{"sql":{
            "main":{"flavor":"sqlite", "database":db, "password":"synthetic-main-db-secret"},
            "hidden_source":{"flavor":"sqlite", "database":root.path().join("hidden.db"), "teams":["Other"]}
        }}},
        "api":{"execution":{"callback_url":origin}, "permissions":{"actions":{"non_admin":true}}},
        "system":{"api":{"permissions":{"actions":{"non_admin":true}}}},
        "jobs":{"startup":{"enabled":false}}
    });
    let log = root.path().join("main.log");
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    command
        .env_clear()
        .env(
            "PATH",
            std::env::var_os("PATH").unwrap_or_else(|| "/usr/bin:/bin".into()),
        )
        .env("HOME", root.path())
        .env("MRSM_ROOT_DIR", root.path())
        .env("MRSM_PLUGINS_DIR", &plugins)
        .env(
            "MRSM_PYTHON",
            std::env::var_os("MRSM_PYTHON").unwrap_or_else(|| "/usr/bin/python3".into()),
        )
        .env("MRSM_CONFIG", patch.to_string())
        .env("AWS_SECRET_ACCESS_KEY", "synthetic-host-only-secret")
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .args(["start", "api", "--host", "127.0.0.1", "--port"])
        .arg(port.to_string())
        .args(["--no-dash", "--no-webterm", "-w", "2"])
        .stdin(Stdio::null())
        .stdout(Stdio::from(std::fs::File::create(&log).unwrap()))
        .stderr(Stdio::from(
            std::fs::OpenOptions::new().append(true).open(&log).unwrap(),
        ));
    if let Some(value) = std::env::var_os("PYTHONPATH") {
        command.env("PYTHONPATH", value);
    }
    let mut server = Server {
        child: command.spawn().unwrap(),
        stderr: log,
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(12))
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        if client
            .get(format!("{origin}/healthcheck"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            break;
        }
        assert!(
            server.child.try_wait().unwrap().is_none(),
            "{}",
            std::fs::read_to_string(&server.stderr).unwrap()
        );
        assert!(
            Instant::now() < deadline,
            "{}",
            std::fs::read_to_string(&server.stderr).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let alice = login(&client, &origin, "alice", PASSWORD).await;
    let operator = login(&client, &origin, "team_operator", PASSWORD).await;
    let admin = login(&client, &origin, "admin", PASSWORD).await;
    create(&client, &origin, &alice, "everyone-private").await;
    let private = assert_context(root.path(), "everyone-private", "alice", true);
    restore_missing_api_definition(&client, &origin, &alice, root.path(), "everyone-private").await;
    rapid_generations(&client, &origin, &alice, root.path(), "everyone-private").await;
    create(&client, &origin, &operator, "host-team-private").await;
    assert_context(root.path(), "host-team-private", "team_operator", false);
    for token in [&alice, &admin] {
        let (status, list) =
            response(client.get(format!("{origin}/jobs")).bearer_auth(token)).await;
        assert_eq!(status, 200, "{list}");
        assert!(
            list.get(&private).is_none(),
            "Private transport leaked: {list}"
        );
        assert_eq!(
            response(
                client
                    .get(format!("{origin}/jobs/{private}"))
                    .bearer_auth(token)
            )
            .await
            .0,
            404
        );
        assert_eq!(
            response(
                client
                    .post(format!("{origin}/jobs/{private}/start"))
                    .bearer_auth(token)
            )
            .await
            .0,
            404
        );
    }
    let running = "host-running-source";
    let (status, created) = response(
        client
            .post(format!("{origin}/jobs/{running}"))
            .bearer_auth(&alice)
            .json(
                &json!({"sysargs":["show","version","--loop","--min-seconds","0.1"],
            "properties":{"mrsm_execution":{"kind":"temporary_execution"}}}),
            ),
    )
    .await;
    assert_eq!(status, 200, "{created}");
    assert_eq!(created[0], true, "{created}");
    let deadline = Instant::now() + Duration::from_secs(12);
    let (before, pid, birth, environment) = loop {
        let record = Catalog::new(root.path()).get(running).unwrap().unwrap();
        let directory = root.path().join("jobs").join(&record.remote_name);
        let pid = std::fs::read_to_string(directory.join("process.pid"))
            .ok()
            .and_then(|value| value.trim().parse::<i32>().ok());
        let meta = response(
            client
                .get(format!("{origin}/jobs/{running}"))
                .bearer_auth(&alice),
        )
        .await
        .1;
        if let Some(pid) = pid
            && meta["status"] == "running"
            && let Some(birth) = meerschaum_jobs::process::process_start_time(pid)
        {
            let properties: Json =
                serde_json::from_slice(&std::fs::read(directory.join("properties.json")).unwrap())
                    .unwrap();
            break (record, pid, birth, properties["env"].clone());
        }
        assert!(
            Instant::now() < deadline,
            "Native loop did not start: {meta}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let grant_path = root
        .path()
        .join("executors/callbacks")
        .join(format!("{}.json", before.callback.unwrap()));
    let grant_before: Json = serde_json::from_slice(&std::fs::read(&grant_path).unwrap()).unwrap();
    let (status, started) = response(
        client
            .post(format!("{origin}/jobs/{running}/start"))
            .bearer_auth(&alice),
    )
    .await;
    assert_eq!(status, 200, "{started}");
    assert_eq!(started[0], true, "{started}");
    let after = Catalog::new(root.path()).get(running).unwrap().unwrap();
    assert_eq!(before.callback, after.callback);
    assert_eq!(before.context_revision, after.context_revision);
    let directory = root.path().join("jobs").join(&after.remote_name);
    let properties: Json =
        serde_json::from_slice(&std::fs::read(directory.join("properties.json")).unwrap()).unwrap();
    assert_eq!(
        properties["env"], environment,
        "An already-running start rotated its sealed paths"
    );
    assert_eq!(
        std::fs::read_to_string(directory.join("process.pid"))
            .unwrap()
            .trim(),
        pid.to_string()
    );
    assert_eq!(
        meerschaum_jobs::process::process_start_time(pid),
        Some(birth)
    );
    let grant_after: Json = serde_json::from_slice(&std::fs::read(grant_path).unwrap()).unwrap();
    for key in ["id", "launch_id", "origin", "purpose", "permissions"] {
        assert_eq!(grant_before[key], grant_after[key]);
    }
    assert!(
        instance
            .edit_user(&User::new("alice", Some("synthetic-changed-pass")))
            .await
            .success
    );
    tokio::time::sleep(Duration::from_secs(6)).await;
    let changed = login(&client, &origin, "alice", "synthetic-changed-pass").await;
    let (status, rejected) = response(
        client
            .post(format!("{origin}/jobs/{running}/start"))
            .bearer_auth(&changed),
    )
    .await;
    assert!(
        status >= 400 || rejected[0] == false,
        "A running job adopted new source credentials: {rejected}"
    );
    let deadline = Instant::now() + Duration::from_secs(12);
    while meerschaum_jobs::process::process_start_time(pid) == Some(birth) {
        assert!(
            Instant::now() < deadline,
            "The original running source was not stopped after revocation"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let revoked = Catalog::new(root.path()).get(running).unwrap().unwrap();
    assert_eq!(revoked.callback, before.callback);
    assert_eq!(revoked.context_revision, before.context_revision);
    assert_eq!(
        response(
            client
                .get(format!("{origin}/jobs/everyone-private"))
                .bearer_auth(&changed)
        )
        .await
        .0,
        200
    );
    let (status, restart) = response(
        client
            .post(format!("{origin}/jobs/everyone-private/start"))
            .bearer_auth(&changed),
    )
    .await;
    assert!(
        status >= 400 || restart[0] == false,
        "Original source was replaced: {restart}"
    );
    assert!(instance.delete_team("HostTeam").await.success);
    let mut replacement = Team::new("HostTeam");
    replacement
        .members
        .insert("manager".into(), TeamRole::Manager);
    assert!(instance.register_team(&mut replacement).await.success);
    assert_ne!(replacement.id, original_team_id);
    assert!(
        instance
            .delete_user(&User::new("alice", None))
            .await
            .success
    );
    let mut replacement = User::new("alice", Some("synthetic-replacement-pass"));
    replacement.user_type = Some("regular".into());
    replacement.attributes = Some(map(json!({"role":"operator"})));
    assert!(instance.register_user(&mut replacement).await.success);
    assert_ne!(
        instance.get_user_id(&replacement).await.unwrap(),
        original_alice_id
    );
    tokio::time::sleep(Duration::from_secs(6)).await;
    let replacement = login(&client, &origin, "alice", "synthetic-replacement-pass").await;
    let manager = login(&client, &origin, "manager", PASSWORD).await;
    for (token, name) in [
        (&replacement, "everyone-private"),
        (&manager, "host-team-private"),
    ] {
        for (method, suffix) in [
            ("GET", ""),
            ("POST", "/start"),
            ("POST", "/stop"),
            ("POST", "/pause"),
        ] {
            let request = client
                .request(
                    method.parse().unwrap(),
                    format!("{origin}/jobs/{name}{suffix}"),
                )
                .bearer_auth(token);
            assert_eq!(response(request).await.0, 404, "{method} {name}{suffix}");
        }
        assert_eq!(
            response(
                client
                    .get(format!("{origin}/logs/{name}"))
                    .bearer_auth(token)
            )
            .await
            .0,
            404
        );
    }
    assert_eq!(
        response(
            client
                .get(format!("{origin}/jobs/host-team-private"))
                .bearer_auth(&operator)
        )
        .await
        .0,
        200
    );
    for name in ["everyone-private", "host-team-private", running] {
        assert_eq!(
            response(
                client
                    .delete(format!("{origin}/jobs/{name}"))
                    .bearer_auth(&admin)
            )
            .await
            .0,
            200
        );
    }
}
