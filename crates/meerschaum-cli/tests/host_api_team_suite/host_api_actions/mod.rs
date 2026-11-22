//! Ordinary endpoint scopes remain exact while Main tracks confined API workers.

#[allow(unused_imports)]
use crate::common;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use crate::common::http::response;
use crate::common::json_map as map;
use meerschaum_cli::executor::jobs::{Catalog, JobPlacement};
use meerschaum_core::users::User;
use meerschaum_core::{Pipe, Value};
use serde_json::{Value as Json, json};

mod main_actions;
mod pipeline_runs;
mod saved_jobs;
mod sync_and_verify;

fn file_tail(path: &std::path::Path) -> String {
    common::fs::file_tail(path, 16 * 1024)
}

struct Main {
    child: Child,
    log: PathBuf,
}
impl Drop for Main {
    fn drop(&mut self) {
        let status = self.child.try_wait().ok().flatten();
        if std::thread::panicking() {
            eprintln!(
                "Owned Main exit status: {status:?}; log: {}",
                file_tail(&self.log)
            );
            if let Some(root) = self.log.parent() {
                eprintln!("{}", log_tails(root));
            }
        }
        if status.is_none() {
            let _ = Command::new("kill")
                .args(["-INT", &self.child.id().to_string()])
                .env_remove("TMUX")
                .env_remove("TMUX_PANE")
                .status();
        }
        let deadline = Instant::now() + Duration::from_secs(8);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn login(client: &reqwest::Client, origin: &str, username: &str) -> String {
    common::http::login(client, origin, username, "synthetic-scope-pass").await
}

async fn tool(client: &reqwest::Client, origin: &str, token: &str, name: &str, args: Json) -> Json {
    eprintln!("Owned Main MCP fixture calling {name}");
    let (status, body) = response(
        client
            .post(format!("{origin}/mcp"))
            .bearer_auth(token)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": name, "arguments": args},
            })),
    )
    .await;
    assert_eq!(status, 200, "{name}: {body}");
    body
}

fn tool_success(body: &Json) -> bool {
    if body.get("error").is_some() || body["result"]["isError"] == true {
        return false;
    }
    let text = body["result"]["content"][0]["text"].as_str().unwrap_or("");
    serde_json::from_str::<Json>(text)
        .ok()
        .is_some_and(|value| {
            value
                .as_array()
                .is_some_and(|values| values.first() == Some(&Json::Bool(true)))
                || value["success"] == true
        })
}

async fn assert_pipe_stats_denied(client: &reqwest::Client, origin: &str, writeonly: &str) {
    let denied_stats = tool(
        client,
        origin,
        writeonly,
        "get_pipe_stats",
        json!({"connector_keys":"sql:main","metric_key":"write-only"}),
    )
    .await;
    assert_eq!(
        denied_stats["result"]["isError"], true,
        "Write-only token gained verification read tools: {denied_stats}"
    );
    assert_eq!(
        denied_stats["result"]["content"][0]["text"],
        "Missing required scope for 'get_pipe_stats': pipes:read.",
        "Expected a scope denial from get_pipe_stats: {denied_stats}"
    );
}

fn log_tails(root: &std::path::Path) -> String {
    use std::io::{Read, Seek, SeekFrom};
    let mut output = String::new();
    let Some(records) = Catalog::new(root).records().ok() else {
        return output;
    };
    for record in records.into_values().take(8) {
        let base = format!("{}.log", record.remote_name);
        let Some(path) = std::fs::read_dir(root.join("logs"))
            .ok()
            .into_iter()
            .flatten()
            .take(4096)
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let name = entry.file_name().to_str()?.to_string();
                let index = name
                    .strip_prefix(&format!("{base}."))?
                    .parse::<u64>()
                    .ok()?;
                Some((index, entry.path()))
            })
            .max_by_key(|(index, _)| *index)
            .map(|(_, path)| path)
        else {
            continue;
        };
        let Ok(fd) = rustix::fs::open(
            &path,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        ) else {
            continue;
        };
        let mut file = std::fs::File::from(fd);
        let Ok(metadata) = file.metadata() else {
            continue;
        };
        if !metadata.is_file() {
            continue;
        }
        if file
            .seek(SeekFrom::Start(metadata.len().saturating_sub(16 * 1024)))
            .is_err()
        {
            continue;
        }
        let mut bytes = Vec::new();
        if file.take(16 * 1024).read_to_end(&mut bytes).is_ok() {
            output.push_str(&format!(
                "\nOwned log {}:\n{}",
                record.remote_name,
                String::from_utf8_lossy(&bytes)
            ));
        }
    }
    output
}

fn main_root() -> tempfile::TempDir {
    let parent = std::env::var_os("MRSM_ROOT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../test_root"));
    std::fs::create_dir_all(&parent).unwrap();
    tempfile::Builder::new()
        .prefix("host-api-actions-")
        .tempdir_in(parent)
        .unwrap()
}

async fn start_main(
    root: &std::path::Path,
    plugins: &std::path::Path,
    database: &std::path::Path,
) -> (Main, reqwest::Client, String) {
    start_main_with_copy_destination(root, plugins, database, None).await
}

async fn start_main_with_copy_destination(
    root: &std::path::Path,
    plugins: &std::path::Path,
    database: &std::path::Path,
    destination: Option<&std::path::Path>,
) -> (Main, reqwest::Client, String) {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let origin = format!("http://127.0.0.1:{port}");
    let mut patch = json!({"meerschaum":{"instance":"sql:main","executor":"local","connectors":{"sql":{"main":{"flavor":"sqlite","database":database,"password":"synthetic-main-private"}}}},
        "api":{"execution":{"callback_url":origin},"permissions":{"actions":{"non_admin":true}}},
        "system":{"api":{"permissions":{"actions":{"non_admin":true}}}},"jobs":{"startup":{"enabled":false}}});
    if let Some(destination) = destination {
        patch["meerschaum"]["connectors"]["sql"]["copy-dest"] =
            json!({"flavor":"sqlite","database":destination});
        patch["api"]["permissions"]["instances"] = json!({"allow_multiple_instances":true,"allowed_instance_keys":["sql:main","sql:copy-dest"]});
    }
    let log = root.join("main.log");
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    command
        .env_clear()
        .env(
            "PATH",
            std::env::var_os("PATH").unwrap_or_else(|| "/usr/bin:/bin".into()),
        )
        .env("HOME", root)
        .env("MRSM_ROOT_DIR", root)
        .env("MRSM_PLUGINS_DIR", plugins)
        .env(
            "MRSM_PYTHON",
            std::env::var_os("MRSM_PYTHON").unwrap_or_else(|| "/usr/bin/python3".into()),
        )
        .env("MRSM_CONFIG", patch.to_string())
        .env(
            "AWS_SECRET_ACCESS_KEY",
            "synthetic-main-environment-private",
        )
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
    if let Some(path) = std::env::var_os("PYTHONPATH") {
        command.env("PYTHONPATH", path);
    }
    let mut main = Main {
        child: command.spawn().unwrap(),
        log,
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(45))
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        if client
            .get(format!("{origin}/healthcheck"))
            .send()
            .await
            .is_ok_and(|reply| reply.status().is_success())
        {
            break;
        }
        assert!(
            main.child.try_wait().unwrap().is_none() && Instant::now() < deadline,
            "{}",
            std::fs::read_to_string(&main.log).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (main, client, origin)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restricted_console_reads_use_the_original_users_filtered_api() {
    let root = main_root();
    let plugins = root.path().join("plugins");
    std::fs::create_dir_all(&plugins).unwrap();
    let database = root.path().join("main.db");
    let instance = meerschaum_connector_sql::SqlConnector::from_uri(
        "main",
        &format!("sqlite:///{}", database.display()),
    )
    .await
    .unwrap();
    let mut user = User::new("console-reader", Some("synthetic-scope-pass"));
    user.user_type = Some("regular".into());
    user.attributes = Some(map(json!({"role":"custom","scopes":[
        "actions:execute", "jobs:execute", "jobs:write", "jobs:read", "logs:read",
        "pipes:read", "pipes:register", "pipes:write"
    ]})));
    assert!(instance.register_user(&mut user).await.success);
    let visible = Pipe::builder("sql:main", "console-visible")
        .instance("sql:main")
        .parameters(map(json!({
            "columns":{"datetime":"tick","id":"tick"},
            "dtypes":{"tick":"int64"},
            "target":"console_visible",
            "fetch":{"definition":"SELECT 6 AS tick"}
        })))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&visible).await.success);
    let hidden = Pipe::builder("sql:main", "console-hidden")
        .instance("sql:main")
        .parameters(map(
            json!({"access":{"visibility":"private", "owner":"fixture-owner"}}),
        ))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&hidden).await.success);

    use meerschaum_connector_api::models::{EngineEvent, RunRecord};
    use meerschaum_server::backends::EngineBackend;
    let run_id = "console-fixture-run";
    let visible_ref = visible.pipe_ref();
    let stamps = meerschaum_connector::sync_metrics::registrations(&instance)
        .await
        .unwrap();
    let registration = &stamps[&meerschaum_connector::sync_metrics::pipe_key(
        visible.connector_keys(),
        visible.metric_key(),
        visible.location_key(),
    )];
    let history = meerschaum_server::engine_store::InstanceEngine::new(
        std::sync::Arc::new(instance),
        "sql:main",
    );
    let now = chrono::Utc::now();
    let recorded = history
        .record_runs(vec![
            RunRecord {
                run_id: run_id.into(),
                pipe_ref: None,
                started: now,
                finished: Some(now),
                status: "success".into(),
                extra: map(json!({"targets":[visible_ref], "num_nodes":1})),
            },
            RunRecord {
                run_id: run_id.into(),
                pipe_ref: Some(visible_ref),
                started: now,
                finished: Some(now),
                status: "succeeded".into(),
                extra: map(json!({"action":"root", "registration_id":registration})),
            },
        ])
        .await;
    assert!(recorded.success, "{}", recorded.message);
    let recorded = history
        .record_events(vec![EngineEvent {
            run_id: run_id.into(),
            created: now,
            event: "run_finished".into(),
            pipe_ref: None,
            payload: Value::Map(map(json!({
                "event":"run_finished","success":true,"message":"Completed the fixture run."
            }))),
        }])
        .await;
    assert!(recorded.success, "{}", recorded.message);

    let (_main, client, origin) = start_main(root.path(), &plugins, &database).await;
    let token = login(&client, &origin, "console-reader").await;
    let source_job = "execute-console-source";
    let (status, started) = response(
        client
            .post(format!("{origin}/execute"))
            .bearer_auth(&token)
            .json(&json!({"name":source_job,"sysargs":["show","version"]})),
    )
    .await;
    assert_eq!(status, 200, "{started}");
    let catalog = Catalog::new(root.path());
    let wait = |job: String| {
        let origin = &origin;
        let token = &token;
        let client = &client;
        let catalog = &catalog;
        async move {
            let deadline = Instant::now() + Duration::from_secs(40);
            loop {
                let (status, state) = response(
                    client
                        .get(format!("{origin}/jobs/{job}"))
                        .bearer_auth(token),
                )
                .await;
                assert_eq!(status, 200, "{state}");
                if state["status"] == "stopped" && state["completion_pending"] != true {
                    assert_eq!(state["result"][0], true, "{job}: {state}");
                    assert_eq!(
                        catalog.get(&job).unwrap().unwrap().placement,
                        JobPlacement::HostApi
                    );
                    let (status, logs) = response(
                        client
                            .get(format!("{origin}/logs/{job}"))
                            .bearer_auth(token),
                    )
                    .await;
                    assert_eq!(status, 200, "{logs}");
                    return logs.as_str().unwrap().to_string();
                }
                assert!(Instant::now() < deadline, "{job}: {state}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    let pipeline_name = "console-pipeline";
    let register = json!({"targets":[visible.pipe_ref()],"graph":true}).to_string();
    let edit =
        json!({"revision":1,"description":"Edited through the authenticated CLI"}).to_string();
    for (verb, params) in [("register", register), ("edit", edit)] {
        let job = format!("execute-console-{verb}");
        let (status, started) = response(
            client.post(format!("{origin}/execute")).bearer_auth(&token)
                .json(&json!({"name":job,"sysargs":[verb,"pipelines",pipeline_name,"--params",params]})),
        ).await;
        assert_eq!(status, 200, "{started}");
        wait(job).await;
    }
    let (status, registered) = response(
        client
            .get(format!("{origin}/pipelines/{pipeline_name}"))
            .bearer_auth(&token),
    )
    .await;
    assert_eq!(status, 200, "{registered}");
    assert_eq!(registered["pipeline"]["revision"], 2, "{registered}");
    assert_eq!(
        registered["pipeline"]["targets"],
        json!([visible.pipe_ref()]),
        "{registered}"
    );
    assert_eq!(
        registered["pipeline"]["description"], "Edited through the authenticated CLI",
        "{registered}"
    );

    wait(source_job.to_string()).await;
    for (index, (words, expected)) in [
        (vec!["show", "pipes", "--nopretty"], "console-visible"),
        (vec!["show", "graph", "--nopretty"], "console-visible"),
        (vec!["show", "runs", "--nopretty"], run_id),
        (vec!["watch", "runs", "--nopretty"], "run_finished"),
        (vec!["show", "logs", source_job, "--nopretty"], source_job),
    ]
    .into_iter()
    .enumerate()
    {
        let job = format!("execute-console-read-{index}");
        let (status, result) = response(
            client
                .post(format!("{origin}/execute"))
                .bearer_auth(&token)
                .json(&json!({"name":job,"sysargs":words})),
        )
        .await;
        assert_eq!(status, 200, "{words:?}: {result}");
        let logs = wait(job).await;
        assert!(logs.contains(expected), "{words:?}: {logs}");
        assert!(!logs.contains("console-hidden"), "{words:?}: {logs}");
    }
    let name = "persisted-console-runs";
    let (status, result) = response(
        client
            .post(format!("{origin}/jobs/{name}"))
            .bearer_auth(&token)
            .json(&json!(["show", "runs", "--nopretty"])),
    )
    .await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result[0], true, "{result}");
    assert!(wait(name.to_string()).await.contains(run_id));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regular_user_syncs_one_registered_pipeline_node_with_visible_history() {
    let root = main_root();
    let plugins = root.path().join("plugins");
    std::fs::create_dir_all(&plugins).unwrap();
    let database = root.path().join("main.db");
    let instance = meerschaum_connector_sql::SqlConnector::from_uri(
        "main",
        &format!("sqlite:///{}", database.display()),
    )
    .await
    .unwrap();
    let mut user = User::new("console-reader", Some("synthetic-scope-pass"));
    user.user_type = Some("regular".into());
    user.attributes = Some(map(json!({"role":"custom","scopes":[
        "actions:execute", "jobs:execute", "jobs:write", "jobs:read", "logs:read",
        "pipes:read", "pipes:register", "pipes:write"
    ]})));
    assert!(instance.register_user(&mut user).await.success);
    let pipe = Pipe::builder("sql:main", "pipeline-single")
        .instance("sql:main")
        .parameters(map(json!({
            "columns":{"datetime":"tick","id":"tick"},
            "dtypes":{"tick":"int64"},
            "target":"pipeline_single",
            "fetch":{"definition":"SELECT 6 AS tick"}
        })))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let (_main, client, origin) = start_main(root.path(), &plugins, &database).await;
    let token = login(&client, &origin, "console-reader").await;
    let name = "console-pipeline";
    let (status, registered) = response(
        client
            .post(format!("{origin}/pipelines/{name}/register"))
            .bearer_auth(&token)
            .json(&json!({"pipeline":{
                "name":name,"registry":"sql:main","schema_version":2,
                "targets":[pipe.pipe_ref()],"graph":true,"revision":1
            }})),
    )
    .await;
    assert_eq!(status, 200, "{registered}");
    assert_eq!(registered["success"], true, "{registered}");

    let run_id = "single-node-pipeline-run";
    let (status, started) = response(
        client
            .post(format!("{origin}/pipelines/{name}/sync"))
            .bearer_auth(&token)
            .json(&json!({"run_id":run_id,"begin":5,"end":8})),
    )
    .await;
    assert_eq!(status, 200, "{started}");
    assert_eq!(started["run_id"], run_id, "{started}");
    let job = started["job"].as_str().unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let (status, state) = response(
            client
                .get(format!("{origin}/jobs/{job}"))
                .bearer_auth(&token),
        )
        .await;
        assert_eq!(status, 200, "{state}");
        if state["status"] == "stopped" && state["completion_pending"] != true {
            assert_eq!(state["result"][0], true, "{state}");
            break;
        }
        assert!(Instant::now() < deadline, "{job}: {state}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let record = Catalog::new(root.path()).get(job).unwrap().unwrap();
    assert_eq!(record.placement, JobPlacement::HostApi);

    let rows = instance
        .get_pipe_data(&pipe, &Default::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rows.height(), 1);
    assert_eq!(
        rows.column("tick").unwrap().data.get(0),
        meerschaum_kernel::value::Value::Int(6)
    );
    let (status, history) = response(
        client
            .get(format!("{origin}/engine/runs/{run_id}"))
            .bearer_auth(&token),
    )
    .await;
    assert_eq!(status, 200, "{history}");
    assert_eq!(history["run"]["status"], "success", "{history}");
    assert_eq!(history["nodes"].as_array().unwrap().len(), 1, "{history}");
    assert_eq!(
        history["nodes"][0]["pipe_ref"],
        pipe.pipe_ref(),
        "{history}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regular_user_copies_one_pipe_between_main_sql_instances() {
    use meerschaum_connector_sql::SqlConnector;
    use meerschaum_kernel::frame::{Column, Frame};
    use meerschaum_kernel::value::Value as Cell;

    let root = main_root();
    let plugins = root.path().join("plugins");
    std::fs::create_dir_all(&plugins).unwrap();
    let database = root.path().join("main.db");
    let destination = root.path().join("copy.db");
    let source = SqlConnector::from_uri("main", &format!("sqlite:///{}", database.display()))
        .await
        .unwrap();
    let target =
        SqlConnector::from_uri("copy-dest", &format!("sqlite:///{}", destination.display()))
            .await
            .unwrap();
    let mut user = User::new("pipe-copier", Some("synthetic-scope-pass"));
    user.user_type = Some("regular".into());
    user.attributes = Some(map(json!({"role":"custom","scopes":[
        "actions:execute", "jobs:execute", "jobs:write", "jobs:read", "logs:read",
        "connectors:read", "pipes:read", "pipes:register", "pipes:write"
    ]})));
    assert!(source.register_user(&mut user).await.success);
    let parameters = map(json!({
        "columns":{"id":"id"}, "dtypes":{"id":"int64"}, "target":"copy_seed"
    }));
    let original = Pipe::builder("manual:posted", "copy-seed")
        .instance("sql:main")
        .parameters(parameters.clone())
        .build()
        .unwrap();
    assert!(source.register_pipe(&original).await.success);
    let row = Frame::new(vec![Column::object("id", vec![Cell::Int(17)])]).unwrap();
    assert!(
        source
            .sync_pipe(&original, row, &Default::default())
            .await
            .success
    );

    let (_main, client, origin) =
        start_main_with_copy_destination(root.path(), &plugins, &database, Some(&destination))
            .await;
    let token = login(&client, &origin, "pipe-copier").await;
    let name = "execute-pipe-copy";
    let (status, started) = response(
        client
            .post(format!("{origin}/execute"))
            .bearer_auth(&token)
            .json(&json!({
                "name":name, "sysargs":[
                    "copy","pipes","sql:copy-dest","-i","sql:main",
                    "-c","manual:posted","-m","copy-seed","-l","None",
                    "--sync-data","all","--yes","--nopretty"
                ]
            })),
    )
    .await;
    assert_eq!(status, 200, "{started}");
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let (status, state) = response(
            client
                .get(format!("{origin}/jobs/{name}"))
                .bearer_auth(&token),
        )
        .await;
        assert_eq!(status, 200, "{state}");
        if state["status"] == "stopped" && state["completion_pending"] != true {
            assert_eq!(state["result"][0], true, "{state}");
            break;
        }
        assert!(Instant::now() < deadline, "{name}: {state}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let record = Catalog::new(root.path()).get(name).unwrap().unwrap();
    assert_eq!(record.placement, JobPlacement::HostApi);

    let copied = Pipe::builder("manual:posted", "copy-seed")
        .instance("sql:copy-dest")
        .parameters(parameters)
        .build()
        .unwrap();
    let attributes = target.get_pipe_attributes(&copied).await.unwrap();
    let access = attributes["parameters"].get("access").unwrap();
    assert_eq!(
        access.get("owner").and_then(Value::as_str),
        Some("pipe-copier")
    );
    let rows = target
        .get_pipe_data(&copied, &Default::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rows.height(), 1);
    assert_eq!(rows.column("id").unwrap().data.get(0), Cell::Int(17));
}
