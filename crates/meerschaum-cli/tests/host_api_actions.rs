//! Ordinary endpoint scopes remain exact while Main tracks confined API workers.
#![cfg(unix)]

mod common;

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::http::response;
use common::json_map as map;
use meerschaum_cli::executor_jobs::{Catalog, JobPlacement};
use meerschaum_core::users::User;
use meerschaum_core::{Pipe, Value};
use serde_json::{Value as Json, json};

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

async fn saved_job_fixture(
    root: &std::path::Path,
    metric: &str,
) -> (
    meerschaum_connector_sql::SqlConnector,
    Pipe,
    Main,
    reqwest::Client,
    String,
    String,
) {
    let plugins = root.join("plugins");
    std::fs::create_dir_all(&plugins).unwrap();
    let database = root.join("main.db");
    let instance = meerschaum_connector_sql::SqlConnector::from_uri(
        "main",
        &format!("sqlite:///{}", database.display()),
    )
    .await
    .unwrap();
    let mut user = User::new("job-owner", Some("synthetic-scope-pass"));
    user.user_type = Some("regular".into());
    user.attributes = Some(map(
        json!({"role":"custom","scopes":["actions:execute","jobs:read","jobs:write","jobs:execute","jobs:stop","jobs:pause","pipes:write"]}),
    ));
    assert!(instance.register_user(&mut user).await.success);
    let pipe = Pipe::builder("sql:main", metric).instance("sql:main")
        .parameters(map(json!({"columns":{"datetime":"tick","id":"tick"},"dtypes":{"tick":"int64"},
            "target":metric.replace('-', "_"),"fetch":{"definition":"SELECT 4 AS tick UNION ALL SELECT 6 AS tick UNION ALL SELECT 8 AS tick"}})))
        .build().unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let (main, client, origin) = start_main(root, &plugins, &database).await;
    let token = login(&client, &origin, "job-owner").await;
    (instance, pipe, main, client, origin, token)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_pipe_jobs_keep_their_public_command_and_run_each_registered_window() {
    let root = main_root();
    let (instance, pipe, _main, client, origin, token) =
        saved_job_fixture(root.path(), "saved-ticks").await;
    let catalog = Catalog::new(root.path());
    for action in ["sync", "verify"] {
        let name = format!("saved-{action}");
        let url = format!("{origin}/jobs/{name}");
        let words = [
            action,
            "pipes",
            "-c",
            "sql:main",
            "-m",
            "saved-ticks",
            "-l",
            "None",
            "-i",
            "sql:main",
            "--begin=5",
            "--end=8",
        ];
        let mut previous = None;
        for revision in 1..=2 {
            let request = if revision == 1 {
                client.post(&url).json(&words)
            } else {
                client.post(format!("{url}/start"))
            };
            let (status, body) = response(request.bearer_auth(&token)).await;
            assert_eq!(status, 200, "{action} launch {revision}: {body}");
            assert_eq!(body[0], true, "{action} launch {revision}: {body}");
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                let (status, body) = response(client.get(&url).bearer_auth(&token)).await;
                assert_eq!(status, 200, "{body}");
                assert_eq!(body["sysargs"], json!(words));
                if body["status"] == "stopped" && body["completion_pending"] != true {
                    assert_eq!(
                        body["result"][0],
                        true,
                        "{action}: {body} {}",
                        log_tails(root.path())
                    );
                    break;
                }
                assert!(Instant::now() < deadline, "{action} remains {body}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let record = catalog.get(&name).unwrap().unwrap();
            assert_eq!(record.context_revision, revision);
            assert!(record.owner_binding.is_some());
            assert!(record.callback.is_some());
            assert_ne!(record.callback, previous);
            previous = record.callback;
            let grant: Json = serde_json::from_slice(
                &std::fs::read(
                    root.path()
                        .join("executors/callbacks")
                        .join(format!("{}.json", record.callback.unwrap())),
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(grant["required"], json!(["jobs:execute", "pipes:write"]));
            let data = instance
                .get_pipe_data(&pipe, &Default::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(data.height(), 1);
            assert_eq!(
                data.column("tick").unwrap().data.get(0),
                meerschaum_kernel::value::Value::Int(6)
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_pipe_schedule_fires_with_fresh_windows_after_manual_rearm() {
    let root = main_root();
    let (instance, scheduled_pipe, _main, client, origin, token) =
        saved_job_fixture(root.path(), "scheduled-ticks").await;
    let catalog = Catalog::new(root.path());
    let name = "scheduled-sync";
    let url = format!("{origin}/jobs/{name}");
    let words = [
        "sync",
        "pipes",
        "-c",
        "sql:main",
        "-m",
        "scheduled-ticks",
        "-l",
        "None",
        "-i",
        "sql:main",
        "--begin=5",
        "--end=8",
        "--schedule=every 10 seconds",
    ];
    let client_properties = json!({
        "sysargs": words,
        "properties": {
            "label": meerschaum_core::shlex::join(&words.map(str::to_owned)),
            "mrsm_execution": {"kind":"persistent_job","origin":"user_job"}
        }
    });
    let (status, body) = response(
        client
            .post(&url)
            .json(&client_properties)
            .bearer_auth(&token),
    )
    .await;
    assert_eq!((status, body[0].as_bool()), (200, Some(true)), "{body}");
    let mut previous_revision = 1;
    for run in 0..2 {
        if run > 0 {
            let (status, body) =
                response(client.post(format!("{url}/start")).bearer_auth(&token)).await;
            assert_eq!((status, body[0].as_bool()), (200, Some(true)), "{body}");
        }
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let record = catalog.get(name).unwrap().unwrap();
            let (status, body) = response(client.get(&url).bearer_auth(&token)).await;
            assert_eq!(status, 200, "{body}");
            assert_eq!(body["sysargs"], json!(words));
            assert!(
                body["schedule"]["error"].is_null(),
                "{body} {}",
                log_tails(root.path())
            );
            if record.context_revision > previous_revision
                && body["result"][0] == true
                && body["schedule"]["last_fire"].is_string()
                && body["schedule"]["firing"] == false
            {
                previous_revision = record.context_revision;
                break;
            }
            assert!(
                Instant::now() < deadline,
                "Scheduled graph: {body} {}",
                log_tails(root.path())
            );
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        let (control, expected_status) = if run == 0 {
            ("pause", "paused")
        } else {
            ("stop", "stopped")
        };
        let (status, body) =
            response(client.post(format!("{url}/{control}")).bearer_auth(&token)).await;
        assert_eq!((status, body[0].as_bool()), (200, Some(true)), "{body}");
        let (_, body) = response(client.get(&url).bearer_auth(&token)).await;
        assert!(body["schedule"]["next_fire"].is_null(), "{body}");
        assert_eq!(body["status"], expected_status);
        let data = instance
            .get_pipe_data(&scheduled_pipe, &Default::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data.height(), 1);
        assert_eq!(
            data.column("tick").unwrap().data.get(0),
            meerschaum_kernel::value::Value::Int(6)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_main_actions_and_pipe_tools_keep_original_scopes_without_main_execution() {
    let root = main_root();
    let plugins = root.path().join("plugins");
    std::fs::create_dir_all(&plugins).unwrap();
    let private = root.path().join("main-private.txt");
    std::fs::write(&private, "synthetic-main-file-private").unwrap();
    std::fs::write(plugins.join("boundregister.py"),format!(
        "import os\nfrom pathlib import Path\ndef register(pipe, **kw):\n    assert os.environ.get('MRSM_API_EXECUTION') == '1'\n    assert 'AWS_SECRET_ACCESS_KEY' not in os.environ\n    try:\n        Path({:?}).read_text()\n    except OSError:\n        pass\n    else:\n        raise RuntimeError('Main private file was readable')\n    return {{'columns':{{'id':'from_hook'}}, 'tags':['confined-register'], 'hook_pid':os.getpid()}}\ndef fetch(pipe, **kw):\n    return []\n",
        private.display().to_string())).unwrap();
    let database = root.path().join("main.db");
    let instance = meerschaum_connector_sql::SqlConnector::from_uri(
        "main",
        &format!("sqlite:///{}", database.display()),
    )
    .await
    .unwrap();
    for (name, scopes) in [
        ("action", vec!["actions:execute"]),
        ("direct", vec!["actions:execute", "pipes:write"]),
        ("writer", vec!["pipes:read", "pipes:write"]),
        ("writeonly", vec!["pipes:write"]),
        ("reader", vec!["pipes:read"]),
        ("registrar", vec!["pipes:register"]),
    ] {
        let mut user = User::new(name, Some("synthetic-scope-pass"));
        user.user_type = Some("regular".into());
        user.attributes = Some(map(json!({"role":"custom","scopes":scopes})));
        assert!(instance.register_user(&mut user).await.success);
    }
    let pipe = Pipe::builder("sql:main","scope-positive").instance("sql:main")
        .parameters(map(json!({"columns":{"id":"id"},"target":"scope_positive", "fetch":{"definition":"SELECT 1 AS id"}}))).build().unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let write_only = Pipe::builder("sql:main", "write-only").instance("sql:main")
        .parameters(map(json!({"columns":{"id":"id"},"dtypes":{"id":"int64"},"target":"write_only_destination", "fetch":{"definition":"SELECT 2 AS id, X'010203' AS payload"}}))).build().unwrap();
    assert!(instance.register_pipe(&write_only).await.success);
    let pipeline = Pipe::builder("mrsm","pipeline").location(Some("reader")).instance("sql:main")
        .parameters(map(json!({"pipeline":{"id":uuid::Uuid::new_v4().to_string(),"name":"reader","targets":[pipe.pipe_ref()],"graph":true}}))).build().unwrap();
    assert!(instance.register_pipe(&pipeline).await.success);
    let (main, client, origin) = start_main(root.path(), &plugins, &database).await;
    let action = login(&client, &origin, "action").await;
    let writer = login(&client, &origin, "writer").await;
    let writeonly = login(&client, &origin, "writeonly").await;
    let reader = login(&client, &origin, "reader").await;
    let registrar = login(&client, &origin, "registrar").await;
    let (status, result) = response(
        client
            .post(format!("{origin}/actions/show"))
            .bearer_auth(&action)
            .json(&json!({"action":["version"]})),
    )
    .await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result[0], true, "{result}");
    let record = Catalog::new(root.path())
        .records()
        .unwrap()
        .into_values()
        .find(|record| record.action_request)
        .unwrap();
    assert_eq!(record.placement, JobPlacement::HostApi);
    assert!(record.operation.is_none());
    let grant: Json = serde_json::from_slice(
        &std::fs::read(
            root.path()
                .join("executors/callbacks")
                .join(format!("{}.json", record.callback.unwrap())),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(grant["required"], json!(["actions:execute"]));
    assert!(
        !grant["permissions"]
            .as_array()
            .unwrap()
            .contains(&json!("jobs:execute"))
    );
    assert_eq!(
        response(
            client
                .post(format!("{origin}/jobs/forged"))
                .bearer_auth(&action)
                .json(&json!({"sysargs":["show","version"]}))
        )
        .await
        .0,
        403
    );
    let before = Catalog::new(root.path()).records().unwrap().len();
    let tables_before = meerschaum_connector::pipe_ops::frame_to_docs(
        &instance
            .read("SELECT name FROM sqlite_master ORDER BY name")
            .await
            .unwrap(),
    );
    let (status, planned) = response(
        client
            .post(format!("{origin}/pipelines/reader/plan"))
            .bearer_auth(&reader)
            .json(&json!({})),
    )
    .await;
    assert_eq!(status, 200, "{planned}");
    assert_eq!(planned["plan"]["nodes"].as_array().unwrap().len(), 1);
    assert_eq!(
        Catalog::new(root.path()).records().unwrap().len(),
        before,
        "A reader plan created a job"
    );
    assert_eq!(
        meerschaum_connector::pipe_ops::frame_to_docs(
            &instance
                .read("SELECT name FROM sqlite_master ORDER BY name")
                .await
                .unwrap()
        ),
        tables_before,
        "Reader planning created state tables"
    );
    // Exercise MCP scope denial and registration independently of the pipe workers.
    assert_pipe_stats_denied(&client, &origin, &writeonly).await;
    let registered = tool(
        &client,
        &origin,
        &registrar,
        "register_pipe",
        json!({
            "connector_keys": "plugin:boundregister",
            "metric_key": "created",
            "columns": {"id": "caller_override"},
        }),
    )
    .await;
    assert!(
        tool_success(&registered),
        "A register-only source did not use its confined hook: {registered}{}",
        log_tails(root.path())
    );
    let registration = Pipe::builder("plugin:boundregister", "created")
        .instance("sql:main")
        .build()
        .unwrap();
    let attributes = instance.get_pipe_attributes(&registration).await.unwrap();
    let parameters = attributes["parameters"].as_map().unwrap();
    assert_eq!(
        parameters["columns"].get("id"),
        Some(&Value::Str("caller_override".into()))
    );
    assert_eq!(
        parameters["tags"],
        Value::List(vec!["confined-register".into()])
    );
    assert_ne!(
        parameters["hook_pid"].as_i64(),
        Some(main.child.id() as i64)
    );
    let canonical = Catalog::new(root.path())
        .records()
        .unwrap()
        .into_values()
        .find(|record| record.connector_registration.is_some())
        .unwrap();
    assert_eq!(canonical.placement, JobPlacement::HostApi);
    let grant: Json = serde_json::from_slice(
        &std::fs::read(
            root.path()
                .join("executors/callbacks")
                .join(format!("{}.json", canonical.callback.unwrap())),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(grant["required"], json!(["pipes:register"]));
    for permission in ["actions:execute", "jobs:execute", "pipes:read"] {
        assert!(
            !grant["permissions"]
                .as_array()
                .unwrap()
                .contains(&json!(permission)),
            "Registration synthesized {permission}"
        );
    }

    for name in ["sync_pipe", "verify_pipe"] {
        let result = tool(
            &client,
            &origin,
            &writer,
            name,
            json!({"connector_keys":"sql:main","metric_key":"scope-positive"}),
        )
        .await;
        assert!(
            tool_success(&result),
            "{name}: {result}{}",
            log_tails(root.path())
        );
    }
    let operations: Vec<_> = Catalog::new(root.path())
        .records()
        .unwrap()
        .into_values()
        .filter(|record| record.operation.is_some())
        .collect();
    assert_eq!(operations.len(), 2);
    for record in operations {
        assert_eq!(record.placement, JobPlacement::HostApi);
        assert!(!record.action_request);
        let grant: Json = serde_json::from_slice(
            &std::fs::read(
                root.path()
                    .join("executors/callbacks")
                    .join(format!("{}.json", record.callback.unwrap())),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(grant["required"], json!(["pipes:write"]));
        assert!(
            !grant["permissions"]
                .as_array()
                .unwrap()
                .contains(&json!("actions:execute"))
        );
        assert!(
            !grant["permissions"]
                .as_array()
                .unwrap()
                .contains(&json!("jobs:execute"))
        );
    }
    for name in ["sync_pipe", "verify_pipe"] {
        let result = tool(
            &client,
            &origin,
            &writeonly,
            name,
            json!({"connector_keys":"sql:main","metric_key":"write-only"}),
        )
        .await;
        assert!(
            tool_success(&result),
            "Strict write-only {name}: {result}{}",
            log_tails(root.path())
        );
    }
    let strict: Vec<_> = Catalog::new(root.path())
        .records()
        .unwrap()
        .into_values()
        .filter(|record| {
            record
                .operation
                .as_ref()
                .and_then(|operation| operation.endpoint())
                .is_some_and(|operation| operation.pipe_ref == write_only.pipe_ref())
        })
        .collect();
    assert_eq!(strict.len(), 2);
    let inferred = instance
        .get_pipe_parameters(&write_only)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        inferred["dtypes"].as_map().unwrap().get("payload"),
        Some(&Value::Str("bytes".into())),
        "Main did not infer the actual new binary column"
    );
    for key in ["target", "fetch", "access"] {
        assert_eq!(
            inferred.get(key),
            write_only.raw_parameters().get(key),
            "Native inference changed protected {key}"
        );
    }
    let mut current = write_only.clone();
    current.set_parameters(inferred.clone());
    let rows = meerschaum_connector::pipe_ops::frame_to_docs(
        &instance
            .get_pipe_data(&current, &Default::default())
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        rows.len(),
        1,
        "Repeated sync/verify duplicated binary source data"
    );
    assert_eq!(
        rows[0].get("payload"),
        Some(&meerschaum_kernel::value::Value::Bytes(vec![1, 2, 3])),
        "Typed input was flattened into JSON text"
    );
    for (hints, expected) in [
        (json!({"payload":"sql:private"}), 400),
        (json!({"absent":"bytes"}), 400),
        (json!({"id":"bytes"}), 409),
    ] {
        let mut url =
            reqwest::Url::parse(&format!("{origin}/pipes/sql:main/write-only/None/data")).unwrap();
        url.query_pairs_mut()
            .append_pair("new_special_cols", &hints.to_string());
        let (status, body) = response(
            client
                .post(url)
                .bearer_auth(&writeonly)
                .json(&json!([{"id":3,"payload":"AQID"}])),
        )
        .await;
        assert_eq!(
            status, expected,
            "Invalid transport hints changed native metadata: {body}"
        );
    }
    assert_eq!(
        instance.get_pipe_parameters(&write_only).await.unwrap(),
        Some(inferred.clone()),
        "Refused hints adopted caller metadata"
    );
    for record in strict {
        assert!(
            record.operation.as_ref().unwrap().snapshot().is_some(),
            "Write-only work has no exact captured read capability"
        );
        let snapshot =
            serde_json::to_value(record.operation.as_ref().unwrap().snapshot().unwrap()).unwrap();
        assert_eq!(
            snapshot["members"][write_only.pipe_ref()]["parameters"],
            serde_json::to_value(Value::Map(inferred.clone())).unwrap(),
            "The canonical operation did not accept its own proven metadata transition"
        );
        let grant: Json = serde_json::from_slice(
            &std::fs::read(
                root.path()
                    .join("executors/callbacks")
                    .join(format!("{}.json", record.callback.unwrap())),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(grant["required"], json!(["pipes:write"]));
        for permission in ["pipes:read", "sql:read", "actions:execute", "jobs:execute"] {
            assert!(
                !grant["permissions"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(permission)),
                "Write-only operation synthesized {permission}"
            );
        }
    }
    // The originating HTTP token cannot borrow the worker's private operation capability.
    for path in [
        "/pipes",
        "/pipes/sql:main/write-only/None/attributes",
        "/pipes/sql:main/write-only/None/data",
        "/pipes/sql:main/write-only/None/rowcount",
        "/pipes/sql:main/scope-positive/None/attributes",
    ] {
        assert_eq!(
            response(
                client
                    .get(format!("{origin}{path}"))
                    .bearer_auth(&writeonly)
            )
            .await
            .0,
            403,
            "Write-only token gained {path}"
        );
    }
    assert_pipe_stats_denied(&client, &origin, &writeonly).await;
    let denied = tool(
        &client,
        &origin,
        &writer,
        "execute_action",
        json!({"action":"show version"}),
    )
    .await;
    assert!(
        !tool_success(&denied),
        "Pipe scopes gained generic action authority: {denied}"
    );
    assert_eq!(
        response(
            client
                .post(format!("{origin}/jobs/forged-write"))
                .bearer_auth(&writer)
                .json(&json!({"sysargs":["show","version"],"properties":{"action_request":true}}))
        )
        .await
        .0,
        403
    );

    // Run after the registration case: this direct-action slice admits an actual
    // observed empty source set. The ordinary show/version action above used its original grant.
    std::fs::remove_file(plugins.join("boundregister.py")).unwrap();
    let selected = Pipe::builder("sql:main","direct-ticks").instance("sql:main")
        .parameters(map(json!({"columns":{"datetime":"tick","id":"tick"},
            "dtypes":{"tick":"int64"},"target":"direct_ticks",
            "fetch":{"definition":"SELECT 4 AS tick UNION ALL SELECT 6 AS tick UNION ALL SELECT 8 AS tick"}
        }))).build().unwrap();
    assert!(instance.register_pipe(&selected).await.success);
    let direct = login(&client, &origin, "direct").await;
    let args = json!({"action":["pipes"],"connector_keys":["sql:main"],"metric_keys":["direct-ticks"],
        "location_keys":["None"],"mrsm_instance":"sql:main","begin":5,"end":8});
    for operation in ["sync", "verify"] {
        let (status, body) = response(
            client
                .post(format!("{origin}/actions/{operation}"))
                .bearer_auth(&direct)
                .json(&args),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(
            body.get(0),
            Some(&json!(true)),
            "{operation}: {body}{}",
            log_tails(root.path())
        );
    }
    let data = instance
        .get_pipe_data(&selected, &Default::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data.height(), 1);
    assert_eq!(
        data.column("tick").unwrap().data.get(0),
        meerschaum_kernel::value::Value::Int(6)
    );
    let captured: Vec<_> = Catalog::new(root.path())
        .records()
        .unwrap()
        .into_values()
        .filter(|r| {
            matches!(
                r.operation.as_ref(),
                Some(meerschaum_server::executor::pipe_operation::GraphOperation::Direct(_))
            )
        })
        .collect();
    assert_eq!(captured.len(), 2);
    for record in captured {
        assert!(
            record.sysargs.contains(&"--begin=5".into())
                && record.sysargs.contains(&"--end=8".into())
        );
        let grant: Json = serde_json::from_slice(
            &std::fs::read(
                root.path()
                    .join("executors/callbacks")
                    .join(format!("{}.json", record.callback.unwrap())),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(grant["required"], json!(["actions:execute", "pipes:write"]));
        assert!(
            !grant["permissions"]
                .as_array()
                .unwrap()
                .contains(&json!("jobs:execute"))
        );
    }
    let (status, body) = response(
        client
            .post(format!("{origin}/actions/sync"))
            .bearer_auth(&action)
            .json(&args),
    )
    .await;
    assert!(
        status == 403 || (status == 200 && body.get(0) == Some(&json!(false))),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confined_verify_delivers_each_sqlite_segment() {
    let root = main_root();
    let plugins = root.path().join("plugins");
    std::fs::create_dir(&plugins).unwrap();
    let database = root.path().join("main.db");
    let instance = meerschaum_connector_sql::SqlConnector::from_uri(
        "main",
        &format!("sqlite:///{}", database.display()),
    )
    .await
    .unwrap();
    let mut user = User::new("direct", Some("synthetic-scope-pass"));
    user.user_type = Some("regular".into());
    user.attributes = Some(map(
        json!({"role":"custom", "scopes":["actions:execute", "pipes:write"]}),
    ));
    assert!(instance.register_user(&mut user).await.success);
    let pipe = Pipe::builder("sql:main", "segmented-ticks")
        .instance("sql:main")
        .parameters(map(json!({
            "columns": {"datetime":"tick", "id":"tick"},
            "dtypes": {"tick":"int64"},
            "target": "segmented_ticks",
            "verify": {"chunk_range": 2},
            "fetch": {"definition": "SELECT 1 AS tick UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8"}
        })))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let (_main, client, origin) = start_main(root.path(), &plugins, &database).await;
    let token = login(&client, &origin, "direct").await;
    let (status, body) = response(
        client
            .post(format!("{origin}/actions/verify"))
            .bearer_auth(token)
            .json(&json!({
                "action": ["pipes"],
                "connector_keys": ["sql:main"],
                "metric_keys": ["segmented-ticks"],
                "location_keys": ["None"],
                "mrsm_instance": "sql:main",
                "begin": 2,
                "end": 8
            })),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body.get(0),
        Some(&json!(true)),
        "{body}{}",
        log_tails(root.path())
    );

    let data = instance
        .get_pipe_data(&pipe, &Default::default())
        .await
        .unwrap()
        .unwrap();
    let column = data.column("tick").unwrap();
    let mut ticks: Vec<_> = (0..data.height())
        .map(|index| match column.data.get(index) {
            meerschaum_kernel::value::Value::Int(value) => value,
            value => panic!("unexpected tick {value:?}"),
        })
        .collect();
    ticks.sort_unstable();
    assert_eq!(ticks, [2, 3, 4, 5, 6, 7]);

    let records = Catalog::new(root.path()).records().unwrap();
    let captures: Vec<_> = records
        .values()
        .filter(|record| {
            matches!(
                record.operation.as_ref(),
                Some(meerschaum_server::executor::pipe_operation::GraphOperation::Direct(_))
            )
        })
        .collect();
    assert_eq!(captures.len(), 1);
    let record = captures[0];
    assert_eq!(record.placement, JobPlacement::HostApi);
    assert!(!record.commit_journal.has_pending());
    // Each two-row segment must be delivered and acknowledged independently.
    let journal = serde_json::to_value(&record.commit_journal).unwrap();
    assert_eq!(journal["audit"]["acknowledged_commits"], 3, "{journal}");
    assert_eq!(journal["audit"]["affected_rows"], 6, "{journal}");
    let stream = &journal["batches"]["nodes"][pipe.pipe_ref()];
    assert_eq!(stream["retired_through"], 3, "{stream}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confined_pipeline_sync_uses_the_saved_revision_and_both_explicit_targets() {
    assert_captured_pipeline_run(PipelineRequest::Rest).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mcp_sync_pipeline_runs_the_saved_revision_and_both_explicit_targets() {
    assert_captured_pipeline_run(PipelineRequest::Mcp).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connected_pipeline_requiring_a_new_plan_preserves_both_targets() {
    assert_captured_pipeline_run(PipelineRequest::Connected).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_pipeline_records_its_prepared_replacement_watermark_after_delivery() {
    assert_captured_pipeline_run(PipelineRequest::Prepared).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_pipeline_replans_a_new_child_from_the_prepared_parent_watermark() {
    assert_captured_pipeline_run(PipelineRequest::PreparedChild).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_pipeline_replans_a_populated_child_after_parent_delivery() {
    assert_captured_pipeline_run(PipelineRequest::PopulatedChild).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_pipeline_replans_two_children_from_one_delivered_parent() {
    assert_captured_pipeline_run(PipelineRequest::SiblingChildren).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saved_pipeline_bounds_a_child_by_both_committed_parents() {
    assert_captured_pipeline_run(PipelineRequest::TwoParents).await;
}

#[derive(Clone, Copy, PartialEq)]
enum PipelineRequest {
    Prepared,
    PreparedChild,
    PopulatedChild,
    SiblingChildren,
    TwoParents,
    Rest,
    Mcp,
    Connected,
}

impl PipelineRequest {
    fn dependent_children(self) -> bool {
        matches!(
            self,
            Self::PreparedChild | Self::PopulatedChild | Self::SiblingChildren | Self::TwoParents
        )
    }
    fn prepared_parent(self) -> bool {
        self == Self::Prepared || self.dependent_children()
    }
}

async fn assert_captured_pipeline_run(request: PipelineRequest) {
    let root = main_root();
    let (instance, mut first, _main, client, origin, token) =
        saved_job_fixture(root.path(), "pipeline-first").await;
    let instance = std::sync::Arc::new(instance);
    if request.prepared_parent() {
        first.update_parameters(map(json!({"fetch":{"replace":true}})));
        assert!(instance.edit_pipe(&first, false).await.success);
        let prior =
            meerschaum_kernel::frame::Frame::new(vec![meerschaum_kernel::frame::Column::object(
                "tick",
                vec![meerschaum_kernel::value::Value::Int(5)],
            )])
            .unwrap();
        let synced = instance.sync_pipe(&first, prior, &Default::default()).await;
        assert!(synced.success, "{}", synced.message);
    }
    if request == PipelineRequest::Mcp {
        // Earlier releases emitted this plain ID index when datetime and ID shared a column.
        for ddl in [
            r#"CREATE TABLE "pipeline_first" ("tick" BIGINT)"#,
            r#"CREATE INDEX None ON "pipeline_first" ("tick")"#,
        ] {
            assert!(instance.exec(ddl).await.is_some(), "{ddl}");
        }
    }
    let mut parameters = json!({"columns":{"datetime":"tick","id":"tick"}, "dtypes":{"tick":"int64"},
        "target":"pipeline_second", "fetch":{"definition":"SELECT 5 AS tick UNION ALL SELECT 7 UNION ALL SELECT 8"}});
    if request == PipelineRequest::Connected || request.dependent_children() {
        parameters["parents"] = json!([{"connector":"sql:main","metric":"pipeline-first"}]);
        parameters["fetch"]["definition"] = json!("SELECT tick FROM pipeline_first");
        if request == PipelineRequest::TwoParents {
            parameters["parents"]
                .as_array_mut()
                .unwrap()
                .push(json!({"connector":"sql:main","metric":"pipeline-third"}));
            parameters["fetch"]["definition"] =
                json!("SELECT tick FROM pipeline_first UNION SELECT tick FROM pipeline_third");
        }
    }
    let second = Pipe::builder("sql:main", "pipeline-second")
        .instance("sql:main")
        .parameters(map(parameters))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&second).await.success);
    if request == PipelineRequest::PopulatedChild {
        let existing =
            meerschaum_kernel::frame::Frame::new(vec![meerschaum_kernel::frame::Column::object(
                "tick",
                vec![meerschaum_kernel::value::Value::Int(4)],
            )])
            .unwrap();
        let result = instance
            .sync_pipe(&second, existing, &Default::default())
            .await;
        assert!(result.success, "{}", result.message);
    }
    let mut targets = vec![first.pipe_ref(), second.pipe_ref()];
    let third = if matches!(
        request,
        PipelineRequest::SiblingChildren | PipelineRequest::TwoParents
    ) {
        let mut parameters = if request == PipelineRequest::TwoParents {
            let mut parameters = first.raw_parameters().clone();
            parameters.insert("fetch".into(),map(json!({"replace":true, "definition":"SELECT 5 AS tick UNION ALL SELECT 7 AS tick"})).into());
            parameters
        } else {
            second.raw_parameters().clone()
        };
        parameters.insert("target".into(), "pipeline_third".into());
        let pipe = Pipe::builder("sql:main", "pipeline-third")
            .instance("sql:main")
            .parameters(parameters)
            .build()
            .unwrap();
        assert!(instance.register_pipe(&pipe).await.success);
        if request == PipelineRequest::TwoParents {
            let seed = meerschaum_kernel::frame::Frame::new(vec![
                meerschaum_kernel::frame::Column::object(
                    "tick",
                    vec![meerschaum_kernel::value::Value::Int(5)],
                ),
            ])
            .unwrap();
            let synced = instance.sync_pipe(&pipe, seed, &Default::default()).await;
            assert!(synced.success, "{}", synced.message);
        }
        targets.push(pipe.pipe_ref());
        Some(pipe)
    } else {
        None
    };
    let api_name = "both-tick-streams";
    let definition = json!({"id":uuid::Uuid::new_v4().to_string(), "name":api_name,
        "registry":"sql:main", "revision":3, "schema_version":2,
        "graph":true, "targets":targets});
    let pipeline = Pipe::builder("mrsm", "pipeline")
        .location(Some(api_name))
        .instance("sql:main")
        .parameters(map(json!({"pipeline":definition})))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&pipeline).await.success);
    let reply = if request == PipelineRequest::Mcp {
        let message = tool(
            &client,
            &origin,
            &token,
            "sync_pipeline",
            json!({"name":api_name,"begin":5,"end":8}),
        )
        .await;
        assert!(tool_success(&message), "{message}");
        let result: Json =
            serde_json::from_str(message["result"]["content"][0]["text"].as_str().unwrap())
                .unwrap();
        assert!(result["run_id"].as_str().unwrap().contains("-api-"));
        result
    } else {
        let run_id = "saved-tick-streams-12";
        let (status, reply) = response(
            client
                .post(format!("{origin}/pipelines/{api_name}/sync"))
                .bearer_auth(&token)
                .json(&json!({"run_id":run_id,"begin":5,"end":8})),
        )
        .await;
        if request == PipelineRequest::Connected {
            assert!(status >= 400, "{reply}");
            assert!(
                reply.to_string().contains("entire parent window"),
                "{reply}"
            );
            assert!(!instance.pipe_exists(&first).await.unwrap());
            assert!(!instance.pipe_exists(&second).await.unwrap());
            return;
        }
        assert_eq!(
            status,
            200,
            "{reply} {}",
            file_tail(&root.path().join("main.log"))
        );
        assert_eq!(reply["run_id"], run_id);
        reply
    };
    let run_id = reply["run_id"].as_str().unwrap();
    let name = reply["job"].as_str().unwrap();
    use meerschaum_server::backends::{EngineBackend, RunsFilter};
    let history =
        meerschaum_server::engine_store::InstanceEngine::new(instance.clone(), "sql:main");
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let (status, job) = response(
            client
                .get(format!("{origin}/jobs/{name}"))
                .bearer_auth(&token),
        )
        .await;
        assert_eq!(status, 200, "{job}");
        if job["status"] == "stopped" && job["completion_pending"] != true {
            if job["result"][0] != true {
                let rows = history
                    .get_runs(&RunsFilter {
                        run_id: Some(run_id.into()),
                        ..Default::default()
                    })
                    .await
                    .unwrap_or_default();
                for row in rows {
                    eprintln!(
                        "Pipeline node {:?}: {} {:?} {:?}",
                        row.pipe_ref,
                        row.status,
                        row.extra.get("message"),
                        row.extra.get("reason")
                    );
                }
                let record = Catalog::new(root.path()).get(name).unwrap().unwrap();
                let journal = serde_json::to_value(&record.commit_journal).unwrap();
                if let Some(completed) = journal["completed_replacements"].as_object() {
                    for (name, value) in completed {
                        eprintln!(
                            "Pipeline completed source {name}: watermark {}",
                            value["source"]["watermark"]
                        );
                    }
                }
            }
            assert_eq!(
                job["result"][0],
                true,
                "{job} {} {}",
                file_tail(&root.path().join("main.log")),
                log_tails(root.path())
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{job} {}",
            log_tails(root.path())
        );
        tokio::time::sleep(Duration::from_millis(125)).await;
    }
    let record = Catalog::new(root.path()).get(name).unwrap().unwrap();
    assert_eq!(record.placement, JobPlacement::HostApi);
    assert_eq!(
        record.sysargs,
        [
            "sync",
            "pipelines",
            api_name,
            "-i",
            "sql:main",
            "--run-id",
            run_id,
            "--begin",
            "5",
            "--end",
            "8"
        ]
    );
    let (saved, mut roots) = record
        .operation
        .as_ref()
        .unwrap()
        .pipeline_bootstrap("sql:main", api_name, run_id)
        .unwrap();
    assert_eq!(saved["id"], definition["id"]);
    assert_eq!(saved["revision"], 3);
    roots.sort();
    targets.sort();
    assert_eq!(roots, targets);
    assert!(!record.commit_journal.has_pending());
    let journal = serde_json::to_value(&record.commit_journal).unwrap();
    if request.prepared_parent() {
        let evidence = &journal["completed_replacements"][first.pipe_ref()]["source"];
        assert_eq!(evidence["watermark"], 6, "{journal}");
        assert_eq!(evidence["rows"], 1);
        assert_eq!(evidence["snapshot_sha256"].as_str().unwrap().len(), 64);
        assert!(
            journal["completed_replacements"]
                .get(second.pipe_ref())
                .is_none()
        );
    }
    let expected_second = if request.dependent_children() {
        let instructions = record
            .operation
            .as_ref()
            .unwrap()
            .snapshot()
            .unwrap()
            .main_instructions()
            .unwrap();
        let child = instructions.planned_node(&second.pipe_ref()).unwrap();
        assert_eq!(child["action"], "incremental");
        assert_ne!(child["provisional"], true);
        assert_eq!(child["bounds"], json!({"begin":5,"end":7}));
        if request == PipelineRequest::PopulatedChild {
            assert_eq!(child["watermark"], 4);
            vec![4, 6]
        } else if request == PipelineRequest::TwoParents {
            vec![5, 6]
        } else {
            vec![6]
        }
    } else {
        vec![5, 7]
    };
    let mut expected_rows = vec![(&first, vec![6_i64]), (&second, expected_second)];
    if let Some(third) = &third {
        let instructions = record
            .operation
            .as_ref()
            .unwrap()
            .snapshot()
            .unwrap()
            .main_instructions()
            .unwrap();
        let node = instructions.planned_node(&third.pipe_ref()).unwrap();
        assert_ne!(node["provisional"], true);
        if request == PipelineRequest::TwoParents {
            assert_eq!(node["action"], "root");
            assert_eq!(
                journal["completed_replacements"][third.pipe_ref()]["source"]["watermark"],
                7
            );
            expected_rows.push((third, vec![5, 7]));
        } else {
            assert_eq!(node["bounds"], json!({"begin":5,"end":7}));
            expected_rows.push((third, vec![6]));
        }
    }
    for (pipe, expected) in expected_rows {
        let data = instance
            .get_pipe_data(pipe, &Default::default())
            .await
            .unwrap()
            .unwrap();
        let tick = &data.column("tick").unwrap().data;
        let mut actual: Vec<_> = (0..data.height())
            .map(|row| match tick.get(row) {
                meerschaum_kernel::value::Value::Int(value) => value,
                other => panic!("unexpected tick {other:?}"),
            })
            .collect();
        actual.sort_unstable();
        assert_eq!(actual, expected, "{}", pipe.pipe_ref());
        assert_eq!(
            journal["batches"]["nodes"][pipe.pipe_ref()]["retired_through"],
            1
        );
    }
    let records = history
        .get_runs(&RunsFilter {
            run_id: Some(run_id.into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let header = records
        .iter()
        .find(|row| row.pipe_ref.is_none())
        .expect("The saved run must publish a header under the requested run ID.");
    assert_eq!(header.run_id, run_id);
    assert_eq!(header.status, "success");
    let saved_selection = &header.extra["selection"];
    let selection: Json = match saved_selection {
        Value::Str(text) => serde_json::from_str(text).unwrap(),
        value => serde_json::to_value(value).unwrap(),
    };
    assert_eq!(selection["pipeline_definitions"][0]["id"], definition["id"]);
    assert_eq!(selection["pipeline_definitions"][0]["revision"], 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_sync_and_verify_filter_integer_station_rows() {
    use meerschaum_kernel::value::Value as Cell;
    use meerschaum_kernel::{Column, Frame};
    let root = main_root();
    let plugins = root.path().join("plugins");
    std::fs::create_dir(&plugins).unwrap();
    let database = root.path().join("main.db");
    let instance = meerschaum_connector_sql::SqlConnector::from_uri(
        "main",
        &format!("sqlite:///{}", database.display()),
    )
    .await
    .unwrap();
    let mut user = User::new("direct", Some("synthetic-scope-pass"));
    user.user_type = Some("regular".into());
    user.attributes = Some(map(
        json!({"role":"custom", "scopes":["actions:execute", "pipes:write"]}),
    ));
    assert!(instance.register_user(&mut user).await.success);
    let pipe = Pipe::builder("sql:main", "station-samples")
        .instance("sql:main")
        .parameters(map(json!({
            "columns":{"datetime":"tick","id":"station"},
            "dtypes":{"tick":"int64","station":"int64"},
            "target":"station_samples", "verify":{"chunk_range":2},
            "fetch":{"definition":"SELECT CAST(t AS INTEGER) AS tick, CAST(s AS INTEGER) AS station FROM (SELECT 1 AS t, 10 AS s UNION ALL SELECT 2, 10 UNION ALL SELECT 2, 20 UNION ALL SELECT 3, 10 UNION ALL SELECT 3, 20 UNION ALL SELECT 4, 20) AS samples"}
        }))).build().unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let seed = Frame::new(vec![
        Column::object("tick", vec![Cell::Int(0)]),
        Column::object("station", vec![Cell::Int(20)]),
    ])
    .unwrap();
    let seeded = instance
        .sync(
            &pipe,
            meerschaum_kernel::enforce::PipeInput::Frame(seed),
            &Default::default(),
        )
        .await;
    assert!(seeded.success, "{}", seeded.message);
    let (_main, client, origin) = start_main(root.path(), &plugins, &database).await;
    let token = login(&client, &origin, "direct").await;
    for (action, station, expected) in [
        ("sync", 10, vec![(0, 20), (2, 10), (3, 10)]),
        (
            "verify",
            20,
            vec![(0, 20), (2, 10), (2, 20), (3, 10), (3, 20), (4, 20)],
        ),
    ] {
        let (status, body) = response(client.post(format!("{origin}/actions/{action}")).bearer_auth(&token).json(&json!({
            "action":["pipes"], "connector_keys":["sql:main"], "metric_keys":["station-samples"], "location_keys":["None"], "mrsm_instance":"sql:main",
            "begin":2,"end":5, "params":{"station":station}
        }))).await;
        assert_eq!(status, 200, "{body}");
        let data = instance
            .get_pipe_data(&pipe, &Default::default())
            .await
            .unwrap()
            .unwrap();
        let mut actual = Vec::new();
        for row in 0..data.height() {
            let (Cell::Int(tick), Cell::Int(station)) = (
                data.column("tick").unwrap().data.get(row),
                data.column("station").unwrap().data.get(row),
            ) else {
                panic!("Expected canonical integer sample columns");
            };
            actual.push((tick, station));
        }
        actual.sort_unstable();
        let records = Catalog::new(root.path()).records().unwrap();
        let record = records
            .values()
            .find(|record| {
                record
                    .operation
                    .as_ref()
                    .and_then(|operation| operation.endpoint())
                    .is_some_and(|endpoint| endpoint.verify == (action == "verify"))
            })
            .unwrap();
        let journal = serde_json::to_value(&record.commit_journal).unwrap();
        let stream = &journal["batches"]["nodes"][pipe.pipe_ref()];
        assert_eq!(
            body.get(0),
            Some(&json!(true)),
            "{action}: {body}; rows {actual:?}; audit {}; retired through {}; active phase {}; pending {}. Main log: {}{}",
            journal["audit"],
            stream["retired_through"],
            stream["active"]["phase"],
            record.commit_journal.has_pending(),
            file_tail(&root.path().join("main.log")),
            log_tails(root.path())
        );
        assert_eq!(actual, expected, "{action}");
        assert!(!record.commit_journal.has_pending());
        assert_eq!(
            journal["audit"]["affected_rows"],
            if action == "verify" { 3 } else { 2 }
        );
    }
}
