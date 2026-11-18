//! The native copy kernel uses authenticated logical aliases for both source and destination.
//! This does not widen the main API's public command-admission policy.
#![cfg(target_os = "linux")]

#[path = "../../meerschaum-server/tests/common/mod.rs"]
mod common;

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::HeaderMap;
use meerschaum_cli::executor_profiles::{Inputs, Prepared, prepare};
use meerschaum_connector::{Connector, InstanceConnector, SyncOptions};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::team_scope::write_private;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_server::backends::InstanceResolver;
use meerschaum_server::executor::callbacks::{self, Purpose};
use serde_json::json;
use uuid::Uuid;

fn map(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

struct Instances(Vec<Arc<SqlConnector>>);
impl InstanceResolver for Instances {
    fn resolve(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        self.0
            .iter()
            .find(|instance| instance.connector_keys() == keys)
            .map(|instance| instance.clone() as Arc<dyn InstanceConnector>)
    }
}

struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn command(prepared: &Prepared, arguments: &[&str], probe: bool) -> Output {
    let environment = prepared.environment.clone();
    let arguments: Vec<String> = arguments.iter().map(|word| word.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .envs(environment)
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if probe {
            command.env(meerschaum_cli::team_execution::PROBE_ENV, "1");
        }
        let mut child = command.spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match child.try_wait().unwrap() {
                Some(_) => return child.wait_with_output().unwrap(),
                None if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
                None => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("owned logical alias copy did not finish within 20 seconds");
                }
            }
        }
    })
    .await
    .unwrap()
}

fn pipe(instance: &SqlConnector, metric: &str) -> Pipe {
    Pipe::builder("manual:posted", metric)
        .instance(&instance.keys())
        .build()
        .unwrap()
}

async fn seed(instance: &SqlConnector, metric: &str, owner: &str, value: i64) {
    let mut p = pipe(instance, metric);
    p.set_parameters(map(json!({"columns":{"id":"id"}, "access":{
        "owner":owner, "team":"Data", "visibility":"private", "edit":"owner"
    }})));
    assert!(instance.register_pipe(&p).await.success);
    let frame = meerschaum_kernel::docs::frame_from_docs(&[[(
        "id".into(),
        meerschaum_kernel::value::Value::Int(value),
    )]
    .into_iter()
    .collect()])
    .unwrap();
    let result = instance.sync_pipe(&p, frame, &SyncOptions::default()).await;
    assert!(result.success, "{}", result.message);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_copy_checks_hidden_source_private_destination_and_stamps_new_owner() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let main_root = root.join("main");
    let runtime_root = root.join("executor");
    std::fs::create_dir_all(&main_root).unwrap();
    std::fs::create_dir_all(&runtime_root).unwrap();
    let source = Arc::new(
        SqlConnector::from_uri(
            "source",
            &format!("sqlite:///{}", main_root.join("owned-source.db").display()),
        )
        .await
        .unwrap(),
    );
    let target = Arc::new(
        SqlConnector::from_uri(
            "target",
            &format!("sqlite:///{}", main_root.join("owned-target.db").display()),
        )
        .await
        .unwrap(),
    );
    seed(&source, "copy_hidden", "bob", 7).await;
    seed(&source, "copy_private_destination", "alice", 3).await;
    seed(&source, "copy_allowed", "alice", 5).await;
    seed(&target, "copy_private_destination", "bob", 9).await;
    let auth = Arc::new(common::MemoryInstance::new("fixture"));
    for name in ["alice", "bob"] {
        auth.seed_user(name, "pass", None);
        auth.seed_user_attributes(name, map(json!({"role":"operator", "primary_team":"Data"})));
    }
    let mut team = Team::new("Data");
    team.members.insert("alice".into(), TeamRole::Member);
    team.members.insert("bob".into(), TeamRole::Member);
    assert!(auth.register_team(&mut team).await.success);
    let mut config = common::config(&main_root);
    config.allow_multiple_instances = true;
    config.allowed_instance_keys = vec!["sql:*".into()];
    config.connectors = map(json!({"sql":{
        "source":{"password":"synthetic-main-source-secret"},
        "target":{"password":"synthetic-main-target-secret"}
    }}));
    let state = meerschaum_server::AppState::builder(config, auth)
        .resolver(Arc::new(Instances(vec![source.clone(), target.clone()])))
        .secret(common::SECRET.to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state.clone());
    let source_auth = common::login(&app, "alice", "pass", None).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", source_auth.parse().unwrap());
    let caller = meerschaum_server::access::resolve(&state, &headers)
        .await
        .unwrap();
    let grant = callbacks::register(
        &state,
        &caller,
        Uuid::new_v4(),
        Some(Uuid::parse_str(team.id.as_deref().unwrap()).unwrap()),
        &["jobs:execute"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let _server = Server(tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    }));
    let directory = runtime_root.join("execution-contexts/copy");
    let native = std::fs::canonicalize(env!("CARGO_BIN_EXE_mrsm-cli")).unwrap();
    let prepared = prepare(&Inputs {
        metadata: false,
        server_root: &runtime_root,
        directory: &directory,
        base: &Map::new(),
        logical_instances: &BTreeSet::from(["sql:source".into(), "sql:target".into()]),
        default_instance: "sql:source",
        callback_url: &origin,
        read_exec: &[native],
        job_dir: None,
        environment: &[("PATH".into(), "/usr/bin:/bin".into())],
    })
    .unwrap();
    let configuration = std::fs::read_to_string(prepared.directory.join("config.json")).unwrap();
    for secret in [
        "synthetic-main-source-secret",
        "synthetic-main-target-secret",
        "owned-source.db",
        "owned-target.db",
    ] {
        assert!(!configuration.contains(secret));
    }
    let credential = callbacks::credential(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    write_private(
        &prepared.credential_file,
        &serde_json::to_string(&credential).unwrap(),
    )
    .unwrap();
    let probe = command(&prepared, &[], true).await;
    let stdout = String::from_utf8_lossy(&probe.stdout);
    let proof: meerschaum_server::team_execution::Probe = stdout
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str(line).ok())
        .unwrap_or_else(|| {
            panic!(
                "missing copy sandbox report: {stdout}\n{}",
                String::from_utf8_lossy(&probe.stderr)
            )
        });
    if let Err(reason) = proof.sandbox.require_sockets_verdict() {
        eprintln!("Actual logical alias copy needs the required kernel boundary: {reason}");
        return;
    }
    for metric in ["copy_hidden", "copy_private_destination", "copy_allowed"] {
        let credential = callbacks::credential(&state, grant.id, grant.launch_id)
            .await
            .unwrap();
        write_private(
            &prepared.credential_file,
            &serde_json::to_string(&credential).unwrap(),
        )
        .unwrap();
        let result = command(
            &prepared,
            &[
                "copy",
                "pipes",
                "sql:target",
                "-i",
                "sql:source",
                "-c",
                "manual:posted",
                "-m",
                metric,
                "--sync-data",
                "all",
                "-f",
                "--nopretty",
            ],
            false,
        )
        .await;
        let output = format!(
            "{}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        if metric == "copy_allowed" {
            assert!(result.status.success(), "{output}");
            let attributes = target
                .get_pipe_attributes(&pipe(&target, metric))
                .await
                .unwrap();
            let attributes = serde_json::to_value(attributes).unwrap();
            assert_eq!(
                attributes["parameters"]["access"]["owner"], "alice",
                "{attributes}"
            );
            assert_eq!(
                attributes["parameters"]["access"]["team"], "Data",
                "{attributes}"
            );
            let frame = target
                .get_pipe_data(&pipe(&target, metric), &Default::default())
                .await
                .unwrap()
                .unwrap();
            let rows = meerschaum_connector::pipe_ops::frame_to_docs(&frame);
            assert_eq!(rows.len(), 1, "{output}");
            assert_eq!(rows[0]["id"], meerschaum_kernel::value::Value::Int(5));
        } else if metric == "copy_hidden" {
            assert!(
                target
                    .get_pipe_id(&pipe(&target, metric))
                    .await
                    .unwrap()
                    .is_none(),
                "{output}"
            );
        } else {
            assert!(
                !result.status.success(),
                "a private destination must refuse mutation: {output}"
            );
            let frame = target
                .get_pipe_data(&pipe(&target, metric), &Default::default())
                .await
                .unwrap()
                .unwrap();
            let rows = meerschaum_connector::pipe_ops::frame_to_docs(&frame);
            assert_eq!(rows.len(), 1, "{output}");
            assert_eq!(rows[0]["id"], meerschaum_kernel::value::Value::Int(9));
            let attributes = serde_json::to_value(
                target
                    .get_pipe_attributes(&pipe(&target, metric))
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(attributes["parameters"]["access"]["owner"], "bob");
        }
    }
    callbacks::revoke(&state, grant.id).await.unwrap();
    source.driver().close().await;
    target.driver().close().await;
}
