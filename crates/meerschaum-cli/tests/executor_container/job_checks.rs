//! Login, job starts and the checks of a job's committed rows, result publication and runtime.

use super::docker::Docker;
use super::*;

pub(super) fn file_tail(path: &Path) -> String {
    common::fs::file_tail(path, 16 * 1024)
}

pub(super) async fn assert_committed_container_row(instance: &SqlConnector, metric: &str) {
    let pipe = Pipe::builder("plugin:container_probe", metric)
        .instance("sql:main")
        .build()
        .unwrap();
    let target = meerschaum_connector_sql::ident::sql_item_name(
        &pipe.target(Some("sqlite")).unwrap(),
        meerschaum_connector_sql::Flavor::Sqlite,
        None,
    );
    let storage = instance.read(&format!(
        "SELECT typeof(payload) AS storage_type, hex(payload) AS stored_hex FROM {target} WHERE id = 2"
    )).await.unwrap();
    let storage = meerschaum_connector::pipe_ops::frame_to_docs(&storage);
    let [stored] = storage.as_slice() else {
        panic!("Expected one stored executor row for {metric}: {storage:?}")
    };
    let declared = instance.get_pipe_columns_types(&pipe).await.unwrap();
    assert_eq!(
        stored["storage_type"],
        KValue::Str("blob".into()),
        "SQLite storage for {metric}: {stored:?}, {declared:?}"
    );
    assert_eq!(
        stored["stored_hex"],
        KValue::Str("636F6E7461696E65722D00FF2D6E61746976652D7772697465".into()),
        "SQLite bytes for {metric}: {stored:?}, {declared:?}"
    );
    assert_eq!(
        declared
            .get("payload")
            .map(|value| value.to_ascii_uppercase()),
        Some("BLOB".into()),
        "SQLite column type for {metric}: {declared:?}"
    );
    let frame = InstanceConnector::get_pipe_data(instance, &pipe, &Default::default())
        .await
        .unwrap()
        .unwrap();
    let rows = meerschaum_connector::pipe_ops::frame_to_docs(&frame);
    assert_eq!(
        rows.len(),
        2,
        "the actual container write was lost or duplicated for {metric}"
    );
    assert!(rows.iter().any(|row| row["id"] == KValue::Int(1)));
    let row = rows.iter().find(|row| row["id"] == KValue::Int(2)).unwrap();
    assert_eq!(
        row["payload"],
        KValue::Bytes(b"container-\0\xff-native-write".to_vec()),
        "the actual executor write changed the binary payload for {metric}"
    );
}

pub(super) async fn login(client: &reqwest::Client, origin: &str, username: &str) -> String {
    common::http::login(client, origin, username, "synthetic-container-user").await
}
pub(super) async fn healthy(client: &reqwest::Client, origin: &str) {
    assert!(
        common::http::wait_healthy(client, origin, Duration::from_secs(60)).await,
        "owned Main did not become ready"
    );
}
pub(super) async fn start_job(
    client: &reqwest::Client,
    origin: &str,
    token: &str,
    name: &str,
    alias: &str,
    metric: &str,
) {
    let (status, result) = response(client.post(format!("{origin}/jobs/{name}")).bearer_auth(token)
        .json(&json!({"sysargs":["sync","pipes","-i",alias,"-c","plugin:container_probe","-m",metric],"properties":{}}))).await;
    assert_eq!(status, 200, "create {name}: {result}");
    assert_eq!(result[0], true, "create {name}: {result}");
}
pub(super) async fn assert_result_publication(
    docker: &Docker,
    service: &str,
    client: &reqwest::Client,
    origin: &str,
    token: &str,
) {
    let name = "api-result-publication";
    start_job(client, origin, token, name, "api:api_writer", "api_hold").await;
    wait_for_held_worker(client, origin, token, name).await;

    let catalog = Catalog::new(&docker.main);
    let record = catalog.get(name).unwrap().unwrap();
    let owned = docker.inspect(service);
    assert_eq!(owned["Config"]["Labels"]["mrsm.test.owner"], docker.project);
    assert_eq!(
        owned["Config"]["Labels"]["com.docker.compose.project"],
        docker.project
    );
    assert_eq!(
        owned["Config"]["Labels"]["com.docker.compose.service"],
        service
    );
    let id = owned["Id"].as_str().unwrap();
    let revision = record.context_revision.to_string();
    let fixture = |mode: &str, args: &[&str]| {
        let mut command = vec![
            "exec",
            id,
            "python3",
            "-P",
            "-c",
            include_str!("../fixtures/executor_container_finalization.py"),
            mode,
            FACTORY_CONTROL_ROOT,
            &record.remote_name,
            &revision,
        ];
        command.extend_from_slice(args);
        docker.command(&command)
    };

    let paused = fixture("pause", &[]);
    let request = || {
        client
            .get(format!("{origin}/jobs/{name}"))
            .bearer_auth(token)
    };
    let (status, pending) = response(request()).await;
    let observed = catalog.get(name).unwrap().unwrap();
    fixture("resume", &[&paused]);

    assert_eq!(status, 200, "{pending}");
    assert_eq!(pending["status"], "stopped", "{pending}");
    assert_eq!(pending["completion_pending"], true, "{pending}");
    assert_eq!(pending["completion_unavailable"], false, "{pending}");
    assert_eq!(pending["result"], Json::Null, "{pending}");
    assert!(observed.completed_at.is_none());
    assert!(!observed.callback_suspended);

    let deadline = Instant::now() + Duration::from_secs(30);
    let completed = loop {
        let (status, result) = response(request()).await;
        assert_eq!(status, 200, "{result}");
        assert_ne!(result["completion_unavailable"], true, "{result}");
        if result["completion_pending"] == false {
            break result;
        }
        assert!(
            Instant::now() < deadline,
            "supervisor did not publish: {result}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let disk: Json = serde_json::from_str(&fixture("result", &[])).unwrap();
    assert!(!disk["ended"].as_str().unwrap().is_empty(), "{disk}");
    assert_eq!(completed["status"], "stopped", "{completed}");
    assert_eq!(completed["result"], disk["result"]);
    assert_eq!(
        completed["result"],
        json!([
            false,
            meerschaum_jobs::supervisor::signal_message(&record.remote_name, 9)
        ])
    );
}

pub(super) async fn wait_for_held_worker(
    client: &reqwest::Client,
    origin: &str,
    token: &str,
    name: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(75);
    loop {
        let (status, logs) = response(
            client
                .get(format!("{origin}/logs/{name}"))
                .bearer_auth(token),
        )
        .await;
        if status == 200
            && logs
                .as_str()
                .is_some_and(|logs| logs.contains("MRSM_CONTAINER_PROOF:"))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "held API job did not produce callback proof: {status} {logs}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub(super) async fn completed(
    client: &reqwest::Client,
    origin: &str,
    token: &str,
    name: &str,
    profile: &str,
) -> Json {
    let deadline = Instant::now() + Duration::from_secs(150);
    loop {
        let (status, result) = response(
            client
                .get(format!("{origin}/jobs/{name}"))
                .bearer_auth(token),
        )
        .await;
        assert_eq!(status, 200, "{name}: {result}");
        assert_eq!(result["executor"]["placement"], "remote", "{result}");
        assert_eq!(result["executor"]["profile"], profile, "{result}");
        if result["status"] == "stopped" && result["completion_pending"] != true {
            let (status, logs) = response(
                client
                    .get(format!("{origin}/logs/{name}"))
                    .bearer_auth(token),
            )
            .await;
            assert_eq!(status, 200, "{logs}");
            assert_eq!(result["result"][0], true, "{name}: {result}\n{logs}");
            let logs = logs.as_str().unwrap();
            let (_, proof) = logs
                .rsplit_once("MRSM_CONTAINER_PROOF:")
                .unwrap_or_else(|| panic!("Missing real job proof: {logs}"));
            return serde_json::from_str(proof.lines().next().unwrap().trim()).unwrap();
        }
        if Instant::now() >= deadline {
            let live = tokio::time::timeout(
                Duration::from_secs(5),
                response(
                    client
                        .get(format!("{origin}/logs/{name}"))
                        .bearer_auth(token),
                ),
            )
            .await;
            let tail = live
                .ok()
                .and_then(|(_, value)| {
                    value.as_str().map(|text| {
                        text.chars()
                            .rev()
                            .take(4096)
                            .collect::<Vec<_>>()
                            .into_iter()
                            .rev()
                            .collect::<String>()
                    })
                })
                .unwrap_or_default();
            panic!("{name} did not finish: {result}\n{tail}");
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}
pub(super) fn assert_runtime(
    docker: &Docker,
    service: &str,
    team: &Team,
    (memory, cpus, pids): (u64, f64, u32),
    administrator: &Path,
    provisioned: bool,
) -> Json {
    let observed = docker.inspect(service);
    let config = &observed["Config"];
    let host = &observed["HostConfig"];
    assert_eq!(config["User"], "1000:1000");
    assert_eq!(host["ReadonlyRootfs"], true);
    assert_eq!(observed["State"]["Running"], true);
    assert_eq!(observed["RestartCount"], 0);
    let run_options = host["Tmpfs"]["/run"].as_str().expect("Factory /run tmpfs");
    for option in ["uid=0", "gid=0"] {
        assert!(
            run_options.split(',').any(|actual| actual == option),
            "Unprotected Factory /run: {run_options}"
        );
    }
    let mode = run_options
        .split(',')
        .find_map(|option| option.strip_prefix("mode="))
        .and_then(|mode| u32::from_str_radix(mode, 8).ok());
    assert_eq!(mode, Some(0o755), "Unprotected Factory /run: {run_options}");
    assert_eq!(config["WorkingDir"], FACTORY_CONTROL_ROOT);
    let metadata = docker.exec(
        service,
        &format!(
            r#"import os
import stat
from pathlib import Path

control = Path({FACTORY_CONTROL_ROOT:?})
assert (os.getuid(), os.getgid()) == (1000, 1000)
for path, uid, gid, mode in [(control.parent, 0, 0, 0o755), (control, 1000, 1000, 0o700)]:
    info = path.lstat()
    assert stat.S_ISDIR(info.st_mode)
    assert (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) == (uid, gid, mode), path
key = Path({PRIVATE_PATH:?})
if key.exists():
    info = key.lstat()
    assert stat.S_ISREG(info.st_mode) and info.st_uid == 1000 and info.st_nlink == 1
    assert stat.S_IMODE(info.st_mode) == 0o600
print(int(key.exists()))"#,
        ),
    );
    assert_eq!(metadata, if provisioned { "1" } else { "0" });
    assert_eq!(host["Privileged"], false);
    assert!(host["CapAdd"].is_null() || host["CapAdd"].as_array().is_some_and(Vec::is_empty));
    assert_ne!(host["NetworkMode"], "host");
    assert!(host["PidMode"].as_str().is_none_or(str::is_empty));
    assert_eq!(host["Init"], true);
    let environment = config["Env"].to_string();
    assert!(!environment.contains("synthetic-main-private"));
    assert!(!environment.contains("synthetic-main-environment-private"));
    assert_eq!(host["CapDrop"], json!(["ALL"]));
    assert!(host["SecurityOpt"].as_array().unwrap().iter().any(|value| {
        value
            .as_str()
            .is_some_and(|value| value.starts_with("no-new-privileges"))
    }));
    assert_eq!(host["IpcMode"], "private");
    assert_eq!(host["Memory"], memory * 1024 * 1024);
    assert_eq!(host["MemorySwap"], memory * 1024 * 1024);
    assert_eq!(host["NanoCpus"], (cpus * 1_000_000_000.) as u64);
    assert_eq!(host["PidsLimit"], pids);
    // Check the limits the Linux kernel installed in each generated executor.
    let limits = docker.exec(
        service,
        r#"from pathlib import Path
root = Path('/sys/fs/cgroup')
groups = [line.split(':', 2) for line in Path('/proc/self/cgroup').read_text().splitlines()]
def limit(subsystem, legacy_file):
    for _, controllers, path in groups:
        if not controllers:
            base, name = root, subsystem + '.max'
        elif subsystem in controllers.split(','):
            base, name = root / subsystem, legacy_file
        else:
            continue
        for current in (base / path.lstrip('/'), base):
            counter = current / name
            if counter.is_file():
                return int(counter.read_text())
    raise RuntimeError('missing ' + subsystem + ' controller')
print(limit('memory', 'memory.limit_in_bytes'), limit('pids', 'pids.max'))"#,
    );
    assert_eq!(limits, format!("{} {pids}", memory * 1024 * 1024));
    assert!(
        host["PortBindings"].is_null()
            || host["PortBindings"]
                .as_object()
                .is_some_and(|bindings| bindings.is_empty())
    );
    assert_eq!(
        config["Labels"]["mrsm.executor.team"],
        team.id.as_deref().unwrap()
    );
    assert_eq!(config["Labels"]["mrsm.test.owner"], docker.project);
    assert_eq!(
        observed["NetworkSettings"]["Networks"]
            .as_object()
            .unwrap()
            .len(),
        1
    );
    let mounts = observed["Mounts"].as_array().unwrap();
    let mount_at = |destination: &str| {
        mounts
            .iter()
            .find(|mount| mount["Destination"] == destination)
    };
    for (destination, suffix) in [("/meerschaum", "root"), (FACTORY_CONTROL_ROOT, "control")] {
        let mount = mount_at(destination).expect("missing generated Team volume");
        assert_eq!(mount["Type"], "volume");
        assert_eq!(mount["RW"], true);
        assert_eq!(
            mount["Name"],
            format!("{}_{service}-{suffix}", docker.project)
        );
    }
    let verifier = mount_at("/run/secrets/mrsm-executor.json").expect("missing verifier mount");
    let team_key = service.strip_prefix("executor-").unwrap();
    let desired = administrator.join("executors/desired");
    assert_eq!(
        Path::new(verifier["Source"].as_str().unwrap()),
        desired.join(format!("{team_key}.json"))
    );
    let generation = uuid::Uuid::parse_str(
        config["Labels"]["mrsm.executor.generation"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let key = mount_at(PRIVATE_PATH);
    assert_eq!(key.is_some(), provisioned);
    if let Some(key) = key {
        let private_file = administrator
            .join("executors/attestation/private")
            .join(generation.simple().to_string())
            .join("factory.json");
        assert_eq!(Path::new(key["Source"].as_str().unwrap()), private_file);
    }
    for mount in mounts {
        let destination = mount["Destination"].as_str().unwrap();
        match destination {
            "/meerschaum" | FACTORY_CONTROL_ROOT => {}
            "/tmp" | "/run" => assert_eq!(mount["Type"], "tmpfs"),
            "/run/secrets/mrsm-executor.json"
            | PRIVATE_PATH
            | "/etc/pki/ca-trust/source/anchors/mrsm-fixture.pem" => {
                assert_eq!(mount["Type"], "bind");
                assert_eq!(mount["RW"], false);
            }
            _ => panic!("Unexpected Factory container mount: {destination}"),
        }
    }
    assert_eq!(host["Sysctls"]["net.ipv6.conf.all.disable_ipv6"], "1");
    assert_eq!(host["Sysctls"]["net.ipv6.conf.default.disable_ipv6"], "1");
    observed
}
