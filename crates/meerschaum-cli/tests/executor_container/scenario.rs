//! One generated-container scenario end to end, and the plugin archives it publishes.

use super::docker::{Docker, SelfCommand, digest_variable};
use super::job_checks::{
    assert_committed_container_row, assert_result_publication, assert_runtime, completed, healthy,
    login, start_job, wait_for_held_worker,
};
use super::*;

fn publish_archive(directory: &Path, plugin: &str, filename: &str, source: &str) {
    common::fs::write_plugin_archive(directory, plugin, filename, source);
}

pub(super) async fn run_container(scenario: Scenario) {
    let image = std::env::var("MRSM_TEST_EXECUTOR_IMAGE")
        .expect("provide a loaded native+SDK candidate image");
    let native_sha = digest_variable("MRSM_TEST_EXECUTOR_NATIVE_SHA256");
    let sdk_sha = digest_variable("MRSM_TEST_EXECUTOR_SDK_SHA256");
    let parent = std::env::var_os("MRSM_ROOT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    std::fs::create_dir_all(&parent).unwrap();
    let root = tempfile::Builder::new()
        .prefix("tpr-container-")
        .tempdir_in(&parent)
        .unwrap();
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            std::fs::metadata(root.path()).unwrap().uid(),
            1000,
            "run this owned bind-state acceptance as UID1000, matching its generated services"
        );
    }
    let main = root.path().join("main");
    std::fs::create_dir_all(main.join("plugins")).unwrap();
    std::fs::write(main.join("main-private.txt"), "synthetic-main-file-private").unwrap();
    let source = if scenario == Scenario::Lifecycle {
        include_str!("../fixtures/executor_container_probe.py")
    } else if scenario == Scenario::Recovery {
        r#"def fetch(pipe, **kw):
    import json
    import socket
    from pathlib import Path
    hostname = socket.gethostname()
    original = []
    for subdir in ("data", "venvs"):
        marker = Path("/meerschaum", subdir, "normal-recovery-origin")
        marker.parent.mkdir(parents=True, exist_ok=True)
        if not marker.exists():
            marker.write_text(hostname)
        original.append(marker.read_text())
    print("MRSM_CONTAINER_PROOF:" + json.dumps({"hostname": hostname, "original": original}), flush=True)
    yield [{"id": 2, "payload": b"container-\x00\xff-native-write"}]
"#
    } else {
        r#"def fetch(pipe, **kw):
    import json
    import socket
    proof = {"hostname": socket.gethostname()}
    if pipe.parameters.get("probe_kind") == "code":
        proof["initial_rows"] = pipe.get_rowcount()
    print("MRSM_CONTAINER_PROOF:" + json.dumps(proof), flush=True)
    yield [{"id": 2, "payload": b"container-\x00\xff-native-write"}]
"#
    };
    std::fs::write(main.join("plugins/container_probe.py"), source).unwrap();
    let archive_path =
        meerschaum_server::ServerConfig::for_root("sql:main", &main).plugins_archives_path;
    assert!(archive_path.starts_with(&main));
    std::fs::create_dir_all(&archive_path).unwrap();
    publish_archive(
        &archive_path,
        "container_probe",
        "container_probe.py",
        source,
    );
    if scenario == Scenario::NativeMetadata {
        let source = r#"#[mcp_resource(uri = "mrsm://release-fixture/greeting", description = "A greeting.", mime_type = "text/plain", scopes = ["pipes:read"])]
fn greeting() -> ResourceContents {
    ResourceContents::Text {text: "Hello".into(), mime_type: Some("text/plain".into())}
}"#;
        publish_archive(
            &archive_path,
            "native_greeting",
            "native_greeting.rs",
            source,
        );
    }
    let instance = Arc::new(
        SqlConnector::from_uri(
            "main",
            &format!("sqlite:///{}", main.join("main.db").display()),
        )
        .await
        .unwrap(),
    );
    for (name, role) in [
        ("alice", "operator"),
        ("bob", "operator"),
        ("svc_api", "viewer"),
        ("svc_code", "viewer"),
        ("svc_api_writer", "viewer"),
        ("svc_code_writer", "viewer"),
    ] {
        let mut user = User::new(
            name,
            Some(if name.starts_with("svc_") {
                "synthetic-container-service"
            } else {
                "synthetic-container-user"
            }),
        );
        let attributes = map(json!({"role":role}));
        user.attributes = Some(if name.starts_with("svc_") {
            let permissions: &[&str] = if name.ends_with("_writer") {
                &["pipes:read", "pipes:write"]
            } else {
                &["pipes:read"]
            };
            meerschaum_core::access::attributes_with_permissions(&attributes, permissions)
        } else {
            attributes
        });
        assert!(instance.register_user(&mut user).await.success);
        if name.starts_with("svc_") {
            let stored = instance.get_user_attributes(&user).await.unwrap().unwrap();
            assert_eq!(
                meerschaum_core::access::user_permissions(
                    None,
                    Some(&stored),
                    meerschaum_core::access::Role::Editor
                ),
                if name.ends_with("_writer") {
                    std::collections::BTreeSet::from([
                        "pipes:read".to_string(),
                        "pipes:write".to_string(),
                    ])
                } else {
                    std::collections::BTreeSet::from(["pipes:read".to_string()])
                }
            );
        }
    }
    let mut teams = Vec::new();
    for (name, username, alias, code, memory, cpus, pids) in [
        (
            "ApiFixture",
            "alice",
            "api:api_writer",
            false,
            1024,
            1.,
            256,
        ),
        (
            "CodeFixture",
            "bob",
            "api:code_writer",
            true,
            1536,
            1.5,
            384,
        ),
    ] {
        let mut team = Team::new(name);
        team.add_member(username, TeamRole::Member);
        team.attributes = map(
            json!({"isolated":true,"plugins_write":code,"instance":alias,"connectors":[alias, if code {"api:code_data"} else {"api:api_data"}],"plugins":if code && scenario == Scenario::NativeMetadata { vec!["native_greeting"] } else { vec!["container_probe"] },
            "execution":{"mode":"container","egress":"proxy","limits":{"memory_mb":memory,"cpus":cpus,"pids":pids},
                "relays":if code { json!([{"host":"fixturetls","port":8443}]) } else { json!([]) }}}),
        );
        assert!(instance.register_team(&mut team).await.success);
        assert!(team.is_isolated());
        assert!(uuid::Uuid::parse_str(team.id.as_deref().unwrap()).is_ok());
        teams.push(team);
    }
    if scenario == Scenario::Terminal(Placement::Host) {
        let mut user = User::new("host_operator", Some("synthetic-container-user"));
        user.attributes = Some(map(json!({"role":"operator"})));
        assert!(instance.register_user(&mut user).await.success);
        let mut team = Team::new("HostFixture");
        team.add_member("host_operator", TeamRole::Member);
        team.attributes = map(
            json!({"isolated":true,"plugins_write":false,"instance":"sql:main",
            "connectors":["sql:main"],"plugins":[],"execution":{"mode":"host"}}),
        );
        assert!(instance.register_team(&mut team).await.success);
    }
    let registry = Registry::new(&main);
    let desired = registry.reconcile(&teams).unwrap();
    // Main owns its request keys and runtime data. The administrator receives the
    // generated public deployment, then mounts that directory back into Main.
    let administrator = root.path().join("administrator");
    let public_desired = Registry::new(&administrator).desired_dir();
    for dir in [public_desired.parent().unwrap(), &public_desired] {
        meerschaum_core::team_scope::create_private_dir(dir).unwrap();
    }
    for entry in std::fs::read_dir(registry.desired_dir()).unwrap() {
        let entry = entry.unwrap();
        assert!(entry.file_type().unwrap().is_file());
        std::fs::copy(entry.path(), public_desired.join(entry.file_name())).unwrap();
    }
    let provisioned = std::env::var("MRSM_TEST_EXECUTOR_PROVISION_FACTORY").as_deref() == Ok("1");
    if matches!(
        scenario,
        Scenario::InstalledProfile(_) | Scenario::NativeMetadata | Scenario::Terminal(_)
    ) {
        assert!(
            provisioned,
            "the native resource fixture requires Factory provisioning"
        );
    }
    let services = teams
        .iter()
        .map(|team| {
            format!(
                "executor-team-{}",
                uuid::Uuid::parse_str(team.id.as_deref().unwrap())
                    .unwrap()
                    .simple()
            )
        })
        .collect::<Vec<_>>();
    for (index, metric, kind, service_owner, hold) in [
        (0, "api_resource", "api", "svc_api_writer", 0),
        (1, "code_resource", "code", "svc_code_writer", 0),
        (0, "api_hold", "api", "svc_api_writer", 180),
        (0, "api_readonly", "api", "svc_api", 0),
        (1, "code_readonly", "code", "svc_code", 0),
    ] {
        let alias = if index == 0 {
            "api:api_writer"
        } else {
            "api:code_writer"
        };
        let other = if index == 0 {
            "api:code_writer"
        } else {
            "api:api_writer"
        };
        let pipe = Pipe::builder("plugin:container_probe", metric).instance("sql:main")
            .parameters(map(json!({"columns":{"id":"id"},"probe_kind":kind,"expected_instance":alias,"other_instance":other,
                "other_executor":services[1-index],"network_probe":kind=="code","hold_seconds":hold,
                "tls_host":"fixturetls","tls_port":8443,"tls_ca":"/etc/pki/ca-trust/source/anchors/mrsm-fixture.pem",
                "public_tls_probe":std::env::var("MRSM_TEST_EXECUTOR_PUBLIC_TLS").as_deref()==Ok("1"),
                "access":{"owner":service_owner,"team":teams[index].name,"visibility":"team"}})))
            .build().unwrap();
        assert!(instance.register_pipe(&pipe).await.success);
        let data = meerschaum_kernel::docs::frame_from_docs(&[[("id".into(), KValue::Int(1))]
            .into_iter()
            .collect()])
        .unwrap();
        assert!(
            instance
                .sync_pipe(&pipe, data, &Default::default())
                .await
                .success
        );
    }
    let project = format!(
        "tpr-executor-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let compose_file = root.path().join("compose.json");
    let docker = Docker {
        project: project.clone(),
        compose: compose_file.clone(),
        main: main.clone(),
    };
    assert!(
        docker
            .command(&[
                "context",
                "inspect",
                "default",
                "--format",
                "{{.Endpoints.docker.Host}}"
            ])
            .starts_with("unix://")
    );
    assert!(
        docker
            .command(&["version", "--format", "{{.Server.Version}}"])
            .split('.')
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            >= 28
    );
    let image_info: Json =
        serde_json::from_str::<Json>(&docker.command(&["image", "inspect", &image])).unwrap()[0]
            .clone();
    let pinned_image = image_info["Id"].as_str().unwrap().to_string();
    assert!(pinned_image.starts_with("sha256:"));
    let tls = root.path().join("tls");
    std::fs::create_dir(&tls).unwrap();
    let ca_key = tls.join("ca.key");
    let ca = tls.join("ca.pem");
    let server_key = tls.join("server.key");
    let request = tls.join("server.csr");
    let certificate = tls.join("server.pem");
    let extensions = tls.join("server.ext");
    std::fs::write(&extensions, "subjectAltName=DNS:fixturetls\nbasicConstraints=critical,CA:FALSE\nextendedKeyUsage=serverAuth\nkeyUsage=digitalSignature,keyEncipherment\n").unwrap();
    docker.openssl(&[
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "1",
        "-subj",
        "/CN=mrsm-fixture-ca",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-keyout",
        ca_key.to_str().unwrap(),
        "-out",
        ca.to_str().unwrap(),
    ]);
    docker.openssl(&[
        "req",
        "-new",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-subj",
        "/CN=fixturetls",
        "-keyout",
        server_key.to_str().unwrap(),
        "-out",
        request.to_str().unwrap(),
    ]);
    docker.openssl(&[
        "x509",
        "-req",
        "-in",
        request.to_str().unwrap(),
        "-CA",
        ca.to_str().unwrap(),
        "-CAkey",
        ca_key.to_str().unwrap(),
        "-CAcreateserial",
        "-days",
        "1",
        "-extfile",
        extensions.to_str().unwrap(),
        "-out",
        certificate.to_str().unwrap(),
    ]);
    {
        use std::os::unix::fs::PermissionsExt;
        for key in [&ca_key, &server_key] {
            assert_eq!(
                std::fs::metadata(key).unwrap().permissions().mode() & 0o077,
                0
            );
        }
    }
    let outbound = format!(
        "outbound-team-{}",
        uuid::Uuid::parse_str(teams[1].id.as_deref().unwrap())
            .unwrap()
            .simple()
    );
    let tls_network = format!("{project}-fixture-outbound");
    docker.command(&[
        "network",
        "create",
        "--label",
        &format!("mrsm.test.owner={project}"),
        "--label",
        &format!("com.docker.compose.project={project}"),
        "--label",
        &format!("com.docker.compose.network={outbound}"),
        &tls_network,
    ]);
    let tls_name = format!("{project}-tls-peer");
    let peer_code = "import http.server,ssl\nclass Handler(http.server.BaseHTTPRequestHandler):\n def do_HEAD(self): self.send_response(200);self.end_headers()\n def do_GET(self): self.do_HEAD();self.wfile.write(b'owned TLS fixture')\nserver=http.server.HTTPServer(('0.0.0.0',8443),Handler)\ncontext=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain('/run/tls/server.pem','/run/tls/server.key')\nserver.socket=context.wrap_socket(server.socket,server_side=True)\nserver.serve_forever()";
    docker.checked(
        vec![
            "run".into(),
            "--detach".into(),
            "--name".into(),
            tls_name.clone(),
            "--network".into(),
            tls_network.clone(),
            "--label".into(),
            format!("mrsm.test.owner={project}"),
            "--label".into(),
            format!("com.docker.compose.project={project}"),
            "--label".into(),
            "com.docker.compose.service=fixture_tls".into(),
            "--read-only".into(),
            "--cap-drop".into(),
            "ALL".into(),
            "--security-opt".into(),
            "no-new-privileges:true".into(),
            "--user".into(),
            "1000:1000".into(),
            "--tmpfs".into(),
            "/meerschaum:rw,nosuid,nodev,size=2m,uid=1000,gid=1000,mode=0700".into(),
            "--mount".into(),
            format!(
                "type=bind,source={},target=/run/tls/server.pem,readonly",
                certificate.display()
            ),
            "--mount".into(),
            format!(
                "type=bind,source={},target=/run/tls/server.key,readonly",
                server_key.display()
            ),
            "--entrypoint".into(),
            "python3".into(),
            pinned_image.clone(),
            "-u".into(),
            "-P".into(),
            "-c".into(),
            peer_code.into(),
        ],
        40,
    );
    let tls_peer: Json =
        serde_json::from_str::<Json>(&docker.command(&["container", "inspect", &tls_name]))
            .unwrap()[0]
            .clone();
    let tls_ip = tls_peer["NetworkSettings"]["Networks"][&tls_network]["IPAddress"]
        .as_str()
        .unwrap();
    let resolver = root.path().join("fixture_dns.py");
    std::fs::write(&resolver, format!("import json,sys\nassert sys.argv[1]=='--_mrsm-resolve-executor-relays'\nassert json.loads(sys.argv[2])==['fixturetls']\nprint(json.dumps({{'fixturetls':[{}]}}))\n", json!(tls_ip))).unwrap();
    let configuration = json!({"meerschaum":{"instance":"sql:main","api_instance":"sql:main","connectors":{
        "sql":{"main":{"flavor":"sqlite","database":"/meerschaum/main.db","password":"synthetic-main-private"}},
        "api":{"api_data":{"host":"api","port":8989,"instance_keys":"sql:main","username":"svc_api","password":"synthetic-container-service","teams":["ApiFixture"]},
        "code_data":{"host":"api","port":8989,"instance_keys":"sql:main","username":"svc_code","password":"synthetic-container-service","teams":["CodeFixture"]},
        "api_writer":{"host":"api","port":8989,"instance_keys":"sql:main","username":"svc_api_writer","password":"synthetic-container-service","teams":["ApiFixture"]},
        "code_writer":{"host":"api","port":8989,"instance_keys":"sql:main","username":"svc_code_writer","password":"synthetic-container-service","teams":["CodeFixture"]}}}},
        "api":{"execution":{"callback_url":"http://api:8989"},"permissions":{"actions":{"non_admin":true},"instances":{"allow_multiple_instances":true,"allowed_instance_keys":["sql:main","api:api_data","api:code_data","api:api_writer","api:code_writer"]}}},
        "system":{"packages":{"auto_install":false},"api":{"permissions":{"actions":{"non_admin":true}}}},"jobs":{"startup":{"enabled":false}}});
    let mut command = vec![
        "start", "api", "--host", "0.0.0.0", "--port", "8989", "-w", "2",
    ];
    if !matches!(scenario, Scenario::Terminal(_)) {
        command.extend(["--no-dash", "--no-webterm"]);
    }
    let base = json!({"services":{"api":{"image":pinned_image,"user":"1000:1000","read_only":true,"init":true,"cap_drop":["ALL"],"security_opt":["no-new-privileges:true"],
        "command":command,
        "environment":{"MRSM_ROOT_DIR":"/meerschaum","HOME":"/meerschaum","MRSM_CONFIG":configuration.to_string(),"MRSM_NO_UPDATE_CHECK":"1","MRSM_NO_AUTO_INSTALL":"1","AWS_SECRET_ACCESS_KEY":"synthetic-main-environment-private"},
        "ports":[{"target":8989,"published":"0","host_ip":"127.0.0.1"}],"volumes":[{"type":"bind","source":main,"target":"/meerschaum"}],
        "tmpfs":["/tmp:rw,nosuid,nodev,size=128m,uid=1000,gid=1000,mode=1777","/run:rw,nosuid,nodev,size=32m,uid=1000,gid=1000,mode=0755"],"networks":["frontend"]}},"networks":{"frontend":{}},"volumes":{}});
    let _self_command = SelfCommand(meerschaum_core::self_exe::self_command());
    meerschaum_core::self_exe::set_self_command(vec![
        "python3".into(),
        "-P".into(),
        resolver.display().to_string(),
    ]);
    if provisioned {
        assert!(meerschaum_cli::executor::stack::provision_up(
            &administrator,
            &project,
            &["compose".into(), "up".into()],
            &[("DOCKER_CONTEXT".into(), "default".into())],
            Some(pinned_image.clone().into()),
            &mut SystemHost,
        ).expect("the selected image must contain the native Factory artifact manifest and match its independently inspected bytes"));
    }
    let model = meerschaum_cli::executor::stack::apply_with_image(
        &administrator,
        Value::from_json_str(&base.to_string()).unwrap(),
        Some(pinned_image.clone().into()),
    )
    .unwrap();
    let mut model = serde_json::to_value(model).unwrap();
    // Only the outbound test instrumentation bridge is pre-created; the executor's
    // isolated network and generated relay/proxy services retain their actual model.
    model["networks"][&outbound] = json!({"external":true,"name":tls_network});
    model["services"][&services[1]]["volumes"].as_array_mut().unwrap().push(json!({"type":"bind","source":ca,"target":"/etc/pki/ca-trust/source/anchors/mrsm-fixture.pem","read_only":true}));
    for kind in ["services", "networks", "volumes"] {
        for value in model[kind].as_object_mut().unwrap().values_mut() {
            if value["external"] == true {
                continue;
            }
            if value.get("labels").is_none() {
                value["labels"] = json!({});
            }
            value["labels"]["mrsm.test.owner"] = json!(project);
        }
    }
    std::fs::write(&compose_file, serde_json::to_vec_pretty(&model).unwrap()).unwrap();
    docker.compose(&["up", "--detach", "--no-build", "--pull", "never"]);
    let origin = docker.main_origin();
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(90))
        .build()
        .unwrap();
    healthy(&client, &origin).await;
    for service in &services {
        docker.wait_executor(service);
    }
    let binaries: Json = serde_json::from_str(&docker.exec("api", "import hashlib,importlib,json; n=importlib.import_module('meerschaum._core._native');print(json.dumps({'native':hashlib.file_digest(open('/usr/local/bin/mrsm','rb'),'sha256').hexdigest(),'sdk':hashlib.file_digest(open(n.__file__,'rb'),'sha256').hexdigest()}))")).unwrap();
    assert_eq!(binaries["native"], native_sha);
    assert_eq!(binaries["sdk"], sdk_sha);
    if let Scenario::Terminal(placement) = scenario {
        container_terminal::round_trip(placement, &client, &origin, &main, instance).await;
        return;
    }
    let alice = login(&client, &origin, "alice").await;
    let bob = login(&client, &origin, "bob").await;
    // The preserved read-only services can observe their own rows, but the original
    // operator's rights cannot substitute for the configured service's pipes:write.
    for (token, alias, metric) in [
        (&alice, "api:api_data", "api_readonly"),
        (&bob, "api:code_data", "code_readonly"),
    ] {
        let url = format!(
            "{origin}/pipes/plugin:container_probe/{metric}/None/data?instance_keys={alias}"
        );
        let (status, _) = response(client.get(&url).bearer_auth(token)).await;
        assert_eq!(status, 200, "the read-only service lost its permitted read");
        let (status, result) = response(
            client
                .post(&url)
                .bearer_auth(token)
                .json(&json!([{"id":99}])),
        )
        .await;
        assert_eq!(status, 403, "read-only service accepted a write: {result}");
    }
    let api_before = assert_runtime(
        &docker,
        &services[0],
        &teams[0],
        (1024, 1., 256),
        &administrator,
        provisioned,
    );
    let code_before = assert_runtime(
        &docker,
        &services[1],
        &teams[1],
        (1536, 1.5, 384),
        &administrator,
        provisioned,
    );
    let private_dir = administrator.join("executors/attestation/private");
    for service in model["services"]
        .as_object()
        .unwrap()
        .keys()
        .filter(|service| !services.contains(service))
    {
        let observed = docker.inspect(service);
        let mounts = observed["Mounts"].as_array().unwrap();
        assert!(mounts.iter().all(|mount| {
            let source = Path::new(mount["Source"].as_str().unwrap());
            mount["Destination"] != PRIVATE_PATH
                && (mount["Type"] != "bind"
                    || (!private_dir.starts_with(source) && !source.starts_with(&private_dir)))
        }));
        let pins = mounts
            .iter()
            .find(|mount| mount["Destination"] == PUBLIC_DIRECTORY);
        assert_eq!(pins.is_some(), provisioned && service == "api");
        if let Some(pins) = pins {
            assert_eq!(pins["RW"], false);
            assert_eq!(
                Path::new(pins["Source"].as_str().unwrap()),
                administrator.join("executors/attestation/public")
            );
        }
    }
    for observed in [&api_before, &code_before, &docker.inspect("api"), &tls_peer] {
        assert_eq!(observed["Image"], pinned_image);
    }
    for (index, observed) in [&api_before, &code_before].into_iter().enumerate() {
        let record = desired
            .teams
            .get(&uuid::Uuid::parse_str(teams[index].id.as_deref().unwrap()).unwrap())
            .unwrap();
        assert_eq!(
            observed["Config"]["Labels"]["mrsm.executor.generation"],
            record.verifier.generation.to_string()
        );
        assert_eq!(
            observed["Config"]["Labels"]["mrsm.executor.issuer"],
            record.verifier.issuer.to_string()
        );
    }
    for service in services.iter().take(2) {
        let network = docker.inspect(service)["NetworkSettings"]["Networks"]
            .as_object()
            .unwrap()
            .keys()
            .next()
            .unwrap()
            .to_string();
        let value: Json =
            serde_json::from_str::<Json>(&docker.command(&["network", "inspect", &network]))
                .unwrap()[0]
                .clone();
        assert_eq!(value["Internal"], true);
        assert_eq!(
            value["Options"]["com.docker.network.bridge.gateway_mode_ipv4"],
            "isolated"
        );
        assert_eq!(
            value["Options"]["com.docker.network.bridge.gateway_mode_ipv6"],
            "isolated"
        );
    }
    if scenario == Scenario::Recovery {
        version_job(&client, &origin, &alice, "api").await;
        let initial = Catalog::new(&main).get("version-api").unwrap().unwrap();
        let api_start = format!("{origin}/jobs/version-api/start");
        docker.compose(&["stop", &services[0]]);
        let (status, result) = response(client.post(&api_start).bearer_auth(&alice)).await;
        assert!(status >= 400 || result[0] == false, "{status} {result}");
        assert!(!main.join("jobs").join(&initial.remote_name).exists());
        docker.compose(&[
            "up",
            "--detach",
            "--no-build",
            "--pull",
            "never",
            &services[0],
        ]);
        docker.wait_executor(&services[0]);
        let (status, result) = response(client.post(&api_start).bearer_auth(&alice)).await;
        assert_eq!((status, result[0].as_bool()), (200, Some(true)), "{result}");
        assert_version_completed(&client, &origin, &alice, "api").await;

        start_job(
            &client,
            &origin,
            &bob,
            "durable-state",
            "api:code_writer",
            "code_resource",
        )
        .await;
        let first = completed(&client, &origin, &bob, "durable-state", "team").await;
        assert_eq!(first["hostname"], code_before["Config"]["Hostname"]);
        assert_eq!(
            first["original"],
            json!([first["hostname"], first["hostname"]])
        );
        let old_id = docker.id(&services[1]);
        docker.compose(&[
            "up",
            "--detach",
            "--force-recreate",
            "--no-build",
            "--pull",
            "never",
            &services[1],
        ]);
        docker.wait_executor(&services[1]);
        assert_ne!(docker.id(&services[1]), old_id);
        let team_start = format!("{origin}/jobs/durable-state/start");
        let (status, result) = response(client.post(team_start).bearer_auth(&bob)).await;
        assert_eq!((status, result[0].as_bool()), (200, Some(true)), "{result}");
        let next = completed(&client, &origin, &bob, "durable-state", "team").await;
        // Match the worker that read the persisted markers to the new container.
        let current = docker.inspect(&services[1]);
        assert_ne!(current["Config"]["Hostname"], first["hostname"]);
        assert_eq!(next["hostname"], current["Config"]["Hostname"]);
        assert_eq!(next["original"], first["original"]);
        assert_committed_container_row(instance.as_ref(), "code_resource").await;

        docker.compose(&["restart", "api"]);
        let origin = docker.main_origin();
        healthy(&client, &origin).await;
        let (status, result) = response(
            client
                .post(format!("{origin}/jobs/version-api/start"))
                .bearer_auth(&alice),
        )
        .await;
        assert_eq!((status, result[0].as_bool()), (200, Some(true)), "{result}");
        assert_version_completed(&client, &origin, &alice, "api").await;
        let restored = Catalog::new(&main).get("version-api").unwrap().unwrap();
        assert_eq!(restored.team_id, initial.team_id);
        assert_eq!(restored.owner_binding, initial.owner_binding);
        assert_eq!(restored.placement, JobPlacement::Remote);
        assert_ne!(restored.callback, initial.callback);
        assert!(restored.context_revision > initial.context_revision);
        return;
    }
    if scenario == Scenario::InstalledProfile(Role::Team) {
        version_job(&client, &origin, &bob, "team").await;
        return;
    }
    if scenario == Scenario::InstalledProfile(Role::Api) {
        version_job(&client, &origin, &alice, "api").await;
        let (status, catalog) = response(
            client
                .get(format!("{origin}/plugins/extensions"))
                .bearer_auth(&alice),
        )
        .await;
        assert_eq!(status, 200, "{catalog}");
        let plugin = &catalog["plugins"]["container_probe"];
        assert_eq!(plugin["kind"], "python_module", "{plugin}");
        assert_eq!(plugin["metadata"]["cli_arguments"], "absent", "{plugin}");
        assert!(plugin["metadata"]["capabilities"].is_object(), "{plugin}");
        return;
    }
    if let Scenario::PipeDelivery(role) = scenario {
        let (name, token, profile, alias, metric, container) = match role {
            Role::Api => (
                "api-delivery",
                &alice,
                "api",
                "api:api_writer",
                "api_resource",
                &api_before,
            ),
            Role::Team => (
                "team-delivery",
                &bob,
                "team",
                "api:code_writer",
                "code_resource",
                &code_before,
            ),
        };
        let started = Instant::now();
        start_job(&client, &origin, token, name, alias, metric).await;
        let admitted = started.elapsed();
        let proof = completed(&client, &origin, token, name, profile).await;
        eprintln!(
            "One-row {profile} job admission {admitted:?}, completion {:?}",
            started.elapsed()
        );
        assert_eq!(proof["hostname"], container["Config"]["Hostname"]);
        if role == Role::Team {
            assert_eq!(proof["initial_rows"], 1, "{proof}");
        }
        assert_committed_container_row(instance.as_ref(), metric).await;
        if role == Role::Api {
            let record = Catalog::new(&main).get(name).unwrap().unwrap();
            let launch =
                uuid::Uuid::parse_str(record.remote_name.strip_prefix("mrsm-").unwrap()).unwrap();
            let prefix = format!("mrsm-graph-{launch}-");
            let history = instance.read("SELECT h.run_id, n.status, n.registration_ref, \
                (SELECT COUNT(*) FROM mrsm_engine_events AS e WHERE e.run_id = h.run_id) AS events \
                FROM mrsm_engine_runs AS h JOIN mrsm_engine_nodes AS n ON n.run_id = h.run_id \
                WHERE h.success = 1 AND n.pipe_ref = 'api:api_writer|plugin:container_probe|api_resource|None'").await.unwrap();
            let rows = meerschaum_connector::pipe_ops::frame_to_docs(&history);
            let runs: Vec<_> = rows.iter().filter(|row| matches!(&row["run_id"], KValue::Str(id) if id.strip_prefix(&prefix).and_then(|suffix| uuid::Uuid::parse_str(suffix).ok()).is_some())).collect();
            let [run] = runs.as_slice() else {
                panic!("Expected one completed run for this API writer launch: {rows:?}")
            };
            assert_eq!(run["status"], KValue::Str("succeeded".into()));
            assert_eq!(
                run["registration_ref"],
                KValue::Str("sql:main|plugin:container_probe|api_resource|None".into())
            );
            assert!(matches!(run["events"], KValue::Int(count) if count > 0));
        }
        return;
    }
    if scenario == Scenario::NativeMetadata {
        let url = format!("{origin}/plugins/extensions");
        let (status, catalog) = response(client.get(&url).bearer_auth(&bob)).await;
        assert_eq!(status, 200, "native extension discovery: {catalog}");
        let plugin = &catalog["plugins"]["native_greeting"];
        assert_eq!(plugin["kind"], "native_source", "{catalog}");
        assert!(
            plugin["metadata"]["mcp_resources"]
                .as_array()
                .unwrap()
                .iter()
                .any(|resource| resource["uri"] == "mrsm://release-fixture/greeting"),
            "{plugin}"
        );
        let cache = std::fs::read_dir(main.join("executor-plugin-metadata"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|file| {
                file.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .contains("-factory-")
            })
            .unwrap();
        use std::os::unix::fs::MetadataExt;
        let cache_file = std::fs::File::open(&cache).unwrap();
        let saved: Json = serde_json::from_reader(&cache_file).unwrap();
        for field in ["pin_sha256", "native_receipt_sha256"] {
            assert!(
                saved["factory_receipt"][field]
                    .as_str()
                    .is_some_and(meerschaum_server::executor::factory_artifacts::valid_digest)
            );
        }
        let (status, repeated) = response(client.get(url).bearer_auth(&bob)).await;
        assert_eq!(status, 200);
        assert_eq!(catalog, repeated);
        // Cache commits atomically replace the file. Holding the first descriptor also
        // prevents inode reuse, so a second production run cannot masquerade as a cache hit.
        assert_eq!(
            cache_file.metadata().unwrap().ino(),
            std::fs::metadata(cache).unwrap().ino()
        );
        return;
    }
    start_job(
        &client,
        &origin,
        &alice,
        "api-in-container",
        "api:api_writer",
        "api_resource",
    )
    .await;
    let api_proof = completed(&client, &origin, &alice, "api-in-container", "api").await;
    assert_eq!(api_proof["hostname"], api_before["Config"]["Hostname"]);
    assert_result_publication(&docker, &services[0], &client, &origin, &alice).await;
    start_job(
        &client,
        &origin,
        &bob,
        "code-in-container",
        "api:code_writer",
        "code_resource",
    )
    .await;
    let code_proof = completed(&client, &origin, &bob, "code-in-container", "team").await;
    assert_eq!(code_proof["hostname"], code_before["Config"]["Hostname"]);
    assert_eq!(code_proof["relay_tls_verified"], true);
    for metric in ["api_resource", "code_resource"] {
        assert_committed_container_row(instance.as_ref(), metric).await;
    }
    let relay_service = model["services"]
        .as_object()
        .unwrap()
        .iter()
        .find(|(_, service)| service["labels"]["mrsm.executor.role"] == "relay")
        .unwrap()
        .0;
    let relay = docker.inspect(relay_service);
    assert!(
        relay["NetworkSettings"]["Networks"]
            .as_object()
            .unwrap()
            .values()
            .any(|network| network["IPAddress"] == code_proof["relay_peer"])
    );
    if std::env::var("MRSM_TEST_EXECUTOR_PUBLIC_TLS").as_deref() == Ok("1") {
        assert_eq!(code_proof["optional_public_proxy_tls_verified"], true);
    }
    for (name, owner, index, profile) in [
        ("api-in-container", "alice", 0, Profile::Api),
        ("code-in-container", "bob", 1, Profile::Team),
    ] {
        let record = Catalog::new(&main).get(name).unwrap().unwrap();
        assert_eq!(record.profile, profile);
        assert_eq!(record.placement, JobPlacement::Remote);
        assert_eq!(
            record.team_id.to_string(),
            teams[index].id.as_deref().unwrap()
        );
        assert_eq!(
            record.properties.get("owner").and_then(Value::as_str),
            Some(owner)
        );
        assert!(!main.join("jobs").join(&record.remote_name).exists());
    }
    // An unavailable executor never changes placement to a Main-host worker.
    docker.compose(&["stop", &services[0]]);
    let (status, result) = response(
        client
            .post(format!("{origin}/jobs/api-in-container/start"))
            .bearer_auth(&alice),
    )
    .await;
    assert!(
        status >= 400 || result[0] == false,
        "offline executor admitted work: {status} {result}"
    );
    assert_eq!(
        Catalog::new(&main)
            .get("api-in-container")
            .unwrap()
            .unwrap()
            .placement,
        JobPlacement::Remote
    );
    docker.compose(&[
        "up",
        "--detach",
        "--no-build",
        "--pull",
        "never",
        &services[0],
    ]);
    docker.wait_executor(&services[0]);
    let (status, result) = response(
        client
            .post(format!("{origin}/jobs/api-in-container/start"))
            .bearer_auth(&alice),
    )
    .await;
    assert_eq!(status, 200, "restored executor: {result}");
    assert_eq!(result[0], true, "{result}");
    completed(&client, &origin, &alice, "api-in-container", "api").await;
    assert_committed_container_row(instance.as_ref(), "api_resource").await;
    // The finished Code process wrote both markers in its ordinary Team directories.
    let durable = |service: &str| -> Json {
        serde_json::from_str(&docker.exec(service, "from pathlib import Path
import json
print(json.dumps([Path('/meerschaum',p,'container-fixture','origin').read_text() for p in ['data','venvs']]))")).unwrap()
    };
    let original = code_before["Config"]["Hostname"].as_str().unwrap();
    let expected = json!([original, original]);
    assert_eq!(durable(&services[1]), expected);
    let old_id = docker.id(&services[1]);
    docker.compose(&[
        "up",
        "--detach",
        "--force-recreate",
        "--no-build",
        "--pull",
        "never",
        &services[1],
    ]);
    docker.wait_executor(&services[1]);
    assert_ne!(docker.id(&services[1]), old_id);
    assert_eq!(durable(&services[1]), expected);
    let current = assert_runtime(
        &docker,
        &services[1],
        &teams[1],
        (1536, 1.5, 384),
        &administrator,
        provisioned,
    );
    let (status, result) = response(
        client
            .post(format!("{origin}/jobs/code-in-container/start"))
            .bearer_auth(&bob),
    )
    .await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result[0], true, "{result}");
    completed(&client, &origin, &bob, "code-in-container", "team").await;
    assert_eq!(durable(&services[1]), expected);
    assert_eq!(docker.exec(&services[1], "from pathlib import Path;print(Path('/meerschaum/data/container-fixture/latest').read_text())"), current["Config"]["Hostname"].as_str().unwrap());
    assert_committed_container_row(instance.as_ref(), "code_resource").await;
    // Main restart retains original owner/UUID/profile, and explicit starts mint fresh leases.
    let previous = Catalog::new(&main)
        .get("api-in-container")
        .unwrap()
        .unwrap();
    docker.compose(&["restart", "api"]);
    let origin = docker.main_origin();
    healthy(&client, &origin).await;
    let (status, result) = response(
        client
            .post(format!("{origin}/jobs/api-in-container/start"))
            .bearer_auth(&alice),
    )
    .await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result[0], true, "{result}");
    completed(&client, &origin, &alice, "api-in-container", "api").await;
    for metric in ["api_resource", "code_resource"] {
        assert_committed_container_row(instance.as_ref(), metric).await;
    }
    let restored = Catalog::new(&main)
        .get("api-in-container")
        .unwrap()
        .unwrap();
    assert_eq!(restored.team_id, previous.team_id);
    assert_eq!(restored.profile, previous.profile);
    assert_eq!(restored.owner_binding, previous.owner_binding);
    assert_ne!(restored.callback, previous.callback);
    assert!(restored.context_revision > previous.context_revision);
    start_job(
        &client,
        &origin,
        &alice,
        "api-lease-outage",
        "api:api_writer",
        "api_hold",
    )
    .await;
    wait_for_held_worker(&client, &origin, &alice, "api-lease-outage").await;
    let held = Catalog::new(&main)
        .get("api-lease-outage")
        .unwrap()
        .unwrap();
    let capture = format!(
        "import json,pathlib\np=int(pathlib.Path('{FACTORY_CONTROL_ROOT}/jobs/{}/process.pid').read_text().strip())\nprocesses={{}}\nfor path in pathlib.Path('/proc').glob('[0-9]*/stat'):\n try:\n  fields=path.read_text().rsplit(')',1)[1].split();processes[int(path.parent.name)]=(int(fields[1]),fields[19])\n except (OSError,ValueError,IndexError): pass\nowned={{}};pending=[p]\nwhile pending:\n pid=pending.pop()\n if pid in owned or pid not in processes: continue\n owned[pid]=processes[pid][1];pending.extend(child for child,fields in processes.items() if fields[0]==pid)\nassert owned,'held job has no live process identity'\nprint(json.dumps(owned))",
        held.remote_name
    );
    let owned: Json = serde_json::from_str(&docker.exec(&services[0], &capture)).unwrap();
    docker.compose(&["kill", "--signal", "SIGKILL", "api"]);
    assert_eq!(docker.inspect("api")["State"]["Running"], false);
    let code = format!(
        "import json,pathlib,time\nowned=json.loads({})\np=pathlib.Path('{FACTORY_CONTROL_ROOT}/jobs/{}/properties.json')\ndeadline=time.monotonic()+50\nwhile True:\n data=json.loads(p.read_text());alive=[]\n for pid,birth in owned.items():\n  try:\n   fields=pathlib.Path('/proc',pid,'stat').read_text().rsplit(')',1)[1].split()\n   if fields[19]==birth and fields[0] not in ('Z','X'): alive.append(pid)\n  except (OSError,IndexError): pass\n if not alive and data.get('result',[None])[0] is False: break\n assert time.monotonic()<deadline,'remote API worker outlived its callback lease';time.sleep(.1)\nprint('expired')",
        json!(owned.to_string()),
        held.remote_name
    );
    assert_eq!(
        docker.checked(
            vec![
                "exec".into(),
                docker.id(&services[0]),
                "python3".into(),
                "-P".into(),
                "-c".into(),
                code
            ],
            65
        ),
        "expired"
    );
    docker.compose(&["up", "--detach", "--no-build", "--pull", "never", "api"]);
    let origin = docker.main_origin();
    healthy(&client, &origin).await;
    let receipt = json!({"owner":project,"image":image_info["Id"],"native_sha256":native_sha,"sdk_sha256":sdk_sha,
        "api":api_proof,"code":code_proof,"dns_pin_source":"fixture-owned command double; native host DNS separately covered by executor_network","teams":desired.teams.keys().map(|id| id.to_string()).collect::<Vec<_>>(),
        "executor_factory_key_provisioned":provisioned,"executor_outage_refused":true,"executor_recreated_state_retained":true,"main_restart_identity_retained":true,"main_outage_remote_lease_closed":true,"executor_result_publication":true});
    std::fs::write(
        parent.join(format!("{project}-receipt.json")),
        serde_json::to_vec_pretty(&receipt).unwrap(),
    )
    .unwrap();
}
