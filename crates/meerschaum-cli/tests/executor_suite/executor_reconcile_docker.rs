//! Explicit local Docker acceptance for destructive executor reconciliation.
//! Run with `cargo test -p meerschaum-cli --test executor_reconcile_docker -- --ignored`.

use std::time::Duration;

use meerschaum_cli::dispatch::{CommandOutput, CommandSpec, Host, SystemHost};
use meerschaum_cli::executor::registry::Registry;
use meerschaum_core::Value;
use meerschaum_core::access::Team;
use serde_json::{Value as Json, json};

struct LocalDocker;
impl Host for LocalDocker {
    fn run(&mut self, _: &CommandSpec) -> std::io::Result<i32> {
        panic!("acceptance uses bounded captured calls");
    }
    fn read_line(&mut self, _: &str) -> Option<String> {
        None
    }
    fn output(
        &mut self,
        spec: &CommandSpec,
        timeout: Option<Duration>,
    ) -> std::io::Result<CommandOutput> {
        let mut spec = spec.clone();
        spec.args
            .splice(0..0, ["--context".into(), "default".into()]);
        spec.env_clear = true;
        spec.env = ["PATH", "HOME"]
            .into_iter()
            .filter_map(|key| std::env::var(key).ok().map(|v| (key.into(), v)))
            .collect();
        SystemHost.output(&spec, timeout)
    }
}

struct Fixture {
    project: String,
    host: LocalDocker,
    containers: Vec<String>,
    networks: Vec<String>,
    volumes: Vec<String>,
}

impl Fixture {
    fn command(&mut self, args: &[&str]) -> CommandOutput {
        self.host
            .output(
                &CommandSpec {
                    program: "docker".into(),
                    args: args.iter().map(|s| s.to_string()).collect(),
                    quiet: true,
                    scrub_secrets: true,
                    ..Default::default()
                },
                Some(Duration::from_secs(30)),
            )
            .unwrap()
    }
    fn docker(&mut self, args: &[&str]) -> String {
        let output = self.command(args);
        assert_eq!(
            output.code,
            0,
            "owned Docker operation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!output.timed_out);
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }
    fn container(
        &mut self,
        suffix: &str,
        network: &str,
        labels: Json,
        volume: Option<&str>,
    ) -> String {
        let name = format!("{}-{suffix}", self.project);
        self.containers.push(name.clone());
        let mut args = vec![
            "run".to_string(),
            "-d".into(),
            "--name".into(),
            name,
            "--network".into(),
            network.into(),
            "--read-only".into(),
            "--cap-drop".into(),
            "ALL".into(),
            "--security-opt".into(),
            "no-new-privileges:true".into(),
            "--user".into(),
            "1000:1000".into(),
            "--pids-limit".into(),
            "32".into(),
            "--memory".into(),
            "64m".into(),
            "--memory-swap".into(),
            "64m".into(),
            "--cpus".into(),
            "0.25".into(),
            "--label".into(),
            format!("mrsm.test.owner={}", self.project),
        ];
        for (key, value) in labels.as_object().unwrap() {
            args.extend([
                "--label".into(),
                format!("{key}={}", value.as_str().unwrap()),
            ]);
        }
        if let Some(volume) = volume {
            args.extend([
                "--mount".into(),
                format!("type=volume,source={volume},target=/data"),
            ]);
        }
        args.extend([
            "python:3.14-alpine".into(),
            "python".into(),
            "-c".into(),
            "import time;time.sleep(300)".into(),
        ]);
        self.docker(&args.iter().map(String::as_str).collect::<Vec<_>>())
    }
    fn exists(&mut self, kind: &str, id: &str) -> bool {
        let field = if kind == "volume" {
            "{{.Name}}"
        } else {
            "{{.Id}}"
        };
        self.command(&[kind, "inspect", "--format", field, id]).code == 0
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for (kind, resources, projection) in [
            (
                "container",
                self.containers.clone(),
                "{{index .Config.Labels \"mrsm.test.owner\"}}",
            ),
            (
                "network",
                self.networks.clone(),
                "{{index .Labels \"mrsm.test.owner\"}}",
            ),
            (
                "volume",
                self.volumes.clone(),
                "{{index .Labels \"mrsm.test.owner\"}}",
            ),
        ] {
            for name in resources {
                let owner = self.command(&[kind, "inspect", "--format", projection, &name]);
                if owner.code == 0 && String::from_utf8_lossy(&owner.stdout).trim() == self.project
                {
                    let args = if kind == "container" {
                        vec![kind, "rm", "--force", name.as_str()]
                    } else {
                        vec![kind, "rm", name.as_str()]
                    };
                    let _ = self.command(&args);
                }
            }
        }
    }
}

#[test]
#[ignore = "requires local Docker Engine >=28 and cached python:3.14-alpine; mandatory executor acceptance"]
fn policy_reconciliation_refuses_foreign_endpoints_and_preserves_named_state() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture {
        project: format!("tpr-executor-reconcile-{}", uuid::Uuid::new_v4().simple()),
        host: LocalDocker,
        containers: Vec::new(),
        networks: Vec::new(),
        volumes: Vec::new(),
    };
    let endpoint = fixture.docker(&[
        "context",
        "inspect",
        "default",
        "--format",
        "{{.Endpoints.docker.Host}}",
    ]);
    assert!(
        endpoint.starts_with("unix://"),
        "acceptance requires a local Unix Docker endpoint"
    );
    fixture.docker(&[
        "image",
        "inspect",
        "--format",
        "{{.Id}}",
        "python:3.14-alpine",
    ]);
    let mut team = Team::new("Owned");
    team.id = Some(uuid::Uuid::new_v4().to_string());
    team.attributes = Value::from_json_str(r#"{"isolated":true,"execution":{"mode":"container"}}"#)
        .unwrap()
        .to_map();
    let desired = Registry::new(root.path()).reconcile(&[team]).unwrap();
    let record = desired.teams.values().next().unwrap();
    meerschaum_cli::executor::stack::preflight(root.path(), &[], &mut fixture.host).unwrap();
    let project = fixture.project.clone();
    let network = format!("{project}-team");
    fixture.networks.push(network.clone());
    let common = json!({"com.docker.compose.project": project, "mrsm.executor.managed": "true",
        "mrsm.executor.issuer": record.verifier.issuer.to_string(), "mrsm.executor.team": record.team_id.to_string()});
    let mut labels = common.clone();
    labels["mrsm.executor.role"] = json!("network");
    labels["mrsm.executor.network"] = json!("team");
    labels["com.docker.compose.network"] = json!(format!("executor-{}", record.key));
    let mut args = vec![
        "network".to_string(),
        "create".into(),
        "--internal".into(),
        "--label".into(),
        format!("mrsm.test.owner={project}"),
    ];
    for (key, value) in labels.as_object().unwrap() {
        args.extend([
            "--label".into(),
            format!("{key}={}", value.as_str().unwrap()),
        ]);
    }
    args.push(network.clone());
    fixture.docker(&args.iter().map(String::as_str).collect::<Vec<_>>());
    let volume = format!("{project}-data");
    fixture.volumes.push(volume.clone());
    fixture.docker(&[
        "volume",
        "create",
        "--label",
        &format!("mrsm.test.owner={project}"),
        &volume,
    ]);
    let api = fixture.container(
        "api",
        &network,
        json!({"com.docker.compose.project": project, "com.docker.compose.service": "api"}),
        None,
    );
    let mut labels = common;
    labels["com.docker.compose.service"] = json!(format!("executor-{}", record.key));
    labels["mrsm.executor.role"] = json!("executor");
    labels["mrsm.executor.generation"] = json!(record.verifier.generation.to_string());
    let executor = fixture.container("executor", &network, labels, Some(&volume));
    let bystander = fixture.container("bystander", &network, json!({"com.docker.compose.project": format!("another-{project}"), "com.docker.compose.service": "api"}), None);
    assert!(
        meerschaum_cli::executor::stack::retire(root.path(), &project, &[], &mut fixture.host)
            .is_err()
    );
    assert!(fixture.exists("container", &executor));
    assert!(fixture.exists("network", &network));
    fixture.docker(&["network", "disconnect", &network, &bystander]);
    meerschaum_cli::executor::stack::retire(root.path(), &project, &[], &mut fixture.host).unwrap();
    assert!(!fixture.exists("container", &executor));
    assert!(!fixture.exists("network", &network));
    assert!(fixture.exists("container", &api));
    assert!(fixture.exists("container", &bystander));
    assert!(
        fixture.exists("volume", &volume),
        "reconciliation removed retained team state"
    );
}
