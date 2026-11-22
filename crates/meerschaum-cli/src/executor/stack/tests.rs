//! Generated executor services, images, networks, relay pins, preflight and retirement.

use super::*;
use crate::executor::registry::Registry;

#[test]
fn reconciliation_uses_the_selected_project_and_refuses_alternate_models() {
    let target = |words: &[&str]| {
        up_target(
            &words.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            "configured",
        )
    };
    assert_eq!(
        target(&["stack", "up"]).unwrap().as_deref(),
        Some("configured")
    );
    for option in [
        vec!["--project-name", "selected"],
        vec!["-pselected"],
        vec!["--project-name=selected"],
    ] {
        let mut words = vec!["stack", "--profile", "up"];
        words.extend(option);
        words.push("up");
        assert_eq!(target(&words).unwrap().as_deref(), Some("selected"));
    }
    assert_eq!(
        target(&["stack", "--profile", "up", "config"]).unwrap(),
        None
    );
    assert_eq!(target(&["stack", "--dry-run", "up"]).unwrap(), None);
    assert_eq!(target(&["stack", "up", "--dry-run"]).unwrap(), None);
    assert!(target(&["stack", "-f", "other.yml", "up"]).is_err());
    assert!(target(&["stack", "--project-directory=elsewhere", "up"]).is_err());
    assert!(target(&["stack", "--future-option", "up"]).is_err());
}

fn fixture() -> (tempfile::TempDir, Desired) {
    let root = tempfile::tempdir().unwrap();
    let mut team = meerschaum_core::access::Team::new("Wetlands");
    team.id = Some(uuid::Uuid::new_v4().to_string());
    team.attributes.insert("isolated".into(), Value::Bool(true));
    team.attributes.insert(
        "execution".into(),
        value(json!({"mode": "container", "relays": [{"host": "database.example", "port": 5432}]})),
    );
    let desired = Registry::new(root.path()).reconcile(&[team]).unwrap();
    (root, desired)
}

fn compose() -> Value {
    value(
        json!({"services": {"api": {"image": "bmeares/meerschaum:5.1.0-full", "volumes": ["api_root:/meerschaum"], "networks": ["backend"]}, "db": {"networks": ["backend"]}, "grafana": {"networks": ["backend"]}, "valkey": {"networks": ["backend"]}}, "networks": {"backend": {}}, "volumes": {"api_root": {}}}),
    )
}

fn pins() -> RelayPins {
    BTreeMap::from([(
        "database.example".into(),
        vec!["10.23.45.67".parse().unwrap()],
    )])
}

#[test]
fn hardened_team_services_have_private_state_and_networks_without_privileged_mounts() {
    let (root, desired) = fixture();
    let result = apply_desired(
        &Registry::new(root.path()).desired_dir(),
        &desired,
        compose(),
        &pins(),
    )
    .unwrap();
    let result = serde_json::to_value(result).unwrap();
    let record = desired.teams.values().next().unwrap();
    let name = format!("executor-{}", record.key);
    let service = &result["services"][&name];
    assert_eq!(
        service["working_dir"],
        meerschaum_core::team_scope::source_namespace::FACTORY_CONTROL_ROOT
    );
    assert_eq!(service["environment"]["HOME"], service["working_dir"]);
    assert_eq!(service["environment"]["MRSM_ROOT_DIR"], "/meerschaum");
    assert_eq!(service["healthcheck"]["test"][2], "-I");
    assert_eq!(service["healthcheck"]["test"][3], "-S");
    assert_eq!(service["user"], "1000:1000");
    assert_eq!(service["read_only"], true);
    assert_eq!(
        service["tmpfs"],
        json!([
            "/tmp:rw,nosuid,nodev,size=64m,uid=1000,gid=1000,mode=1777",
            "/run:rw,nosuid,nodev,size=16m,uid=0,gid=0,mode=0755"
        ])
    );
    for (name, service) in result["services"].as_object().unwrap() {
        if matches!(
            service["labels"]["mrsm.executor.role"].as_str(),
            Some("proxy" | "relay")
        ) {
            assert!(
                service["tmpfs"].as_array().unwrap().contains(&json!(
                    "/run:rw,nosuid,nodev,size=16m,uid=1000,gid=1000,mode=0755"
                )),
                "Network helper {name} changed its scratch runtime mount."
            );
        }
    }
    assert_eq!(service["cap_drop"], json!(["ALL"]));
    assert_eq!(service["mem_limit"], 4 * 1024 * 1024 * 1024u64);
    assert_eq!(service["memswap_limit"], service["mem_limit"]);
    let api = &result["services"]["api"];
    assert!(
        api["volumes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["target"] == crate::executor::registry::CONTAINER_DESIRED_DIR),
        "API desired mount should sit outside /meerschaum/executors: {:?}",
        api["volumes"]
    );
    assert_eq!(
        api["environment"][crate::executor::registry::DESIRED_DIR_ENV],
        crate::executor::registry::CONTAINER_DESIRED_DIR
    );
    assert_eq!(service["cpus"], 2.0);
    assert_eq!(service["pids_limit"], 1024);
    assert_eq!(result["networks"][&name]["internal"], true);
    assert_eq!(
        result["networks"][&name]["driver_opts"]["com.docker.network.bridge.gateway_mode_ipv4"],
        "isolated"
    );
    assert_eq!(
        result["networks"][&name]["driver_opts"]["com.docker.network.bridge.gateway_mode_ipv6"],
        "isolated"
    );
    assert_eq!(service["sysctls"]["net.ipv6.conf.all.disable_ipv6"], "1");
    assert_eq!(service["networks"], json!([name]));
    let mounts = service["volumes"].as_array().unwrap();
    assert_eq!(mounts.len(), 2);
    assert!(
        mounts
            .iter()
            .any(|mount| mount.as_str().is_some_and(|mount| mount
                .ends_with(meerschaum_core::team_scope::source_namespace::FACTORY_CONTROL_ROOT)))
    );
    assert!(!service.to_string().contains("docker.sock"));
    assert!(service.get("ports").is_none());
    assert_eq!(
        result["services"]["grafana"]["networks"],
        json!(["backend"])
    );
    assert_eq!(result["services"]["valkey"]["networks"], json!(["backend"]));
    let encoded = result.to_string();
    assert!(!encoded.contains("pk8"));
    assert!(!encoded.contains("registry.json"));
    for (name, service) in result["services"].as_object().unwrap() {
        assert!(
            service.get("privileged").is_none_or(|value| value == false),
            "Service {name} enables privileged mode."
        );
    }
}

#[test]
fn executor_images_are_native_full_by_default_independent_of_the_api_variant() {
    assert!(!default_executor_image().contains("-rc."));
    let (root, desired) = fixture();
    let mut model = compose().to_map();
    let services = map_entry(&mut model, "services").unwrap();
    let api = match services.get_mut("api").unwrap() {
        Value::Map(api) => api,
        _ => panic!("fixture API"),
    };
    api.insert("image".into(), "bmeares/meerschaum:5.1.0-slim".into());
    let result = serde_json::to_value(
        apply_desired(root.path(), &desired, Value::Map(model.clone()), &pins()).unwrap(),
    )
    .unwrap();
    assert_eq!(
        result["services"]["api"]["image"],
        "bmeares/meerschaum:5.1.0-slim"
    );
    let owned: Vec<_> = result["services"]
        .as_object()
        .unwrap()
        .values()
        .filter(|service| service["labels"]["mrsm.executor.managed"] == "true")
        .collect();
    assert_eq!(
        owned.len(),
        3,
        "executor, proxy and fixed-destination relay"
    );
    assert!(
        owned
            .iter()
            .all(|service| service["image"] == default_executor_image())
    );
    let custom = executor_image(Some(&"registry.example:5000/mrsm/native:fixture".into())).unwrap();
    let result = serde_json::to_value(
        apply_desired_with_image(root.path(), &desired, Value::Map(model), &pins(), &custom)
            .unwrap(),
    )
    .unwrap();
    assert!(
        result["services"]
            .as_object()
            .unwrap()
            .values()
            .filter(|service| service["labels"]["mrsm.executor.managed"] == "true")
            .all(|service| service["image"] == serde_json::to_value(&custom).unwrap())
    );
}

#[test]
fn malformed_or_known_wheel_only_executor_image_overrides_are_refused() {
    assert_eq!(
        executor_image(None).unwrap().as_str(),
        Some(default_executor_image().as_str())
    );
    for setting in [
        Value::Null,
        Value::Bool(false),
        Value::Int(1),
        Value::List(Vec::new()),
        Value::new_map(),
        "".into(),
        " ".into(),
        "image with spaces".into(),
        "https://registry.example/native".into(),
        "repo::tag".into(),
        "repo:".into(),
        "../native:tag".into(),
        "registry/$IMAGE".into(),
        "native@sha256:no-digest".into(),
        "bmeares/meerschaum:5.1.0-slim".into(),
        "docker.io/bmeares/meerschaum:slim".into(),
    ] {
        assert!(executor_image(Some(&setting)).is_err(), "{setting:?}");
    }
}

#[test]
fn retirement_removes_services_and_retains_the_private_volume_declaration() {
    let (root, mut desired) = fixture();
    let record = desired.teams.values_mut().next().unwrap();
    record.retired = true;
    let name = format!("executor-{}", record.key);
    let volume = format!("{name}-root");
    let result = apply_desired(root.path(), &desired, compose(), &pins()).unwrap();
    let result = serde_json::to_value(result).unwrap();
    assert!(result["services"].get(&name).is_none());
    assert!(result["volumes"].get(&volume).is_some());
}

#[test]
fn an_existing_resource_or_inconsistent_identity_is_refused() {
    let (root, mut desired) = fixture();
    desired.teams.values_mut().next().unwrap().key = "another-service".into();
    assert!(apply_desired(root.path(), &desired, compose(), &pins()).is_err());
}

#[test]
fn relay_dns_aliases_preserve_hostname_and_group_ports_only_on_the_owning_team_network() {
    let (root, mut desired) = fixture();
    let record = desired.teams.values_mut().next().unwrap();
    record
        .settings
        .relays
        .push(meerschaum_core::execution::Relay {
            host: "database.example".into(),
            port: 8883,
        });
    let mut other = record.clone();
    other.team_id = uuid::Uuid::new_v4();
    other.key = format!("team-{}", other.team_id.simple());
    other.verifier = meerschaum_server::executor::control::Authority::generate(
        record.verifier.issuer,
        other.team_id,
        uuid::Uuid::new_v4(),
    )
    .unwrap()
    .0
    .verifier();
    other.settings.relays.clear();
    let other_name = format!("executor-{}", other.key);
    desired.teams.insert(other.team_id, other);
    let result =
        serde_json::to_value(apply_desired(root.path(), &desired, compose(), &pins()).unwrap())
            .unwrap();
    let (relay_name, relay) = result["services"]
        .as_object()
        .unwrap()
        .iter()
        .find(|(_, s)| s["labels"]["mrsm.executor.role"] == "relay")
        .unwrap();
    let team = relay["labels"]["mrsm.executor.team"].as_str().unwrap();
    let name = format!(
        "executor-team-{}",
        uuid::Uuid::parse_str(team).unwrap().simple()
    );
    assert_eq!(
        result["services"]
            .as_object()
            .unwrap()
            .values()
            .filter(|s| s["labels"]["mrsm.executor.role"] == "relay")
            .count(),
        1
    );
    assert_eq!(
        relay["networks"][&name]["aliases"],
        json!(["database.example"])
    );
    assert!(relay["networks"].get(&other_name).is_none());
    assert!(
        relay["networks"]
            .as_object()
            .unwrap()
            .iter()
            .filter(|(network, _)| *network != &name)
            .all(|(_, spec)| spec.get("aliases").is_none())
    );
    let parameters: serde_json::Value = serde_json::from_str(
        relay["command"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(parameters["destination"]["host"], "database.example");
    assert_eq!(parameters["destination"]["ports"], json!([5432, 8883]));
    assert_eq!(
        parameters["destination"]["addresses"],
        json!(["10.23.45.67"])
    );
    assert!(
        result["services"][&name]["environment"]["NO_PROXY"]
            .as_str()
            .unwrap()
            .split(',')
            .any(|v| v == "database.example")
    );
    assert!(
        result["services"][&name]["depends_on"]
            .get(relay_name)
            .is_some()
    );
    assert_eq!(relay["sysctls"]["net.ipv4.ip_unprivileged_port_start"], "0");
    assert!(relay.get("ports").is_none());
}

#[test]
fn isolated_numeric_relays_and_reserved_service_aliases_fail_without_weakening_tls() {
    let (root, mut desired) = fixture();
    let record = desired.teams.values_mut().next().unwrap();
    record.settings.relays[0].host = "10.23.45.67".into();
    assert!(relay_groups(record).unwrap_err().contains("TLS"));
    record.settings.egress = Egress::Open;
    assert!(relay_groups(record).unwrap().is_empty());
    record.settings.egress = Egress::None;
    record.settings.relays[0].host = "db".into();
    let pins = BTreeMap::from([("db".into(), vec!["10.23.45.67".parse().unwrap()])]);
    assert!(
        apply_desired(root.path(), &desired, compose(), &pins)
            .unwrap_err()
            .contains("reserved")
    );
}

#[test]
fn deployment_resolution_is_bounded_credential_free_and_requires_exact_approved_answers() {
    struct Resolver {
        answer: &'static str,
        timeout: bool,
    }
    impl crate::dispatch::Host for Resolver {
        fn run(&mut self, _: &crate::dispatch::CommandSpec) -> std::io::Result<i32> {
            panic!("DNS helper only")
        }
        fn read_line(&mut self, _: &str) -> Option<String> {
            None
        }
        fn output(
            &mut self,
            spec: &crate::dispatch::CommandSpec,
            timeout: Option<std::time::Duration>,
        ) -> std::io::Result<crate::dispatch::CommandOutput> {
            assert_eq!(timeout, Some(std::time::Duration::from_secs(10)));
            assert!(spec.env_clear);
            assert!(spec.env.iter().all(|(k, _)| !matches!(
                k.as_str(),
                "TMUX" | "TMUX_PANE" | "MRSM_CONFIG" | "MRSM_PATCH" | "MRSM_SQL_MAIN"
            )));
            assert_eq!(spec.args[spec.args.len() - 2], RESOLVE_RELAYS);
            assert_eq!(spec.args.last().unwrap(), "[\"database.example\"]");
            Ok(crate::dispatch::CommandOutput {
                stdout: self.answer.as_bytes().to_vec(),
                timed_out: self.timeout,
                ..Default::default()
            })
        }
    }
    let (root, desired) = fixture();
    let mut resolver = Resolver {
        answer: r#"{"database.example":["10.23.45.67"]}"#,
        timeout: false,
    };
    assert_eq!(
        resolve_relay_pins(root.path(), &desired, &mut resolver).unwrap(),
        pins()
    );
    for invalid in [
        r#"{"other.example":["10.23.45.67"]}"#,
        r#"{"database.example":[]}"#,
        r#"{"database.example":["127.0.0.1"]}"#,
    ] {
        resolver.answer = invalid;
        assert!(resolve_relay_pins(root.path(), &desired, &mut resolver).is_err());
    }
    resolver.answer = r#"{"database.example":["10.23.45.67"]}"#;
    resolver.timeout = true;
    assert!(resolve_relay_pins(root.path(), &desired, &mut resolver).is_err());
}

#[test]
fn retirement_requires_project_issuer_team_and_generation_ownership() {
    let (_root, mut desired) = fixture();
    let record = desired.teams.values().next().unwrap();
    let mut labels = std::collections::BTreeMap::from([
        ("com.docker.compose.project".into(), "tpr-owned".into()),
        (
            "com.docker.compose.service".into(),
            format!("executor-{}", record.key),
        ),
        ("mrsm.executor.managed".into(), "true".into()),
        (
            "mrsm.executor.issuer".into(),
            record.verifier.issuer.to_string(),
        ),
        ("mrsm.executor.team".into(), record.team_id.to_string()),
        (
            "mrsm.executor.generation".into(),
            record.verifier.generation.to_string(),
        ),
        ("mrsm.executor.role".into(), "executor".into()),
    ]);
    assert!(!retired_container(&labels, "tpr-owned", &desired));
    assert!(!retired_container(&labels, "unrelated", &desired));
    labels.insert("mrsm.executor.role".into(), "relay".into());
    let relay = &record.settings.relays[0];
    labels.insert(
        "com.docker.compose.service".into(),
        format!("relay-{}-{}", record.key, relay_key(&relay.host)),
    );
    assert!(!retired_container(&labels, "tpr-owned", &desired));
    labels.insert(
        "com.docker.compose.service".into(),
        format!("relay-{}-{}", record.key, relay.key()),
    );
    assert!(
        retired_container(&labels, "tpr-owned", &desired),
        "The old per-port relay must not share the original-host alias with its replacement."
    );
    labels.insert("mrsm.executor.role".into(), "executor".into());
    labels.insert(
        "com.docker.compose.service".into(),
        format!("executor-{}", record.key),
    );
    desired.teams.values_mut().next().unwrap().retired = true;
    assert!(retired_container(&labels, "tpr-owned", &desired));
    let issuer = labels
        .insert(
            "mrsm.executor.issuer".into(),
            uuid::Uuid::new_v4().to_string(),
        )
        .unwrap();
    assert!(!retired_container(&labels, "tpr-owned", &desired));
    labels.insert("mrsm.executor.issuer".into(), issuer);
    labels.insert("mrsm.executor.role".into(), "unrelated".into());
    assert!(!retired_container(&labels, "tpr-owned", &desired));
    labels.insert("mrsm.executor.role".into(), "executor".into());
    desired.teams.values_mut().next().unwrap().retired = false;
    labels.insert(
        "mrsm.executor.generation".into(),
        uuid::Uuid::new_v4().to_string(),
    );
    assert!(retired_container(&labels, "tpr-owned", &desired));
}

#[test]
fn unsupported_or_unknown_engines_cannot_start_generated_executors() {
    struct Engine(&'static str);
    impl crate::dispatch::Host for Engine {
        fn run(&mut self, _: &crate::dispatch::CommandSpec) -> std::io::Result<i32> {
            panic!("preflight must not mutate Docker");
        }
        fn read_line(&mut self, _: &str) -> Option<String> {
            None
        }
        fn output(
            &mut self,
            spec: &crate::dispatch::CommandSpec,
            _: Option<std::time::Duration>,
        ) -> std::io::Result<crate::dispatch::CommandOutput> {
            assert_eq!(spec.args, ["version", "--format", "{{.Server.Version}}"]);
            Ok(crate::dispatch::CommandOutput {
                stdout: self.0.as_bytes().to_vec(),
                ..Default::default()
            })
        }
    }
    let (root, _) = fixture();
    assert!(preflight(root.path(), &[], &mut Engine("29.9.3\n")).is_ok());
    assert!(preflight(root.path(), &[], &mut Engine("28.0.0")).is_ok());
    assert!(preflight(root.path(), &[], &mut Engine("27.5.1")).is_err());
    assert!(preflight(root.path(), &[], &mut Engine("")).is_err());
    assert!(preflight(root.path(), &[], &mut Engine("unavailable")).is_err());
}

#[test]
fn network_reconciliation_checks_all_endpoints_then_removes_only_owned_resources_without_volumes() {
    use crate::dispatch::{CommandOutput, CommandSpec, Host};
    struct Docker {
        executor: String,
        api: String,
        network: String,
        executor_labels: serde_json::Value,
        api_labels: serde_json::Value,
        network_info: serde_json::Value,
        mutations: Vec<Vec<String>>,
    }
    impl Host for Docker {
        fn run(&mut self, _: &CommandSpec) -> std::io::Result<i32> {
            panic!("unbounded Docker call");
        }
        fn read_line(&mut self, _: &str) -> Option<String> {
            None
        }
        fn output(
            &mut self,
            spec: &CommandSpec,
            timeout: Option<std::time::Duration>,
        ) -> std::io::Result<CommandOutput> {
            assert!(timeout.is_some());
            assert_eq!(
                spec.env,
                [("TPR_DOCKER_SCOPE".to_owned(), "owned".to_owned())]
            );
            let args = &spec.args;
            let output = if args[0] == "ps" {
                self.executor.clone()
            } else if args[..2] == ["network", "ls"] {
                self.network.clone()
            } else if args[..2] == ["network", "inspect"] {
                self.network_info.to_string()
            } else if args[..2] == ["container", "inspect"] {
                let id = args.last().unwrap();
                if id == &self.executor {
                    self.executor_labels.to_string()
                } else if id == &self.api {
                    self.api_labels.to_string()
                } else {
                    panic!("an unrelated container was inspected")
                }
            } else {
                self.mutations.push(args.clone());
                String::new()
            };
            Ok(CommandOutput {
                stdout: output.into_bytes(),
                ..Default::default()
            })
        }
    }
    let (root, desired) = fixture();
    let record = desired.teams.values().next().unwrap();
    let executor = "a".repeat(64);
    let api = "b".repeat(64);
    let network = "c".repeat(64);
    let mut labels = serde_json::to_value(team_network(record, true)).unwrap()["labels"].clone();
    labels["com.docker.compose.project"] = json!("tpr-owned");
    labels["com.docker.compose.network"] = json!(format!("executor-{}", record.key));
    let mut executor_labels = serde_json::to_value(hardened(
        &"fixture".into(),
        record,
        "executor",
        8989,
    ))
    .unwrap()["labels"]
        .clone();
    executor_labels["com.docker.compose.project"] = json!("tpr-owned");
    executor_labels["com.docker.compose.service"] = json!(format!("executor-{}", record.key));
    let info = json!({"labels": labels, "driver": "bridge", "internal": false, "ipv6": false, "options": {},
            "endpoints": {executor.clone(): {}, api.clone(): {}}});
    let mut docker = Docker {
        executor: executor.clone(),
        api: api.clone(),
        network: network.clone(),
        executor_labels,
        api_labels: json!({"com.docker.compose.project": "another-project", "com.docker.compose.service": "api"}),
        network_info: info,
        mutations: Vec::new(),
    };
    let environment = vec![("TPR_DOCKER_SCOPE".into(), "owned".into())];
    assert!(retire(root.path(), "tpr-owned", &environment, &mut docker).is_err());
    assert!(
        docker.mutations.is_empty(),
        "A foreign endpoint must prevent all network mutations."
    );
    docker.api_labels["com.docker.compose.project"] = json!("tpr-owned");
    retire(root.path(), "tpr-owned", &environment, &mut docker).unwrap();
    assert_eq!(
        docker.mutations,
        vec![
            vec!["container", "rm", "--force", &executor],
            vec!["network", "disconnect", "--force", &network, &api],
            vec!["network", "rm", &network]
        ]
    );
    assert!(
        !docker
            .mutations
            .iter()
            .flatten()
            .any(|a| a == "-v" || a == "volume" || a == "prune")
    );
    docker.mutations.clear();
    docker.network_info["internal"] = json!(true);
    docker.network_info["options"] = json!({"com.docker.network.bridge.gateway_mode_ipv4": "isolated", "com.docker.network.bridge.gateway_mode_ipv6": "isolated"});
    retire(root.path(), "tpr-owned", &environment, &mut docker).unwrap();
    assert!(
        docker.mutations.is_empty(),
        "A matching network must be left running."
    );
}
