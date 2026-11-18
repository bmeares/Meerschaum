//! Desired sibling services added by `mrsm stack up`, without a Docker socket in the API.

use meerschaum_core::execution::{Egress, Mode};
use meerschaum_core::{Map, Value};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::net::IpAddr;

use crate::executor_registry::{Desired, Registry};

/// Isolated bridge gateway mode (no host bridge address) was introduced in Engine 28.
pub const MIN_EXECUTOR_DOCKER_MAJOR: u64 = 28;

/// Executors use the same-release native full image independently of the API's image variant.
pub fn default_executor_image() -> String {
    format!(
        "bmeares/meerschaum:{}-full",
        meerschaum_core::python_guard::expected_version()
    )
}

fn executor_image(setting: Option<&Value>) -> Result<Value, String> {
    let reference = match setting {
        None => default_executor_image(),
        Some(Value::Str(reference)) => reference.clone(),
        _ => return Err("stack:executor:image must name a native Meerschaum container image; use the same-release full image.".into()),
    };
    let invalid = || {
        "stack:executor:image must be a nonempty container image reference without whitespace, variables or URL syntax.".to_string()
    };
    if reference.is_empty()
        || reference.len() > 512
        || reference.trim() != reference
        || reference.contains("://")
        || reference.starts_with(['-', '/', '.', ':', '@'])
        || reference.ends_with(['/', ':', '@'])
        || !reference.bytes().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, b'/' | b'.' | b'_' | b'-' | b':' | b'@')
        })
    {
        return Err(invalid());
    }
    let mut pieces = reference.split('@');
    let name = pieces.next().ok_or_else(invalid)?;
    if let Some(digest) = pieces.next()
        && (pieces.next().is_some()
            || !digest.strip_prefix("sha256:").is_some_and(|hash| {
                hash.len() == 64 && hash.bytes().all(|c| c.is_ascii_hexdigit())
            }))
    {
        return Err(invalid());
    }
    let (repository, tag) = name
        .rsplit_once(':')
        .filter(|(_, suffix)| !suffix.contains('/'))
        .map_or((name, None), |(repository, tag)| (repository, Some(tag)));
    let parts: Vec<_> = repository.split('/').collect();
    if repository.len() > 255
        || parts.iter().enumerate().any(|(index, part)| {
            let part = if index == 0 && parts.len() > 1 && part.contains(':') {
                match part.split_once(':') {
                    Some((host, port))
                        if !port.is_empty() && port.bytes().all(|c| c.is_ascii_digit()) =>
                    {
                        host
                    }
                    _ => return true,
                }
            } else {
                part
            };
            part.is_empty()
                || !part
                    .bytes()
                    .next()
                    .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
                || !part
                    .bytes()
                    .last()
                    .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
                || !part.bytes().all(|c| {
                    c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'.' | b'_' | b'-')
                })
        })
        || tag.is_some_and(|tag| {
            tag.len() > 128
                || !tag
                    .bytes()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphanumeric() || c == b'_')
        })
    {
        return Err(invalid());
    }
    let official = [
        "bmeares/meerschaum",
        "docker.io/bmeares/meerschaum",
        "index.docker.io/bmeares/meerschaum",
    ]
    .contains(&repository);
    if official && tag.is_some_and(|tag| tag == "slim" || tag.ends_with("-slim")) {
        return Err("stack:executor:image cannot use the wheel-only slim image. Select the same-release native full image or an administrator-provided native executor image.".into());
    }
    Ok(reference.into())
}

type RelayPins = BTreeMap<String, Vec<IpAddr>>;

fn routable_relay_pin(ip: IpAddr) -> bool {
    let ip = match ip {
        IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or(IpAddr::V6(ip), IpAddr::V4),
        ip => ip,
    };
    !ip.is_unspecified()
        && !ip.is_loopback()
        && !ip.is_multicast()
        && !matches!(ip, IpAddr::V4(ip) if ip.is_broadcast())
        && !matches!(ip, IpAddr::V6(ip) if ip.is_unicast_link_local())
}

const RESOLVE_RELAYS: &str = "--_mrsm-resolve-executor-relays";

/// Internal child entry, handled after mandatory process confinement and before any config,
/// plugin manifest, worker, or instance is opened. The parent enforces its process deadline.
pub fn resolve_if_requested(arguments: &[String]) -> Option<i32> {
    if arguments.first().map(String::as_str) != Some(RESOLVE_RELAYS) {
        return None;
    }
    let answer = arguments
        .get(1)
        .filter(|_| arguments.len() == 2)
        .ok_or_else(|| "Invalid declared relay resolution request.".to_string())
        .and_then(|argument| {
            serde_json::from_str::<Vec<String>>(argument)
                .map_err(|_| "Invalid declared relay hostnames.".to_string())
        })
        .and_then(resolve_hostnames)
        .and_then(|pins| {
            serde_json::to_vec(&pins)
                .map_err(|_| "Could not encode declared relay addresses.".to_string())
        });
    Some(match answer {
        Ok(bytes) if std::io::stdout().lock().write_all(&bytes).is_ok() => 0,
        _ => {
            eprintln!("Could not resolve declared relay hostnames on the administrator's host.");
            1
        }
    })
}

fn resolve_hostnames(hosts: Vec<String>) -> Result<RelayPins, String> {
    use std::net::ToSocketAddrs;
    if hosts.is_empty() || hosts.len() > meerschaum_core::execution::MAX_RELAYS {
        return Err("Relay resolution needs 1–64 hostnames.".into());
    }
    let mut answers = BTreeMap::new();
    for host in hosts {
        let mut settings = meerschaum_core::execution::Settings {
            relays: vec![meerschaum_core::execution::Relay {
                host: host.clone(),
                port: 1,
            }],
            ..Default::default()
        };
        settings.normalize().map_err(|e| e.to_string())?;
        if settings.relays[0].host != host || host.parse::<IpAddr>().is_ok() {
            return Err("Relay resolution needs normalized DNS hostnames.".into());
        }
        let mut addresses = BTreeSet::new();
        for address in (host.as_str(), 1)
            .to_socket_addrs()
            .map_err(|_| "A declared relay hostname could not be resolved.")?
        {
            addresses.insert(address.ip());
            if addresses.len() > 64 {
                return Err("A declared relay has too many DNS addresses.".into());
            }
        }
        if addresses.is_empty() {
            return Err("A declared relay hostname has no DNS addresses.".into());
        }
        answers.insert(host, addresses.into_iter().collect());
    }
    Ok(answers)
}

fn relay_groups(
    record: &crate::executor_registry::Registration,
) -> Result<BTreeMap<String, Vec<u16>>, String> {
    let mut groups: BTreeMap<String, Vec<u16>> = BTreeMap::new();
    for relay in &record.settings.relays {
        if relay.host.parse::<IpAddr>().is_ok() {
            if record.settings.egress != Egress::Open {
                return Err("An isolated executor's relay needs a DNS hostname so the connector can retain its original TLS certificate name. Numeric-IP endpoints require a DNS name or administrator-selected open networking.".into());
            }
            // Open networking already permits the original numeric destination directly.
            continue;
        }
        groups
            .entry(relay.host.clone())
            .or_default()
            .push(relay.port);
    }
    Ok(groups)
}

fn relay_key(host: &str) -> String {
    // One stable service per hostname, independent of the number/order of approved ports.
    meerschaum_core::execution::Relay {
        host: host.into(),
        port: 1,
    }
    .key()
}

/// Resolve outside the team network. A child process gives libc DNS a hard overall deadline;
/// a hanging platform resolver cannot leak one background resolver thread per stack operation.
fn resolve_relay_pins(
    directory: &std::path::Path,
    desired: &Desired,
    host: &mut dyn crate::dispatch::Host,
) -> Result<RelayPins, String> {
    let mut names = BTreeSet::new();
    for record in desired
        .teams
        .values()
        .filter(|r| !r.retired && r.settings.mode == Mode::Container)
    {
        names.extend(relay_groups(record)?.into_keys());
    }
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }
    let scratch = tempfile::Builder::new()
        .prefix("relay-resolution-")
        .tempdir_in(directory)
        .map_err(|_| "Could not prepare isolated relay resolution storage.")?;
    let plugins = scratch.path().join("plugins");
    meerschaum_core::team_scope::create_private_dir(&plugins)
        .map_err(|_| "Could not prepare isolated relay resolution plugins.")?;
    let mut environment: Vec<_> = meerschaum_core::process_env::vars()
        .into_iter()
        .filter(|(name, _)| {
            matches!(
                name.as_str(),
                "PATH"
                    | "PYTHONPATH"
                    | "LD_LIBRARY_PATH"
                    | "DYLD_LIBRARY_PATH"
                    | "SYSTEMROOT"
                    | "WINDIR"
                    | "PATHEXT"
                    | "VIRTUAL_ENV"
            )
        })
        .collect();
    environment.extend([
        ("HOME".into(), scratch.path().display().to_string()),
        ("MRSM_ROOT_DIR".into(), scratch.path().display().to_string()),
        ("MRSM_PLUGINS_DIR".into(), plugins.display().to_string()),
        ("MRSM_NO_UPDATE_CHECK".into(), "1".into()),
    ]);
    let mut pins = BTreeMap::new();
    for batch in names
        .iter()
        .collect::<Vec<_>>()
        .chunks(meerschaum_core::execution::MAX_RELAYS)
    {
        let (program, mut args) = meerschaum_core::self_exe::self_program();
        args.extend([RESOLVE_RELAYS.into(), json!(batch).to_string()]);
        let output = host
            .output(
                &crate::dispatch::CommandSpec {
                    program,
                    args,
                    env: environment.clone(),
                    env_clear: true,
                    quiet: true,
                    ..Default::default()
                },
                Some(std::time::Duration::from_secs(10)),
            )
            .map_err(
                |_| "Could not resolve declared relay destinations on the administrator's host.",
            )?;
        if output.code != 0 || output.timed_out || output.stdout.len() > 512 * 1024 {
            return Err("Declared relay DNS resolution failed or exceeded its 10-second deadline. Check the administrator host's DNS and regenerate the stack.".into());
        }
        let resolved: RelayPins = serde_json::from_slice(&output.stdout)
            .map_err(|_| "Invalid declared relay DNS response.")?;
        if resolved.keys().collect::<BTreeSet<_>>() != batch.iter().copied().collect() {
            return Err(
                "Declared relay DNS response does not match the approved hostnames.".into(),
            );
        }
        pins.extend(resolved);
    }
    if pins.keys().cloned().collect::<BTreeSet<_>>() != names {
        return Err("Declared relay DNS response does not match the approved hostnames.".into());
    }
    for addresses in pins.values() {
        if addresses.is_empty()
            || addresses.len() > 64
            || addresses.iter().any(|ip| !routable_relay_pin(*ip))
        {
            return Err("A declared relay resolved to an empty, excessive, loopback or non-routable address set. Use a hostname reachable from the relay container.".into());
        }
    }
    Ok(pins)
}

/// Reconcile the project selected by Compose's global options, using the generated model.
/// Alternate files/directories do not carry a verified executor deployment and are refused.
pub fn reconcile_up(
    root: &std::path::Path,
    project: &str,
    arguments: &[String],
    environment: &[(String, String)],
    host: &mut dyn crate::dispatch::Host,
) -> Result<(), String> {
    if Registry::new(root).desired()?.issuer.is_none() {
        return Ok(());
    }
    if let Some(project) = up_target(arguments, project)? {
        retire(root, &project, environment, host)?;
    }
    Ok(())
}

/// Provision reverse Factory identity only for an explicit, non-dry-run `stack up`.
/// This runs on the administrator's host, before Compose is regenerated or deployed.
pub fn provision_up(
    root: &std::path::Path,
    project: &str,
    arguments: &[String],
    environment: &[(String, String)],
    setting: Option<Value>,
    host: &mut dyn crate::dispatch::Host,
) -> Result<bool, String> {
    if up_target(arguments, project)?.is_none() {
        return Ok(false);
    }
    let desired = Registry::new(root).desired()?;
    let image = executor_image(setting.as_ref())?;
    crate::executor_factory_stack::provision(
        root,
        &desired,
        image
            .as_str()
            .ok_or("Invalid administrator executor image.")?,
        environment,
        host,
    )
}

fn up_target(arguments: &[String], default_project: &str) -> Result<Option<String>, String> {
    if arguments.iter().any(|a| a == "--dry-run") {
        return Ok(None);
    }
    let mut project = default_project.to_string();
    let mut alternate_model = false;
    let mut words = arguments.iter().map(String::as_str).skip(1);
    while let Some(word) = words.next() {
        match word {
            "--project-name" | "-p" => {
                project = words.next().ok_or("Missing Compose project name.")?.into();
            }
            "--file" | "-f" | "--project-directory" => {
                words.next().ok_or("Missing Compose model option value.")?;
                alternate_model = true;
            }
            "--profile" | "--env-file" | "--parallel" | "--ansi" | "--progress" => {
                words.next().ok_or("Missing Compose global option value.")?;
            }
            "--all-resources" | "--compatibility" | "--debug" | "--force" => {}
            "up" => {
                if alternate_model {
                    return Err("Container executors require the generated stack model. Put file and directory overrides in the stack configuration before running stack up.".into());
                }
                if project.is_empty() || project.starts_with('-') {
                    return Err("Invalid Compose project name.".into());
                }
                return Ok(Some(project));
            }
            _ if word.starts_with("--project-name=") => {
                project = word[15..].into();
            }
            _ if word.starts_with("-p") && !word.starts_with("--") && word.len() > 2 => {
                project = word[2..].into();
            }
            _ if word.starts_with("--file=")
                || word.starts_with("--project-directory=")
                || (word.starts_with("-f") && !word.starts_with("--") && word.len() > 2) =>
            {
                alternate_model = true;
            }
            _ if [
                "--profile=",
                "--env-file=",
                "--parallel=",
                "--ansi=",
                "--progress=",
            ]
            .iter()
            .any(|prefix| word.starts_with(prefix)) => {}
            _ if word.starts_with('-') => {
                return Err(
                    "Unrecognized Compose global option while executor reconciliation is enabled."
                        .into(),
                );
            }
            _ => return Ok(None),
        }
    }
    Ok(None)
}

/// Refuse an engine that cannot enforce the generated network policy before starting work.
pub fn preflight(
    root: &std::path::Path,
    environment: &[(String, String)],
    host: &mut dyn crate::dispatch::Host,
) -> Result<(), String> {
    let desired = Registry::new(root).desired()?;
    if !desired
        .teams
        .values()
        .any(|r| !r.retired && r.settings.mode == Mode::Container)
    {
        return Ok(());
    }
    let output = host
        .output(
            &crate::dispatch::CommandSpec {
                program: "docker".into(),
                args: vec![
                    "version".into(),
                    "--format".into(),
                    "{{.Server.Version}}".into(),
                ],
                env: environment.to_vec(),
                scrub_secrets: true,
                quiet: true,
                ..Default::default()
            },
            Some(std::time::Duration::from_secs(10)),
        )
        .map_err(|_| "Could not verify Docker's executor isolation support.")?;
    let major = std::str::from_utf8(&output.stdout)
        .ok()
        .and_then(|s| s.trim().split('.').next())
        .and_then(|s| s.parse::<u64>().ok());
    if output.code != 0 || output.timed_out || major.is_none_or(|v| v < MIN_EXECUTOR_DOCKER_MAJOR) {
        return Err(format!(
            "Container execution requires Docker Engine {MIN_EXECUTOR_DOCKER_MAJOR} or newer for isolated team bridges."
        ));
    }
    Ok(())
}

fn team_network(record: &crate::executor_registry::Registration, internal: bool) -> Value {
    let mut network = value(json!({"driver": "bridge", "internal": internal, "enable_ipv6": false,
        "labels": {"mrsm.executor.managed": "true", "mrsm.executor.issuer": record.verifier.issuer.to_string(),
            "mrsm.executor.team": record.team_id.to_string(), "mrsm.executor.role": "network", "mrsm.executor.network": "team"}})).to_map();
    if internal {
        // `internal` alone still assigns a bridge address reachable on the Docker host.
        // https://docs.docker.com/engine/network/port-publishing/#gateway-modes
        network.insert(
            "driver_opts".into(),
            value(json!({
                "com.docker.network.bridge.gateway_mode_ipv4": "isolated",
                "com.docker.network.bridge.gateway_mode_ipv6": "isolated"
            })),
        );
    }
    Value::Map(network)
}

fn outbound_network(record: &crate::executor_registry::Registration) -> Value {
    let mut network = team_network(record, false).to_map();
    map_entry(&mut network, "labels")
        .expect("constructed labels")
        .insert("mrsm.executor.network".into(), "outbound".into());
    Value::Map(network)
}

#[derive(Debug, PartialEq, Eq)]
enum NetworkChoice {
    Ignore,
    Remove,
    Expect { internal: bool },
}

fn network_choice(
    labels: &std::collections::BTreeMap<String, String>,
    project: &str,
    desired: &Desired,
) -> NetworkChoice {
    let get = |key: &str| labels.get(key).map(String::as_str);
    if get("com.docker.compose.project") != Some(project)
        || get("mrsm.executor.managed") != Some("true")
        || get("mrsm.executor.role") != Some("network")
    {
        return NetworkChoice::Ignore;
    }
    let issuer = get("mrsm.executor.issuer").and_then(|s| uuid::Uuid::parse_str(s).ok());
    if issuer.is_none() || issuer != desired.issuer {
        return NetworkChoice::Ignore;
    }
    let Some(id) = get("mrsm.executor.team").and_then(|s| uuid::Uuid::parse_str(s).ok()) else {
        return NetworkChoice::Ignore;
    };
    let key = format!("team-{}", id.simple());
    let prefix = match get("mrsm.executor.network") {
        Some("team") => "executor",
        Some("outbound") => "outbound",
        _ => return NetworkChoice::Ignore,
    };
    if get("com.docker.compose.network") != Some(format!("{prefix}-{key}").as_str()) {
        return NetworkChoice::Ignore;
    }
    let Some(record) = desired
        .teams
        .get(&id)
        .filter(|r| !r.retired && r.settings.mode == Mode::Container)
    else {
        return NetworkChoice::Remove;
    };
    if prefix == "outbound" {
        if record.settings.egress == Egress::Proxy || !record.settings.relays.is_empty() {
            NetworkChoice::Expect { internal: false }
        } else {
            NetworkChoice::Remove
        }
    } else {
        NetworkChoice::Expect {
            internal: record.settings.egress != Egress::Open,
        }
    }
}

#[derive(serde::Deserialize)]
struct NetworkInfo {
    labels: std::collections::BTreeMap<String, String>,
    driver: String,
    internal: bool,
    ipv6: bool,
    options: Option<std::collections::BTreeMap<String, String>>,
    endpoints: Option<std::collections::BTreeMap<String, serde_json::Value>>,
}

fn network_matches(network: &NetworkInfo, internal: bool) -> bool {
    network.driver == "bridge"
        && network.internal == internal
        && !network.ipv6
        && (!internal
            || [
                "com.docker.network.bridge.gateway_mode_ipv4",
                "com.docker.network.bridge.gateway_mode_ipv6",
            ]
            .iter()
            .all(|key| {
                network
                    .options
                    .as_ref()
                    .and_then(|o| o.get(*key))
                    .is_some_and(|v| v == "isolated")
            }))
}

fn value(json: serde_json::Value) -> Value {
    Value::from_json_str(&json.to_string()).expect("constructed Compose values")
}

fn map_entry<'a>(map: &'a mut Map, key: &str) -> Result<&'a mut Map, String> {
    let entry = map
        .entry(key.into())
        .or_insert_with(|| Value::Map(Map::new()));
    match entry {
        Value::Map(map) => Ok(map),
        _ => Err(format!(
            "The stack's '{key}' must be a mapping to provision team executors."
        )),
    }
}

fn network(service: &mut Map, name: &str) -> Result<(), String> {
    let networks = service
        .entry("networks".into())
        .or_insert_with(|| Value::Map(Map::new()));
    match networks {
        Value::List(names) => {
            if !names.iter().any(|v| v.as_str() == Some(name)) {
                names.push(name.into());
            }
        }
        Value::Map(names) => {
            names.entry(name.into()).or_insert(Value::Null);
        }
        _ => return Err("A stack service's networks must be a list or mapping.".into()),
    }
    Ok(())
}

fn generated(map: &mut Map, name: &str, spec: Value) -> Result<(), String> {
    if map.contains_key(name) {
        return Err(format!(
            "The generated executor resource '{name}' conflicts with an existing stack resource."
        ));
    }
    map.insert(name.into(), spec);
    Ok(())
}

fn hardened(
    image: &Value,
    record: &crate::executor_registry::Registration,
    role: &str,
    port: u16,
) -> Map {
    // Factory authority lives below /run/secrets. Its ancestry must remain owned by
    // root even before a response key is provisioned; writable state is /tmp and
    // the dedicated /meerschaum volume. Network helpers do not load this authority.
    let runtime_tmpfs = if role == "executor" {
        "/run:rw,nosuid,nodev,size=16m,uid=0,gid=0,mode=0755"
    } else {
        "/run:rw,nosuid,nodev,size=16m,uid=1000,gid=1000,mode=0755"
    };
    value(json!({
        "image": image, "user": "1000:1000", "read_only": true,
        "cap_drop": ["ALL"], "security_opt": ["no-new-privileges:true"], "ipc": "private",
        "init": true, "restart": "unless-stopped", "stop_grace_period": "30s",
        "pids_limit": 128, "mem_limit": 256 * 1024 * 1024u64, "memswap_limit": 256 * 1024 * 1024u64, "cpus": 1.0,
        "tmpfs": ["/tmp:rw,nosuid,nodev,size=64m,uid=1000,gid=1000,mode=1777", runtime_tmpfs],
        "environment": {
            "MRSM_ROOT_DIR": "/meerschaum", "HOME": "/meerschaum",
            "MRSM_NO_UPDATE_CHECK": "1", "PYTHONPYCACHEPREFIX": "/meerschaum/cache/pycache",
            "PIP_CACHE_DIR": "/meerschaum/cache/pip", "UV_CACHE_DIR": "/meerschaum/cache/uv",
            "XDG_CACHE_HOME": "/meerschaum/cache", "CARGO_HOME": "/meerschaum/cache/cargo", "MRSM_SRC": "/meerschaum/src",
            "PATH": "/usr/local/cargo/bin:/usr/local/bin:/usr/bin:/meerschaum/.local/bin"
        },
        "labels": {"mrsm.executor.managed": "true", "mrsm.executor.team": record.team_id.to_string(),
            "mrsm.executor.issuer": record.verifier.issuer.to_string(),
            "mrsm.executor.generation": record.verifier.generation.to_string(), "mrsm.executor.role": role},
        "healthcheck": {"test": ["CMD", "/usr/bin/python3", "-I", "-S", "-c", format!("import socket; socket.create_connection(('127.0.0.1', {port}), 3).close()")], "interval": "15s", "timeout": "5s", "retries": 3}
    })).to_map()
}

/// Patch an already resolved Compose model. Administrator-installed reverse identities
/// are projected separately; Main API request signing keys always stay local.
pub fn apply(root: &std::path::Path, compose: Value) -> Result<Value, String> {
    apply_with_image(root, compose, None)
}

/// The optional global stack:executor:image is an administrator's native-image assertion;
/// executable provenance and signed release matching are checked again before runtime readiness.
pub fn apply_with_image(
    root: &std::path::Path,
    compose: Value,
    setting: Option<Value>,
) -> Result<Value, String> {
    let image = executor_image(setting.as_ref())?;
    let registry = Registry::new(root);
    let desired = registry.desired()?;
    let directory = registry.desired_dir();
    meerschaum_core::team_scope::create_private_dir(&directory)
        .map_err(|_| "Could not prepare public executor deployment storage.")?;
    let pins = resolve_relay_pins(&directory, &desired, &mut crate::dispatch::SystemHost)?;
    let reference = image
        .as_str()
        .ok_or("Invalid administrator executor image.")?;
    let selected = crate::executor_factory_docker::selected(root, reference)?;
    let selected_image = selected
        .as_ref()
        .map(|selected| Value::Str(selected.image.clone()))
        .unwrap_or(image.clone());
    let compose = apply_desired_with_image(&directory, &desired, compose, &pins, &selected_image)?;
    crate::executor_factory_stack::render(root, &desired, reference, compose)
}

fn retired_container(
    labels: &std::collections::BTreeMap<String, String>,
    project: &str,
    desired: &Desired,
) -> bool {
    let get = |key: &str| labels.get(key).map(String::as_str);
    if get("com.docker.compose.project") != Some(project)
        || get("mrsm.executor.managed") != Some("true")
    {
        return false;
    }
    let Some(issuer) = get("mrsm.executor.issuer").and_then(|v| uuid::Uuid::parse_str(v).ok())
    else {
        return false;
    };
    if Some(issuer) != desired.issuer {
        return false;
    }
    let Some(id) = get("mrsm.executor.team").and_then(|v| uuid::Uuid::parse_str(v).ok()) else {
        return false;
    };
    let Some(generation) =
        get("mrsm.executor.generation").and_then(|v| uuid::Uuid::parse_str(v).ok())
    else {
        return false;
    };
    let role = get("mrsm.executor.role");
    if !matches!(role, Some("executor" | "proxy" | "relay")) {
        return false;
    }
    let Some(record) = desired.teams.get(&id) else {
        return true;
    };
    if record.retired
        || record.settings.mode != Mode::Container
        || record.verifier.generation != generation
    {
        return true;
    }
    let service = get("com.docker.compose.service");
    match role {
        Some("executor") => service != Some(format!("executor-{}", record.key).as_str()),
        Some("proxy") => {
            record.settings.egress != Egress::Proxy
                || service != Some(format!("egress-{}", record.key).as_str())
        }
        Some("relay") => !record.settings.relays.iter().any(|r| {
            r.host.parse::<IpAddr>().is_err()
                && service == Some(format!("relay-{}-{}", record.key, relay_key(&r.host)).as_str())
        }),
        _ => false,
    }
}

/// Reconcile only this stack's explicitly labelled executor containers. Never use Compose's
/// broad orphan removal, never remove a volume, and never inspect container environments.
pub fn retire(
    root: &std::path::Path,
    project: &str,
    environment: &[(String, String)],
    host: &mut dyn crate::dispatch::Host,
) -> Result<(), String> {
    use crate::dispatch::CommandSpec;
    let desired = Registry::new(root).desired()?;
    if desired.issuer.is_none() {
        return Ok(());
    }
    let capture =
        |host: &mut dyn crate::dispatch::Host, args: Vec<String>| -> Result<Vec<u8>, String> {
            let output = host
                .output(
                    &CommandSpec {
                        program: "docker".into(),
                        args,
                        env: environment.to_vec(),
                        scrub_secrets: true,
                        quiet: true,
                        ..Default::default()
                    },
                    Some(std::time::Duration::from_secs(30)),
                )
                .map_err(|_| "Could not inspect this stack's executor containers.")?;
            if output.code != 0 || output.timed_out || output.stdout.len() > 1024 * 1024 {
                return Err("Could not reconcile this stack's executor containers.".into());
            }
            Ok(output.stdout)
        };
    let ids = capture(
        host,
        vec![
            "ps".into(),
            "-aq".into(),
            "--no-trunc".into(),
            "--filter".into(),
            format!("label=com.docker.compose.project={project}"),
            "--filter".into(),
            "label=mrsm.executor.managed=true".into(),
        ],
    )?;
    let ids = std::str::from_utf8(&ids).map_err(|_| "Invalid executor container identities.")?;
    if ids.lines().count() > 1024 {
        return Err("The stack exceeds its executor reconciliation limit.".into());
    }
    for id in ids.lines().filter(|s| !s.is_empty()) {
        if id.len() != 64 || !id.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err("Invalid executor container identity.".into());
        }
        let metadata = capture(
            host,
            vec![
                "container".into(),
                "inspect".into(),
                "--format".into(),
                "{{json .Config.Labels}}".into(),
                id.into(),
            ],
        )?;
        let labels =
            serde_json::from_slice(&metadata).map_err(|_| "Invalid executor ownership labels.")?;
        if retired_container(&labels, project, &desired) {
            // No -v: private state survives retirement and replacement.
            capture(
                host,
                vec!["container".into(), "rm".into(), "--force".into(), id.into()],
            )?;
        }
    }
    let networks = capture(
        host,
        vec![
            "network".into(),
            "ls".into(),
            "--no-trunc".into(),
            "--format".into(),
            "{{.ID}}".into(),
            "--filter".into(),
            format!("label=com.docker.compose.project={project}"),
            "--filter".into(),
            "label=mrsm.executor.managed=true".into(),
        ],
    )?;
    let networks =
        std::str::from_utf8(&networks).map_err(|_| "Invalid executor network identities.")?;
    if networks.lines().count() > 1024 {
        return Err("The stack exceeds its executor network reconciliation limit.".into());
    }
    for id in networks.lines().filter(|s| !s.is_empty()) {
        if id.len() != 64 || !id.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err("Invalid executor network identity.".into());
        }
        let projection = r#"{"labels":{{json .Labels}},"driver":{{json .Driver}},"internal":{{json .Internal}},"ipv6":{{json .EnableIPv6}},"options":{{json .Options}},"endpoints":{{json .Containers}}}"#;
        let info = capture(
            host,
            vec![
                "network".into(),
                "inspect".into(),
                "--format".into(),
                projection.into(),
                id.into(),
            ],
        )?;
        let info: NetworkInfo =
            serde_json::from_slice(&info).map_err(|_| "Invalid executor network metadata.")?;
        match network_choice(&info.labels, project, &desired) {
            NetworkChoice::Ignore => continue,
            NetworkChoice::Expect { internal } if network_matches(&info, internal) => continue,
            _ => {}
        }
        if info.driver != "bridge" {
            return Err(
                "An owned executor network has an unsupported driver; reconcile it explicitly."
                    .into(),
            );
        }
        // Validate every endpoint before touching this network. Only the stack's api/db and
        // this team's managed containers may be disconnected; never a bystander's endpoint.
        let mut remove = Vec::new();
        let mut detach = Vec::new();
        for container in info.endpoints.as_ref().into_iter().flat_map(|e| e.keys()) {
            if container.len() != 64 || !container.bytes().all(|c| c.is_ascii_hexdigit()) {
                return Err("Invalid executor network endpoint identity.".into());
            }
            let metadata = capture(
                host,
                vec![
                    "container".into(),
                    "inspect".into(),
                    "--format".into(),
                    "{{json .Config.Labels}}".into(),
                    container.clone(),
                ],
            )?;
            let labels: std::collections::BTreeMap<String, String> =
                serde_json::from_slice(&metadata)
                    .map_err(|_| "Invalid network endpoint ownership labels.")?;
            let get = |key: &str| labels.get(key).map(String::as_str);
            if get("com.docker.compose.project") != Some(project) {
                return Err("An executor network has an endpoint outside this stack; no network endpoints were changed.".into());
            }
            if matches!(get("com.docker.compose.service"), Some("api" | "db")) {
                detach.push(container.clone());
            } else if get("mrsm.executor.managed") == Some("true")
                && labels.get("mrsm.executor.issuer") == info.labels.get("mrsm.executor.issuer")
                && labels.get("mrsm.executor.team") == info.labels.get("mrsm.executor.team")
                && matches!(
                    get("mrsm.executor.role"),
                    Some("executor" | "proxy" | "relay")
                )
            {
                remove.push(container.clone());
            } else {
                return Err("An executor network has an unmanaged endpoint; no network endpoints were changed.".into());
            }
        }
        for container in remove {
            capture(
                host,
                vec!["container".into(), "rm".into(), "--force".into(), container],
            )?;
        }
        for container in detach {
            capture(
                host,
                vec![
                    "network".into(),
                    "disconnect".into(),
                    "--force".into(),
                    id.into(),
                    container,
                ],
            )?;
        }
        capture(host, vec!["network".into(), "rm".into(), id.into()])?;
    }
    Ok(())
}

#[cfg(test)]
fn apply_desired(
    directory: &std::path::Path,
    desired: &Desired,
    compose: Value,
    pins: &RelayPins,
) -> Result<Value, String> {
    apply_desired_with_image(directory, desired, compose, pins, &executor_image(None)?)
}

fn apply_desired_with_image(
    directory: &std::path::Path,
    desired: &Desired,
    compose: Value,
    pins: &RelayPins,
    image: &Value,
) -> Result<Value, String> {
    let Value::Map(mut compose) = compose else {
        return Err("The stack Compose configuration must be a mapping.".into());
    };
    let mut services = map_entry(&mut compose, "services")?.clone();
    let has_containers = desired
        .teams
        .values()
        .any(|r| !r.retired && r.settings.mode == Mode::Container);
    let api = services.get_mut("api").and_then(|v| match v {
        Value::Map(m) => Some(m),
        _ => None,
    });
    let Some(api) = api else {
        return if has_containers {
            Err("Container executors require the stack's 'api' service.".into())
        } else {
            Ok(Value::Map(compose))
        };
    };
    let mount = value(
        json!({"type": "bind", "source": directory.display().to_string(), "target": "/meerschaum/executors/desired", "bind": {"create_host_path": false}}),
    );
    let volumes = api
        .entry("volumes".into())
        .or_insert_with(|| Value::List(Vec::new()));
    match volumes {
        Value::List(volumes) => volumes.push(mount),
        _ => return Err("The API service's volumes must be a list.".into()),
    }
    let mut networks = map_entry(&mut compose, "networks")?.clone();
    let mut volumes = map_entry(&mut compose, "volumes")?.clone();
    let mut secrets = map_entry(&mut compose, "secrets")?.clone();
    for record in desired
        .teams
        .values()
        .filter(|r| r.settings.mode == Mode::Container)
    {
        if record.key != format!("team-{}", record.team_id.simple())
            || record.verifier.team_id != record.team_id
        {
            return Err("An executor deployment record has an inconsistent team identity.".into());
        }
        let root_volume = format!("executor-{}-root", record.key);
        let control_volume = format!("executor-{}-control", record.key);
        // Retired volumes stay declared and are never pruned by reconciliation.
        generated(&mut volumes, &root_volume, value(json!({})))?;
        generated(&mut volumes, &control_volume, value(json!({})))?;
        if record.retired {
            continue;
        }
        record
            .verifier
            .validate()
            .map_err(|_| "The executor bootstrap belongs to a different release or is invalid.")?;
        let mut settings = record.settings.clone();
        settings.normalize().map_err(|e| e.to_string())?;
        let name = format!("executor-{}", record.key);
        let outbound = format!("outbound-{}", record.key);
        let proxy_name = format!("egress-{}", record.key);
        let relay_groups = relay_groups(record)?;
        let secret = format!("executor-{}-verifier", record.key);
        generated(
            &mut networks,
            &name,
            team_network(record, settings.egress != Egress::Open),
        )?;
        generated(
            &mut secrets,
            &secret,
            value(
                json!({"file": directory.join(format!("{}.json", record.key)).display().to_string()}),
            ),
        )?;
        let mut executor = hardened(image, record, "executor", 8989);
        let control = meerschaum_core::team_scope::source_namespace::FACTORY_CONTROL_ROOT;
        executor.insert("working_dir".into(), control.into());
        let environment = map_entry(&mut executor, "environment")?;
        environment.insert("HOME".into(), control.into());
        environment.insert(
            "PATH".into(),
            "/usr/local/cargo/bin:/usr/local/bin:/usr/bin".into(),
        );
        if settings.egress != Egress::Open {
            executor.insert(
                "sysctls".into(),
                value(json!({"net.ipv6.conf.all.disable_ipv6": "1", "net.ipv6.conf.default.disable_ipv6": "1"})),
            );
        }
        executor.insert(
            "command".into(),
            value(json!([
                "start", "executor", "--host", "0.0.0.0", "--port", "8989"
            ])),
        );
        executor.insert(
            "mem_limit".into(),
            Value::Int(
                settings
                    .limits
                    .memory_bytes()
                    .map_err(|e| e.to_string())?
                    .try_into()
                    .map_err(|_| "Executor memory exceeds Compose's signed integer range.")?,
            ),
        );
        executor.insert("memswap_limit".into(), executor["mem_limit"].clone());
        executor.insert("cpus".into(), Value::Float(settings.limits.cpus));
        executor.insert("pids_limit".into(), Value::Int(settings.limits.pids.into()));
        executor.insert(
            "volumes".into(),
            value(json!([
                format!("{root_volume}:/meerschaum"),
                format!(
                    "{control_volume}:{}",
                    meerschaum_core::team_scope::source_namespace::FACTORY_CONTROL_ROOT
                )
            ])),
        );
        executor.insert(
            "secrets".into(),
            value(json!([{"source": secret, "target": "mrsm-executor.json"}])),
        );
        executor.insert("networks".into(), value(json!([name])));
        map_entry(&mut executor, "environment")?.insert(
            "MRSM_EXECUTOR_BOOTSTRAP".into(),
            "/run/secrets/mrsm-executor.json".into(),
        );
        if let Some(runtime) = &settings.runtime {
            executor.insert("runtime".into(), runtime.clone().into());
        }
        network(
            services
                .get_mut("api")
                .and_then(|v| match v {
                    Value::Map(m) => Some(m),
                    _ => None,
                })
                .expect("checked API"),
            &name,
        )?;
        // Grafana and Valkey keep their existing networks. Only the database service is joined.
        if let Some(Value::Map(database)) = services.get_mut("db") {
            network(database, &name)?;
        }
        if settings.egress == Egress::Proxy || !settings.relays.is_empty() {
            generated(&mut networks, &outbound, outbound_network(record))?;
        }
        if settings.egress == Egress::Proxy {
            let mut proxy = hardened(image, record, "proxy", 3128);
            proxy.insert(
                "command".into(),
                value(json!([
                    "start",
                    "executor",
                    "--port",
                    "3128",
                    "-P",
                    "{\"role\":\"proxy\"}"
                ])),
            );
            proxy.insert("networks".into(), value(json!([name, outbound])));
            if let Some(Value::List(tmpfs)) = proxy.get_mut("tmpfs") {
                tmpfs.push(
                    "/meerschaum:rw,nosuid,nodev,size=64m,uid=1000,gid=1000,mode=0700".into(),
                );
            }
            generated(&mut services, &proxy_name, Value::Map(proxy))?;
            let environment = map_entry(&mut executor, "environment")?;
            for variable in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"] {
                environment.insert(variable.into(), format!("http://{proxy_name}:3128").into());
            }
            for variable in ["NO_PROXY", "no_proxy"] {
                let mut names = vec!["api".to_string(), "db".to_string(), name.clone()];
                names.extend(relay_groups.keys().cloned());
                environment.insert(variable.into(), names.join(",").into());
            }
            executor.insert(
                "depends_on".into(),
                value(json!({proxy_name.clone(): {"condition": "service_started"}})),
            );
        }
        for (hostname, ports) in relay_groups {
            if ["api", "db", name.as_str(), proxy_name.as_str()].contains(&hostname.as_str()) {
                return Err("A declared relay hostname conflicts with a reserved team-network service name. Use its external DNS hostname.".into());
            }
            let relay_name = format!("relay-{}-{}", record.key, relay_key(&hostname));
            let destination = meerschaum_server::executor::egress::PinnedRelay {
                host: hostname.clone(),
                ports,
                addresses: pins
                    .get(&hostname)
                    .cloned()
                    .ok_or("A declared relay has no deployment-host DNS pins.")?,
            };
            destination.validate()?;
            let mut service = hardened(image, record, "relay", destination.ports[0]);
            let parameters = json!({"role": "relay", "destination": destination}).to_string();
            service.insert(
                "command".into(),
                value(json!(["start", "executor", "-P", parameters])),
            );
            service.insert(
                "networks".into(),
                value(json!({
                    name.clone(): {"aliases": [hostname]}, outbound.clone(): {}
                })),
            );
            // The namespace-local setting permits declared ports below 1024 without capabilities.
            service.insert(
                "sysctls".into(),
                value(json!({"net.ipv4.ip_unprivileged_port_start": "0"})),
            );
            map_entry(&mut executor, "depends_on")?.insert(
                relay_name.clone(),
                value(json!({"condition": "service_started"})),
            );
            if let Some(Value::List(tmpfs)) = service.get_mut("tmpfs") {
                tmpfs.push(
                    "/meerschaum:rw,nosuid,nodev,size=64m,uid=1000,gid=1000,mode=0700".into(),
                );
            }
            generated(&mut services, &relay_name, Value::Map(service))?;
        }
        generated(&mut services, &name, Value::Map(executor))?;
    }
    compose.insert("services".into(), Value::Map(services));
    compose.insert("networks".into(), Value::Map(networks));
    compose.insert("volumes".into(), Value::Map(volumes));
    compose.insert("secrets".into(), Value::Map(secrets));
    Ok(Value::Map(compose))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor_registry::Registry;

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
        team.attributes.insert("execution".into(), value(json!({"mode": "container", "relays": [{"host": "database.example", "port": 5432}]})));
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
                .any(|mount| mount.as_str().is_some_and(|mount| mount.ends_with(
                    meerschaum_core::team_scope::source_namespace::FACTORY_CONTROL_ROOT
                )))
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
        let custom =
            executor_image(Some(&"registry.example:5000/mrsm/native:fixture".into())).unwrap();
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
        let defaults = meerschaum_core::config::default_config("", 1, false);
        assert_eq!(
            defaults
                .get("stack")
                .and_then(|stack| stack.get("executor"))
                .and_then(|executor| executor.get("image"))
                .and_then(Value::as_str),
            Some(default_executor_image().as_str())
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
    fn network_reconciliation_checks_all_endpoints_then_removes_only_owned_resources_without_volumes()
     {
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
        let mut labels =
            serde_json::to_value(team_network(record, true)).unwrap()["labels"].clone();
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
}
