//! Desired sibling services added by `mrsm stack up`, without a Docker socket in the API.

use meerschaum_core::execution::{Egress, Mode};
use meerschaum_core::{Map, Value};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::net::IpAddr;

use crate::executor::registry::{Desired, Registry};

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
    record: &crate::executor::registry::Registration,
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
    crate::executor::factory::stack::provision(
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

fn team_network(record: &crate::executor::registry::Registration, internal: bool) -> Value {
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

fn outbound_network(record: &crate::executor::registry::Registration) -> Value {
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
    record: &crate::executor::registry::Registration,
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
    let selected = crate::executor::factory::docker::selected(root, reference)?;
    let selected_image = selected
        .as_ref()
        .map(|selected| Value::Str(selected.image.clone()))
        .unwrap_or(image.clone());
    let compose = apply_desired_with_image(&directory, &desired, compose, &pins, &selected_image)?;
    crate::executor::factory::stack::render(root, &desired, reference, compose)
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
    // Mount the public desired directory outside `/meerschaum/executors` so Docker never
    // pre-creates `/meerschaum/executors` as `root:root` inside an existing `api_root` volume
    // (which would otherwise block `meerschaum` UID 1000 from writing the job catalog).
    let mount = value(json!({
        "type": "bind",
        "source": directory.display().to_string(),
        "target": crate::executor::registry::CONTAINER_DESIRED_DIR,
        "bind": {"create_host_path": false}
    }));
    let volumes = api
        .entry("volumes".into())
        .or_insert_with(|| Value::List(Vec::new()));
    match volumes {
        Value::List(volumes) => volumes.push(mount),
        _ => return Err("The API service's volumes must be a list.".into()),
    }
    let env = api
        .entry("environment".into())
        .or_insert_with(|| Value::Map(Map::new()));
    crate::native::os_sql_stack::set_environment(
        env,
        crate::executor::registry::DESIRED_DIR_ENV,
        crate::executor::registry::CONTAINER_DESIRED_DIR,
        true,
    );
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
mod tests;
