//! The stack's Grafana sign-in: the auth-proxy network and addresses between the API server
//! and Grafana, and the environment that makes Grafana trust the API server's header.

use meerschaum_core::value::{Map, Value};

use crate::dispatch::Runtime;

use crate::native::warn;

/// The network between the stack's API server and its Grafana (`api:grafana`): Grafana
/// trusts the auth-proxy header from the API server's fixed address on it only.
pub const GRAFANA_PROXY_NETWORK: &str = "grafana_auth";

/// The addresses on `api:grafana:proxy_subnet` (an IPv4 network of at least eight addresses,
/// [`meerschaum_web::grafana::DEFAULT_PROXY_SUBNET`] by default): `(subnet, API server, Grafana)`,
/// the first two hosts after Docker's gateway.
pub fn grafana_proxy_addresses(
    grafana: &Map,
) -> Result<(String, std::net::Ipv4Addr, std::net::Ipv4Addr), String> {
    let subnet = meerschaum_web::grafana::setting_text(grafana, "proxy_subnet")
        .unwrap_or_else(|| meerschaum_web::grafana::DEFAULT_PROXY_SUBNET.to_string());
    let invalid = || {
        format!(
            "`api:grafana:proxy_subnet` ('{subnet}') must be an IPv4 network such as \
             '{}' (a prefix of /29 or shorter).",
            meerschaum_web::grafana::DEFAULT_PROXY_SUBNET
        )
    };
    let (address, prefix) = subnet.split_once('/').ok_or_else(invalid)?;
    let address: std::net::Ipv4Addr = address.trim().parse().map_err(|_| invalid())?;
    let prefix: u32 = prefix.trim().parse().map_err(|_| invalid())?;
    if !(8..=29).contains(&prefix) {
        return Err(invalid());
    }
    let mask = u32::MAX << (32 - prefix);
    let network = u32::from(address) & mask;
    Ok((
        format!("{}/{prefix}", std::net::Ipv4Addr::from(network)),
        std::net::Ipv4Addr::from(network + 2),
        std::net::Ipv4Addr::from(network + 3),
    ))
}

/// What the stack's Grafana is given for the API server to sign its users in (`api:grafana`).
/// `forced`: set over whatever the stack config has (the auth proxy and the server
/// administrator); `defaults`: set only when missing (served under `/grafana`). Anonymous access
/// is left as the stack config has it.
///
/// The auth-proxy header is trusted from `proxy_whitelist` (a string or a list), else from
/// `api_address` (the stack's API server on [`GRAFANA_PROXY_NETWORK`]); with neither, the auth
/// proxy is turned off (`warning` says what to set). Never a network range by default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrafanaSignIn {
    pub forced: Vec<(&'static str, String)>,
    pub defaults: Vec<(&'static str, String)>,
    pub warning: Option<String>,
}

pub fn grafana_sign_in_environment(
    grafana: &Map,
    admin_password: &str,
    api_address: Option<std::net::Ipv4Addr>,
) -> GrafanaSignIn {
    use meerschaum_web::grafana::setting_text;
    let whitelist = setting_text(grafana, "proxy_whitelist")
        .or_else(|| api_address.map(|address| address.to_string()));
    let mut forced = vec![
        (
            "GF_SECURITY_ADMIN_USER",
            setting_text(grafana, "admin_user").unwrap_or_else(|| "admin".into()),
        ),
        ("GF_SECURITY_ADMIN_PASSWORD", admin_password.to_string()),
        ("GF_AUTH_PROXY_HEADER_NAME", "X-WEBAUTH-USER".into()),
        ("GF_AUTH_PROXY_HEADER_PROPERTY", "username".into()),
        ("GF_AUTH_PROXY_AUTO_SIGN_UP", "true".into()),
    ];
    let mut warning = None;
    match whitelist {
        Some(whitelist) => {
            forced.push(("GF_AUTH_PROXY_ENABLED", "true".into()));
            forced.push(("GF_AUTH_PROXY_WHITELIST", whitelist));
        }
        None => {
            forced.push(("GF_AUTH_PROXY_ENABLED", "false".into()));
            forced.push(("GF_AUTH_PROXY_WHITELIST", String::new()));
            warning = Some(
                "Grafana's sign-in through Meerschaum is off: the stack has no `api` service, so \
                 the API server's address is not known. Set `api:grafana:proxy_whitelist` to the \
                 address Grafana sees the API server connect from (and `api:grafana:url` to how \
                 the API server reaches Grafana)."
                    .to_string(),
            );
        }
    }
    GrafanaSignIn {
        forced,
        defaults: vec![
            (
                "GF_SERVER_ROOT_URL",
                setting_text(grafana, "root_url")
                    .unwrap_or_else(|| "%(protocol)s://%(domain)s:%(http_port)s/grafana/".into()),
            ),
            ("GF_SERVER_SERVE_FROM_SUB_PATH", "true".into()),
        ],
        warning,
    }
}

/// Set `name` in a compose `environment` (a map or a `KEY=value` list); `force` replaces an
/// existing value.
pub(crate) fn set_environment(environment: &mut Value, name: &str, value: &str, force: bool) {
    match environment {
        Value::Map(map) => {
            if force || !map.contains_key(name) {
                map.insert(name.to_string(), Value::Str(value.to_string()));
            }
        }
        Value::List(items) => {
            let prefix = format!("{name}=");
            let exists = items
                .iter()
                .any(|item| matches!(item, Value::Str(s) if s.starts_with(&prefix) || s == name));
            if exists && !force {
                return;
            }
            items.retain(
                |item| !matches!(item, Value::Str(s) if s.starts_with(&prefix) || s == name),
            );
            items.push(Value::Str(format!("{name}={value}")));
        }
        other => {
            let mut map = Map::new();
            map.insert(name.to_string(), Value::Str(value.to_string()));
            *other = Value::Map(map);
        }
    }
}

/// A service's `networks` as a map (compose takes a list or a map), with `network` attached at
/// `address`.
fn attach_network(service: &mut Map, network: &str, address: std::net::Ipv4Addr) {
    let mut networks = match service.get("networks") {
        Some(Value::Map(map)) => map.clone(),
        Some(Value::List(items)) => items
            .iter()
            .map(|item| (item.py_str(), Value::Null))
            .collect(),
        _ => Map::new(),
    };
    let mut attachment = Map::new();
    attachment.insert("ipv4_address".into(), Value::Str(address.to_string()));
    networks.insert(network.to_string(), Value::Map(attachment));
    service.insert("networks".into(), Value::Map(networks));
}

/// The api service's `MRSM_PATCH` with `api:grafana:admin_password` (the stack's API server has
/// a root of its own, so it is given the password Grafana is started with). `None` when the
/// variable is not JSON.
pub(super) fn patched_api_patch(existing: Option<&str>, admin_password: &str) -> Option<String> {
    let mut patch: serde_json::Value = match existing.map(str::trim).filter(|s| !s.is_empty()) {
        Some(text) => serde_json::from_str(text).ok()?,
        None => serde_json::json!({}),
    };
    let object = patch.as_object_mut()?;
    let api = object
        .entry("api")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()?;
    let grafana = api
        .entry("grafana")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()?;
    grafana.insert("admin_password".into(), admin_password.into());
    serde_json::to_string_pretty(&patch).ok()
}

/// The compose file's config with what `api:grafana:enabled` needs: the `grafana` service
/// gets [`grafana_sign_in_environment`] (security settings forced, the others only when
/// missing); when the stack has an `api` service, both join [`GRAFANA_PROXY_NETWORK`] at fixed
/// addresses, the API server reaches `grafana` there (`extra_hosts`), and it is given the
/// administrator password. Other files and a stack without a `grafana` service are returned as
/// they are.
pub(super) fn with_grafana_sign_in(rt: &mut Runtime<'_>, keys: &[&str], mut value: Value) -> Value {
    if keys != ["stack", "docker-compose.yaml"] {
        return value;
    }
    let grafana = rt
        .config()
        .get_with(
            &["api", "grafana"],
            meerschaum_core::config::GetOptions {
                patch: true,
                substitute: true,
                write_missing: false,
                warn: false,
            },
        )
        .1
        .to_map();
    if !grafana.get("enabled").is_some_and(Value::truthy) {
        return value;
    }
    let has_grafana = value
        .get("services")
        .and_then(|services| services.get("grafana"))
        .is_some_and(|service| service.as_map().is_some());
    if !has_grafana {
        return value;
    }
    let admin_password = match meerschaum_web::grafana::setting_text(&grafana, "admin_password") {
        Some(password) => password,
        None => {
            let path = meerschaum_web::grafana::admin_password_path(
                &rt.config().paths().path("API_CONFIG_RESOURCES_PATH"),
            );
            match meerschaum_web::grafana::read_or_generate_admin_password(&path) {
                Ok(password) => password,
                Err(e) => {
                    warn(rt, &format!("{e}\nGrafana's sign-in was not configured."));
                    return value;
                }
            }
        }
    };
    if admin_password == meerschaum_web::grafana::DEFAULT_ADMIN_PASSWORD {
        warn(
            rt,
            "`api:grafana:admin_password` is Grafana's default ('admin'): set another one. \
             Grafana's sign-in was not configured.",
        );
        return value;
    }
    let has_api = value
        .get("services")
        .and_then(|services| services.get("api"))
        .is_some_and(|service| service.as_map().is_some());
    let addresses = if has_api {
        match grafana_proxy_addresses(&grafana) {
            Ok(addresses) => Some(addresses),
            Err(e) => {
                warn(rt, &e);
                None
            }
        }
    } else {
        None
    };
    let sign_in = grafana_sign_in_environment(
        &grafana,
        &admin_password,
        addresses.as_ref().map(|(_, api, _)| *api),
    );
    if let Some(warning) = &sign_in.warning {
        warn(rt, warning);
    }
    let Some(compose) = value.as_map_mut() else {
        return value;
    };
    if let Some((subnet, api_address, grafana_address)) = &addresses {
        let networks = compose
            .entry("networks".to_string())
            .or_insert_with(|| Value::Map(Map::new()));
        if !matches!(networks, Value::Map(_)) {
            *networks = Value::Map(Map::new());
        }
        if let Value::Map(networks) = networks {
            let mut pool = Map::new();
            pool.insert("subnet".into(), Value::Str(subnet.clone()));
            let mut ipam = Map::new();
            ipam.insert("config".into(), Value::List(vec![Value::Map(pool)]));
            let mut network = Map::new();
            network.insert("ipam".into(), Value::Map(ipam));
            networks.insert(GRAFANA_PROXY_NETWORK.into(), Value::Map(network));
        }
        if let Some(Value::Map(services)) = compose.get_mut("services") {
            if let Some(Value::Map(api)) = services.get_mut("api") {
                attach_network(api, GRAFANA_PROXY_NETWORK, *api_address);
                // `grafana` from the API server is Grafana's address on this network.
                let host = format!("grafana:{grafana_address}");
                let mut hosts = match api.get("extra_hosts") {
                    Some(Value::List(items)) => items.clone(),
                    Some(Value::Map(map)) => map
                        .iter()
                        .map(|(k, v)| Value::Str(format!("{k}:{}", v.py_str())))
                        .collect(),
                    _ => Vec::new(),
                };
                hosts.retain(|item| {
                    !matches!(item, Value::Str(s) if s.starts_with("grafana:") || s.starts_with("grafana="))
                });
                hosts.push(Value::Str(host));
                api.insert("extra_hosts".into(), Value::List(hosts));
            }
            if let Some(Value::Map(service)) = services.get_mut("grafana") {
                attach_network(service, GRAFANA_PROXY_NETWORK, *grafana_address);
            }
        }
    }
    if let Some(Value::Map(services)) = compose.get_mut("services") {
        if has_api && let Some(Value::Map(api)) = services.get_mut("api") {
            let environment = api
                .entry("environment".to_string())
                .or_insert_with(|| Value::Map(Map::new()));
            let existing = match environment {
                Value::Map(map) => map.get("MRSM_PATCH").map(Value::py_str),
                Value::List(items) => items.iter().find_map(|item| match item {
                    Value::Str(s) => s.strip_prefix("MRSM_PATCH=").map(str::to_string),
                    _ => None,
                }),
                _ => None,
            };
            match patched_api_patch(existing.as_deref(), &admin_password) {
                Some(patch) => set_environment(environment, "MRSM_PATCH", &patch, true),
                None => warn(
                    rt,
                    "The stack's `api` service has an `MRSM_PATCH` that is not JSON: set \
                     `api:grafana:admin_password` for it yourself.",
                ),
            }
        }
        if let Some(Value::Map(service)) = services.get_mut("grafana") {
            let environment = service
                .entry("environment".to_string())
                .or_insert_with(|| Value::Map(Map::new()));
            for (name, setting) in &sign_in.forced {
                set_environment(environment, name, setting, true);
            }
            for (name, setting) in &sign_in.defaults {
                set_environment(environment, name, setting, false);
            }
        }
    }
    value
}
