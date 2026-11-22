//! Tests of the stack's Grafana sign-in: the auth-proxy environment and the proxy network addresses.

use super::grafana::patched_api_patch;
use super::*;
use meerschaum_core::value::{Map, Value};
use std::collections::HashMap;

fn map(json: &str) -> Map {
    Value::from_json_str(json).unwrap().to_map()
}

fn env(list: &[(&'static str, String)]) -> HashMap<&'static str, String> {
    list.iter().cloned().collect()
}

#[test]
fn the_sign_in_environment_trusts_the_api_servers_address_only() {
    let api: std::net::Ipv4Addr = "10.213.71.2".parse().unwrap();
    let sign_in = grafana_sign_in_environment(&Map::new(), "generated", Some(api));
    let forced = env(&sign_in.forced);
    assert_eq!(forced["GF_AUTH_PROXY_ENABLED"], "true");
    assert_eq!(forced["GF_AUTH_PROXY_HEADER_NAME"], "X-WEBAUTH-USER");
    // Exactly the API server's address, never a network range.
    assert_eq!(forced["GF_AUTH_PROXY_WHITELIST"], "10.213.71.2");
    assert_eq!(forced["GF_SECURITY_ADMIN_USER"], "admin");
    assert_eq!(forced["GF_SECURITY_ADMIN_PASSWORD"], "generated");
    // Anonymous access is not Meerschaum's to change.
    assert!(!forced.keys().any(|k| k.starts_with("GF_AUTH_ANONYMOUS")));
    assert!(sign_in.warning.is_none());
    let defaults = env(&sign_in.defaults);
    assert_eq!(defaults["GF_SERVER_SERVE_FROM_SUB_PATH"], "true");
    assert!(defaults["GF_SERVER_ROOT_URL"].ends_with("/grafana/"));

    // Lists are joined with commas (Grafana's syntax), not a Python repr.
    let section = map(r#"{"root_url": "https://data.example.com/grafana/",
                "proxy_whitelist": ["10.1.2.3", "10.1.2.4"], "admin_user": "root"}"#);
    let sign_in = grafana_sign_in_environment(&section, "s3cret", Some(api));
    let forced = env(&sign_in.forced);
    assert_eq!(forced["GF_AUTH_PROXY_WHITELIST"], "10.1.2.3,10.1.2.4");
    assert_eq!(forced["GF_SECURITY_ADMIN_USER"], "root");
    assert_eq!(
        env(&sign_in.defaults)["GF_SERVER_ROOT_URL"],
        "https://data.example.com/grafana/"
    );
}

#[test]
fn without_an_address_the_auth_proxy_is_off() {
    let sign_in = grafana_sign_in_environment(&Map::new(), "generated", None);
    let forced = env(&sign_in.forced);
    assert_eq!(forced["GF_AUTH_PROXY_ENABLED"], "false");
    assert_eq!(forced["GF_AUTH_PROXY_WHITELIST"], "");
    assert!(sign_in.warning.unwrap().contains("proxy_whitelist"));
    // A configured whitelist is enough.
    let sign_in =
        grafana_sign_in_environment(&map(r#"{"proxy_whitelist": "192.0.2.7"}"#), "g", None);
    assert_eq!(env(&sign_in.forced)["GF_AUTH_PROXY_ENABLED"], "true");
}

#[test]
fn the_proxy_subnet_gives_fixed_addresses() {
    let (subnet, api, grafana) = grafana_proxy_addresses(&Map::new()).unwrap();
    assert_eq!(subnet, "10.213.71.0/29");
    assert_eq!(api.to_string(), "10.213.71.2");
    assert_eq!(grafana.to_string(), "10.213.71.3");
    let (subnet, api, _) =
        grafana_proxy_addresses(&map(r#"{"proxy_subnet": "10.9.8.77/28"}"#)).unwrap();
    assert_eq!(subnet, "10.9.8.64/28");
    assert_eq!(api.to_string(), "10.9.8.66");
    for bad in ["10.0.0.0/30", "10.0.0.0", "nonsense/24", "10.0.0.0/7"] {
        assert!(
            grafana_proxy_addresses(&map(&format!(r#"{{"proxy_subnet": "{bad}"}}"#))).is_err(),
            "{bad}"
        );
    }
}

#[test]
fn security_settings_override_the_stack_and_the_rest_is_kept() {
    let mut environment = Value::Map(map(
        r#"{"GF_AUTH_PROXY_WHITELIST": "172.16.0.0/12", "GF_SERVER_ROOT_URL": "mine",
                "GF_AUTH_ANONYMOUS_ENABLED": "true"}"#,
    ));
    set_environment(
        &mut environment,
        "GF_AUTH_PROXY_WHITELIST",
        "10.213.71.2",
        true,
    );
    set_environment(&mut environment, "GF_SERVER_ROOT_URL", "theirs", false);
    let m = environment.to_map();
    assert_eq!(m["GF_AUTH_PROXY_WHITELIST"].py_str(), "10.213.71.2");
    assert_eq!(m["GF_SERVER_ROOT_URL"].py_str(), "mine");
    assert_eq!(m["GF_AUTH_ANONYMOUS_ENABLED"].py_str(), "true");
    let mut list = Value::List(vec![Value::Str("GF_SECURITY_ADMIN_PASSWORD=admin".into())]);
    set_environment(&mut list, "GF_SECURITY_ADMIN_PASSWORD", "generated", true);
    assert_eq!(
        list,
        Value::List(vec![Value::Str(
            "GF_SECURITY_ADMIN_PASSWORD=generated".into()
        )])
    );
}

#[test]
fn the_api_server_is_given_the_password() {
    let patch = patched_api_patch(Some(r#"{"meerschaum": {"x": 1}}"#), "pw").unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&patch).unwrap();
    assert_eq!(parsed["api"]["grafana"]["admin_password"], "pw");
    assert_eq!(parsed["meerschaum"]["x"], 1);
    assert!(patched_api_patch(Some("not json"), "pw").is_none());
    assert!(patched_api_patch(None, "pw").is_some());
}
