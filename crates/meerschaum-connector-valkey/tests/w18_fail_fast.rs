//! Wave 18 (DECISIONS item 23): a refused or unresponsive Valkey server fails with one clear
//! message. Offline: a closed local port and an unroutable address.

use std::time::{Duration, Instant};

use meerschaum_connector_valkey::ValkeyConnector;
use meerschaum_core::{Map, Value};

fn attributes(host: &str, port: i64, extra: &[(&str, Value)]) -> Map {
    let mut attributes = Map::new();
    attributes.insert("host".into(), Value::from(host));
    attributes.insert("port".into(), Value::Int(port));
    for (k, v) in extra {
        attributes.insert((*k).into(), v.clone());
    }
    attributes
}

#[tokio::test]
async fn a_refused_connection_names_the_connector_and_the_cause() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let started = Instant::now();
    let error = ValkeyConnector::connect("w18cn", attributes("127.0.0.1", i64::from(port), &[]))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(&format!(
            "valkey:w18cn: cannot connect to 127.0.0.1:{port} (connection refused). Is the Valkey server running?"
        )),
        "{error}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn an_unroutable_host_is_bounded_by_connect_timeout() {
    let started = Instant::now();
    let error = ValkeyConnector::connect(
        "w18cn",
        attributes("10.255.255.1", 6379, &[("connect_timeout", Value::Int(1))]),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("valkey:w18cn: cannot connect to 10.255.255.1:6379 ("),
        "{error}"
    );
    assert!(
        error.contains("no response within 1 s") || error.contains("network unreachable"),
        "{error}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn the_reachable_test_server_still_connects() {
    let url = std::env::var("MRSM_VALKEY_TEST_URL")
        .unwrap_or_else(|_| "redis://localhost:6399/12".to_string());
    let Ok(connector) = ValkeyConnector::from_uri("w18cn", &url).await else {
        eprintln!("SKIPPED: no Valkey server reachable at {url}");
        return;
    };
    assert!(connector.backend().ping().await.unwrap());
}

/// Rejected credentials: needs a server started with `--requirepass`; set
/// `MRSM_TEST_VALKEY_AUTH_PORT` to its (local) port. The password sent is wrong on purpose.
#[tokio::test]
async fn a_wrong_password_fails_with_an_auth_message() {
    let Some(port) = std::env::var("MRSM_TEST_VALKEY_AUTH_PORT")
        .ok()
        .and_then(|p| p.parse::<i64>().ok())
    else {
        eprintln!("Skipping: set MRSM_TEST_VALKEY_AUTH_PORT to a server with a password.");
        return;
    };
    let error = ValkeyConnector::connect(
        "w18cn",
        attributes(
            "127.0.0.1",
            port,
            &[("password", Value::from("w18cn-wrong-password"))],
        ),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("authentication failed for user 'default'"),
        "{error}"
    );
    assert!(!error.contains("w18cn-wrong-password"));
}
