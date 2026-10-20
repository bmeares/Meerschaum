//! Wave 18 (DECISIONS item 23): a refused or unresponsive API server fails with one clear message.

use std::time::{Duration, Instant};

use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::RequestOptions;
use meerschaum_core::{Map, Value};

fn connector(host: &str, port: u16, extra: &[(&str, Value)]) -> APIConnector {
    let mut attributes = Map::new();
    attributes.insert("host".into(), Value::from(host));
    attributes.insert("port".into(), Value::Int(i64::from(port)));
    attributes.insert("protocol".into(), Value::from("http"));
    for (k, v) in extra {
        attributes.insert((*k).into(), v.clone());
    }
    APIConnector::new(Some("w18cn"), attributes).unwrap()
}

#[tokio::test]
async fn a_refused_connection_names_the_connector_and_the_cause() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let api = connector("127.0.0.1", port, &[]);
    let started = Instant::now();
    let error = api
        .get("/", RequestOptions::default())
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(
        error,
        format!(
            "api:w18cn: cannot connect to 127.0.0.1:{port} (connection refused). Is the API server running?"
        )
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn an_unroutable_host_is_bounded_by_connect_timeout() {
    let api = connector("10.255.255.1", 8000, &[("connect_timeout", Value::Int(1))]);
    assert_eq!(api.connect_timeout(), Duration::from_secs(1));
    let started = Instant::now();
    let error = api
        .get("/", RequestOptions::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.starts_with("api:w18cn: cannot connect to 10.255.255.1:8000 ("),
        "{error}"
    );
    assert!(
        error.contains("no response within 1 s") || error.contains("network unreachable"),
        "{error}"
    );
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[test]
fn the_default_connect_timeout_is_five_seconds() {
    if std::env::var_os(meerschaum_core::connect::CONNECT_TIMEOUT_ENV).is_some() {
        return;
    }
    assert_eq!(
        connector("localhost", 8000, &[]).connect_timeout(),
        Duration::from_secs(5)
    );
}
