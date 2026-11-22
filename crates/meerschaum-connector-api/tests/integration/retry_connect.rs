//! `retry_connect()` for an API instance (`ApiRetryProbe`): the chaining check, the login, the
//! two `enforce_*` switches and what an attempt remembers, against a scripted server.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::{Canned, MockServer};
use meerschaum_connector::retry::{
    CollectEvents, RetryOptions, RetryOutcome, SleepWaiter, retry_connect,
};
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::retry::ApiRetryProbe;
use meerschaum_core::{Map, Value};

const PASSWORD: &str = "retry-not-a-secret";

fn json(status: u16, body: &str) -> Canned {
    Canned {
        status,
        content_type: "application/json".into(),
        body: body.as_bytes().to_vec(),
    }
}

fn login_ok() -> Canned {
    json(
        200,
        r#"{"access_token": "jwt", "token_type": "bearer", "expires": "2099-01-01T00:00:00"}"#,
    )
}

fn login_refused() -> Canned {
    json(401, r#"{"detail": "Incorrect username or password"}"#)
}

fn connector(port: u16) -> Arc<APIConnector> {
    let mut attrs = Map::new();
    attrs.insert("host".into(), Value::Str("127.0.0.1".into()));
    attrs.insert("port".into(), Value::Int(i64::from(port)));
    attrs.insert("protocol".into(), Value::Str("http".into()));
    attrs.insert("username".into(), Value::Str("scratch".into()));
    attrs.insert("password".into(), Value::Str(PASSWORD.into()));
    Arc::new(APIConnector::new(Some("retry"), attrs).unwrap())
}

fn options(max_retries: u64) -> RetryOptions {
    RetryOptions {
        max_retries,
        retry_wait: 0.0,
        wait_text: "0".into(),
        ..RetryOptions::new("api:retry")
    }
}

async fn run(probe: &mut ApiRetryProbe, options: &RetryOptions) -> (RetryOutcome, Vec<String>) {
    let mut events = CollectEvents::default();
    let outcome = retry_connect(probe, options, &mut events, &mut SleepWaiter).await;
    (outcome, events.lines)
}

fn probe(
    connector: &Arc<APIConnector>,
    enforce_chaining: bool,
    enforce_login: bool,
) -> ApiRetryProbe {
    ApiRetryProbe::new(
        connector.clone(),
        "api:retry",
        enforce_chaining,
        enforce_login,
        true,
    )
}

fn requests(server: &MockServer) -> Vec<String> {
    let (recorded, pending, _) = server.take();
    assert_eq!(pending, 0, "canned responses were left unused");
    recorded
        .iter()
        .map(|r| {
            let path = r.target.split('?').next().unwrap_or_default().to_string();
            format!("{} {path}", r.method)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_instance_that_allows_chaining_and_logs_in_connects() {
    let server = MockServer::start().await;
    // The chaining request logs in first (it carries the token); the attempt logs in again.
    server.push_response(login_ok());
    server.push_response(json(200, "true"));
    server.push_response(login_ok());
    let conn = connector(server.port);
    let mut opts = options(3);
    opts.print_on_connect = true;
    let (outcome, lines) = run(&mut probe(&conn, true, true), &opts).await;
    assert_eq!(outcome, RetryOutcome::Connected);
    assert_eq!(lines, ["info: Connected to 'api:retry'."]);
    assert_eq!(
        requests(&server),
        ["POST /login", "GET /chaining", "POST /login"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enforce_chaining_refuses_an_instance_without_chaining_at_once() {
    let server = MockServer::start().await;
    server.push_response(login_ok());
    server.push_response(json(200, "false"));
    let conn = connector(server.port);
    let mut opts = options(5);
    opts.retry_wait = 60.0;
    let started = Instant::now();
    let (outcome, lines) = run(&mut probe(&conn, true, true), &opts).await;
    assert_eq!(outcome, RetryOutcome::Refused);
    assert!(!outcome.connected());
    // No retry, no wait, no login after the refusal.
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(
        lines,
        [
            "warn: Meerschaum instance 'api:retry' does not allow chaining and cannot be used \
             as the parent for this instance."
        ]
    );
    assert_eq!(requests(&server), ["POST /login", "GET /chaining"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_chaining_refusal_is_silent_without_warn() {
    let server = MockServer::start().await;
    server.push_response(login_ok());
    server.push_response(json(200, "false"));
    let conn = connector(server.port);
    let mut opts = options(5);
    opts.warn = false;
    let (outcome, lines) = run(&mut probe(&conn, true, true), &opts).await;
    assert_eq!(outcome, RetryOutcome::Refused);
    assert!(lines.is_empty(), "{lines:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_enforce_chaining_the_status_is_ignored_and_the_login_goes_on() {
    let server = MockServer::start().await;
    server.push_response(login_ok());
    server.push_response(json(200, "false"));
    server.push_response(login_ok());
    let conn = connector(server.port);
    let (outcome, lines) = run(&mut probe(&conn, false, true), &options(2)).await;
    assert_eq!(outcome, RetryOutcome::Connected);
    assert!(lines.is_empty(), "{lines:?}");
    assert_eq!(
        requests(&server),
        ["POST /login", "GET /chaining", "POST /login"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_login_is_reported_and_the_chaining_status_is_remembered() {
    let server = MockServer::start().await;
    // Attempt 1: token login, chaining, a refused explicit login. Attempt 2 asks nothing
    // about chaining again and its login succeeds.
    server.push_response(login_ok());
    server.push_response(json(200, "true"));
    server.push_response(login_refused());
    server.push_response(login_ok());
    let conn = connector(server.port);
    let (outcome, lines) = run(&mut probe(&conn, true, true), &options(3)).await;
    assert_eq!(outcome, RetryOutcome::Connected);
    assert!(
        lines.contains(&"warn: Unable to login to 'api:retry'!".to_string()),
        "{lines:?}"
    );
    assert!(
        lines.contains(&"info: Failed connection attempt (1 / 3)".to_string()),
        "{lines:?}"
    );
    assert!(!lines.iter().any(|l| l.contains("(2 / 3)")));
    assert_eq!(
        requests(&server),
        ["POST /login", "GET /chaining", "POST /login", "POST /login"]
    );
    assert!(!lines.iter().any(|l| l.contains(PASSWORD)), "{lines:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_enforce_login_reaching_the_instance_is_enough() {
    let server = MockServer::start().await;
    server.push_response(login_ok());
    server.push_response(json(200, "true"));
    let conn = connector(server.port);
    let (outcome, _) = run(&mut probe(&conn, true, false), &options(2)).await;
    assert_eq!(outcome, RetryOutcome::Connected);
    assert_eq!(requests(&server), ["POST /login", "GET /chaining"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_error_status_from_chaining_is_a_failed_attempt_asked_again() {
    let server = MockServer::start().await;
    server.push_response(login_ok());
    server.push_response(json(500, r#"{"detail": "boom"}"#));
    server.push_response(json(200, "true"));
    server.push_response(login_ok());
    let conn = connector(server.port);
    let (outcome, lines) = run(&mut probe(&conn, true, true), &options(3)).await;
    assert_eq!(outcome, RetryOutcome::Connected);
    // Nothing is said about the login for an instance that did not answer chaining.
    assert!(
        !lines.iter().any(|l| l.contains("Unable to login")),
        "{lines:?}"
    );
    assert_eq!(
        lines,
        [
            "warn: Connection to 'api:retry' failed.\n    Press [Enter] to retry or wait 0 seconds.",
            "info: Failed connection attempt (1 / 3)",
        ]
    );
    assert_eq!(
        requests(&server),
        [
            "POST /login",
            "GET /chaining",
            "GET /chaining",
            "POST /login"
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_port_exhausts_the_attempts() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let conn = connector(port);
    let started = Instant::now();
    let (outcome, lines) = run(&mut probe(&conn, true, true), &options(2)).await;
    assert_eq!(outcome, RetryOutcome::Exhausted);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(lines.contains(&"info: Failed connection attempt (2 / 2)".to_string()));
    assert!(
        !lines.iter().any(|l| l.contains("Unable to login")),
        "{lines:?}"
    );
    assert!(!lines.iter().any(|l| l.contains(PASSWORD)), "{lines:?}");
    // Each failed attempt says why, on standard output (v4 printed the request's exception).
    let cause = format!("out: api:retry: cannot connect to 127.0.0.1:{port} (connection refused)");
    assert_eq!(
        lines.iter().filter(|l| l.starts_with(&cause)).count(),
        2,
        "{lines:?}"
    );
}

#[tokio::test]
async fn the_attempt_timeout_covers_two_requests() {
    let conn = connector(1);
    assert_eq!(
        conn.retry_attempt_timeout(),
        conn.connect_timeout() * 2 + Duration::from_secs(1)
    );
}
