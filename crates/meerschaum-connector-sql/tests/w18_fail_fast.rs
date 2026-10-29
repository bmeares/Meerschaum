//! Wave 18 (DECISIONS item 23): the first connection of a command fails fast and says why.
//!
//! The refused, unresolvable and unroutable cases need no database. The wrong-password cases
//! run against the shared test containers when `MRSM_TEST_PG_URI` / `MRSM_TEST_MYSQL_URI` /
//! `MRSM_TEST_MSSQL_URI` / `MRSM_TEST_ORACLE_URI` are set (they log in as a user that does not
//! exist; nothing is created or altered).

use std::time::{Duration, Instant};

use meerschaum_connector_sql::SqlError;
use meerschaum_connector_sql::driver::SqlDriver;
use meerschaum_connector_sql::driver::connect::{self, ConnectErrorKind};
use meerschaum_connector_sql::uri::ConnectionSpec;

/// A local TCP port nothing listens on.
fn closed_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn spec(scheme: &str, host: &str, port: u16, query: &str) -> ConnectionSpec {
    let uri = format!("{scheme}://scratch:not-a-secret@{host}:{port}/scratchdb{query}");
    ConnectionSpec::from_uri(&uri, Some("w18cn"), None).unwrap()
}

fn connect_error(error: SqlError) -> connect::ConnectError {
    match error {
        SqlError::Connect(e) => e,
        other => panic!("expected a connection error, got: {other}"),
    }
}

async fn first_failure(spec: &ConnectionSpec) -> (connect::ConnectError, Duration) {
    let driver = SqlDriver::connect_lazy(spec).unwrap();
    let started = Instant::now();
    let error = driver.exec("SELECT 1").await.unwrap_err();
    let elapsed = started.elapsed();
    eprintln!("{elapsed:?}: {error}");
    (connect_error(error), elapsed)
}

#[tokio::test]
async fn refused_connections_fail_at_once_for_every_network_flavor() {
    for scheme in ["postgresql", "mysql", "mssql"] {
        let port = closed_port();
        // The default timeout is 5 s; a refused port must not wait for it (nor sqlx's 30 s).
        let spec = spec(scheme, "127.0.0.1", port, "");
        let (error, elapsed) = first_failure(&spec).await;
        assert_eq!(error.kind(), ConnectErrorKind::Refused, "{scheme}: {error}");
        assert_eq!(
            error.message(),
            format!(
                "sql:w18cn: cannot connect to 127.0.0.1:{port} (connection refused). Is the database running?"
            ),
            "{scheme}"
        );
        assert!(elapsed < Duration::from_secs(2), "{scheme}: {elapsed:?}");
    }
}

#[tokio::test]
async fn driver_connect_also_fails_fast() {
    let port = closed_port();
    let spec = spec("postgresql", "127.0.0.1", port, "");
    let started = Instant::now();
    let error = SqlDriver::connect(&spec).await.unwrap_err();
    assert_eq!(connect_error(error).kind(), ConnectErrorKind::Refused);
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn timeouts_are_remembered_briefly() {
    let spec = spec("postgresql", "10.255.255.1", 5432, "?connect_timeout=1");
    let driver = SqlDriver::connect_lazy(&spec).unwrap();
    let first = connect_error(driver.exec("SELECT 1").await.unwrap_err());
    // A command that tries many pipes must not wait `connect_timeout` once per pipe.
    let started = Instant::now();
    let second = connect_error(driver.value("SELECT 1").await.unwrap_err());
    assert_eq!(first.message(), second.message());
    assert!(
        started.elapsed() < Duration::from_millis(50),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_refused_connection_is_retried_every_time() {
    // The server may come up between two calls: after a refusal, a listener appears on the
    // port and the next call must attempt again (it no longer sees "refused").
    let port = closed_port();
    let spec = spec("postgresql", "127.0.0.1", port, "?connect_timeout=1");
    let driver = SqlDriver::connect_lazy(&spec).unwrap();
    let first = connect_error(driver.exec("SELECT 1").await.unwrap_err());
    assert_eq!(first.kind(), ConnectErrorKind::Refused);
    // Accepts connections but never speaks the protocol.
    let listener = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
    let second = connect_error(driver.exec("SELECT 1").await.unwrap_err());
    assert_eq!(second.kind(), ConnectErrorKind::Timeout, "{second}");
    drop(listener);
}

#[tokio::test]
async fn unresolvable_hosts_fail_with_a_dns_message() {
    let spec = spec("postgresql", "w18cn-no-such-host.invalid", 5432, "");
    let (error, elapsed) = first_failure(&spec).await;
    // A resolver that is offline reports the same failure; a hijacking resolver is not expected.
    assert!(
        matches!(
            error.kind(),
            ConnectErrorKind::Dns | ConnectErrorKind::Timeout | ConnectErrorKind::Unreachable
        ),
        "{error}"
    );
    assert!(
        error
            .message()
            .starts_with("sql:w18cn: cannot connect to w18cn-no-such-host.invalid:5432 (")
    );
    assert!(elapsed < Duration::from_secs(7), "{elapsed:?}");
}

#[tokio::test]
async fn unresponsive_hosts_are_bounded_by_connect_timeout() {
    // 10.255.255.1 is unroutable: SYNs are dropped (or the sandbox has no route at all).
    for scheme in ["postgresql", "mysql", "mssql"] {
        let spec = spec(scheme, "10.255.255.1", 5432, "?connect_timeout=1");
        let (error, elapsed) = first_failure(&spec).await;
        assert!(
            matches!(
                error.kind(),
                ConnectErrorKind::Timeout | ConnectErrorKind::Unreachable
            ),
            "{scheme}: {error}"
        );
        if error.kind() == ConnectErrorKind::Timeout {
            assert!(
                error.message().contains("no response within 1 s"),
                "{}",
                error.message()
            );
            assert!(
                elapsed >= Duration::from_millis(900),
                "{scheme}: {elapsed:?}"
            );
        }
        assert!(elapsed < Duration::from_secs(3), "{scheme}: {elapsed:?}");
    }
}

#[test]
fn connect_timeout_attribute_and_default() {
    // `MRSM_CONNECT_TIMEOUT` overrides both; the test harness leaves it unset.
    if std::env::var_os(connect::CONNECT_TIMEOUT_ENV).is_some() {
        return;
    }
    let default = spec("postgresql", "127.0.0.1", 5432, "");
    assert_eq!(connect::connect_timeout(&default), Duration::from_secs(5));
    let configured = spec("postgresql", "127.0.0.1", 5432, "?connect_timeout=2.5");
    assert_eq!(
        connect::connect_timeout(&configured),
        Duration::from_millis(2500)
    );
    let invalid = spec("postgresql", "127.0.0.1", 5432, "?connect_timeout=0");
    assert_eq!(connect::connect_timeout(&invalid), Duration::from_secs(5));
}

async fn wrong_password(var: &str) {
    let Ok(uri) = std::env::var(var) else {
        eprintln!("Skipping: set {var} to run the wrong-password test.");
        return;
    };
    let mut spec = ConnectionSpec::from_uri(&uri, Some("w18cn"), None).unwrap();
    // A user that does not exist: never a failed login of a real account (no lockout).
    spec.username = Some("w18cn_no_such_user".into());
    spec.password = Some("w18cn-wrong-password".into());
    let (error, elapsed) = first_failure(&spec).await;
    assert_eq!(error.kind(), ConnectErrorKind::Auth, "{var}: {error}");
    assert!(
        error
            .message()
            .contains("authentication failed for user 'w18cn_no_such_user'"),
        "{}",
        error.message()
    );
    assert!(!error.message().contains("w18cn-wrong-password"));
    assert!(elapsed < Duration::from_secs(5), "{var}: {elapsed:?}");
}

#[tokio::test]
async fn postgres_wrong_password_fails_with_an_auth_message() {
    wrong_password("MRSM_TEST_PG_URI").await;
}

#[tokio::test]
async fn mysql_wrong_password_fails_with_an_auth_message() {
    wrong_password("MRSM_TEST_MYSQL_URI").await;
}

#[tokio::test]
async fn mssql_wrong_password_fails_with_an_auth_message() {
    wrong_password("MRSM_TEST_MSSQL_URI").await;
}

#[tokio::test]
async fn a_reachable_database_connects_and_later_queries_skip_the_gate() {
    let Ok(uri) = std::env::var("MRSM_TEST_PG_URI") else {
        eprintln!("Skipping: set MRSM_TEST_PG_URI.");
        return;
    };
    let spec = ConnectionSpec::from_uri(&uri, Some("w18cn"), None).unwrap();
    let driver = SqlDriver::connect_lazy(&spec).unwrap();
    assert!(driver.value("SELECT 1").await.unwrap().is_some());
    assert!(driver.value("SELECT 2").await.unwrap().is_some());
}

#[cfg(feature = "oracle")]
#[tokio::test]
async fn oracle_wrong_password_and_refused_fail_with_clear_messages() {
    wrong_password("MRSM_TEST_ORACLE_URI").await;
    let port = closed_port();
    let spec = spec("oracle", "127.0.0.1", port, "");
    let (error, elapsed) = first_failure(&spec).await;
    assert_eq!(error.kind(), ConnectErrorKind::Refused, "{error}");
    assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
}
