//! `retry_connect()` for a SQL instance (`meerschaum_connector::retry` over `SqlRetryProbe`):
//! the loop against a real SQLite file, a closed port of a network flavor, a database that appears
//! between attempts, and a server that accepts the connection and never answers. No database
//! server is needed.

use std::time::{Duration, Instant};

use meerschaum_connector::retry::{
    CollectEvents, RetryEvents, RetryOptions, RetryOutcome, RetryWaiter, SleepWaiter, Wait,
    retry_connect,
};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::instance::InstanceSettings;
use meerschaum_connector_sql::instance::retry::SqlRetryProbe;
use meerschaum_connector_sql::uri::ConnectionSpec;
use std::sync::Arc;

const PASSWORD: &str = "retry-not-a-secret";

fn closed_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn lazy(uri: &str) -> Arc<SqlConnector> {
    let spec = ConnectionSpec::from_uri(uri, Some("rs_retry"), None).unwrap();
    let settings = InstanceSettings::defaults(spec.flavor);
    Arc::new(SqlConnector::connect_lazy("rs_retry", spec, settings).unwrap())
}

fn options(max_retries: u64) -> RetryOptions {
    RetryOptions {
        max_retries,
        retry_wait: 0.0,
        wait_text: "0".into(),
        ..RetryOptions::new("sql:rs_retry")
    }
}

async fn run(
    connector: Arc<SqlConnector>,
    options: &RetryOptions,
) -> (RetryOutcome, Vec<String>, Duration) {
    let mut events = CollectEvents::default();
    let started = Instant::now();
    let outcome = retry_connect(
        &mut SqlRetryProbe::new(connector),
        options,
        &mut events,
        &mut SleepWaiter,
    )
    .await;
    (outcome, events.lines, started.elapsed())
}

#[tokio::test]
async fn a_reachable_sqlite_file_connects_on_the_first_try() {
    let dir = tempfile::tempdir().unwrap();
    let connector = lazy(&format!("sqlite:///{}", dir.path().join("ok.db").display()));
    let mut opts = options(3);
    opts.print_on_connect = true;
    let (outcome, lines, _) = run(connector, &opts).await;
    assert_eq!(outcome, RetryOutcome::Connected);
    assert_eq!(lines, ["info: Connected to 'sql:rs_retry'."]);
}

#[tokio::test]
async fn a_closed_port_fails_each_attempt_with_the_drivers_message() {
    let port = closed_port();
    let connector = lazy(&format!(
        "postgresql://scratch:{PASSWORD}@127.0.0.1:{port}/scratchdb"
    ));
    let (outcome, lines, elapsed) = run(connector, &options(2)).await;
    assert_eq!(outcome, RetryOutcome::Exhausted);
    // A refused connection fails at once: two attempts do not wait for the pool's 30 s.
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    let detail = format!(
        "out: sql:rs_retry: cannot connect to 127.0.0.1:{port} (connection refused). Is the database running?"
    );
    assert_eq!(
        lines.iter().filter(|l| **l == detail).count(),
        2,
        "{lines:?}"
    );
    assert!(lines.contains(&"info: Failed connection attempt (1 / 2)".to_string()));
    assert!(lines.contains(&"info: Failed connection attempt (2 / 2)".to_string()));
    assert!(!lines.iter().any(|l| l.contains(PASSWORD)), "{lines:?}");
}

#[tokio::test]
async fn warn_off_hides_the_failures() {
    let port = closed_port();
    let connector = lazy(&format!(
        "postgresql://scratch:{PASSWORD}@127.0.0.1:{port}/scratchdb"
    ));
    let mut opts = options(2);
    opts.warn = false;
    let (outcome, lines, _) = run(connector, &opts).await;
    assert_eq!(outcome, RetryOutcome::Exhausted);
    assert!(lines.is_empty(), "{lines:?}");
}

/// Creates the database's directory during the first wait: the second attempt connects.
struct CreatesDirectory {
    directory: std::path::PathBuf,
    waits: usize,
}

impl RetryWaiter for CreatesDirectory {
    async fn wait(&mut self, _wait: Duration, _events: &mut dyn RetryEvents) -> Wait {
        self.waits += 1;
        std::fs::create_dir_all(&self.directory).unwrap();
        Wait::Elapsed
    }
}

#[tokio::test]
async fn a_database_that_appears_between_attempts_is_picked_up() {
    let dir = tempfile::tempdir().unwrap();
    let directory = dir.path().join("later");
    let connector = lazy(&format!(
        "sqlite:///{}",
        directory.join("late.db").display()
    ));
    let mut opts = options(5);
    opts.retry_wait = 1.0;
    opts.wait_text = "1".into();
    let mut waiter = CreatesDirectory {
        directory,
        waits: 0,
    };
    let mut events = CollectEvents::default();
    let outcome = retry_connect(
        &mut SqlRetryProbe::new(connector),
        &opts,
        &mut events,
        &mut waiter,
    )
    .await;
    assert_eq!(outcome, RetryOutcome::Connected, "{:?}", events.lines);
    assert_eq!(waiter.waits, 1);
    assert!(
        events
            .lines
            .contains(&"info: Failed connection attempt (1 / 5)".to_string()),
        "{:?}",
        events.lines
    );
    assert!(!events.lines.iter().any(|l| l.contains("(2 / 5)")));
}

/// A listener that accepts connections and never answers.
fn silent_listener() -> (std::net::TcpListener, u16) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

#[tokio::test]
async fn the_connection_timeout_bounds_each_attempt() {
    let (listener, port) = silent_listener();
    let accept = std::thread::spawn(move || {
        // Hold the accepted sockets open until the test ends.
        let mut held = Vec::new();
        listener.set_nonblocking(false).unwrap();
        while let Ok((socket, _)) = listener.accept() {
            held.push(socket);
            if held.len() >= 4 {
                break;
            }
        }
        std::thread::sleep(Duration::from_secs(3));
        drop(held);
    });
    let connector = lazy(&format!(
        "postgresql://scratch:{PASSWORD}@127.0.0.1:{port}/scratchdb?connect_timeout=1"
    ));
    assert_eq!(connector.connect_timeout(), Duration::from_secs(1));
    let mut opts = options(1);
    opts.attempt_timeout = Some(connector.connect_timeout() * 2 + Duration::from_secs(1));
    let (outcome, lines, elapsed) = run(connector, &opts).await;
    assert_eq!(outcome, RetryOutcome::Exhausted);
    assert!(elapsed >= Duration::from_millis(900), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    assert!(
        lines.iter().any(|l| l.contains("no response within 1 s")),
        "{lines:?}"
    );
    drop(accept);
}

#[tokio::test]
async fn the_loops_own_timeout_ends_an_attempt_the_driver_would_hold() {
    let (listener, port) = silent_listener();
    let accept = std::thread::spawn(move || {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept() {
            held.push(socket);
            if held.len() >= 2 {
                break;
            }
        }
        std::thread::sleep(Duration::from_secs(3));
    });
    // The driver would wait its default 5 s; the loop gives up after 300 ms, twice.
    let connector = lazy(&format!(
        "postgresql://scratch:{PASSWORD}@127.0.0.1:{port}/scratchdb"
    ));
    let mut opts = options(2);
    opts.attempt_timeout = Some(Duration::from_millis(300));
    let (outcome, lines, elapsed) = run(connector, &opts).await;
    assert_eq!(outcome, RetryOutcome::Exhausted);
    assert!(elapsed >= Duration::from_millis(600), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(4), "{elapsed:?}");
    assert_eq!(
        lines
            .iter()
            .filter(|l| l
                .starts_with("out: The connection attempt to 'sql:rs_retry' did not finish within"))
            .count(),
        2,
        "{lines:?}"
    );
    drop(accept);
}
