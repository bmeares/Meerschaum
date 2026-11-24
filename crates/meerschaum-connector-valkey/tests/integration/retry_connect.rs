//! `retry_connect()` for a Valkey instance (`ValkeyRetryProbe`): a closed port, a server that
//! starts between attempts, one that never answers, and the connection the loop leaves behind.
//! The server is a minimal RESP responder started here.

use std::time::{Duration, Instant};

use meerschaum_connector::retry::{
    CollectEvents, RetryEvents, RetryOptions, RetryOutcome, RetryWaiter, SleepWaiter, Wait,
    retry_connect,
};
use meerschaum_connector_valkey::retry::ValkeyRetryProbe;
use meerschaum_core::{Map, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

const PASSWORD: &str = "retry-not-a-secret";

fn attributes(port: u16, extra: &[(&str, Value)]) -> Map {
    let mut attributes = Map::new();
    attributes.insert("host".into(), Value::from("127.0.0.1"));
    attributes.insert("port".into(), Value::Int(i64::from(port)));
    attributes.insert("password".into(), Value::from(PASSWORD));
    for (k, v) in extra {
        attributes.insert((*k).into(), v.clone());
    }
    attributes
}

/// Reserve a port that refuses connections until the socket starts listening.
fn bound_socket() -> TcpSocket {
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind(([127, 0, 0, 1], 0).into()).unwrap();
    socket
}

fn options(max_retries: u64) -> RetryOptions {
    RetryOptions {
        max_retries,
        retry_wait: 0.0,
        wait_text: "0".into(),
        ..RetryOptions::new("valkey:retry")
    }
}

/// Answer commands: `PING` with `PONG`, `HELLO` with an error (RESP2), anything else `OK`.
async fn serve(stream: TcpStream) {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    loop {
        let mut line = String::new();
        if read.read_line(&mut line).await.unwrap_or(0) == 0 {
            return;
        }
        let Some(count) = line
            .trim()
            .strip_prefix('*')
            .and_then(|n| n.parse::<usize>().ok())
        else {
            continue;
        };
        let mut parts: Vec<Vec<u8>> = Vec::new();
        for _ in 0..count {
            let mut size = String::new();
            if read.read_line(&mut size).await.unwrap_or(0) == 0 {
                return;
            }
            let size: usize = size.trim().trim_start_matches('$').parse().unwrap_or(0);
            let mut bytes = vec![0u8; size + 2];
            if read.read_exact(&mut bytes).await.is_err() {
                return;
            }
            bytes.truncate(size);
            parts.push(bytes);
        }
        let name = parts
            .first()
            .cloned()
            .unwrap_or_default()
            .to_ascii_uppercase();
        let reply: &[u8] = match name.as_slice() {
            b"PING" => b"+PONG\r\n",
            b"HELLO" => b"-ERR unknown command 'HELLO'\r\n",
            _ => b"+OK\r\n",
        };
        if write.write_all(reply).await.is_err() {
            return;
        }
    }
}

async fn start_server(listener: TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        tokio::spawn(serve(stream));
    }
}

async fn run(
    probe: &mut ValkeyRetryProbe,
    options: &RetryOptions,
) -> (RetryOutcome, Vec<String>, Duration) {
    let mut events = CollectEvents::default();
    let started = Instant::now();
    let outcome = retry_connect(probe, options, &mut events, &mut SleepWaiter).await;
    (outcome, events.lines, started.elapsed())
}

#[tokio::test]
async fn a_reachable_server_connects_and_the_connection_is_kept() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(start_server(listener));
    let mut probe = ValkeyRetryProbe::new("retry", attributes(port, &[]), None);
    let mut opts = options(3);
    opts.print_on_connect = true;
    let (outcome, lines, _) = run(&mut probe, &opts).await;
    assert_eq!(outcome, RetryOutcome::Connected, "{lines:?}");
    assert_eq!(lines, ["info: Connected to 'valkey:retry'."]);
    let live = probe.live().expect("the connection the loop made");
    assert!(live.ping().await.unwrap());
}

#[tokio::test]
async fn a_closed_port_fails_each_attempt_with_the_connectors_message() {
    let socket = bound_socket();
    let port = socket.local_addr().unwrap().port();
    let mut probe = ValkeyRetryProbe::new("retry", attributes(port, &[]), None);
    let (outcome, lines, elapsed) = run(&mut probe, &options(2)).await;
    assert_eq!(outcome, RetryOutcome::Exhausted);
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    let detail = format!(
        "out: valkey:retry: cannot connect to 127.0.0.1:{port} (connection refused). Is the Valkey server running?"
    );
    assert_eq!(
        lines.iter().filter(|l| **l == detail).count(),
        2,
        "{lines:?}"
    );
    assert!(lines.contains(&"info: Failed connection attempt (2 / 2)".to_string()));
    assert!(probe.live().is_none());
    assert!(!lines.iter().any(|l| l.contains(PASSWORD)), "{lines:?}");
}

struct StartServerOnRetry(Option<TcpSocket>);

impl RetryWaiter for StartServerOnRetry {
    async fn wait(&mut self, _wait: Duration, _events: &mut dyn RetryEvents) -> Wait {
        if let Some(socket) = self.0.take() {
            tokio::spawn(start_server(socket.listen(1).unwrap()));
        }
        Wait::Elapsed
    }
}

#[tokio::test]
async fn a_server_that_starts_between_attempts_is_picked_up() {
    let socket = bound_socket();
    let port = socket.local_addr().unwrap().port();
    let mut probe = ValkeyRetryProbe::new("retry", attributes(port, &[]), None);
    let opts = RetryOptions {
        max_retries: 2,
        ..RetryOptions::new("valkey:retry")
    };
    let mut events = CollectEvents::default();
    let mut waiter = StartServerOnRetry(Some(socket));
    let outcome = retry_connect(&mut probe, &opts, &mut events, &mut waiter).await;
    assert_eq!(outcome, RetryOutcome::Connected, "{:?}", events.lines);
    assert!(
        events
            .lines
            .contains(&"info: Failed connection attempt (1 / 2)".to_string())
    );
    assert!(probe.live().is_some());
}

#[tokio::test]
async fn an_existing_connection_is_used_without_connecting_again() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(start_server(listener));
    let mut first = ValkeyRetryProbe::new("retry", attributes(port, &[]), None);
    let (outcome, _, _) = run(&mut first, &options(2)).await;
    assert!(outcome.connected());
    let live = first.live();
    // The address is dead now, but the probe holds a connection: it pings that one.
    server.abort();
    let socket = bound_socket();
    let port = socket.local_addr().unwrap().port();
    let mut second = ValkeyRetryProbe::new("retry", attributes(port, &[]), live);
    let (outcome, lines, _) = run(&mut second, &options(1)).await;
    assert_eq!(outcome, RetryOutcome::Connected, "{lines:?}");
}

#[tokio::test]
async fn the_loops_timeout_ends_an_attempt_against_a_silent_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        // Accept and hold: never answer.
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    let mut probe = ValkeyRetryProbe::new("retry", attributes(port, &[]), None);
    let mut opts = options(2);
    opts.attempt_timeout = Some(Duration::from_millis(300));
    let (outcome, lines, elapsed) = run(&mut probe, &opts).await;
    assert_eq!(outcome, RetryOutcome::Exhausted);
    assert!(elapsed >= Duration::from_millis(600), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(4), "{elapsed:?}");
    assert_eq!(
        lines
            .iter()
            .filter(
                |l| l.starts_with("out: The connection attempt to 'valkey:retry' did not finish")
            )
            .count(),
        2,
        "{lines:?}"
    );
}

#[test]
fn the_attempt_timeout_follows_connect_timeout() {
    // `MRSM_CONNECT_TIMEOUT` overrides the attribute and the default for every connector.
    if std::env::var(meerschaum_core::connect::CONNECT_TIMEOUT_ENV).is_ok() {
        return;
    }
    // Two connection timeouts (the connect and the `PING`) and a second of grace.
    assert_eq!(
        ValkeyRetryProbe::attempt_timeout(&attributes(1, &[])),
        Duration::from_secs(11)
    );
    assert_eq!(
        ValkeyRetryProbe::attempt_timeout(&attributes(1, &[("connect_timeout", Value::Int(1))])),
        Duration::from_secs(3)
    );
}
