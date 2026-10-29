//! Classified connection failures shared by the SQL, API and Valkey connectors.
//!
//! A first connection that fails says why in one clear message, for example
//! `sql:main: cannot connect to localhost:5432 (connection refused). Is the database running?`.
//! The connector crates decide *when* to fail (a refused connection fails at once, an unresponsive
//! host after `connect_timeout`); this module owns the vocabulary: the failure kinds, the
//! `connect_timeout` attribute / `MRSM_CONNECT_TIMEOUT` resolution, the classification of I/O
//! errors and driver messages, and the message text.

use std::io;
use std::time::Duration;

/// The default `connect_timeout` (seconds).
pub const DEFAULT_CONNECT_TIMEOUT_SECS: f64 = 5.0;

/// The environment variable that overrides every connector's `connect_timeout` (seconds).
pub const CONNECT_TIMEOUT_ENV: &str = "MRSM_CONNECT_TIMEOUT";

/// Why a connection could not be established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectErrorKind {
    /// Nothing listens at the address (`ECONNREFUSED`).
    Refused,
    /// The host name does not resolve.
    Dns,
    /// The host or network cannot be reached.
    Unreachable,
    /// No answer within `connect_timeout`.
    Timeout,
    /// The server rejected the credentials.
    Auth,
    /// The TLS handshake failed.
    Tls,
    /// The server is reachable but refused the session (unknown database, starting up, ...).
    Server,
    /// Any other connection failure.
    Other,
}

/// A failed connection attempt with its user-facing message.
#[derive(Debug, Clone)]
pub struct ConnectError {
    kind: ConnectErrorKind,
    message: String,
}

impl ConnectError {
    /// What went wrong.
    pub fn kind(&self) -> ConnectErrorKind {
        self.kind
    }

    /// The complete message (`sql:main: cannot connect to ...`).
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ConnectError {}

/// What a message names: the connector, where it connects and as whom.
#[derive(Debug, Clone)]
pub struct ConnectTarget {
    /// `sql:main`.
    pub connector: String,
    /// `localhost:5432` (or a socket path / URL host).
    pub address: String,
    /// The user the credentials belong to (shown for rejected credentials).
    pub user: String,
    /// What the server is called in the hint of a refused connection (`database`, `API server`).
    pub noun: &'static str,
}

/// Parse a `connect_timeout` value (seconds, integer or decimal); anything not positive and
/// finite is rejected.
pub fn parse_timeout(value: &str) -> Option<Duration> {
    let secs: f64 = value.trim().parse().ok()?;
    (secs.is_finite() && secs > 0.0 && secs < 86_400.0).then(|| Duration::from_secs_f64(secs))
}

/// The connection timeout: `MRSM_CONNECT_TIMEOUT`, else the connector's `connect_timeout`
/// attribute (seconds), else 5 seconds.
pub fn connect_timeout(attribute: Option<&str>) -> Duration {
    std::env::var(CONNECT_TIMEOUT_ENV)
        .ok()
        .and_then(|v| parse_timeout(&v))
        .or_else(|| attribute.and_then(parse_timeout))
        .unwrap_or_else(|| Duration::from_secs_f64(DEFAULT_CONNECT_TIMEOUT_SECS))
}

fn describe_seconds(limit: Duration) -> String {
    let secs = limit.as_secs_f64();
    if (secs - secs.round()).abs() < 0.05 {
        format!("{} s", secs.round() as u64)
    } else {
        format!("{secs:.1} s")
    }
}

/// Build the message for one failure.
pub fn connect_error(
    target: &ConnectTarget,
    kind: ConnectErrorKind,
    detail: &str,
    limit: Duration,
) -> ConnectError {
    let ConnectTarget {
        connector,
        address,
        user,
        noun,
    } = target;
    let message = match kind {
        ConnectErrorKind::Refused => format!(
            "{connector}: cannot connect to {address} (connection refused). Is the {noun} running?"
        ),
        ConnectErrorKind::Dns => format!(
            "{connector}: cannot connect to {address} (the host name could not be resolved). \
             Check the connector's host."
        ),
        ConnectErrorKind::Unreachable => format!(
            "{connector}: cannot connect to {address} (network unreachable). \
             Check the host and the network."
        ),
        ConnectErrorKind::Timeout => format!(
            "{connector}: cannot connect to {address} (no response within {}). \
             Check the host and the network, or raise connect_timeout.",
            describe_seconds(limit)
        ),
        ConnectErrorKind::Auth => format!(
            "{connector}: cannot connect to {address} (authentication failed for user '{user}'). \
             Check the connector's credentials."
        ),
        ConnectErrorKind::Tls => format!(
            "{connector}: cannot connect to {address} (TLS handshake failed: {detail}). \
             Check the connector's TLS settings and certificates."
        ),
        ConnectErrorKind::Server | ConnectErrorKind::Other => {
            format!("{connector}: cannot connect to {address} ({detail}).")
        }
    };
    ConnectError { kind, message }
}

/// Classify an I/O error raised while connecting.
pub fn classify_io(e: &io::Error) -> ConnectErrorKind {
    match e.kind() {
        io::ErrorKind::ConnectionRefused => return ConnectErrorKind::Refused,
        io::ErrorKind::TimedOut => return ConnectErrorKind::Timeout,
        io::ErrorKind::NetworkUnreachable | io::ErrorKind::HostUnreachable => {
            return ConnectErrorKind::Unreachable;
        }
        _ => {}
    }
    classify_text(&e.to_string()).unwrap_or(ConnectErrorKind::Other)
}

/// Classify a driver message (the native drivers and `fred` report errors as text).
pub fn classify_text(text: &str) -> Option<ConnectErrorKind> {
    let t = text.to_ascii_lowercase();
    let has = |needles: &[&str]| needles.iter().any(|n| t.contains(n));
    if has(&[
        "failed to lookup address",
        "name or service not known",
        "no address associated",
        "nodename nor servname",
        "temporary failure in name resolution",
        "no such host is known",
        "os error 11001",
        "dns error",
        "ora-12154",
        "ora-12545",
    ]) {
        return Some(ConnectErrorKind::Dns);
    }
    if has(&[
        "connection refused",
        "os error 111",
        "os error 10061",
        "actively refused",
        "ora-12541",
        "no listener",
    ]) {
        return Some(ConnectErrorKind::Refused);
    }
    if has(&[
        "network is unreachable",
        "no route to host",
        "os error 101",
        "os error 113",
        "os error 10051",
        "os error 10065",
    ]) {
        return Some(ConnectErrorKind::Unreachable);
    }
    if has(&[
        "timed out",
        "timeout",
        "ora-12170",
        "ora-12535",
        "ora-12537",
    ]) {
        return Some(ConnectErrorKind::Timeout);
    }
    if has(&[
        "login failed",
        "ora-01017",
        "access denied",
        "password authentication failed",
        "authentication failed",
        "no pg_hba.conf entry",
        "wrongpass",
        "invalid username-password",
        "noauth",
        "401 unauthorized",
    ]) {
        return Some(ConnectErrorKind::Auth);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeouts_parse() {
        assert_eq!(parse_timeout("5"), Some(Duration::from_secs(5)));
        assert_eq!(parse_timeout(" 1.5 "), Some(Duration::from_millis(1500)));
        assert_eq!(parse_timeout("0"), None);
        assert_eq!(parse_timeout("-3"), None);
        assert_eq!(parse_timeout("soon"), None);
        assert_eq!(parse_timeout("inf"), None);
    }

    #[test]
    fn text_classification() {
        use ConnectErrorKind::*;
        assert_eq!(
            classify_text("Connection refused (os error 111)"),
            Some(Refused)
        );
        assert_eq!(
            classify_text("failed to lookup address information: Name or service not known"),
            Some(Dns)
        );
        assert_eq!(classify_text("Login failed for user 'sa'."), Some(Auth));
        assert_eq!(classify_text("ORA-01017: invalid credential"), Some(Auth));
        assert_eq!(
            classify_text("ORA-12541: Cannot connect. No listener"),
            Some(Refused)
        );
        assert_eq!(
            classify_text("Network is unreachable (os error 101)"),
            Some(Unreachable)
        );
        assert_eq!(classify_text("connection timed out"), Some(Timeout));
        assert_eq!(classify_text("syntax error"), None);
    }

    #[test]
    fn messages_name_the_connector_and_the_cause() {
        let target = ConnectTarget {
            connector: "sql:main".into(),
            address: "localhost:5432".into(),
            user: "mrsm".into(),
            noun: "database",
        };
        let limit = Duration::from_secs(5);
        assert_eq!(
            connect_error(&target, ConnectErrorKind::Refused, "", limit).message(),
            "sql:main: cannot connect to localhost:5432 (connection refused). Is the database running?"
        );
        assert!(
            connect_error(
                &target,
                ConnectErrorKind::Timeout,
                "",
                Duration::from_millis(1500)
            )
            .message()
            .contains("no response within 1.5 s")
        );
        assert!(
            connect_error(&target, ConnectErrorKind::Auth, "", limit)
                .message()
                .contains("authentication failed for user 'mrsm'")
        );
    }
}
