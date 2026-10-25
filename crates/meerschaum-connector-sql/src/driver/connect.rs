//! Fail-fast first connections (D23-style polish for rc2, DECISIONS item 23).
//!
//! `sqlx` retries a refused connection until its pool acquire timeout (30 s), and a host that
//! never answers holds the first query for as long. The first connection a driver makes in a
//! process is therefore *established* here: the pool is asked for one connection while, in the
//! same `select!`, a TCP probe runs after a short delay and a deadline of `connect_timeout`
//! (default 5 s) bounds the whole attempt. A refused connection, an unresolvable host, an
//! unreachable network and rejected credentials fail at once with one clear message
//! (`sql:main: cannot connect to localhost:5432 (connection refused). Is the database running?`);
//! an unresponsive host fails after `connect_timeout`.
//!
//! Only the first attempt is gated. Once a connection was established the pool keeps its own
//! reconnect behavior, so a long-running job that loses its database still retries as in v4.
//! A timeout and rejected credentials are remembered for `FAILURE_TTL`, so a command that tries
//! many pipes against a dead host does not wait `connect_timeout` (nor log in wrongly) once per
//! pipe; a refused connection is retried every time, since the server may come up in between.

use std::future::Future;
#[cfg(unix)]
use std::io;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub use meerschaum_core::connect::{
    CONNECT_TIMEOUT_ENV, ConnectError, ConnectErrorKind, DEFAULT_CONNECT_TIMEOUT_SECS,
    parse_timeout,
};
use meerschaum_core::connect::{ConnectTarget, classify_io, classify_text, connect_error};

use crate::error::SqlError;
use crate::uri::ConnectionSpec;

/// How long a pending connection attempt runs before a TCP probe classifies why it is slow.
/// A healthy local or LAN connection finishes first and never probes.
const PROBE_DELAY: Duration = Duration::from_millis(100);

/// How long a failed first attempt is remembered.
const FAILURE_TTL: Duration = Duration::from_secs(5);

/// The connection timeout of a spec: `MRSM_CONNECT_TIMEOUT`, else the connector's
/// `connect_timeout` attribute (seconds), else 5 seconds.
pub fn connect_timeout(spec: &ConnectionSpec) -> Duration {
    let attribute = spec
        .params
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("connect_timeout"))
        .map(|(_, v)| v.as_str());
    meerschaum_core::connect::connect_timeout(attribute)
}

/// What messages name for this spec: `sql:main`, `host:port` (or the socket path), the user.
fn target(spec: &ConnectionSpec) -> ConnectTarget {
    let connector = match spec.label.as_deref().filter(|l| !l.is_empty()) {
        Some(label) => format!("sql:{label}"),
        None => "sql".to_string(),
    };
    let host = spec.host.as_deref().filter(|h| !h.is_empty());
    let address = match (host, spec.port) {
        (Some(h), Some(p)) if !h.starts_with('/') => format!("{h}:{p}"),
        (Some(h), _) => h.to_string(),
        (None, Some(p)) => format!("localhost:{p}"),
        (None, None) => "the database".to_string(),
    };
    ConnectTarget {
        connector,
        address,
        user: spec.username.clone().unwrap_or_default(),
        noun: "database",
    }
}

pub(crate) fn make_error(
    spec: &ConnectionSpec,
    kind: ConnectErrorKind,
    detail: &str,
    limit: Duration,
) -> ConnectError {
    connect_error(&target(spec), kind, detail, limit)
}

/// Map a `sqlx` error raised while establishing the first connection. Errors that are not
/// about connecting stay as they are.
pub(crate) fn map_sqlx(spec: &ConnectionSpec, e: sqlx::Error, elapsed: Duration) -> SqlError {
    let connect = |kind, detail: &str| {
        SqlError::Connect(make_error(
            spec,
            kind,
            detail,
            elapsed.max(Duration::from_secs(1)),
        ))
    };
    match &e {
        sqlx::Error::Io(io) => connect(classify_io(io), &io.to_string()),
        sqlx::Error::PoolTimedOut => connect(ConnectErrorKind::Timeout, ""),
        sqlx::Error::Tls(inner) => connect(ConnectErrorKind::Tls, &inner.to_string()),
        sqlx::Error::Database(db) => {
            let code = db.code().map(|c| c.into_owned()).unwrap_or_default();
            let message = db.message();
            match classify_text(message) {
                Some(ConnectErrorKind::Auth) => connect(ConnectErrorKind::Auth, message),
                _ if matches!(code.as_str(), "28P01" | "28000") => {
                    connect(ConnectErrorKind::Auth, message)
                }
                _ if code == "3D000"
                    || message.to_ascii_lowercase().contains("unknown database") =>
                {
                    connect(ConnectErrorKind::Server, message)
                }
                _ if matches!(code.as_str(), "57P03" | "53300") => {
                    connect(ConnectErrorKind::Server, message)
                }
                _ => SqlError::Driver(e),
            }
        }
        _ => SqlError::Driver(e),
    }
}

/// Map a native-driver (text) error raised while establishing the first connection.
pub(crate) fn map_text(spec: &ConnectionSpec, e: SqlError, elapsed: Duration) -> SqlError {
    let SqlError::Database(text) = &e else {
        return e;
    };
    match classify_text(text) {
        Some(kind) => SqlError::Connect(make_error(
            spec,
            kind,
            text,
            elapsed.max(Duration::from_secs(1)),
        )),
        None => e,
    }
}

/// Probe whether something accepts connections at the spec's address. Returns the failure
/// (refused, unresolvable, unreachable) or `Ok(())` when a connection was accepted or the
/// address cannot be probed (no host).
async fn tcp_probe(
    spec: &ConnectionSpec,
    limit: Duration,
) -> std::result::Result<(), ConnectError> {
    let fail = |kind: ConnectErrorKind, detail: String| make_error(spec, kind, &detail, limit);
    let host = spec.host.as_deref().filter(|h| !h.is_empty());
    #[cfg(unix)]
    if let Some(dir) = host.filter(|h| h.starts_with('/')) {
        let path =
            std::path::Path::new(dir).join(format!(".s.PGSQL.{}", spec.port.unwrap_or(5432)));
        return match tokio::net::UnixStream::connect(&path).await {
            Ok(_) => Ok(()),
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                ) =>
            {
                Err(fail(ConnectErrorKind::Refused, e.to_string()))
            }
            Err(_) => Ok(()),
        };
    }
    let (Some(host), Some(port)) = (host, spec.port) else {
        return Ok(());
    };
    if host.starts_with('/') {
        return Ok(());
    }
    let addrs: Vec<std::net::SocketAddr> = match tokio::net::lookup_host((host, port)).await {
        Ok(addrs) => addrs.collect(),
        Err(e) => return Err(fail(ConnectErrorKind::Dns, e.to_string())),
    };
    if addrs.is_empty() {
        return Err(fail(ConnectErrorKind::Dns, "no addresses".into()));
    }
    match tokio::net::TcpStream::connect(addrs.as_slice()).await {
        Ok(_) => Ok(()),
        Err(e) => Err(fail(classify_io(&e), e.to_string())),
    }
}

/// Run `attempt` (open the first pooled connection) bounded by `connect_timeout`, probing the
/// address when it is slow so that a refused connection or an unknown host fails at once.
pub(crate) async fn establish<F>(
    spec: &ConnectionSpec,
    attempt: F,
) -> std::result::Result<(), SqlError>
where
    F: Future<Output = std::result::Result<(), SqlError>>,
{
    let limit = connect_timeout(spec);
    let probe = async {
        tokio::time::sleep(PROBE_DELAY.min(limit)).await;
        match tcp_probe(spec, limit).await {
            Err(e) => e,
            // Reachable: the attempt (or the deadline) decides.
            Ok(()) => std::future::pending().await,
        }
    };
    tokio::select! {
        result = attempt => result,
        failure = probe => Err(SqlError::Connect(failure)),
        () = tokio::time::sleep(limit) => Err(SqlError::Connect(make_error(
            spec,
            ConnectErrorKind::Timeout,
            "",
            limit,
        ))),
    }
}

/// Remembers a failed first attempt (timeouts and rejected credentials) for [`FAILURE_TTL`].
#[derive(Default)]
pub(crate) struct FailureCache {
    last: Mutex<Option<(Instant, ConnectError)>>,
}

impl FailureCache {
    /// The failure recorded within the TTL, if any.
    pub(crate) fn recent(&self) -> Option<ConnectError> {
        let guard = self.last.lock().ok()?;
        guard
            .as_ref()
            .filter(|(at, _)| at.elapsed() < FAILURE_TTL)
            .map(|(_, e)| e.clone())
    }

    /// Record `error` when remembering it saves something: a timeout (each retry would wait
    /// `connect_timeout` again) or rejected credentials / TLS settings (each retry is another
    /// failed login, which an account lockout policy counts). A refused connection, an unknown
    /// host or an unreachable network fails again at once, and is retried every time: the
    /// server may come up between two calls.
    pub(crate) fn record(&self, error: &SqlError) {
        if let SqlError::Connect(e) = error
            && matches!(
                e.kind(),
                ConnectErrorKind::Timeout | ConnectErrorKind::Auth | ConnectErrorKind::Tls
            )
            && let Ok(mut guard) = self.last.lock()
        {
            *guard = Some((Instant::now(), e.clone()));
        }
    }
}
