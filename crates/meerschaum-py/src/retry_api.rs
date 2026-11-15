//! `meerschaum.connectors.poll.retry_connect()` for the connectors the core implements.
//!
//! `_core.retry_connect_native(connector, ...)` runs v4's retry loop ([`meerschaum_connector::retry`])
//! in this process for a `_core.SqlConnector`, `_core.ValkeyConnector` or `_core.ApiConnector`:
//! no child interpreter, no temporary file, no Python package. The loop is a task on the
//! extension's Tokio runtime (keyed by PID, so a forked job daemon builds its own); the calling
//! thread waits with the GIL released and looks for `KeyboardInterrupt` every 100 ms
//! ([`crate::api_client_api::run_interruptible`]). The loop's messages take the interpreter lock
//! one at a time and go through the SDK (`warn(..., stack=False)`, `info()`, `dprint()`); the
//! failed attempt's own text is a plain line on `sys.stdout`, as v4 `print`ed it.
//!
//! The wait between attempts listens on standard input like v4's `timed_input`: Enter retries at
//! once, `q` (or `quit`, `pass`, `exit`, `stop`) ends the loop, and end of input prints an empty
//! line and sleeps the full time. Windows only sleeps, as v4 did.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use meerschaum_connector::retry::{
    RetryEvents, RetryOptions, RetryOutcome, RetryProbe, RetryWaiter, Wait, retry_connect,
};
use meerschaum_connector_api::client::retry::ApiRetryProbe;
use meerschaum_connector_sql::instance::retry::SqlRetryProbe;
use meerschaum_connector_valkey::retry::ValkeyRetryProbe;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::api_client_api::{PyApiConnector, run_interruptible};
use crate::sql_api::PySqlConnector;
use crate::valkey_api::PyValkeyConnector;

/// The loop's messages, printed through the SDK. Errors in the printing (a `warnings` filter that
/// raises, a closed stream) never end the loop; nothing is printed once the interpreter is
/// finalizing.
struct PyEvents;

impl PyEvents {
    fn call(module: &str, function: &str, message: &str, stack_false: bool, debug: bool) {
        let _ = Python::try_attach(|py| {
            let _ = (|| -> PyResult<()> {
                let function = py.import(module)?.getattr(function)?;
                let kwargs = PyDict::new(py);
                if stack_false {
                    kwargs.set_item("stack", false)?;
                }
                if debug {
                    kwargs.set_item("_package", "meerschaum_connector::retry")?;
                    kwargs.set_item("_lineno", line!())?;
                }
                function.call((message,), Some(&kwargs))?;
                Ok(())
            })();
        });
    }
}

impl RetryEvents for PyEvents {
    fn warn(&mut self, message: &str) {
        Self::call("meerschaum.utils.warnings", "warn", message, true, false);
    }

    fn info(&mut self, message: &str) {
        Self::call("meerschaum.utils.warnings", "info", message, false, false);
    }

    fn debug(&mut self, message: &str) {
        Self::call("meerschaum.utils.debug", "dprint", message, false, true);
    }

    fn stdout(&mut self, line: &str) {
        let _ = Python::try_attach(|py| {
            let _ = (|| -> PyResult<()> {
                let stdout = py.import("sys")?.getattr("stdout")?;
                stdout.call_method1("write", (format!("{line}\n"),))?;
                Ok(())
            })();
        });
    }
}

/// What reading one line from standard input within a time limit found.
#[derive(Debug, PartialEq, Eq)]
enum LineRead {
    /// A line (with its line ending, or the text before the end of input).
    Line(String),
    /// Nothing was typed in time (or the wait was cancelled).
    Timeout,
    /// Standard input is at its end, closed, or unusable.
    Eof,
}

/// Read one line from file descriptor `fd`, waiting at most `wait`, one byte at a time so that
/// nothing after the line is consumed. Looks at `cancel` every 100 ms.
#[cfg(unix)]
fn read_line_within(fd: i32, wait: Duration, cancel: &AtomicBool) -> LineRead {
    use std::time::Instant;

    let deadline = Instant::now().checked_add(wait);
    let mut bytes: Vec<u8> = Vec::new();
    loop {
        if cancel.load(Ordering::SeqCst) {
            return LineRead::Timeout;
        }
        let slice = match deadline {
            Some(deadline) => {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return LineRead::Timeout;
                }
                left.min(Duration::from_millis(100))
            }
            None => Duration::from_millis(100),
        };
        let mut pollfd = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let timeout_ms = slice.as_millis().clamp(1, 100) as libc::c_int;
        // SAFETY: `pollfd` is one valid, initialized `struct pollfd` and `nfds` is 1.
        let ready = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
        if ready < 0 {
            if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return LineRead::Eof;
        }
        if ready == 0 {
            continue;
        }
        if pollfd.revents & libc::POLLNVAL != 0 {
            return LineRead::Eof;
        }
        if pollfd.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) == 0 {
            continue;
        }
        let mut byte = 0u8;
        // SAFETY: `byte` is one writable byte and the length passed is 1.
        let read = unsafe { libc::read(fd, (&mut byte as *mut u8).cast(), 1) };
        if read < 0 {
            if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return LineRead::Eof;
        }
        if read == 0 {
            // End of input: text typed before it is a line (as `input()` returns it).
            return if bytes.is_empty() {
                LineRead::Eof
            } else {
                LineRead::Line(String::from_utf8_lossy(&bytes).into_owned())
            };
        }
        bytes.push(byte);
        if byte == b'\n' {
            return LineRead::Line(String::from_utf8_lossy(&bytes).into_owned());
        }
    }
}

/// The wait between attempts: stdin on Unix (v4's `timed_input`), a sleep elsewhere.
struct StdinWaiter {
    /// Set when the loop is dropped, so a blocked reader gives up within 100 ms.
    cancel: Arc<AtomicBool>,
}

impl StdinWaiter {
    fn new() -> StdinWaiter {
        StdinWaiter {
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Drop for StdinWaiter {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::SeqCst);
    }
}

impl RetryWaiter for StdinWaiter {
    async fn wait(&mut self, wait: Duration, events: &mut dyn RetryEvents) -> Wait {
        #[cfg(unix)]
        {
            let cancel = self.cancel.clone();
            let read = tokio::task::spawn_blocking(move || read_line_within(0, wait, &cancel))
                .await
                .unwrap_or(LineRead::Timeout);
            match read {
                LineRead::Line(line) => meerschaum_connector::retry::classify_input_line(&line),
                LineRead::Timeout => Wait::Elapsed,
                LineRead::Eof => {
                    // v4 printed the (empty) prompt and slept the whole time.
                    events.stdout("");
                    tokio::time::sleep(wait).await;
                    Wait::Elapsed
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = events;
            tokio::time::sleep(wait).await;
            Wait::Elapsed
        }
    }
}

async fn run_loop<P: RetryProbe>(mut probe: P, options: RetryOptions) -> (RetryOutcome, P) {
    let mut events = PyEvents;
    let mut waiter = StdinWaiter::new();
    let outcome = retry_connect(&mut probe, &options, &mut events, &mut waiter).await;
    (outcome, probe)
}

/// Run the loop on the extension's runtime, interruptibly.
fn run<P: RetryProbe + 'static>(
    py: Python<'_>,
    probe: P,
    options: RetryOptions,
) -> PyResult<(RetryOutcome, P)> {
    let result = run_interruptible(py, run_loop(probe, options), None, None)?;
    result.ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("The retry loop was stopped."))
}

/// `retry_connect_native(connector, keys, max_retries, retry_wait, wait_text, workers, warn,
/// print_on_connect, enforce_chaining, enforce_login, login_warns, debug)`: v4's retry loop for a
/// `_core.SqlConnector`, `_core.ValkeyConnector` or `_core.ApiConnector`. `keys` is
/// `str(connector)` and `wait_text` the caller's `retry_wait` as text, both for the messages.
/// Returns whether a connection was made; raises `KeyboardInterrupt` on Ctrl-C.
#[pyfunction]
#[pyo3(signature = (
    connector,
    keys,
    max_retries,
    retry_wait,
    wait_text,
    workers,
    warn,
    print_on_connect,
    enforce_chaining,
    enforce_login,
    login_warns,
    debug,
))]
#[allow(clippy::too_many_arguments)]
pub fn retry_connect_native(
    py: Python<'_>,
    connector: &Bound<'_, PyAny>,
    keys: String,
    max_retries: i64,
    retry_wait: f64,
    wait_text: String,
    workers: i64,
    warn: bool,
    print_on_connect: bool,
    enforce_chaining: bool,
    enforce_login: bool,
    login_warns: bool,
    debug: bool,
) -> PyResult<bool> {
    let mut options = RetryOptions::new(keys.clone());
    options.max_retries = max_retries.max(0) as u64;
    options.retry_wait = retry_wait;
    options.wait_text = wait_text;
    options.workers = workers;
    options.warn = warn;
    options.print_on_connect = print_on_connect;
    options.debug = debug;

    if let Ok(sql) = connector.cast::<PySqlConnector>() {
        let inner = sql.get().inner.clone();
        // One request (the first connection, gated by `connect_timeout`) and the query.
        options.attempt_timeout = Some(
            inner
                .connect_timeout()
                .saturating_mul(2)
                .saturating_add(Duration::from_secs(1)),
        );
        let (outcome, _) = run(py, SqlRetryProbe::new(inner), options)?;
        return Ok(outcome.connected());
    }
    if let Ok(api) = connector.cast::<PyApiConnector>() {
        let inner = api.get().inner.clone();
        options.attempt_timeout = Some(inner.retry_attempt_timeout());
        let probe = ApiRetryProbe::new(inner, keys, enforce_chaining, enforce_login, login_warns);
        let (outcome, _) = run(py, probe, options)?;
        return Ok(outcome.connected());
    }
    if let Ok(valkey) = connector.cast::<PyValkeyConnector>() {
        let valkey = valkey.get();
        let (label, shared) = valkey.retry_parts();
        options.attempt_timeout = Some(ValkeyRetryProbe::attempt_timeout(&shared.attributes));
        let probe = ValkeyRetryProbe::new(label, shared.attributes.clone(), shared.live());
        let (outcome, probe) = run(py, probe, options)?;
        // The connection the loop made is the connector's from now on.
        if let Some(live) = probe.live() {
            shared.set_live(live);
        }
        return Ok(outcome.connected());
    }
    Err(PyTypeError::new_err(
        "retry_connect_native() takes a _core.SqlConnector, _core.ValkeyConnector or \
         _core.ApiConnector",
    ))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::fd::AsRawFd;

    /// A pipe whose write end the test controls: (read fd, writer).
    fn pipe() -> (std::fs::File, std::fs::File) {
        let mut fds = [0 as libc::c_int; 2];
        // SAFETY: `fds` has room for the two descriptors `pipe` writes.
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        use std::os::fd::FromRawFd;
        // SAFETY: both descriptors were just created and are owned by nothing else.
        unsafe {
            (
                std::fs::File::from_raw_fd(fds[0]),
                std::fs::File::from_raw_fd(fds[1]),
            )
        }
    }

    #[test]
    fn a_typed_line_is_read_without_consuming_the_next() {
        let (read, mut write) = pipe();
        write.write_all(b"q\nrest\n").unwrap();
        let cancel = AtomicBool::new(false);
        let first = read_line_within(read.as_raw_fd(), Duration::from_secs(5), &cancel);
        assert_eq!(first, LineRead::Line("q\n".into()));
        let second = read_line_within(read.as_raw_fd(), Duration::from_secs(5), &cancel);
        assert_eq!(second, LineRead::Line("rest\n".into()));
    }

    #[test]
    fn silence_times_out() {
        let (read, _write) = pipe();
        let cancel = AtomicBool::new(false);
        let started = std::time::Instant::now();
        let result = read_line_within(read.as_raw_fd(), Duration::from_millis(150), &cancel);
        assert_eq!(result, LineRead::Timeout);
        assert!(started.elapsed() >= Duration::from_millis(150));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn the_end_of_input_is_eof_and_partial_text_is_a_line() {
        let (read, write) = pipe();
        let cancel = AtomicBool::new(false);
        drop(write);
        assert_eq!(
            read_line_within(read.as_raw_fd(), Duration::from_secs(5), &cancel),
            LineRead::Eof
        );
        let (read, mut write) = pipe();
        write.write_all(b"quit").unwrap();
        drop(write);
        assert_eq!(
            read_line_within(read.as_raw_fd(), Duration::from_secs(5), &cancel),
            LineRead::Line("quit".into())
        );
    }

    #[test]
    fn a_closed_descriptor_is_eof() {
        let cancel = AtomicBool::new(false);
        assert_eq!(
            read_line_within(1_000_000, Duration::from_secs(5), &cancel),
            LineRead::Eof
        );
    }

    #[test]
    fn a_cancelled_wait_returns_at_once() {
        let (read, _write) = pipe();
        let cancel = AtomicBool::new(true);
        let started = std::time::Instant::now();
        assert_eq!(
            read_line_within(read.as_raw_fd(), Duration::from_secs(30), &cancel),
            LineRead::Timeout
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn the_waiter_stops_reading_when_dropped() {
        let (read, _write) = pipe();
        let cancel = Arc::new(AtomicBool::new(false));
        let flag = cancel.clone();
        let fd = read.as_raw_fd();
        let reader = tokio::task::spawn_blocking(move || {
            read_line_within(fd, Duration::from_secs(30), &flag)
        });
        let waiter = StdinWaiter { cancel };
        drop(waiter);
        let result = tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .expect("the reader stopped within the grace period")
            .unwrap();
        assert_eq!(result, LineRead::Timeout);
        drop(read);
    }
}
