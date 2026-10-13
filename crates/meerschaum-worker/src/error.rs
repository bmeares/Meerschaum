//! Errors raised by the worker pool and the adapter.

use std::time::Duration;

use meerschaum_connector::ConnectorError;

/// How a Python-side failure was classified by the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyErrorKind {
    /// `NotImplementedError` (or a missing optional method / function).
    NotImplemented,
    /// The worker rejected the request's arguments.
    BadRequest,
    /// Any other exception raised by plugin code.
    Exception,
}

impl PyErrorKind {
    pub(crate) fn parse(s: &str) -> PyErrorKind {
        match s {
            "NotImplemented" => PyErrorKind::NotImplemented,
            "BadRequest" => PyErrorKind::BadRequest,
            _ => PyErrorKind::Exception,
        }
    }
}

/// An error from a worker call.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// The interpreter could not be started.
    #[error("failed to start the Python worker: {0}")]
    Spawn(String),
    /// The worker started but never completed the handshake.
    #[error("the Python worker did not complete the handshake: {0}")]
    Handshake(String),
    /// The worker speaks another protocol version.
    #[error("the Python worker speaks protocol {got}, but this host speaks protocol {expected}")]
    ProtocolVersion {
        /// The host's version.
        expected: u32,
        /// The worker's version.
        got: u32,
    },
    /// A malformed message.
    #[error("worker protocol error: {0}")]
    Protocol(String),
    /// Socket I/O failed.
    #[error("worker I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The worker process died while running `op`. Only that call fails.
    #[error("the Python worker crashed while running `{op}`{detail}")]
    Crashed {
        /// The operation that was running.
        op: String,
        /// The exit status, if known (formatted with a leading separator).
        detail: String,
    },
    /// The call exceeded its timeout; the worker was killed.
    #[error("the Python worker timed out after {after:?} while running `{op}`")]
    Timeout {
        /// The operation that was running.
        op: String,
        /// The timeout.
        after: Duration,
    },
    /// Plugin code raised.
    #[error("{message}")]
    Python {
        /// The classification.
        kind: PyErrorKind,
        /// The exception's type name.
        exception: String,
        /// `str(exception)`.
        message: String,
        /// The formatted traceback.
        traceback: Option<String>,
    },
    /// The pool is shutting down.
    #[error("the Python worker pool is shutting down")]
    ShuttingDown,
}

impl WorkerError {
    /// Whether the worker that produced this error must be discarded (the process is dead,
    /// hung or out of sync). Python exceptions leave the worker usable.
    pub fn is_fatal(&self) -> bool {
        !matches!(self, WorkerError::Python { .. })
    }

    /// Whether this is a Python `NotImplementedError`.
    pub fn is_not_implemented(&self) -> bool {
        matches!(
            self,
            WorkerError::Python {
                kind: PyErrorKind::NotImplemented,
                ..
            }
        )
    }
}

impl From<WorkerError> for ConnectorError {
    fn from(e: WorkerError) -> Self {
        match e {
            WorkerError::Python {
                kind: PyErrorKind::NotImplemented,
                message,
                ..
            } => ConnectorError::NotImplemented(message),
            WorkerError::Python {
                kind: PyErrorKind::BadRequest,
                message,
                ..
            } => ConnectorError::Invalid(message),
            other => ConnectorError::Backend(other.to_string()),
        }
    }
}

/// The result type of worker calls.
pub type WorkerResult<T> = Result<T, WorkerError>;
