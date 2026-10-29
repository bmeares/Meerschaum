//! Errors and user-facing messages.

use std::fmt;

/// The Python exception class a compose failure corresponds to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// `FileNotFoundError`.
    FileNotFound,
    /// `FileExistsError`.
    FileExists,
    /// `ValueError`.
    Value,
    /// `TypeError`.
    Type,
    /// `AttributeError`.
    Attribute,
    /// Any other `OSError`.
    Os,
    /// A YAML parsing error (`yaml.YAMLError`).
    Yaml,
    /// A plain `Exception` (what `meerschaum.utils.warnings.error()` raises, e.g. an instance
    /// connector that cannot be reached).
    Exception,
}

impl ErrorKind {
    /// The Python exception class name.
    pub fn python_name(&self) -> &'static str {
        match self {
            ErrorKind::FileNotFound => "FileNotFoundError",
            ErrorKind::FileExists => "FileExistsError",
            ErrorKind::Value => "ValueError",
            ErrorKind::Type => "TypeError",
            ErrorKind::Attribute => "AttributeError",
            ErrorKind::Os => "OSError",
            ErrorKind::Yaml => "YAMLError",
            ErrorKind::Exception => "Exception",
        }
    }
}

/// A compose failure that Python raises as an exception (instead of returning a failed
/// `SuccessTuple`). The message matches Python's `str(exception)`.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{message}")]
pub struct ComposeError {
    /// The exception class.
    pub kind: ErrorKind,
    /// `str(exception)`.
    pub message: String,
}

impl ComposeError {
    /// Build an error.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> ComposeError {
        ComposeError {
            kind,
            message: message.into(),
        }
    }

    /// `ValueError(message)`.
    pub fn value(message: impl Into<String>) -> ComposeError {
        ComposeError::new(ErrorKind::Value, message)
    }

    /// `TypeError(message)`.
    pub fn type_error(message: impl Into<String>) -> ComposeError {
        ComposeError::new(ErrorKind::Type, message)
    }

    /// `AttributeError(message)`.
    pub fn attribute(message: impl Into<String>) -> ComposeError {
        ComposeError::new(ErrorKind::Attribute, message)
    }
}

/// A message compose prints with `warn()` or `info()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// `warn(message, stack=False)`.
    Warn(String),
    /// `info(message)`.
    Info(String),
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::Warn(m) => write!(f, "warning: {m}"),
            Message::Info(m) => write!(f, "info: {m}"),
        }
    }
}

/// The messages emitted while running compose, in order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Messages {
    /// The messages.
    pub items: Vec<Message>,
}

impl Messages {
    /// Record a warning.
    pub fn warn(&mut self, message: impl Into<String>) {
        self.items.push(Message::Warn(message.into()));
    }

    /// Record an info message.
    pub fn info(&mut self, message: impl Into<String>) {
        self.items.push(Message::Info(message.into()));
    }

    /// The warnings only.
    pub fn warnings(&self) -> Vec<&str> {
        self.items
            .iter()
            .filter_map(|m| match m {
                Message::Warn(w) => Some(w.as_str()),
                Message::Info(_) => None,
            })
            .collect()
    }
}
