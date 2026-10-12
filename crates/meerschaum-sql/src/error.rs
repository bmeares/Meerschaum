//! Errors raised by query building and the driver layer.

use crate::flavor::Flavor;

/// The crate's error type.
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    /// `clean()` rejected a substring (`utils/sql.py:668`, `error(f"Invalid string: '{substring}'")`).
    #[error("Invalid string: '{0}'")]
    InvalidString(String),

    /// `dateadd_str()` was given a datepart outside `_VALID_DATEPARTS`.
    #[error(
        "Invalid datepart '{0}'. Must be one of: ['day', 'hour', 'minute', 'month', 'second', 'year']"
    )]
    InvalidDatepart(String),

    /// A function was called with arguments Python would reject (`ValueError` / `TypeError`).
    #[error("{0}")]
    InvalidArgument(String),

    /// `find_top_level_select_index` could not locate the top-level `SELECT`.
    #[error("Could not locate the top-level SELECT of the following query:\n{0}")]
    NoTopLevelSelect(String),

    /// The URI or connector attributes could not be parsed.
    #[error("Invalid connection: {0}")]
    InvalidConnection(String),

    /// The flavor's driver is not compiled in or not implemented yet (D13).
    #[error("No driver is available for flavor '{flavor}': {reason}")]
    DriverUnavailable { flavor: Flavor, reason: String },

    /// A database driver error.
    #[error(transparent)]
    Driver(#[from] sqlx::Error),

    /// An Arrow error while building record batches.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    /// A pipe attribute could not be computed (Python raises from the `Pipe` property).
    #[error(transparent)]
    Pipe(#[from] meerschaum_core::pipe::PipeError),

    /// Query results could not be converted into a kernel `Frame`.
    #[error(transparent)]
    Frame(#[from] meerschaum_kernel::frame::FrameError),

    /// A value could not be converted to the column's Arrow type.
    #[error("Failed to convert column '{column}': {reason}")]
    Conversion { column: String, reason: String },
}

/// Convenience alias.
pub type Result<T, E = SqlError> = std::result::Result<T, E>;
