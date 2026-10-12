//! Flavors without a Rust driver yet (D13 lists `tiberius`, `oracle` and `duckdb`).
//!
//! Connecting returns [`SqlError::DriverUnavailable`]; nothing here panics.

use crate::error::SqlError;
use crate::flavor::Flavor;

/// The error returned when connecting to a flavor whose driver is not available.
pub(crate) fn unavailable(flavor: Flavor) -> SqlError {
    let (feature, krate, enabled) = match flavor {
        Flavor::Mssql => ("mssql", "tiberius", cfg!(feature = "mssql")),
        Flavor::Oracle => ("oracle", "oracle", cfg!(feature = "oracle")),
        Flavor::Duckdb => ("duckdb", "duckdb", cfg!(feature = "duckdb")),
        other => {
            return SqlError::DriverUnavailable {
                flavor: other,
                reason: "no driver family is registered for this flavor".to_string(),
            };
        }
    };
    let reason = if enabled {
        format!("the `{krate}` driver is not implemented yet (D13)")
    } else {
        format!(
            "meerschaum-sql was built without the `{feature}` cargo feature \
             (and the `{krate}` driver is not implemented yet, D13)"
        )
    };
    SqlError::DriverUnavailable { flavor, reason }
}
