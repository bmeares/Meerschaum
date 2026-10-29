//! Flavors whose driver was not compiled in (the `mssql`, `oracle` and `duckdb` cargo features).
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
        format!("the `{krate}` driver is compiled in but could not be selected")
    } else {
        format!(
            "meerschaum-connector-sql was built without the `{feature}` cargo feature \
             (the `{krate}` driver)"
        )
    };
    SqlError::DriverUnavailable { flavor, reason }
}
