//! **Internal crate** of [Meerschaum](https://meerschaum.io): its API may change in any minor
//! release. Depend on the [`meerschaum`](https://docs.rs/meerschaum) crate instead.
//!
//! `meerschaum-connector-sql`: the v5 SQL connector (see `docs/design/v5.0.0.md` §4.1, §6.2, D13).
//!
//! This wave covers the flavor-aware query-building layer (ported function by function from
//! `meerschaum/utils/sql.py`, `meerschaum/utils/dtypes/sql.py` and `meerschaum/connectors/sql/`)
//! and the driver layer (`sqlx` pools for the PostgreSQL, MySQL and SQLite / GeoPackage
//! families; `tiberius` for MSSQL, `duckdb` and `oracle` behind their cargo features).
//! Pipe attributes, the dtype vocabulary, `Value` and the Python text helpers come from
//! `meerschaum-core`; query results convert into the kernel's `Frame` (see
//! [`driver::SqlDriver::read_frame`]). [`instance::SqlConnector`] ports the instance methods
//! (pipes, users, plugins, maintenance) and implements `meerschaum_connector::InstanceConnector`.
//!
//! Every query builder reproduces the exact text the Python implementation emits; the golden
//! fixtures under `tests/fixtures/` are generated from the Python code in this repository.
#![deny(unsafe_code)]

pub mod dateadd;
pub mod driver;
pub mod dtypes;
pub mod error;
pub mod facade;
pub mod flavor;
pub mod ident;
pub mod instance;
pub mod pipe;
pub mod queries;
pub mod uri;
pub mod where_clause;

pub use error::{Result, SqlError};
pub use flavor::Flavor;
pub use instance::SqlConnector;
