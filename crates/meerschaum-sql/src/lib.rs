//! `meerschaum-sql`: the v5 SQL connector (see `docs/design/v5.0.0.md` §4.1, §6.2, D13).
//!
//! This wave covers the flavor-aware query-building layer (ported function by function from
//! `meerschaum/utils/sql.py`, `meerschaum/utils/dtypes/sql.py` and `meerschaum/connectors/sql/`)
//! and the driver layer (`sqlx` pools for the PostgreSQL, MySQL and SQLite families).
//! Pipe-level instance methods arrive once `meerschaum-core` lands.
//!
//! Every query builder reproduces the exact text the Python implementation emits; the golden
//! fixtures under `tests/fixtures/` are generated from the Python code in this repository.
#![deny(unsafe_code)]

pub mod dateadd;
pub mod driver;
pub mod dtypes;
pub mod error;
pub mod flavor;
pub mod ident;
pub mod pipe;
pub mod pytext;
pub mod queries;
pub mod uri;
pub mod value;
pub mod where_clause;

pub use error::{Result, SqlError};
pub use flavor::Flavor;
