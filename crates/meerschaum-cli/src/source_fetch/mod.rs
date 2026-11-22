//! The canonical source broker's native side: bounded SQL reads of the sources an API-only job
//! or terminal may read, served by the main API (`crates/meerschaum-server/src/routes/source_fetch.rs`).
//!
//! - [`backend`]: the native SQL data kernel and the main-owned SQLite source replacement.
//! - [`postgres`]: the PostgreSQL backend with pinned native relation identities.
//! - `snapshot`: private, bounded native source snapshots behind client cursors.

pub mod backend;
pub mod postgres;
mod snapshot;
