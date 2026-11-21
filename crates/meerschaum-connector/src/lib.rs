//! **Internal crate** of [Meerschaum](https://meerschaum.io): its API may change in any minor
//! release. Depend on the [`meerschaum`](https://docs.rs/meerschaum) crate instead.
//!
//! `meerschaum-connector`: the `Connector` / `InstanceConnector` traits (port of
//! `meerschaum/connectors/_Connector.py` and `instance/_InstanceConnector.py`) and the pipe-level
//! helpers their default implementations need.
//!
//! - [`traits`]: the traits, their option/result types (`DataQuery`, `SyncOptions`,
//!   `PipesKeys`, `DateBound`, `PipeId`, ...) and `ConnectorError`.
//! - [`pipe_ops`]: value conversions, `Pipe.get_dtypes()` / `enforce_dtypes()`, the essentials
//!   of `Pipe.sync()` for internal pipes, and the generic users / plugins / tokens
//!   implementations over the internal pipes.
//! - [`models`]: the stored token model (`TokenModel`).
//! - [`policy`]: protected connectors and the hidden internal `mrsm` pipes.
//! - [`retry`]: the loop of `retry_connect()` (attempts, waits and messages), over probes the
//!   instance crates implement.
//! - [`schema`]: the schema plans of `verify schema`.
//! - [`sync_status`]: the per-pipe sync status every sync path records (`mrsm:engine_sync_status`).
//! - [`locks`]: pipe locks (`parameters['locks']`) in every instance connector's write paths, and
//!   the defaults of `lock_pipe`, `unlock_pipe` and `set_pipe_access`.
//! - [`acting_team`]: the isolated team a request acts for (5.1).
//! - [`teams`]: the team store over the internal pipe `mrsm:teams` (the defaults of the team
//!   methods) and the renames and deletions that keep pipes' `access` consistent.
//!
//! Layering: depends on `meerschaum-core` (keys, parameters, users, tokens, `SuccessTuple`) and
//! `meerschaum-kernel` (`Frame`); every instance connector crate (`meerschaum-connector-sql`,
//! `meerschaum-connector-valkey`, `meerschaum-connector-api`), the engine and the server depend on this crate.
#![deny(unsafe_code)]

pub mod acting_team;
pub mod api_target;
pub mod batch_delivery;
pub mod inplace;
pub mod locks;
pub mod models;
pub mod mutation;
pub mod passwords;
pub mod pipe_ops;
pub mod policy;
pub mod replace;
pub mod retry;
pub mod schema;
pub mod source_replace;
pub mod sync_metrics;
pub mod sync_status;
pub mod teams;
pub mod template;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod testing;
pub mod traits;
pub mod windows;

pub use async_trait::async_trait;
pub use inplace::{InplaceBegin, InplaceObserver, InplaceSyncOptions};
pub use locks::{edit_refusal, lock_refusal};
pub use models::TokenModel;
pub use policy::{
    check_protected, hide_internal_pipes, hides_internal_pipes, is_internal_pipe, protected_refusal,
};
pub use schema::{SchemaChange, SchemaChangeKind, SchemaPlan};
pub use template::{InstanceProvider, SnapshotResolver, resolve_pipe};
pub use traits::{
    CPI_FIRST_POINT, CPI_PRIME, ChunkStream, ChunkedFetch, ColumnIndexInfo, Connector,
    ConnectorError, ConnectorResult, CpiPolynomials, CpiSamples, CpiSeries, DataQuery, DateBound,
    FetchedKind, InstanceConnector, PipeId, PipeKeysEntry, PipesKeys, PipesKeysQuery, PluginRecord,
    SyncOptions,
};
