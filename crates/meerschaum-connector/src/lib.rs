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
//!
//! Layering: depends on `meerschaum-core` (keys, parameters, users, tokens, `SuccessTuple`) and
//! `meerschaum-kernel` (`Frame`); every instance connector crate (`meerschaum-sql`,
//! `meerschaum-valkey`, `meerschaum-api`), the engine and the server depend on this crate.
#![deny(unsafe_code)]

pub mod models;
pub mod pipe_ops;
pub mod template;
pub mod traits;

pub use async_trait::async_trait;
pub use models::TokenModel;
pub use template::{InstanceProvider, SnapshotResolver, resolve_pipe};
pub use traits::{
    ChunkStream, ChunkedFetch, ColumnIndexInfo, Connector, ConnectorError, ConnectorResult,
    DataQuery, DateBound, FetchedKind, InstanceConnector, PipeId, PipeKeysEntry, PipesKeys,
    PipesKeysQuery, PluginRecord, SyncOptions,
};
