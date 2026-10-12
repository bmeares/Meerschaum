//! `meerschaum-api`: the `APIConnector` client and the shared route and auth models
//! (`docs/design/v5.0.0.md` §4.1, §7.1, §5.8).
//!
//! - [`traits`]: the `Connector` / `InstanceConnector` traits every instance connector implements
//!   (shared here until core and kernel are consolidated; see the module docs).
//! - [`pipe_ops`]: pipe-level helpers and the generic users / plugins / tokens implementations.
//! - [`models`]: route request/response bodies, `ScopedAuth`, JWT claims, engine-state models.
//! - [`routes`]: endpoint paths and path builders.
//! - [`client`] (feature `client`, default): the `APIConnector` over `reqwest`, speaking every
//!   route of §7.1 with the same paths, query parameters and bodies as the v4 Python client.
//!
//! Wire quirks of the v4 client are kept and marked `// PARITY:`.
#![deny(unsafe_code)]

pub mod models;
pub mod pipe_ops;
pub mod routes;
pub mod traits;
pub mod wire;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "client")]
pub use client::APIConnector;
pub use traits::{
    Connector, ConnectorError, ConnectorResult, DataQuery, DateBound, InstanceConnector, PipeId,
    PipesKeys, PipesKeysQuery, SyncOptions,
};
