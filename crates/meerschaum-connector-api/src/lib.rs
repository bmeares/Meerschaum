//! **Internal crate** of [Meerschaum](https://meerschaum.io): its API may change in any minor
//! release. Depend on the [`meerschaum`](https://docs.rs/meerschaum) crate instead.
//!
//! `meerschaum-connector-api`: the `APIConnector` client and the shared route and auth models
//! (`docs/design/v5.0.0.md` §4.1, §7.1, §5.8).
//!
//! - [`traits`], [`pipe_ops`]: re-exports of `meerschaum_connector` (the canonical home of the
//!   `Connector` / `InstanceConnector` traits), kept so `meerschaum_connector_api::traits::*` paths work.
//! - [`models`]: route request/response bodies, `ScopedAuth`, JWT claims, engine-state models.
//! - [`routes`]: endpoint paths and path builders.
//! - [`wire`]: Python-compatible query/form encoding and the pandas `to_json` port.
//! - [`client`] (feature `client`, default): the `APIConnector` over `reqwest`, speaking every
//!   route of §7.1 with the same paths, query parameters and bodies as the v4 Python client.
//!
//! Wire quirks of the v4 client are kept and marked `// PARITY:`.
#![deny(unsafe_code)]

pub mod models;
pub mod routes;
pub mod wire;

pub use meerschaum_connector::{pipe_ops, traits};

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "client")]
pub use client::APIConnector;
pub use meerschaum_connector::{
    Connector, ConnectorError, ConnectorResult, DataQuery, DateBound, InstanceConnector, PipeId,
    PipesKeys, PipesKeysQuery, SyncOptions,
};
