//! # meerschaum
//!
//! The Rust API of [Meerschaum](https://meerschaum.io): *pipes* sync data between databases, APIs
//! and files, incrementally and idempotently. This crate is the one to depend on, the way
//! `polars` fronts the `polars-*` crates: it re-exports a curated surface and keeps the
//! implementation crates behind cargo features. It contains no logic of its own.
//!
//! ```toml
//! [dependencies]
//! # Use the exact pre-release version (e.g. "5.0.0-rc.12") until 5.0.0 is out: a bare "5" does not match one.
//! meerschaum = { version = "5", default-features = false, features = ["sql", "engine"] }
//! ```
//!
//! A library takes `default-features = false`: the default `cli` feature compiles the whole command
//! line (shell, API server, jobs) for the `mrsm` binary, which a library does not need.
//! `cargo install meerschaum` builds that binary (see below).
//!
//! ## Features
//!
//! | Feature | Adds |
//! |---|---|
//! | `sql` (default) | [`SqlConnector`], [`Flavor`], [`SqlError`] and the [`sql`] module |
//! | `cli` (default) | the standalone `mrsm` binary (`cargo install meerschaum`); implies `sql`; adds no library API |
//! | `oracle` | the Oracle flavor of `sql` (opt-in: needs Oracle Instant Client at runtime) |
//! | `valkey` | [`ValkeyConnector`] and the [`valkey`] module |
//! | `api` | [`APIConnector`] (the client of a Meerschaum API server) and the [`api`] module |
//! | `engine` | the Syncing Engine: [`Pipeline`], [`Plan`], [`Engine`], [`RunOptions`], ... and the [`engine`] module |
//! | `server` | the `axum` HTTP API server (the [`server`] module); implies `api` |
//! | `plugin` | the native plugin SDK: [`Plugin`], [`Request`], [`Reply`], [`export_plugin!`] and the [`plugin`] module |
//! | `full` | every feature above except `oracle` |
//!
//! ## What is semver-stable
//!
//! Stable from 5.0 (breaking changes only in a major release):
//!
//! - the **items re-exported at the crate root** and in [`prelude`]: [`Pipe`], [`Value`], [`Map`],
//!   [`Config`], [`StaticConfig`], [`SuccessTuple`], [`SyncResult`], [`Frame`], the
//!   [`Connector`] / [`InstanceConnector`] traits and their option types, and the feature-gated
//!   connector and engine types listed above;
//! - the **public surface of the `meerschaum` Python package, the `mrsm` CLI, the HTTP routes,
//!   the config layout and the on-disk formats**, which are governed by the compatibility
//!   contract and not by this crate's version.
//!
//! **Not** stable: the modules re-exported as [`core`], [`kernel`], [`connector`], [`sql`],
//! [`valkey`], [`api`], [`engine`] and [`server`] expose the implementation crates
//! (`meerschaum-core`, ...) as they are. Those crates are internal, may change in any minor
//! release, and are published only because this crate depends on them. Reach into the modules
//! when you must, and pin an exact version of `meerschaum` if you do. The native plugin SDK
//! ([`plugin`], from `meerschaum-plugin`) has its own versioned C ABI (`mrsm_plugin_v1`) and is
//! stable independently of the Rust types around it.
//!
//! The `mrsm` command line is built from this crate: `cargo install meerschaum` produces the
//! standalone `mrsm` binary (the `cli` feature, on by default). `pip install meerschaum` installs
//! the same program inside the one Python wheel, whose `mrsm` console script runs it from the
//! extension. `meerschaum-cli` is the internal library behind both.

#![deny(unsafe_code)]

pub use meerschaum_connector as connector;
pub use meerschaum_core as core;
pub use meerschaum_kernel as kernel;

#[cfg(feature = "api")]
pub use meerschaum_connector_api as api;
#[cfg(feature = "sql")]
pub use meerschaum_connector_sql as sql;
#[cfg(feature = "valkey")]
pub use meerschaum_connector_valkey as valkey;
#[cfg(feature = "engine")]
pub use meerschaum_engine as engine;
#[cfg(feature = "plugin")]
pub use meerschaum_plugin as plugin;
#[cfg(feature = "server")]
pub use meerschaum_server as server;

pub use meerschaum_connector::{
    ChunkStream, ColumnIndexInfo, Connector, ConnectorError, ConnectorResult, DataQuery, DateBound,
    InstanceConnector, PipeId, PipesKeys, PluginRecord, SyncOptions, async_trait,
};
pub use meerschaum_core::{
    Config, Map, Pipe, StaticConfig, SuccessTuple, SyncResult, Value, static_config,
};
pub use meerschaum_kernel::Frame;

#[cfg(feature = "api")]
pub use meerschaum_connector_api::APIConnector;
#[cfg(feature = "sql")]
pub use meerschaum_connector_sql::{Flavor, SqlConnector, SqlError};
#[cfg(feature = "valkey")]
pub use meerschaum_connector_valkey::ValkeyConnector;
#[cfg(feature = "engine")]
pub use meerschaum_engine::{
    Engine, EngineConfig, Event, EventKind, Graph, Pipeline, Plan, PlanOptions, RunOptions,
    RunResult, Selection,
};
#[cfg(feature = "plugin")]
pub use meerschaum_plugin::{Plugin, Reply, Request, export_plugin};
#[cfg(feature = "server")]
pub use meerschaum_server::{AppState, ServerConfig};

/// The names most programs import: `use meerschaum::prelude::*;`.
pub mod prelude {
    pub use crate::{
        Config, Connector, Frame, InstanceConnector, Map, Pipe, SuccessTuple, SyncOptions,
        SyncResult, Value,
    };

    #[cfg(feature = "api")]
    pub use crate::APIConnector;
    #[cfg(feature = "sql")]
    pub use crate::SqlConnector;
    #[cfg(feature = "valkey")]
    pub use crate::ValkeyConnector;
    #[cfg(feature = "engine")]
    pub use crate::{Pipeline, RunOptions};
}

/// The crate version (the single workspace version, Cargo form, e.g. `5.0.0-rc.12`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
