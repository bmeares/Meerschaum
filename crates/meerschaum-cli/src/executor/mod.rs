//! Team executors, both sides: the main API's client, job catalog, launch profiles, executor
//! registry and generated stack, and the machinery the crate uses internally (confined
//! compilation, executor contexts, the Factory, the host API-only runtime and plugin objects
//! served to executors). The executor process itself is [`server::executor`](crate::server).
//!
//! - [`client`]: signed main-API clients for one team's ordinary executor API.
//! - [`jobs`]: main-owned identity, routing and the canonical catalog of delegated jobs.
//! - [`profiles`]: preparation of API-only launches.
//! - [`registry`]: main-API executor identities and the desired deployment shared with
//!   `stack up`.
//! - [`stack`]: the sibling executor services `mrsm stack up` adds.
//! - `compiler`: the confined compiler for single-file native sources, its administrator
//!   provisioning policy and its confined runner.
//! - `contexts`: executor-local paths and configuration for signed, main-owned launches.
//! - `extensions`: the fixed native consumer of a sealed main-selected extension operation.
//! - `factory`: the administrator-installed native material in an executor image.
//! - `host_jobs`, `host_sources`: the host API-only job runtime and its source copies.
//! - [`plugin_files`], [`plugin_producer`], [`plugins`]: immutable plugin source objects, the
//!   projection of a confined runtime's discovery output, and host-owned plugin catalogs.
//! - `registration`: the fixed confined connector-register operation.
//! - `terminal_guard`: the native lifetime owner of an API-only terminal.

pub mod client;
pub(crate) mod compiler;
pub(crate) mod contexts;
pub(crate) mod extensions;
pub(crate) mod factory;
pub(crate) mod host_jobs;
pub(crate) mod host_sources;
pub mod jobs;
pub mod plugin_files;
pub mod plugin_producer;
pub mod plugins;
pub mod profiles;
pub(crate) mod registration;
pub mod registry;
pub mod stack;
pub(crate) mod terminal_guard;
