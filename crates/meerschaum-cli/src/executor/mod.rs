//! Team executor machinery that the crate uses internally: confined compilation, executor
//! contexts, the Factory, the host API-only runtime and plugin objects served to executors.
//! The public team-executor surfaces sit beside this directory at the crate root
//! ([`executor_client`](crate::executor_client), [`executor_jobs`](crate::executor_jobs),
//! [`executor_profiles`](crate::executor_profiles), [`executor_registry`](crate::executor_registry),
//! [`executor_stack`](crate::executor_stack)), and the executor process itself is
//! [`server::executor`](crate::server).
//!
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

pub(crate) mod compiler;
pub(crate) mod contexts;
pub(crate) mod extensions;
pub(crate) mod factory;
pub(crate) mod host_jobs;
pub(crate) mod host_sources;
pub mod plugin_files;
pub mod plugin_producer;
pub mod plugins;
pub(crate) mod registration;
pub(crate) mod terminal_guard;
