//! Separate team executors: authenticated control, delegation and untrusted response handling.
//!
//! A team's ordinary Meerschaum API receives only its own filtered configuration. The main
//! API retains identity, authorization and routing; an executor never receives its signing key.

pub mod admission;
pub mod callbacks;
pub mod context;
pub mod control;
pub mod egress;
pub mod environment;
pub mod extensions;
pub mod factory_artifacts;
pub mod factory_attestation;
pub mod gateway;
pub mod pipe_operation;
pub mod plugin_caller;
pub mod plugin_capabilities;
pub mod plugin_delivery;
pub mod plugin_discovery;
pub mod plugin_handlers;
pub mod plugin_host;
pub mod plugins;
pub mod registration;
pub mod source_ceiling;
pub mod storage;
pub mod terminal;
