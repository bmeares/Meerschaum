#[path = "../common/mod.rs"]
mod common;

mod anonymous_sdk;
mod contract;
mod data_mutation_authority;
mod f2_reads;
mod graceful_drain;
#[cfg(unix)]
mod limits;
mod native_extensions;
mod openapi_parity;
mod p5_job_access;
mod pipe_operation_admission;
mod pipe_operation_reads;
mod plugin_tokens;
mod rc15_access_matrix;
mod rc15_api;
mod rc15_physical_targets;
mod rc15_replace_api;
mod registered_pipes_hydration;
mod routes;
mod tls;
mod tracked_extensions;
