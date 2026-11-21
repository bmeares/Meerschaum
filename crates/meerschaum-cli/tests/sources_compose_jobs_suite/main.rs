#[allow(dead_code)]
#[path = "../python_support/mod.rs"]
mod python_support;

mod compose_execution;
mod compose_explain_instances;
mod compose_include_parent;
mod compose_pipelines;
#[cfg(unix)]
mod compose_registration;
mod engine_registry_scope;
mod explicit_target_sync;
#[cfg(target_os = "linux")]
mod job_prompts;
#[cfg(unix)]
mod job_restart;
#[cfg(unix)]
mod job_startup_gate;
mod listen_custom_connector;
#[cfg(unix)]
mod local_job_history;
mod pipeline_jobs;
mod pipelines;
mod pipelines_api;
mod schedule;
mod schedule_resume;
mod source_fetch_geopackage;
mod source_fetch_http;
mod source_fetch_paging;
mod source_fetch_postgres;
#[cfg(unix)]
mod stack_setup;
mod sync_hooks_output;
#[cfg(unix)]
mod sync_status_cli;
mod upgrade_extensions;
