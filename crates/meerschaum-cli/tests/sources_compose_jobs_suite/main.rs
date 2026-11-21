#[allow(dead_code)]
#[path = "../python_support/mod.rs"]
mod python_support;

mod compose_execution;
mod compose_pipelines;
#[cfg(unix)]
mod compose_registration;
mod explicit_target_sync;
#[cfg(target_os = "linux")]
mod job_prompts;
#[cfg(unix)]
mod job_restart;
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
mod w17c_upgrade_extensions;
mod w18_compose;
#[cfg(unix)]
mod w18_job_startup;
mod w9_compose_instance;
mod w9_engine_sources;
mod w9_listen_v4_mqtt;
