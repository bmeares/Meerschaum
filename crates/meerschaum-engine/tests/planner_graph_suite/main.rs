#[path = "../common/mod.rs"]
mod common;

mod bare_sync;
mod chunk_events;
mod chunk_pipelining;
mod date_bounds;
mod explicit_target_templates;
mod fetch_chunks;
mod fetch_errors;
mod fetch_stream;
mod golden;
mod lookup_dependencies;
mod lookup_replay;
mod lookup_sql;
mod loop_run_name;
mod mqtt_stream;
mod pipe_schedule;
mod python_glue;
mod read_api;
mod rebuild_upserts;
mod register_hook;
mod source_kwargs;
mod stream_children;
mod templates;
mod v4_sync;
