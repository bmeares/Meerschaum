#[macro_use]
#[path = "../common/mod.rs"]
mod common;

mod connect_fail_fast;
mod cross_pipe_templates;
mod default_schema;
mod driver_mysql;
mod driver_native;
mod driver_postgres;
mod driver_sqlite;
#[cfg(feature = "duckdb")]
mod duckdb_concurrent_first_use;
mod explicit_target_templates;
mod extension_updates;
mod geometry_srid;
mod golden;
mod internal_upserts_and_sqlite_docs;
mod pool_ping;
mod postgres_bound_params;
mod read_ddl;
mod retry_connect;
mod sqlite_inplace_contention;
mod sync_round_trips;
mod uri;
mod uri_wins;
