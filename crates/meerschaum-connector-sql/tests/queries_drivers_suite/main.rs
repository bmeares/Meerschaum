#[macro_use]
#[path = "../common/mod.rs"]
mod common;

mod driver_mysql;
mod driver_native;
mod driver_postgres;
mod driver_sqlite;
mod golden;
mod retry_connect;
mod uri;
mod w16_default_schema;
mod w17b_read_ddl;
mod w17b_uri_wins;
mod w17c_extensions;
mod w17c_geometry_srid;
mod w18_fail_fast;
mod w18_fx10_sqlite_inplace_contention;
mod w18_fx4_ping;
#[cfg(feature = "duckdb")]
mod w18_fx9_duckdb_first_use;
mod w18_perf;
mod w5sql;
mod w6b_templates;
mod w8_explicit_target;
mod w9u_pg_params;
