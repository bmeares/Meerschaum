#[macro_use]
#[path = "../common/mod.rs"]
mod common;
#[path = "../fork_helpers/mod.rs"]
mod fork_helpers;

mod catalog_cache;
mod f2_instance_features;
mod f2_protected;
mod index_database_scope;
mod instance_cross_impl;
mod instance_dedup;
mod instance_dtypes;
mod instance_enforce_false;
mod instance_geopackage;
mod instance_golden;
mod instance_indices;
mod instance_inplace;
mod instance_leases;
mod instance_maintenance;
mod instance_partition;
mod instance_pipes_keys;
mod instance_smoke;
mod instance_sync;
mod instance_verify;
mod internal_initialization;
mod pipeline_metadata_move;
mod readonly_registration;
mod team_database;
