#[macro_use]
#[path = "../common/mod.rs"]
mod common;

mod access_model;
mod engine_history_precision;
mod engine_state_placement;
mod fetch_stream;
mod frame_roundtrip;
mod inplace_null_replacement;
mod inplace_partitioned;
mod inplace_rollups;
mod lookup_upserts;
mod metrics_initialization;
mod metrics_partitioning;
mod metrics_window;
mod native_inference;
mod native_parent_pushdown;
mod null_key_upserts;
mod null_markers;
#[cfg(feature = "oracle")]
mod oracle_internal_indices;
mod replace_sync;
mod upsert_changed_only;
