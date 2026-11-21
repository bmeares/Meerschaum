#[macro_use]
#[path = "../common/mod.rs"]
mod common;

mod engine_history_precision;
mod engine_state_placement;
mod fetch_stream;
mod frame_roundtrip;
mod inplace_null_replacement;
mod inplace_partitioned;
mod lookup_upserts;
mod metrics_initialization;
mod metrics_partitioning;
mod metrics_window;
mod native_inference;
mod native_parent_pushdown;
#[cfg(feature = "oracle")]
mod oracle_internal_indices;
mod rc15_access;
mod rc15_inplace_rollups;
mod rc15_null_key_upserts;
mod rc15_null_markers;
mod rc15_replace;
mod upsert_changed_only;
