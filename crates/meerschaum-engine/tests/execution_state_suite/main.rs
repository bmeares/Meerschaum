#[path = "../common/mod.rs"]
mod common;

mod continuous;
mod crash;
mod crash_restart;
mod duration_history;
mod event_persistence;
mod execution_write_only;
mod first_run_ledger;
mod forget_state;
mod immutable_sync;
mod inplace_sync;
mod internal_schema;
mod leases;
mod leases_backends;
mod legacy_flush;
mod metrics_persistence;
mod pg_state;
mod pipe_locks;
mod protected_sync;
mod remote_state_forget;
mod replace_chunks;
mod replace_ledger;
mod state_cost;
mod sync_status;
