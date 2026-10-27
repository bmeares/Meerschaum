//! Per-pipe sync status (rc15 contract C2) from the engine's side: every sync the engine runs
//! records its pipe's outcome through [`meerschaum_connector::sync_status`] (one row per pipe in
//! `mrsm:engine_sync_status` on the pipe's instance).
//!
//! | Sync | Recorded by | Once per |
//! |---|---|---|
//! | a node of an engine run (flat `sync pipes`, laps, graph runs) | [`crate::executor::execute`], through [`crate::StateWriter::record_sync_status`] | node, after its windows and retries |
//! | a bare sync ([`crate::Engine::sync_pipe`], Python's `pipe.sync()`, worker callbacks, native plugin host calls) | [`crate::sync::sync_pipe`] | call |
//! | a stream flush, a `--listen` callback sync | [`crate::sync::sync_pipe`] | flush / callback |
//! | a failed `--listen` start or subscription | [`crate::native::NativeLegacySink`], [`crate::stream::supervise`] | failure |
//!
//! A sync inside another sync of the same pipe (a plugin's `pipe.sync()` inside its own
//! `sync()`) is part of it ([`crate::SyncArgs::status_owner`]). A lease held by another process
//! (it is syncing the pipe) is not a sync of this one and records nothing. Writes are debounced,
//! run in the background and are bounded in time, so they never slow or fail a sync (see
//! [`meerschaum_connector::sync_status`]).

use std::sync::Arc;

use meerschaum_core::Pipe;
use meerschaum_core::success::SyncResult;

pub use meerschaum_connector::sync_status::{
    SyncOutcome, SyncSource, SyncStatus, current_job, detect_source, process_role,
    set_default_process_role, set_process_role,
};

use crate::registry::ConnectorRegistry;

/// The outcome of a sync result, with this process's source and job ([`detect_source`];
/// `graph_run`: a node of an engine graph run).
pub fn outcome_of(result: &SyncResult, graph_run: bool) -> SyncOutcome {
    SyncOutcome::detected(result.success, result.message.clone(), graph_run)
        .with_counts(result.inserted, result.updated, result.upserted)
        .with_duration(result.duration)
}

/// Record `result` as the latest sync of `pipe` on its instance (best effort, written in the
/// background; see the module docs).
pub fn record_result(
    registry: &dyn ConnectorRegistry,
    pipe: &Pipe,
    result: &SyncResult,
    graph_run: bool,
) {
    if !meerschaum_connector::sync_status::is_recorded(pipe) {
        return;
    }
    let conn = match registry.instance(pipe.instance_keys()) {
        Ok(conn) => conn,
        Err(e) => {
            tracing::debug!("Cannot record the sync status of {pipe}: {e}");
            return;
        }
    };
    record_outcome(conn, pipe, outcome_of(result, graph_run));
}

/// Record `outcome` for `pipe` on `conn` (best effort, written in the background).
pub fn record_outcome(
    conn: Arc<dyn meerschaum_connector::InstanceConnector>,
    pipe: &Pipe,
    outcome: SyncOutcome,
) {
    meerschaum_connector::sync_status::record(conn, pipe, outcome);
}

/// The sync status of `pipe` on its instance (`None` if no sync of it was recorded).
pub async fn get_sync_status(
    registry: &dyn ConnectorRegistry,
    pipe: &Pipe,
) -> Result<Option<SyncStatus>, String> {
    let conn = registry.instance(pipe.instance_keys())?;
    conn.get_sync_status(pipe).await.map_err(|e| e.to_string())
}

/// Every recorded sync status on `instance_keys`.
pub async fn get_sync_statuses(
    registry: &dyn ConnectorRegistry,
    instance_keys: &str,
) -> Result<Vec<SyncStatus>, String> {
    let conn = registry.instance(instance_keys)?;
    conn.get_sync_statuses(instance_keys)
        .await
        .map_err(|e| e.to_string())
}
