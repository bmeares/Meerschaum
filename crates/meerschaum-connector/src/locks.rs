//! Pipe locks in the write paths (`parameters['locks']`; the rules are
//! [`meerschaum_core::access`]'s).
//!
//! Locks are enforced everywhere: every instance connector's write methods ask [`lock_refusal`]
//! (sync, replace windows, clear, drop, delete, deduplicate, table schema changes) or
//! [`edit_refusal`] (`edit_pipe`) before they write, so the command line, Python, jobs, the
//! Syncing Engine, the API server, the MCP server and the console all meet the same refusal.
//! `-y`, `--force` and `--allow-protected` never bypass a lock.
//!
//! The locks in force are the **registered** ones (read from the instance on every check, so a
//! lock set by another process applies at once); a pipe that is not registered (or is
//! `temporary`) is locked by its in-memory parameters. Meerschaum's internal `mrsm` pipes are
//! never locked, except saved Pipelines (`mrsm:pipeline:<name>`), which carry `locks` like pipes.
//!
//! Under a `parameters` lock the only edit allowed is to `locks` itself; inside
//! [`access_maintenance`] (the team and user stores renaming or deleting a team, or deleting a
//! user) the `access` an edit moves is allowed too, so a locked pipe never keeps naming a team
//! that no longer exists.

use std::future::Future;

use meerschaum_core::access::{self, ACCESS_KEY, Lock, LockOp, Locks, PipeAccess};
use meerschaum_core::value::apply_patch_maps;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};

use crate::traits::{ConnectorError, ConnectorResult, InstanceConnector};

tokio::task_local! {
    /// Set inside [`access_maintenance`].
    static ACCESS_MAINTENANCE: ();
}

/// Run `fut` as access maintenance: the edits it makes may move a pipe's `access` under a
/// `parameters` lock (a team renamed or deleted, a user deleted). The scope is the task's own:
/// other tasks and threads are not affected.
pub async fn access_maintenance<F: Future>(fut: F) -> F::Output {
    ACCESS_MAINTENANCE.scope((), fut).await
}

/// Whether the current task runs inside [`access_maintenance`].
pub fn in_access_maintenance() -> bool {
    ACCESS_MAINTENANCE.try_with(|_| ()).is_ok()
}

/// Whether locks apply to `pipe`: every pipe but Meerschaum's internal `mrsm` ones (users,
/// tokens, plugins, teams, engine state), except saved Pipelines (`mrsm:pipeline:<name>`).
pub fn is_lockable(pipe: &Pipe) -> bool {
    pipe.connector_keys() != crate::policy::INTERNAL_CONNECTOR_KEYS
        || pipe.metric_key() == "pipeline"
}

/// The registered parameters of `pipe` (`None` when it is not registered or is temporary).
pub async fn registered_parameters<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
) -> ConnectorResult<Option<Map>> {
    if pipe.temporary {
        return Ok(None);
    }
    let attributes = conn.get_pipe_attributes(pipe).await?;
    if attributes.is_empty() {
        return Ok(None);
    }
    Ok(Some(match attributes.get("parameters") {
        Some(Value::Map(m)) => m.clone(),
        Some(Value::Str(text)) => Value::from_json_str(text)
            .map(|v| v.to_map())
            .unwrap_or_default(),
        _ => Map::new(),
    }))
}

/// The locks in force given the registered parameters (`None`: not registered, so the pipe's
/// in-memory parameters).
pub fn locks_of(pipe: &Pipe, registered: Option<&Map>) -> Locks {
    if !is_lockable(pipe) {
        return Locks::new();
    }
    match registered {
        Some(parameters) => Locks::from_parameters(parameters),
        None => Locks::of_pipe(pipe),
    }
}

/// The locks in force on `pipe` (read from `conn`; see [`locks_of`]).
pub async fn pipe_locks<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
) -> ConnectorResult<Locks> {
    if !is_lockable(pipe) {
        return Ok(Locks::new());
    }
    match registered_parameters(conn, pipe).await {
        Ok(registered) => Ok(locks_of(pipe, registered.as_ref())),
        // A connector that cannot say: the in-memory parameters.
        Err(ConnectorError::NotImplemented(_)) => Ok(Locks::of_pipe(pipe)),
        Err(e) => Err(e),
    }
}

/// [`lock_refusal`] with the registered parameters already read (`None`: not registered).
pub fn refusal_of(pipe: &Pipe, registered: Option<&Map>, op: LockOp) -> Option<SuccessTuple> {
    access::lock_refusal(pipe, &locks_of(pipe, registered), op).map(SuccessTuple::fail)
}

/// The refusal of `op` on `pipe` when its locks refuse it (`None`: allowed). The locks are read
/// from `conn`; a read that fails refuses the write (it names the error).
pub async fn lock_refusal<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    op: LockOp,
) -> Option<SuccessTuple> {
    if !is_lockable(pipe) {
        return None;
    }
    match pipe_locks(conn, pipe).await {
        Ok(locks) => access::lock_refusal(pipe, &locks, op).map(SuccessTuple::fail),
        Err(e) => Some(SuccessTuple::fail(format!(
            "Cannot {} {pipe}: its locks could not be read:\n{e}",
            op.verb()
        ))),
    }
}

/// The lock a sync with these options needs: [`LockOp::Replace`] for a replacing sync, else
/// [`LockOp::Sync`].
pub fn sync_op(replace: bool) -> LockOp {
    if replace {
        LockOp::Replace
    } else {
        LockOp::Sync
    }
}

/// Whether `res` is a lock's refusal ([`access::is_lock_refusal`]).
///
/// A sync records what it learns in the pipe's parameters (the dtypes of new special columns,
/// `autoincrement`, widened numeric dtypes): under a `parameters` lock those edits are refused
/// and the sync goes on without them (the lock keeps the parameters as they are; only `sync`
/// refuses the write itself). Callers use this to skip such a refusal quietly.
pub fn refused_by_lock(res: &SuccessTuple) -> bool {
    !res.success && access::is_lock_refusal(&res.message)
}

/// The refusal of `edit_pipe(pipe, patch)` when the registered pipe has a `parameters` lock and
/// the edit changes anything but `locks` (and, inside [`access_maintenance`], `access`). A pipe
/// that is not registered has no registered lock (its edit fails on its own).
pub async fn edit_refusal<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    patch: bool,
) -> Option<SuccessTuple> {
    if !is_lockable(pipe) || pipe.temporary {
        return None;
    }
    let registered = match registered_parameters(conn, pipe).await {
        Ok(Some(parameters)) => parameters,
        Ok(None) | Err(ConnectorError::NotImplemented(_)) => return None,
        Err(e) => {
            return Some(SuccessTuple::fail(format!(
                "Cannot edit {pipe}: its locks could not be read:\n{e}"
            )));
        }
    };
    edit_refusal_of(pipe, &registered, patch)
}

/// [`edit_refusal`] with the registered parameters already read.
pub fn edit_refusal_of(pipe: &Pipe, registered: &Map, patch: bool) -> Option<SuccessTuple> {
    let locks = locks_of(pipe, Some(registered));
    if !locks.contains(Lock::Parameters) {
        return None;
    }
    let new = if patch {
        apply_patch_maps(registered, pipe.raw_parameters())
    } else {
        pipe.raw_parameters().clone()
    };
    if access::parameters_edit_allowed(registered, &new, &locks, in_access_maintenance()) {
        return None;
    }
    access::lock_refusal(pipe, &locks, LockOp::EditParameters).map(SuccessTuple::fail)
}

/// `pipe` carrying `parameters` (a clone).
fn with_parameters(pipe: &Pipe, parameters: Map) -> Pipe {
    let mut out = pipe.clone();
    out.set_parameters(parameters);
    out
}

/// Persist `parameters` as the registered parameters of `pipe` (a full replacement, which every
/// instance connector supports, whether or not it honors `patch`).
async fn write_parameters<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    parameters: Map,
) -> SuccessTuple {
    conn.edit_pipe(&with_parameters(pipe, parameters), false)
        .await
}

/// The registered parameters, or the refusal to give when the pipe is not registered.
async fn registered_or_fail<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
) -> Result<Map, SuccessTuple> {
    match registered_parameters(conn, pipe).await {
        Ok(Some(parameters)) => Ok(parameters),
        Ok(None) => Err(SuccessTuple::fail(format!("{pipe} is not registered."))),
        Err(e) => Err(SuccessTuple::fail(e.to_string())),
    }
}

/// `InstanceConnector::lock_pipe`: add `locks` to the registered pipe's locks (an edit of
/// `locks` alone, allowed under a `parameters` lock).
pub async fn default_lock_pipe<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    locks: &Locks,
) -> SuccessTuple {
    if !is_lockable(pipe) {
        return SuccessTuple::fail(format!("{pipe} is an internal pipe and cannot be locked."));
    }
    let registered = match registered_or_fail(conn, pipe).await {
        Ok(p) => p,
        Err(fail) => return fail,
    };
    let current = Locks::from_parameters(&registered);
    let new = current.with(locks);
    if new == current {
        return SuccessTuple::ok(format!("{pipe} is already locked ({current})."));
    }
    let res = write_parameters(conn, pipe, access::parameters_with_locks(&registered, &new)).await;
    if !res.success {
        return SuccessTuple::fail(format!("Failed to lock {pipe}:\n{}", res.message));
    }
    SuccessTuple::ok(format!("Locked {pipe} ({new})."))
}

/// `InstanceConnector::unlock_pipe`: remove `locks` from the registered pipe's locks (removing
/// `all` removes every lock; removing one kind from `all` keeps the others).
pub async fn default_unlock_pipe<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    locks: &Locks,
) -> SuccessTuple {
    if !is_lockable(pipe) {
        return SuccessTuple::fail(format!("{pipe} is an internal pipe and cannot be locked."));
    }
    let registered = match registered_or_fail(conn, pipe).await {
        Ok(p) => p,
        Err(fail) => return fail,
    };
    let current = Locks::from_parameters(&registered);
    if current.is_empty() {
        return SuccessTuple::ok(format!("{pipe} is not locked."));
    }
    let new = current.without(locks);
    if new == current {
        return SuccessTuple::ok(format!("{pipe} is still locked ({current})."));
    }
    let res = write_parameters(conn, pipe, access::parameters_with_locks(&registered, &new)).await;
    if !res.success {
        return SuccessTuple::fail(format!("Failed to unlock {pipe}:\n{}", res.message));
    }
    if new.is_empty() {
        SuccessTuple::ok(format!("Unlocked {pipe}."))
    } else {
        SuccessTuple::ok(format!("Unlocked {pipe} (still locked: {new})."))
    }
}

/// `InstanceConnector::set_pipe_access`: store `access` as the registered pipe's `access` (a
/// parameter edit: refused under a `parameters` lock).
pub async fn default_set_pipe_access<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    access: &PipeAccess,
) -> SuccessTuple {
    if !is_lockable(pipe) {
        return SuccessTuple::fail(format!(
            "{pipe} is an internal pipe: its access cannot be set."
        ));
    }
    let registered = match registered_or_fail(conn, pipe).await {
        Ok(p) => p,
        Err(fail) => return fail,
    };
    if PipeAccess::from_parameters(&registered) == *access && registered.contains_key(ACCESS_KEY) {
        return SuccessTuple::ok(format!("The access of {pipe} is unchanged."));
    }
    let res = write_parameters(
        conn,
        pipe,
        access::parameters_with_access(&registered, Some(access)),
    )
    .await;
    if !res.success {
        return SuccessTuple::fail(format!(
            "Failed to set the access of {pipe}:\n{}",
            res.message
        ));
    }
    SuccessTuple::ok(format!(
        "Set the access of {pipe}: team {}, visibility {}, edit {}{}.",
        access.team_name(),
        access.visibility,
        access.edit,
        access
            .owner
            .as_deref()
            .map(|o| format!(", owner {o}"))
            .unwrap_or_default()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipe(params: &str) -> Pipe {
        Pipe::builder("plugin:x", "y")
            .instance("sql:test")
            .parameters(Value::from_json_str(params).unwrap().to_map())
            .build()
            .unwrap()
    }

    #[test]
    fn internal_pipes_are_not_locked_but_pipelines_are() {
        let users = Pipe::builder("mrsm", "users").build().unwrap();
        assert!(!is_lockable(&users));
        let pipeline = Pipe::builder("mrsm", "pipeline")
            .location(Some("weather"))
            .build()
            .unwrap();
        assert!(is_lockable(&pipeline));
        assert!(is_lockable(&pipe("{}")));
    }

    #[test]
    fn registered_locks_win_over_in_memory_ones() {
        let p = pipe(r#"{"locks": []}"#);
        let registered = Value::from_json_str(r#"{"locks": ["sync"]}"#)
            .unwrap()
            .to_map();
        assert!(refusal_of(&p, Some(&registered), LockOp::Sync).is_some());
        // An unregistered pipe is locked by its own parameters.
        let p = pipe(r#"{"locks": ["clear"]}"#);
        assert!(refusal_of(&p, None, LockOp::Clear).is_some());
        assert!(refusal_of(&p, None, LockOp::Sync).is_none());
        // A registered pipe without locks ignores in-memory ones (they are not persisted).
        assert!(refusal_of(&p, Some(&Map::new()), LockOp::Clear).is_none());
    }

    #[test]
    fn parameter_locks_allow_lock_edits_and_maintenance() {
        let registered =
            Value::from_json_str(r#"{"locks": ["parameters"], "columns": {"id": "a"}}"#)
                .unwrap()
                .to_map();
        let unlock = pipe(r#"{"locks": [], "columns": {"id": "a"}}"#);
        assert!(edit_refusal_of(&unlock, &registered, false).is_none());
        let edit = pipe(r#"{"locks": ["parameters"], "columns": {"id": "b"}}"#);
        let refused = edit_refusal_of(&edit, &registered, false).unwrap();
        assert!(
            refused.message.contains("it is locked (parameters)"),
            "{}",
            refused.message
        );
        // A patch that only touches `locks`.
        let patch = pipe(r#"{"locks": ["parameters", "sync"]}"#);
        assert!(edit_refusal_of(&patch, &registered, true).is_none());
        // `access` moves only under maintenance.
        let moved =
            pipe(r#"{"locks": ["parameters"], "columns": {"id": "a"}, "access": {"team": null}}"#);
        assert!(edit_refusal_of(&moved, &registered, false).is_some());
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let allowed = rt.block_on(access_maintenance(async {
            assert!(in_access_maintenance());
            edit_refusal_of(&moved, &registered, false)
        }));
        assert!(allowed.is_none());
        assert!(!in_access_maintenance());
    }
}
