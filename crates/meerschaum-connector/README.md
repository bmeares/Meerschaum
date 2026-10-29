# meerschaum-connector

> **Internal crate.** `meerschaum-connector` is an implementation detail of [Meerschaum](https://meerschaum.io)
> and may change in any minor release. Depend on the [`meerschaum`](https://crates.io/crates/meerschaum)
> crate instead.

The `Connector` and `InstanceConnector` traits, their option and result types, and the pipe-level helpers every instance connector shares.

## The access model on an instance

- **Locks** (`locks`): every instance connector's write methods (`sync_pipe` and replacing
  syncs, `clear_pipe`, `drop_pipe`, `delete_pipe`, `edit_pipe`, `deduplicate_pipe`, schema
  changes, `partition_pipe`, `drop_pipe_indices`) ask `lock_refusal(conn, pipe, op)` /
  `edit_refusal(conn, pipe, patch)` first; the locks in force are the registered ones, read at
  each check. `lock_pipe`, `unlock_pipe` and `set_pipe_access` have defaults over `edit_pipe`
  (changing `locks` is the one edit a `parameters` lock allows). `refused_by_lock(&res)` lets the
  automatic edits a sync makes skip a `parameters` lock quietly.
- **Teams** (`teams`): the `InstanceConnector` team methods (`get_teams`, `get_team`,
  `register_team`, `edit_team`, `delete_team`, `rename_team`, `get_user_teams`) default to the
  internal pipe `mrsm:teams` (`mrsm_teams`), so SQL, Valkey and custom instances share one
  store. Renames and deletions rewrite the `access` of the pipes and saved Pipelines that name the
  team (as access maintenance, allowed under a `parameters` lock); `forget_user` removes a deleted
  user from every team and clears their ownership.
- **Users**: the default lookups match usernames and ids exactly (`pipe_ops::exact_row`), never
  through the `params` grammar (a leading `_` there is a negation).
- `testing` (feature `testing`): the shared checks (`check_locks`, `check_teams`,
  `check_user_lookups`) every instance crate's tests run.

See the [design document](https://github.com/bmeares/Meerschaum/blob/v5/docs/design/v5.0.0.md) for the crate's place in the workspace.
