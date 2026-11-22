# 🔒 Access and Locks

Meerschaum is open by default—every user sees and edits every pipe within their permissions until you configure access or locks. Two reserved [parameters](/reference/pipes/parameters/) control who owns a pipe and what can happen to it:

- **`access`:** Sets owner, team, visibility, and edit mode for API servers, MCP, and the web console.
- **`locks`:** Blocks destructive or mutating operations (`sync`, `clear`, `drop`, `delete`, `parameters`) **everywhere**—CLI, Python, jobs, Pipelines, API, and console.

!!! note "Where each boundary applies"
    Teams, visibility, and permissions apply through an API server (`api:` instances, MCP, web console). Anyone holding raw database credentials can query the database directly—use separate database users or [isolated teams](/reference/api-instance/teams/) for hard DB isolation. **Locks** apply everywhere, even locally and for admins.

For CLI commands (`mrsm lock pipes`, `mrsm edit pipes access`, `mrsm register teams`), see [Teams, Access and Locks](/reference/actions/teams-and-access/).

## Locks

A lock refuses matching operations until explicitly unlocked—`-y`, `--force`, and `--allow-protected` never bypass it:

=== "Python"
    ```python
    import meerschaum as mrsm

    pipe = mrsm.Pipe('sql:main', 'weather', instance='sql:main')
    pipe.lock('drop', 'delete')
    pipe.locks                        # ['drop', 'delete']

    pipe.clear(begin='2024-01-01')    # allowed: no `clear` lock
    pipe.drop()                       # (False, "Cannot drop ...: it is locked (drop)...")

    pipe.unlock('drop')               # still locked: delete
    pipe.unlock()                     # unlock all
    pipe.lock()                       # lock `all`
    ```

=== "CLI"
    ```bash
    mrsm lock pipes -c sql:main -m weather --locks drop delete
    mrsm unlock pipes -c sql:main -m weather --locks drop
    ```

| Lock | Refuses |
|---|---|
| `parameters` | Parameter edits (`pipe.edit()`, `set_access()`, `edit pipes`). Syncs still write; lock edits stay allowed. |
| `sync` | Syncs, replaced windows (`replace=True`), deduplication, and verification syncs. |
| `clear` | Row deletes (`clear`, replaced windows, rebuilds). |
| `drop` | Drops, rebuilds, `verify schema`, repartitioning, and dropping indices. |
| `delete` | Deleting (unregistering) the pipe. |
| `all` | All of the above. |

Compound operations require every affected lock to be open:

| Operation | Refused by |
|---|---|
| `sync --replace` / `pipe.sync(..., replace=True)` | `sync`, `clear` |
| Definition rebuild | `sync`, `clear`, `drop` |
| `delete pipes` / `pipe.delete()` | `drop`, `delete` |
| `pipe.delete(drop=False)` | `delete` |
| `repartition pipes` / `pipe.repartition()` | `drop`, `parameters` |

- Locks are re-read from the instance before every write, so changes from another process apply immediately.
- Saved [Pipelines](/reference/pipelines/) also support locks; internal `mrsm` pipes do not.

### Locks in Pipeline runs

During a [Pipeline](/reference/pipelines/) run (`--parents`, `Pipeline.sync()`, or scheduled Pipeline jobs), a `sync`-locked pipe is skipped with status **locked** (not a failure), and downstream pipes continue using its existing data. A direct `pipe.sync()` on a locked pipe is refused immediately.

## Access

```python
pipe.set_access(team='analytics', visibility='private', edit='owner', owner='carol')
pipe.owner, pipe.team, pipe.visibility, pipe.edit_mode
# ('carol', 'analytics', 'private', 'owner')

pipe.set_access(owner=None)   # clear owner, keep other keys
pipe.set_access(team=None)    # move back to Everyone
```

| Key | Values |
|---|---|
| `owner` | Username, or `None`. |
| `team` | Team name, or `None` for **Everyone**. |
| `visibility` | `team` (default: team members + owner), `private` (owner, team managers, admins), `everyone` (all users). |
| `edit` | `team` (default: anyone who can see it), `owner` (owner, team managers, admins). |

- Pipes registered through an `api:` instance or the web console default to the creator as `owner` and their primary team as `team`.
- Only the owner, team managers, and admins can edit `access` or locks.
- `copy pipes` preserves locks and never copies `access`. Deleting a user clears ownership while leaving pipes with their teams.

## Teams

Every user belongs to **Everyone** (cannot be deleted, renamed, or left). Admins create additional teams with **members** and **managers** (managers can add/remove members and manage the team's pipe access):

```python
import meerschaum as mrsm

team = mrsm.Team('analytics', instance='sql:main', members=['alice', 'bob'], managers=['carol'])
team.register()

team.add_members('dave')
team.add_managers('bob')
team.remove_members('alice')

team.rename('data-team')   # updates pipes, Pipelines, and jobs
team.delete()              # moves pipes and legacy jobs to Everyone
```

Team names are case-insensitive (`ANALYTICS` matches `analytics`). Members must be registered users.

## Users, roles and permissions

Roles are presets of token scopes (`meerschaum.core.access.ALL_PERMISSIONS`):

| Role | Permissions |
|---|---|
| `viewer` | Read visible pipes, jobs, logs, plugins, and connectors list; edit own account. |
| `editor` | Viewer, plus register, edit, and sync pipes (`pipes:register`, `pipes:write`, `pipes:define_sql`), start jobs, and run read-only SQL. |
| `operator` | Editor, plus execute actions (`actions:execute`), use the web terminal (`webterm:use`), and create/stop/pause/delete jobs. |
| `admin` | All permissions, including users, teams, and connector details (`type: admin`). |

```python
from meerschaum.core import User

user = User('bob', instance='sql:main')
user.set_role('viewer')
user.set_permissions(['pipes:read', 'logs:read'])   # user.role == 'custom'
user.teams                                          # [('Everyone', 'member'), ('analytics', 'manager')]
```

- A user without an explicit role or permission list inherits `api:permissions:default_role` (`editor` by default).
- A token's scopes always intersect with its owner's current permissions.

## Checking rules in Python

Plugins and console pages can use the same access helpers the API server runs:

```python
from meerschaum.core.access import Principal, can_see, can_edit, filter_visible_pipes, get_principal

alice = get_principal('alice', instance='sql:main')
can_see(alice, pipe), can_edit(alice, pipe)
visible = filter_visible_pipes(alice, mrsm.get_pipes(instance='sql:main', as_list=True))
```

| Helper | Purpose |
|---|---|
| `can_see(principal, access)` / `can_edit(...)` | Check visibility and edit rights for a pipe or access dict. |
| `can_manage_access(...)` / `can_lock(...)` | Check if caller is owner, team manager, or admin. |
| `can_see_email(viewer, user_teams, target_username=)` | Check email visibility (self, team manager, or admin). |
| `check_lock(locks, op)` / `lock_refusal(pipe, locks, op)` | Check whether an operation (`sync`, `clear`, `drop`, etc.) is locked. |
| `user_permissions(...)`, `role_permissions(...)`, `token_permissions(...)` | Compute effective permissions. |

Custom [instance connectors](/reference/connectors/instance-connectors/) inherit teams, access, and locks automatically—teams are stored in `Pipe('mrsm', 'teams')` and locks are checked before every write.
