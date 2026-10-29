# 🔒 Access and Locks

Meerschaum is open by default. On an instance where nobody sets up teams, access or locks, every
user sees and changes every pipe within their permissions, and nothing here needs a thought. When
you need more, two reserved [parameters](/reference/pipes/parameters/) say who a pipe belongs to and
what may happen to it:

- **`access`**: the pipe's owner, team and visibility. The API server, the MCP server and the web
  console decide with it who may see and change the pipe.
- **`locks`**: the operations the pipe refuses (syncs, clears, drops, ...), **everywhere**: the
  command line, Python, background jobs, the Syncing Engine, the API and the web console.

!!! note "Who enforces what"

    Teams, visibility and permissions apply to the users an API server serves (every `api:`
    instance, its MCP tools and its web console). Anyone holding an instance's database
    credentials works with it directly and sees everything in it: the credentials are the trust
    boundary. To keep teams' data apart for certain, give each team its own connectors and
    database users. Locks are the exception: they refuse their operations whoever asks.

The command line does the same through `mrsm lock pipes`, `mrsm edit pipes access` and the team
actions: see [Teams, Access and Locks](/reference/actions/teams-and-access/).

## Locks

A lock refuses its operations on a pipe until someone removes it. `-y`, `--force` and
`--allow-protected` (or `allow_protected()` in Python) never get past a lock.

```python
import meerschaum as mrsm

pipe = mrsm.Pipe('sql:main', 'weather', instance='sql:main')
pipe.lock('drop', 'delete')       # (True, "Locked Pipe('sql:main', 'weather') (drop, delete).")
pipe.locks                        # ['drop', 'delete']

pipe.clear(begin='2024-01-01')    # allowed: no `clear` lock
pipe.drop()
# (False, "Cannot drop Pipe('sql:main', 'weather'): it is locked (drop). Unlock it with
#  `mrsm unlock pipes -c sql:main -m weather --locks drop` or in the web console (Manage > Locks).")

pipe.unlock('drop')               # still locked: delete
pipe.unlock()                     # every lock
pipe.lock()                       # `all`
```

| Lock | Refuses |
|---|---|
| `parameters` | Parameter edits (`pipe.edit()`, `update_parameters(..., persist=True)`, `set_access()`, `edit pipes`). Changing the locks themselves stays allowed. |
| `sync` | Writes: syncs, replaced windows (`replace=True`), deduplication, verification syncs. |
| `clear` | Row deletes: clears, replaced windows, rebuilds. |
| `drop` | Drops, rebuilds and changes to the table's schema: `verify schema` (`pipe.verify_schema(apply=True)`), repartitioning, dropping indices. |
| `delete` | Deleting (unregistering) the pipe. |
| `all` | All of the above. |

Some operations need more than one kind unlocked:

| Operation | Refused by |
|---|---|
| `pipe.sync(df, begin=..., end=..., replace=True)`, `sync pipes --replace` | `sync`, `clear` |
| A rebuild (a definition change, approved with `--yes`) | `sync`, `clear`, `drop` |
| `pipe.delete()`, `delete pipes` (they drop the table first) | `drop`, `delete` |
| `pipe.delete(drop=False)` (the registration only) | `delete` |
| `pipe.repartition(...)`, `mrsm repartition pipes` (it rebuilds the table and records the width) | `drop`, `parameters` |

- A refusal is a `SuccessTuple` like any other (`(False, "Cannot ...: it is locked (...)...")`).
  It names the pipe, the lock and the command that removes it.
- The registered locks are the ones in force, read again before every write: a lock set in another
  process or by another user applies at once. A pipe that is not registered keeps the locks you give
  it in memory and saves them when it is registered.
- **Under a `parameters` lock, syncs still write.** What a sync would otherwise record in the
  parameters (the dtype of a new JSON, UUID, numeric or geometry column, a new `autoincrement`) is
  not recorded: the lock keeps the parameters as they are.
- Renaming or deleting a team and deleting a user still update the `access` of locked pipes, so a
  pipe never names a team that no longer exists.
- Meerschaum's internal pipes (users, tokens, plugins, teams, engine state) have no locks. Saved
  [Pipelines](/reference/pipelines/) do: their registry pipe `Pipe('mrsm', 'pipeline', name)`
  carries `locks` like any pipe.

### Locks in Pipeline runs

In a [Pipeline](/reference/pipelines/) run (`pipe.sync(parents=True)`, `Pipeline.sync()`,
`mrsm sync pipes --parents`, a scheduled Pipeline job), a pipe locked against syncs is not synced
and its status is **locked**. It is not a failure: the run succeeds, and the pipes downstream sync
from its current data. A pipe whose definition changed is rebuilt (cleared, then synced again)
only when `drop` and `clear` are unlocked; until then it is **locked** too, with `--yes` or
without. The run's warnings say which pipes were locked and how to unlock them.

A direct sync of a locked pipe (`pipe.sync()`, `mrsm sync pipes -m weather`) is refused like any
other locked write.

## Access

```python
pipe.set_access(team='analytics', visibility='private', edit='owner', owner='carol')
pipe.access
# {'owner': 'carol', 'team': 'analytics', 'visibility': 'private', 'edit': 'owner'}
pipe.owner, pipe.team, pipe.visibility, pipe.edit_mode
# ('carol', 'analytics', 'private', 'owner')

pipe.set_access(owner=None)       # no owner (the other keys keep their values)
pipe.set_access(team=None)        # back to Everyone
```

| Key | Values |
|---|---|
| `owner` | A username, or `None`. |
| `team` | A team name; `None` is the built-in team **Everyone**. |
| `visibility` | `team` (default): the team's members (and the owner). `private`: the owner, the team's managers and admins. `everyone`: every user. |
| `edit` | `team` (default): anyone who can see the pipe may change it within their permissions. `owner`: the owner, the team's managers and admins. |

A pipe without `access` belongs to Everyone, with visibility `team` and edit `team`: everyone sees
and changes it. `access` is an ordinary parameter, so compose files, `--params` and
`pipe.parameters` may set it too. Through an `api:` instance:

- a pipe someone registers belongs to them (admins may name another owner) and to their primary
  team unless they name another;
- only the owner, the team's managers and admins change a pipe's `access`, lock it or unlock it;
  someone other than an admin may only give a pipe to a team they are in, and only managers and
  admins give it to another owner (an owner may give up ownership).

`copy pipes` keeps the source's locks and never copies its access. Deleting a user clears their
ownership: their pipes stay with their teams.

## Teams

Every user is a member of the built-in team **Everyone**, which cannot be deleted, renamed or left
(its record may name managers). Admins create more teams; a user can be in several, as a
**member** or a **manager**. Managers add and remove members of their team and manage the access
of its pipes. Teams live on the instance beside users and tokens (the internal pipe
`Pipe('mrsm', 'teams')`, table `mrsm_teams`).

```python
import meerschaum as mrsm

team = mrsm.Team('analytics', instance='sql:main', members=['alice', 'bob'], managers=['carol'])
team.register()                   # (True, "Registered team 'analytics'.")

team.add_members('dave')          # saved at once
team.add_managers('bob')
team.remove_members('alice')
team.members                      # ['bob', 'carol', 'dave'] (managers included)
team.managers                     # ['bob', 'carol']

team.rename('data-team')          # its pipes and jobs follow
team.delete()                     # its pipes and jobs move to Everyone

[t.name for t in mrsm.get_teams('sql:main')]     # ['Everyone', ...]
```

- Team names follow the username rules (letters, digits and `_ - . @`) and are unique without
  case: `mrsm.Team('ANALYTICS')` is the team `analytics`.
- Members must be registered users.
- Renaming a team renames it in the `access` of its pipes and saved Pipelines, in its users'
  primary team and in the jobs started for it; deleting one moves them to Everyone.

## Users, roles and permissions

Permissions are the token scopes (`meerschaum.core.access.ALL_PERMISSIONS`), from `pipes:read` to
`webterm:use`. Roles are presets of them:

| Role | Permissions |
|---|---|
| `viewer` | Read pipes, jobs, logs, plugins and the connectors list (secrets masked); read and edit their own account. |
| `editor` | Viewer, plus register, edit and sync pipes (SQL definitions included: `pipes:register`, `pipes:write`, `pipes:define_sql`), start existing jobs and run read-only SQL. |
| `operator` | Editor, plus run actions (`actions:execute`), use the web terminal (`webterm:use`), and create, stop, pause and delete jobs. |
| `admin` | The user type `admin`: every permission, users, teams and connector details included. |

A user's permissions: an admin holds all of them; a user with explicit permissions
(`attributes['scopes']`, `'*'` for every permission) holds those; anyone else holds their role's
(`attributes['role']`), else the instance's **default role** (`api:permissions:default_role`:
`editor` unless configured; Meerschaum 4's `api:permissions:actions:non_admin: true`, when your
configuration sets it, means `operator`).

```python
from meerschaum.core import User

user = User('bob', instance='sql:main')
user.role                         # 'editor'
user.permissions                  # ['pipes:read', 'pipes:write', ...]
user.set_role('viewer')           # saved; explicit permissions are removed
user.set_permissions(['pipes:read', 'logs:read'])   # user.role == 'custom'
user.teams                        # [('Everyone', 'member'), ('analytics', 'manager')]
user.primary_team                 # 'analytics'
```

- A **primary team** (`attributes['primary_team']`) is the team of the pipes a user registers
  through the API or the web console. Without one, it is their first team other than Everyone,
  else Everyone.
- Scope lists written before these permissions existed keep what they allowed: `pipes:write` there
  also grants `pipes:register` and `pipes:define_sql`, and `actions:execute` grants `webterm:use`.
- A token never holds more than its user: its scopes are intersected with the user's current
  permissions when it is used.

## The rules in Python

`meerschaum.core.access` applies the rules the API server applies (they are the core's, so both
always agree). The web console uses it, and plugins can too:

```python
from meerschaum.core.access import Principal, can_see, can_edit, filter_visible_pipes, get_principal

alice = get_principal('alice', instance='sql:main')     # her permissions and teams
can_see(alice, pipe), can_edit(alice, pipe)
visible = filter_visible_pipes(alice, mrsm.get_pipes(instance='sql:main', as_list=True))

guest = Principal('guest', permissions=['pipes:read'], teams=[('analytics', 'member')])
can_see(guest, {'team': 'analytics', 'visibility': 'private'})     # False: managers only
```

| Function | Answers |
|---|---|
| `can_see(principal, access)` | Admins and the owner always; `everyone`: every user; `team`: its members; `private`: its managers. |
| `can_edit(principal, access)` | Who can see it (edit `team`), or the owner, the team's managers and admins (edit `owner`). |
| `can_manage_access(principal, access)`, `can_lock(...)` | The owner, the team's managers and admins. |
| `can_see_email(viewer, user_teams, target_username=)` | Admins, the managers of a team the user is in, and the user themself. |
| `check_lock(locks, op)`, `lock_refusal(pipe, locks, op)` | Whether a write (`'sync'`, `'replace'`, `'clear'`, `'drop'`, `'rebuild'`, `'schema'`, `'delete'`, ...) is refused, and the message. |
| `user_permissions(type, attributes)`, `role_permissions(role)`, `token_permissions(scopes, user_permissions)` | Permissions, as described above. |

`access` may be a dictionary, a pipe or a pipe's parameters; a principal a `Principal` or a
dictionary with `username`, `is_admin`, `permissions` and `teams`.

## Custom instance connectors

A custom [instance connector](/reference/connectors/) gets teams, locks and access with no code of
its own: the defaults store teams in the internal pipe `Pipe('mrsm', 'teams')` through the
connector's own pipe methods (`sync_pipe` with upserts on `team_id`, `get_pipe_data`,
`clear_pipe` with `params`), as users and tokens are stored, and the core checks the locks before
every write it sends to the connector. Users are looked up by their exact username: a username that
starts with `_` (which a `params` filter reads as a negation) never matches another user.
