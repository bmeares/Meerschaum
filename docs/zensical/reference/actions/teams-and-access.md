# 👥 Teams, Access and Locks

Meerschaum is open by default: on an instance where nobody sets up teams, access or locks, every
user sees and changes every pipe within their permissions. These actions set up the rest from the
command line; the web console does the same on its **Users & Teams** page and in a pipe's
**Manage** menu (**Access…**, **Locks…**).

On your own machine, the instance's credentials are the trust boundary: with them, these actions
see and change every user, team and pipe. Through an `api:` instance (`-i api:main`), the server
decides who may do what. **Locks apply everywhere**, to everyone.

## Users

```bash
mrsm register users alice bob    # asks for each password and email
mrsm show users                  # a table: User, Type, Role, Teams, Email
mrsm edit users bob              # password, type, role, primary team, email
mrsm delete users bob
```

```text
╭─────────┬─────────┬──────────────────┬─────────────────────┬───────────────────╮
│ User    │ Type    │ Role             │ Teams               │ Email             │
├─────────┼─────────┼──────────────────┼─────────────────────┼───────────────────┤
│ alice   │ admin   │ Admin            │                     │ alice@example.com │
│ bob     │ regular │ Editor (default) │ analytics           │                   │
│ carol   │ regular │ Operator         │ analytics (manager) │ carol@example.com │
╰─────────┴─────────┴──────────────────┴─────────────────────┴───────────────────╯
```

- **The first user** registered on an instance without users is offered the admin type (the
  default answer is yes), so a new instance always has someone who can manage it.
- **Roles** are presets of permissions:

    | Role | Permissions |
    |---|---|
    | Viewer | Read pipes, jobs, logs, plugins and the connectors list (secrets masked); edit their own account. |
    | Editor | Viewer, plus register, edit and sync pipes (SQL definitions included), start existing jobs and read SQL. |
    | Operator | Editor, plus run actions, use the web terminal, and create, stop, pause and delete jobs. |
    | Admin | Every permission, users, teams and connector details included (the user type `admin`). |

- A user without a role of their own follows the instance's **default role**
  (`api:permissions:default_role`: `editor` unless you change it with `mrsm edit config`); the
  table says so with `(default)`.
- `edit users` offers **Custom permissions** to list permissions one by one (`*` grants every
  permission, plugins' and future ones included). `register users --scopes pipes:read jobs:read`
  gives new users explicit permissions.
- A user's **primary team** is the team of the pipes they create in the web console; `edit users`
  asks for it when the user is in a team.
- `mrsm show users --nopretty` prints one JSON object per user (`username`, `type`, `role`,
  `default_role`, `teams`, `email`).
- Deleting a user clears their ownership of pipes: the pipes stay with their teams.
- Through an `api:` instance, other users' emails are shown only to admins and to the managers of a
  team the user is in.

## Teams

Every user is a member of the built-in team **Everyone**, which cannot be deleted, renamed or left.
Other teams have **members** and **managers**: a manager also adds and removes members and manages
the access of the team's pipes.

```bash
mrsm register teams analytics --members alice,bob --managers carol
mrsm show teams
mrsm edit teams analytics        # members, managers and name in your editor
mrsm edit team analytics --managers carol,dave
mrsm delete teams analytics      # its pipes and jobs move to Everyone
```

- Team names follow the username rules (letters, digits and `_ - . @`) and are unique without case.
- `edit teams` opens a YAML file with `name`, `managers` and `members`. Change `name` to rename the
  team: the pipes and jobs that name it are renamed too (the question counts them). With
  `--members` or `--managers`, the lists are set without opening the editor.
- `delete teams` says how many pipes and jobs move to Everyone before it asks.
- `mrsm show teams --nopretty` prints one JSON object per team.

## Pipe access

A pipe's access is its reserved parameter `access`:

| Key | Values |
|---|---|
| `owner` | A username, or none. |
| `team` | A team name; none is Everyone. |
| `visibility` | `team` (default): the team's members. `private`: the owner, the team's managers and admins. `everyone`: every user. |
| `edit` | `team` (default): anyone who sees the pipe may change it within their permissions. `owner`: the owner, the team's managers and admins. |

A pipe without `access` belongs to Everyone and is visible to and editable by everyone. A pipe
registered through an `api:` instance or the web console starts with its creator as its owner, in
the creator's primary team.

```bash
mrsm edit pipes access -m weather --team analytics --visibility private --edit owner --owner carol
mrsm edit pipes access -c sql:main     # asks for whatever the flags leave out
```

`edit pipes access` asks for the owner, team, visibility and edit mode the flags leave out
(`--noask` or `-y` keeps them as they are). `--owner none` clears the owner; `--team Everyone`
gives the pipes back to everyone. Compose files and `--params` may set `access` too.

## Locks

A lock refuses an operation on a pipe until someone removes it, whoever asks and however: the
command line, Python, background jobs, the Syncing Engine, the API and the web console. `-y`,
`--force` and `--allow-protected` never get past a lock.

| Lock | Refuses |
|---|---|
| `parameters` | Parameter edits (changing the locks themselves stays allowed). |
| `sync` | Writes: syncs, replaced windows (`--replace`), deduplication, verification syncs, rebuilds. |
| `clear` | Row deletes: `clear pipes`, replaced windows, rebuilds. |
| `drop` | Drops, rebuilds, table schema changes (`verify schema`, repartitioning, dropping indices) and deleting the pipe (which drops its table). |
| `delete` | Deleting the pipe. |
| `all` | All of the above. |

```bash
mrsm lock pipes -m weather                      # all
mrsm lock pipes -m weather --locks drop,delete
mrsm unlock pipes -m weather --locks drop
mrsm unlock pipes -m weather                    # every lock
```

A refused operation names the pipe, the lock and how to unlock it:

```text
Cannot drop Pipe('sql:main', 'weather'): it is locked (drop). Unlock it with
`mrsm unlock pipes -c sql:main -m weather --locks drop` or in the web console (Manage > Locks).
```

In a Pipeline run, a pipe locked against syncs is not synced and its status is **locked** (not a
failure): the pipes downstream use its current data.

## Copying pipes

`copy pipes` keeps the source's locks (added to those of an existing destination pipe) and never
copies its access: a new copy has no owner and belongs to Everyone (on an `api:` instance the
server makes you its owner, in your primary team), and an existing destination keeps its own
owner, team and visibility.

## Connectors

`show connectors` masks every secret the way the web console's Connectors page does: only
connection settings (flavor, host, port, database, username, driver, timeouts, ...) are shown, a
URI keeps its address without its password or token, and every other value is masked, whatever its
name. Run `mrsm edit config` to see the values themselves.

```text
$ mrsm show connectors sql:warehouse
...
Attributes for connector 'sql:warehouse':
{
    'database': 'analytics',
    'flavor': 'postgresql',
    'host': 'db.example.com',
    'password': '******',
    'port': 5432,
    'username': 'analyst',
}
```

A setting whose value equals a secret is masked too, so a password never shows through another
field: the shipped `sql:main` uses `mrsm` as both its username and its password, and its username
reads `******`. `POST /execute` runs the same action, so remote users see the same masks.

!!! note "True isolation"

    Teams, visibility and permissions decide what the API server, the MCP server and the web
    console show and allow. Anyone with an instance's database credentials sees everything in it.
    To keep teams' data apart for certain, give each team its own connectors and database users.
