# 🛡️ Permissions and Rate Limits

An API server controls three layers under `api:permissions` (`mrsm edit config api`): enabled route groups, authentication and [scopes](/reference/api-instance/tokens/#scopes), and rate limits.

## Route groups

Disabled core groups return `404` and are never proxied to plugins.

| Group | Routes |
|---|---|
| `pipes` | `/pipes/*`, `/metadata` |
| `pipelines` | `/pipelines`, `/pipelines/*` |
| `engine` | `/engine/*` |
| `actions` | `/actions/*`, `/execute*` |
| `jobs` | `/jobs/*`, `/logs/*` |
| `connectors` | `/connectors` |
| `users`, `tokens`, `plugins` | `/users/*`, `/teams/*`, `/tokens/*`, `/plugins/*` |
| `version` | `/version`, `/version/mrsm` |
| `misc` | `/healthcheck`, `/info`, `/chaining`, `/favicon.ico` (`--debug`: `/id`) |
| `index` | `/` (redirect) |
| `mcp` | `/mcp` ([MCP server](/reference/mcp/)) |
| `dash` | Web console pages |

`/login` stays enabled unless explicitly disabled. `/docs`, `/redoc`, `/openapi.json`, the web terminal, and plugin routes are outside core groups. All checks match the URL-decoded path (so `/%64ocs` matches `/docs`).

```yaml
api:
  permissions:
    routes:
      allowlist: ['*']          # Glob patterns to serve (null/omitted: all)
      disabled: ['actions']     # Always disabled (`login` included)
```

## The public preset

To expose only plugin routes and plugin pages (such as a public dashboard), set the public preset or pass `mrsm start api --public`:

```yaml
api:
  permissions:
    preset: public
```

This serves only `/login`, `/healthcheck`, `/version`, `@api_plugin` routes, plugin pages (plus console login when required), and their static assets. Console pages, webterm, `/actions`, `/execute`, registration, API docs, and all other core groups are turned off.

Explicit `allowlist` patterns (other than `*`) and `disabled` entries still apply on top:

```yaml
api:
  permissions:
    preset: public
    routes:
      allowlist: ['pipes', 'docs']
      disabled: ['version']
```

## Roles and teams

The first account on an empty instance becomes an administrator even with registration disabled. Afterward, self-registration requires `api:permissions:registration:users: true` (default `false`); new users join **Everyone** with `api:permissions:default_role` (`editor`).

| Role | Permissions |
|---|---|
| **Viewer** | Read visible pipes, Pipelines, runs, jobs, logs, plugins, and connectors list; manage own account and tokens. |
| **Editor** | Viewer, plus register, define, edit, and sync pipes, read SQL, and start existing jobs. |
| **Operator** | Editor, plus execute actions, use the web terminal, and create, stop, pause, or delete jobs. |
| **Admin** | All permissions, user/team administration, and connector details (user type `admin`). |

Explicit user permission lists replace the role (an empty list grants nothing). Token scopes always intersect with the owner's current permissions, so demoting a user immediately narrows existing tokens. Legacy lists keep compatibility (`pipes:write` implies registering/defining pipes; `actions:execute` implies terminal access), while new lists can grant `pipes:register`, `pipes:define_sql`, and `webterm:use` separately; see [token scopes](/reference/api-instance/tokens/#scopes).

Every user belongs to **Everyone** (cannot be left, renamed, or deleted). Administrators create teams and appoint managers; a user's primary team owns their new pipes and jobs. Deleting a team moves its pipes and legacy jobs to Everyone, while registered private job history remains an owner/admin archive ([job access](../background-jobs/access.md)).

Pipes default to Everyone; see [Pipe access and locks](/reference/pipes/access-and-locks/) for `access` settings and locks (locks apply everywhere, including local CLI/Python). Connector `teams: [Data, Operations]` restricts API connector use, and `pipes:define_sql` applies best-effort checks against internal and hidden tables—use separate database users for strict SQL isolation. For isolated teams with dedicated databases and sandboxes, see [Teams on One Instance](/reference/api-instance/teams/).

## Who may run actions

Remote actions require `actions:execute` and the web terminal requires `webterm:use` (Operator and Admin). Pipe **Sync** and **Verify** only require `pipes:write` and edit access on the pipe.

Non-administrators can only run approved Meerschaum actions via `POST /execute`, `POST /jobs/{name}`, `POST /actions/{action}`, console **Exec**, MCP `execute_action`, and the web terminal—never shell commands (`sh`, `os`, `python`, `!command`), editors, or SQL clients. Their web terminal runs in restricted mode, validating each command against the server and closing if their role changes. `POST /jobs/{name}` ignores non-admin `cwd`, `logs`, and `target` properties, and job commands act only on visible jobs.

For isolated teams, members with plugin-write access run sandboxed team commands and team terminals (`POST /actions/{action}` and MCP `execute_action` refuse them; see [Commands of a team that runs code](teams.md#commands-of-a-team-that-runs-code)). Eligible registered teams without plugin writes can run [API-only commands](../team-executors.md#commands-under-the-callers-identity).

### Allowed non-administrator commands

Every command below is scoped to objects the caller may read or modify with matching API permissions (`show tokens` lists only their own; granted plugin actions follow the plugin's grants):

<!-- non-admin-commands:start -->
| Action | Subactions |
|---|---|
| `help` | (none) |
| `show` | `version`, `help`, `actions`, `arguments`, `schedules`, `pipes`, `data`, `columns`, `rowcounts`, `sizes`, `partitions`, `targets`, `tags`, `pipelines`, `jobs`, `daemons`, `tokens` |
| `sync` | `pipes`, `pipelines` |
| `verify` | `pipes`, `rowcounts`, `schemas` |
| `deduplicate` | `pipes` |
| `clear` | `pipes`, `pipelines` |
| `drop` | `pipes`, `pipelines` |
| `delete` | `pipes`, `pipelines`, `jobs` |
| `lock` | `pipes` |
| `unlock` | `pipes` |
| `edit` | `pipes` |
| `partition` | `pipes` |
| `tag` | `pipes` |
| `attach` | `jobs`, `logs` |
| `watch` | `jobs`, `logs` |
| `stop` | `jobs`, `daemons` |
| `pause` | `jobs` |
| `restart` | `jobs`, `daemons` |
<!-- non-admin-commands:end -->

Non-administrators cannot redirect commands to another executor or run `show connectors`. Restricted API keys keep these limits even when owned by an administrator.

`api:permissions:actions:non_admin: false` (`mrsm start api --secure`) restricts actions, terminals, and console mutations to administrators. Meerschaum 4's `actions:non_admin: true` maps the default role to Operator unless `default_role` is set.

## Permissions fail closed

Failed permission, team, or credential reads return `503` instead of granting defaults (including password login, all instance backends, and local `register`/`edit tokens`). Identities cache up to 5 seconds and pipe access up to 3 seconds, invalidated immediately on mutations.

## Rate limits

Rate limits use per-client token buckets (`requests` per `seconds`, default window 60s; `max`/`window_seconds` also accepted):

```yaml
api:
  permissions:
    rate_limits:
      login: {requests: 10, seconds: 60}
      pipes: {max: 1000, window_seconds: 60}
      /my-plugin: {requests: 30, seconds: 60}
      '*': {requests: 600, seconds: 60}
```

Keys match route groups (plus `login`, `docs`, `mcp`, `webterm`, `other`), globs, or decoded `/prefix` paths (longest prefix wins). Throttled requests receive `429` with `Retry-After`. Client IP uses the peer address, or the first untrusted `X-Forwarded-For` behind trusted proxies (`api:server:forwarded_allow_ips`, `api:server:proxy_headers`). Invalid entries warn at startup without disabling other rules.

## Request size limits

Body limits apply before buffering (`Content-Length` or streaming chunked bodies return `413`):

```yaml
api:
  limits:
    auth_bytes: 65536           # /login, /users/register, /tokens/* (64 KiB)
    body_bytes: 16777216        # JSON/form, /execute, MCP (16 MiB)
    data_bytes: 1073741824      # Pipe data sync (1 GiB)
    proxy_bytes: 16777216       # Requests proxied to Python API host (16 MiB)
  host:
    response_seconds: 300       # Timeout waiting for host response start (504)
```

Set a limit to `null` to disable it (invalid values abort `start api`). Responses, streams, and websockets are unlimited; engine routes (64 MiB) and plugin uploads (128 MiB) keep their own caps. The server health-checks the Python API host every 30s and restarts it after three missed 10s probes.
