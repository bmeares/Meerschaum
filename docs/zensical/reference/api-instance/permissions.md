# 🛡️ Permissions and Rate Limits

An API server decides three things about every request: whether the route is served at all (route groups), who may call it (authentication and [scopes](/reference/api-instance/tokens/#scopes)), and how often (rate limits). All of it lives under `api:permissions` in your config (`mrsm edit config api`).

## Route groups

Every route belongs to a group, and a group that is not served answers `404` (it is not proxied to plugins either, so a plugin cannot serve a core path you turned off).

| Group | Routes |
|---|---|
| `pipes` | `/pipes/*` and `/metadata` |
| `pipelines` | `/pipelines` and `/pipelines/*` (saved Pipelines) |
| `engine` | `/engine/*` (engine state, run history, run event streams) |
| `actions` | `/actions/*` and `/execute*` (running actions) |
| `jobs` | `/jobs/*` and `/logs/*` |
| `connectors` | `/connectors` |
| `users`, `tokens`, `plugins` | `/users/*` and `/teams/*`, `/tokens/*`, `/plugins/*` |
| `version` | `/version`, `/version/mrsm` |
| `misc` | `/healthcheck`, `/info`, `/chaining`, `/favicon.ico` (and `/id` with `--debug`) |
| `index` | `/` (a redirect to the console or the docs) |
| `mcp` | `/mcp` ([MCP server](/reference/mcp/)) |
| `dash` | the web console's own pages (the Python API host reads it) |

`/login` is always served unless you disable it explicitly. The API docs (`/docs`, `/redoc`, `/openapi.json`), the web terminal and plugin routes are not core groups.

A request's group is decided on its decoded path, the path the API host would serve: `/%64ocs` is `/docs` and `/my%2Dplugin/data` is `/my-plugin/data`, for the allowlist, the public preset and rate limits alike.

```yaml
api:
  permissions:
    routes:
      ### Glob patterns of the groups to serve (missing or null: every group).
      allowlist: ['*']
      ### Groups to turn off, whatever the allowlist says (`login` too).
      disabled: ['actions']
```

## The public preset

For a server whose job is to publish plugin routes and plugin pages to anyone (a public dashboard, a data feed), the public preset turns everything else off:

```yaml
api:
  permissions:
    preset: public
```

or, for one run, `mrsm start api --public`.

It serves only `/login`, `/healthcheck`, `/version`, `@api_plugin` routes, plugin pages (and the console's login page, for plugin pages that require a login) and the static files they need. The web console's own pages, the web terminal, `/actions`, `/execute`, user registration, the API docs (`/docs`, `/redoc`, `/openapi.json`) and every other core route group are off. The server says so at startup:

```text
Public preset: serving only /login, /healthcheck, /version, plugin routes and plugin pages; ...
```

Your explicit route settings still apply on top of the preset. A group the allowlist names (any pattern but the default `*`) is served too, and `disabled` still turns groups off:

```yaml
api:
  permissions:
    preset: public
    routes:
      ### Also serve the pipe routes (for `api:` connectors syncing into this server) and the docs.
      allowlist: ['pipes', 'docs']
      ### No version endpoint either.
      disabled: ['version']
```

## Roles and teams

The first account on an empty instance is an administrator, even when user registration is off. After that, only administrators create accounts unless you turn on `api:permissions:registration:users` (default `false`). A self-registered account joins **Everyone** and receives the default role, set with `api:permissions:default_role` (`editor` by default).

| Role | Permissions |
|---|---|
| **Viewer** | Read visible pipes, Pipelines, runs, jobs, logs, plugins and the connectors list; manage their own account and tokens. |
| **Editor** | Viewer, plus register, define, edit and sync pipes, read SQL, and start existing jobs. |
| **Operator** | Editor, plus execute actions, open the web terminal and create, stop, pause or delete jobs. |
| **Admin** | Every permission, user and team administration, and connector details (user type `admin`). |

An administrator may set a role or an explicit permission list for a user. Explicit permissions replace the role; an empty list grants no permissions. A token's scopes are intersected with its owner's current permissions, so reducing the user's rights also reduces existing tokens. Lists made before the new scopes existed keep their earlier meanings: `pipes:write` also registers and defines pipes, and `actions:execute` also opens the terminal. New lists can use `pipes:register`, `pipes:define_sql` and `webterm:use` independently. See [token scopes](/reference/api-instance/tokens/#scopes).

Every user belongs to the built-in team **Everyone**, which cannot be left, renamed or deleted. Administrators create additional teams, appoint managers and choose memberships. Managers add and remove members of their own teams and manage those teams' pipe access. A user's primary team supplies the team for the pipes and jobs they create. Renaming a team updates its pipes and jobs; deleting it moves them to Everyone.

Pipes without access settings belong to Everyone and remain shared. A pipe's `access` parameters choose its owner, team, visibility (**team**, **private**, or **everyone**) and who may edit it (**team** or **owner**). Only its owner, team managers and administrators may change its access or locks. The server stamps new pipes with the caller as owner; a caller cannot impersonate another owner when registering. [Pipe access and locks](/reference/pipes/access-and-locks/) describes the settings and Python API.

The HTTP API, MCP and console enforce these rights and filter hidden members from Pipelines, runs, events, lineage and searches. Jobs started through them belong to their creator and primary team; local jobs belong to Everyone. Email addresses are visible to their owners, administrators, and managers of a team the user belongs to. Local SQL / Valkey credentials are the trust boundary: direct CLI and Python access with them sees everything. Pipe locks apply everywhere, including local access, and no force flag bypasses them.

A connector's `teams: [Data, Operations]` attribute restricts who may use it through the API. Setting SQL definitions requires `pipes:define_sql`; the server refuses internal tables and the tables of pipes the caller cannot see, before and after templates resolve. These SQL checks are best effort. True isolation needs separate connectors and database users with appropriate database permissions.

## Who may run actions

Actions need `actions:execute` and the terminal needs `webterm:use` (Operator and administrators by default). Pipe Sync and Verify controls need `pipes:write` and edit rights on each selected pipe; they do not require arbitrary action execution. Pipeline sync checks every member it would change; a sync-locked member is skipped using its existing data. Remote action selections may not include pipes the caller cannot see or change.

Remote actions run on this server. Nonadministrators cannot redirect them to another executor or inspect server configuration with `show connectors`; use the console's Connectors page for the details your account may see. For Pipe registration and sensitive parameter edits, use the console or dedicated Pipe endpoints. Restricted API keys keep these limits even when their owner is an administrator.

`api:permissions:actions:non_admin: false` (`start api --secure`) further restricts actions, terminals and console mutations to administrators. Meerschaum 4's explicit `actions:non_admin: true` maps the default role to Operator for compatibility, unless `default_role` is set explicitly. The shipped configuration leaves that compatibility key unset and uses Editor.

## Permissions fail closed

When the server cannot read a user's permissions, teams or credentials, it refuses the request with `503` and asks to try again. It never treats a failed read as a default permission grant. Password login fails the same way. Identities are cached for at most five seconds and pipe access for three seconds; changes through this server clear those caches immediately. This holds for every instance type (SQL, Valkey, `api:` and custom instance connectors). Local `register tokens` and `edit tokens` refuse without changing anything when they cannot read the owner's permissions.

## Rate limits

Rate limits are token buckets per client and route group: `requests` per `seconds`.

```yaml
api:
  permissions:
    rate_limits:
      login: {requests: 10, seconds: 60}
      ### `max` and `window_seconds` are accepted too.
      pipes: {max: 1000, window_seconds: 60}
      ### A path prefix limits a plugin's routes (here `/my-plugin` and everything under it).
      /my-plugin: {requests: 30, seconds: 60}
      ### A glob over the groups: every other group.
      '*': {requests: 600, seconds: 60}
```

- A key is a route group (the table above, plus `login`, `docs`, `mcp`, `webterm` and `other` for anything else, such as plugin routes), a glob pattern over them (`'*'`, `'pipe*'`), or a path prefix starting with `/`.
- A path prefix matches the path itself and everything below it (`/my-plugin` covers `/my-plugin/data`, not `/my-plugins`), compared with the decoded path (`/my%2Dplugin/data` counts too); the longest prefix wins and all the paths under it share one bucket per client. Otherwise the first matching group pattern applies, with a bucket per group.
- Without `seconds`, the window is one minute.
- A limited request gets `429` with `Retry-After`.
- An entry the server cannot use (a count below 1, a window that is not a positive number, a key it does not know inside an entry, a name that matches no route group) is reported at startup, naming the entry, and the rest of the config still applies.

The client is the connection's address, or behind a trusted proxy (`api:server:forwarded_allow_ips`, `api:server:proxy_headers`) the first untrusted `X-Forwarded-For` address.
