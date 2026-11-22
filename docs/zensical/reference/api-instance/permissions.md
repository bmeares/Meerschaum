# 🛡️ Permissions and Rate Limits

Control who can reach your API server and what they can run under `api:permissions` (`mrsm edit config api`):

1. **Route groups** — turn entire surfaces on or off (`--public`, `allowlist`, `disabled`).
2. **Roles & scopes** — control per-user and per-token permissions ([Tokens](/reference/api-instance/tokens/#scopes)).
3. **Rate & size limits** — throttle abusive clients and cap request payloads.

## Route groups

Disabled core groups return `404` and are never proxied to plugins:

```yaml
# mrsm edit config api
api:
  permissions:
    routes:
      allowlist: ['*']        # Glob patterns to serve (omit for all)
      disabled: ['actions']   # Always disabled (including `login`)
```

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
| `misc` | `/healthcheck`, `/info`, `/chaining`, `/favicon.ico` |
| `index` | `/` (redirect) |
| `mcp` | `/mcp` ([MCP server](/reference/mcp/)) |
| `dash` | Web console pages |

`/login` stays enabled unless explicitly disabled; `/docs`, `/redoc`, `/openapi.json`, the web terminal, and plugin routes live outside core groups.

## The public preset

To expose **only** your plugin routes and public web pages (such as a public dashboard), start with `--public` or set `preset: public`:

=== "CLI"
    ```bash
    mrsm start api --public
    ```

=== "YAML (`api.yaml`)"
    ```yaml
    api:
      permissions:
        preset: public
        routes:
          allowlist: ['pipes', 'docs']   # Optional extras
          disabled: ['version']
    ```

`public` serves only `/login`, `/healthcheck`, `/version`, `@api_plugin` routes, plugin pages, and their static assets—turning off the console, web terminal, `/actions`, `/execute`, registration, and API docs.

## Roles and teams

The first account created on an empty instance becomes an administrator. After that, self-registration is off by default (`api:permissions:registration:users: false`), and new users join **Everyone** with `api:permissions:default_role` (`editor`).

| Role | Permissions |
|---|---|
| **Viewer** | Read visible pipes, Pipelines, runs, jobs, logs, plugins, and connectors list; manage own account and tokens. |
| **Editor** | Viewer, plus register, define, edit, and sync pipes, read SQL, and start existing jobs. |
| **Operator** | Editor, plus execute actions, use the web terminal, and create, stop, pause, or delete jobs. |
| **Admin** | Full access, including users, teams, and connector details (`type: admin`). |

- **Custom scopes:** Explicit permission lists replace the role. Token scopes always intersect with the owner's current permissions, so demoting a user immediately narrows their tokens.
- **Teams & locks:** Every user belongs to **Everyone**. See [Pipe access and locks](/reference/pipes/access-and-locks/) for visibility and locks, and [Teams on One Instance](/reference/api-instance/teams/) for isolated databases and sandboxes.
- **Connector isolation:** Add `teams: [Data, Operations]` to a connector to restrict API access; use least-privilege DB users when strict SQL isolation is required.

## Who may run actions

Running remote actions needs `actions:execute` and opening the web terminal needs `webterm:use` (Operator or Admin). Clicking **Sync** or **Verify** on a pipe only needs `pipes:write` and edit access on that pipe.

Non-administrators can only run approved Meerschaum actions through `/execute`, `/jobs`, `/actions`, console **Exec**, MCP `execute_action`, and the web terminal—never raw shell commands (`sh`, `os`, `python`, `!command`), editors, or SQL clients.

- Lock all remote actions and terminals to admins with `mrsm start api --secure` (`api:permissions:actions:non_admin: false`).
- Isolated teams with `plugins_write` run sandboxed [team commands and terminals](teams.md#commands-of-a-team-that-runs-code); eligible teams without `plugins_write` can run [API-only commands](../team-executors.md#commands-under-the-callers-identity).

### Allowed non-administrator commands

Every command below is scoped to objects the caller can read or edit with matching API permissions:

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

Non-administrators cannot redirect commands to another executor or run `show connectors`. Scoped API keys keep these limits even when owned by an admin.

## Permissions fail closed

If reading permissions, teams, or credentials fails, the server returns `503` rather than falling back to defaults. Identities cache up to 5 seconds and pipe access up to 3 seconds, and both invalidate immediately on writes.

## Rate limits

Configure per-client token buckets under `api:permissions:rate_limits` (`requests` per `seconds`, or `max` per `window_seconds`):

```yaml
# mrsm edit config api
api:
  permissions:
    rate_limits:
      login: {requests: 10, seconds: 60}
      pipes: {max: 1000, window_seconds: 60}
      /my-plugin: {requests: 30, seconds: 60}
      '*': {requests: 600, seconds: 60}
```

Keys match route groups (`login`, `docs`, `mcp`, `webterm`, `other`), globs, or `/prefix` paths (longest prefix wins). Throttled callers receive `429` with `Retry-After`. Behind a reverse proxy, set `api:server:forwarded_allow_ips` and `api:server:proxy_headers` so limits track the real client IP.

## Request size limits

Oversized payloads are rejected with `413` before buffering (set any limit to `null` to disable):

```yaml
# mrsm edit config api
api:
  limits:
    auth_bytes: 65536           # /login, /users/register, /tokens/* (64 KiB)
    body_bytes: 16777216        # JSON/form, /execute, MCP (16 MiB)
    data_bytes: 1073741824      # Pipe data sync (1 GiB)
    proxy_bytes: 16777216       # Requests proxied to Python API host (16 MiB)
  host:
    response_seconds: 300       # Timeout waiting for host response start (504)
```

Responses, SSE streams, and WebSockets are unlimited; engine routes (64 MiB) and plugin uploads (128 MiB) keep their own caps.
