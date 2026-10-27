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
| `users`, `tokens`, `plugins` | `/users/*`, `/tokens/*`, `/plugins/*` |
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

## Who may run actions

`api:permissions:actions:non_admin` (default `true`; `start api --secure` turns it off) decides whether users who are not administrators may run actions from the console (`POST /execute`), open the web terminal and start Pipeline runs (`POST /pipelines/{name}/sync`). A non-administrator also needs the scopes of the route, and runs only the actions the console offers. A Pipeline run by a non-administrator must touch only instances they may use (`api:permissions:instances`).

## Permissions fail closed

A user's scopes come from their `scopes` attribute (no attribute means every scope, as in v4), and an API key's from its token. When the server cannot read them (the instance is down or answers with an error), it refuses the request with `503` and asks to try again: it never falls back to granting every scope. A password login fails the same way. This holds for every kind of instance (SQL, Valkey, `api:`), and `register tokens` / `edit tokens` on the command line refuse (and change nothing) when they cannot read the owner's scopes.

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
