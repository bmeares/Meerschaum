# MCP Security

The MCP server hands an AI agent the same capabilities a person has at the CLI or web console. Treat a token for it exactly as you would a Meerschaum login.

## The two trust models

**stdio (`mrsm start mcp`) has no authentication and needs none.** It is spawned by a client on your machine and runs as you. It can do anything your `mrsm` command can. There is no scope enforcement because there is no principal to scope — this is the CLI's trust model.

Never expose the stdio server over a network shim (`socat`, an SSH tunnel to an untrusted peer, a container port). Use the HTTP transport, which authenticates.

**HTTP (`/mcp`) authenticates every request and checks a scope per tool.** The scopes are the same ones the REST routes enforce, so a token cannot do more through MCP than through the API.

## Scope enforcement

Each tool declares the scope it needs. Tools your token does not cover are **omitted from `tools/list`**, not advertised and then refused — an agent never sees a capability it cannot use.

See the [tool reference](tools.md#scopes) for the generated scope-to-tool matrix.

A token with the `*` scope can call everything. Mint narrow tokens instead:

```bash
### An agent that only reads.
mrsm register token --label reader --scopes pipes:read

### An agent that reads and ingests, but cannot drop or delete.
mrsm register token --label writer --scopes pipes:read,pipes:write

### An agent that may run read-only SQL through a connector.
mrsm register token --label analyst --scopes pipes:read,sql:read
```

## Read-only mode

Scopes are per-token; read-only mode is per-deployment. With

```yaml
api:
  mcp:
    read_only: true
```

every tool that modifies anything is hidden and refused, **even for a `*`-scoped token**. Use it for a public or shared analytics deployment where no agent should ever write. It is a second lock, not a replacement for scoping tokens.

## `execute_action` is broad by design

`execute_action` runs Meerschaum actions, which is the whole CLI. The `actions:execute` scope has always meant this — the REST `/actions` route behaves identically.

The actions which execute arbitrary code or SQL on the host are refused by default:

```yaml
api:
  mcp:
    actions:
      denylist: ['sh', 'os', 'python', 'stack', 'sql', 'install']
```

The denylist applies to subactions too, so a denied action cannot be smuggled in through a wrapper (`start job` with `{"action": ["sh", …]}` is refused).

For a locked-down deployment, invert it with an allowlist, which takes precedence:

```yaml
api:
  mcp:
    actions:
      allowlist: ['sync', 'show', 'verify']
```

Do not grant `actions:execute` to a token you would not grant a shell to. If an agent only needs to sync, give it `pipes:write` and let it call `sync_pipe`.

## `read_sql` and read-only enforcement

`read_sql` runs under its own `sql:read` scope — running `SELECT` against every database the host can reach is a much larger privilege than `connectors:read`, which only lists connector labels. Because `SQLConnector.read()` executes whatever it is given, the tool checks the statement first and refuses:

- anything whose leading keyword is not `SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `EXPLAIN`, `VALUES`, or `TABLE`
- more than one statement (`SELECT 1; DROP TABLE users`)
- any query containing a mutating keyword, including `SELECT ... INTO` and `COPY ... TO PROGRAM`
- mutating or filesystem functions inside an otherwise ordinary `SELECT` (`dblink_exec()`, `lo_export()`, `pg_read_file()`, `xp_cmdshell`)
- MySQL executable comments (`/*! … */`), which the server runs as SQL
- any query naming a protected instance table (`mrsm_users`, `mrsm_tokens`, `mrsm_pipes`, `mrsm_plugins`)

Comments, string literals, and quoted identifiers are stripped in a single quote-aware pass, so `WHERE note = 'please delete'` is fine while neither `-- ; DROP TABLE u` nor `SELECT '--' ; DROP TABLE u` is a bypass. Results are read one bounded chunk at a time, so an unqualified `SELECT * FROM big_table` cannot exhaust the server's memory.

## Protected instance tables

The pipe tools refuse any pipe whose `target` is `mrsm_users`, `mrsm_tokens`, `mrsm_pipes`, or `mrsm_plugins`, matching the REST routes: registering, editing, reading, syncing, clearing, and dropping are all blocked, and such pipes are omitted from `list_pipes`. Without this, `pipes:read` on a pipe pointed at `mrsm_users` would return password hashes.

!!! warning "This is a keyword check, not a SQL parser"
    It blocks the realistic escalation paths from a read scope, but a sufficiently exotic flavor-specific construct could slip through. **If `read_sql` is reachable by anyone you do not trust, back the connector with a read-only database role.** That is the only durable guarantee; the statement check is defense in depth.

## Deployment checklist

- [ ] Tokens are scoped to the minimum each agent needs — no `*` in production.
- [ ] `api:mcp:read_only` is `true` anywhere agents should only look.
- [ ] `actions:execute` is granted deliberately, if at all.
- [ ] Connectors reachable via `read_sql` use read-only database roles.
- [ ] The API is behind TLS ([NGINX](../api-instance/nginx.md)) — bearer tokens must not cross plaintext.
- [ ] `api:dash:mcp:enabled` is left `false` on any public instance.

## Dash's MCP server is not authenticated

Dash's own MCP server is served from inside the Dash WSGI app, which sits **outside** FastAPI's authentication. Nothing it exposes is scope-checked. Meerschaum therefore ships it disabled, and when you do enable it, limits it to layout introspection — callbacks must opt in individually rather than all being exposed by default.

Enable `api:dash:mcp:enabled` only on a local development instance:

```yaml
api:
  dash:
    mcp:
      enabled: false   # leave this off on anything reachable
```
