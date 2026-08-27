# 🤖 MCP Server

Meerschaum ships a [Model Context Protocol](https://modelcontextprotocol.io) server, so an AI agent can discover your pipes, read their data, and run actions without you writing any glue code.

There are two transports. They expose the same tools, resources, and prompts:

| Transport | Where | Auth | Use it when |
| --- | --- | --- | --- |
| **stdio** | `mrsm start mcp` | None — inherits your shell | The client runs on the same machine as Meerschaum |
| **HTTP** | `/mcp` on the API | OAuth2 scopes per tool | The client is remote |

## Quick start: local (stdio)

Point your MCP client at the `mrsm` executable. For Claude Desktop or Claude Code, add this to your MCP configuration:

```json
{
  "mcpServers": {
    "meerschaum": {
      "command": "mrsm",
      "args": ["start", "mcp"]
    }
  }
}
```

That's it — no API server, no token. The stdio server runs as the user who launched it and can do anything that user's `mrsm` command can, exactly like the CLI.

!!! note "Isolate a project"
    Set `MRSM_ROOT_DIR` in the client's `env` to scope the server to one Meerschaum root:

    ```json
    {
      "mcpServers": {
        "meerschaum": {
          "command": "mrsm",
          "args": ["start", "mcp"],
          "env": {"MRSM_ROOT_DIR": "/path/to/project/root"}
        }
      }
    }
    ```

## Quick start: remote (HTTP)

Start an API instance and [register a token](../api-instance/tokens.md) with only the scopes the agent needs:

```bash
mrsm start api
mrsm register token --label agent --scopes pipes:read
```

Then point the client at `/mcp` with the token as a bearer credential:

```json
{
  "mcpServers": {
    "meerschaum": {
      "url": "https://your-api-host/mcp",
      "headers": {"Authorization": "Bearer mrsm-key:<client-id>:<client-secret>"}
    }
  }
}
```

To see exactly what a token can reach, `GET /mcp` with it:

```bash
curl -H "Authorization: Bearer mrsm-key:..." https://your-api-host/mcp
```

## What the agent gets

**Tools** — the operations. Discovering and reading pipes (`list_pipes`, `get_pipe_data`, `get_pipe_attributes`, `get_pipe_stats`), writing to them (`sync_pipe`, `sync_documents`, `register_pipe`, `edit_pipe`, `verify_pipe`, `deduplicate_pipe`), removing data (`clear_pipe`, `drop_pipe`, `delete_pipe`), and two escape hatches (`read_sql`, `execute_action`). See the [tool reference](tools.md).

**Resources** — documents the agent can fetch once and cache: `mrsm://pipes`, `mrsm://pipes/{ck}/{mk}/{lk}`, `mrsm://actions`, `mrsm://connectors`, and `mrsm://docs` (this reference, served locally, no network needed).

**Prompts** — guided workflows the client surfaces as commands: `bootstrap_pipe`, `diagnose_sync_failure`, and `explain_pipe`.

Every tool carries MCP annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`), so a well-behaved client asks before it deletes anything.

## Configuration

```yaml
api:
  mcp:
    enabled: true
    ### Expose only tools which never modify data, whatever the token allows.
    read_only: false
    actions:
      ### Actions `execute_action` refuses to run.
      denylist: ['sh', 'os', 'python']
      ### If non-empty, the only actions `execute_action` may run.
      allowlist: []
```

Set `api:mcp:enabled` to `false` to drop the `/mcp` route entirely. The stdio transport is unaffected — it is only ever started explicitly.

## Security

Read [MCP security](security.md) before exposing `/mcp` to anything you do not control. The short version:

- Scope your tokens. A token with `pipes:read` cannot write, drop, or run actions, and the tools it cannot call are hidden from it entirely.
- `actions:execute` is broad by design — it is the CLI. Do not hand it out casually.
- Use `read_only: true` for any deployment where an agent should only ever look.

## Migrating from the `mcp` plugin

The community `mcp` plugin is superseded by this endpoint. The built-in route is registered before plugins load, so it takes precedence automatically and the API warns you at startup. Uninstall the plugin to clear the warning:

```bash
mrsm uninstall plugin mcp
```

The tools are broadly the same, with a few changes:

- `get_sync_time`, `get_bound_time`, `get_pipe_rowcount`, `pipe_exists`, and `get_pipe_columns_types` are consolidated into **`get_pipe_stats`**, which returns all of it in one call.
- `get_pipe_doc` and `get_pipe_value` are gone — use `get_pipe_data` with `limit: 1` and `select_columns`.
- `get_backtrack_data` is now `get_pipe_data`'s `backtrack_minutes` argument.
- `list_actions` and `list_connectors` are now the `mrsm://actions` and `mrsm://connectors` resources.
- `read_sql` now refuses anything that is not a single read-only statement.

## Dash's MCP server

Plotly Dash has [its own MCP server](../plugins/types-of-plugins.md), separate from this one, which exposes the web console's layout so an agent can understand the page it is building. It is **off by default** and is a development aid, not an API — see the [web console docs](../api-instance/web-console.md#dash-mcp-server).
