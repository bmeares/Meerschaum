# 🤖 MCP Server

Meerschaum ships a [Model Context Protocol](https://modelcontextprotocol.io) server, so an AI agent can discover your pipes, read their data, and run actions without you writing any glue code.

There are two transports. They expose the same tools, resources, and prompts:

| Transport | Where | Auth | Use it when |
| --- | --- | --- | --- |
| **stdio** | `mrsm start mcp` | None, inherits your shell | The client runs on the same machine as Meerschaum |
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

That's it, no API server, no token. The stdio server runs as the user who launched it and can do anything that user's `mrsm` command can, exactly like the CLI.

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

Then point the client at `/mcp` with the token's **API Key** as a bearer credential. The API key is the value shown as "API Key" when the token is created (in the web console and in the output of `register token`): `mrsm-key:` followed by the base64 encoding of `<client-id>:<client-secret>`.

```json
{
  "mcpServers": {
    "meerschaum": {
      "url": "https://your-api-host/mcp",
      "headers": {"Authorization": "Bearer mrsm-key:YTg1ZTI3NDEtZ..."}
    }
  }
}
```

To see exactly what a token can reach, `GET /mcp` with it:

```bash
curl -H "Authorization: Bearer mrsm-key:YTg1ZTI3NDEtZ..." https://your-api-host/mcp
```

### Connect from the web console

The [web console](../api-instance/web-console.md)'s **Tokens** page has a **Connect an agent** button: pick your client and copy its configuration, with this server's MCP URL already filled in. **New agent token** there starts a token named `agent` with read-only scopes (`pipes:read`, `sql:read`, `connectors:read`, `jobs:read`, `logs:read`, out of those you hold), and the dialog shown once the token is created offers the same configurations with the new API key inlined. The button is hidden when the server does not serve `/mcp`.

### Client configurations

These are the snippets the console generates. Where a client can read the key from the environment, the key stays out of the configuration file:

```bash
export MRSM_API_KEY="mrsm-key:YTg1ZTI3NDEtZ..."
```

=== "Claude Code"

    ```bash
    claude mcp add --transport http meerschaum https://your-api-host/mcp \
      --header "Authorization: Bearer mrsm-key:YTg1ZTI3NDEtZ..."
    ```

    Or, to share the server with a project, a `.mcp.json` which reads `MRSM_API_KEY`:

    ```json
    {
      "mcpServers": {
        "meerschaum": {
          "type": "http",
          "url": "https://your-api-host/mcp",
          "headers": {
            "Authorization": "Bearer ${MRSM_API_KEY}"
          }
        }
      }
    }
    ```

=== "Claude Desktop"

    `claude_desktop_config.json` starts local servers only, so bridge to the remote one with [`mcp-remote`](https://github.com/geelen/mcp-remote) (needs Node.js; add `"--allow-http"` to `args` for a plain `http://` server):

    ```json
    {
      "mcpServers": {
        "meerschaum": {
          "command": "npx",
          "args": [
            "-y",
            "mcp-remote",
            "https://your-api-host/mcp",
            "--header",
            "Authorization:${AUTH_HEADER}"
          ],
          "env": {
            "AUTH_HEADER": "Bearer mrsm-key:YTg1ZTI3NDEtZ..."
          }
        }
      }
    }
    ```

=== "Codex"

    In `~/.codex/config.toml` (the token is read from `MRSM_API_KEY`):

    ```toml
    [mcp_servers.meerschaum]
    url = "https://your-api-host/mcp"
    bearer_token_env_var = "MRSM_API_KEY"
    ```

=== "OpenCode"

    In `opencode.json` (the header reads `MRSM_API_KEY`; OpenCode 1.x puts the server directly under `mcp`):

    ```json
    {
      "$schema": "https://opencode.ai/config.json",
      "mcp": {
        "servers": {
          "meerschaum": {
            "type": "remote",
            "url": "https://your-api-host/mcp",
            "oauth": false,
            "headers": {
              "Authorization": "Bearer {env:MRSM_API_KEY}"
            }
          }
        }
      }
    }
    ```

## What the agent gets

**Tools**, the operations. Discovering and reading pipes (`list_pipes`, `get_pipe_data`, `get_pipe_attributes`, `get_pipe_stats`), writing to them (`sync_pipe`, `sync_documents`, `register_pipe`, `edit_pipe`, `verify_pipe`, `deduplicate_pipe`), removing data (`clear_pipe`, `drop_pipe`, `delete_pipe`), saved [Pipelines](/reference/pipelines/) (`list_pipelines`, `plan_pipeline` to see what a run would do, `sync_pipeline` to start one in the background), their history (`list_runs`, `get_run`), background jobs (`list_jobs`, `get_job_logs`), the configured connectors (`list_connectors`), and two escape hatches (`read_sql`, `execute_action`). `sync_pipe` with `parents` or `children` also syncs a pipe's upstream or downstream pipes in dependency order, and the datetime bounds (`begin`, `end`) take ISO datetimes or integers. See the [tool reference](tools.md).

**Resources**, documents the agent can fetch once and cache: `mrsm://pipes`, `mrsm://pipes/{ck}/{mk}/{lk}`, `mrsm://actions`, `mrsm://connectors`, and `mrsm://docs` (this reference, served locally, no network needed).

**Prompts**, guided workflows the client surfaces as commands: `bootstrap_pipe`, `diagnose_sync_failure`, and `explain_pipe`.

Every tool carries MCP annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`), so a well-behaved client asks before it deletes anything.

**Plugin entries**, plugins add their own tools, resources and prompts next to these: Python plugins with `meerschaum.mcp.tool(...)` (served through the API host), and Rust plugins by declaring them in their manifest (served in-process, over HTTP and stdio alike; see [Routes and MCP in Rust plugins](../plugins/rust-plugins/routes-and-mcp.md)). Plugin entries obey the same scopes and read-only mode, and never replace a built-in tool.

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

Set `api:mcp:enabled` to `false` to drop the `/mcp` route entirely. The stdio transport is unaffected, it is only ever started explicitly.

## Security

Read [MCP security](security.md) before exposing `/mcp` to anything you do not control. The short version:

- Scope your tokens. A token with `pipes:read` cannot write, drop, or run actions, and the tools it cannot call are hidden from it entirely.
- `actions:execute` is broad by design, it is the CLI. Do not hand it out casually.
- Use `read_only: true` for any deployment where an agent should only ever look.

## Migrating from the `mcp` plugin

The community `mcp` plugin is superseded by this endpoint. The built-in route is registered before plugins load, so it takes precedence automatically and the API warns you at startup. Uninstall the plugin to clear the warning:

```bash
mrsm uninstall plugin mcp
```

The tools are broadly the same, with a few changes:

- `get_sync_time`, `get_bound_time`, `get_pipe_rowcount`, `pipe_exists`, and `get_pipe_columns_types` are consolidated into **`get_pipe_stats`**, which returns all of it in one call.
- `get_pipe_doc` and `get_pipe_value` are gone, use `get_pipe_data` with `limit: 1` and `select_columns`.
- `get_backtrack_data` is now `get_pipe_data`'s `backtrack_minutes` argument.
- `list_actions` and `list_connectors` are now the `mrsm://actions` and `mrsm://connectors` resources.
- `read_sql` now refuses anything that is not a single read-only statement.

## Dash's MCP server

Plotly Dash has [its own MCP server](../plugins/types-of-plugins.md), separate from this one, which exposes the web console's layout so an agent can understand the page it is building. It is **off by default** and is a development aid, not an API, see the [web console docs](../api-instance/web-console.md#dash-mcp-server).
