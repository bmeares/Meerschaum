# 🎛️ Web Console

The Web Console is a graphical UI for your [API instance](/reference/api-instance/). Start the server with `mrsm start api` and open [http://localhost:8000](http://localhost:8000).

!!! note "Included with the Python package"
    Install the Plotly Dash console with `pip install meerschaum[dash]` (included in `meerschaum[full]` and the `full` Docker image). Standalone Rust installs serve the API, `/docs`, `/engine/*`, MCP, and web terminal without `/dash`. Pass `--no-dash` to disable it.

![The v5 console Overview](../../assets/screenshots/v5-overview.png){ .mrsm-screenshot }

---

## Navigation

Every console page shares the same top bar:

- **Logo:** Returns to [Overview](#overview).
- **Core tabs:** **Pipes**, **Pipelines**, **Performance**, **Jobs**, **Connectors**, **Plugins**, and **Terminal** (omitted when disabled or unauthorized; **Connectors** and **Terminal** are admin-only under `--secure`).
- **Pages:** Plugin `@web_page` links filtered by allowed plugins. Since `5.1.1`, multi-page groups collapse into nested sections with counts (groups with >3 pages start collapsed unless active), trim repeated prefixes, and nest sub-paths.
- **Team switcher (`5.1.1`):** Shown to admins and multi-team members beside **Instance**. Select a team (or **Everyone**) to preview that team's instances, pipes, connectors, plugins, and jobs without escalating permissions; actions and the terminal pause until returning to **All teams**.
- **Instance selector:** Sets the active instance across pages (persisted in the browser; override with `?instance=sql:other`).
- **User menu:** **Users & Teams**, **Tokens**, **Settings**, **Grafana** (if enabled), and **Sign out**.

Plugin pages can opt into the full bar via `@web_page(..., navbar=True)`; see [Writing plugins](../plugins/writing-plugins.md#the-navigation-bar).

## Signing in

On an empty instance, the login page shows **Create admin account** so the first user is an administrator. Self-registration is off by default (`api:permissions:registration:users: false`); when enabled, new users join **Everyone** with `api:permissions:default_role` (`editor`).

Sessions expire after `api:cache:session_expires_minutes` (30 days default) and return you to your page after login. Use `--no-auth` to skip login entirely, or `api:dash:callbacks_require_session: false` to keep login pages while allowing unauthenticated callbacks.

## Overview

`/dash` refreshes every 30s with tiles for **Pipes**, **Pipelines** (24h hourly runs; balanced layout when none exist yet in `5.1.1`), **Jobs & Next runs** (`job.health['next_run']`), **Recent runs**, and 24h **Performance**.

## Performance

`/dash/performance` compares duration, **rows affected/s**, and failures by Pipe or Pipeline, plus an **Engine** section (**Runs**, **Freshness**, **Backlog**, **Nodes**) filtered to readable pipes. See [Sync metrics](sync-metrics.md).

## Terminal

**Terminal** (`/dash/terminal`) pairs a command builder with an interactive web terminal. Viewing requires `webterm:use` (Operator/Admin); **Exec** also needs `actions:execute`.

- **Admins** get a full `mrsm` shell.
- **Non-admins** get a restricted `mrsm` shell validated against `POST /execute` (no shell commands, `!command`, `python`, editors, or SQL clients); see [Permissions](permissions.md#who-may-run-actions).
- **Isolated teams** with plugin-write access get a sandboxed **team terminal** when host isolation holds ([Commands of a team that runs code](teams.md#commands-of-a-team-that-runs-code)); other isolated teams have no host terminal.

!!! danger "⚠️ Open registration + Operator role exposes a server terminal"
    Never combine `api:permissions:registration:users: true` with `api:permissions:default_role: operator` (or v4's `actions:non_admin: true`) on an exposed API. Use `--secure` or `--private`.

<img src="/assets/screenshots/web-console-webterm.png" alt="Webterm">

**Exec** submits into an idle terminal prompt; pipe **Manage** actions and Pipeline syncs run asynchronously via `POST /execute` and stream logs (`/logs/<job>/stream`) or engine events (`/engine/runs/<run_id>/events/stream`).

<img src="/assets/screenshots/web-console-execute-action-button.png" alt="Execute actions via the webterm">

Toolbar buttons refresh, toggle fullscreen, and open new `tmux` tabs (`+`). Sessions run on private tmux servers separate from `~/.tmux.conf`:

```yaml
api:
  webterm:
    tmux:
      enabled: true
      session_suffix: _mrsm
      config: ~/.config/meerschaum-tmux.conf  # Optional override
```

On hosts with [isolated teams](teams.md#isolated-teams), tmux stays enabled whenever Unix sockets are denied to team code; otherwise terminals fall back to single-tab shells ([Terminals without tmux](teams.md#terminals-without-tmux)).

## Connectors

**Connectors** (`/dash/connectors`) lists visible connectors, types, and sources (**Config**, **Default**, **Environment**, **Plugin**). Passwords, tokens, and URI secrets are always masked as `••••••`. Admins can **Inspect** settings and click **Test connection** (using `connect_timeout` / `MRSM_CONNECT_TIMEOUT`). Connectors with `teams: [ops]` appear only to those teams.

## Pipes

**Pipes** (`/dash/pipes`) shows readable [pipes](/reference/pipes/) with shareable URL filters and **All / Mine / My teams** scopes.

<img src="/assets/screenshots/web-console-pipes.png" alt="The Pipes page">

| Column | Shows |
|---|---|
| **Pipe** | Keys and lock icon when [locked](#pipe-access-and-locks). |
| **Rows** | Exact count (SQLite, DuckDB, <100k rows) or fast DB estimate (`~1.2M`). |
| **Sync time** | Latest UTC datetime or integer timestamp. |
| **Status** | Last sync outcome across CLI, jobs, Pipelines, API, and Python. |
| **Team / Tags / Jobs** | Team & visibility badge, [tags](/reference/pipes/tags/), and matching [jobs](/reference/background-jobs/). |

### Pipe details & New pipe

Click a pipe for `/dash/pipes/<connector>/<metric>[/<location>]`, offering **Sync**, **Verify**, **Edit**, and **Manage** (**Copy**, **Access…**, **Locks…**, **Clear**, **Drop**, **Delete**) across six tabs: **Overview**, **Data** (CSV export + JSON row sync), **Parameters**, **Definition** (with resolved SQL), **Lineage**, and **Runs**.

<img src="/assets/screenshots/web-console-pipe.png" alt="A pipe's Overview">

**New pipe** (`/dash/pipes/new`) mirrors `mrsm bootstrap pipes` with a 10-row SQL **Preview** (or YAML/JSON parameters) and requires `api:permissions:registration:pipes` + `pipes:register` (`pipes:define_sql` for SQL).

!!! warning "SQL Preview uses the connector's credentials"
    Non-admin SQL checks are best-effort. Revoke `pipes:define_sql`, restrict connectors with `teams:`, and use least-privilege DB users when strict isolation is required.

### Pipe access and locks

Owners, team managers, and admins manage visibility (**Its team**, **Private**, **Everyone**), edit mode (**Anyone who can see it** or **Owner and team managers**), and locks (**Edit parameters**, **Sync**, **Clear rows**, **Drop or rebuild**, **Delete**). Locks apply everywhere—console, API, CLI, Python, and jobs—even for admins. See [Teams and access](/reference/actions/teams-and-access/).

## Pipelines & Plugins

- **Pipelines (`/dash/pipelines`):** Lists saved Pipelines with **Canvas** and **Runs** views ([Visualization](/reference/pipelines/visualization/#running-a-pipeline-in-the-console)). Unreadable members appear as **Hidden pipe**; running or clearing requires edit access to all members.
- **Plugins (`/dash/plugins`):** **Installed** and **Repository** (`?tab=repository`) tabs with source viewer. Admins (and plugin-writing team members) can create/upload via **New plugin**, **Edit** source with conflict detection, **Publish** to API repositories, and **Install / Update / Uninstall** plugins as tracked runs.

<img src="/assets/screenshots/web-console-plugins.png" alt="Plugins page">

## Jobs

**Jobs** (`/dash/jobs`) groups visible [background jobs](/reference/background-jobs/) by state (**Running**, **Paused**, **Stopped**) and refreshes every 15s; **Runs** (`/dash/runs`) lists console-triggered runs.

| Status | Meaning |
|---|---|
| **Running** | Running normally. |
| **Stalled** | No lap completed for 3× its interval (or 1h for `--listen`). |
| **Failing** | Consecutive failed laps while running. |
| **Paused / Stopped / Failed** | Paused, stopped cleanly, or exited with failure. |

Each row's **⋮** menu offers **Start**, **Pause**, **Stop**, **Restart**, **Delete**, **View logs**, and **Download logs** (`jobs:*`, `logs:read`). **New job** creates one-off, scheduled (`-s`), or looping (`--loop`) jobs (`jobs:write` + `actions:execute`).

## Users, Teams & Tokens

- **Users & Teams (`/dash/users`, `/dash/teams`):** Admins manage users, roles/custom permissions, password resets (signing the user out), and teams; team managers manage their teams' members.
- **Tokens (`/dash/tokens`):** Create, edit, invalidate, or delete API [tokens](/reference/api-instance/tokens/). Secrets show once, and token scopes always intersect with the owner's current permissions.

| | Viewer | Editor | Operator | Team manager | Admin |
|---|---|---|---|---|---|
| Core pages (Pipes, Pipelines, Jobs, Plugins, Connectors) | ✓ | ✓ | ✓ | ✓ | ✓ |
| Terminal & Exec | | | ✓ | (by role) | ✓ |
| Register, edit, and sync pipes | | ✓ | ✓ | ✓ | ✓ |
| Access… & Locks… | own | own | own | team's | all |
| Users & Teams emails / Add users & teams | | | | team emails | all / ✓ |

<img src="/assets/screenshots/web-console-tokens-register.png" alt="Register token popup">

## Dash MCP server

Plotly Dash can expose the console's component tree at `/dash/_mcp` to help AI agents build [web page plugins](../plugins/types-of-plugins.md). Separate from Meerschaum's `/mcp` server and disabled by default:

```yaml
api:
  dash:
    mcp:
      enabled: true   # requires dash>=4.3.0
```

!!! danger "Do not enable on a public instance"
    `/dash/_mcp` runs outside FastAPI scope checks (requiring only a session or any bearer token unless `callbacks_require_session: false`). Meerschaum limits it to layout introspection (`dash://layout`, `dash://components`, `get_dash_component`) and opts callbacks out unless decorated with `@dash_app.callback(..., mcp_enabled=True)`. Use [`/mcp`](../mcp/index.md) for data access.
