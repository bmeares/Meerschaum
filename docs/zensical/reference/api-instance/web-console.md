# 🎛️ Web Console

The Meerschaum Web Console provides a graphical interface for managing your Meerschaum instance, served by the [API instance](/reference/api-instance/). To access it, start the API (`mrsm start api`) and navigate to your host URL (e.g., [http://localhost:8000](http://localhost:8000)).

!!! note "The console comes with the Python package"

    The Web Console is the Plotly Dash app of the Python package: install it with `pip install meerschaum[dash]` (also part of `meerschaum[full]` and the `full` Docker image). The Python API host serves it and the Rust API server proxies `/dash` to it. A standalone install (`cargo install meerschaum`, the `rust` Docker image) serves the API, `/docs`, `/engine/*`, MCP and the web terminal, and shows a pointer page at `/dash` that names this command. `--no-dash` turns the console off.

<img src="/assets/screenshots/web-console.png" alt="The Web Console's Overview">

---

## Navigation

Every page of the console has the same bar at the top:

- the **Meerschaum logo**, which opens the [Overview](#overview);
- **Pipes**, **Pipelines**, **Jobs**, **Connectors**, **Plugins** and **Terminal** (shown to users who may use the [web terminal](#terminal)). Under `--secure`, **Connectors** and **Terminal** are for administrators only;
- **Pages**, the pages your plugins add (grouped by their `page_group`), when there are any;
- the **instance** selector: the instance every page shows (the Pipes, Pipelines and Overview pages, the Terminal's filters). Your choice is remembered in the browser, and a link with `?instance=sql:other` opens a page on another instance;
- the **user menu** (your username): **Users**, **Tokens**, **Settings** (change your password) and **Sign out**.

The current page is underlined. On a phone, the bar collapses behind the ☰ button. A link to a page the server does not serve (see `api:permissions:routes:allowlist`) is left out.

A plugin's page (`@web_page`) keeps its own layout, under a plain bar with the logo and **Sign out**; a plugin can give its page the full navigation with `@web_page(..., navbar=True)` (see [Writing plugins](../plugins/writing-plugins.md#the-navigation-bar)).

## Signing in

The console answers only signed-in visitors: a page shows the login form until you sign in, and when your session ends (it lasts `api:cache:session_expires_minutes`, 30 days by default), your next click takes you to the login page and back to where you were. The login and registration pages work for everyone, and so do plugin pages made public (see [who may run a callback](../plugins/web-pages.md#who-may-run-a-callback)). `mrsm start api --no-auth` needs no login at all; `api:dash:callbacks_require_session: false` keeps the logins but lets any callback run without a session.

## Overview

`/dash` is the Overview of the selected instance, with a tile for each part of it:

- **Pipes**: how many are registered and how many are failing (their last sync failed); the failing count opens the Pipes page filtered to them;
- **Pipelines**: each saved Pipeline with its last run (failed ones first), opening it on that run;
- **Jobs**: the jobs running, stalled (no successful sync for three times their interval), failing and stopped, each opening the Jobs page filtered to that state;
- **Recent runs**: the latest runs of the Syncing Engine, failures first. A job that syncs the same pipes again and again takes one row (`×12`).

The tiles refresh every 30 seconds. On an instance without pipes, the Pipes tile shows **New pipe** and **Open terminal** instead of its numbers; the other tiles stay.

## Terminal

The **Terminal** page (`/dash/terminal`; the old `/dash/webterm` and links to `/dash` with an action in the query open it too) is the console's command builder beside the web terminal: pick the connector, metric, location and [tags](/reference/pipes/tags/) filters on the left, an action and its flags, and click **Exec**. **Pipes**, **Jobs** and **Pipelines** list their cards beside the form.

You have complete control of your Meerschaum instance via the webterm, an interactive terminal tied to your username. 

!!! danger "⚠️ Open registration plus terminals for non-admins is a shell for anyone"

    With the defaults, anyone who can reach the API can register an account (`api:permissions:registration:users`) and, as a logged-in user, open the web terminal (`api:permissions:actions:non_admin`): **a shell on the server for anyone who can reach the port**. Meerschaum 4 behaves the same, and v5 keeps it. Before exposing an API, start it with `--secure` (terminals and actions for administrators only) or `--private` (authentication on every endpoint), or set `api:permissions:registration:users: false` (no self-registration) or `api:permissions:actions:non_admin: false` (administrators only). `mrsm start api` prints a warning at startup when it binds a non-loopback address with both settings on; it cannot see a reverse proxy, so a loopback bind behind a proxy exposes the same terminal **without** the warning. (`--no-auth` on a non-loopback address also prints a warning: every route, `/actions` included, is then open to anyone who can connect.)


<img src="/assets/screenshots/web-console-webterm.png" alt="Webterm">

Run actions by clicking **Exec**, which uses your selected filters and flags. Exec validates the command and submits it once into the existing webterm iframe; it keeps the terminal visible and reports a busy or disconnected session. You can also type directly into the same shell. Pipe-card **Manage** actions and Pipeline sync controls run as authenticated background executions through `POST /execute`, following their output over `GET /logs/<job>/stream`; the Pipelines page follows a running engine run's events over `GET /engine/runs/<run_id>/events/stream` (live stage statuses, chunk progress and timeline), which needs the `engine` route group and the `pipes:read` scope, and otherwise polls every few seconds. Destructive actions confirm first. Automatic executions appear under **Runs**, while manually managed and scheduled **Jobs** retain their own view. The **Pipelines** button renders registered Pipeline cards, with a smaller **Term** button beside it.

<img src="/assets/screenshots/web-console-execute-action-button.png" alt="Execute actions via the webterm">

The buttons above the terminal **refresh** it, toggle **fullscreen** (the terminal takes the whole width), and (if `tmux` is available) open a **new tab** (`+`).

When `tmux` is installed (and `api:webterm:tmux:enabled` is on, its default), your terminal lives in a tmux session named after you (`<username>_mrsm`), so a reload or a second browser tab reattaches to the same shell. These sessions run on a tmux server of their own: its socket is `tmux.sock` in a private (`0700`) directory under your Meerschaum root, never your own default tmux server, so they do not appear in your `tmux ls`, are not touched by your `tmux kill-server`, and do not pick up your `~/.tmux.conf`. It starts with Meerschaum's own `tmux.conf` (written next to the socket: no status bar, mouse scrolling, a 50,000-line history); point `api:webterm:tmux:config` at a file of yours to use it instead. The API stops that tmux server when it stops, if it started it.

```yaml
api:
  webterm:
    tmux:
      enabled: true
      session_suffix: _mrsm
      ### Your own tmux configuration for the terminal's tmux server (default: Meerschaum's).
      config: ~/.config/meerschaum-tmux.conf
```

## Connectors

The **Connectors** page (`/dash/connectors`) lists the connectors the server knows, read-only: each connector's keys and type, where it is defined (**Config**, **Default** for a built-in connector left as shipped, **Environment** with the variable's name such as `MRSM_SQL_FOO` or `MRSM_CONFIG`, or **Plugin**), its flavor, host, port and database. Click a connector's keys to see its other attributes. Type in the filter box to narrow the list.

Passwords, tokens, keys and secrets never reach the browser. The page shows the values of a connector's connection settings only (such as `flavor`, `host`, `port`, `database`, `username`, `schema`, `driver`, `sslmode` and the timeouts); every other attribute shows `••••••`, whatever its name. A URI keeps its scheme, user, host, port and database (`postgresql://user:••••••@host:5432/db`), and a connection string its settings of those names (`host=h user=u password=•••••• dbname=d`). A connection test's error is masked the same way.

Administrators can click **Test connection** to try a connector: the result is the time it took or the error, within the connector's `connect_timeout` (or `MRSM_CONNECT_TIMEOUT`). On a server started with `--secure` (`api:permissions:actions:non_admin: false`), the page and its link are for administrators only, like the Terminal.

## Pipes

The **Pipes** page (`/dash/pipes`) is a table of the selected instance's [pipes](/reference/pipes/):

<img src="/assets/screenshots/web-console-pipes.png" alt="The Pipes page">

| Column | What it shows |
|---|---|
| **Pipe** | `Pipe('connector', 'metric', 'location')`, with an instance badge when the page shows another instance than the global one. Click a row to open the pipe. |
| **Rows** | The table's row count. On PostgreSQL, TimescaleDB, MySQL, MariaDB, SQL Server and Oracle large tables show the database's estimate (`~1.2M`, `pipe.get_rowcount(estimate=True)`); SQLite, DuckDB and tables under 100,000 rows are counted exactly. A pipe's Overview counts exactly on demand. |
| **Sync time** | The newest datetime in the pipe's data (`pipe.get_sync_time()`), in UTC. |
| **Status** | How the last sync went, whatever ran it (the CLI, a job, a Pipeline, the API or Python): **Synced 5 min ago**, **Failed** (or **Failed 3 times**; hover for the error) or **Never synced**. |
| **Tags** | The pipe's [tags](/reference/pipes/tags/). |
| **Jobs** | The [jobs](/reference/background-jobs/) that sync it: `sync pipes` and `verify pipes` jobs (also as part of an action chain such as `sync pipes -c sql:main + verify pipes : --loop`) whose keys, tags and instance select the pipe (a compose project's jobs use the project's instance), and the scheduled jobs of saved Pipelines that include it. Hover for the schedule and command. |

The table shows its keys, tags and targets at once and fills the other columns for the rows on screen. Type in **Search pipes** to keep the rows whose keys, target table or tags contain every word; the search, the **Connectors**, **Metrics**, **Locations** and **Tags** filters and the status filter are part of the URL, so a filtered table can be shared (`/dash/pipes?tags=weather&status=failing&q=station`). Click a column's header to sort by it (again for the other direction); on a phone, the rows become cards and a **Sort** menu replaces the headers. More than 100 pipes are split into pages. **Refresh** reads everything again. A pipe last synced before Meerschaum recorded how each sync went (4.1 and the earlier 5.0 release candidates) shows a dash under **Status** until its next sync.

### A pipe's page

A pipe's page (`/dash/pipes/<connector>/<metric>[/<location>]?instance=...`; the links of older versions still work) has the pipe and its instance at the top, then the actions and tabs:

<img src="/assets/screenshots/web-console-pipe.png" alt="A pipe's Overview">

- **Sync** and **Verify** run `sync pipes` and `verify pipes` for this pipe on the server, and the output streams into the page. The Overview reloads when they finish.
- **Edit** makes the Parameters, Definition, Lineage and Data tabs editable; **Done** ends it.
- **Manage** has **Copy**, **Clear**, **Drop** and **Delete**. Clear (optionally only the rows between **Begin** and **End**), Drop and Delete ask first and run only once you type the pipe's metric key. Copy registers the pipe under new keys or on another instance with the same parameters; to another instance under the same keys it runs `copy pipes <instance>` and can copy the rows too.

The tabs:

- **Overview**: the row count (with **Count** for the exact number), the sync time and the oldest time, the last sync (when, how it went, the rows it wrote, how long it took, what ran it and, for a failure, the error), the target table, tags, jobs, every column's role, dtype (**declared** when set in the parameters) and database type, and the indices.
- **Data**: the most recent rows, **Download CSV** (the latest chunk), and a query by **Params**, **Begin**, **End** and a row limit. In Edit mode, **Sync rows** syncs JSON documents into the pipe.
- **Parameters**: the parameters as YAML or JSON (**Show resolved values** shows them with their `{{ Pipe(...) }}` and `MRSM{...}` references resolved); **Update** saves them in Edit mode.
- **Definition**: the SQL of a `sql:` pipe as written, with its `{{ Pipe(...) }}` references (**Show resolved query** shows the SQL that runs), or the `fetch` parameters of another pipe; **Update** saves it in Edit mode.
- **Lineage**: the pipe's parents and children and what the next sync does with each; in Edit mode, add and remove them. **Explore graph** opens its connected graph on the Pipelines page.
- **Runs**: the Syncing Engine's runs that included the pipe, newest first; a run of a saved Pipeline opens on its page.

Sync, Verify, Edit and Manage are shown to users who may run actions: everyone signed in, or only administrators when the API runs with `--secure` (`api:permissions:actions:non_admin: false`). The server checks each action again when it runs. They show only for a pipe the page could read as registered: when the instance cannot be read (the database is down, the credentials are wrong) the page says **Could not read** with the error, and a pipe that is not registered (a deleted pipe, an old link) offers **Register**, which opens New pipe with its keys filled in.

### New pipe

**New pipe** (`/dash/pipes/new`, the button above the table) registers a pipe from the console, like `mrsm bootstrap pipes`:

1. Choose the **Connector** (the list has your connectors and plugins; any keys may be typed), the **Metric**, an optional **Location** and the **Instance** (the selected instance by default).
2. For a `sql:` connector, write the **Query** and click **Preview**: the first 10 rows of the query on that connector (references like `{{ Pipe('sql:main', 'raw') }}` resolve on the instance). The preview's columns fill the **Datetime column** and **ID column** choices, and the datetime column is guessed (a datetime column, else a column named like one whose values are dates). For other connectors, set the pipe's **Parameters** (JSON or YAML), e.g. a plugin's `fetch` options.
3. Click **Register** to open the new pipe's page, or **Register and sync** to also start its first sync and watch it there.

Nothing is saved before you click one of them. Registering follows the API's rules: `api:permissions:registration:pipes` must be on, the user needs the `pipes:write` scope, and with `--secure` only administrators register pipes.

!!! warning "Registering a SQL pipe reads with the connector's credentials"
    **Preview** runs your query with the credentials of the connector you chose, as every sync of the pipe does once it is registered. Whoever may register pipes can therefore read whatever those connectors can read, including the instance's own tables (users, tokens, sessions) through the instance's connector. When some users should not, take registration away from them (`start api --secure`, `api:permissions:registration:pipes: false`, or no `pipes:write` scope), and connect with database users that can read only what your pipes need.

## Pipelines

The **Pipelines** page (`/dash/pipelines`) shows the dependency graph of your pipes, a Gantt chart of the latest or selected run, the runs table and the run events, and a pipe's page has its parents and children on the **Lineage** tab. See [Visualization](/reference/pipelines/visualization/#in-the-web-console).

## Plugins

The **Plugins** page has two tabs:

- **Installed** (`/dash/plugins`): the plugins installed on the server, with their version, their kind (**Python**, or **Rust** as a single `.rs` file or a directory with a `plugin.toml`), what each provides (connectors, a data source, actions, pages, API routes, MCP tools, sync hooks) and whether the packages it requires are installed. A plugin that failed to import says so. Administrators can **Update** a plugin installed from a repository (the latest version from that repository) and **Uninstall** it.
- **Repository** (`/dash/plugins?tab=repository`): the plugins published to this API as a [repository](/reference/connectors/#instances-and-repositories) (`mrsm register plugin <name> -r api:<label>`), to read and download. A plugin's owner edits its description here. Administrators can **Install** a published plugin on the server (**Reinstall** when it is installed already).

Install, Update and Uninstall run the usual actions (`install plugins`, `uninstall plugins`) as one of the console's runs, listed under Jobs > **Runs**; the page says when the action finished, with **View output**. When the change adds or removes a plugin's pages or API routes, the server restarts its Python host to load it. An install from this server's repository downloads the plugin from the server itself (the job's connector `api:this_server`, also recorded as the plugin's origin for **Update**); on a `--private` server that connector carries a token of the administrator who clicked, which may only read plugins (`plugins:read`) and expires after 30 minutes.

<img src="/assets/screenshots/web-console-plugins.png" alt="Plugins page">

## Jobs

The **Jobs** page (`/dash/jobs`) lists the server's [background jobs](/reference/background-jobs/); its **Runs** tab (`/dash/runs`) lists the console's own runs (a pipe's Sync, a Pipeline's Run, a plugin's Install). Each job's status says how it is doing:

| Status | Meaning |
|---|---|
| **Running · last good sync 2 min ago** | Its last lap succeeded. |
| **Stalled · last good sync 3 h ago** | No successful lap for three times its interval (`--min-seconds` or its `-s` schedule; an hour for `--listen`). |
| **Failing · 4 failed syncs** | Its laps fail one after another. |
| **Stopped · last sync failed** | It stopped after a failure. |

The dot before the status has the state's color, as on the Overview: green while running, amber when stalled, red when failing or stopped after a failure.

**Start**, **Pause**, **Stop**, **Restart** and **Delete** manage a job (Delete asks you to type the job's name), and **Logs** views, follows or downloads its output. With more than eight jobs, the page lists them in a table with a filter box (on a phone, a list: each job's name with its status under it), and each row's **⋮** menu has the same actions. A link with `?status=stalled` (the Overview's Jobs tile) shows the jobs in one state: `running`, `stalled`, `failing`, `paused` or `stopped`.

### New job

**New job** creates a job, like `start job` (or `-d`) in a terminal:

1. **Action**: `sync pipes` by default, or any action the console offers (plugin actions included).
2. For an action on pipes, the **Connectors**, **Metrics**, **Locations** and **Tags** of the pipes to select on the selected instance (every pipe when left empty); for `sync pipelines`, the saved **Pipeline**.
3. **Arguments**: more flags, for example `--workers 4`.
4. **Repeat**: **Once** (with **Restart if it stops**), **On a schedule** (`-s`: `every 10 minutes`, `daily starting 00:00`, or a cron line such as `0 * * * *`) or **Continuously** (`--loop`, with the seconds between runs).
5. **Name**: a generated name you may change.

**Command** shows the terminal command that creates the same job, and **Create job** creates and starts it with the server's executor (`systemd` where the server uses it, else a local job). Creating jobs follows the API's rules for actions: everyone signed in may, or only administrators when the API runs with `--secure`, and a non-administrator's command must be one the console lets them run. The server must serve the `jobs` route group (`api:permissions:routes:allowlist`).

## Users

The **Users** page (the user menu > **Users**, `/dash/users`) lists the instance's users. Administrators see each user's type (**admin** or **user**) and email, **Add user** (username, password, type and email), and in each user's **⋮** menu **Change type**, **Reset password** and **Delete** (type the username to confirm). The instance's only admin cannot be demoted or deleted: make another user an admin first.

A user whose type, password or account changes is signed out of their console sessions, so the change applies at once (a promoted user stays signed in, and the administrator making the change keeps their own session). Other users see the usernames only (their own type and email are on their own page); on a `--private` or `--secure` server, they see only themselves.

## Tokens

Create and manage long-lived [tokens](/reference/api-instance/tokens/) on the page `/dash/tokens` (the user menu > **Tokens**): **New token** registers one and shows its secret once, and each token's **⋮** menu edits, invalidates or deletes it. **Grant all scopes, including custom and future ones** (the scope `*`) starts on for administrators and holders of `*`; anyone else starts from their own scopes.

<img src="/assets/screenshots/web-console-tokens-register.png" alt="Register token popup">

## Dash MCP server

Plotly Dash can expose the web console's layout over its own [MCP](../mcp/index.md) server at `/dash/_mcp`, which lets an AI agent inspect the component tree while you build a [web page plugin](../plugins/types-of-plugins.md). It is a development aid, **not** part of Meerschaum's MCP server, and it is disabled by default:

```yaml
api:
  dash:
    mcp:
      enabled: true   # requires dash>=4.3.0
```

!!! danger "Do not enable this on a public instance"
    Dash's MCP server is served from inside the Dash WSGI app, which sits outside FastAPI's authentication. Like the console's callbacks, it needs a console session or an API token (`Authorization: Bearer ...`, any scope) unless `api:dash:callbacks_require_session` is `false`; nothing it exposes is scope-checked. Meerschaum limits it to layout introspection (`dash://layout`, `dash://components`, and the `get_dash_component` tool) and opts every callback out, so a callback must opt in explicitly:

    ```python
    @dash_app.callback(..., mcp_enabled=True)
    def my_callback(...):
        ...
    ```

    Only opt in callbacks that are safe to run unauthenticated. Use Meerschaum's own [`/mcp` endpoint](../mcp/index.md) for anything that touches data.
