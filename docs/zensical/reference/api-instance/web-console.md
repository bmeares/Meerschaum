# 🎛️ Web Console

The Meerschaum Web Console provides a graphical interface for managing your Meerschaum instance, served by the [API instance](/reference/api-instance/). To access it, start the API (`mrsm start api`) and navigate to your host URL (e.g., [http://localhost:8000](http://localhost:8000)).

!!! note "The console comes with the Python package"

    The Web Console is the Plotly Dash app of the Python package: install it with `pip install meerschaum[dash]` (also part of `meerschaum[full]` and the `full` Docker image). The Python API host serves it and the Rust API server proxies `/dash` to it. A standalone install (`cargo install meerschaum`, the `rust` Docker image) serves the API, `/docs`, `/engine/*`, MCP and the web terminal, and shows a pointer page at `/dash` that names this command. `--no-dash` turns the console off.

![The v5 console Overview](../../assets/screenshots/v5-overview.png){ .mrsm-screenshot }

---

## Navigation

Every page of the console has the same bar at the top:

- the **Meerschaum logo**, which opens the [Overview](#overview);
- **Pipes**, **Pipelines**, **Performance**, **Jobs**, **Connectors**, **Plugins** and **Terminal**, each shown to the users whose permissions include it (see [Who sees what](#who-sees-what)): **Terminal** needs `webterm:use` (the Operator role and administrators). Under `--secure`, **Connectors** and **Terminal** are for administrators only;
- **Pages**, the pages your plugins add (grouped by their `page_group`), when there are any;
- the **instance** selector: the instance every page shows (the Pipes, Pipelines and Overview pages, the Terminal's filters). Your choice is remembered in the browser, and a link with `?instance=sql:other` opens a page on another instance;
- the **user menu** (your username): **Users & Teams**, **Tokens**, **Settings** (change your password) and **Sign out**.

The current page is underlined. On a phone, the bar collapses behind the ☰ button. A link to a page the server does not serve (see `api:permissions:routes:allowlist`) is left out, and so is a page your permissions do not include: buttons you may not use are not shown either (not shown disabled). Opening such a page by its address says **This page is not available to your account**.

A plugin's page (`@web_page`) keeps its own layout, under a plain bar with the logo and **Sign out**; a plugin can give its page the full navigation with `@web_page(..., navbar=True)` (see [Writing plugins](../plugins/writing-plugins.md#the-navigation-bar)).

## Signing in

On an instance without users (a new server), the login page is **Create admin account**: the first account is an administrator, and it is signed in at once. After that the page is the login form. Visitors register their own accounts only when `api:permissions:registration:users` is `true` (it is `false` by default); then the login page offers **Create account**, and a new account gets the default role (`api:permissions:default_role`, Editor unless configured) and is in the team Everyone. Otherwise an administrator adds users on [Users & Teams](#users-teams).

The console answers only signed-in visitors: a page shows the login form until you sign in, and when your session ends (it lasts `api:cache:session_expires_minutes`, 30 days by default), your next click takes you to the login page and back to where you were. The login and registration pages work for everyone, and so do plugin pages made public (see [who may run a callback](../plugins/web-pages.md#who-may-run-a-callback)). `mrsm start api --no-auth` needs no login at all; `api:dash:callbacks_require_session: false` keeps the logins but lets any callback run without a session.

## Overview

`/dash` is the Overview of the selected instance, with a tile for each part of it:

- **Pipes**: how many are registered and how many are failing (their last sync failed); the failing count opens the Pipes page filtered to them;
- **Pipelines**: each saved Pipeline with its last run (failed ones first), opening it on that run;
- **Jobs**: the jobs running, stalled (no successful sync for three times their interval), failing and stopped, each opening the Jobs page filtered to that state;
- **Recent runs**: the latest runs of the Syncing Engine, failures first. A job that syncs the same pipes again and again takes one row (`×12`).
- **Performance**: sync counts, failures, average duration, and rows affected/s over the last 24 hours.

The tiles refresh every 30 seconds. On an instance without pipes, the Pipes tile shows **New pipe** and **Open terminal** instead of its numbers; the other tiles stay.

## Performance

Open **Performance** (`/dash/performance`) to compare duration, write rate, and failures. Choose a time window and filter by Pipe or Pipeline. Charts show the trend; tables show each Pipe or Pipeline and recent syncs.

**Rows affected/s** counts inserted, updated, upserted, and deleted rows. Replacements count both deletion and new rows; an unchanged sync affects zero rows. See [Sync metrics](sync-metrics.md) for Python access, retention, and capture limits.

## Terminal

The **Terminal** page (`/dash/terminal`; the old `/dash/webterm` and links to `/dash` with an action in the query open it too) is the console's command builder beside the web terminal: pick the connector, metric, location and [tags](/reference/pipes/tags/) filters on the left, an action and its flags, and click **Exec**. **Pipes**, **Jobs** and **Pipelines** list their cards beside the form.

You have complete control of your Meerschaum instance via the webterm, an interactive terminal tied to your username. 

The Terminal page is for users with the `webterm:use` permission (the Operator role and administrators); Exec runs actions, which needs `actions:execute` too.

!!! danger "⚠️ Open registration plus the Operator role for new users is a shell for anyone"

    The web terminal is a shell on the server. By default nobody can register an account (`api:permissions:registration:users: false`) and users without a role of their own are Editors, who have no terminal. If you turn registration on **and** make Operator the default role (`api:permissions:default_role: operator`, or Meerschaum 4's `api:permissions:actions:non_admin: true` in your configuration), anyone who can reach the API can register and open **a shell on the server**. `mrsm start api` prints a warning at startup when it binds a non-loopback address with both settings on; it cannot see a reverse proxy, so a loopback bind behind a proxy exposes the same terminal **without** the warning. Start an exposed API with `--secure` (actions and terminals for administrators only) or `--private` (authentication on every endpoint). (`--no-auth` on a non-loopback address also prints a warning: every route, `/actions` included, is then open to anyone who can connect.)


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

The **Connectors** page (`/dash/connectors`) lists each connector's keys, type, and source: **Config**, **Default**, **Environment**, or **Plugin**. Type in the filter box to narrow the list. Team restrictions determine which connectors appear.

Administrators also see flavor, host, port, database, and schema. Choose **Inspect** to open masked settings and required / supported keys. Passwords, tokens, nested secrets, and private URI values stay masked. Other readers see keys, type, and source only (`connectors:read`).

Passwords, tokens, keys and secrets never reach the browser. The page shows the values of a connector's connection settings only (such as `flavor`, `host`, `port`, `database`, `username`, `schema`, `driver`, `sslmode` and the timeouts); every other attribute shows `••••••`, whatever its name. A URI keeps its scheme, user, host, port and database (`postgresql://user:••••••@host:5432/db`), and a connection string its settings of those names (`host=h user=u password=•••••• dbname=d`). A connection test's error is masked the same way.

Administrators can click **Test connection** to try a connector: the result is the time it took or the error, within the connector's `connect_timeout` (or `MRSM_CONNECT_TIMEOUT`). On a server started with `--secure` (`api:permissions:actions:non_admin: false`), the page and its link are for administrators only, like the Terminal.

A connector with a `teams` attribute (`teams: [ops]`) is offered by New pipe only to users of those teams, and the console (like the API) refuses a pipe of another team that uses it. True isolation between teams still needs separate connectors with their own database users: see [Pipe access](#pipe-access-and-locks).

## Pipes

The **Pipes** page (`/dash/pipes`) is a table of the selected instance's [pipes](/reference/pipes/) that you may see (see [Pipe access](#pipe-access-and-locks)):

<img src="/assets/screenshots/web-console-pipes.png" alt="The Pipes page">

| Column | What it shows |
|---|---|
| **Pipe** | `Pipe('connector', 'metric', 'location')`, with an instance badge when the page shows another instance than the global one. Click a row to open the pipe. |
| **Rows** | The table's row count. On PostgreSQL, TimescaleDB, MySQL, MariaDB, SQL Server and Oracle large tables show the database's estimate (`~1.2M`, `pipe.get_rowcount(estimate=True)`); SQLite, DuckDB and tables under 100,000 rows are counted exactly. A pipe's Overview counts exactly on demand. |
| **Sync time** | The newest datetime in the pipe's data (`pipe.get_sync_time()`), in UTC. |
| **Status** | How the last sync went, whatever ran it (the CLI, a job, a Pipeline, the API or Python): **Synced 5 min ago**, **Failed** (or **Failed 3 times**; hover for the error) or **Never synced**. |
| **Team** | The pipe's team (**Everyone** for a pipe without one), with **private** or **everyone** when its visibility is not its team's. Shown once you are in a team or see a team's pipe. A lock icon after the pipe's keys means it is [locked](#pipe-access-and-locks) (hover for what). |
| **Tags** | The pipe's [tags](/reference/pipes/tags/). |
| **Jobs** | The [jobs](/reference/background-jobs/) that sync it: `sync pipes` and `verify pipes` jobs (also as part of an action chain such as `sync pipes -c sql:main + verify pipes : --loop`) whose keys, tags and instance select the pipe (a compose project's jobs use the project's instance), and the scheduled jobs of saved Pipelines that include it. Hover for the schedule and command. |

The table shows its keys, tags and targets at once and fills the other columns for the rows on screen. Type in **Search pipes** to keep the rows whose keys, target table, team or tags contain every word. **All**, **Mine** (the pipes you own) and **My teams** (the pipes of your teams other than Everyone) narrow the table to whose pipes they are. The search, the **Connectors**, **Metrics**, **Locations** and **Tags** filters, the owner filter and the status filter are part of the URL, so a filtered table can be shared (`/dash/pipes?tags=weather&status=failing&scope=mine&q=station`). The filters list only the keys and tags of pipes you may see. Click a column's header to sort by it (again for the other direction); on a phone, the rows become cards and a **Sort** menu replaces the headers. More than 100 pipes are split into pages. **Refresh** reads everything again. A pipe last synced before Meerschaum recorded how each sync went (4.1 and the earlier 5.0 release candidates) shows a dash under **Status** until its next sync.

### A pipe's page

A pipe's page (`/dash/pipes/<connector>/<metric>[/<location>]?instance=...`; the links of older versions still work) has the pipe and its instance at the top, its **Owner**, **Team** and visibility badges (**Private** or **Everyone**; none for a pipe left to Everyone), a lock icon when it is locked, then the actions and tabs:

<img src="/assets/screenshots/web-console-pipe.png" alt="A pipe's Overview">

- **Sync** and **Verify** run `sync pipes` and `verify pipes` for this pipe on the server, and the output streams into the page. The Overview reloads when they finish.
- **Edit** makes the Parameters, Definition, Lineage and Data tabs editable; **Done** ends it.
- **Manage** has **Copy**, **Access…**, **Locks…**, **Clear**, **Drop** and **Delete**. Clear (optionally only the rows between **Begin** and **End**), Drop and Delete ask first and run only once you type the pipe's metric key. Copy registers the pipe under new keys or on another instance with the same parameters and locks; the copy is yours, in your primary team (a copy never keeps the original's access). To another instance under the same keys it runs `copy pipes <instance>` and can copy the rows too. **Access…** and **Locks…**: see [below](#pipe-access-and-locks).

The tabs:

- **Overview**: the row count (with **Count** for the exact number), the sync time and the oldest time, the last sync (when, how it went, the rows it wrote, how long it took, what ran it and, for a failure, the error), the target table, tags, jobs, every column's role, dtype (**declared** when set in the parameters) and database type, and the indices.
- **Data**: the most recent rows, **Download CSV** (the latest chunk), and a query by **Params**, **Begin**, **End** and a row limit. In Edit mode, **Sync rows** syncs JSON documents into the pipe.
- **Parameters**: the parameters as YAML or JSON (**Show resolved values** shows them with their `{{ Pipe(...) }}` and `MRSM{...}` references resolved); **Update** saves them in Edit mode.
- **Definition**: the SQL of a `sql:` pipe as written, with its `{{ Pipe(...) }}` references (**Show resolved query** shows the SQL that runs), or the `fetch` parameters of another pipe; **Update** saves it in Edit mode.
- **Lineage**: the pipe's parents and children and what the next sync does with each; in Edit mode, add and remove them. **Explore graph** opens its connected graph on the Pipelines page.
- **Runs**: the Syncing Engine's runs that included the pipe, newest first; a run of a saved Pipeline opens on its page.

Each action is shown to the users who may run it on this pipe: Sync, Verify and Edit need `pipes:write`, Clear and Delete `pipes:delete`, Drop `pipes:drop` and Copy `pipes:register` (the Editor role has all but Clear, Drop and Delete), and the pipe's edit mode must let you change it. Under `--secure` (`api:permissions:actions:non_admin: false`) only administrators change anything. The console checks each action again when you click it, and the server when it runs. They show only for a pipe the page could read as registered: when the instance cannot be read (the database is down, the credentials are wrong) the page says **Could not read** with the error, and a pipe that is not registered (a deleted pipe, an old link) offers **Register**, which opens New pipe with its keys filled in. A pipe you may not see reads as not registered, and its page shows nothing of it.

### Pipe access and locks

Every pipe has an **owner** (a user, or nobody), a **team** (Everyone unless one is set), a **visibility** and an **edit mode** (its `access` parameter):

| Who can see it | |
|---|---|
| **Its team** (the default) | The team's members (every user, for a pipe of Everyone), its owner and administrators. |
| **Its owner and team managers** (private) | Its owner, the managers of its team and administrators. |
| **Everyone** | Every user. |

| Who can change it | |
|---|---|
| **Anyone who can see it** (the default) | Within their permissions (Sync needs `pipes:write`, Drop `pipes:drop`, ...). |
| **Owner and team managers** | Its owner, the managers of its team and administrators. |

**Access…** sets them: the owner (administrators and the team's managers may name any user; an owner may give the pipe up), the team (one of yours; every team for administrators) and the two choices above. It is shown to the pipe's owner, its team's managers and administrators, and the console refuses a change from anyone else, whatever the browser sends. A pipe you register from the console (New pipe, Copy, a new Pipe of a Pipeline) is yours, in your primary team.

**Locks…** turns a pipe's locks on and off: **Edit parameters**, **Sync** (syncs, replaced windows, deduplication and verification), **Clear rows**, **Drop or rebuild** and **Delete**. A lock refuses that operation for everyone, administrators included, from the console, the API, the CLI, Python and jobs alike: clicking Sync on a pipe locked against syncs says which lock refuses it and how to remove it (`mrsm unlock pipes ...`, or **Manage > Locks…**). Its owner, its team's managers and administrators may lock and unlock it from the console.

On an instance where nobody set a team, an owner or a lock, every user sees and may change every pipe within their permissions, as in earlier versions. The console, the API and MCP enforce access; the CLI and Python with the instance's own credentials see everything (locks still apply). A pipe's SQL runs with its connector's credentials, so access alone cannot keep a definition from reading another team's tables: give each team its own connectors (database users) when that matters. See [Teams and access](/reference/actions/teams-and-access/) for the same settings in the CLI.

### New pipe

**New pipe** (`/dash/pipes/new`, the button above the table) registers a pipe from the console, like `mrsm bootstrap pipes`:

1. Choose the **Connector** (the list has your connectors and plugins, without those reserved for other teams; any keys may be typed), the **Metric**, an optional **Location** and the **Instance** (the selected instance by default).
2. For a `sql:` connector, write the **Query** and click **Preview**: the first 10 rows of the query on that connector (references like `{{ Pipe('sql:main', 'raw') }}` resolve on the instance). The preview's columns fill the **Datetime column** and **ID column** choices, and the datetime column is guessed (a datetime column, else a column named like one whose values are dates). For other connectors, set the pipe's **Parameters** (JSON or YAML), e.g. a plugin's `fetch` options.
3. **Team** starts at your primary team and **Visibility** at Team (see [Pipe access](#pipe-access-and-locks)); you own the new pipe.
4. Click **Register** to open the new pipe's page, or **Register and sync** (for users who may sync) to also start its first sync and watch it there.

Nothing is saved before you click one of them. Registering follows the API's rules: `api:permissions:registration:pipes` must be on, you need the `pipes:register` permission (the Editor role has it), a SQL definition needs `pipes:define_sql`, and with `--secure` only administrators register pipes. For users who are not administrators, the console refuses a query (in Preview, and in a pipe's definition later) that reads Meerschaum's own tables (`mrsm_*`), runs query text through a function (`query_to_xml()`, `dblink()`, ...), uses an `MRSM{...}` config reference, names the table of a pipe they may not see, or reads the database's catalog while the instance has pipes they may not see: the same checks as the API server's.

!!! warning "Registering a SQL pipe reads with the connector's credentials"
    **Preview** runs your query with the credentials of the connector you chose, as every sync of the pipe does once it is registered. The checks above are a best-effort reading of the SQL, not a parser: whoever may set SQL definitions can still read whatever those connectors can read. When some users should not, take `pipes:define_sql` (or registration) away from them (a Viewer role, `start api --secure`, `api:permissions:registration:pipes: false`), restrict connectors to teams (`teams:`), and connect with database users that can read only what your pipes need.

## Pipelines

The **Pipelines** page (`/dash/pipelines`) shows the dependency graph of your pipes, a Gantt chart of the latest or selected run, the runs table and the run events, and a pipe's page has its parents and children on the **Lineage** tab. See [Visualization](/reference/pipelines/visualization/#in-the-web-console).

A saved Pipeline has an access of its own, like a pipe: **New pipeline** asks for its **Team** and **Visibility** (your primary team and Team by default), and you own it. Its members you may not see are only counted (**1 hidden pipe**) on its card and canvas, and you may not run it, analyze its tags or clear its pipes until you may change every member. Runs, the canvas, lineage, plans and the Overview show only the pipes you may see; a run with none of them is not listed, and a stage of a pipe you may not see is shown as **Hidden pipe**.

## Plugins

The **Plugins** page has two tabs:

- **Installed** (`/dash/plugins`): the plugins installed on the server, with their version, their kind (**Python**, or **Rust** as a single `.rs` file or a directory with a `plugin.toml`), what each provides (connectors, a data source, actions, pages, API routes, MCP tools, sync hooks) and whether the packages it requires are installed. A plugin that failed to import says so. Administrators can **Update** a plugin installed from a repository (the latest version from that repository) and **Uninstall** it.
- **Repository** (`/dash/plugins?tab=repository`): the plugins published to this API as a [repository](/reference/connectors/#instances-and-repositories) (`mrsm register plugin <name> -r api:<label>`), to read and download. A plugin's owner edits its description here. Administrators can **Install** a published plugin on the server (**Reinstall** when it is installed already).

Open a published plugin's page to inspect its **Source**, then choose a file. Administrators can also open an installed plugin's name to read its local source. Source previews are read-only; large, binary, or linked files may not have a preview.

Install, Update and Uninstall run the usual actions (`install plugins`, `uninstall plugins`) as one of the console's runs, listed under Jobs > **Runs**; the page says when the action finished, with **View output**. When the change adds or removes a plugin's pages or API routes, the server restarts its Python host to load it. An install from this server's repository downloads the plugin from the server itself (the job's connector `api:this_server`, also recorded as the plugin's origin for **Update**); on a `--private` server that connector carries a token of the administrator who clicked, which may only read plugins (`plugins:read`) and expires after 30 minutes.

<img src="/assets/screenshots/web-console-plugins.png" alt="Plugins page">

## Jobs

The **Jobs** page (`/dash/jobs`) lists the server's [background jobs](/reference/background-jobs/) you may see: a job started from the console or the API belongs to the user who started it and their primary team (its team's members see it), and a job created on the server (the CLI, Compose, systemd) is Everyone's. Its **Runs** tab (`/dash/runs`) lists the console's own runs (a pipe's Sync, a Pipeline's Run, a plugin's Install): yours, your team's members' for its managers, and every one for administrators. Each job's status says how it is doing:

| Status | Meaning |
|---|---|
| **Running · last good sync 2 min ago** | Its last lap succeeded. |
| **Stalled · last good sync 3 h ago** | No successful lap for three times its interval (`--min-seconds` or its `-s` schedule; an hour for `--listen`). |
| **Failing · 4 failed syncs** | Its laps fail one after another. |
| **Stopped · last sync failed** | It stopped after a failure. |

The dot before the status has the state's color, as on the Overview: green while running, amber when stalled, red when failing or stopped after a failure.

**Start**, **Pause**, **Stop**, **Restart** and **Delete** manage a job (Delete asks you to type the job's name), each shown to the users with its permission (`jobs:execute`, `jobs:pause`, `jobs:stop`, `jobs:delete`: Editors start jobs, Operators do all of it), and **Logs** (`logs:read`) views, follows or downloads its output. With more than eight jobs, the page lists them in a table with a filter box (on a phone, a list: each job's name with its status under it), and each row's **⋮** menu has the same actions. A link with `?status=stalled` (the Overview's Jobs tile) shows the jobs in one state: `running`, `stalled`, `failing`, `paused` or `stopped`.

### New job

**New job** creates a job, like `start job` (or `-d`) in a terminal:

1. **Action**: `sync pipes` by default, or any action the console offers (plugin actions included).
2. For an action on pipes, the **Connectors**, **Metrics**, **Locations** and **Tags** of the pipes to select on the selected instance (every pipe when left empty); for `sync pipelines`, the saved **Pipeline**.
3. **Arguments**: more flags, for example `--workers 4`.
4. **Repeat**: **Once** (with **Restart if it stops**), **On a schedule** (`-s`: `every 10 minutes`, `daily starting 00:00`, or a cron line such as `0 * * * *`) or **Continuously** (`--loop`, with the seconds between runs).
5. **Name**: a generated name you may change.

**Command** shows the terminal command that creates the same job, and **Create job** creates and starts it with the server's executor (`systemd` where the server uses it, else a local job). Creating jobs needs `jobs:write` and `actions:execute` (the Operator role and administrators; only administrators under `--secure`), and a non-administrator's command must be one the console lets them run. The new job is yours, in your primary team. The server must serve the `jobs` route group (`api:permissions:routes:allowlist`).

## Users & Teams

**Users & Teams** (the user menu > **Users & Teams**) has two tabs.

**Users** (`/dash/users`) is a table of the users: **User**, then for administrators **Type** and **Role**, **Teams** (a team's managers are marked) and **Email**. Administrators see every user; other users see themselves and the members of their teams. An email address is shown to administrators and to the managers of a team the user is in, and the column is not shown to anyone else (everyone sees their own address on their own page).

Administrators have **Add user** (username, password, type, role, teams and email) and, in each user's **⋮** menu:

- **Edit**: the **Type** (user or admin), the **Role** (**Default**: the server's default role, **Viewer**, **Editor**, **Operator** or **Custom**) with its permissions as a grid of checkboxes (choosing a role sets the grid; changing the grid makes the role Custom, or the role it now equals), the **Teams** the user is in, the ones they **Manage**, and their **Primary team** (the team of the pipes and jobs they create; by default their first team other than Everyone);
- **Reset password**, and **Delete** (type the username to confirm; the pipes they owned stay with their teams, without an owner).

The instance's only admin cannot be demoted or deleted: make another user an admin first. A user whose type, password or account changes is signed out of their console sessions, so the change applies at once (a user whose role or teams change stays signed in with the new rights, and the administrator making the change keeps their own session).

| Role | What it may do in the console |
|---|---|
| **Viewer** | Read pipes, Pipelines, runs, jobs, logs, plugins and the connectors list; their own account and tokens. |
| **Editor** (the default role) | A Viewer's, plus register, edit and sync pipes (SQL definitions included), save Pipelines and start existing jobs. |
| **Operator** | An Editor's, plus run actions (Exec), use the web terminal, and create, stop, pause and delete jobs. |
| **Admin** (a user's type) | Everything, including users, teams, connectors' details and Test connection. |

**Teams** (`/dash/teams`) lists the teams, Everyone first (every user is in it, and it cannot be renamed, deleted or left), with their members, managers and pipes. Administrators create teams (**New team**: the username rules, unique without case) and **Rename** or **Delete** them in a team's **⋮** menu: renaming a team renames it in its pipes and jobs, and deleting one (type its name to confirm; the dialog says how many pipes and jobs) moves them to Everyone. **Members** (for administrators and the team's managers) adds and removes members and makes them members or managers; in Everyone it lists only its managers, who manage every user.

## Who sees what

| | Viewer | Editor | Operator | Team manager | Admin |
|---|---|---|---|---|---|
| Pipes, Pipelines, Performance, Jobs, Plugins, Connectors pages | ✓ | ✓ | ✓ | ✓ | ✓ |
| Terminal page and Exec | | | ✓ | (by role) | ✓ |
| Register, edit and sync their pipes | | ✓ | ✓ | ✓ | ✓ |
| Access… and Locks… | their own pipes | their own pipes | their own pipes | their team's pipes | every pipe |
| Users & Teams: other users' emails | | | | their teams' members | every user |
| Add users, create teams | | | | | ✓ |

A team manager has the permissions of their role; managing a team adds its pipes' access and locks, its members and their email addresses.

## Tokens

Create and manage long-lived [tokens](/reference/api-instance/tokens/) on the page `/dash/tokens` (the user menu > **Tokens**): **New token** registers one and shows its secret once, and each token's **⋮** menu edits, invalidates or deletes it. **Grant all scopes, including custom and future ones** (the scope `*`) starts on for administrators and holders of `*`; anyone else starts from their own permissions. Whatever its scopes, a token never does more than its user may when it is used: the server intersects them with the user's current permissions, so a demoted user's tokens lose the rights they lost.

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
