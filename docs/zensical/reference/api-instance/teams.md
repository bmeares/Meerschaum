# 👥 Teams on One Instance

Several teams can share one Meerschaum API instance: each signs in to the same web console, HTTP API and MCP server, and sees its own pipes, jobs, connectors and plugins. An **isolated** team is walled off from everything that belongs to Everyone, and a team may get a database of its own on your existing database server.

[Roles and teams](/reference/api-instance/permissions/#roles-and-teams) describes roles, members and managers. This page covers what an administrator sets when creating a team.

## Create a team

In the console, open **Users & Teams**, then **New team**. From a shell on the server:

```bash
mrsm bootstrap teams wetlands
```

Both ask the same questions:

| Setting | What it does |
|---|---|
| **Hide Everyone's pipes, jobs, connectors and plugins** | Makes the team isolated (see below). |
| **Storage** | **New database** or **New schema** on the server of an existing PostgreSQL-family connector (`sql:main` by default), or an **Existing instance** as the team's default instance. |
| **Connectors** | Other connectors the team may use. |
| **Plugins** | The installed plugins the team may use. |
| **Members install, edit and upload plugins** | Lets the team's members add plugins of their own. |

Answer up front with `--params` to skip the questions:

```bash
mrsm bootstrap teams wetlands \
  --members ann,cal --managers ann \
  --params '{"isolated": true, "database": "database", "base": "sql:main", "plugins": ["noaa"]}' \
  --yes
```

`database` is `database`, `schema` or `none`; `name` and `label` override the database (or schema) name and the connector label, which default to the team's name in lowercase with underscores.

Change a team's settings later with its **Settings** in the console. They are stored in the team's `attributes` (`isolated`, `instance`, `connectors`, `plugins`, `plugins_write`), which `PATCH /teams/{name}/edit` also accepts from an administrator.

## Isolated teams

A user is walled off when every team they are in (other than Everyone) is isolated. A user who is also in an open team keeps that team's ordinary view. Administrators are never walled off.

A walled-off user:

- sees only their teams' pipes, saved Pipelines, runs and jobs. Pipes without access settings (they belong to Everyone), pipes shared with "everyone" by other teams and local jobs are hidden;
- may use only the connectors reserved for their teams (`teams: [wetlands]`) and those their teams were granted. Unrestricted connectors, the server's own instance included, are not offered;
- sees only the plugins their teams were granted or installed, in the console and in the plugin repository routes;
- may register pipes whose connector is one of theirs, a granted data plugin (`plugin:<name>`), or a plain label that names no connector of the server's (data pushed through the API, such as `manual:gauge`);
- sees only their own teams and the accounts of users who share one;
- never holds `actions:execute`, `webterm:use` or `instance:chain`, whatever their role. Actions and the terminal run on the server with its own credentials, which reach every team's data. They still sync and verify the pipes they may edit;
- gets no plugin tools, resources or prompts over MCP (they are not attributed to a team's plugin).

When a request names no instance, the console, the pipe routes and the MCP tools use the default instance of the caller's team.

!!! note "Serve the API privately"
    Without `--private`, `/users` and `/plugins` answer callers who send no credentials, as they always have. The legacy SDK's `Bearer None` header also means anonymous. Start the server with `mrsm start api --private` so those listings need a valid login and follow these rules.

## A database for each team

**New database** and **New schema** need a PostgreSQL-family connector (`postgresql`, `timescaledb`, `timescaledb-ha`, `postgis`, `citus`) whose user may create roles and databases. Meerschaum then creates:

- a login role `mrsm_<team>` with a generated password. It owns the team's database (or schema) and is not a superuser;
- a read-only login role `mrsm_<team>_ro` for dashboards, which may only `SELECT` what the team's role creates;
- the database (TimescaleDB and PostGIS are installed in it when the base server has them), which nobody else may connect to, or the schema, which becomes the roles' `search_path`. Nothing on it is granted to `PUBLIC`, nor to the stack's read-only Grafana role (`mrsm__main_ro`, see [the stack's main data source](#the-stacks-main-data-source)), also when that role already exists;
- a connector `sql:<team>` in your configuration, reserved for the team, which becomes the team's default instance. The team's pipes are registered there. Only that connector is added to `config/meerschaum.yaml`, with the role's password: the rest of the file is kept as it is written (references such as `MRSM{...}` stay), connectors and patches from environment variables (`MRSM_SQL_MAIN`, `MRSM_CONFIG`, ...) are never written to it, and the file is made readable by you only.

It also takes `CONNECT` on the base database away from `PUBLIC`, so a team's role cannot open it. Every login role that could connect before keeps an explicit grant.

A database is the stronger wall: the roles of one team cannot connect to the database of another. A schema shares the base database: its role cannot read other tables, but it can see their names.

The server follows the configuration file, so the new connector works without a restart.

### Deleting and renaming

Deleting a team keeps its database, roles, connector and data; drop them yourself when you no longer need them. Its connector is no longer reserved for its name: its `teams` becomes `Deleted team <name> #<id>`, which no team can be named, so a team created later with the same name, or renamed to it, gets nothing of the old one. Renaming a team moves its reservations to the new name. Both happen in the console, with `delete teams` / `edit teams` and through the API (`DELETE /teams/{name}/delete`, `POST /teams/{name}/rename`); the configuration file is rewritten as it is written (references such as `MRSM{...}` are kept). A deletion or rename made directly with the Python SDK (`Team.delete()`, `Team.rename()`) does not change the configuration.

A connector that is still reserved for a name no team has (left by such a deletion, by a version before 5.1, or written by hand) is not handed out by name: creating a team with that name, or renaming one to it, is refused and names the connector. An administrator who wants the team to use it says so: `"adopt": true` in the body of `POST /teams/{name}/register` or `/rename`, or `--params '{"adopt": true}'` with `register teams`, `bootstrap teams` or `edit teams`. The console does not adopt; remove the connector's `teams` first, or use the command line.

`api:permissions:instances:allow_multiple_instances` must stay `true` (the default) for teams to use their own instance.

## Plugins

Plugins run on the server as its operating-system user. Granting a plugin lets a team use it; **Members install, edit and upload plugins** lets them add code that runs on the server, so turn it on only for teams you trust with the server. What keeps one team's plugin code away from other teams' credentials, and what does not, is described in [Team-scoped workers](#team-scoped-workers).

With it on, a team's members can install a plugin from the repository, create one from a template, or **upload** files they have locally (a `.py` or `.rs` file, several files, or a `.zip` / `.tar.gz` archive) from **Plugins → New plugin**. What they add belongs to their team: they may edit, update, publish and uninstall those plugins, and nobody else's.

## Team-scoped workers

Python code that runs for an isolated team (its plugins' connectors, fetches, hooks and `register()` during a sync, its plugin actions, its jobs) runs in processes that never receive the server's configuration:

- **On the API server**, each isolated team gets its own pool of Python workers, created on its first call. They are started with a configuration that holds only the team's connectors, in an owner-only file under `<root>/team_scopes/<team id>/` named by `MRSM_CONFIG_FILE`, with an empty config directory of their own (`MRSM_CONFIG_DIR`), and with an environment built from an allowlist (see [The environment of a team's processes](#the-environment-of-a-teams-processes)): none of the server's `MRSM_CONFIG`, `MRSM_PATCH`, their `_FILE` forms, `MRSM_<TYPE>_<LABEL>` connector variables or other variables of its own.
- **A job of an isolated team** (started through the API, the console or MCP, or restarted by the server after a reboot) gets the same configuration in its own directory (`jobs/<name>/.mrsm_config.json`, owner-only) and the same allowlisted environment, so its `mrsm` process and the workers it starts see only the team's connectors. A job stays with its team's settings for good: when the team is deleted, its jobs move to Everyone and get no connector at all. The `env` the API server records for such a job (in its `properties.json`) holds only the allowlisted variables, so no later launch, the restart after a reboot or a `systemd` unit included, brings the server's environment back.
- **When plugin code calls back into Meerschaum** (a `pipe.sync(df)` from a plugin's `sync()`, a `subscribe()` sink), the pipe must be on one of the team's instances, name a connector the team may use, and be one a member of the team may change, as for an API request by one of its members. `{{ Pipe(..., instance='sql:other') }}` in a team's pipe resolves against the team's connectors only.

**What the team's configuration holds.** Its connectors: those reserved for it (`teams: [wetlands]`), those granted in its settings and its default instance, with their passwords read by the server (a `MRSM{file:...}` reference of the team's own connector is read once more each time the team's configuration is built). Every other connector the server or the defaults define (`sql:main`, `sql:local`, other teams' connectors, environment connectors) is not defined. `meerschaum:instance` is the team's default instance. `system`, `formatting`, `shell`, `pipes`, `jobs`, `dash` and `api` are passed as they are written, minus `api:grafana` and any value under a credential's name (`password`, `token`, `api_key`, ...), any `MRSM{file:...}` / `MRSM{env:...}` reference and any URI with a password. `plugins` holds the settings of the team's plugins only. `stack` (its compose file holds the database and Grafana passwords) and any other file of your own under the config directory are withheld. A plugin whose settings live elsewhere should move them under `plugins:<name>`.

**The environment of a team's processes.** A team's workers and jobs start from an empty environment and receive only:

- what a process needs to run and reach the network: `PATH`, `HOME`, `USER`, `LOGNAME`, `USERNAME`, `SHELL`, `LANG`, `LANGUAGE`, every `LC_*`, `TZ`, `TZDIR`, `TMPDIR`, `TEMP`, `TMP`, the `XDG_*` runtime, config, cache and data directories, `TERM`, `COLORTERM`, `NO_COLOR`, `COLUMNS`, `LINES`, the certificate locations `SSL_CERT_FILE`, `SSL_CERT_DIR`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`, the proxies `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`, `ALL_PROXY` (in both cases), `LD_LIBRARY_PATH` (and `DYLD_LIBRARY_PATH` / `DYLD_FALLBACK_LIBRARY_PATH` on macOS), the interpreter's and virtual environment's variables (`PYTHONPATH`, `PYTHONHOME`, `PYTHONNOUSERSITE`, `PYTHONUSERBASE`, `PYTHONSAFEPATH`, `PYTHONDONTWRITEBYTECODE`, `PYTHONPYCACHEPREFIX`, `PYTHONUNBUFFERED`, `PYTHONUTF8`, `PYTHONIOENCODING`, `PYTHONWARNINGS`, `PYTHONFAULTHANDLER`, `VIRTUAL_ENV`, `CONDA_PREFIX`, `CONDA_DEFAULT_ENV`), `RUST_LOG` / `RUST_BACKTRACE`, and on Windows `SYSTEMROOT`, `WINDIR`, `COMSPEC`, `PATHEXT`, `USERPROFILE`, `APPDATA`, `LOCALAPPDATA` and the like;
- Meerschaum's variables that hold no secret: `MRSM_ROOT_DIR`, `MRSM_PLUGINS_DIR`, `MRSM_VENVS_DIR`, `MRSM_WORK_DIR`, `MRSM_HOME`, `MRSM_SRC`, `MRSM_USER`, `MRSM_UID`, `MRSM_GID`, `MRSM_RUNTIME`, `MRSM_DEP_GROUP`, `MRSM_SERVER_ID`, `MRSM_PYTHON`, `MRSM_BIN`, `MRSM_JOB_SUPERVISOR`, `MRSM_JOB_PROGRAM`, the worker's own (`MRSM_WORKER_*`), `MRSM_NO_AUTO_INSTALL`, `MRSM_NO_UPDATE_CHECK`, `MRSM_NOASK`, `MRSM_NONINTERACTIVE`, `MRSM_DATAFRAME`, `MRSM_CONNECT_TIMEOUT`, `MRSM_DUCKDB_LIB` and `MRSM_CORE_TRACE`;
- for a job, its own name and Compose stamps (`MRSM_DAEMON_ID`, `MRSM__COMPOSE_PROJECT`, `MRSM__COMPOSE_ROOT`), from the job's settings, never from the server (which may itself run as a job);
- the team's `MRSM_CONFIG_DIR`, `MRSM_CONFIG_FILE` and `MRSM_TEAM_SCOPE`;
- the variables you list by name under `system.engine.python_team_env_passthrough`, for example `[AWS_PROFILE, PIP_INDEX_URL]`. Any `MRSM_*` name not allowed above (`MRSM_CONFIG`, `MRSM_PATCH`, their `_FILE` forms, `MRSM_<TYPE>_<LABEL>` connector variables) is never passed on, even when listed: the server warns and leaves it out.

Everything else in the server's environment (cloud credentials, tokens, a private package index's URL, `PG*` variables) stays out of a team's processes. Pass a variable on only when every isolated team may read it. The shared workers, administrators' work, open teams and every other job keep the server's whole environment.

**Who acts for a team.** The server decides it from the caller's identity, never from anything a client or plugin code sends: a user every one of whose teams (other than Everyone) is isolated acts for their team; a user who is also in an open team, and administrators, keep the open view and the shared workers exactly as before. A user in several isolated teams acts for the team that owns the pipe or job at hand when they are in it, else for their primary team; never for several teams at once.

**Changes take effect without a restart.** The server records each isolated team's connectors (keys only) in `<root>/team_scopes/` when it starts, after every team change it makes and every 10 seconds (the console changes teams directly). A team whose configuration changed (a connector granted or revoked, a rename, a deletion, a rotated password) gets new workers at its next call; the old ones stop, so a revoked connector is never served by a warm worker. When a team loses a connector or is deleted, its running jobs are restarted with the new configuration.

**The Python package must be 5.1.0 or newer.** Only a `meerschaum` package that knows team-scoped workers applies the team's configuration, so the server checks before any of a team's code runs. Each team worker reports, when it starts, the team and the configuration it loaded and the connectors that configuration defines; a worker that does not (an older package, which would load the default connectors such as `sql:main` instead), reports another team or configuration, or defines a connector the team was not given is stopped, and the call fails with `The Python worker for the isolated team '<team>' was stopped before running any of the team's code: ...` naming the package version, the interpreter and the fix (install `meerschaum>=5.1.0` there, or point `MRSM_PYTHON` at an interpreter that has it). A team job's own workers are checked the same way, so a job whose interpreter is too old fails instead of running. The Python API host reports the same capability: when its package is older and an isolated team exists, the server answers `503` with the cause instead of forwarding a console session or an isolated team member's request to plugin routes and pages (it cannot tell whose a console session is, so while the host is outdated this includes administrators' console sessions; bearer-token requests of users who do not act for an isolated team, and anonymous requests, are forwarded as before). The shared workers and the open side keep accepting a package of another version as before.

**In the processes every team shares,** the Python API host (plugin routes, pages and console callbacks of every team) and the shared workers, a request of an isolated team's member is a guard rail, not a wall: while the host serves it, `get_connector()` refuses connectors outside the team's set (`PermissionError: The connector 'sql:main' is not available to your teams.`) and `get_config()` leaves them out, including in threads the request starts. Meerschaum offers no function that sets or clears this.

!!! warning "What team-scoped workers protect, and what they do not"
    They keep the credentials of connectors a team was not given, and the server's environment variables outside the allowlist, out of that team's worker and job processes: its plugin code cannot read them from its configuration, its environment or Meerschaum's API. They do not protect against:

    - code in the shared API host, which can read the host's memory and environment (the server's whole configuration);
    - anything the server's operating-system user can read on disk: the server's own `config/` files with every connector's password, other teams' job directories and `team_scopes/` files, the plugins directory and other teams' plugins;
    - a team that was given a connector: it can use it fully (a team's own database is the wall between teams, see [A database for each team](#a-database-for-each-team)).

    Real isolation from a team's code needs a separate operating-system user or container per team, which Meerschaum does not provide. Allow **Members install, edit and upload plugins** only for teams you trust with the server.

**Settings and cost.** Under `system.engine` (`mrsm edit config system`):

| Key | Default | Meaning |
|---|---|---|
| `python_team_workers` | the shared pool's size (CPUs, 1 to 4) | The most live workers of all isolated teams together. A team that needs one more stops another team's idle worker, else waits for one to finish |
| `python_worker_team_idle_seconds` | `60` | Stop an isolated team's idle worker after this long (`0`: never) |
| `python_team_env_passthrough` | `[]` | Further variables of the server's environment, by name, that isolated teams' workers and jobs receive (never Meerschaum's configuration, patch or connector variables) |

A team's workers otherwise follow `python_workers`, `python_worker_max_calls` and `python_worker_max_memory_mb`. The shared pool is not affected. Each live team worker is a Python interpreter: measured on one Linux host (Python 3.14), an idle one held about 59 MiB resident (27 MiB of it private), a team's first call after its workers stopped took about 0.6 s instead of 0.08 s (the interpreter starts), and later calls about 0.2 s (the team's configuration is read again for each call). Ten teams syncing at once with `python_team_workers: 10` kept ten workers (about 590 MiB resident); with the default limit (4 on that host) they shared four, and calls waited up to about a second.

## Agents and MCP

Every user can connect an AI agent to the instance: **Tokens → Connect an agent** shows ready-to-paste configuration for Claude Code, Claude Desktop, Codex, OpenCode and other MCP clients. **New agent token** creates a read-only token and fills its API key into the snippets. See [MCP](/reference/mcp/).

A token acts as its owner, so an agent of a walled-off user sees what that user sees.

## Grafana

Meerschaum can sign the console's users in to Grafana and keep one Grafana organization per team. Grafana's data sources are separated per organization, so a team's dashboards can only query the team's own database, through its read-only role.

```yaml
### `mrsm edit config api`
api:
  grafana:
    enabled: true
    ### The address users reach Grafana at.
    root_url: https://your-api-host/grafana/
```

Then run `mrsm stack up -d`. With `enabled`, the stack's `grafana` service is started with what the sign-in needs, and the settings that keep it safe replace whatever your stack config has for them:

- **Grafana's administrator.** Meerschaum owns its password. Without `admin_password`, a strong one is generated once and kept in `api/.grafana_admin_password` under Meerschaum's root (`~/.config/meerschaum` by default; beside the API's secret key, readable by you only), and the stack's Grafana and API server are both given it. Grafana applies `GF_SECURITY_ADMIN_PASSWORD` only when its database is new: if an existing Grafana still has the default `admin` / `admin`, the API server changes it to Meerschaum's on its first call and logs that it did. If Grafana takes neither password, the integration stays off (the server logs why; users are told Grafana is not available). Grafana's default password is never used.
- **Who may name the user.** Grafana accepts the signed-in user (the `X-WEBAUTH-USER` header) only from `proxy_whitelist`. By default that is exactly one address: the stack's `api` service joins a network of its own with Grafana (`proxy_subnet`), at a fixed address, and reaches `grafana` there. No other container, and no connection from the Docker host, comes from that address. When the stack has no `api` service and `proxy_whitelist` is not set, the sign-in is turned off (`GF_AUTH_PROXY_ENABLED=false`) and `mrsm stack` says what to set. It never falls back to a range of addresses.
- **Served under `/grafana`.** `GF_SERVER_ROOT_URL` and `GF_SERVER_SERVE_FROM_SUB_PATH` are set unless your stack config sets them.

Anonymous access is not changed: `GF_AUTH_ANONYMOUS_ENABLED`, its organization and its role stay as your stack config (or Grafana) has them, so public dashboards keep working.

| Key | Default | Meaning |
|---|---|---|
| `url` | `http://grafana:3000` | How the API server reaches Grafana. The default resolves only inside the stack; an API server that runs outside the stack sets it, e.g. `http://localhost:3000`. |
| `admin_user` | `admin` | Grafana's server administrator. |
| `admin_password` | generated | Its password (never `admin`). |
| `root_url` | Grafana's own guess | The public address of `/grafana/`. |
| `proxy_whitelist` | the stack's API server | The address (or a list of addresses) Grafana sees the API server connect from. Set it when the API server runs outside the stack. |
| `proxy_subnet` | `10.213.71.0/29` | The network between the stack's API server and Grafana. Change it if it collides with one of yours. |
| `database_url` | the connector's host and port | How Grafana reaches the teams' database server (`host:port`). |

A Grafana you run yourself needs the same settings: `GF_AUTH_PROXY_ENABLED=true`, `GF_AUTH_PROXY_HEADER_NAME=X-WEBAUTH-USER`, `GF_AUTH_PROXY_HEADER_PROPERTY=username`, `GF_AUTH_PROXY_AUTO_SIGN_UP=true`, `GF_AUTH_PROXY_WHITELIST` set to the API server's address only, `GF_SERVER_ROOT_URL` and `GF_SERVER_SERVE_FROM_SUB_PATH=true`.

An API server that runs on the Docker host (outside the stack) reaches a published Grafana port from the Docker network's gateway address, which every process on that host shares: anyone with a login there can then name any user to Grafana. Run the API server in the stack on a host other people use.

Grafana's own port keeps working. Its old addresses without `/grafana` (such as `/d/<uid>/<slug>`) now redirect to the same page under `root_url`, so links and embeds of public dashboards still open, signed out, through the API server.

Users then open **Grafana** from the console's user menu (`https://your-api-host/grafana/`):

- a team gets its organization when an administrator creates it (**New team**, `bootstrap teams`) or saves its **Settings**. The organization's id is kept in the team's settings, and from then on that id, not its name, is the team's organization. A team with a database of its own gets a data source there for that database, and only there;
- members are Editors, the team's managers are organization Admins, and Meerschaum administrators are Grafana administrators. A Meerschaum user whose name is Grafana's administrator login (any case) is let in only if they are a Meerschaum administrator;
- members of isolated teams are removed from Grafana's main organization, so they see only their teams' dashboards. A member of isolated teams none of which has an organization is not let in;
- renaming a team renames its organization when it is renamed in the console, with `mrsm edit teams` or through the API (`POST /teams/{name}/rename`). A rename made directly with the Python SDK (`Team.rename()`) on a database instance does not reach Grafana: the organization keeps its old name until you rename it in Grafana (the team still uses it, by id);
- deleting a team (console, `delete teams`, API) keeps its dashboards: its organization is renamed `Deleted team <name> #<id>`, everyone but Grafana's administrator leaves it, and its data sources, with the team's database credentials, are deleted;
- memberships are checked when a user opens Grafana (at most once a minute). If Grafana's API cannot be reached, the request is refused instead of using memberships that may be stale. A deleted user, like a signed-out one, gets only what anonymous access allows.

A signed-out visitor to `/grafana` gets only what Grafana's anonymous access allows: Meerschaum forwards the request without any identity and without Grafana's own session cookies, so signing out of Meerschaum also signs out of Grafana. With anonymous access off, they are sent to the console's login.

!!! warning "Anonymous access runs queries"
    Anyone who can open a public dashboard can run any query with the credentials of every data source in the anonymous organization, not only the dashboard's own queries. The stack's **Meerschaum Main** data source connects as a read-only role that reads the instance's own schemas and nothing of any team's (see below). Data sources you add yourself in the main or the anonymous organization are not Meerschaum's to check: while anonymous access is on, the API server lists them in one warning when it starts; give them read-only credentials that reach public data only. A team can never use the anonymous organization: a team named like it is refused, and so is a rename to that name.

The built-in **Syncs** and **Engine** dashboards stay in the main organization.

### The stack's main data source

The stack provisions a **Meerschaum Main** data source in Grafana's main organization, which is also the stack's anonymous organization (`GF_AUTH_ANONYMOUS_ORG_NAME: Main Org.`). It connects as `mrsm__main_ro`, a read-only role the stack sets up before Grafana starts, whether or not `api:grafana` is enabled: see [Grafana's read-only data source](/reference/stack/#grafanas-read-only-data-source).

So every member of the main organization, and every signed-out visitor while anonymous access is on, can read the instance's own tables (the `public` schema and the other schemas `sql:main`'s user owns) and the schemas listed in `stack:grafana:readonly_schemas`, and nothing else: not a schema team's schema or tables, not a database team's database (no `CONNECT`), and they cannot write, create tables or schemas, or install extensions such as `dblink`. A team created after the role exists is never readable by it. Like any user of the database they can still see the names of other schemas and tables in PostgreSQL's catalogs.

A **Meerschaum Main** you changed to connect as another user is left as you wrote it and is included in the warning above.
