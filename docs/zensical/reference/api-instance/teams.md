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
- the database (TimescaleDB and PostGIS are installed in it when the base server has them), which nobody else may connect to, or the schema, which becomes the roles' `search_path`;
- a connector `sql:<team>` in your configuration, reserved for the team, which becomes the team's default instance. The team's pipes are registered there.

It also takes `CONNECT` on the base database away from `PUBLIC`, so a team's role cannot open it. Every login role that could connect before keeps an explicit grant.

A database is the stronger wall: the roles of one team cannot connect to the database of another. A schema shares the base database: its role cannot read other tables, but it can see their names.

The server follows the configuration file, so the new connector works without a restart. Deleting a team keeps its database, roles and connector; drop them yourself when you no longer need the data.

`api:permissions:instances:allow_multiple_instances` must stay `true` (the default) for teams to use their own instance.

## Plugins

Plugins run in the server's process with its credentials. Granting a plugin lets a team use it; **Members install, edit and upload plugins** lets them add code that runs on the server, so turn it on only for teams you trust with the server.

With it on, a team's members can install a plugin from the repository, create one from a template, or **upload** files they have locally (a `.py` or `.rs` file, several files, or a `.zip` / `.tar.gz` archive) from **Plugins → New plugin**. What they add belongs to their team: they may edit, update, publish and uninstall those plugins, and nobody else's.

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
    ### Grafana's server administrator (for its HTTP API). Change the password.
    admin_user: admin
    admin_password: <password>
    ### The address users reach Grafana at.
    root_url: https://your-api-host/grafana/
```

Then run `mrsm stack up -d grafana`. With `enabled`, the stack's `grafana` service is started with what the sign-in needs: it is served under `/grafana`, and it accepts the signed-in user only from the API server (the auth-proxy header, from Docker's own networks). Settings your stack config already has are kept.

| Key | Default | Meaning |
|---|---|---|
| `url` | `http://grafana:3000` | How the API server reaches Grafana. |
| `admin_user`, `admin_password` | `admin`, `admin` | Grafana's server administrator. A Grafana that already has data keeps the password it has; change it in Grafana too. |
| `root_url` | Grafana's own guess | The public address of `/grafana/`. |
| `proxy_whitelist` | `172.16.0.0/12` | Who may name the signed-in user to Grafana: the API server's address, or Docker's networks. |
| `database_url` | the connector's host and port | How Grafana reaches the teams' database server (`host:port`). |

!!! warning "Who may name the user"
    Grafana trusts the user named by an address in `proxy_whitelist`. The default covers Docker's networks, which on the Docker host includes local connections to Grafana's published port: narrow it to the API container's address, or stop publishing Grafana's own port, when other people have a login on that host. A Grafana you run yourself needs the same settings (`GF_AUTH_PROXY_*`, `GF_SERVER_ROOT_URL`, `GF_SERVER_SERVE_FROM_SUB_PATH`).

Grafana's own address changes to `http://localhost:3000/grafana/`.

Users then open **Grafana** from the console's user menu (`https://your-api-host/grafana/`):

- each team is an organization named after it. A team with a database of its own gets a data source there for that database;
- members are Editors, the team's managers are organization Admins, and Meerschaum administrators are Grafana administrators;
- members of isolated teams are removed from Grafana's main organization, so they see only their teams' dashboards;
- renaming a team renames its organization;
- memberships are checked when a user opens Grafana (at most once a minute), and a signed-out or deleted user has no access. If Grafana's API cannot be reached, the request is refused instead of using memberships that may be stale.

The built-in **Syncs** and **Engine** dashboards stay in the main organization.
