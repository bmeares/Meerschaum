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

The server follows the configuration file, so the new connector works without a restart. Deleting a team keeps its database, roles and connector; drop them yourself when you no longer need the data. Until you do, the connector stays reserved for the team's name (`teams: [<name>]`), so a team created later with that name, or renamed to it, can use it: remove the connector (or its `teams`) when you delete the team.

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
    Anyone who can open a public dashboard can run any query with the credentials of every data source in the anonymous organization, not only the dashboard's own queries. Give those data sources read-only credentials that reach public data only. The stack's **Meerschaum Main** data source connects with `sql:main`'s user; see below. A team can never use the anonymous organization: a team named like it is refused, and so is a rename to that name.

The built-in **Syncs** and **Engine** dashboards stay in the main organization.

### The stack's main data source

The stack provisions a **Meerschaum Main** data source in Grafana's main organization, which is also the stack's anonymous organization (`GF_AUTH_ANONYMOUS_ORG_NAME: Main Org.`). It connects with `sql:main`'s user, which in the stack's database is a superuser that owns the main database. Anyone who can query it (every member of the main organization, and every signed-out visitor while anonymous access is on) can read a schema team's tables in the main database and, as a superuser, reach a team's own database too. Users without a team and the members of open teams are in the main organization (members of isolated teams are taken out of it). Before you give teams data of their own on the stack's database, point **Meerschaum Main** (`mrsm edit config stack`, under `grafana: datasource:`) at a read-only role that can read only public tables; until then, treat that database as readable by every user of the main organization and, while anonymous access is on, by anyone.
