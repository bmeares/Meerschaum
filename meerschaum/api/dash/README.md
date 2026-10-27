# `meerschaum.api.dash`: the web console

The Plotly Dash console of v4, restored on the v5 façade (design decision D21). The Rust server
(`mrsm start api`) serves the API, `/docs`, `/engine/*` and MCP and proxies `/dash/*` to the
Python API host (`meerschaum/api/host/`), which builds and mounts this package's one Dash app at
`/dash` (`requests_pathname_prefix` `/dash/`). Without the `dash` extra (`pip install
"meerschaum[dash]"`) `/dash` shows a pointer page.

```
api/dash/
├── __init__.py        # build_dash_app(): the one Dash app (built on first use, not on import)
├── pages/             # the layouts: overview (`/dash`), terminal (v4's dashboard; dashboard.py
│                      #   re-exports it), connectors, pipes, jobs, plugins, users, tokens, login,
│                      #   register, password-reset (a `@web_page`), pipelines
├── callbacks/         # the callbacks of those pages; dashboard.py is the router (`_paths`,
│                      #   `_required_login`, `_pages`); custom.py registers the plugins'
│                      #   `@dash_plugin` functions and `@web_page` pages (and their assets)
├── components.py      # the top navigation (build_pages_navbar, build_plugin_navbar), icons,
│                      #   buttons, build_cards_grid, alert_from_success_tuple, ...
├── overview.py        # what the Overview's tiles read (one bulk call each) and draw
├── connector_listing.py  # the Connectors page's rows, secrets masked, and Test connection
├── pipes_table.py     # the Pipes table: rows, filters in the URL, search/sort/pages, slow columns
├── pipe_detail.py     # a pipe's page: header actions, tabs, Manage dialogs (Copy, Clear, ...)
├── new_pipe.py        # New pipe: connectors, flavor-limited Preview, datetime guess, Register
├── pipes_access.py    # who may read, act, edit and register on the Pipes pages
├── keys.py, pipes.py, jobs.py, plugins.py, tokens.py, users.py, connectors.py
├── execute.py         # what the Exec button, a pipe's Manage menu and "Follow logs" run
├── engine.py          # the Syncing Engine's graph, runs, Gantt bars and events (pipelines page)
├── sessions.py        # the session store shared with the Rust server's terminal (5 s cache)
├── webterm.py         # the terminal iframe (`/webterm/<session id>`) and its controls
├── _support.py        # stand-ins for the v4 internals v5 deleted (get_job, get_pipe_query, ...)
└── assets/            # served by Dash at /dash/assets/ (ansi_up.js, logos, webterm_keys.js,
                       #   nav.css, overview.css, ...)
```

- **Navigation.** Every console page starts with `components.build_pages_navbar()`: the logo (the
  Overview), Pipes, Pipelines, Jobs, Connectors, Plugins, Terminal and the plugins' Pages menu, the
  global instance selector (`pages-instance-select`; the Terminal page's is `instance-select`) and
  the user menu (Users, Tokens, Settings, Sign out). Build a fresh one per page (never share a
  component between layouts). The router renders every page inside `console_nav_context()`, and
  rebuilds a module-level layout's navbar per request (`personalize_navbar`), so the navbar knows
  the viewer (Terminal only for users who may use the terminal) and the current page. Built in a
  callback instead, it reads the session cookie and the referrer. A plugin's `@web_page` keeps its
  own layout under `build_plugin_navbar()` (logo and Sign out) unless it passes `navbar=True`.
- **Overview** (`/dash`): tiles for the pipes (failing ones from the per-pipe sync statuses), the
  saved Pipelines' last runs, the jobs by health and the recent engine runs, each loaded by its own
  callback (`callbacks/overview.py`) and refreshed every 30 s. Links to the Pipes and Jobs pages
  carry `?status=` (`failing`; `running`, `stalled`, `failing`, `paused`, `stopped`).
- **Connectors** (`/dash/connectors`): never sends a secret to the browser (names containing
  password, secret, token, key, ...; URI credentials; and the secret values wherever they appear).
- **Pipes** (`/dash/pipes`): `pages/pipes.py::layout` builds each page per request: the table
  (`pipes_table.py`: keys, tags and targets from one `fetch_pipes_keys`, then the row estimate,
  sync time, sync status and jobs of the rows on screen), `/dash/pipes/new` (`new_pipe.py`) and a
  pipe's page (`pipe_detail.py`; its tabs reuse the pipe card's section builders in `pipes.py`
  and their callbacks). Sync, Verify and the Manage actions run through `POST /execute` and
  stream into the page (`assets/pipes.js`). A store that drives other callbacks has exactly one
  writer: under the app's `MultiplexerTransform`, a write through `allow_duplicate=True` updates
  the store but does not trigger the callbacks listening to it. Three more Dash lessons from these
  pages: the renderer's time grows faster than the number of components one callback returns (100
  table rows of components took seconds to appear, 200 about 20 s), so the table is one
  `dcc.Markdown` of escaped HTML (`pipes_table._esc`, `dccLink` rows, sort buttons through
  `assets/pipes.js`); a cycle of callbacks (render, then statistics, then render) runs once per
  chain, so the statistics are asked for from a new chain (`dash_clientside.set_props` after a
  `setTimeout`); and after `history.replaceState`, a `dcc.Location`'s `search` and `href` props
  must be set to the new address, or its next update pushes its old `href` back.

- **One app, v4 paths.** `@dash_plugin` functions receive the console's `dash_app`; `@web_page`
  pages are pages of it at `/dash/<path>`, with the console's navbar. A page returning an HTML string
  (the v5 page contract) is shown in an iframe of the host's `/_pages/<path>?frame=1` document. The
  per-plugin Dash apps of 5.0.0rc1 (`/dash/<plugin>/`) are gone: those URLs redirect to the v4
  paths, and `make_dash_app()` / `get_dash_app()` return the one app.
- **The core ids work as in v4**: `session-store` (`{'session-id': ...}`), `mrsm-location`,
  `dbc-dark-store`, `mrsm-theme-dark` / `mrsm-theme-light`.
- **Exec.** v4 typed the chosen action into the terminal beside it. The terminal is now the Rust
  server's, so Exec runs the action as a job through the server (`POST /execute`) and follows its log
  over `GET /logs/<job>/stream`; without a server to call (a bare host) it runs a hidden job itself
  and polls the log. A pipe card's destructive Manage actions (delete, drop, clear) ask first.
- **Pipelines** (`/dash/pipelines`), a pipe's **Lineage** tab and the **Parents & Children** tabs
  of a pipe card read the engine through `meerschaum.core.Pipeline`'s bindings (the functions
  behind `/engine/*`).
  `pages/pipelines.py::route` builds each Pipelines page per request: the registry's list,
  `/dash/pipelines/new` (the stepper, `callbacks/pipeline_pages.py`), `/dash/pipelines/explore`
  and `/dash/pipelines/<name>` (header, Canvas | Runs, Edit mode; `callbacks/pipelines.py` and
  `callbacks/pipeline_management.py`). The registry is the console's global instance: the
  `mrsm-instance` cookie written by either instance selector (`assets/global_instance.js`, read by
  `engine.global_instance_keys`); `?instance=` overrides it. All labels use `Pipe(...)`.
- **Sessions.** The hosted console always stores sessions in the instance database, matching the
  Rust terminal and log stream even when a Valkey cache is configured.
- **Terminal.** The Terminal page (`/dash/terminal`, v4's dashboard) embeds the terminal beside the
  action builder; `/dash/webterm` and links to `/dash?action=...` open it.
- Removed in 5.0 (importing them raises a `ModuleNotFoundError` naming the replacement,
  `meerschaum/_internal/_removed.py`): `websockets` and `sync`.

Tests: `tests/api_host/` (the host, the console's pieces without a browser) and `tests/console/`
(Playwright against a real `mrsm start api`; opt-in, `MRSM_TEST_CONSOLE=1`).
