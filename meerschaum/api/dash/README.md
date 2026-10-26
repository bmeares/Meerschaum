# `meerschaum.api.dash`: the web console

The Plotly Dash console of v4, restored on the v5 façade (design decision D21). The Rust server
(`mrsm start api`) serves the API, `/docs`, `/engine/*` and MCP and proxies `/dash/*` to the
Python API host (`meerschaum/api/host/`), which builds and mounts this package's one Dash app at
`/dash` (`requests_pathname_prefix` `/dash/`). Without the `dash` extra (`pip install
"meerschaum[dash]"`) `/dash` shows a pointer page.

```
api/dash/
├── __init__.py        # build_dash_app(): the one Dash app (built on first use, not on import)
├── pages/             # the layouts: dashboard, pipes, jobs, plugins, users, tokens, login,
│                      #   register, password-reset (a `@web_page`), pipelines (new)
├── callbacks/         # the callbacks of those pages; custom.py registers the plugins'
│                      #   `@dash_plugin` functions and `@web_page` pages (and their assets)
├── components.py      # navbar, buttons, build_cards_grid, alert_from_success_tuple, ...
├── keys.py, pipes.py, jobs.py, plugins.py, tokens.py, users.py, connectors.py
├── execute.py         # what the Exec button, a pipe's Manage menu and "Follow logs" run
├── engine.py          # the Syncing Engine's graph, runs, Gantt bars and events (pipelines page)
├── sessions.py        # the session store shared with the Rust server's terminal (5 s cache)
├── webterm.py         # the terminal iframe (`/webterm/<session id>`) and its controls
├── _support.py        # stand-ins for the v4 internals v5 deleted (get_job, get_pipe_query, ...)
└── assets/            # served by Dash at /dash/assets/ (ansi_up.js, logos, webterm_keys.js)
```

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
- **Pipelines** (`/dash/pipelines`) and the **Parents & Children** tabs of a pipe card read the
  engine through `meerschaum.core.Pipeline`'s bindings (the functions behind `/engine/*`).
  `pages/pipelines.py::route` builds each Pipelines page per request: the registry's list,
  `/dash/pipelines/new` (the stepper, `callbacks/pipeline_pages.py`), `/dash/pipelines/explore`
  and `/dash/pipelines/<name>` (header, Canvas | Runs, Edit mode; `callbacks/pipelines.py` and
  `callbacks/pipeline_management.py`). The registry is the console's global instance: the
  `mrsm-instance` cookie written by either instance selector (`assets/global_instance.js`, read by
  `engine.global_instance_keys`); `?instance=` overrides it. All labels use `Pipe(...)`.
- **Sessions.** The hosted console always stores sessions in the instance database, matching the
  Rust terminal and log stream even when a Valkey cache is configured.
- **Terminal.** The terminal is embedded in the Web Console; `/dash/webterm` falls back to it.
- Removed in 5.0 (importing them raises a `ModuleNotFoundError` naming the replacement,
  `meerschaum/_internal/_removed.py`): `websockets` and `sync`.

Tests: `tests/api_host/` (the host, the console's pieces without a browser) and `tests/console/`
(Playwright against a real `mrsm start api`; opt-in, `MRSM_TEST_CONSOLE=1`).
