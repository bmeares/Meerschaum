# Dash Web Console

The Meerschaum web dashboard. A `dash_extensions.enrich.DashProxy` app
(`__init__.py`) mounted onto the FastAPI app as WSGI middleware under the
`/dash` endpoint.

## Layout

```
api/dash/
├── __init__.py        # builds dash_app, root layout, mounts onto FastAPI
├── components.py      # shared dcc/html components (intervals, navbars, ...)
├── pages/             # one module per route; each exports `layout`
├── callbacks/         # one module per page; registers @dash_app.callback handlers
├── <feature>.py       # builder helpers (jobs.py, pipes.py, ...) — build_*_card etc.
└── assets/            # static css/js served by Dash
```

`pages/*` define **what** a route renders. `callbacks/*` define **behavior**.
`<feature>.py` at the top level hold the `build_*` helpers both call.

## Routing (read this first)

There is **no** Dash Pages (`dash.register_page`) usage. Routing is one
hand-rolled callback.

- Root layout (`__init__.py`) is static: a `mrsm-location` (`dcc.Location`),
  a few stores, the persistent `pages_offcanvas`, and an empty
  `page-layout-div`.
- `update_page_layout_div` (`callbacks/dashboard.py`) is the router. It maps
  `mrsm-location.pathname` → a page's `layout` via the `_paths` dict
  (`callbacks/dashboard.py:92`), enforces `_required_login`, and writes the
  result into `page-layout-div`.
- Registering a page = add an entry to `_paths` (and `_pages` for the nav,
  `_required_login` if auth-gated). Plugin pages do this dynamically via
  `add_plugin_pages` (`callbacks/custom.py`).
- The router wraps each page in `html.Div(layout, key=f'page-content::{path}')`.
  The **key matters**: React reconciles by type+position, not Dash id, so
  without a changing key it reuses DOM nodes across navigation and leaks one
  page's subtree (and its still-firing callbacks) into the next. Keep the key.

### Per-page lazy rendering (jobs, pipes, tokens)

These pages put their **own** `dcc.Location` in their `layout` plus an empty
output `Div`, and a page-local callback fills the Div from that pathname
(e.g. `render_job_page_from_url` in `callbacks/jobs.py`). So loading one of
these pages is **two** round-trips: the router mounts the page shell, then the
page-local callback fetches and renders content. Expect a visible gap on first
paint — it is the second round-trip, not slow data. Content does **not** live
in `layout`; do not look for it there.

## Refresh intervals

`components.py` defines `dcc.Interval` components (e.g. `refresh-jobs-interval`)
that re-fire callbacks on a timer to live-update status. Cheap per tick but
constant — raise the `interval` before adding work to the callback.

## Loading spinners

Pages may wrap output in `dcc.Loading`. Avoid `delay_hide` — it forces the
spinner to linger that many ms *after* content is ready, adding dead time to
every load. `delay_show` (defer spinner until load exceeds N ms) is fine; it
only suppresses flashes on fast loads.

## Theme

Dark (Darkly) is default; the `dbc-dark-store` callback flips to light
(Flatly) per route for plugin pages registered `@web_page(dark_theme=False)`.
The light sheet is disabled inline before first paint to avoid a flash. See
the comments in `__init__.py` and `callbacks/dashboard.py`.
