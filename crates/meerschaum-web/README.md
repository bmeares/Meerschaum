# meerschaum-web

The server-rendered `/dash` console (design §8). Askama renders the chrome; ordinary GET/POST
forms work without JavaScript. HTMX updates pipe filters. Plotly figure JSON, MapLibre map
specifications, sortable tables, and Cytoscape run nodes render on load and after HTMX swaps.

## Composition

`meerschaum_web::app(state, WebConfig::for_state(&state))` returns the API and console router.
The server's additive `app_with_console` hook keeps the dependency direction `web -> server`.
The executable must choose this constructor when serving the console. `--no-dash` is respected.
`WebConfig::with_cache(&valkey)` selects an existing cache connector; otherwise sessions use
the configured instance. Supply the configured session TTL and HTTPS cookie policy in WebConfig.
`webterm` optionally links a same-origin terminal route. The terminal PTY remains server-owned.

Exact core and registered page paths are mounted, never `/dash/*`. Unmatched plugin-owned
Dash paths reach the server's existing Python-host fallback. Manifest consumers populate
`pages`, `dash_plugins`, and `legacy_pages`. Reserved core paths, wildcard registrations,
duplicate routes, and pages overlapping a Dash owner are rejected. Redirects retain queries
and do not repeat an already-prefixed plugin name.

`PluginPage` describes trusted plugin HTML, an asynchronous native `PageHandler`, or a same-host API fragment route, with page group,
login requirement and optional omitted chrome. Inline HTML uses the same `data-mrsm-figure`
and `data-mrsm-map` contract as host fragments. Plugin HTML is trusted extension code, not a
user-content sanitization boundary. Core data is HTML escaped.

## Sessions and Python integration request

`mrsm-session-id` is a UUID cookie with Path=/, HttpOnly, SameSite=Lax and configurable Secure.
Default lifetime is 43,200 minutes, matching v4. Login goes through the Rust `/login` route.
The session payload preserves `username`, optional `admin` and `authenticated`, and adds a
random `csrf` field for console mutations. The server guards `mrsm_sessions` against pipe
alias reads and retargeting, like its other internal tables. API credentials stay server-side; scopes and user
existence are refreshed for each console API request. Logout deletes the shared session.

With Valkey, use `mrsm_session_id:{uuid}` -> compact JSON with EX ttl, exactly as v4.
The fallback follows the internal users-pipe pattern:

```python
Pipe('mrsm', 'sessions', instance=instance, temporary=True,
     target='mrsm_sessions', columns={'primary': 'session_id'},
     dtypes={'session_id': 'str', 'data': 'str', 'expires': 'int'},
     upsert=True, hypertable=False)
```

Rows contain `session_id`, compact JSON text `data`, and Unix-seconds `expires`.
`meerschaum/api/dash/sessions.py` must replace `_active_sessions` with this pipe when no cache
connector is configured: sync by primary key, query with params={'session_id': id}, ignore
expired rows, clear on deletion. Preserve unknown payload keys during updates. The fallback
is deliberately durable rather than reproducing the v4 process-local dictionary. No Python
files are modified by this task. The host shim must consume the shared cookie and populate
its core Dash stores. Configure both hosts with the same instance/cache and TTL.

## Front-end assets

Vendored files and upstream URLs are pinned in `assets/versions.json`, with adjacent licenses:
HTMX 2.0.4, Plotly.js 2.35.2, MapLibre GL JS 4.7.1, Cytoscape.js 3.30.4, xterm.js 5.5.0.
There are no runtime CDN fetches. Map specs referencing external tiles remain plugin-owned.
The Python SDK should pin a compatible Plotly.py 5.24.x range for Plotly.js 2.35.x figures.

## Verification

Always isolate every invocation, including Rust tests:

```sh
export PATH="$HOME/.cargo/bin:$PATH"
export MRSM_ROOT_DIR=$(mktemp -d) MRSM_PLUGINS_DIR=$(mktemp -d)
cargo fmt -p meerschaum-web
cargo clippy -p meerschaum-web --all-targets -- -D warnings
cargo test -p meerschaum-web
cargo test -p meerschaum-server
```

Tests exercise the composed Axum router, auth redirects, cookies, fragments, pipe filtering,
mutations, escaping, expiry, Valkey payload compatibility, and SQLite sharing with a Python
stub. An installed Chromium is used for a headless Plotly page-contract smoke test; no browser
is downloaded. Temporary SQLite files, browser profiles and plugin directories are isolated.

## Route-group gating

Like v4 (`_console_pages_allowed`), the console's own pages, actions and partials are served only
when `api.permissions.routes.allowlist` allows the `dash` group; otherwise they answer 404. The login
page, logout, assets and plugin pages stay. `--no-dash` serves no console and (host config) no plugin
pages or Dash apps.

## Browser tests

`MRSM_TEST_CONSOLE=1 PYTHONPATH=$(pwd) python -m pytest tests/console` (Playwright, real `mrsm start api`).

## Remaining milestone work

This is not yet the full b3 parity gate. The pipeline view has dependency edges, run status, a filtered runs table,
and timing bars, but needs ledger lag and a read-side engine events stream.
The current server only accepts events (POST); it has no events subscription endpoint.
Plugin management supports search, metadata, install, delete and archive upload (`POST /dash/plugins/upload`, CSRF-checked, forwarded to `POST /plugins/{name}` with the session user's credentials and the API's size limit, `ARCHIVE_MAX_BYTES`).
User and token forms delegate writes to the API. Settings exposes password changes. The terminal
is a configured link, not a PTY implementation. Page-contract callbacks are ordinary plugin
HTTP routes; manifest auto-discovery and dynamic native plugin handlers need the plugin/host
integration. Pixel-level equivalence to v4 Dash is not claimed. Source-sync execution depends
on the configured API/worker backend.
