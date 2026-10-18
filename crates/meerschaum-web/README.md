# meerschaum-web

> **Internal crate.** `meerschaum-web` is an implementation detail of [Meerschaum](https://meerschaum.io)
> and may change in any minor release. Depend on the [`meerschaum`](https://crates.io/crates/meerschaum)
> crate instead.

The server-rendered `/dash` console (design §8). Askama renders the chrome; ordinary GET/POST
forms work without JavaScript. HTMX updates pipe filters. Plotly figure JSON, MapLibre map
specifications, sortable tables, and Cytoscape run nodes render on load and after HTMX swaps.

## Composition

`meerschaum_web::app(state, WebConfig::for_state(&state))` returns the API and console router.
The server's additive `app_with_console` hook keeps the dependency direction `web -> server`.
The executable must choose this constructor when serving the console. `--no-dash` is respected.
`WebConfig::with_cache(&valkey)` selects an existing cache connector; otherwise sessions use
the configured instance. Supply the configured session TTL and HTTPS cookie policy in WebConfig.
`webterm` (`WebtermSettings`, set by `mrsm start api` unless `--no-webterm`) serves the web terminal:
the `/dash/webterm` page embeds xterm.js in the console chrome (vendored `xterm.js`, `addon-fit.js`,
`addon-unicode11.js`, `webterm.js`, `webterm.css`; the theme follows the console's; the mobile key row
has ESC/CTRL/SHIFT/TAB and the arrows, CTRL and SHIFT sticky), and the router also carries the
standalone `/webterm/{session_id}` page and the websocket `/websocket/{session_id}`. Both are
`meerschaum_server::webterm` (the PTY bridge over `portable-pty`, tmux sessions, terminado's wire
protocol), authenticated through this crate's session store (`WebConfig::sessions`). The standalone
routes do not depend on the `dash` route group, like v4's.

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
HTMX 2.0.11 (the npm `latest` channel; 4.x is on `next`), Plotly.js 4.1.2, MapLibre GL JS 6.14.0,
Cytoscape.js 3.34.4, xterm.js 6.0.0 (addon-fit 0.11.0, addon-unicode11 0.9.0). Each file is checked
against the npm tarball's `dist.integrity`. There are no runtime CDN fetches. Map specs referencing
external tiles remain plugin-owned. Figures from the current Plotly.py (7.1 bundles Plotly.js 4.1)
render as they are.

- **Plotly.js 4** (`console.js` `normalizeFigure`): string titles and `titlefont`/`titleside`/
  `titleposition` (dropped in 3.0) are rewritten to `title.text`/`title.font`/..., and the
  `scattermapbox`/`choroplethmapbox`/`densitymapbox` traces and `layout.mapbox` (dropped in 4.0) to the
  `*map` traces and `layout.map`, so figures written for Plotly.js 2 keep drawing. The "Share chart"
  button (4.0 turns `showSendToCloud` on and posts the figure to a Plotly cloud service) is off
  unless a figure's own `config` turns it on.
- **MapLibre GL JS 6** ships only as an ES module (`maplibre-gl.mjs`) plus a sibling module worker
  (`maplibre-gl-worker.mjs`, found through `import.meta.url`), so `console.js` `import()`s it the
  first time a `data-mrsm-map` element appears; a page without a map never fetches it. It needs
  WebGL2.

### Embedding and serving

`build.rs` gzips every file of `assets/` (licenses and `versions.json` excluded) at the best level
into `OUT_DIR` and writes the table `src/assets.rs` includes (name, MIME type, a SHA-256 prefix of the
uncompressed bytes, the compressed bytes). The sources stay uncompressed in git; only the compressed
copies are in the binary (7.5 MB of JavaScript and CSS become about 2.2 MB). Gzip, not Brotli, because
browsers only offer `br` over HTTPS and the console is often served over plain HTTP (Brotli would also
need a second copy: about +1.7 MB).

`GET /dash/assets/<name>` sends the embedded gzip bytes with `Content-Encoding: gzip` when
`Accept-Encoding` allows it (q-values, `x-gzip` and `*` honored) and inflates them for any other
client. Every response has `Vary: Accept-Encoding`, an exact `Content-Length`, `Cache-Control:
no-cache` and a strong `ETag` per representation (`"<hash>"`, `"<hash>-gzip"`); `If-None-Match`
(lists, weak tags, `*`) gets a 304, so a browser revalidates cheaply and a new build is never hidden
by a stale cache. To update an asset, replace the file, update `versions.json`, and rebuild.

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
User and token forms delegate writes to the API. Settings exposes password changes. Page-contract callbacks are ordinary plugin
HTTP routes; manifest auto-discovery and dynamic native plugin handlers need the plugin/host
integration. Pixel-level equivalence to v4 Dash is not claimed. Source-sync execution depends
on the configured API/worker backend.
