# meerschaum-web

> **Internal crate.** `meerschaum-web` is an implementation detail of [Meerschaum](https://meerschaum.io)
> and may change in any minor release. Depend on the [`meerschaum`](https://crates.io/crates/meerschaum)
> crate instead.

What the Rust server keeps of the web console (design decision D21). The console itself is the
Plotly Dash app of the Python package (`pip install meerschaum[dash]`), mounted by the API host at
the v4 paths (`/dash`, `/dash/pipes`, ...). The server proxies `/dash` and `/dash/*` to the host when
the host reports that it mounted the console (`GET /_mrsm/host/status`), and shows a static pointer
page ("The web console comes with the Python package: `pip install meerschaum[dash]`") otherwise:
that logic is `meerschaum_server::web::console_entry`, so the standalone binary (`cargo install
meerschaum`, the `minimal` Docker image) serves it too. This crate keeps three things.

## The web terminal

`WebtermSettings` (set by `mrsm start api` unless `--no-webterm`) serves `/webterm/{session_id}`, the
page v4's `termpage.html` was, which the Dash console embeds in an iframe (`?theme=light` selects a
light terminal; the default is dark as in v4), its websocket `/websocket/{session_id}` and the
vendored xterm.js bundle at `/webterm/assets/{name}` (`xterm.js`, `xterm.css`, `addon-fit.js`,
`addon-unicode11.js`, `webterm.js`, `webterm.css`; `/dash/assets/` belongs to Dash). The PTY bridge
is `meerschaum_server::webterm` (`portable-pty`, tmux sessions, terminado's wire protocol). Both
routes authenticate through the console session store: the `mrsm-session-id` cookie (a UUID) names a
row (`mrsm_session_id:<id>` in Valkey, else the `mrsm_sessions` pipe of the server's instance) whose
`username` is the user. `webterm.js` also takes `__SEND_KEY` / `__TOGGLE_MOD` messages from the
parent page for the touch key row.

## The job-log stream

`GET /logs/{name}/stream` follows a job's log over Server-Sent Events. It authenticates by bearer
token (`jobs:read`) or, for the browser's `EventSource`, by the session cookie (the user's own scopes
apply, a session of a user that no longer exists is refused with a 401, never a redirect). One `data:`
event per log chunk, `event: stdin` when the job waits for input, `event: stopped` with the final
message, then the stream ends. Jobs come from `POST /execute` (`meerschaum-server`): the Dash
dashboard's Execute console.

## Native plugin pages

A native (Rust source) plugin's `web_pages` are served as HTML fragments for the Dash console:
`GET /_mrsm/native-pages` (JSON list) and `GET /_mrsm/native-pages/{path}?query` (the fragment, with
`X-Mrsm-User`), both behind the host's internal token (`X-Mrsm-Host-Token`, signed with the API
secret, the same token the server uses for the host's registry). Paths are exact; wildcards, `..`
and duplicates are refused when the router is built.

## Sessions

`InstanceSessions` and `ValkeySessions` implement the protocol `meerschaum/api/dash/sessions.py`
writes (rows `session_id` primary key, `data` compact JSON, `expires` Unix seconds in the temporary
pipe `mrsm_sessions`; or `mrsm_session_id:<uuid>` with `EX` in Valkey). The hosted console always uses `InstanceSessions`, even when a Valkey cache is configured.
Rust no longer creates or
deletes console sessions: login, logout, registration and password reset are Dash's. The server
guards `mrsm_sessions` against pipe aliases like its other internal tables.

## Embedding the assets

`build.rs` gzips every file of `assets/` (licenses and `versions.json` excluded) at the best level
into `OUT_DIR` and writes the table `src/assets.rs` includes (name, MIME type, a SHA-256 prefix of the
uncompressed bytes, the compressed bytes). The sources stay uncompressed in git. A response carries
the embedded gzip bytes when `Accept-Encoding` allows (q-values, `x-gzip` and `*` honored) and the
inflated file otherwise, with `Vary: Accept-Encoding`, an exact `Content-Length`, `Cache-Control:
no-cache` and a strong `ETag` per representation; `If-None-Match` gets a 304. To update an asset,
replace the file, update `versions.json` (xterm.js 6.0.0, addon-fit 0.11.0, addon-unicode11 0.9.0,
each checked against the npm tarball's `dist.integrity`) and rebuild. There are no CDN fetches.

## Verification

```sh
export PATH="$HOME/.cargo/bin:$PATH"
cargo fmt -p meerschaum-web
cargo clippy -p meerschaum-web --all-targets -- -D warnings
cargo test -p meerschaum-web
cargo test -p meerschaum-server --test console
```

Tests drive the composed router in process: the shared session store (an in-memory instance, a
SQLite file shared with a Python stub, the Valkey key protocol), the terminal page and asset
negotiation, the SSE stream (bearer, cookie, scopes, route-group gating) and the native-page
fragments (token, login, path validation). The browser tests of the Dash console are
`tests/console` (`MRSM_TEST_CONSOLE=1`).
