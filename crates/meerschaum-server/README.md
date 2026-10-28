# meerschaum-server

> **Internal crate.** `meerschaum-server` is an implementation detail of [Meerschaum](https://meerschaum.io)
> and may change in any minor release. Depend on the [`meerschaum`](https://crates.io/crates/meerschaum)
> crate instead.

The `axum` HTTP API server: every v4 route plus `/engine/*`, OAuth2/JWT, tokens, rate limits, websockets, MCP and the extension-host proxy.

API-token revocation and expiration apply to both direct API keys and JWTs issued through
client-credentials login. Invalid or expired tokens cannot obtain new JWTs. Deleting a user
also ends access through their existing JWTs; authentication checks that the account still exists.

`/logs/{name}/ws` accepts a raw JWT or `mrsm-key:` API key as its first message. Reading logs
requires `jobs:read` and `logs:read`, as on `GET /logs/{name}`. Answering a job's stdin prompt
also requires `actions:execute`, `jobs:execute`, and permission to execute actions on the server.
An `execute-*` job's logs remain visible only to its creator and administrators.

`GET /engine/runs/{run_id}/events/stream` (`engine_stream`) serves a run's typed engine events as
Server-Sent Events (`event:` = kind, `id:` = `seq`), resumable with `Last-Event-ID`, until
`run_finished` or a synthetic `run_closed` (`reason`: `interrupted`, `holder_gone`, `idle`;
`?idle_timeout=`) for a run that will never finish. Events published to `AppState::engine_streams`
(an `api:` engine's `/engine/pipes/engine_events/write` batches, and through
`EngineStreams::publish_json` the engine runs of the server process, which `mrsm start api` forwards
with `meerschaum_engine::add_process_event_sink`) arrive live; otherwise the stream tails the
persisted rows of the run's instance. It requires `pipes:read`; `meerschaum-web` adds
console-session (cookie) authentication through `EngineStreams::set_browser_auth`. A run whose
history cannot be read is an error, never "not found".

The v5-only routes (`/pipelines`, `/pipelines/*`, `/engine/*`, `/execute*`) answer
`{"success": bool, "message": str, ...}` with real status codes (`envelope`): handlers build their
bodies with `envelope::ok`, and the `envelope::v5_errors` middleware rewrites every error of those
paths (a handler's `{"detail": ...}`, validation errors as `400`, the router's `404` / `405`, the
rate limiter's `429`) into that shape; a `500` names what failed only to administrators. The saved
Pipelines' routes (`routes::pipelines`) include one Pipeline (`GET /pipelines/{name}`), its plan
(`POST .../plan`, through `backends::PipelinesBackend`, which the CLI implements with the engine)
and a background run (`POST .../sync`, a temporary execution job). `GET /engine/runs` takes
`pipeline`, `pipe`, `status`, `since` and `limit` (`backends::RunsFilter`, pushed into the store's
reads), and `GET /engine/runs/{run_id}` returns one run with its nodes.

Authorization fails closed: a scope read that fails (`get_user_attributes`, `get_token_scopes`) is
a `503`, never "every scope" (the instance connectors return the read's error: SQL, Valkey and
`api:` alike). `api:permissions:rate_limits` takes `{requests, seconds}` and
`{max, window_seconds}`, route-group globs and path prefixes (`/my-plugin`), and reports unusable
entries at startup (`config::parse_rate_limits`). `api:permissions:preset: public` (`--public`)
serves only `/login`, `/healthcheck`, `/version`, plugin routes and plugin pages, plus the groups the
allowlist names explicitly.

Every decision about a request that may reach the API host (its route group, its path-prefix rate
limit, `routes::is_proxyable`, the console paths, the v5 error shape) is made on
`routes::decoded_path`, the path percent-decoded once as uvicorn decodes it: the server's own
routes match the path as sent, but the host serves `/%64ocs` as `/docs`.

HTTPS is native (`tls`): with a key and a certificate (`start api --keyfile/--certfile`,
`api:server:ssl_keyfile` / `ssl_certfile`, 4.1's `api:uvicorn:ssl_*`) the listener terminates TLS
with rustls (aws-lc-rs) and serves HTTP/2 or HTTP/1.1 by ALPN, with upgrades (websockets use
HTTP/1.1: extended CONNECT is not advertised). `tls::prepare` checks the PEM files (PKCS#8, PKCS#1
and SEC1 keys, full chains; precise messages for half a pair, unreadable, swapped, encrypted, DER or
mismatched files) and binds the internal listener: plain HTTP on `127.0.0.1` with a random port,
which `ServerConfig::internal_addr` hands to the Python API host as its `server_url`. The certificate
(`tls::ServerCertificate`, a rustls resolver) is reloaded when the files' content changes (polled
every 30 s) and on `SIGHUP`; a reload that fails keeps the previous certificate. Plain HTTP reaching
the HTTPS port gets `/healthcheck` and a `308` to `https://`. HTTP/2 requests get a `Host` header from
their authority, and every request the peer address (`ConnectInfo`), as on the plain listener.

The web terminal's tmux sessions run on a dedicated tmux server (`webterm::tmux_socket_path`, a
`0700` directory under the root) started with Meerschaum's `tmux.conf` (`webterm::TMUX_CONF`) or
`api:webterm:tmux:config`; `webterm::stop_owned_servers` stops the servers this process started.

The MCP registry is v4's (`mcp/registry.json`, generated) with v5's additions (`mcp::v5`: the
Pipelines, runs, jobs and connectors tools, `sync_pipe`'s `parents` / `children`, bounds typed as
strings or integers, the primer); `docs/zensical/reference/mcp/tools.md` is rendered from it
(`mcp::docs::render_tool_reference`, checked by `tests/mcp_docs.rs`).

Native (Rust) plugins' routes are mounted by `native::native_router` (credentials, declared
scopes, Starlette-style path parameters with converters, shadowing refusal including the
console's `/_pages`) and their MCP entries are served through
`mcp::NativeMcp`; the CLI implements both handler traits over its loaded plugins.

See the [design document](https://github.com/bmeares/Meerschaum/blob/v5/docs/design/v5.0.0.md) for the crate's place in the workspace.
