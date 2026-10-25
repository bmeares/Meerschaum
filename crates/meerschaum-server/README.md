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
persisted rows of the run's instance. It requires `pipes:write`; `meerschaum-web` adds
console-session (cookie) authentication through `EngineStreams::set_browser_auth`.

Native (Rust) plugins' routes are mounted by `native::native_router` (credentials, declared
scopes, Starlette-style path parameters with converters, shadowing refusal including the
console's `/_pages`) and their MCP entries are served through
`mcp::NativeMcp`; the CLI implements both handler traits over its loaded plugins.

See the [design document](https://github.com/bmeares/Meerschaum/blob/v5/docs/design/v5.0.0.md) for the crate's place in the workspace.
