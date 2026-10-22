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

See the [design document](https://github.com/bmeares/Meerschaum/blob/v5/docs/design/v5.0.0.md) for the crate's place in the workspace.
