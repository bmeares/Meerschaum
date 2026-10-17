# meerschaum-server

> **Internal crate.** `meerschaum-server` is an implementation detail of [Meerschaum](https://meerschaum.io)
> and may change in any minor release. Depend on the [`meerschaum`](https://crates.io/crates/meerschaum)
> crate instead.

The `axum` HTTP API server: every v4 route plus `/engine/*`, OAuth2/JWT, tokens, rate limits, websockets, MCP and the extension-host proxy.

See the [design document](https://github.com/bmeares/Meerschaum/blob/v5/docs/design/v5.0.0.md) for the crate's place in the workspace.
