# meerschaum-connector-api

> **Internal crate.** `meerschaum-connector-api` is an implementation detail of [Meerschaum](https://meerschaum.io)
> and may change in any minor release. Depend on the [`meerschaum`](https://crates.io/crates/meerschaum)
> crate instead.

The `APIConnector` HTTP client and the shared route, auth and wire models of the Meerschaum API.

`APIConnector::follow_engine_run(run_id, after_seq, instance_keys)` follows a run's events over
`GET /engine/runs/{run_id}/events/stream` as an async stream of `models::EngineRunEvent` (the
shape of `meerschaum_engine::Event`), reconnecting with `Last-Event-ID` (and retrying a failed
reconnect with backoff) until `run_finished` or the server's `run_closed`
(`EngineRunEvent::is_terminal`). `follow_engine_run_with(run_id, client::FollowRunOptions)` also
sets `wait` and `idle_timeout`.

See the [design document](https://github.com/bmeares/Meerschaum/blob/v5/docs/design/v5.0.0.md) for the crate's place in the workspace.
