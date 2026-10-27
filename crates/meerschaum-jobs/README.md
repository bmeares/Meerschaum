# meerschaum-jobs

> **Internal crate.** `meerschaum-jobs` is an implementation detail of [Meerschaum](https://meerschaum.io)
> and may change in any minor release. Depend on the [`meerschaum`](https://crates.io/crates/meerschaum)
> crate instead.

Job supervision with the Meerschaum 4 on-disk layout: daemons, schedules, laps, job health (lap heartbeats in `jobs/<name>/laps.json`) and the local, systemd (units named per root) and remote executors.

See the [design document](https://github.com/bmeares/Meerschaum/blob/v5/docs/design/v5.0.0.md) for the crate's place in the workspace.
