# meerschaum-jobs

> **Internal crate.** `meerschaum-jobs` is an implementation detail of [Meerschaum](https://meerschaum.io)
> and may change in any minor release. Depend on the [`meerschaum`](https://crates.io/crates/meerschaum)
> crate instead.

Job supervision with the Meerschaum 4 on-disk layout: daemons, schedules, laps, job health (lap heartbeats in `jobs/<name>/laps.json`), job ownership (the `owner` and `team` properties a job started through the API server or the console records, which a job started by another job inherits; `ownership` renames and moves a team's jobs) and the local, systemd (units named per root) and remote executors.

A job deleted before its detached supervisor started is never brought back: the launcher records `process.launch_id` and the supervisor, which never creates the job's directory, exits when the directory is gone or a newer launch replaced its own.

An isolated team's job is launched with the team's configuration, an allowlisted environment and its sandbox spec (`config_files::externalize_team_with`); a job marked as team execution requires the sandbox. The `executor` module is the job-executor interface (`local`, `systemd`, `api:<label>`, plugin types); team executors are a different thing, mapped in `docs/agents/teams-and-executors.md`.

See the [design document](https://github.com/bmeares/Meerschaum/blob/main/docs/design/v5.0.0.md) for the crate's place in the workspace.
