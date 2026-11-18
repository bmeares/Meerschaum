# Executors

An executor runs a job. Select one with `-e` (`--executor-keys`):

| Executor | Runs on | Returns after reboot? |
|---|---|---|
| `local` | This host, as a managed daemon. | Needs a supervisor to restart it. |
| `systemd` | This host, as a user service. | Yes, once the user session starts. |
| `api:label` | Another Meerschaum API server. | Depends on that server's executor. |

An installed Meerschaum uses `systemd` when a user systemd session is available, including in Compose roots. Source checkouts, development builds, and hosts without systemd use `local`. Set `meerschaum:executor` in `edit config` to change the default.

For API-managed work, an administrator can also select a team's [execution environment](../team-executors.md):
the main host, a generated Stack container, or an independently managed service. This placement
applies after the main API authorizes the caller. Selecting `-e api:label` sends a request to that
API; it does not override the team's placement or permissions there.

## Run a job remotely

```bash
mrsm sync pipes --loop --name syncing-engine -e api:prod -d
mrsm show logs syncing-engine -e api:prod
```

**`-e` chooses where code runs; `-i` chooses where Pipe data is stored.**

```bash
# Execute on the remote host, using its sql:local instance.
mrsm sync pipes -i sql:local -e api:prod

# Execute here, storing data through the remote API.
mrsm sync pipes -i api:prod
```

## Local daemons

Job metadata lives in `$MRSM_ROOT_DIR/jobs/<name>/`; logs live in `$MRSM_ROOT_DIR/logs/`. When the process that starts a job holds `MRSM_CONFIG` or `MRSM_PATCH` (a Compose project's `config:` block, for example), the job receives them as owner-only files in its directory (`.mrsm_config.json`, `.mrsm_patch.json`, written at every start and removed with the job) named by `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE`, so its environment holds no credentials. A job started before 5.1 keeps its environment until it is started again. `start`, `stop`, `pause`, and `delete jobs` manage the daemon. A local job does not restart itself after a host reboot. While an API server runs on the root (`mrsm start api`, or a Docker container), it restarts stopped `--restart` jobs every second, so the jobs inside a container come back with it; jobs stopped by hand stay stopped. Otherwise use systemd or another supervisor.

## Systemd services

Jobs are enabled as user services. Logs are available through both `mrsm show logs` and `journalctl --user`.

Scoped jobs in 5.1 require the trusted standalone native `mrsm` launcher. Their units require
systemd 235 or newer and use `UnsetEnvironment=` to remove inherited loader and interpreter
settings before startup. After upgrading, recreate existing scoped units through the job's
original CLI or Compose definition; restarting an old unit alone does not add these rules.
The [migration guide](../team-executors.md#migrate-an-existing-deployment) covers team runtime changes.

??? info "Unit names, environment, and login sessions"

    The default root uses `mrsm-<name>.service`. Other roots use `mrsm-<project>.<name>.service`, with the Compose project name or a short root-path hash. Commands list only the current root's jobs.

    Units save the root, plugins directory, and project environment. Their files use mode `0600`. Since 5.1 a unit carries no `MRSM_CONFIG` / `MRSM_PATCH`: their text is written to owner-only files next to the unit (`$MRSM_ROOT_DIR/systemd/services/<name>/.mrsm_config.json` and `.mrsm_patch.json`) and the unit sets `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE`. Units written by earlier versions keep their `Environment=MRSM_CONFIG=...` line until the job is created again (`compose up` recreates its jobs). To keep user services running while logged out, enable lingering:

    ```bash
    loginctl enable-linger "$USER"
    ```

    Older installations which used local Compose jobs move them to systemd on the next `compose up` or `restart jobs`. Set `meerschaum:executor: local` to keep local daemons.

??? info "Custom executors"

    Plugins may implement an executor type. See the [Python Executor API](https://docs.meerschaum.io/meerschaum/jobs.html#Executor) or [Rust executors and streams](../plugins/rust-plugins/executors-and-streams.md).
