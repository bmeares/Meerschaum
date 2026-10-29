# Executors

An executor runs a job. Select one with `-e` (`--executor-keys`):

| Executor | Runs on | Returns after reboot? |
|---|---|---|
| `local` | This host, as a managed daemon. | Needs a supervisor to restart it. |
| `systemd` | This host, as a user service. | Yes, once the user session starts. |
| `api:label` | Another Meerschaum API server. | Depends on that server's executor. |

An installed Meerschaum uses `systemd` when a user systemd session is available, including in Compose roots. Source checkouts, development builds, and hosts without systemd use `local`. Set `meerschaum:executor` in `edit config` to change the default.

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

Job metadata lives in `$MRSM_ROOT_DIR/jobs/<name>/`; logs live in `$MRSM_ROOT_DIR/logs/`. `start`, `stop`, `pause`, and `delete jobs` manage the daemon. A local job does not restart itself after a host reboot; use systemd or another supervisor for that.

## Systemd services

Jobs are enabled as user services. Logs are available through both `mrsm show logs` and `journalctl --user`.

??? info "Unit names, environment, and login sessions"

    The default root uses `mrsm-<name>.service`. Other roots use `mrsm-<project>.<name>.service`, with the Compose project name or a short root-path hash. Commands list only the current root's jobs.

    Units save the root, plugins directory, and project environment. Their files use mode `0600` because configuration may contain credentials. To keep user services running while logged out, enable lingering:

    ```bash
    loginctl enable-linger "$USER"
    ```

    Older installations which used local Compose jobs move them to systemd on the next `compose up` or `restart jobs`. Set `meerschaum:executor: local` to keep local daemons.

??? info "Custom executors"

    Plugins may implement an executor type. See the [Python Executor API](https://docs.meerschaum.io/meerschaum/jobs.html#Executor) or [Rust executors and streams](../plugins/rust-plugins/executors-and-streams.md).
