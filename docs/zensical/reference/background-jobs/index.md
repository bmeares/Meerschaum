# Background Jobs

Add `-d` to run an action in the background. Give it a name so you can find its logs later:

```bash
mrsm sync pipes --loop --name syncing-engine -d
mrsm show jobs
mrsm show logs syncing-engine
```

Jobs keep running after your terminal closes. Their [executor](executors.md) determines where they run and whether they return after a reboot.

## Manage a job

| Command | Effect |
|---|---|
| `mrsm start jobs syncing-engine` | Start it. |
| `mrsm pause jobs syncing-engine` | Suspend it. |
| `mrsm stop jobs syncing-engine` | Stop it. |
| `mrsm delete jobs syncing-engine` | Stop it and remove its definition. |
| `mrsm attach job syncing-engine` | Follow output and answer prompts. |
| `mrsm show logs syncing-engine` | Follow output without sending input. |

The console's [Jobs page](../api-instance/web-console.md#jobs) offers the same controls. It also shows the job's last successful sync and [health](health.md).

## Choose what happens next

| Need | Guide |
|---|---|
| Run every hour or on a calendar | [Schedules](schedules.md) |
| Run locally, with systemd, or on another server | [Executors](executors.md) |
| Understand who can see or control a job | [Access](access.md) |
| Find temporary console executions | [Automatic Runs](runs.md) |
| Diagnose a repeating job | [Health](health.md) |
| Start many workers without a resource spike | [Startup stagger](startup.md) |

??? example "Manage jobs from Python"

    ```python
    import meerschaum as mrsm

    job = mrsm.Job('syncing-engine', 'sync pipes --loop')
    success, message = job.start()
    print(job.health)
    job.stop()
    ```

    See the full [Job API](https://docs.meerschaum.io/meerschaum.html#Job).
