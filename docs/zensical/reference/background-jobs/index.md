# Background Jobs

Add `-d` to run an action in the background. Give it a name so you can find its logs later:

```bash
mrsm sync pipes --loop --name syncing-engine -d
mrsm show jobs
mrsm show logs syncing-engine
```

Jobs keep running after your terminal closes. Their [executor](executors.md) determines where they run and whether they return after a reboot.

??? example "Watch starting and managing a job"
    <asciinema-player src="/assets/casts/v5-jobs.cast" preload="true" rows="20" idle-time-limit="2" poster="npt:0:05"></asciinema-player>

## Manage a job

| Command | Effect |
|---|---|
| `mrsm start jobs syncing-engine` | Start it. |
| `mrsm pause jobs syncing-engine` | Suspend it. |
| `mrsm stop jobs syncing-engine` | Stop it. |
| `mrsm delete jobs syncing-engine` | Stop it and remove its definition. |
| `mrsm attach job syncing-engine` | Follow output and answer prompts. |
| `mrsm show logs syncing-engine` | Follow output without sending input. |

The console's [Jobs page](../api-instance/web-console.md#jobs) offers the same controls. It also shows when the job last ran and its [health](health.md).

## Run several actions in one job

Use `+` between actions and `: --loop --independent-laps` after the last one. Each step has its own interval and failure cooldown. Due steps run one at a time in the job's host process and can reuse its Python worker.

```bash
mrsm sync pipes -t readings --min-seconds 30 + sync pipes -t summary --min-seconds 300 : --loop --independent-laps -d --name readings-and-summary
```

Use actions that finish one lap. A long-running step delays the next due step. The group has one owner, environment, log, and stop control.

## Answer a job's prompts

A job that asks a question waits until someone answers. `mrsm show jobs` says **waiting for input**, and `mrsm attach job <name>` shows the question and sends the line you type; a remote client gets the same through the job log websocket. Nobody needs to be attached when the job asks: it waits.

On Linux this works for anything in the job that reads its input: Meerschaum's own prompts, a plugin's `input()` or `prompt()`, a shell `read`, a program the job started. On macOS only Meerschaum's own prompts (the Rust prompts and `meerschaum.utils.prompt.prompt()`) are seen as waiting; anything else still gets the line you send with `attach`, which waits for the job to ask first. A job that asks before it prints anything is answered too.

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
