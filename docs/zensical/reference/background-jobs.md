<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# 👷 Background Jobs

Meerschaum's job management system lets you run any process in the background ― just add `-d` to any command. Thanks to the built-in [scheduler](#schedules), you no longer have to worry about manually configuring `crontab` or `systemd`.

!!! tip ""
    Depending on the [executor](#executors), jobs are created as either `systemd` services or managed daemon processes.

<asciinema-player src="/assets/casts/jobs.cast" autoplay="true" loop="true" size="small" preload="true"></asciinema-player>

## 👔 Jobs

Any Meerschaum action may be executed as a background job by adding the `-d` (`--daemon`) flag or by prefacing the command with `start job`.

```bash
mrsm sync pipes --loop -d
```

New jobs will be given random names, and you can specify a label with `--name`.

```bash
mrsm sync pipes --loop --name syncing-engine -d
```

!!! tip "Chaining commands"
    Combine multiple commands with `+`, similar to `&&` in `bash`:

    ```bash
    sync pipes -i sql:raw + \
    sync pipes -c sql:raw -i sql:etl + \
    sync pipes -c sql:etl -i sql:dest
    ```

### Job Management

| Command       | Description                                                                          | Flags              |
|---------------|--------------------------------------------------------------------------------------|--------------------|
| `show jobs`   | Print the existing jobs.                                                             | `--nopretty`, `-e` |
| `start jobs`  | Start the service(s).                                                                | `-e`               |
| `stop jobs`   | Stop the service(s).                                                                 | `-y`, `-e`         |
| `pause jobs`  | Suspend the service(s).                                                              | `-e`               |
| `delete jobs` | Stop and remove the service(s).                                                      | `-y`               |
| `attach job`  | Stream the job's output, and write user input to the process `STDIN` (if necessary). | `-e`               |
| `show logs`   | Stream jobs' output (similar to `docker compose logs -f`).                           | `-e`               |
| `attach logs` | Alias for `show logs`.                                                               | `-e`               |

### Python API

Jobs may be managed with the [Job](https://docs.meerschaum.io/meerschaum.html#Job) class.

```python
import meerschaum as mrsm
job = mrsm.Job('syncing-engine', 'sync pipes --loop')
success, msg = job.start()
```

??? "More examples"
    ```python
    import meerschaum as mrsm

    job = mrsm.Job('syncing-engine', 'sync pipes --loop')
    job.start()

    print(job.pid)
    # 12155

    print(job.began)
    # datetime.datetime(2024, 8, 10, 3, 38, 40, 573621)

    job.pause()

    print(job.paused)
    # datetime.datetime(2024, 8, 10, 3, 38, 42, 998261)

    job.stop()

    print(job.ended)
    # datetime.datetime(2024, 8, 10, 3, 38, 44, 618737)

    ### Stream the job's output.
    job.monitor_logs()

    job.delete()
    ```

## ⏲️ Schedules

!!! warning ""
    As of Meerschaum v2.2.0, scheduling is handled by the library [APScheduler](https://apscheduler.readthedocs.io) rather than [Rocketry](https://rocketry.readthedocs.io/en/stable/condition_syntax/index.html).

You can run any command regularly with the flag `-s` or `--schedule` ― for example, `-s hourly` will execute the command once per hour. Below are the supported intervals:

```
every N [seconds | minutes | hours | days | weeks | months | years]
```

As shorthand, the following `[unit]ly` aliases correspond to `every 1 [unit]`:

  - `secondly`
  - `minutely`
  - `hourly`
  - `daily`
  - `weekly`
  - `monthly`
  - `yearly`

Add the `--schedule` flag to any Meerschaum command, and it will run forever according to your schedule. This pairs really well with `--daemon`:

```bash
mrsm sync pipes -s 'every 30 minutes' -d
mrsm sync pipes -s 'hourly' -d
mrsm sync pipes -s 'daily starting 00:00' -d
```


### Start Time

Append the phrase `starting [time]` to a schedule to set the reference point. If the starting time is in the past, the schedule will also fire immediately.

!!! tip "Tip: Add `tomorrow` or `in 1 minute` to keep a job from immediately firing."
    If you are creating long-running jobs that run at night, add `tomorrow` or `in 1 minute` to your start time so that the job does not immediately fire (e.g. `daily starting tomorrow 10:00`, `daily starting in 1 minute`).

 Schedule | Description 
----------|-------------
 `every 10 minutes starting in 30 seconds` | Beginning 30 seconds from now, fire every 10 seconds.
 `hourly starting 00:30` | Fire every hour on the 30th minute.
 `daily starting tomorrow 00:30` | Beginning at 30 minutes past midnight UTC, fire daily.
 `weekly starting Monday at 12:15 PM` | Fire every week on Monday at 12:15 PM.
 `monthly starting 2nd` | Fire once at 00:00 UTC on the second day of the month (will fire immediately if the current day is greater than 2).
 `yearly starting 2025-07-01 12:00` | Beginning in 2025, fire at noon UTC every July 1st.

!!! note ""
    Omitting the starting time will use the current time as the starting point. Unless specified, the default timezone is UTC. See [Verifying Schedules](#verifying-schedules) below for ways you can experiment with different schedule strings.

### Cron Format

For more fine-grained control, you may specify your schedule in a [`cron`](https://en.wikipedia.org/wiki/Cron) format:

```
[minute] [hour] [day] [month] [week]
```

For example, the schedule `30 * * may-aug mon-fri` runs once per hour on the 30th minute, but only on weekdays in the months May through August. See [APScheduler](https://apscheduler.readthedocs.io/en/master/api.html#apscheduler.triggers.cron.CronTrigger) for additional documentation.

You may find it more readable to achieve similar results by combining fragments of `cron` schedules with interval schedules (e.g. `daily and mon-fri`). Read below to see what's possible:

### Schedule Combinations

You may combine schedules with `&` (alias `and`) or `|` (alias `or`) logic. For example, the following example fires a job every 6 hours but only on weekdays in the summer months of 2024:

!!! warning inline end "Joining multiple schedules"
    For the time being, `&` and `|` may not both be used within the same schedule. You may, however, join more than two schedules with the same logic (e.g. `daily and mon-fri and 2024`).

```
every 6 hours & mon-fri & jun-aug & 2024 starting 2024-06-03

# Equivalent cron schedule:
0 0,6,12,18 * jun-aug mon-fri
```

The `cron` version of the schedule is confusing, isn't it? Combining `cron` fragments with `and` produces a much more readable result.

If you combine overlapping schedules with `&`, only mutual timestamps are used:

```
# Equivalent to `weekly`:
daily and weekly
```

Combining with `|` will fire on the next earliest timestamp of any schedule:

```
# Fire at midnight and 2 PM every day (starts immediately):
daily or 0 14 * * * starting 00:00

# Equivalent cron-only schedule (starts tomorrow):
0 0,14 * * *
```

### Aliases

For your convenience, common aliases are mapped to keywords:

 Keyword | Aliases 
---------|---------
 `&` | `and` 
 `|` | `or` 
 `-` | ` through `, ` thru `, ` - ` (with spaces)
 `starting` | `beginning`
 Weekdays (`mon`, `tue`, etc.) | Full names (e.g. `Monday`) and `tues`, `thurs` 
 Months (`jan`, etc.) | Full names (e.g. `January`) 

### Verifying Schedules

You may verify your schedules with the command `show schedule`:

!!! tip inline end ""
    Add an integer to print more than 5 timestamps, e.g.:

    ```bash
    show schedule 'daily' 10
    ```

```bash
mrsm show schedule 'daily and mon-fri starting May 2, 2024'
```

This command prints out a preview of the next fire times:

```
Next 5 timestamps for schedule 'daily and mon-fri starting May 2, 2024':

  2024-05-02 00:00:00+00:00
  2024-05-03 00:00:00+00:00
  2024-05-06 00:00:00+00:00
  2024-05-07 00:00:00+00:00
  2024-05-08 00:00:00+00:00
```

??? info "Schedules Python API"

    You may also parse your schedules with the function [`parse_schedule()`](https://docs.meerschaum.io/meerschaum/utils/schedule.html#parse_schedule), which returns an [APScheduler `Trigger`](https://apscheduler.readthedocs.io/en/master/api.html#triggers).

    ```python
    from meerschaum.utils.schedule import parse_schedule
    trigger = parse_schedule('daily starting 2024-01-01')
    trigger.next()
    # datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
    trigger.next()
    # datetime.datetime(2024, 1, 2, 0, 0, tzinfo=datetime.timezone.utc)
    ```

## ⚙️ Executors

Executors are to jobs as connectors are to pipes; all jobs run on an executor. There are three built-in kinds of executors:

- `local` (daemons)  
  Run jobs as managed Unix daemon processes.

- `systemd` (services)  
  Run jobs as `systemd` *user* services.

- `api:{label}` (remote)  
  Run jobs remotely on another Meerschaum API instance.

Add the flag `-e` (`--executor-keys`) to any command to choose the executor:

```bash
# Create the job 'syncing-engine' on 'api:prod':
sync pipes --name syncing-engine -e api:prod -d

# Run `show pipes` on 'api:prod':
show pipes -e api:prod
```

### Choosing an Executor

When `-e` is omitted, Meerschaum picks a default based on your environment:

- If `systemd` is available **and** you are using the default root directory (`~/.config/meerschaum`) **and** Meerschaum is installed (not run from source), the default is `systemd`.
- Otherwise, the default is `local`.

You can override the default for all commands by setting `meerschaum:executor` in your configuration (`edit config`), or per-command with `-e`.

!!! tip "Comparison"
    | Executor    | Where it runs                        | Survives reboot                          | Logs                                                  | Requires                              |
    |-------------|--------------------------------------|------------------------------------------|-------------------------------------------------------|---------------------------------------|
    | `local`     | Local managed daemon process         | No (unless restarted by a supervisor)    | `$MRSM_ROOT_DIR/logs/`                                | Nothing (always available)            |
    | `systemd`   | Local `systemd` user service         | Yes (when the service is enabled)        | `journalctl --user` + `$MRSM_ROOT_DIR/systemd/logs/`  | `systemd` user session                |
    | `api:{label}` | The remote API instance            | Depends on the remote executor           | Streamed back from the remote instance                | Reachable API connector (`api:label`) |

### `local` (Default Daemons)

A `local` job runs as a self-managed Unix daemon process. Its state — `pid`, logs, start/stop timestamps, and the captured `SuccessTuple` result — is written under `$MRSM_ROOT_DIR/jobs/<name>/`, and its rotating log files live under `$MRSM_ROOT_DIR/logs/`.

- **Lifecycle:** `start jobs` launches the daemon, `stop jobs` terminates it, `pause jobs` suspends it, and `delete jobs` stops it and removes its directory.
- **Restarts:** jobs created with `--restart` (or `--loop`) are restored by a periodic check. The API server, for example, creates all of its jobs as `local` and restarts any that were killed. A `local` job does **not** survive a host reboot on its own unless something (such as the API server or a `systemd` healthcheck) restarts it.

### `systemd` (User Services)

A `systemd` job is registered as a `systemd` *user* service named `mrsm-<name>.service`. The unit file is written to the user service directory and the service is enabled (`WantedBy=default.target`), so the job is restarted automatically on reboot once the user session starts.

- **Availability:** `systemd` is only used when a user `systemd` session is present (and is the default only under the conditions described in [Choosing an Executor](#choosing-an-executor)).
- **Persistence across reboots:** because the unit is enabled, the job comes back after a reboot. To keep user services running even while you are logged out, enable lingering for your user (`loginctl enable-linger $USER`).
- **Logs:** output is available through the journal (`journalctl --user -u mrsm-<name>.service`) and is also mirrored to `$MRSM_ROOT_DIR/systemd/logs/`, so `show logs` works the same way as for `local` jobs.

### `api:{label}` (Remote)

Specifying `-e api:{label}` "remotes into" another Meerschaum API instance: the job is posted to that server and runs *there*, on that server's own executor. Output is streamed back to your terminal. This lets you manage and observe jobs on a remote host as if they were local.

```bash
# Create the job 'syncing-engine' on 'api:prod':
sync pipes --loop --name syncing-engine -e api:prod -d

# Tail its logs from the remote instance:
show logs syncing-engine -e api:prod
```

### Instance vs Executor

Note the distinction between `-e` and `-i`:

- `-e` executes the code remotely, streaming back the output.  
- `-i` still executes locally but syncs data back to the API.

```
# Run `sync pipes` remotely:
sync pipes -e api:main

# Run `sync pipes` locally against the instance 'api:main':
sync pipes -i api:main

# Run `sync pipes` remotely, against the server's 'sql:local':
sync pipes -i sql:local -e api:main
```


!!! note "Custom executors"
    You can implement a custom executor with the [`#!python @make_executor` decorator](). See [`Executor`](https://docs.meerschaum.io/meerschaum/jobs.html#Executor) and [`SystemdExecutor`](https://docs.meerschaum.io/meerschaum/jobs/systemd.html) for an example implementation.