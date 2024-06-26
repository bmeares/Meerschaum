<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# 👷 Background Jobs

Some actions need to run continuously, such as running the API or syncing pipes in a loop. Rather than relying on `systemd` or `cron`, you can use the built-in jobs system.

<asciinema-player src="/assets/casts/jobs.cast" autoplay="true" loop="true" size="small" preload="true"></asciinema-player>

## 👔 Jobs

All Meerschaum actions may be executed as background jobs by adding `-d` or `--daemon` flags or by prefacing the command with `start job`. New jobs will be given random names, and you can choose to specify a label with `--name`.

```bash
mrsm sync pipes -c plugin:foo --loop -d
```

### Starting Jobs

Start a previous job by typing its name after `start job[s]`:

```bash
mrsm start job awake_sushi -y
```

### Stopping Jobs

Stop a running job with `stop job[s]`:

```bash
mrsm stop job awake_sushi -y
```

You can stop and remove a job with `delete job[s]`:

```bash
mrsm delete job awake_sushi -y
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

!!! tip "Tip: Add `tomorrow` to keep a job from immediately firing."
    If you are creating long-running jobs that run at night, add `tomorrow` to your start time so that the job does not immediately fire (e.g. `daily starting tomorrow 10:00`).

 Schedule | Description 
----------|-------------
 `hourly starting 00:30` | Fire every hour on the 30th minute.
 `daily starting tomorrow 00:30` | Beginning at 30 minutes past midnight UTC, fire daily.
 `weekly starting Monday at 12:15 PM` | Fire every week on Monday at 12:15 PM.
 `monthly starting 2nd` | Fire once at 00:00 UTC on the second day of the month (will fire immediately if the current day is greater than 2).
 `every 10 seconds starting 2024-01-01` | Relative to the first second of 2024 (UTC), fire every 10 seconds.
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

## 🪵 Logs

Monitor the status of jobs with `show logs`, which will follow the logs of running jobs.

```bash
mrsm show logs
```

You can attach to specific jobs by listing their names:

```bash
mrsm show logs awake_sushi my_job
```

You can get a plain printout by adding `--nopretty`:

```bash
mrsm show logs --nopretty
```

