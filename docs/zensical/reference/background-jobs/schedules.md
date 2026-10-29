# Schedules

Use `--schedule` (`-s`) to repeat an action. Add `-d` to keep it running in the background:

```bash
mrsm sync pipes -s 'every 30 minutes' --name half-hourly -d
mrsm sync pipelines reporting -s 'daily starting tomorrow 00:30' -d
```

Times use **UTC** unless the schedule specifies a timezone. With no `starting` clause, the interval begins now. A starting time in the past also fires immediately.

## Intervals and start times

Use `every N seconds`, `minutes`, `hours`, `days`, `weeks`, `months`, or `years`. The aliases `hourly`, `daily`, `weekly`, and so on mean every one unit.

| Schedule | When it runs |
|---|---|
| `every 10 minutes starting in 30 seconds` | Wait 30 seconds, then every 10 minutes. |
| `hourly starting 00:30` | At 30 minutes past every hour. |
| `daily starting tomorrow 00:30` | At 00:30 UTC, starting tomorrow. |
| `weekly starting Monday at 12:15 PM` | Mondays at 12:15 PM. |
| `monthly starting 2nd` | At midnight on the second day of the month. |

## Preview before starting

```bash
mrsm show schedule 'daily and mon-fri starting tomorrow 00:30' 10
```

This prints the next ten fire times without starting a job.

## Cron and combinations

Five-field cron schedules use `minute hour day month weekday`:

```bash
# 30 minutes past every hour, weekdays in May through August.
mrsm sync pipes -s '30 * * may-aug mon-fri' -d

# Every six hours on weekdays in summer.
mrsm sync pipes -s 'every 6 hours and mon-fri and jun-aug' -d
```

Cron fields accept `*`, lists, ranges, steps, and month or weekday names. Numeric weekdays use `0` or `7` for Sunday and `1` for Monday.

Combine schedules with `and` (`&`) for matching times or `or` (`|`) for either schedule. A schedule may use one kind of combination, not both.

??? example "Jitter and the Python API"

    Add `jitter N seconds` to spread scheduled work over time:

    ```bash
    mrsm sync pipes -s 'hourly jitter 30 seconds' -d
    ```

    Parse a schedule in Python to inspect its next times:

    ```python
    from meerschaum.utils.schedule import parse_schedule

    trigger = parse_schedule('daily starting 2027-01-01')
    print(trigger.next())
    print(trigger.next())
    ```
