# Schedules

Use `--schedule` (`-s`) to repeat an action. Add `-d` to keep it running in the background:

```bash
mrsm sync pipes -s 'every 30 minutes' --name half-hourly -d
mrsm sync pipelines reporting -s 'daily starting tomorrow 00:30' -d
```

Times use **UTC** unless the schedule names a time zone (see [Time zones](#time-zones)). With no `starting` clause, the interval begins now. A starting time in the past also fires immediately.

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

## Time zones

A time without a zone (`daily starting 21:00`) is UTC, and the schedule steps in fixed UTC intervals: `daily` is always 24 hours.

To run at a wall-clock time that follows daylight saving, end the schedule with `tz <zone>`:

```bash
# 21:00 on this host's clock, every day, all year.
mrsm sync pipes -s 'daily starting 21:00 tz local' -d

# 21:00 New York time on weekdays.
mrsm sync pipes -s '0 21 * * mon-fri tz America/New_York' -d

# The same nightly job, with every fire moved by the job's own fixed offset of up to 5 minutes.
mrsm sync pipes -s 'daily starting 21:00 tz local jitter 5 minutes' -d
```

`<zone>` is `local` (the host's zone), `utc` (the same as no `tz` clause), or an IANA name such as `America/New_York` or `Europe/London` (any capitalization). An unknown name is an error. Under `tz`:

- a `starting` time without a zone, `today`, `tomorrow` and the cron fields are read on that zone's clock, and `now` and `in 5 minutes` are the current instant;
- a `starting` time with its own offset or abbreviation (`21:00 -04:00`) keeps the instant it names, and the schedule steps on the `tz` zone's clock from there;
- the clause applies to the whole schedule, `and` and `or` included, and comes at the end, before or after `jitter`;
- fire times are shown with the zone's offset at each instant (`mrsm show schedules 'daily starting 21:00 tz local'`).

`local` inside a container is the container's zone (`TZ`, or its `/etc/localtime`), usually UTC. When the host's zone cannot be determined, `tz local` uses UTC and logs a warning.

### Abbreviations after `starting`

As in 4.x, a zone abbreviation after the starting time is accepted:

- either abbreviation of the host's own zone (`EST` or `EDT` on a host in `America/New_York`) means the host's zone, as if the schedule ended in `tz local`: the time takes the zone's offset on that date, and the schedule keeps that wall-clock time across daylight-saving changes;
- `UTC`, `GMT` and `Z` mean UTC;
- any other abbreviation (`PDT` on that host) is accepted with a warning in the job's log and the time is read as UTC. Use an offset (`21:00 -07:00`) or `tz America/Los_Angeles` instead.

Abbreviations are matched in capitals only (`edt` is an error), and never on Windows, where 4.x does not match them either.

### Clock changes

A zoned schedule steps on the wall clock, so `every 12 hours starting 09:00 tz local` runs at 09:00 and 21:00 all year. When the clock changes:

| Schedule | Spring forward (02:00 becomes 03:00) | Fall back (02:00 becomes 01:00) |
|---|---|---|
| `hourly`, `every 5 minutes`, a cron hour of `*` or `*/n` | Times inside the skipped hour do not run; the cadence continues after it. | Times inside the repeated hour run in both occurrences. |
| A time inside the skipped hour, for `daily`, `weekly`, `monthly`, `yearly`, a cron hour such as `2`, or any interval longer than the skipped hour | Runs once, at 03:00. | |
| A time inside the repeated hour, for any interval longer than an hour or a specific cron hour | | Runs once, at the first 01:30. |

A schedule never runs twice at the same instant, never runs before the skipped hour for a time inside it, and never skips a day, week, month or year because of a clock change. A job restarted on either side of a change keeps its wall-clock time.

## Jitter

`jitter <duration>` moves every fire time of a job by one fixed offset between zero and the duration, so many jobs on the same cadence do not all start at once:

```bash
mrsm sync pipes -s 'hourly jitter 30 seconds' -d
```

The offset is the job's own (it is derived from the job's name): the same on every fire and after every restart, applied after time zone handling. The default is no jitter; `--jitter 30s` or `jobs.schedule.jitter_seconds` set a default for schedules without the clause. `jitter 0` forces exact times even when a default is configured.

??? example "The Python API"

    Parse a schedule in Python to inspect its next times:

    ```python
    from meerschaum.utils.schedule import parse_schedule

    trigger = parse_schedule('daily starting 2027-01-01')
    print(trigger.next())
    print(trigger.next())

    trigger = parse_schedule('daily starting 2027-03-13 21:00 tz America/New_York')
    print(trigger.next())  # 2027-03-13 21:00:00-05:00
    print(trigger.next())  # 2027-03-14 21:00:00-04:00
    ```
