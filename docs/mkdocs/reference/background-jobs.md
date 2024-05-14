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

You can run any command regularly with the flag `-s` or `--schedule` ― for example, `-s hourly` will execute the command once per hour. You may specify a specific interval, e.g. `-s 'every 10 seconds'`. Here are a few of the supported frequencies:

  - `every N [seconds | minutes | hours | days | weeks]`
  - `hourly`
  - `daily`
  - `weekly`
  - `monthly`
  - `daily starting 12:00`
  - `monthly starting 6th`

```bash
mrsm sync pipes -m weather -s 'every 30 seconds' -d
```

### Cron Format

For more fine-grained control, you may specify your schedule in a `cron` format:

```
[minute] [hour] [day] [month] [week]
```

See [APScheduler](https://apscheduler.readthedocs.io/en/3.x/modules/triggers/cron.html#expression-types) for the full `cron` format documentation.

### Schedule Combinations

You may combine schedule with "and" (`&`) and "or" (`|`) logic:

```
every 10 seconds and * * * may-aug *
```


!!! warning ""
    As of Meerschaum v2.2.0, scheduling is handled by the library [APScheduler](https://apscheduler.readthedocs.io) rather than [Rocketry](https://rocketry.readthedocs.io/en/stable/condition_syntax/index.html).

## 🪵 Logs

You can monitor the status of jobs with `show logs`, which will follow the logs of running jobs.

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

