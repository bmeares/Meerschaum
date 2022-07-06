<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# 👷 Background Jobs

Some actions need to run continuously, such as running the API or syncing pipes in a loop. Rather than relying on `systemd` or `cron`, you can use the built-in jobs system.

<asciinema-player src="/assets/casts/jobs.cast" autoplay="true" loop="true" size="small" preload="true"></asciinema-player>

## Jobs

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


## Schedules

You can run any command regularly with the flag `-s` or `--schedule` ― for example, `-s hourly` will execute the command once per hour. You can also specify a specific interval, e.g. `-s 'every 10 seconds'`. Here are a few of the supported frequencies:

  - `every N [seconds | minutes | hours | days]`
  - `hourly`
  - `daily`
  - `monthly`
  - `daily starting 12:00`
  - `monthly starting 6th`

Here is the complete [documentation for the interval syntax](https://red-engine.readthedocs.io/en/stable/condition_syntax/execution.html).

```bash
mrsm sync pipes -m weather -s 'every 30 seconds' -d
```


## Logs

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

