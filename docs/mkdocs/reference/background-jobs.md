<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# 👷 Background Jobs

Some actions need to run continuously, such as running the API or syncing pipes in a loop. Rather than relying on `systemd` or `cron`, you can use the built-in jobs system.

All Meerschaum actions may be executed as background jobs by adding `-d` or `--daemon` flags or by prefacing the command with `start job`. A specific label may be assigned to a job with `--name`.

You can monitor the status of jobs with `show logs`, which will follow the logs of running jobs.

<asciinema-player src="/assets/casts/jobs.cast" autoplay="true" loop="true" size="small" preload="true"></asciinema-player>
