# ⏯️ Actions

!!! note ""
    To see available actions, run `help` or `show actions`.

    Add `-h` to a command or preface with `show help` to learn more.

!!! tip inline end "Custom actions"
    Use the [`#!python @make_action` decorator](/reference/plugins/writing-plugins/#the-make_action-decorator) to make your scripts into custom actions.

Actions are commands for managing your Meerschaum instance and are a great way to perform bulk operations.


## Syntax

Actions follow a simple verb-noun syntax (singular or plural):

```bash
bootstrap pipes
show pipes
sync pipes
verify pipes
clear pipes
drop pipes
delete pipes
```

Run `mrsm` (`python -m meerschaum`) to execute actions from the Meerschaum shell:

```bash
$ mrsm
[ mrsm@sql:main ] ➤ show pipes
```

Or run actions directly from the command line:

```bash
$ mrsm show pipes
```

## Chaining Actions

Join actions with `+` to run them in a single process (similar to `&&` in `bash`):

```bash
sync pipes -i sql:local + \
sync pipes -i sql:main
```

Flags added after `:` apply to the entire pipeline:

```bash
show version + show arguments : --loop

sync pipes -c plugin:noaa + \
sync pipes -c sql:main : -s 'daily starting 00:00' -d
```

> You can escape `:` with `::`, e.g. `mrsm echo ::` will output `:`.

Here are some useful pipeline flags:

- `--loop`  
  Run the pipeline commands continuously.
- `--min-seconds` (default `1`)  
  How many seconds to sleep between laps (if `--loop` or `x3`).
- `-s`, `--schedule`, `--cron`  
  Execute the pipeline on a [schedule](/reference/background-jobs/#-schedules).
- `-d`, `--daemon`  
  Create a background job to run the pipeline.
- `x3`, `3`  
  Execute the pipeline a specific number of times.

Note that you can add `:` to single commands as well:

```bash
mrsm show version : x3
```

## Daemonize Actions

Add `-d` to any action to run it as a [background job](/reference/background-jobs/).

```bash
mrsm sync pipes -s 'every 3 hours' -d
```

This works well when [chaining actions](#chaining-actions) to create a pipeline job:

```bash
sync pipes -i sql:local + \
sync pipes -c sql:local : -s 'daily starting 10:00' -d
```

## `bash` Actions

Any `bash` command may be run as an action. This is a great way to run shell scripts through Meerschaum.

```bash
mrsm /path/to/script.sh -s 'every 5 minutes' -d
```