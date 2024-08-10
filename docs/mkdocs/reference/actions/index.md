# ⏯️ Actions

!!! note ""
    To see available actions, run `help` or `show actions`.

    Add `-h` to a command or preface with `show help` to learn more.

!!! tip inline end "Custom actions"
    Use the [`#!python @make_action` decorator](/reference/plugins/writing-plugins/#the-make_action-decorator) to make your scripts into custom actions.

Actions are commands for managing your Meerschaum instance and are a great way to perform bulk operations.


## Syntax

Actions follow a simple verb-noun syntax:

```bash
mrsm show pipes
```

## Chaining Actions

Join actions with `+` to run them in a single process (similar to `&&` in `bash`):

```bash
sync pipes -i sql:local + \
sync pipes -i sql:main
```

Flags added after `:` apply to the entire pipeline:

```bash
show version + show arguments :: --loop

sync pipes -c plugin:noaa + sync pipes -c sql:main : -s 'daily'
```

> You can escape `:` with `::`, e.g. `mrsm echo ::`.

## Daemonize Actions

Add `-d` to any action to run it as a [background job](/reference/background-jobs/).

```bash
mrsm sync pipes -s 'every 3 hours' -d
```

## `bash` Actions

Any `bash` command may be run as an action. This is a great way to run shell scripts through Meerschaum.

```bash
mrsm /path/to/script.sh -s 'every 5 minutes' -d
```