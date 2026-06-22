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

## Subaction Pattern

Most actions take a **subaction** (the noun) that selects what to operate on — usually `pipes`, but also `plugins`, `users`, `connectors`, `jobs`, `config`, `venvs`, and more:

```bash
register pipes        register plugins        register users
show pipes            show plugins            show jobs
delete pipes          delete connectors       delete jobs
```

Most subactions accept the standard pipe selectors (`-c`/`--connector-keys`, `-m`/`--metric-keys`, `-l`/`--location-keys`, `-t`/`--tags`, `-i`/`--instance`) so you can scope operations to a subset of pipes. Run `mrsm <action> --help` (or `show help <action>`) to see the full list of flags for any action.

!!! tip ""
    Every action also has a Python equivalent. See the [API reference at docs.meerschaum.io](https://docs.meerschaum.io) for programmatic usage (e.g. `mrsm.Pipe.sync()`, `mrsm.actions.actions['sync']`).

## Action Reference

### Pipe Lifecycle

Create, edit, duplicate, and remove pipes (and their registrations).

| Action | Description |
|---|---|
| `register` | Register new pipes, plugins, or users on an instance / repository. |
| `bootstrap` | Launch an interactive wizard to create pipes or connectors. |
| `edit` | Edit an existing element (pipe parameters, config, users, etc.). |
| `copy` | Duplicate pipes or connectors. |
| `tag` | Add or remove tags on existing pipes. |
| `delete` | Delete an element's registration (`pipes`, `plugins`, `users`, `connectors`, `jobs`, `venvs`, `config`). |
| `drop` | Drop a pipe's data / target table while keeping its registration. |

#### `edit`

Edit the parameters of an existing element. `edit pipes` opens the selected pipes' parameters in your `$EDITOR` as YAML; saving persists the changes. `edit config` opens the configuration registry, and `edit users` edits user attributes.

```bash
mrsm edit pipes -c plugin:noaa -m weather
```

#### `copy`

Duplicate pipes or connectors. `copy pipes` clones the registration (and optionally the data) of the selected pipes into new keys, which is handy for branching a stream onto a new instance.

```bash
mrsm copy pipes -c plugin:noaa -m weather -i sql:local
```

### Syncing & Data

Move data into pipes and manage what's already stored.

| Action | Description |
|---|---|
| `sync` | Fetch from sources and sync new/changed rows into pipes. |
| `verify` | Re-sync a historical range in chunks to backfill gaps and reconcile mismatches; also verifies packages. |
| `clear` | Remove rows from pipes within a datetime window (or clear the screen). |
| `flush` | Drop and re-sync pipes or indices from scratch. |
| `deduplicate` | Remove duplicate rows from pipes' tables, chunking across the datetime axis. |

#### `deduplicate`

Scan pipes' target tables for duplicate rows (rows sharing the same index columns) and remove them, working chunk-by-chunk across the datetime axis so large tables stay manageable. Useful after a misconfigured sync or schema change introduced duplicates.

```bash
mrsm deduplicate pipes -c plugin:noaa -m weather --begin 2024-01-01
```

#### `flush` vs. `clear`

These both remove data, but differ in scope:

- **`clear pipes`** deletes rows from the existing table within a window (defaults to everything; narrow with `--begin` / `--end` / `--params`). The table and registration stay intact.
- **`flush pipes`** is destructive-then-rebuild: it drops the pipe's data (and indices) and re-syncs from the source, effectively recreating the pipe's contents from scratch.

```bash
# Remove rows on/after 2024-01-01 but keep the table.
mrsm clear pipes -c plugin:noaa -m weather --begin 2024-01-01

# Wipe and rebuild the pipe entirely.
mrsm flush pipes -c plugin:noaa -m weather
```

Bare `clear` (no subaction) just clears the terminal screen.

### Maintenance

Optimize storage and statistics for pipes' target tables. See [Pipe Maintenance](/reference/pipes/maintenance/) for the full guide and per-flavor behavior.

| Action | Description |
|---|---|
| `vacuum` | Reclaim dead-tuple disk space (PostgreSQL `VACUUM`; `--full` for `VACUUM FULL`). |
| `analyze` | Refresh the query planner's statistics (no space reclaimed). |
| `compress` | Enable compression / columnstore and install a policy so future chunks compress automatically. |
| `decompress` | Inverse of `compress` — remove the policy and convert chunks back to row-store. |
| `partition` | Repartition target tables to a new chunk width (`--chunk-minutes`). |
| `restart` | Restart stopped jobs that were not manually stopped. |
| `index` | Create the indices defined in pipes' parameters. |

#### `index`

Create the indices declared in each pipe's `parameters['indices']` (plus the implicit datetime/id indices) on the target table. Run it after editing a pipe's indices, or to (re)build indices on a table that was created externally.

```bash
mrsm index pipes -c plugin:noaa -m weather
```

#### `partition`

Repartition natively range-partitioned tables (PostgreSQL/PostGIS, MySQL/MariaDB, MSSQL with `hypertable: True`, or TimescaleDB hypertables) to a new chunk width. On TimescaleDB this changes the interval for *future* chunks; on other flavors the table is rebuilt (read, dropped, re-synced) at the new width.

```bash
# Rebuild the 'weather' pipes' tables into 7-day partitions.
mrsm partition pipes -m weather --chunk-minutes 10080
```

### Plugins & Packages

Manage Meerschaum plugins and their Python dependencies.

| Action | Description |
|---|---|
| `install` | Install Meerschaum plugins (from a repository) or Python packages (into the `mrsm` venv). |
| `uninstall` | Uninstall plugins or Python packages. |
| `upgrade` | Upgrade Meerschaum itself, plugins, or packages (e.g. `upgrade meerschaum`). |
| `setup` | Run a plugin's `setup()` function (e.g. `setup plugins noaa`). |
| `reload` | Reload the running Meerschaum instance (re-import plugins and modules in place). |

### Instances & API

Run servers, control jobs, and connect to remote instances.

| Action | Description |
|---|---|
| `api` | Start the WebAPI server, or send commands to a running API instance. |
| `stack` | Control the bundled Docker Compose stack (`up`, `down`, `config`, …). |
| `start` | Start subsystems: `api`, `jobs`, `gui`, `webterm`, `connectors`, `pipeline`, … |
| `stop` | Stop background jobs started with `-d` or `start job`. |
| `pause` | Pause running background jobs. |
| `login` | Log into a Meerschaum API instance to authenticate the connector. |
| `attach` | Attach to a running background job's logs / interactive session. |

### Utility & Shell

Inspect state and drop down to the shell, Python, or SQL.

| Action | Description |
|---|---|
| `show` | Display elements: `pipes`, `data`, `columns`, `rowcounts`, `sizes`, `partitions`, `jobs`, `logs`, `config`, `version`, `plugins`, `users`, `packages`, `tags`, `connectors`, … |
| `os` | Launch a subprocess and stream its output to stdout. |
| `sh` | Execute system shell commands. |
| `python` | Open a Python REPL with Meerschaum imported (or run inline Python). |
| `sql` | Run a SQL query against a connector, or open an interactive SQL CLI. |

#### `sql`

Interact directly with a SQL connector. With no arguments it opens an interactive CLI on `sql:main`; otherwise `{label} {method} {query/table}` reads a table/query into a DataFrame (`read`) or executes a statement (`exec`).

```bash
mrsm sql                                   # interactive CLI on sql:main
mrsm sql local table                       # SELECT * FROM table on sql:local
mrsm sql "SELECT * FROM users WHERE id = 1"  # run a query on sql:main
mrsm sql local exec "INSERT INTO t (id) VALUES (1)"
```

!!! danger "Arbitrary execution"
    `sql`, `os`, `sh`, and `python` run **arbitrary** SQL, shell commands, and Python code with the privileges of the Meerschaum process. They are intended for trusted, interactive use only — never expose them to untrusted input, and be cautious enabling them on shared or networked instances.