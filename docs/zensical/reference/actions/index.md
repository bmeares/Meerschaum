# ⏯️ Actions

!!! note ""
    Run `mrsm help` (or `mrsm -h`) to see every action with a one-line description, then the options.

    Add `-h` to a command (`mrsm sync pipes -h`) or run `mrsm help sync pipes` to learn more about one action.

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

## Getting Help

`mrsm help` (the same as `mrsm -h`, `mrsm show help`, or `help` in the shell) starts with the actions, grouped by what they are for, each with a one-line description. The actions your plugins add follow in a **Plugins** group (with their subcommands), and the options come last, by group (the graph-run flags `--parents`, `--children`, `--resume`, `--run-id` and `--chunk-pipelining` are under **Pipeline options**; the flags of `edit pipes access`, `lock pipes` / `unlock pipes` and `register teams` / `edit teams` are under **Access options**).

```text
$ mrsm help
usage: mrsm [actions] {options}

Create and Build Pipes with Meerschaum.

Create:
  bootstrap    Create pipes, connectors, jobs, Pipelines or plugins step by step.
  register     Register pipes, connectors, Pipelines, plugins, tokens or users.
  copy         Copy connectors, pipes or Pipelines.

Sync:
  sync         Fetch and sync new data into pipes or Pipelines.
  plan         Preview what a sync would do, without syncing.
  verify       Verify pipes' data, schemas and row counts, or packages, plugins and venvs.
  watch        Watch jobs, logs or runs live, full-screen (`show` prints once).

...

Run `mrsm <action> -h` for an action's details, e.g. `mrsm sync pipes -h`.

Actions options:
  ...
```

Add `-h` to any action (`mrsm sync pipes -h`, `mrsm show -h`) or name it after `help` (`mrsm help sync pipes`) for its details. For a plugin's action, `mrsm <action> -h` prints its docstring and lists its subcommands: a plugin that defines `weather_wait` and `weather_status` shows them under `mrsm weather -h` as `wait` and `status`. `mrsm show actions` lists every action name.

Help starts no Python: it lists the plugin actions that the last command which loaded the plugins recorded. When a plugin changed since (or on a new root), help says so on its last line; run `mrsm reload` (or any command that loads plugins) and ask again.

## Colors and Plain Output

Output is styled (colors, bold) only when it goes to a terminal. Redirected to a file or piped into another program (`mrsm show pipes > pipes.txt`, `mrsm show jobs | grep running`), it is plain text without escape sequences. Standard output and standard error decide separately: warnings and `--debug` lines go to stderr, so `mrsm sync pipes --debug 2> debug.log` writes a plain log while the terminal keeps its colors, and the other way around. Jobs keep their colors, so their logs are colored in the web console and in `mrsm show logs`; the steps of an action chain run with `--timeout-seconds` are styled like the rest of the output.

| Setting | Effect |
|---|---|
| `NO_COLOR` (any non-empty value) | Plain text everywhere, also in a terminal. The full-screen views still work, without colors. |
| `FORCE_COLOR` (any value but `0`, `false` or `no`) | Styled output even when it is not going to a terminal. |
| `formatting:ansi: false` (`mrsm edit config`) | Plain text, like `NO_COLOR`. |

The same rules apply to Python code that prints through `meerschaum.utils.formatting` (`mrsm.pprint`, `info`, `warn`), so plugins and scripts follow them too (a Jupyter notebook counts as a terminal).

## Chaining Actions

Join actions with `+` to run them as one **action chain**, in a single process (similar to `&&` in `bash`): each step runs after the previous one succeeds.

```bash
sync pipes -i sql:local + \
sync pipes -i sql:main
```

Flags added after `:` apply to the whole chain:

```bash
show version + show arguments : --loop

sync pipes -c plugin:noaa + \
sync pipes -c sql:main : -s 'daily starting 00:00' -d
```

> You can escape `:` with `::`, e.g. `mrsm echo ::` will output `:`.

Here are some useful chain flags:

- `--loop`
  Run the chain continuously.
- `--min-seconds` (default `1`)
  How many seconds to sleep between laps (if `--loop` or `x3`).
- `-s`, `--schedule`, `--cron`
  Run the chain on a [schedule](/reference/background-jobs/schedules/).
- `-d`, `--daemon`
  Create a background job which runs the chain.
- `x3`, `3`
  Run the chain a specific number of times.

Note that you can add `:` to single commands as well:

```bash
mrsm show version : x3
```

!!! note "Action chains and Pipelines"

    An action chain is a one-off sequence of commands. A [Pipeline](/reference/pipelines/) is a saved selection of pipes the Syncing Engine runs in dependency order: `start pipeline daily` runs the Pipeline `daily` in the background. A chain that runs as a job is stored as the internal action `chain` (Meerschaum 4.1 stored it as `start pipeline ... -P {...}`; those jobs keep working), and `show jobs` prints it as you typed it.

## Repeating Any Action

Add `--loop` to any action to repeat it until it is stopped, `--min-seconds` apart (default 1), like `sync pipes --loop`. A failed run is tried again after a pause that grows while it keeps failing, up to an hour; `stop jobs` (or Ctrl-C) ends the loop after the current run. Actions from plugins repeat the same way, so a plugin action does not need its own `while True` loop: each run calls it once, with `loop=False`.

```bash
mrsm show pipes --loop --min-seconds 300
mrsm my_plugin_action --loop --min-seconds 60 -d
```

Actions that already loop or start jobs (`sync`, `verify` and `deduplicate pipes`, `sync pipelines`, `start job ...`, `-d`, the servers) keep their own behavior. So does a plugin action whose function has a `loop` parameter (`def poll(loop=False, **kwargs)`, the 4.1 way): it receives `loop=True` and repeats by itself. Remove the parameter and the loop to let Meerschaum repeat it, which also records each run for [job health](/reference/background-jobs/health/).

## Daemonize Actions

Add `-d` to any action to run it as a [background job](/reference/background-jobs/).

```bash
mrsm sync pipes -s 'every 3 hours' -d
```

This works well when [chaining actions](#chaining-actions) to run a chain as a job:

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

Most actions take a **subaction** (the noun) that selects what to operate on, usually `pipes`, but also `plugins`, `users`, `connectors`, `jobs`, `config`, `venvs`, and more:

```bash
register pipes        register plugins        register users
show pipes            show plugins            show jobs
delete pipes          delete connectors       delete jobs
```

Most subactions accept the standard pipe selectors (`-c`/`--connector-keys`, `-m`/`--metric-keys`, `-l`/`--location-keys`, `-t`/`--tags`, `-i`/`--instance`) so you can scope operations to a subset of pipes. Run `mrsm <action> --help` (or `mrsm help <action>`) to see the full list of flags for any action.

!!! tip ""
    Every action also has a Python equivalent. See the [API reference at docs.meerschaum.io](https://docs.meerschaum.io) for programmatic usage (e.g. `mrsm.Pipe.sync()`, `mrsm.actions.actions['sync']`).

## Shell Completions

!!! note ""
    New in v5.

`mrsm` completes actions, subactions, flags and values in `bash`, `zsh`, `fish` and PowerShell, for both the `mrsm` and `meerschaum` commands. Inside the Meerschaum shell (`mrsm`) completion is built in; this is for your own shell.

Install the script for the shell you use (`mrsm install completions` picks it from `$SHELL`; on Windows it is PowerShell), then start a new shell session:

```bash
mrsm install completions bash
mrsm install completions zsh
mrsm install completions fish
mrsm install completions powershell
```

Each command lists every file it wrote and where. Running it again is harmless, a file with the same name that `mrsm` did not write is never replaced (unless you add `--force`), and `mrsm uninstall completions <shell>` removes exactly what was installed.

| Shell | Written to |
|---|---|
| `bash` | `${XDG_DATA_HOME:-~/.local/share}/bash-completion/completions/mrsm` and `meerschaum` (loaded on demand by the `bash-completion` package; without it, add `source <(mrsm show completions bash)` to `~/.bashrc`) |
| `zsh` | `${XDG_DATA_HOME:-~/.local/share}/zsh/site-functions/_mrsm`. Add `fpath=(~/.local/share/zsh/site-functions $fpath)` and `autoload -Uz compinit && compinit` to `~/.zshrc` if that directory is not already on your `fpath` (the command prints the exact lines). |
| `fish` | `${XDG_CONFIG_HOME:-~/.config}/fish/completions/mrsm.fish` and `meerschaum.fish` |
| `powershell` | `mrsm.ps1` in your user data directory, loaded from a marked block that is added to the profile PowerShell reports (`$PROFILE`; a profile that is not UTF-8, such as one Windows PowerShell 5.1 wrote as UTF-16, is never edited and the line to add is printed instead) |

To try it without installing, load the script into the current session:

```bash
source <(mrsm show completions bash)           # bash
source <(mrsm show completions zsh)            # zsh (after compinit)
mrsm show completions fish | source            # fish
mrsm show completions powershell | Out-String | Invoke-Expression   # PowerShell
```

`show completions` is answered before anything else starts, so loading the script from a start-up file (`~/.bashrc`, `~/.zshrc`) is cheap and never runs plugin code.

The script contains no candidates. On every <kbd>Tab</kbd> it asks the program, so the answers follow what is installed and configured:

- actions (including those of your plugins) and subactions: `mrsm sh<Tab>`, `mrsm show <Tab>`
- flags and the values they take: `mrsm show pipes -<Tab>`, `mrsm show jobs --executor-keys <Tab>` (flags that a plugin declares with `choices` complete them too)
- connector keys for `-i` / `--instance`, `-c` / `--connector-keys`, `-r` / `--repository` and `-e` / `--executor-keys`: `mrsm show pipes -c <Tab>`
- job names (`stop jobs <Tab>`), plugin names (`edit plugins <Tab>`), configuration keys (`show config <Tab>`)
- file names for `--file` / `--compose-file` and `--env-file`

Asking never runs an action or writes anything: it reads the configuration and the plugin manifest the last command cached, starts no plugin code and no Python, and takes about 20 ms (50 ms through the Python `mrsm` command of the wheel). A plugin added since the last command appears after any command that loads plugins (`mrsm reload` does it on purpose). Pipe keys (`-m`, `-l`) and tags are not completed, since they need a database connection.

## Action Reference

### Pipe Lifecycle

Create, edit, duplicate, and remove pipes (and their registrations).

| Action | Description |
|---|---|
| `register` | Register new pipes, plugins, users or teams on an instance / repository. |
| `bootstrap` | Launch an interactive wizard to create pipes or connectors. |
| `edit` | Edit an existing element (pipe parameters, config, users, etc.). |
| `copy` | Duplicate pipes or connectors. |
| `tag` | Add or remove tags on existing pipes. |
| `delete` | Delete an element's registration (`pipes`, `plugins`, `users`, `teams`, `connectors`, `jobs`, `venvs`, `config`). |
| `drop` | Drop a pipe's data / target table while keeping its registration. |
| `lock` / `unlock` | Lock pipes against parameter edits, syncs, clears, drops or deletion, and remove the locks. See [Teams, Access and Locks](teams-and-access.md). |

#### `edit`

Edit the parameters of an existing element. `edit pipes` opens the selected pipes' parameters in your `$EDITOR` as YAML; saving persists the changes. `edit config` opens the configuration registry, `edit users` changes a user's password, type, role, primary team and email, and `edit teams` a team's members, managers and name. `edit pipes access` sets the selected pipes' owner, team and visibility ([Teams, Access and Locks](teams-and-access.md)).

```bash
mrsm edit pipes -c plugin:noaa -m weather
mrsm edit pipes access -c plugin:noaa -m weather --team analytics --visibility private
```

#### `copy`

Duplicate pipes or connectors. `copy pipes` clones the registration (and optionally the data) of the selected pipes into new keys, which is handy for branching a stream onto a new instance. A copy keeps the source's locks and never its access (owner, team, visibility).

```bash
mrsm copy pipes -c plugin:noaa -m weather -i sql:local
```

### Syncing & Data

Move data into pipes and manage what's already stored.

| Action | Description |
|---|---|
| `sync` | Fetch from sources and sync new/changed rows into pipes. |
| `verify` | Re-sync a historical range in chunks to backfill gaps and reconcile mismatches; `verify schema` fixes tables whose columns differ from their dtypes; also verifies packages. |
| `clear` | Remove rows from pipes within a datetime window (or clear the screen). |
| `flush` | Drop and re-sync pipes or indices from scratch. |
| `deduplicate` | Remove duplicate rows from pipes' tables, chunking across the datetime axis. |

#### `verify schema`

Compare each selected pipe's table with the pipe's dtypes (its `dtypes`, its `columns`
roles and geometry notations) and fix the differences. The plan prints as a table per pipe,
then `verify schema` asks before changing anything:

```bash
mrsm verify schema -c plugin:noaa -m weather
#  💬 The table "plugin_noaa_weather" of Pipe('plugin:noaa', 'weather') differs from its dtypes:
#     Column       Now        New                                Change
#     -----------  ---------  ---------------------------------  -----------------------
#     station_id   INTEGER    BIGINT (int)                       widen
#     temperature  INTEGER    DOUBLE PRECISION (float)           widen
#     humidity     (missing)  DOUBLE PRECISION (float)           add column
#     reading_ts   TEXT       TIMESTAMPTZ (datetime64[ns, UTC])  change type (needs confirmation); USING ...
#
#  ❓ Apply 4 changes to the table of Pipe('plugin:noaa', 'weather')? [y/N]
```

- Added columns and safe widenings (`int` to `bigint`, `int` to `float`, `float` to `numeric`,
  `varchar(n)` to `text`, `text` to `json`) keep every value.
- Other type changes may fail on existing values (`'abc'` is not a number) or change them, so
  they need confirmation. On PostgreSQL they run as `ALTER COLUMN ... TYPE ... USING ...` in one
  statement; SQLite cannot change a column's type in place, so its table is rebuilt (the plan
  says so).
- A column is never dropped. Static pipes are included: their syncs never change the table, and
  this is how their tables catch up with edited `dtypes`.

| Option | Effect |
|---|---|
| `-y`, `--yes` | Apply every change without asking. |
| `--noask` | Apply the safe changes only; the others are refused. |
| `--dry-run` | Print the SQL only (nothing changes). |

Some changes cannot be made in place, so the plan marks them as blocked, says what to do, and
the other changes still apply:

- A column the table is partitioned by keeps its type on PostgreSQL, PostGIS and MSSQL (tables
  of pipes with a `datetime` column are partitioned by it there by default). A TimescaleDB
  hypertable's time column changes only between timestamp and date types (or between integer
  types), and a MySQL / MariaDB partitioning column only between `DATE` and `DATETIME` (or
  between integer types). Keep the column's current type in `dtypes`, or drop the pipe and sync
  it again to rebuild the table with the new type.
- A TimescaleDB hypertable with compressed chunks cannot change column types (decompress it first
  with `mrsm decompress pipes`).
- MSSQL cannot change the type of an indexed column (drop the pipe's indices first with
  `mrsm drop indices`, then rebuild them with `mrsm index pipes`).

From Python, the same plan is `pipe.verify_schema()` (`apply=True` applies it).

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
| `decompress` | Inverse of `compress`, remove the policy and convert chunks back to row-store. |
| `partition` | Repartition target tables to a new chunk width (`--chunk-minutes`). |
| `restart` | Restart stopped jobs that were not manually stopped. |
| `index` | Create the indices defined in pipes' parameters. |
| `backup` | `backup postgres`: an online backup of the stack's database (v5, see [Backing up and restoring](../stack/upgrades.md#backing-up-and-restoring)). |
| `restore` | `restore postgres`: restore the stack's database from a backup into a new volume (v5, same page). |

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
| `upgrade` | Upgrade Meerschaum itself, plugins, or packages (e.g. `upgrade meerschaum`); `upgrade extensions` updates a PostgreSQL instance's extensions, `upgrade postgres` moves the stack's database to a newer PostgreSQL major (v5, see [Upgrading the stack](../stack/upgrades.md)). |
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
| `watch` | Full-screen monitors (v5): `watch run` (a live engine run), `watch jobs` (jobs and their logs). See [Watch and Shell Shortcuts](watch.md). |

### Utility & Shell

Inspect state and drop down to the shell, Python, or SQL.

| Action | Description |
|---|---|
| `show` | Display elements: `pipes`, `data`, `columns`, `rowcounts`, `sizes`, `partitions`, `jobs`, `logs`, `config`, `version`, `plugins`, `users` (a table with roles and teams), `teams`, `packages`, `tags`, `connectors` (secrets masked), … |
| `os` | Launch a subprocess and stream its output to stdout. |
| `sh` | Execute system shell commands. |
| `python` | Open a Python REPL with Meerschaum imported, run inline Python, or run a file (`mrsm python script.py --verbose`: every word after the file name is the script's, as with `python script.py`). |
| `sql` | Run a SQL query against a connector, or open an interactive SQL CLI. |

#### `sql`

Interact directly with a SQL connector. With no arguments it opens an interactive CLI on `sql:main`; otherwise `{label} {method} {query/table}` reads a table/query into a DataFrame (`read`) or executes a statement (`exec`).

```bash
mrsm sql                                   # interactive CLI on sql:main
mrsm sql local table                       # SELECT * FROM table on sql:local
mrsm sql "SELECT * FROM users WHERE id = 1"  # run a query on sql:main
mrsm sql local exec "INSERT INTO t (id) VALUES (1)"
```

On a [protected connector](/reference/connectors/#protected-connectors), a query runs only if
it is one read statement (in a transaction that is rolled back), and a statement that writes is
refused unless you pass `--allow-protected`.

!!! danger "Arbitrary execution"
    `sql`, `os`, `sh`, and `python` run **arbitrary** SQL, shell commands, and Python code with the privileges of the Meerschaum process. They are intended for trusted, interactive use only, never expose them to untrusted input, and be cautious enabling them on shared or networked instances.
