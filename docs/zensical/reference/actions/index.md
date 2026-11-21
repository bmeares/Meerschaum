# ⏯️ Actions

!!! note ""
    Run `mrsm help` (or `mrsm -h`) for every action and its options. Add `-h` to any command (`mrsm sync pipes -h`) or run `mrsm help sync pipes` for action details.

!!! tip inline end "Custom actions"
    Use [`@make_action`](/reference/plugins/writing-plugins/#the-make_action-decorator) to turn your own scripts into CLI actions.

Actions are commands for managing your Meerschaum instance and running bulk operations.

## Syntax

Actions use a simple verb-noun pattern (singular or plural):

```bash
bootstrap pipes
show pipes
sync pipes
verify pipes
clear pipes
drop pipes
delete pipes
```

Run them interactively in the Meerschaum shell (`mrsm`) or directly from your terminal:

```bash
$ mrsm
[ mrsm@sql:main ] ➤ show pipes

$ mrsm show pipes
```

## Getting Help

`mrsm help` (or `mrsm -h`, `mrsm show help`, or `help` in the shell) lists actions grouped by category, followed by plugin actions under **Plugins** and flags grouped at the bottom (including **Pipeline options** and **Access options**).

```text
$ mrsm help
usage: mrsm [actions] {options}

Create:
  bootstrap    Create pipes, connectors, jobs, Pipelines, plugins or teams step by step.
  register     Register pipes, connectors, Pipelines, plugins, tokens or users.
  copy         Copy connectors, pipes or Pipelines.
Sync:
  sync         Fetch and sync new data into pipes or Pipelines.
  plan         Preview what a sync would do, without syncing.
  verify       Verify pipes' data, schemas and row counts, or packages, plugins and venvs.
  watch        Watch jobs, logs or runs live, full-screen (`show` prints once).
```

Run `mrsm <action> -h` or `mrsm help <action>` for full details and plugin subcommands (e.g. `weather_wait` and `weather_status` appear under `mrsm weather -h`), or `mrsm show actions` to list every action name. Help reads the cached plugin manifest without starting Python; run `mrsm reload` after adding or editing a plugin to refresh it.

## Colors and Plain Output

ANSI styling is enabled only on a terminal — redirected or piped output (`mrsm show pipes > pipes.txt`) is plain text automatically. `stdout` and `stderr` are detected independently (`mrsm sync pipes --debug 2> debug.log` writes a plain log while keeping terminal colors), while background jobs preserve colors for the web console and `mrsm show logs`.

| Setting | Effect |
|---|---|
| `NO_COLOR` (any non-empty value) | Plain text everywhere, including terminals (full-screen views still work without colors). |
| `FORCE_COLOR` (anything except `0`, `false`, `no`) | Force styled output even when piped or redirected. |
| `formatting:ansi: false` (`mrsm edit config`) | Plain text everywhere and disables full-screen views. |

Python output through `meerschaum.utils.formatting` (`mrsm.pprint`, `info`, `warn`) follows the same rules (Jupyter notebooks count as terminals).

## Chaining Actions

Join actions with `+` to run them sequentially in a single process (like `&&` in bash). Flags after `:` apply to the entire chain (escape a literal colon as `::`):

```bash
mrsm sync pipes -i sql:local + sync pipes -i sql:main
mrsm sync pipes -c plugin:noaa + sync pipes -c sql:main : -s 'daily starting 00:00' -d
mrsm show version : x3
```

| Chain flag | Effect |
|---|---|
| `--loop` | Repeat the chain continuously. |
| `--min-seconds` | Seconds to sleep between laps (default `1`). |
| `-s`, `--schedule`, `--cron` | Run on a [schedule](/reference/background-jobs/schedules/). |
| `-d`, `--daemon` | Run the chain as a background job. |
| `x3` (or `3`) | Run the chain a fixed number of times. |

!!! note "Action chains vs. Pipelines"
    An action chain is an ad-hoc command sequence (stored internally as `chain` when daemonized; v4.1 `start pipeline ... -P {...}` jobs still work). A [Pipeline](/reference/pipelines/) is a saved dependency graph of pipes run via `start pipeline <name>`.

## Repeating Any Action

Add `--loop` to any action to repeat it every `--min-seconds` (default `1`). Failed runs back off up to an hour until they succeed; `stop jobs` or Ctrl-C stops the loop after the current run.

```bash
mrsm show pipes --loop --min-seconds 300
mrsm my_plugin_action --loop --min-seconds 60 -d
```

Actions that loop natively (`sync`, `verify`, `deduplicate`, `start job`, servers) keep their own behavior. Plugin actions without a `loop` parameter are called once per lap with `loop=False` and record each run for [job health](/reference/background-jobs/health/); legacy actions declaring `loop=False` continue receiving `loop=True`.

## Daemonize & `bash` Actions

Add `-d` to run any action — or any shell script — as a [background job](/reference/background-jobs/):

```bash
mrsm sync pipes -s 'every 3 hours' -d
mrsm /path/to/script.sh -s 'every 5 minutes' -d
```

## Subaction Pattern

Most actions take a **subaction** noun (`pipes`, `plugins`, `users`, `connectors`, `jobs`, `config`, `venvs`, `teams`) and standard pipe selectors (`-c`/`--connector-keys`, `-m`/`--metric-keys`, `-l`/`--location-keys`, `-t`/`--tags`, `-i`/`--instance`):

```bash
mrsm show pipes -c plugin:noaa -t production -i sql:main
```

!!! tip ""
    Every action also has a Python equivalent — see [docs.meerschaum.io](https://docs.meerschaum.io) (e.g. `mrsm.Pipe.sync()`, `mrsm.actions.actions['sync']`).

## Shell Completions

New in v5: install dynamic tab completions for `mrsm` and `meerschaum` (`mrsm install completions` auto-detects `$SHELL`, or PowerShell on Windows):

```bash
mrsm install completions bash        # or: zsh, fish, powershell
mrsm uninstall completions bash      # removes only files written by mrsm
```

| Shell | Install path | Try in current session |
|---|---|---|
| `bash` | `${XDG_DATA_HOME:-~/.local/share}/bash-completion/completions/{mrsm,meerschaum}` | `source <(mrsm show completions bash)` |
| `zsh` | `${XDG_DATA_HOME:-~/.local/share}/zsh/site-functions/_mrsm` | `source <(mrsm show completions zsh)` |
| `fish` | `${XDG_CONFIG_HOME:-~/.config}/fish/completions/{mrsm,meerschaum}.fish` | `mrsm show completions fish \| source` |
| `powershell` | `mrsm.ps1` in user data dir (sourced from UTF-8 `$PROFILE`) | `mrsm show completions powershell \| Out-String \| Invoke-Expression` |

Passing `--force` overwrites existing files not created by `mrsm`. Completions query `mrsm` live on `<Tab>` (~20 ms, no Python or plugin execution) for actions, subactions, flags, `choices`, connectors (`-i`/`-c`/`-r`/`-e`), jobs, plugins, config keys, and `--file`/`--env-file` paths. Run `mrsm reload` after adding plugins so they appear.

## Action Reference

### Pipe Lifecycle

| Action | Description |
|---|---|
| `register` | Register new pipes, plugins, users, tokens, Pipelines, or teams. |
| `bootstrap` | Interactive wizard to create pipes, connectors, jobs, Pipelines, plugins, or teams. |
| `edit` | Edit pipe parameters in `$EDITOR`, `edit config`, `edit users`, `edit teams`, or `edit pipes access`. |
| `copy` | Clone pipes or connectors (copies locks, never access metadata). |
| `tag` | Add or remove tags on existing pipes. |
| `delete` | Delete registrations (`pipes`, `plugins`, `users`, `teams`, `connectors`, `jobs`, `venvs`, `config`). |
| `drop` | Drop target tables while keeping pipe registrations. |
| `lock` / `unlock` | Protect pipes against edits, syncs, clears, drops, or deletion ([Teams, Access and Locks](teams-and-access.md)). |

Use `edit pipes access` to change owner/team/visibility, and `copy pipes <dest> --sync-data <mode>` (`all`, `backtrack`, `incremental`, or `filter`; add `--force` to overwrite existing registrations) to copy data in chunks:

```bash
mrsm edit pipes access -c plugin:noaa -m weather --team analytics --visibility private
mrsm copy pipes sql:dest -i sql:src --sync-data incremental -y
```

### Syncing & Data

| Action | Description |
|---|---|
| `sync` | Fetch and sync new/changed rows into pipes or Pipelines. |
| `verify` | Backfill historical ranges in chunks (`verify pipes`), reconcile table types (`verify schema`), or check packages. |
| `clear` | Delete rows within a `--begin`/`--end`/`--params` window while keeping the table (bare `clear` clears the terminal). |
| `flush` | Drop target tables/indices and re-sync from scratch. |
| `deduplicate` | Detect and remove duplicate rows chunk-by-chunk across the datetime axis. |

```bash
mrsm clear pipes -c plugin:noaa -m weather --begin 2024-01-01
mrsm deduplicate pipes -c plugin:noaa -m weather --begin 2024-01-01
mrsm flush pipes -c plugin:noaa -m weather
```

#### `verify schema`

Compare each pipe's database table against its declared `dtypes`, `columns`, and geometry types, print a plan, and prompt before altering (also available in Python as `pipe.verify_schema(apply=True)`):

```bash
mrsm verify schema -c plugin:noaa -m weather
#     Column       Now        New                                Change
#     station_id   INTEGER    BIGINT (int)                       widen
#     humidity     (missing)  DOUBLE PRECISION (float)           add column
#     reading_ts   TEXT       TIMESTAMPTZ (datetime64[ns, UTC])  change type (needs confirmation)
#  ❓ Apply 3 changes to the table of Pipe('plugin:noaa', 'weather')? [y/N]
```

| Option | Effect |
|---|---|
| `-y`, `--yes` | Apply all changes without prompting. |
| `--noask` | Apply safe additions/widenings only; skip risky type changes. |
| `--dry-run` | Print SQL only without modifying tables. |

Safe additions and widenings (`int` → `bigint`/`float`, `float` → `numeric`, `varchar(n)` → `text`, `text` → `json`) preserve all values; columns are never dropped, and static pipes are included. On PostgreSQL, type changes run via `ALTER COLUMN ... TYPE ... USING ...`; SQLite rebuilds the table.

Blocked in-place changes are reported while other changes still apply: partition columns (PostgreSQL/PostGIS/MSSQL; TimescaleDB and MySQL/MariaDB only allow date/timestamp or integer swaps), compressed TimescaleDB chunks (run `mrsm decompress pipes` first), and MSSQL indexed columns (`mrsm drop indices`, then `mrsm index pipes`).

### Maintenance

Optimize storage and query statistics (see [Pipe Maintenance](/reference/pipes/maintenance/)).

| Action | Description |
|---|---|
| `vacuum` | Reclaim dead-tuple disk space (`--full` for `VACUUM FULL`). |
| `analyze` | Refresh query planner statistics. |
| `compress` / `decompress` | Toggle columnstore compression and automatic compression policies. |
| `partition` | Change chunk width (`--chunk-minutes`) on partitioned tables / hypertables. |
| `index` | Create indices declared in `parameters['indices']` (and datetime/id indices). |
| `restart` | Restart stopped jobs that were not stopped manually. |
| `backup` / `restore` | `backup postgres` / `restore postgres` for the bundled stack ([Upgrading the stack](../stack/upgrades.md#backing-up-and-restoring)). |

```bash
mrsm index pipes -c plugin:noaa -m weather
mrsm partition pipes -m weather --chunk-minutes 10080
```

### Plugins & Packages

| Action | Description |
|---|---|
| `install` | Install plugins (from a repository) or Python packages (into the `mrsm` venv). |
| `uninstall` | Remove plugins or Python packages. |
| `upgrade` | Upgrade Meerschaum, plugins, packages, PostgreSQL extensions (`upgrade extensions`), or major DB versions (`upgrade postgres`). |
| `setup` | Run a plugin's `setup()` hook (`setup plugins noaa`). |
| `reload` | Reload plugins and refresh the cached plugin manifest. |

### Instances & API

| Action | Description |
|---|---|
| `api` | Start the WebAPI server or send commands to a remote API instance. |
| `stack` | Manage the bundled Docker Compose stack (`up`, `down`, `config`, …). |
| `start` | Start `api`, `mcp`, `jobs`, `gui`, `webterm`, `connectors`, or `pipeline`. |
| `stop` / `pause` | Stop or pause background jobs. |
| `login` | Authenticate an `api:` connector against a Meerschaum server. |
| `attach` | Attach interactively to a running background job. |
| `watch` | Full-screen live monitors (`watch run`, `watch jobs`; see [Watch and Shell Shortcuts](watch.md)). |

### Utility & Shell

| Action | Description |
|---|---|
| `show` | Inspect `pipes`, `data`, `columns`, `rowcounts`, `sizes`, `partitions`, `jobs`, `logs`, `config`, `version`, `plugins`, `users`, `teams`, `packages`, `tags`, `connectors` (secrets masked). |
| `os` / `sh` | Run subprocesses or system shell commands. |
| `python` | Open a REPL with `meerschaum` imported, run inline code, or execute `mrsm python script.py --flag`. |
| `sql` | Open an interactive SQL CLI or run a query/statement directly against a SQL connector. |

```bash
mrsm sql                                        # interactive CLI on sql:main
mrsm sql local table                            # SELECT * FROM table on sql:local
mrsm sql "SELECT * FROM users WHERE id = 1"     # query on sql:main
mrsm sql local exec "INSERT INTO t (id) VALUES (1)"
```

On [protected connectors](/reference/connectors/#protected-connectors), `sql` only permits single read statements (rolled back) unless `--allow-protected` is passed.

!!! danger "Arbitrary execution"
    `sql`, `os`, `sh`, and `python` execute arbitrary SQL, shell, and Python code with the Meerschaum process's privileges. Use them interactively in trusted environments only.
