//! The core action docstrings, ported from `meerschaum/actions/*.py`.
//!
//! Each entry is the action function's `__doc__` as v4 sees it at runtime (CPython 3.14 strips
//! the common indentation of docstrings at compile time; the module-level
//! `choices_docstring(...)` suffix is already appended). `mrsm <action> --help` and
//! `help <action>` print `textwrap.dedent(__doc__)` (see [`crate::dispatch::parse_help`]).
//!
//! Keyed by the v4 function name (`sync`, `_sync_pipes`, ...).

/// `function.__doc__` for a core action function, by its v4 name.
pub fn docstring(function: &str) -> Option<&'static str> {
    Some(match function {
        "_analyze_pipes" => {
            r##"
Analyze the target tables for the selected pipes.
"##
        }
        "_api_start" => {
            r##"Start the API server.

Parameters
----------
port: Optional[int], default None
    Port to bind the API server to.
    If `None`, use 8000.

host: Optional[str], defailt None
    The address to bind to.
    If `None`, use '0.0.0.0'.

webterm_port: Optional[int], default None
    Port to bind the webterm server to.
    If `None`, use 8765.

workers: Optional[int], default None
    How many worker threads to run.
    If `None`, defaults to the number of CPU cores or 1 on Android.

production: bool, default False
    Start the API server with Gunicorn instead of Uvicorn.
    Useful for production deployments.

private: bool, default False
    Require authentication for all endpoints.

secure: bool, default False
    Only allow `admin` users to execute actions.

keyfile: str, default None
    If provided, serve over HTTPS with this key file.
    Requires `--certfile`.

certfile: str, default None
    If provided, serve over HTTPS with this certfile.
    Requires `--keyfile`.
"##
        }
        "_attach_jobs" => {
            r##"
Attach to a job, and prompt the user when blocking on input.
"##
        }
        "_attach_logs" => {
            r##"
Attach to jobs' logs.
"##
        }
        "_bootstrap_connectors" => {
            r##"
Prompt the user for the details necessary to create a Connector.
"##
        }
        "_bootstrap_jobs" => {
            r##"
Launch an interactive wizard to create new jobs.
"##
        }
        "_bootstrap_pipes" => {
            r##"
Create a new pipe.
If no keys are provided, guide the user through the steps required.

"##
        }
        "_bootstrap_plugins" => {
            r##"
Launch an interactive wizard to guide the user to creating a new plugin.
"##
        }
        "_clear_cache" => {
            r##"
Delete local cache or temporary tables or keys from a SQL or Valkey connector.
"##
        }
        "_clear_pipes" => {
            r##"
Clear pipes' data without dropping any tables.
"##
        }
        "_compress_pipes" => {
            r##"
Compress the target tables for the selected pipes.
"##
        }
        "_copy_connectors" => {
            r##"
Create a new connector from an existing one.
"##
        }
        "_copy_pipes" => {
            r##"
Copy pipes' attributes and make new pipes.
"##
        }
        "_decompress_pipes" => {
            r##"
Decompress the target tables for the selected pipes.
"##
        }
        "_deduplicate_pipes" => {
            r##"
Deduplicate pipes' tables, chunking across their datetime axes.
"##
        }
        "_delete_cache" => {
            r##"
Delete local cache or temporary tables or keys from a SQL or Valkey connector.

Examples
--------
delete cache
delete cache valkey:main sql:main
"##
        }
        "_delete_config" => {
            r##"
Delete configuration files.
"##
        }
        "_delete_connectors" => {
            r##"
Delete configured connectors.

Example:
    `delete connectors sql:test`
"##
        }
        "_delete_jobs" => {
            r##"
Remove a job's log files and delete the job's ID.

If the job is running, ask to kill the job first.
"##
        }
        "_delete_pipes" => {
            r##"
Drop pipes and delete their registrations.
"##
        }
        "_delete_plugins" => {
            r##"
Delete plugins from a Meerschaum repository.
"##
        }
        "_delete_users" => {
            r##"
Delete users from a Meerschaum instance. Adequate permissions are required.
"##
        }
        "_delete_venvs" => {
            r##"
Remove virtual environments.
Specify which venvs to remove, or remove everything at once.
"##
        }
        "_drop_indices" => {
            r##"
Drop pipes' indices.
"##
        }
        "_drop_pipes" => {
            r##"
Drop pipes' tables but keep pipe metadata registration.
"##
        }
        "_edit_config" => {
            r##"
Edit Meerschaum configuration files.

Specify a specific configuration key to edit.
Defaults to editing `meerschaum` configuration (connectors, instance, etc.).

Pass `--rollback` to restore a key from its most recent backup instead of
opening the editor. Backups are taken automatically before each edit.

Examples:
    ```
    ### Edit the main 'meerschaum' configuration.
    edit config

    ### Edit 'system' configuration.
    edit config system

    ### Create a new configuration file called 'myconfig'.
    edit config myconfig

    ### Roll back the 'stack' configuration to its previous state.
    edit config stack --rollback
    ```
"##
        }
        "_edit_definition" => {
            r##"
Edit pipes' definitions.
Alias for `edit pipes definition`.
"##
        }
        "_edit_jobs" => {
            r##"
Edit existing jobs.
"##
        }
        "_edit_pipes" => {
            r##"
Open and edit pipes' configuration files.

If `fetch:definition` is specified, open a definition file (e.g. `.sql` for `sql` pipes).

Usage:
    ```
    ### Edit all pipes.
    edit pipes

    ### Edit a SQL definition for the pipe `sql_main_mymetric`.
    edit pipes definition -c sql:main -m mymetric
    ```
"##
        }
        "_edit_plugins" => {
            r##"
Edit a plugin's source code.
"##
        }
        "_edit_tokens" => {
            r##"
Edit tokens registered to an instance.
"##
        }
        "_edit_users" => {
            r##"
Edit users' registration information.
"##
        }
        "_flush_indices" => {
            r##"
Drop and rebuild pipes' indices.
"##
        }
        "_flush_pipes" => {
            r##"
Drop and resync pipes.
Equivalent to `drop pipes + sync pipes`.
"##
        }
        "_index_pipes" => {
            r##"
Create pipes' indices.
"##
        }
        "_install_packages" => {
            r##"
Install PyPI packages into the Meerschaum virtual environment.

Example:
    `install packages pandas numpy`
"##
        }
        "_install_plugins" => {
            r##"
Install a plugin.

By default, install from the main Meerschaum repository (mrsm.io).
Use a private repository by specifying the API label after the plugin.
NOTE: the --instance flag is ignored!

Usage:
    install plugins [plugin]

Examples:
    - install plugins noaa
    - install plugins noaa --repo mrsm  (mrsm is the default instance)
    - install plugins noaa --repo mycustominstance
"##
        }
        "_install_required" => {
            r##"
Install the required packages for Meerschaum plugins.
Each plugin's packages will be installed into its virtual environment.

Example:
    `install required noaa covid`
"##
        }
        "_partition_pipes" => {
            r##"
Repartition the target tables for the selected pipes.

Select pipes with the standard `-c` / `-m` / `-l` / `-i` / `-t` filters. Each selected pipe is
repartitioned via `Pipe.repartition()`; non-partitioned pipes (and unsupported flavors like
SQLite) report a failure and are skipped.

Parameters
----------
chunk_minutes: Optional[int], default None
    The new partition width in minutes (`--chunk-minutes`). Defaults to each pipe's
    `verify.chunk_minutes` (30 days). For example, `10080` is 7 days and `1440` is 1 day.

yes, force, noask:
    Confirmation controls. Pass `-y` / `--yes` (or `-f` / `--force`) to skip the prompt — the
    rebuild reads the whole table into memory and briefly drops it, so confirm for large tables.

Usage:
- partition pipes -c sql:main -m weather -l us --chunk-minutes 10080
    - Rebuild one pipe's table to 7-day partitions.
- partition pipes -i sql:main -f
    - Repartition every partitioned pipe on `sql:main` to its configured width, no prompt.

"##
        }
        "_pause_jobs" => {
            r##"
Pause (suspend) running jobs.
"##
        }
        "_register_connectors" => {
            r##"
Create new connectors programmatically with `--params`.
See `bootstrap connector`.

Examples:

mrsm register connector sql:tmp --params 'uri:sqlite:////tmp/tmp.db'

mrsm register connector -c sql:new --params '{"database": "/tmp/new.db"}'
"##
        }
        "_register_pipes" => {
            r##"
Create and register Pipe objects.

Required:
    `connector_keys` and `metric_keys`.
    If `location_keys` is empty, assume [`None`].
"##
        }
        "_register_plugins" => {
            r##"
Upload plugins to an API instance (repository).
"##
        }
        "_register_tokens" => {
            r##"
Register a new long-lived access token for API access.
Note that omitting and end time or TTL will generate token which does not expire.

Examples:

mrsm register token --end 2032-01-01

mrsm register token --ttl-days 1000

mrsm register token --name weather-sensor

"##
        }
        "_register_users" => {
            r##"
Register a new user to a Meerschaum instance.
"##
        }
        "_restart_daemons" => {
            r##"
Restart the CLI daemons.
"##
        }
        "_restart_jobs" => {
            r##"
Restart stopped jobs which have not been manually stopped.
"##
        }
        "_setup_plugins" => {
            r##"
Run the `setup()` function for Meerschaum plugins.

Example:
    `setup plugins noaa covid`
"##
        }
        "_show_actions" => {
            r##"
Show available actions.
"##
        }
        "_show_arguments" => {
            r##"
Show the parsed keyword arguments.
"##
        }
        "_show_columns" => {
            r##"
Show the registered and table columns for pipes.
"##
        }
        "_show_config" => {
            r##"
Show the configuration dictionary.
Sub-actions defined in the action list are recursive indices in the config dictionary.

Example:
    `show config pipes` -> cf['pipes']
"##
        }
        "_show_connectors" => {
            r##"
Show connectors configuration and, if requested, specific connector attributes.

Examples:
    `show connectors`
        Display the connectors configuration.

    `show connectors sql:main`
        Show the connectors configuration and the attributes for the connector 'sql:main'.
"##
        }
        "_show_daemons" => {
            r##"
Print information about the running CLI daemons.
"##
        }
        "_show_data" => {
            r##"
Show pipes data as Pandas DataFrames.

Usage:
    - Use --gui to open an interactive window.

    - `show data all` to grab all data for the chosen Pipes.
      WARNING: This may be dangerous!

    - `show data 60` to grab the last 60 (or any number) minutes of data for all pipes.

    - `show data --begin 2020-01-01 --end 2021-01-01` to specify date rangers.
      **NOTE:** You must specify to at least the day, otherwise the date parser will assume
      you mean today's date.

    - Regular pipes parameters (-c, -m, -l, etc.)

Examples:
    - show data -m weather --gui
        Open an interactive pandasgui window for the last 1440 minutes of data
        for all pipes of metric 'weather'.
"##
        }
        "_show_environment" => {
            r##"
Show all of the current environment variables with begin with `'MRSM_'`.
"##
        }
        "_show_help" => {
            r##"
Print the --help menu from `argparse`.
"##
        }
        "_show_jobs" => {
            r##"
Show the currently running and stopped jobs.
"##
        }
        "_show_logs" => {
            r##"
Show the logs for jobs.

You may specify jobs to only print certain logs.
To print the entire log file, add the `--nopretty` flag.

Usage:
    `show logs`
    `show logs --nopretty`
    `show logs myjob myotherjob`
"##
        }
        "_show_packages" => {
            r##"
Show the packages in dependency groups, or as a list with `--nopretty`.
"##
        }
        "_show_partitions" => {
            r##"
Show partitioning information for the selected pipes.

For natively range-partitioned pipes (PostgreSQL / PostGIS, MySQL / MariaDB, MSSQL) and
TimescaleDB hypertables, lists the partition / chunk count, the physical partition width, and
the approximate number of rows per partition (total rowcount ÷ partition count) — a useful
signal for tuning the width.
"##
        }
        "_show_graph" => {
            r##"
Print the selected pipes and their dependencies as a DAG (v5).

Each node is indented under its deepest parent and lists its in-selection parents after `←`;
`*` marks the pipes that were selected directly (the rest were pulled in by `--parents` or
`--children`). Edges come from `parents` / `children` and from `{{ Pipe() }}` references.

Examples:
    - `show graph -t demo`
    - `show graph -m daily --parents`
"##
        }
        "_show_runs" => {
            r##"
List the recorded runs of the Syncing Engine, newest first (v5).

Give a run ID (`show run RUN_ID`) to print that run's Gantt chart instead.

Examples:
    - `show runs`
    - `show runs --last 5`
    - `show run 01JA...`
"##
        }
        "_plan_pipes" => {
            r##"
Print the Syncing Engine's plan for the selected pipes without syncing (v5).

One row per node: the action (`root`, `skip`, `incremental`, `backfill`, `rebuild`, `observe`),
the sync bounds, the number of chunks and the reason. Accepts the usual key and tag filters,
`--begin`, `--end`, `--parents [N]`, `--children [N]` and `--resume RUN_ID`.

Examples:
    - `plan pipes -t demo --parents`
    - `plan pipes -m daily --nopretty`
"##
        }
        "plan" => {
            r##"
Print what a sync would do, without syncing (v5).

Command:
    `plan {option}`

Example:
    `plan pipes --parents`

    Options:
        `plan [pipes]`"##
        }
        "_show_pipes" => {
            r##"
Print a stylized tree of available Meerschaum pipes.
Respects global ANSI and UNICODE settings.
"##
        }
        "_show_plugins" => {
            r##"
Show the installed plugins.
"##
        }
        "_show_rowcounts" => {
            r##"
Show the rowcounts for pipes.

To see remote rowcounts (execute `COUNT(*)` on the source server),
execute `show rowcounts remote`.
"##
        }
        "_show_schedules" => {
            r##"
Print the upcoming timestamps according to the given schedule.

Examples:
    show schedule 'daily starting 00:00'
    show schedule 'every 12 hours and mon-fri starting 2024-01-01'
"##
        }
        "_show_sizes" => {
            r##"
Show the on-disk sizes of pipes' target tables, sorted largest first.
"##
        }
        "_show_tags" => {
            r##"
Show the existing tags and their associated pipes.
"##
        }
        "_show_targets" => {
            r##"
Show the target tables for pipes, grouped by target name.
"##
        }
        "_show_tokens" => {
            r##"
Print a table of the registered tokens on the instance.
"##
        }
        "_show_users" => {
            r##"
Show the registered users in a Meerschaum instance (default is the current instance).
"##
        }
        "_show_venvs" => {
            r##"
Print the available virtual environments in the current MRSM_ROOT_DIR.
"##
        }
        "_prune_venvs" => {
            r##"
Remove packages from virtual environments that no installed plugin needs.

A package is kept if `meerschaum` requested it, if an installed plugin requested it
or lists it in `required`, or if a kept package depends on it. Packages with no recorded
requester are never removed. Use `--dry` to only list what would be removed.

Examples:
    - `prune venvs --dry`
    - `prune venvs mrsm -y`
"##
        }
        "_show_version" => {
            r##"
Show the Meerschaum doc string.

Examples:
    - `show version`
    - `show version --nopretty`
"##
        }
        "_start_api" => {
            r##"
Start the API server.

Usage:
    `start api {options}`

Options:
    - `-p, --port {number}`
        Port to bind the API server to.

    - `-w, --workers {number}`
        How many worker threads to run.
        Defaults to the number of CPU cores or 1 on Android.
"##
        }
        "_start_connectors" => {
            r##"
Start polling connectors to verify a connection can be made.
"##
        }
        "_start_daemons" => {
            r##"
Start the Meerschaum CLI daemon processes.
"##
        }
        "_start_gui" => {
            r##"
Start the Meerschaum GUI application.
"##
        }
        "_start_jobs" => {
            r##"
Run a Meerschaum action as a background job.

To create a new job, pass the command arguments after `start job`.
To start a stopped job, pass the job name after `start job`.

You may also run a background job with the `-d` or `--daemon` flags.

Examples:

    Create new jobs:

      - `start job sync pipes --loop`
            Run the action `sync pipes --loop` as a background job.
            Generates a random name; e.g. 'happy_seal'.

      - `start api --daemon --name api-server`
            Run the action `start api` as a background job, and assign the job
            the name 'api-server'.

    Start stopped jobs:

      - `start job happy_seal`
            Start the job 'happy_seal'.

      - `start job --name happy_seal`
            Start the job 'happy_seal' but via the `--name` flag.
"##
        }
        "_start_mcp" => {
            r##"
Start an MCP (Model Context Protocol) server on stdin and stdout.

An MCP client spawns this as a subprocess and speaks JSON-RPC over the pipe,
so it exposes this Meerschaum installation with no API server and no token.
It runs with the privileges of the shell which launched it, the same as any
other Meerschaum command — use the API's `/mcp` endpoint instead when the
client is remote.

Usage:
    `start mcp`

Example client configuration:
    {"mcpServers": {"meerschaum": {"command": "mrsm", "args": ["start", "mcp"]}}}
"##
        }
        "_start_pipeline" => {
            r##"
Run a series of Meerschaum commands as a single action.

Add `:` to the end of chained arguments to apply additional flags to the pipeline.

Examples
--------

`sync pipes -i sql:local + sync pipes -i sql:main : -s 'daily'`

`show version + show arguments : --loop`

"##
        }
        "_start_webterm" => {
            r##"
Start the Meerschaum Web Terminal.

Options:
    - `-p`, `--port`
        The port to which the webterm binds.
        Defaults to 8765, and `--force` will choose the next available port.

    - `--host`
        The host interface to which the webterm binds.
        Defaults to '127.0.0.1'.

    - `-i`, '--instance'
        The default instance to use for the Webterm shell.
"##
        }
        "_start_worker" => {
            r##"
Start a CLI worker process. This is intended for internal use only.
"##
        }
        "_stop_daemons" => {
            r##"
Stop the Meerschaum CLI daemon.
"##
        }
        "_stop_jobs" => {
            r##"
Stop running jobs that were started with `-d` or `start job`.

To see running processes, run `show jobs`.
"##
        }
        "_sync_pipes" => {
            r##"
Fetch and sync new data for pipes.

Usage:
    - `--loop`
        - Sync indefinitely.
    - `--listen`
        - Keep the process alive after the initial sync pass
          (for subscription-style connectors which sync via callbacks).
    - `--min-seconds 10`
        - Wait 10 seconds between laps.
    - `--async`, `--unblock``
        - Spin up background threads for each pipe.
    - `--debug`
        - Print verbose messages.
"##
        }
        "_tag_pipes" => {
            r##"
Add or remove tags to registered pipes.
Prefix a tag with a leading underscore to remove it.

Note that the `--tags` flag applies to existing tags.
Specify the tags you wish to add or remove as positional arguments, e.g.:

```
mrsm tag pipes production sync-daily -c sql:main
mrsm tag pipes _production --tags production
```
"##
        }
        "_uninstall_packages" => {
            r##"
Uninstall PyPI packages from the Meerschaum virtual environment.

Example:
    `uninstall packages pandas numpy`
"##
        }
        "_uninstall_plugins" => {
            r##"
Remove installed plugins. Does not affect repository registrations.
"##
        }
        "_upgrade_meerschaum" => {
            r##"
Upgrade the current Meerschaum instance.
Optionally specify dependency groups.

Examples:
    - `upgrade meerschaum`
    - `upgrade meerschaum full`

"##
        }
        "_upgrade_packages" => {
            r##"
Upgrade and install dependencies.
If provided, upgrade only a dependency group, otherwise default to `full`.

Examples:
    upgrade packages
    upgrade packages full
"##
        }
        "_upgrade_plugins" => {
            r##"
Upgrade all installed plugins to the latest versions.
If no plugins are specified, attempt to upgrade all,
otherwise only upgrade the specified plugins.

Examples:

upgrade plugins
upgrade plugins noaa
"##
        }
        "_vacuum_pipes" => {
            r##"
Vacuum the target tables for the selected pipes.
"##
        }
        "_verify_packages" => {
            r##"
Verify the versions of packages.
"##
        }
        "_verify_pipes" => {
            r##"
Verify the contents of pipes, syncing across their entire datetime intervals.
"##
        }
        "_verify_plugins" => {
            r##"
Verify that all of the available plugins are able to be imported as expected.
"##
        }
        "_verify_rowcounts" => {
            r##"
Verify the contents of pipes, syncing across their entire datetime intervals.
"##
        }
        "_verify_venvs" => {
            r##"
Verify your virtual environments.
"##
        }
        "analyze" => {
            r##"
Analyze pipes' target tables to refresh the database planner's statistics.

This does not reclaim disk space; it helps the query planner choose better plans
after large syncs.
"##
        }
        "api" => {
            r##"
Send commands to a Meerschaum WebAPI instance.

Usage:
    `api [commands] {options}`

Examples:
    - `api [start, boot, init]`
        - Start the API server
        - **NOTE:** The command `start api` also starts the server.
    - `api show config`
        - Execute `show config` on the `main` api instance
    - `api main show config`
        - See above

If command is `start`, launch the Meerschaum WebAPI. If command is an api connector label,
    connect to that label. Otherwise connect to `main` api connector.

"##
        }
        "attach" => {
            r##"
Attach to a job, and prompt the user when blocking on input.

    Options:
        `attach [jobs, logs]`"##
        }
        "bootstrap" => {
            r##"
Launch an interactive wizard to bootstrap pipes or connectors.

Example:
    `bootstrap pipes`


    Options:
        `bootstrap [connectors, jobs, pipes, plugins]`"##
        }
        "clear" => {
            r##"
Clear pipes of their data, or clear the screen.

Usage:
- clear
    - Clear the screen.
- clear pipes --begin 2022-01-01
    - For all pipes, remove rows newer than or equal to `2022-01-01 00:00:00`.


    Options:
        `clear [cache, pipes]`"##
        }
        "compose" => r##"Manage an isolated Meerschaum environment with Meerschaum Compose."##,
        "compress" => {
            r##"
Compress pipes' target tables to reduce disk usage.

For TimescaleDB hypertables this enables native compression and installs a
compression policy so future synced chunks are compressed automatically.

    Options:
        `compress [pipes]`"##
        }
        "copy" => {
            r##"
Duplicate connectors or pipes.

Command:
    `copy {pipes, connectors}`

Example:
    `copy pipes`


    Options:
        `copy [connectors, pipes]`"##
        }
        "decompress" => {
            r##"
Decompress pipes' target tables, the inverse of `compress pipes`.

For TimescaleDB hypertables this removes the compression policy and converts compressed
chunks back to row-store so future synced chunks stay uncompressed.

    Options:
        `decompress [pipes]`"##
        }
        "deduplicate" => {
            r##"
Deduplicate pipes' tables, chunking across their datetime axes.

    Options:
        `deduplicate [pipes]`"##
        }
        "delete" => {
            r##"
Delete an element.

Command:
    `delete {config, pipes, plugins, users, connectors, jobs, venvs}`


    Options:
        `delete [cache, config, connectors, jobs, pipes, plugins, users, venvs]`"##
        }
        "drop" => {
            r##"
Drop pipe data (maintaining registration) or tables.

    Options:
        `drop [indices, pipes]`"##
        }
        "edit" => {
            r##"
Edit an existing element.

    Options:
        `edit [config, definition, jobs, pipes, plugins, tokens, users]`"##
        }
        "flush" => {
            r##"
Drop and resync pipes or indices.

    Options:
        `flush [indices, pipes]`"##
        }
        "index" => {
            r##"
Create pipes' indices.

    Options:
        `index [pipes]`"##
        }
        "install" => {
            r##"
Install Meerschaum plugins or Python packages.

    Options:
        `install [packages, plugins, required]`"##
        }
        "login" => {
            r##"
Log into a Meerschaum API instance.
"##
        }
        "os" => {
            r##"
Launch a subprocess and read its output to stdout.
"##
        }
        "partition" => {
            r##"
Repartition pipes' target tables to a new chunk (partition) width.

Only natively range-partitioned pipes are affected — those created on PostgreSQL / PostGIS,
MySQL / MariaDB, or MSSQL with `hypertable: True` — plus TimescaleDB hypertables. Set the new
width with `--chunk-minutes` (defaults to each pipe's `verify.chunk_minutes`, 30 days). On
TimescaleDB this changes the chunk interval for FUTURE chunks (existing chunks are unchanged);
on the other flavors the table is rebuilt at the new width (read, dropped, and re-synced).

Usage:
- partition pipes -m weather --chunk-minutes 10080
    - Rebuild the 'weather' pipes' tables to 7-day partitions.
- partition pipes -i sql:main --chunk-minutes 1440 -y
    - Rebuild all partitioned pipes on `sql:main` to 1-day partitions, skipping the prompt.
- partition pipes -t production
    - Repartition tagged pipes to each one's configured `verify.chunk_minutes`.


    Options:
        `partition [pipes]`"##
        }
        "pause" => {
            r##"
Pause running jobs.

    Options:
        `pause [jobs]`"##
        }
        "python" => {
            r##"
Launch a virtual environment's Python interpreter with Meerschaum imported.
You may pass flags to the Python binary by surrounding each flag with `[]`.

Usage:
    `python {commands}`

Examples:
    mrsm python
    mrsm python -c "print(mrsm.__version__)"
    mrsm python [-m pip -V]

Flags:
    `--nopretty`
    Open a plain Pyhthon REPL rather than ptpython.

    `--noask`
    Run the supplied Python code and do not open a REPL.

    `--venv`
    Run the Python interpreter from a virtual environment.
    Will not have Meercshaum imported.

    `--sub-args` (or flags surrounded by `[]`)
    Rather than run Python code, execute the interpreter with the given sub-flags
    (e.g. `mrsm python [-V]`)
"##
        }
        "register" => {
            r##"
Register new items (pipes, plugins, users).

Pipes and users reside on instances (`-i`), and plugins reside on repositories (`-r`).

    Options:
        `register [connectors, pipes, plugins, tokens, users]`"##
        }
        "reload" => {
            r##"
Reload the running Meerschaum instance.
"##
        }
        "restart" => {
            r##"
Restart stopped jobs which have not been manually stopped.
"##
        }
        "setup" => {
            r##"
Run the `setup()` function for Meerschaum plugins.

Usage:
    setup plugins noaa

    Options:
        `setup [plugins]`"##
        }
        "sh" => {
            r##"
Execute system commands.
"##
        }
        "show" => {
            r##"Show elements of a certain type.

Command:
    `show {option}`

Example:
    `show pipes`

    Options:
        `show [actions, arguments, columns, config, connectors, daemons, data, environment, help, jobs, logs, packages, partitions, pipes, plugins, rowcounts, schedules, sizes, tags, targets, tokens, users, venvs, version]`"##
        }
        "sql" => {
            r##"Execute a SQL query or launch an interactive CLI. All positional arguments are optional.

Usage:
    `sql {label} {method} {query / table}`

Options:
    - `sql {label}`
        Launch an interactive CLI. If {label} is omitted, use 'main'.

    - `sql {label} read [query / table]`
        Read a table or query as a pandas DataFrame and print the result.

    - `sql {label} exec [query]`
        Execute a query and print the success status.

Examples:
    - `sql`
        Open an interactive CLI for `sql:main`.

    - `sql local`
        Open an interactive CLI for `sql:local`.

    - `sql table`
        Read from `table` on `sql:main`
          (translates to `SELECT * FROM table`).

    - `sql local table`
        Read from `table` on `sql:local`.

    - `sql local read table`
        Read from `table` on `sql:local`.

    - `sql "SELECT * FROM table WHERE id = 1"`
        Execute the above query on `sql:main` and print the results.

    - `sql local exec "INSERT INTO table (id) VALUES (1)"
        Execute the above query on `sql:local`.
"##
        }
        "stack" => {
            r##"
Control the Meerschaum stack with Docker Compose.
Usage: `stack {command}`

Command: action[0]: default 'up'
    Docker Compose command to run. E.g. 'config' will print Docker Compose configuration
"##
        }
        "start" => {
            r##"
Start subsystems (API server, background job, etc.).

    Options:
        `start [api, connectors, daemons, gui, jobs, mcp, pipeline, webterm, worker]`"##
        }
        "stop" => {
            r##"
Stop running jobs.

    Options:
        `stop [daemons, jobs]`"##
        }
        "sync" => {
            r##"
Fetch and sync data for pipes.

Usage:
    - `--loop`
        - Sync indefinitely.
    - `--listen`
        - Keep the process alive after the initial sync pass
          (for subscription-style connectors which sync via callbacks).
    - `--min-seconds 10`
        - Wait 10 seconds between laps.
    - `--async`, `--unblock``
        - Spin up background threads for each pipe.
    - `--debug`
        - Print verbose messages.

    Options:
        `sync [pipes]`"##
        }
        "tag" => {
            r##"
Edit an existing element.

    Options:
        `tag [pipes]`"##
        }
        "uninstall" => {
            r##"
Uninstall Meerschaum plugins or Python packages.

    Options:
        `uninstall [packages, plugins]`"##
        }
        "upgrade" => {
            r##"
Upgrade Meerschaum, plugins, or packages.

Command:
    `upgrade {option}`

Example:
    `upgrade meerschaum`

    Options:
        `upgrade [meerschaum, packages, plugins]`"##
        }
        "prune" => {
            r##"
Prune virtual environments of packages no installed plugin needs.

Command:
    `prune {option}`

Example:
    `prune venvs --dry`

    Options:
        `prune [venvs]`"##
        }
        "vacuum" => {
            r##"
Vacuum pipes' target tables to reclaim dead-tuple disk space.

For PostgreSQL-family tables this runs `VACUUM` (optionally `VACUUM FULL` with `--full`);
other flavors fall back to their respective space-reclaiming mechanisms.

    Options:
        `vacuum [pipes]`"##
        }
        "verify" => {
            r##"
Verify the states of pipes, packages, and more.

    Options:
        `verify [packages, pipes, plugins, rowcounts, venvs]`"##
        }
        _ => return None,
    })
}
