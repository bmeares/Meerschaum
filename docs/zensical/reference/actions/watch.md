# 📺 Watch and Shell Shortcuts

`show` prints something once; `watch` keeps it on screen as it changes. `watch run` and `watch jobs` are full-screen views (new in v5) that take the whole terminal and give it back as it was when you leave; `watch logs` is another name for `show logs`.

| Command | Shell key | What it shows |
|---|---|---|
| `watch run [RUN_ID]` | <kbd>F2</kbd> | An engine run: a live Gantt chart, its nodes and its events. |
| `watch jobs [NAME ...]` | <kbd>F3</kbd> | Background jobs and the selected job's log, with keys to start, stop and pause them. |
| `watch logs [NAME ...]` | | The logs of jobs as they are written, in the terminal's own scrollback: the same as `show logs`. |

Every view shares these keys:

| Key | Does |
|---|---|
| <kbd>q</kbd>, <kbd>Esc</kbd>, <kbd>Ctrl</kbd>+<kbd>C</kbd> | Leave the view. |
| <kbd>?</kbd> | List the view's keys. |
| <kbd>↑</kbd> <kbd>↓</kbd> (<kbd>k</kbd> <kbd>j</kbd>) | Select a row. |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd>, <kbd>End</kbd> | Scroll the log, follow the newest lines again. |

## `watch run`

```bash
mrsm watch run                # the newest run (by start time)
mrsm watch run 01JA...        # one run (IDs are listed by `show runs`)
```

Without a `RUN_ID`, `watch run` opens the run that started last, finished or not. To watch an older run that is still going, pass its ID from `show runs`.

The monitor reads the events the Syncing Engine persists on the instance (`system.engine.events`, every event by default), so the run can be in any process on any host: a `sync pipes` in another terminal, a job, a compose project or an API server. It shows:

- **A header**: the run's status, wall time, node count, workers and how many nodes are running, done, failed and waiting.
- **The timeline**: one bar per node on the run's observed time axis (`█` while it syncs, `░` while it waited). It is the live form of the chart `show run RUN_ID` prints. <kbd>p</kbd> shades each node's forecast (`▒`, the average of its earlier runs); forecasts never stretch the axis.
- **The nodes**: state (`planned`, `running`, `retrying`, `succeeded`, `failed`, `skipped`, `upstream_failed`, `rebuild_pending`, `observed`), action (`root`, `incremental`, `backfill`, `rebuild`, ...), chunk progress (`3/8` planned windows, or the fetched chunks), rows inserted and updated, rows per second, attempts (with retries) and duration.
- **The events**: `node_started`, `chunk_finished`, `node_failed`, ... as they happen.

<kbd>Enter</kbd> opens the selected node's details (its reason, window, counts, timestamps and the sync's message or error); <kbd>Tab</kbd> moves the arrow keys to the event log.

When a `sync pipes` in the same process opens the monitor itself, it reads the engine's events directly and <kbd>d</kbd> returns to its inline output.

A run whose process was killed (`kill -9`, out of memory, a lost host) is never marked finished and keeps saying `running`. When a running run has had no new events for 10 minutes, the footer says its process may have died.

## `watch jobs`

```bash
mrsm watch jobs               # the jobs of the default executor
mrsm watch jobs -e systemd
mrsm watch jobs my-job other-job
```

The list shows each job's status, when its process began and ended (UTC), its schedule (`-s` / `--schedule`), its executor and its command; it is re-read every two seconds. The lower pane follows the selected job's log as it is written, the same lines `show logs` prints.

| Key | Does |
|---|---|
| <kbd>s</kbd> | Start the selected job (or resume it when paused). |
| <kbd>x</kbd> | Stop the selected job. |
| <kbd>p</kbd> | Pause the selected job. |
| <kbd>r</kbd> | Read the list again now. |

Each command asks first (`Stop job 'my-job'? [y/N]`); its result is shown at the bottom.

## Without a terminal

A view needs a terminal on stdin and stdout. When there is none (a pipe, a file, a job, CI), with `--nopretty`, `MRSM_NONINTERACTIVE` or `formatting:ansi` off, `watch` prints lines instead:

- `watch run` prints the run's events one per line (`HH:MM:SS  node  event`) until the run ends, and its result is the run's (the exit code is 1 for a failed run). With `--nopretty` every line is the event as JSON.
- If the run has had no new events for 10 minutes and is still not finished, `watch run` gives up and fails: the run's process may have died. `--timeout SECONDS` sets that limit (`--timeout 0` waits for good). A long node that syncs one big chunk can be quiet for a while; give it a larger `--timeout`.
- `watch jobs` prints `show jobs`.

```bash
mrsm watch run --nopretty | jq -c 'select(.event == "node_failed")'
```

`NO_COLOR` keeps the views and draws them without colors; `formatting:unicode: false` draws them with ASCII borders and bars.

## Shell shortcuts

In the shell, <kbd>F2</kbd> opens `watch run` and <kbd>F3</kbd> `watch jobs`. The line you were typing is kept: leave the view and carry on where you were. The views run on the shell's instance (`instance sql:other`) and executor.

Bind other keys, or other commands, under `shell:keybindings` (`mrsm edit config shell`):

```yaml
shell:
  keybindings:
    f2: watch run          # the default
    f3: null               # remove a default
    ctrl-g: watch jobs -e systemd
    f5: show pipes         # any command line
```

Keys are `f1` to `f24` or a character with `ctrl-` or `alt-` (`c-`, `a-`), optionally with `shift-`. A command that is not a view runs as if typed, but without clearing the screen or entering the history: its output (or its error, such as `watch run` on an instance with no runs) is printed where the prompt was, and the prompt comes back below it with your line.

!!! tip "Terminals"
    The views use the alternate screen and raw mode through crossterm, as the shell does. They are tested in a pseudo-terminal and inside tmux on Linux. The web console's terminal (`/webterm`, xterm.js), screen, and the Windows terminals support the same features, but have not been tested yet. Some terminals take <kbd>F2</kbd> / <kbd>F3</kbd> for themselves: bind other keys, or type `watch run` / `watch jobs`.
