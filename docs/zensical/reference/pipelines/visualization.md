# 📊 Visualization

The engine emits one typed event per transition, and everything you see is drawn from that stream or from the recorded run:

`run_started`, `node_planned`, `node_started`, `chunk_started`, `chunk_finished`, `node_finished`, `node_failed`, `node_skipped`, `node_rebuild_pending`, `watermark_advanced`, `stream_flushed`, `children_triggered`, `stream_restarted`, `run_finished`.

## 🖥️ In the terminal

`mrsm sync pipes` prints each node's result as it finishes and a summary of the run (the output shown throughout this section). A run with `--parents` / `--children` and more than one node ends with a static **Gantt chart** of the actual run and the plan (a plain `sync pipes` keeps v4's output, so it prints none, and neither does `--nopretty`). On a terminal the run also prints one progress line per node as it starts and finishes (`▶`, `✔`, `✘`, `○`). `mrsm show run <id>` draws the chart of any recorded run:

```text
run 01JW9GANTT  4 nodes  4 workers  wall 1m40s  planned 1m15s  critical path: raw → clean
raw    ██████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  40s    +12,301 ins                  ✔
       ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒                                        30s    planned
clean  ░░░░░░░░░░░░░░░░░░░░░░███████████████████████████░░░░░  50s    +12,290 ins  ~11 upd         ✔
                       ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒               45s    planned
geo    ░░░░░░░░░░░░░░░░░░░░░░████                              9s     error: connection reset      ✘
                       ▒▒▒▒▒▒▒▒▒▒▒                             20s    planned
daily  ·                                                              skip                         ○
█ actual  ▒ planned
```

Each node has two lanes on one time axis (which spans the longer of the run's wall time and the planned makespan):

- **Actual** (first line): `█` is when the node ran, `░` the time around it, `·` a node that did not run (`skip`, `upstream_failed`, `rebuild_pending`, `observe` or never ran). The right-hand columns are the duration, a count summary (`+N ins`, `~N upd`, `+N ups`, `-N del`) or `error: …`, and ✔ / ✘ / ○.
- **Planned** (second line, `▒`): the bar the plan drew before the run started. It starts at the node's earliest start in the critical-path schedule and **ends** after its planned duration: the moving average (EWMA, `alpha` 0.3) of the node's earlier successful runs with the same action, else of all its earlier successful runs, else **`system.engine.default_duration_seconds` (30 seconds)**. A node planned not to sync (a `skip`) has no planned lane; a child planned against parents that are about to change (a provisional `skip`) is planned as an `incremental`, because it usually syncs once its parents finish.

The header adds the planned makespan (`planned 1m15s`) next to the actual wall time. A first run has no history, so every planned bar is 30 seconds long. For a run read back from the instance (`show run <id>`, the console), the planned duration is recomputed from the history before that run (the run's own rows store the planned start and slack, not the duration).

`mrsm show graph` draws a selection as a top-down tree (a pipe with several parents is drawn once, under the first parent the walk reaches, and referred to elsewhere):

```text
Pipe graph on sql:local: 4 pipes, 4 edges, 1 root, depth 3

● sql:local/a
├── ◆ sql:local/b
│   └── ○ sql:local/d  (2 parents)
└── ◆ sql:local/c
    └── ↗ sql:local/d (shown above)

● root   ◆ has children   ○ leaf   ↗ shown above
```

Pipes use the colors `Pipe(...)` has in `show pipes` (connector, metric, location, instance); the guides and notes are dim. `★` marks the pipes you selected, `┄┄` in place of `──` an edge that exists only because of a `{{ Pipe() }}` reference, and `lookback`, `lookahead` and `axis` of an edge follow the pipe. A cycle (flat selections only; a `--parents` / `--children` selection that contains one fails with the cycle path) is drawn with its real edges, its members marked `⟲`, and a `⟲ cycle: a → b → a` note; the JSON gains a `cycles` key. Unconnected pipes are listed together at the end, and a pipe with more than 12 children collapses to `… and N more children` (`--full` lists them all; the line says how many hidden children have descendants of their own). Lines are cut to the terminal width, `formatting:unicode: false` gives an ASCII tree, and colors appear only on a terminal.

`show graph` accepts the usual filters plus `--parents` and `--children`. `mrsm show runs [--last N]` lists the recorded runs, newest first:

```text
run                         started (UTC)        status     wall  ok/nodes  targets
01M4VPBBSG8NMTDT2NJYE98D2Q  2026-10-13 22:32:53  succeeded  0s    3/3       sql:local/daily
```

`--nopretty` prints JSON for `plan pipes`, `show graph`, `show runs` and `show run`.

!!! note "No full-screen view yet"

    The terminal view is line-oriented: progress lines while the run is going and the Gantt chart at the end. A full-screen live chart is not part of this build.

## 🌐 In the web console

Start the API (`mrsm start api`) and open **`/dash/pipelines`** (the console needs `pip install meerschaum[dash]`). It reads the same instance-backed engine state described in [Engine State](/reference/pipelines/state/) and never writes.

- **Dependency graph.** Your pipes as a Plotly figure, each node colored by the action the engine plans for it (`skip`, `incremental`, `backfill`, `rebuild`, ...), with an edge table showing the ledger lag (how many unconsumed entries a child still has to process).
- **Gantt chart per run.** Planned and actual bars side by side (planned bars end after the planned duration described above), plus a table of each node's action, status and planned and actual start. (The plan's critical path and slack are recorded with each run and are drawn in the terminal Gantt chart.)
- **Runs table.** Filter by run ID, pipe and status.
- **Parents / Children tabs** in the "Parents & Children" section of every pipe card on the pipes page.

The page updates itself (it refreshes about every 15 seconds). Select a run with the link in the runs table (`/dash/pipelines?run_id=…`) to see its Gantt chart. State lives in the URL, so a run or filter is shareable.

The console is available through `mrsm start api` and is covered in [Web Console](/reference/api-instance/web-console/).

## 🐍 Live events in Python

`Pipeline.sync()` and `pipe.sync(parents=..., children=...)` take an **`on_event`** callable. It receives every event of the run as it happens, in order, as a plain `dict`:

```python
import meerschaum as mrsm

def on_event(event):
    if event['event'] == 'node_finished':
        print(event['pipe_ref'], event['result']['inserted'], 'inserted')
    elif event['event'] == 'chunk_finished':
        print('  chunk', event['chunk'], event['begin'], event['end'])

run = mrsm.Pipeline(pipe, parents=True).sync(on_event=on_event)
run = pipe.sync(children=True, on_event=print)
```

Every event has the same envelope, and the event's own fields at the top level:

| Key | Type | Meaning |
|---|---|---|
| `event` | `str` | The event name (one of the names at the top of this page). |
| `run_id` | `str` | The run (the same as `run.run_id`). |
| `seq` | `int` | Emission order within the run, from 0. |
| `ts` | `datetime` | When it happened (timezone-aware, UTC). |
| `pipe_ref` | `str` or `None` | `instance|connector|metric|location` of the node; `None` for `run_started` / `run_finished`. |

| Event | Fields |
|---|---|
| `run_started` | `nodes`, `workers`, `targets` |
| `node_planned` | `action`, `reason`, `provisional` (sent again when a node is re-planned after its parents) |
| `node_started` | `action`, `attempt` |
| `chunk_started` | `chunk`, `begin`, `end`, `source` (`window` or `fetch`), `rows`, `total` |
| `chunk_finished` | `chunk`, `success`, `message`, `source`, `rows`, `inserted`, `updated`, `upserted`, `begin`, `end`, `duration_ms` |
| `node_finished` | `result` (`success`, `message`, `inserted`, `updated`, `upserted`, `deleted`, `changed_intervals`, `duration`, `begin`, `end`) |
| `node_failed` | `error`, `traceback`, `attempt`, `will_retry` |
| `node_skipped` / `node_rebuild_pending` | `reason` |
| `watermark_advanced` | `from`, `to` |
| `stream_flushed` | `rows`, `bytes`, `trigger`, `success`, `begin`, `end`, `inserted`, `updated`, `upserted`, `message` |
| `children_triggered` | `run_id`, `nodes`, `held`, `flushes` |
| `stream_restarted` | `error` |
| `run_finished` | `success`, `message` |

A bound (`begin`, `end`, `from`, `to`) is `{"Datetime": "2024-01-01T00:00:00Z"}` or `{"Int": 5}`; fields that do not apply are absent. New fields may be added; read them with `event.get(...)`.

**Threads and the GIL.** The callback runs on the thread that called `sync()`, between events. The sync work runs on the engine's own threads without holding the GIL, so other Python threads keep running and a slow callback only delays later callbacks: events queue up and none is dropped.

**Errors.** If the callback raises an `Exception`, the run is **not** stopped: the exception is reported once with `warnings.warn` (including its traceback), the callback receives no further events, the run result carries it as `run.callback_error`, and `run.warnings` gets a line about it. A `KeyboardInterrupt` (raised in the callback, or CTRL-C while the run waits) **cancels** the run: in-flight node syncs are dropped as if the process had crashed (the engine's write ordering keeps every table and cursor consistent, and the next run picks up where it stopped), and the `KeyboardInterrupt` propagates.

`run.events()` (and `Pipeline.get_events(run_id)`) still read a run's *persisted* events back from the instance after the fact, at the granularity of `system.engine.events`; those rows are shaped `{ts, run_id, seq, pipe_ref, event, payload}`.
