# 📊 Visualization

The engine emits one typed event per transition, and everything you see is drawn from that stream or from the recorded run:

`run_started`, `node_planned`, `node_started`, `chunk_started`, `chunk_finished`, `node_finished`, `node_failed`, `node_skipped`, `node_rebuild_pending`, `watermark_advanced`, `stream_flushed`, `children_triggered`, `stream_restarted`, `run_finished`.

## Running a Pipeline in the console

Open `/dash/pipelines` and select a registered Pipeline. **Workspace** contains the editable graph;
**Runs** contains execution history, the selected timeline and its horizontally scrollable stages table.
The two views keep their state when switched. On phones, **Graph**, **Pipe details** and **Output**
switch between workspace panes; selecting a node opens its details. **Tools** groups analysis and
registration controls, while **Find pipes**, **Pipeline members** and **View options** reveal less frequent
actions. You may also select a Pipeline by clicking its named boundary. Shared Pipes
may appear in several groups. Select a Pipe to open its inspector immediately; Query, Data / Plan,
Dependencies, Recent syncs and Parameters load independently. **Resolve symlinks** changes displayed
SQL in the selected Pipe's instance context, preserving the stored definition.

**Fullscreen** keeps the canvas, inspector, editor and output dock together. Escape returns to
the page. Drag the background to pan without selecting page text; zoom, Fit and Center selected
provide keyboard-accessible controls. Search and status filters highlight matching Pipes, and
Collapse groups provides an overview. Ctrl/Cmd/Shift-click selects several cards. Alt-drag a Pipe
to arrange it, then **Save layout** in the canvas toolbar persists coordinates with revision checks. Zoom and pan
remain personal to the browser session.

Right-click a Pipe or use its inspector to **Sync this Pipe**. **Sync options** chooses parent or
child traversal and optional bounds for that operation. These controls launch one execution through the same
authenticated server endpoint; they do not run the entire registered Pipeline implicitly.
The canvas output drawer opens from each node’s output icon and keeps completed single-Pipe executions.
It opens at full size, renders ANSI colors, and follows new output until you scroll upward.
Close it with the button on the right. **Cancel sync** appears only for a running or paused execution. Expanded scopes use exact
run/Pipe engine event identities rather than assigning interleaved stdout by matching text.
Arbitrary plugin stdout from a concurrent Pipeline run remains in **Pipeline command output**;
per-Pipe panes provide structured events and results. Run an individual Pipe for its dedicated
stdout stream.
**Run now** executes the saved Pipeline and follows its allocated run ID. History remains
separate from editable current state.

**Pipeline members** manages explicit membership. Choose a configured instance from the dropdown
and select existing Pipes, or open the separate new-Pipe registration form. Draft membership and
traversal edits support Undo, Redo, Discard and Save. Use **Open Pipe editor** in the inspector to
change shared Pipe parameters. The inspector’s section selector loads Query, Data / Plan,
Dependencies, Recent syncs and Parameters independently.

**Analyze** detects traversal members, shared Pipes and proposed `pipeline-{name}` tag changes.
Review the proposal, then explicitly **Apply proposed tags**. Stale proposals require analysis and
approval again. Ordinary saves and syncs do not apply tags.

The canvas's **Plan** describes dependency order, readiness, actions and bounds without a time axis.
The **Actual execution timeline** uses observed starts and finishes, extends live running bars and
shows overlap and elapsed time. It omits forecast bars and the image-download control. Engine
estimates may use prior durations or a 30-second default; these are scheduling hints, not deadlines.
In **Runs**, **Filters** and **Sync activity** reveal run/Pipe filters and recorded events.
A stopped execution worker cannot always persist its final engine event; retained Job metadata
reconciles unfinished runs to their actual stop time, including after reloading the page.
**Tools → Manage pipeline** also creates
ordinary scheduled Jobs with their own execution settings.

## 🖥️ In the terminal

`mrsm sync pipes` prints each node's result and a run summary. Graph selections also report
node transitions as they start and finish. `mrsm show run <id>` reads the recorded run; actual
elapsed time and observed execution intervals are the primary Timeline view. `plan pipes` and
`plan pipelines` explain the next actions and bounds independently of that execution history.
`--nopretty` prints machine-readable plan and run information.

Scheduling forecasts can use a per-action historical moving average (alpha 0.3), other successful
action history, or `system.engine.default_duration_seconds` (30 seconds). These estimates omit
worker contention and can greatly exceed a fast first run. They are planning hints; the console
therefore does not overlay them on the actual-time chart.

`mrsm show graph` draws a selection as a top-down tree (a pipe with several parents is drawn once, under the first parent the walk reaches, and referred to elsewhere):

```text
Pipe graph on sql:local: 4 pipes, 4 edges, 1 root, depth 3

● Pipe('sql:local', 'a')
├── ◆ Pipe('sql:local', 'b')
│   └── ○ Pipe('sql:local', 'd')  (2 parents)
└── ◆ Pipe('sql:local', 'c')
    └── ↗ Pipe('sql:local', 'd') (shown above)

● root   ◆ has children   ○ leaf   ↗ shown above
```

Pipes use the colors `Pipe(...)` has in `show pipes` (connector, metric, location, instance); the guides and notes are dim. `★` marks the pipes you selected, `┄┄` in place of `──` an edge that exists only because of a `{{ Pipe() }}` reference, and `lookback`, `lookahead` and `axis` of an edge follow the pipe. A cycle (flat selections only; a `--parents` / `--children` selection that contains one fails with the cycle path) is drawn with its real edges, its members marked `⟲`, and a `⟲ cycle: a → b → a` note; the JSON gains a `cycles` key. Unconnected pipes are listed together at the end, and a pipe with more than 12 children collapses to `… and N more children` (`--full` lists them all; the line says how many hidden children have descendants of their own). Lines are cut to the terminal width, `formatting:unicode: false` gives an ASCII tree, and colors appear only on a terminal.

`show graph` accepts the usual filters plus `--parents` and `--children`. `mrsm show runs [--last N]` lists the recorded runs, newest first:

```text
run                         started (UTC)        status     wall  ok/nodes  targets
01M4VPBBSG8NMTDT2NJYE98D2Q  2026-10-13 22:32:53  succeeded  0s    3/3       Pipe('sql:local', 'daily')
```

`--nopretty` prints JSON for `plan pipes`, `show graph`, `show runs` and `show run`.

!!! note "No full-screen view yet"

    The terminal view is line-oriented: progress lines while the run is going and the Gantt chart at the end. A full-screen live chart is not part of this build.

## 🌐 In the web console

Start the API (`mrsm start api`) and open **`/dash/pipelines`**. The console needs
`pip install meerschaum[dash]`. The workspace reads [Engine State](/reference/pipelines/state/)
and writes definitions or shared Pipe facts only through explicitly named controls described above.
The Pipe cards on the Pipes page retain their Parents / Children tabs.

The engine's *ledger* is its record of changed parent data, used to find updates a child still needs to process, including changes to older rows. It remains part of syncing; it is not a separate lag metric on the page. A *watermark* is the latest value of a pipe's datetime or ordering column. Without a previous dependency-sync record, the engine compares parent and child watermarks. When a parent is ahead, the child reads from its own latest value minus its backtrack window, up to the earliest parent watermark. The canvas calls this **New parent data available**.

Sync history refreshes about every 15 seconds. Use the refresh button to update the pipeline canvas. Select a run with the link in the runs table (`/dash/pipelines?run_id=…`) to see its actual execution timeline. State lives in the URL, so a run or filter is shareable.

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


After selecting a saved Pipeline, **Run now** opens its command output directly in the
canvas. **View output** reopens that execution's retained logs after closing the drawer.
Use node output icons for individual Pipe executions and **Runs** for historical timelines.

**Manage**, beside the Pipeline selector, contains definition and schedule controls.
Deleting a Pipeline asks you to confirm its name and registry. Member Pipes, their data,
and execution history are kept; remove referencing scheduled Jobs before deleting a definition.
