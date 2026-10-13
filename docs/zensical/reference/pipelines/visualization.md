# 📊 Visualization

The engine emits one typed event per transition, and everything you see is drawn from that stream or from the recorded run:

`run_started`, `node_planned`, `node_started`, `chunk_started`, `chunk_finished`, `node_finished`, `node_failed`, `node_skipped`, `node_rebuild_pending`, `watermark_advanced`, `stream_flushed`, `children_triggered`, `stream_restarted`, `run_finished`.

## 🖥️ In the terminal

`mrsm sync pipes` prints each node's result as it finishes and a summary of the run (the output shown throughout this section). The engine can also draw a finished run as a static **Gantt chart**, one line per node, bars scaled to the run's wall time:

```text
run 01JA…  6 nodes  4 workers  wall 2m14s  critical path: raw → clean → rollup
raw_events     ████████░░░░░░░░░░░░░░░░░░  48s   +12,301 ins          ✔
weather        ██████░░░░░░░░░░░░░░░░░░░░  31s   +1,044 ins           ✔
clean_events   ░░░░░░░░██████████░░░░░░░░  40s   +12,290 ins  ~11 upd ✔
geo_join       ░░░░░░░░██▌                 9s    error: …             ✘
daily_rollup   ░░░░░░░░░░░░░░░░░░████████  46s   +31 ups              ✔
report         ·                                 upstream_failed      ○
```

`█` is when the node ran, `░` the time around it, `·` a node that did not run (`skip`, `upstream_failed`, `rebuild_pending`, `observe` or never ran). The right-hand columns are the duration, a count summary (`+N ins`, `~N upd`, `+N ups`, `-N del`) or `error: …`, and ✔ / ✘ / ○.

The engine also renders a selection as an indented DAG, each node under its deepest parent, targets marked with `*`:

```text
sql:src/raw
  sql:local/clean  ← sql:src/raw
    sql:local/daily*  ← sql:local/clean
```

!!! warning "Status in this pre-release"

    The Gantt and DAG renderers are part of the engine library, and the console page below uses the same run data. The terminal actions that print them (`mrsm show runs`, `mrsm show run <id>`, `mrsm show graph`, and the live full-screen view on a TTY) are not yet wired in this build.

## 🌐 In the web console

Start the API (`mrsm start api`) and open **`/dash/pipelines`**. It reads the same instance-backed engine state described in [Engine State](/reference/pipelines/state/) and never writes.

- **DAG view.** The dependency graph of your pipes, with each node's status and the ledger lag on each edge (how many unconsumed entries a child still has to process).
- **Gantt chart per run.** Planned and actual bars side by side, plus a table of each node's action, status and planned and actual start. (The plan's critical path and slack are recorded with each run and are drawn in the terminal Gantt chart.)
- **Runs table.** Filter by run ID, pipe and status.
- **Parents / Children tabs** on every pipe card on the pipes page.

The page updates itself (it re-requests its graph panel about every 15 seconds, and the console streams engine events to open pages). Select a run with the link in the runs table (`/dash/pipelines?run_id=…`) to see its Gantt chart. State lives in the URL, so a run or filter is shareable.

The console is available through `mrsm start api` and is covered in [Web Console](/reference/api-instance/web-console/).
