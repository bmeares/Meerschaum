# 🕸️ Pipelines

> New in v5. The **Syncing Engine** understands how your pipes depend on one another. It decides which pipes need to run, how wide each sync must be, runs independent branches in parallel, and records what it did.

A plain `mrsm sync pipes` still works exactly as it did in v4: every selected pipe syncs on its own, nothing is capped, nothing is rebuilt. The engine only takes over when you ask for the graph (`--parents`, `--children`) or run in [continuous mode](/reference/pipelines/continuous/) with a selection that has edges. Nothing here changes how a pipe is defined.

!!! note "Not the same as `start pipeline`"

    A *pipeline* here is a slice of the pipe graph. The `start pipeline` action, which chains several actions with `:`, is unrelated and unchanged.

| Page | What it covers |
|---|---|
| **This page** | The pipe graph, selecting a slice of it, the plan, failures and resuming |
| [Engine State](/reference/pipelines/state/) | Run history, the dirty-range ledger, cursors, leases and the `system.engine.*` settings |
| [Continuous Mode and Streams](/reference/pipelines/continuous/) | `--loop`, `--listen`, micro-batching and triggering children |
| [Visualization](/reference/pipelines/visualization/) | The Gantt chart, the DAG view and the `/dash/pipelines` page |

## 🌳 The Pipe Graph

Each pipe is a **node**. Edges point from a parent to the pipe that reads it. Edges come from three places, combined and de-duplicated:

1. **`parents` / `parent`** in a pipe's [parameters](/reference/pipes/parameters/): an edge from the parent to this pipe.
2. **`children` / `child`**: an edge from this pipe to the child. (v4 never recorded the reverse direction. The graph builder looks at both sides.)
3. **`{{ Pipe(...) }}` references** inside a pipe's `sql` definition or any parameter value. These are *implicit edges*: you do not have to declare anything.

`reference` / `references` only mean parameter inheritance. They are **not** data edges.

For example, three pipes where each one reads the previous one through `{{ Pipe() }}`:

```python
import meerschaum as mrsm

inst = 'sql:local'
raw = mrsm.Pipe('sql:src', 'raw', instance=inst, parameters={
    'columns': {'datetime': 'ts', 'id': 'site'},
    'dtypes': {'ts': 'datetime'},
    'sql': 'SELECT * FROM readings',
    'tags': ['demo'],
})
clean = mrsm.Pipe('sql:local', 'clean', instance=inst, parameters={
    'columns': {'datetime': 'ts', 'id': 'site'},
    'dtypes': {'ts': 'datetime'},
    'sql': "SELECT ts, site, temp FROM {{ Pipe('sql:src', 'raw', instance='sql:local') }} WHERE temp IS NOT NULL",
    'tags': ['demo'],
})
daily = mrsm.Pipe('sql:local', 'daily', instance=inst, parameters={
    'columns': {'datetime': 'day', 'id': 'site'},
    'dtypes': {'day': 'datetime'},
    'sql': "SELECT date(ts) AS day, site, AVG(temp) AS avg_temp "
           "FROM {{ Pipe('sql:local', 'clean', instance='sql:local') }} GROUP BY 1, 2",
    'tags': ['demo'],
})
for pipe in (raw, clean, daily):
    pipe.register()
```

The engine sees `raw → clean → daily` without any `parents` key.

A few rules:

- A reference to a pipe that does not exist is a **warning**, not an error. Stale references are common in long-lived instances.
- A `{{ Pipe() }}` reference from a pipe to itself (the incremental `MAX(ts)` pattern) is ignored. Declaring a pipe as its own parent is a warning.
- A **cycle** is an error that prints the cycle path. It only matters if the cycle is inside your selection. In a flat `sync pipes` a cycle becomes a warning and the pipes run in registration order.
- Edges may cross instances. The child's ledger entries and cursor live on the **child's** instance. If a parent's instance is unreachable, the child is marked `upstream_failed`.
- Set `system.engine.implicit_edges: false` to ignore `{{ Pipe() }}` references and use only `parents` / `children`.

## 🎯 Selecting a Slice

The usual `-c`, `-m`, `-l`, `-t` and `-i` filters pick the **targets**. Add the graph flags to pull in their neighbours:

| Flag | Meaning |
|---|---|
| `--parents` | Sync every ancestor of the targets first, in dependency order, then the targets |
| `--parents N` | Only the `N` nearest generations of ancestors (`--parents 1` is the direct parents) |
| `--children` | Sync the targets, then propagate to every descendant |
| `--children N` | Only `N` generations of descendants |

The two combine: `--parents --children` syncs the whole connected slice. Ancestors that lie outside your selection are synced only because you asked for them; ancestors you did *not* ask for are never touched.

```bash
mrsm sync pipes -i sql:local -c sql:local -m daily --parents
```

The first of the three result blocks:

```text
✅ Finished syncing Pipe('sql:src', 'raw', instance='sql:local'):
   Synced 1 chunk to Pipe('sql:src', 'raw', instance='sql:local')
   (1 succeeded, 0 failed):

   2024-01-01 00:00:00.000000 - 2024-01-02 00:00:00.000000
   Inserted 3, updated 0 rows.
```

The first run walks the chain: `raw` first, then `clean`, then `daily`, each in the order its parents finish. Independent branches run in parallel, up to `system.engine.workers` (default 4) or `-w`.

Run it a second time and nothing upstream changed, so the engine **skips** the children:

```text
 🎉 It took 0.28 seconds to sync 1 pipe
    (1 succeeded, 0 failed).
```

Only `raw` is a *root* (it has no parent in the selection), so it syncs the normal v4 way and checks its source. `clean` and `daily` were skipped because their cursors are caught up (see [Engine State](/reference/pipelines/state/)).

!!! note "Flags and `--help`"

    `--parents`, `--children` and `--resume` are parsed ahead of the v4 argument schema, so they do not appear in `mrsm sync pipes --help`. They work with every other `sync pipes` flag (`--begin`, `--end`, `-w`, `--yes`, `--nopretty`, ...).

### Python

```python
pipe.sync()                          # v4 behavior, unchanged
pipe.sync(parents=True)              # ancestors in dependency order, then this pipe
pipe.sync(parents=1)                 # direct parents only
pipe.sync(children=True)             # this pipe, then every descendant
pipe.sync(parents=True, children=True)
```

`mrsm.Pipeline` selects a set of target pipes with traversal depths and is not stored anywhere. A saved pipeline is just a [job](/reference/background-jobs/) whose arguments include `--parents` / `--children` (and `mrsm compose sync pipes --parents` works the same inside a compose project):

```python
pipeline = mrsm.Pipeline(pipe, parents=True, children=False)   # or Pipeline([p1, p2]) / Pipeline(tags=['demo'])
plan = pipeline.plan()           # dry run, no data movement
run = pipeline.run(workers=8)
```

!!! warning "Status in this pre-release"

    The engine is wired to `mrsm sync pipes` (above). The Python entry points `pipe.sync(parents=, children=)` and `mrsm.Pipeline`, and the actions `mrsm plan pipes`, `mrsm show graph` and `mrsm show runs`, are specified (design §5.2) but not yet wired in this build. Until they land, use the `sync pipes` flags to run a slice and read [Engine State](/reference/pipelines/state/) for the recorded plan and history.

## 🧮 What the Planner Decides

For every node in the selection the planner picks one **action**. The ledger cursors (see [Engine State](/reference/pipelines/state/)) are the primary signal; watermarks (a pipe's sync time) are the fallback.

| Action | When | Sync window |
|---|---|---|
| `root` | The node has no parent in the selection | v4 semantics: `begin` is the sync time minus the backtrack |
| `skip` | No unconsumed ledger entries on any in-selection parent, no parent watermark advanced since the cursor moved, and the pipe's definition is unchanged | none |
| `incremental` | A parent's watermark advanced and nothing earlier is dirty | from the child's own watermark minus the backtrack, up to the minimum of its parents' watermarks |
| `backfill` | A parent changed rows before the child's tail (late or corrected data) | exactly the dirty ranges, widened to the child's chunk grid |
| `rebuild` | The pipe's `sql`, `fetch` config or `dtypes` changed | everything: clear, then a full sync (needs `--yes`, see below) |
| `observe` | The node is a [stream source](/reference/pipelines/continuous/#streaming-sources) | none; the watermark and ledger are read only |

Two more statuses describe nodes that did not run: `upstream_failed` and `rebuild_pending`.

Each decision is recorded with a human-readable reason. After the second run above, the stored reasons were:

```text
raw    root         no parents in the selection: v4 sync (begin = sync time − backtrack)
clean  skip         sql:src/raw unchanged since its cursor (watermark 2024-01-02 00:00, no unconsumed entries); definition unchanged
daily  skip         sql:local/clean unchanged since its cursor (watermark 2024-01-02 00:00, no unconsumed entries); definition unchanged
```

### Late data becomes a backfill

A source row arrived with a timestamp *before* everything already synced (`2023-12-30`, while the pipes had reached `2024-01-02`). A plain incremental sync only looks back by the backtrack window, so it misses it, exactly as in v4. Re-syncing the root over a wider window records which ranges changed, and the children replay exactly those:

```bash
mrsm sync pipes -i sql:local -c sql:src -m raw --children --begin 2023-12-29
```

```text
raw   -> 2023-12-30 00:00:00.000000 - 2024-01-02 06:00:00.000000 | Inserted 1, updated 0 rows.
clean -> 2023-12-30 00:00:00 - 2024-01-02 06:00:00 | Inserted 1, updated 0 rows.
daily -> 2023-12-30 - 2024-01-02 | Inserted 1, updated 0 rows.
```

and the run history shows why the children ran:

```text
backfill  1 unconsumed ledger entry from sql:src/raw (seq 3); dirty ranges start before the tail (2024-01-01 06:00): backfill them on the chunk grid
backfill  1 unconsumed ledger entry from sql:local/clean (seq 3); dirty ranges start before the tail (2024-01-01 00:00): backfill them on the chunk grid
```

### The capping rule

Inside an engine run a child is **never synced past the minimum of its parents' watermarks** (plus one time unit, so rows exactly at a parent's watermark sync together; `system.engine.inclusive_cap: false` makes the bound half-open). This prevents a rollup from computing a partial day. A bare `pipe.sync()` never caps.

### Your `--begin` and `--end`

Roots take them verbatim. Every other node's window is clamped to them. A bound of the wrong kind for a node's axis (a datetime for an integer axis) is ignored with a warning.

### Pipes without a datetime column

They have no watermark. Any change in a parent triggers a full re-sync of the child, which relies on the child's own `filter_existing`. Pipes with an integer datetime axis behave the same way on the integer grid. If a parent is an integer epoch and the child a datetime, the parent's [`precision`](/reference/pipes/parameters/#precision) converts ranges between the two. An edge can name the unit explicitly with `axis: {unit: ms}`.

### Windows for edges: `lookback` and `lookahead`

If a child's row at time `t` reads its parent over `[t − 7d, t]`, say so on the edge so a parent change dirties enough of the child:

```yaml
parents:
  - {connector: sql:main, metric: raw_events, lookback_days: 7}
```

Keys are `lookback_{seconds,minutes,hours,days,weeks}` and `lookahead_*`, or a bare number (raw units on an integer axis, minutes on a datetime axis).

## 💥 Failures and Resuming

- A node that fails marks every descendant `upstream_failed`. Independent branches keep running.
- Give a pipe a retry policy with `parameters['retry'] = {'max': 3, 'backoff_seconds': 30}`. `max` counts retries after the first attempt and the backoff is fixed. Losing the pipe's lease counts as a failed attempt.
- `--resume <run_id>` re-plans only the nodes that failed or never ran. Nodes that already succeeded count as satisfied parents. With no other selection flags, `--resume` reuses the original run's selection.
- A run succeeds when nothing failed, was `upstream_failed` or never ran. `rebuild_pending` counts as success.

```bash
mrsm sync pipes -i sql:local --resume 01M4VCTF759XD89GSA6046KPHM
```

### Rebuilds

When you change a pipe's `sql`, `fetch` config or `dtypes`, an engine run notices (the definition is hashed from the *unresolved* text, so reordering YAML keys never triggers it). Because a definition change can drop rows or columns that `filter_existing` would never remove, a `rebuild` clears the pipe and syncs it in full.

- **Interactively**, the plan asks you to confirm (or pass `--yes`). Declining leaves the pipe `rebuild_pending`.
- **In a job, with `--loop`, or over the API** a rebuild without `--yes` never blocks: the node is marked `rebuild_pending`, its descendants run against the stale data, and a warning repeats on every run until someone confirms.
- Set `system.engine.auto_rebuild: true` to rebuild without asking.

The first engine run on a pipe has no stored hash, so it records a baseline rather than treating it as a change.

## ⚖️ Bare Syncs Stay v4

| | `sync pipes` (flat) | `--parents` / `--children` |
|---|---|---|
| Ordering | By edges when there are any, otherwise v4's `connector, metric, location` order | Dependency order |
| Capping at parents' watermark | no | yes |
| Rebuild detection | no | yes |
| Cursors and hashes written | no | yes |
| Failure propagation | no | `upstream_failed` |
| Ledger entries written | yes (`system.engine.flat_ledger`, default on), so a later graph run sees what a flat run changed | yes |

!!! tip "Chunk pipelining"

    By default a child starts after its parents finish. `system.engine.chunk_pipelining` is the setting reserved for overlapping chunks of a backfill at `(pipe, chunk)` granularity. It is accepted but **not implemented** in this build.
