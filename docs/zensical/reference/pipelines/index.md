# 🕸️ Pipelines

> New in v5. The **Syncing Engine** understands how your pipes depend on one another. It decides which pipes need to run, how wide each sync must be, runs independent branches in parallel, and records what it did.

A plain `mrsm sync pipes` still works exactly as it did in v4: every selected pipe syncs on its own, nothing is capped, nothing is rebuilt. The engine only takes over when you ask for the graph (`--parents`, `--children`) or run in [continuous mode](/reference/pipelines/continuous/) with a selection that has edges. Nothing here changes how a pipe is defined.

!!! note "Not the same as `start pipeline`"

    A Pipeline is a registered selection over the Pipe dependency graph. Its registration belongs to an instance, while its member Pipes may belong to several instances. The `start pipeline` action, which chains several actions with `:`, is unrelated and unchanged.

The pipe parameters that configure the engine are `parents` / `children` (the edges), `retry`, `schedule`, `min_seconds` and `stream`; each is documented in the [Parameters reference](/reference/pipes/parameters/) and used on the pages below. The CLI surface is `sync pipes --parents / --children / --resume / --loop / --listen`, `plan pipes`, `show graph`, `show runs` and `show run <id>`; the Python surface is `pipe.sync(parents=, children=)` and `mrsm.Pipeline`. Moving from v4? See the [migration guide](/reference/migrating-to-v5/#the-syncing-engine).

| Page | What it covers |
|---|---|
| **This page** | The pipe graph, selecting a slice of it, the plan, failures and resuming |
| [Engine State](/reference/pipelines/state/) | Run history, the dirty-range ledger, cursors, leases and the `system.engine.*` settings |
| [Continuous Mode and Streams](/reference/pipelines/continuous/) | `--loop`, `--listen`, micro-batching and triggering children |
| [Visualization](/reference/pipelines/visualization/) | The Gantt chart, the DAG view and the `/dash/pipelines` page |

## Registered Pipelines

Pipelines have instance-local names, stable identities and revisions. A definition stores exact
instance-qualified Pipe references, traversal settings and execution defaults. Selecting its
registry with `-i` does not change the instances of its members. Shared Pipes may belong to
several Pipelines; dependencies continue to come from their Pipe parameters and SQL references.

```bash
mrsm show pipelines -i sql:local
mrsm register pipelines daily -i sql:local -c sql:local -m daily
mrsm analyze pipelines daily -i sql:local
mrsm plan pipelines daily -i sql:local
mrsm sync pipelines daily -i sql:local --parents
```

`show pipelines` reads saved state. `analyze pipelines` resolves members and configured traversal,
reports missing references and shared membership, and previews tagging explicit and detected
traversal Pipes with `pipeline-daily`. Approve its proposal to edit tags, or pass `--yes` to
approve the displayed proposal in a batch command. Without approval, analysis changes nothing.
Saving, planning, ordinary syncing and opening the console never silently tag Pipes or register
suggested connected components. Tags are indexes; the Pipeline registration owns membership.

Use `bootstrap pipelines` for guided creation, `edit pipelines` to change the definition,
`copy pipelines daily daily-copy` to reference the same members under a new identity, and
`delete pipelines daily` to remove a definition while preserving member registrations and data.
Scheduled Jobs referencing a Pipeline must be removed or reassigned before deletion.

`drop pipelines` drops the explicit members' tables and `clear pipelines` clears their data
using the normal bounds. Both preview exact instances and shared membership before confirmation;
they do not implicitly expand traversal. Shared Pipe data affects every Pipeline using it.
Schedule a Pipeline with an ordinary Job running `sync pipelines NAME -i REGISTRY`.

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

    `--parents`, `--children`, `--resume` and `--chunk-pipelining` are parsed ahead of the v4 argument schema, so they are not in `mrsm --help`. `mrsm sync pipes --help` prints v4's text followed by a **v5 additions** block that lists them. They work with every other `sync pipes` flag (`--begin`, `--end`, `-w`, `--yes`, `--nopretty`, ...).

### Python

```python
pipe.sync()                          # v4 behavior, unchanged
pipe.sync(parents=True)              # ancestors in dependency order, then this pipe
pipe.sync(parents=1)                 # direct parents only
pipe.sync(children=True)             # this pipe, then every descendant
pipe.sync(parents=True, children=True)
```

`parents` and `children` take `True` (every generation) or an `int` (that many generations); `False`, `None` and `0` mean no traversal. A bare `pipe.sync()` never touches the engine: it is the v4 code path, returns a plain `(bool, str)` tuple and records nothing.

With `parents` or `children` the sync runs through the Rust engine and returns a `RunResult`, a `SuccessTuple` (`success, message = pipe.sync(children=True)` still works) that also carries the run's details:

```python
run = pipe.sync(children=True, workers=4)
run.success, run.message     # the SuccessTuple
run.run_id                   # the ID in the run history
run.outcomes                 # per node, in completion order: pipe_ref, pipe, result (inserted, updated, ...)
run.plan                     # the Plan that was executed
run.events()                 # the run's persisted events (run_started, node_finished, ...)

# Live: every event as it happens, as a plain dict, on this thread.
run = pipe.sync(children=True, on_event=lambda e: print(e['event'], e['pipe_ref']))
```

All the usual `sync` options (`begin`, `end`, `workers`, `force`, `retries`, `check_existing`, `chunksize`, `params`, ...) apply to every node, with the engine's rules: roots take `begin` / `end` as given, every other node is clamped to them. `''` and `None` leave the window to the planner.

`mrsm.Pipeline` selects target pipes with traversal depths. Call `save(name)` to persist its membership on the instance, then use `Pipeline.load(name, instance=...)` or `mrsm sync pipeline <name>` to resolve it later. [Jobs](/reference/background-jobs/) provide schedules for saved pipelines or ordinary `sync pipes --parents` commands:

```python
pipeline = mrsm.Pipeline(pipe, parents=True, children=False)   # or Pipeline([p1, p2]) / Pipeline(tags=['demo'])
plan = pipeline.plan()           # dry run, no data movement (the static plan)
plan = pipeline.plan(expected=True)  # what `mrsm plan pipes` prints (see below)
print(plan)                      # node | action | bounds | chunks | reason
plan.nodes                       # the same table as data: node.pipe, node.action, node.reason, ...
plan.to_dataframe()
run = pipeline.sync(workers=8)   # or pipeline.run(...) or plan.execute()
pipeline.pipes                   # every pipe the selection resolves to, in dependency order
```

| `Pipeline(...)` argument | Meaning |
|---|---|
| `pipes` | A `Pipe`, or a list of `Pipe` objects and `(connector_keys, metric_key[, location_key])` tuples. All must share one instance (the engine's home instance); pass `instance=` to choose it. |
| `tags`, `connector_keys`, `metric_keys`, `location_keys` | Select the targets with the `-t`, `-c`, `-m`, `-l` filters (`_` negates) instead of listing pipes. |
| `parents`, `children` | `True` or a number of generations. |
| `name` | Optional name used in run IDs. Named selections respect dependencies between their members even without traversal. |

`Pipeline.sync(**kwargs)` accepts the `sync` options above plus `yes=True` (confirm `rebuild` actions), `resume='<run_id>'` (re-plan only the nodes that failed or never ran in that run) and `on_event=callable` (the [live event stream](/reference/pipelines/visualization/#live-events-in-python)).

!!! note "How the Python API reaches the engine"

    The engine is written in Rust only (there is no Python copy of it), so `parents=`, `children=` and `Pipeline` use the compiled `meerschaum._core` extension that ships in the `meerschaum` wheel (when you build from a checkout, `scripts/rust/build_py.sh` builds it; without it they raise an `ImportError` naming that script). The engine reads the graph from the **instance**, so the pipes it traverses must be registered there (a `Pipeline` registers its target pipes if they are not), and it uses their stored definitions rather than unsaved in-memory edits. `pipe.sync(df, children=True)` is rejected: the engine fetches each node itself, so sync a DataFrame with a bare `pipe.sync(df)`. Plugin sources run in the engine's own Python worker pool, not in your interpreter, and `begin=None` means "the planner's default" rather than v4's "no lower bound".

### Dry run, graph and history

```bash
mrsm plan pipes -m daily --parents      # the plan table, nothing is synced
mrsm show graph -m daily --parents      # the selection as a box-drawing tree
mrsm show runs --last 5                 # recorded runs, newest first
mrsm show run <run_id>                  # one run's Gantt chart
```

For the three pipes above on one SQLite instance (`raw` reads a `readings` table), `plan pipes` before the first run, and the graph:

```text
$ mrsm plan pipes -i sql:local -m daily --parents
node             action        bounds        chunks  reason
sql:local/raw    root          ['', None)    -       no parents in the selection: v4 sync (begin = sync time − backtrack)
sql:local/clean  incremental*  [None, None)  1       if its parents sync new rows: no cursor on the edge from sql:local/raw (first engine run); 1 unconsumed ledger entry (seq 1); sync from own watermark − backtrack to min parent watermark
sql:local/daily  incremental*  [None, None)  1       if its parents sync new rows: no cursor on the edge from sql:local/clean (first engine run); 1 unconsumed ledger entry (seq 1); sync from own watermark − backtrack to min parent watermark
* re-planned after its parents finish
 💬 Planned 3 nodes (1 would sync, 2 re-planned after their parents finish, shown as if they sync new rows).
Nothing was synced.

$ mrsm show graph -i sql:local -m daily --parents
Pipe graph on sql:local: 3 pipes, 2 edges, 1 root, depth 3

● sql:local/raw
└── ◆ sql:local/clean
    └── ○ sql:local/daily ★

● root   ◆ has children   ○ leaf   ★ selected
```

In the plan, a `*` after the action marks a provisional row: a child cannot know what its parent will change, so the executor plans it again when the parent finishes. `plan pipes` shows that second plan as it would be if every parent that runs synced new rows after its watermark: the parent's ledger entry covers its whole chunk-grid cell, so a child usually backfills from the start of that cell (`[2026-10-04 00:00, …)` on the default 30-day grid), which is the `begin` its `fetch` receives. Only the start of the window is predicted: its end is the parents' current watermarks, and the executor caps it at their new ones. `--nopretty` prints the same plan as JSON. In Python, `pipeline.plan()` returns the static plan (each provisional row as the state is now, often `skip*`) and `pipeline.plan(expected=True)` the one `plan pipes` prints. In the graph, `★` marks the targets you selected (the others were pulled in by `--parents`), `●` a root and `○` a leaf; see [Visualization](/reference/pipelines/visualization/) for back-references, collapsed fan-outs and edge details. After a run, `show runs` lists it:

```text
$ mrsm show runs --last 5
run                         started (UTC)        status     wall  ok/nodes  targets
01M53RS5VZ8NRBQTDJBHMJAV5J  2026-10-17 01:49:18  succeeded  0s    3/3       sql:local/daily
 💬 Run `show run <run>` for a run's Gantt chart.
```

`plan pipes` takes the same selection and bound flags as `sync pipes` (`--parents [N]`, `--children [N]`, `--begin`, `--end`, `--resume`) and prints one row per node: the action, the sync bounds, the number of chunks and the reason. A `*` marks a node planned against the current state that the executor re-plans when its parents finish. `--nopretty` prints the plan as JSON.

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

### Windows your plugin receives

Ledger ranges are recorded on the epoch-aligned chunk grid (`verify.chunk_minutes`), and a child backfills them on its own chunk grid. So under `--parents`, `--children` or a `Pipeline`, a plugin's `fetch(pipe, begin, end)` can receive a window that starts at a chunk edge, earlier than v4's `sync time − backtrack`. A bare `pipe.sync()` and a flat `sync pipes` keep v4's window. If your plugin **clears its window and recomputes** it (for example `pipe.clear(begin=begin, end=end)` followed by a rebuild from its parents), it must be able to rebuild the whole window it is given, not only the most recent part.

Dropping, deleting or fully clearing a pipe (`pipe.drop()`, `pipe.delete()`, `pipe.clear()` without bounds, and the matching actions) forgets its engine state: its ledger entries, the edge cursors it is part of and its definition hash. A pipe recreated under the same keys is planned like a new one. Run history is kept. Cursors and ledger entries the pipe left on the instances of its cross-instance children are forgotten too (the pipe's own instance remembers where it wrote them); an instance that is not configured on this host or cannot be reached keeps them, and a warning names it.

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

When you change a pipe's `sql`, `fetch` config, `dtypes` or `columns` (a renamed or new `datetime`, `id` or `primary` column), an engine run notices (the definition is hashed from the *unresolved* text, so reordering YAML keys never triggers it). Because a definition change can drop rows or columns that `filter_existing` would never remove, a `rebuild` clears the pipe and syncs it in full.

- **Interactively**, the plan asks you to confirm (or pass `--yes`). Declining leaves the pipe `rebuild_pending`.
- **In a job, with `--loop`, or over the API** a rebuild without `--yes` never blocks: the node is marked `rebuild_pending`, its descendants run against the stale data, and a warning repeats on every run until someone confirms.
- Set `system.engine.auto_rebuild: true` to rebuild without asking.

The first engine run on a pipe has no stored hash, so it records a baseline rather than treating it as a change. A hash stored by 5.0.0rc1 (which did not cover `columns`) is compared the way rc1 computed it, so upgrading never rebuilds a pipe whose definition did not change; the next successful sync stores the new hash.

## ⚖️ Bare Syncs Stay v4

| | `sync pipes` (flat) | `--parents` / `--children` |
|---|---|---|
| Ordering | By edges when there are any, otherwise v4's `connector, metric, location` order | Dependency order |
| Capping at parents' watermark | no | yes |
| Rebuild detection | no | yes |
| Cursors and hashes written | no | yes |
| Failure propagation | no | `upstream_failed` |
| Ledger entries written | yes (`system.engine.flat_ledger`, default on), so a later graph run sees what a flat run changed | yes |

### Chunk pipelining

By default a child starts after its parents finish. With `--chunk-pipelining` (or `system.engine.chunk_pipelining: true`) a child of a parent that syncs several chunks (an `incremental` or `backfill` node with two or more chunks) can start early: whenever the parent has finished a chunk, the child plans again against what the parent has *committed* and syncs those ranges while the parent keeps going. The child's edge cursor advances only as far as committed ledger entries, so nothing is skipped; when the parent finishes, the child makes one final pass for the rest. The child's `node_started` event appears once per pass, and its row in the run history sums the passes.

Pipelining goes one level deep (a child overlaps its parents, not its grandchildren), needs the native executor, never applies to flat selections, and never starts a `rebuild` early. The resulting tables are identical to a run without it.

## Saved pipelines and scheduled Jobs

A named pipeline stores an exact selection on its instance. Its metadata uses an internal
`mrsm` registration and is excluded from the data graph. Saving a selection never changes
its members' tags. Opening the Pipelines page does not register connected components.
Choose members in the draft editor and save a named definition explicitly. Editing an existing
definition checks its revision, while registering or importing a new definition refuses an
accidental overwrite. Shared Pipe parameter edits are separate, explicitly staged changes.

```python
pipeline = mrsm.Pipeline([raw, daily], parents=True, name='daily-totals')
pipeline.save()
loaded = mrsm.Pipeline.load('daily-totals', instance='sql:main')
result = loaded.sync(workers=4)
```

```bash
mrsm sync pipeline daily-totals -i sql:main
mrsm sync pipeline daily-totals -i sql:main --schedule 'every 5 minutes' -d -n daily-sync
```

On the Pipelines page, select a saved pipeline and open **Manage pipeline** to create a
Job with its own name, interval, workers, retries, and rebuild permission. A Job reads the
pipeline's current membership each time it runs. Several Jobs may use the same pipeline
with different schedules or options. The same panel shows their status and start/stop
controls; the Jobs page provides logs and other Job controls. Deleting a saved selection
preserves its member Pipes and data. Referencing scheduled Jobs must be removed or reassigned
before the definition can be deleted.

By default, named Pipeline run IDs use the Pipeline name followed by a UTC timestamp and
collision suffix, for example `daily-totals-2026-10-21T14-30-00.123456Z-a1b2c3d4`. Unnamed selections
use `pipeline` as their prefix. The console allocates an exact execution ID before admission
with `--run-id`; saved definition identity and revision are recorded independently of its spelling.
Earlier run IDs remain valid for history and resume.

Traversal discovery reads the Pipeline registry, explicit members' instances, and instances named
by their dependency references. A child stored only on an otherwise unknown instance cannot be
inferred from its parent; add it explicitly or declare the parent's cross-instance child reference.
Analysis reports missing or unreachable references rather than silently dropping them.

Pipeline registries may also be API instances: use `-i api:registry` or
`Pipeline.load('daily-totals', instance='api:registry')`. Exact member references retain their
own instances. The API uses a separate `pipelines` route group for definition metadata:
`GET /pipelines`, `GET /pipelines/keys`, and `/pipelines/{name}/{register,edit,delete,id,attributes}`.
Read operations require `pipes:read`, registration/edit/metadata leases require `pipes:write`,
and deletion requires `pipes:delete`. These endpoints transport raw registration metadata;
use the CLI or SDK for lifecycle operations that reconcile owned tags and check scheduled Jobs.
They do not expose arbitrary internal `mrsm` Pipes or data-table operations.

Schedule guards check Jobs available on the client and registry server. Server-side schedules
should use the server's own instance keys, such as `sql:main`, rather than an HTTP alias pointing
back to that server: the server cannot infer that arbitrary client aliases name its own registry.
Jobs on unrelated executors are not discoverable automatically.
