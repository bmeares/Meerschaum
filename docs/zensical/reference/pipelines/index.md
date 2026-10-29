# 🕸️ Pipelines

> New in v5. The **Syncing Engine** understands how your pipes depend on one another. It decides which pipes need to run, how wide each sync must be, runs independent branches in parallel, and records what it did.

A plain `mrsm sync pipes` still works exactly as it did in v4: every selected pipe syncs on its own, nothing is capped, nothing is rebuilt. The engine only takes over when you ask for the graph (`--parents`, `--children`) or run in [continuous mode](/reference/pipelines/continuous/) with a selection that has edges. Nothing here changes how a pipe is defined.

!!! note "Pipelines and action chains"

    A Pipeline is a registered selection over the Pipe dependency graph. Its registration belongs to an instance, while its member Pipes may belong to several instances. `start pipeline NAME` runs a saved Pipeline in the background (see [below](#saved-pipelines-and-scheduled-jobs)). Commands joined with `+` and `:` are [action chains](/reference/actions/#chaining-actions), which are unrelated to Pipelines.

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

`show pipelines` uses a column-based overview of registered definitions, explicit member Pipes,
and latest run status. It reads saved state without traversing dependencies. Select one name
(`show pipelines daily`) for its identity, revision, traversal settings, and exact member references.
Use `show pipelines --nopretty` for saved definitions and run status as JSON. Neither view syncs
data or applies tags.

`analyze pipelines` resolves members and configured traversal,
reports missing references and shared membership, and previews tagging explicit and detected
traversal Pipes with `pipeline-daily`. Approve its proposal to edit tags, or pass `--yes` to
approve the displayed proposal in a batch command. Without approval, analysis changes nothing.
Saving, planning, ordinary syncing and opening the console never silently tag Pipes or register
suggested connected components. Tags are indexes; the Pipeline registration owns membership.

Use `bootstrap pipelines` for guided creation, `edit pipelines [NAME ...]` to edit YAML definitions,
`copy pipelines daily daily-copy` to reference the same members under a new identity, and
`delete pipelines daily` to remove a definition while preserving member registrations and data.
Bare `delete pipelines` selects all definitions and asks for confirmation. Unknown names warn and
are skipped; an explicitly named selection with no valid names fails. Ctrl+C aborts a confirmation
and returns to the shell. Scheduled Jobs referencing a Pipeline must be removed or reassigned
before deletion.

`drop pipelines` drops the explicit members' tables and `clear pipelines` clears their data
using the normal bounds. Both preview exact instances and shared membership before confirmation;
they do not implicitly expand traversal. Shared Pipe data affects every Pipeline using it.
Schedule a Pipeline with an ordinary Job running `sync pipelines NAME -i REGISTRY`.

## 🌳 The Pipe Graph

Dependencies come from `parents`, `children`, and `{{ Pipe(...) }}` references. The graph combines these edges and runs parents before children.

<details markdown="1">
<summary>Edges, cross-instance references, and cycle handling</summary>

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

</details>

## 🎯 Selecting a Slice

Use `--parents` or `--children` to include dependencies. Preview with `plan pipes` before syncing.

<details markdown="1">
<summary>Selection flags, Python examples, planning, and history</summary>

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

    `mrsm --help` lists `--parents`, `--children`, `--resume`, `--run-id` and `--chunk-pipelining` under **Pipeline options**, and `mrsm sync pipes --help` describes them with the other `sync pipes` flags. They work with every other `sync pipes` flag (`--begin`, `--end`, `-w`, `--yes`, `--nopretty`, ...).

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
run.inserted, run.updated    # rows inserted and updated by every pipe of the run together
run.duration                 # wall-clock seconds (parallel pipes overlap)
run.run_id                   # the ID in the run history
run.outcomes                 # per node, in completion order: pipe_ref, pipe, result (inserted, updated, duration in seconds, ...)
run.plan                     # the Plan that was executed
run.events()                 # the run's persisted events (run_started, node_finished, ...)

# Live: every event as it happens, as a plain dict, on this thread.
run = pipe.sync(children=True, on_event=lambda e: print(e['event'], e['pipe_ref']))
```

All the usual `sync` options (`begin`, `end`, `workers`, `force`, `retries`, `check_existing`, `chunksize`, `params`, ...) apply to every node, with the engine's rules: roots take `begin` / `end` as given, every other node is clamped to them. `''` and `None` leave the window to the planner, so a root syncs from its sync time minus the backtrack. (In a bare `pipe.sync()`, `begin=None` means "no lower bound"; with `parents=`, `children=` or a `Pipeline` it does not.)

`mrsm.Pipeline` selects **member** pipes plus traversal depths. Save it under a name to keep it on its instance, and load it again by name, from Python or with `mrsm sync pipelines <name>`. [Jobs](/reference/background-jobs/) provide schedules for saved Pipelines (`pipeline.schedule(...)`) or ordinary `sync pipes --parents` commands:

```python
pipeline = mrsm.Pipeline(pipe, parents=True, children=False)   # or Pipeline([p1, p2]) / Pipeline(tags=['demo'])
plan = pipeline.plan()           # dry run: no data movement, nothing registered (the static plan)
plan = pipeline.plan(expected=True)  # what `mrsm plan pipes` prints (see below)
print(plan)                      # node | action | bounds | chunks | reason
plan.nodes                       # the same table as data: node.pipe, node.action, node.reason, ...
plan.unregistered                # member pipes that are not registered yet (sync() registers them)
plan.to_dataframe()
run = pipeline.sync(workers=8)   # or pipeline.run(...) or plan.execute()
pipeline.pipes                   # every pipe the selection resolves to, in dependency order
pipeline.members                 # the member pipes, before traversal (also `pipeline.targets`)

success, msg = pipeline.save('daily')   # a SuccessTuple
pipeline = mrsm.Pipeline('daily')       # load it (instance= the registry, default the configured instance)
mrsm.get_pipelines()                    # every saved Pipeline on the instance
```

| `Pipeline(...)` argument | Meaning |
|---|---|
| `pipes` | What to select: the name of a saved Pipeline, member pipes, or a list of both (the rules are below the table). |
| `tags`, `connector_keys`, `metric_keys`, `location_keys` | Select the members with the `-t`, `-c`, `-m`, `-l` filters (`_` negates) instead of listing pipes. |
| `parents`, `children` | `True` or a number of generations. For a saved Pipeline, `None` keeps its saved setting and `False` turns traversal off. |
| `instance` | The instance that keeps the engine state and the saved Pipelines (where `Pipeline('daily')` is loaded from). |
| `name` | The name to save the selection under, also used in its run IDs. `Pipeline(name='daily')` with no pipes loads the saved Pipeline. |
| `graph` | `True` makes a run respect the dependencies between members even without traversal (a child member waits for its parent and is capped at its watermark). Saved Pipelines do this by default, like `sync pipelines NAME`; an unsaved selection without `parents` / `children` syncs each member on its own, like `sync pipes`. |

How `pipes` is read:

- A string without `|` is the **name of a saved Pipeline**: `mrsm.Pipeline('daily')` loads it from `instance` (a `ValueError` lists the saved names if it does not exist).
- A string with `|` is a pipe reference, `'instance|connector|metric|location'` (the `pipe_ref` of a plan node or a run).
- A `Pipe`, or a `(connector_keys, metric_key[, location_key])` tuple, is one member pipe.
- A list may mix them: the members of every named Pipeline plus the given pipes (the named Pipelines must agree on `parents` / `children`, or pass them). `mrsm.get_pipes()`'s dictionary works too.
- An unsaved selection's member pipes must share one instance (the engine's home instance); pass `instance=` to choose it. A saved Pipeline's members may live on other instances than its registry.

`Pipeline.sync(**kwargs)` accepts the `sync` options above plus `yes=True` (confirm `rebuild` actions), `resume='<run_id>'` (re-plan only the nodes that failed or never ran in that run) and `on_event=callable` (the [live event stream](/reference/pipelines/visualization/#live-events-in-python)).

Saved Pipelines from Python:

| Method | What it does |
|---|---|
| `save(name=None, overwrite=False, description=None, defaults=None)` | Save the members and traversal. A different Pipeline already saved under the name is only replaced with `overwrite=True`; a Pipeline loaded from that name saves its changes (refused if someone saved a newer revision since you loaded it). Saving the same selection again is a no-op. Returns a `SuccessTuple`. |
| `exists()` | Whether a Pipeline is saved under `pipeline.name`. |
| `delete()` | Delete the saved definition (and the membership tags it owns). Member pipes and their data are kept. |
| `rename(new_name)`, `copy(new_name, instance=None)` | Rename, or save a copy (on another registry with `instance=`; members keep their own instances). |
| `analyze()`, `apply_analysis(proposal=None)` | Preview, then apply, the `pipeline-NAME` membership tags (`mrsm analyze pipelines NAME`). |
| `schedule('every 10 minutes', name=None, workers=None, retries=0, force=False)` | Create and start the job `pipeline-NAME` that runs `sync pipeline NAME --schedule ...`; returns the `Job`. |
| `get_runs()`, `get_run(run_id)`, `get_events(run_id)` | The Pipeline's run history; `mrsm.get_runs(pipeline=..., pipe=..., status=..., since=...)` filters every run on an instance. |

To change a saved Pipeline, load it, change it and save it:

```python
daily = mrsm.Pipeline('daily')
daily.members = daily.members + [mrsm.Pipe('sql:main', 'weekly', instance='sql:main')]
daily.children = True
success, msg = daily.save()
```

!!! note "How the Python API reaches the engine"

    The engine is written in Rust only (there is no Python copy of it), so `parents=`, `children=` and `Pipeline` use the compiled `meerschaum._core` extension that ships in the `meerschaum` wheel (when you build from a checkout, `scripts/rust/build_py.sh` builds it; without it they raise an `ImportError` naming that script). The engine reads the graph from the **instance**, so the pipes it traverses must be registered there, and it uses their stored definitions rather than unsaved in-memory edits. `Pipeline.sync()` registers member pipes you passed as `Pipe` objects if they are not registered yet; `plan()` and `.pipes` never register anything (unregistered members are listed in `plan.unregistered`), and a saved Pipeline's members must already be registered. `pipe.sync(df, children=True)` is rejected: the engine fetches each node itself, so sync a DataFrame with a bare `pipe.sync(df)`. Plugin sources run in the engine's own Python worker pool, not in your interpreter, and `begin=None` means "the planner's default" rather than "no lower bound" as in a bare `pipe.sync()`.

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
pipe                        action        bounds                           chunks  reason
Pipe('sql:src', 'raw')      root          last sync − 1 day → latest data  -       no parents in this run: syncs from its last sync, minus the backtrack window
Pipe('sql:local', 'clean')  incremental*  all data                         1       if its parents sync new rows: first run with Pipe('sql:src', 'raw') as a parent; it has 1 new change; syncs everything up to its parents' latest data
Pipe('sql:local', 'daily')  incremental*  all data                         1       if its parents sync new rows: first run with Pipe('sql:local', 'clean') as a parent; it has 1 new change; syncs everything up to its parents' latest data
* planned again after its parents sync
 💬 Planned 3 pipes (1 would sync, 2 planned again after their parents sync, shown as if they sync new rows). Nothing was synced.

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

`plan pipes` takes the same selection and bound flags as `sync pipes` (`--parents [N]`, `--children [N]`, `--begin`, `--end`, `--resume`) and prints one row per pipe: the action, the sync window in words (`last sync − 1 day → latest data`, `2026-10-12 09:00 → latest data`, `all data`), the number of chunks and the reason in plain words. A `*` marks a pipe planned against the current state that the executor plans again when its parents finish. `--nopretty` prints the plan as JSON (each row keeps its machine-readable `bounds` and adds `bounds_text`, the window in words).

</details>

## 🧮 What the Planner Decides

The planner chooses root, incremental, backfill, rebuild, skip, or observe actions. Run history records why each node ran.

<details markdown="1">
<summary>Actions, late data, bounds, and edge windows</summary>

For every node in the selection the planner picks one **action**. The ledger cursors (see [Engine State](/reference/pipelines/state/)) are the primary signal; watermarks (a pipe's sync time) are the fallback.

| Action | When | Sync window |
|---|---|---|
| `root` | The node has no parent in the selection | like a plain `pipe.sync()`: from the last sync time minus the backtrack window |
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

</details>

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

The first engine run on a pipe has no stored hash, so it records a baseline rather than treating it as a change: upgrading from 4.x never rebuilds a pipe. A hash stored by an earlier 5.0 pre-release is compared the way that release computed it, so upgrading never rebuilds a pipe whose definition did not change; the next successful sync stores the new hash.

## ⚖️ Bare Syncs Stay v4

A plain `sync pipes` keeps independent Pipe behavior. Graph syncs add watermark capping, rebuild detection, and failure propagation.

<details markdown="1">
<summary>Flat sync comparison and chunk pipelining</summary>

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

</details>

## Saved pipelines and scheduled Jobs

A saved Pipeline owns a named selection. A [Job](../background-jobs/index.md) gives it a schedule. Deleting the definition keeps its member Pipes and their data.

<details markdown="1">
<summary>Saved definitions, schedules, background jobs, and API registries</summary>

A named pipeline stores an exact selection on its instance. Its metadata uses an internal
`mrsm` registration and is excluded from the data graph. Saving a selection never changes
its members' tags. Opening the Pipelines page does not register connected components.
Choose members in the draft editor and save a named definition explicitly. Editing an existing
definition checks its revision, while registering or importing a new definition refuses an
accidental overwrite. Shared Pipe parameter edits are separate, explicitly staged changes.

```python
pipeline = mrsm.Pipeline([raw, daily], parents=True, name='daily-totals')
success, msg = pipeline.save()          # refuses to replace a different 'daily-totals'
loaded = mrsm.Pipeline('daily-totals', instance='sql:main')
result = loaded.sync(workers=4)
job = loaded.schedule('every 5 minutes')   # the job 'pipeline-daily-totals'
```

```bash
mrsm sync pipeline daily-totals -i sql:main
mrsm sync pipeline daily-totals -i sql:main --schedule 'every 5 minutes' -d -n daily-sync
```

### Running a Pipeline in the background

`start pipeline NAME` starts the saved Pipeline's background job, and `stop pipeline NAME` stops it (the plurals work too, with several names):

```bash
mrsm start pipeline daily-totals -i sql:main
mrsm show jobs
mrsm stop pipeline daily-totals -i sql:main
```

A Pipeline's background job is a job that syncs it on its instance (`sync pipelines NAME -i <instance> ...`): the job its schedule runs in (created from the Pipelines page or with `pipeline.schedule(...)`), or the one `start pipeline` created. `start pipeline` starts the Pipeline's scheduled jobs when it has any (and names the other jobs it left alone), else its other jobs. When the Pipeline has none, `start pipeline` creates `pipeline-NAME`, which runs `sync pipelines NAME -i <instance> --loop --min-seconds 60` and prints what it created. For a new job, `--min-seconds 300` sets the pace (default 60 seconds, or the Pipeline's saved `min_seconds` default), `-s 'every 10 minutes'` makes it a schedule instead of a loop, and `--name` names it. A name that is not saved on the instance is an error that lists the saved Pipelines (`stop pipeline` stops every job of the Pipeline, even one whose Pipeline has since been deleted). When a loop job and another job of the same Pipeline both run, `start pipeline` warns that its pipes sync more than once. `show jobs` shows how the job's runs go (for example `last good sync 2 min ago`; see [Job health](/reference/background-jobs/health/)).

`sync pipelines NAME --loop` runs the engine's [continuous mode](/reference/pipelines/continuous/) over the Pipeline's members, as `sync pipes --parents --loop` does: a member with a `schedule` of its own syncs when it is due (a lap with nothing due records no run), and its children follow it as usual. Each run is named after the Pipeline. An edit of the Pipeline (on the Pipelines page or with `edit pipelines`) takes effect within half a minute: the loop finishes its current run and starts again with the new members.

On the Pipelines page, **Workspace** contains the graph and editor; **Runs** contains history,
actual execution time and a scrollable stages table. Phone layouts provide **Graph**, **Pipe details**
and **Output** workspace panes. Select a saved pipeline and open **Tools → Manage pipeline** to create a
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
`mrsm.Pipeline('daily-totals', instance='api:registry')`. Exact member references retain their
own instances. The API uses a separate `pipelines` route group for definition metadata:
`GET /pipelines`, `GET /pipelines/keys`, and `/pipelines/{name}/{register,edit,delete,id,attributes}`. An API server also shows one Pipeline (`GET /pipelines/{name}`: its definition, members and latest run), plans a run without moving data (`POST /pipelines/{name}/plan`) and starts one in the background (`POST /pipelines/{name}/sync`); see [Pipelines, Runs and Actions over HTTP](/reference/api-instance/routes/). The [MCP server](/reference/mcp/) has the same as tools (`list_pipelines`, `plan_pipeline`, `sync_pipeline`, `list_runs`, `get_run`).
Read operations require `pipes:read`, registration/edit/metadata leases require `pipes:write`,
and deletion requires `pipes:delete`. These endpoints transport raw registration metadata;
use the CLI or SDK for lifecycle operations that reconcile owned tags and check scheduled Jobs.
They do not expose arbitrary internal `mrsm` Pipes or data-table operations.

Schedule guards check Jobs available on the client and registry server. Server-side schedules
should use the server's own instance keys, such as `sql:main`, rather than an HTTP alias pointing
back to that server: the server cannot infer that arbitrary client aliases name its own registry.
Jobs on unrelated executors are not discoverable automatically.

</details>
