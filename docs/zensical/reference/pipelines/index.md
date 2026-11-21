# 🕸️ Pipelines

> New in v5. The **Syncing Engine** tracks pipe dependencies, computes the exact window each node needs, runs independent branches in parallel, and records every run.

Plain `mrsm sync pipes` still works like v4: every selected pipe syncs independently with no capping or rebuild checks. The engine takes over when you pass `--parents`, `--children`, or `--resume`, run a saved Pipeline, or use [continuous mode](/reference/pipelines/continuous/) on pipes with edges.

!!! note "Pipelines vs. action chains"

    A **Pipeline** is a named selection over the pipe graph stored on an instance (members may live on different instances). Commands joined with `+` and `:` are [action chains](/reference/actions/#chaining-actions), which are unrelated to Pipelines.

Configure engine behavior via `parents`/`children`, `retry`, `schedule`, `min_seconds`, and `stream` in [pipe parameters](/reference/pipes/parameters/). Moving from v4? See the [migration guide](/reference/migrating-to-v5/#the-syncing-engine).

| Page | Covers |
|---|---|
| **This page** | Pipe graph, planning, Python API, failures, and saved Pipelines |
| [Engine State](/reference/pipelines/state/) | Ledger, cursors, leases, run history, sync status, `system.engine.*` |
| [Continuous Mode](/reference/pipelines/continuous/) | `--loop`, `--listen`, micro-batching, child triggers |
| [Pipelines in Compose](/reference/pipelines/compose/) | Declaring Pipelines in `mrsm-compose.yaml` |
| [Visualization](/reference/pipelines/visualization/) | Live terminal view, Gantt charts, DAG view, `/dash/pipelines` |

## 🌳 The Pipe Graph

Edges point from parent to child and are merged from three sources:

1. `parents` / `parent` in pipe parameters.
2. `children` / `child` in pipe parameters.
3. Implicit `{{ Pipe(...) }}` references in `sql` or parameter values (disable with `system.engine.implicit_edges: false`).

`reference` / `references` only inherit parameters and never create data edges.

```python
import meerschaum as mrsm

inst = 'sql:local'
raw = mrsm.Pipe('sql:src', 'raw', instance=inst, parameters={
    'columns': {'datetime': 'ts', 'id': 'site'},
    'sql': 'SELECT * FROM readings',
})
clean = mrsm.Pipe('sql:local', 'clean', instance=inst, parameters={
    'columns': {'datetime': 'ts', 'id': 'site'},
    'sql': "SELECT ts, site, temp FROM {{ Pipe('sql:src', 'raw', instance='sql:local') }} WHERE temp IS NOT NULL",
})
daily = mrsm.Pipe('sql:local', 'daily', instance=inst, parameters={
    'columns': {'datetime': 'day', 'id': 'site'},
    'sql': (
        "SELECT date(ts) AS day, site, AVG(temp) AS avg_temp "
        "FROM {{ Pipe('sql:local', 'clean', instance='sql:local') }} GROUP BY 1, 2"
    ),
})
for pipe in (raw, clean, daily):
    pipe.register()
```

The engine builds `raw → clean → daily` automatically. Missing pipe references warn rather than fail; self-references via `{{ Pipe() }}` are ignored; cycles error inside a graph selection (flat `sync pipes` warns and runs in registration order). Cross-instance edges store ledger entries and cursors on the **child's** instance; an unreachable parent marks the child `upstream_failed`.

## 🎯 Selecting and Planning

Combine standard `-c`, `-m`, `-l`, `-t`, `-i` filters with graph flags:

| Flag | Meaning |
|---|---|
| `--parents [N]` | Sync ancestors first in dependency order (`--parents 1` for direct parents) |
| `--children [N]` | Sync targets, then propagate to descendants (`--children 1` for direct children) |
| `--resume <run_id>` | Re-plan only failed or unrun nodes from that run |
| `--chunk-pipelining` | Pre-sync committed parent chunks early (see [below](#chunk-pipelining)) |

```bash
mrsm plan pipes -i sql:local -m daily --parents
mrsm show graph -i sql:local -m daily --parents
mrsm sync pipes -i sql:local -m daily --parents -w 4
mrsm show runs --last 5
mrsm show run <run_id>
```

Independent branches run in parallel up to `system.engine.workers` (default `4`, overridden with `-w`). When nothing changed upstream, unchanged children are skipped automatically. In `plan pipes`, `*` marks provisional actions re-planned after parents finish; `--nopretty` outputs JSON. See [Visualization](/reference/pipelines/visualization/) for the live terminal view and DAG symbols.

### Python

`pipe.sync()` stays on the v4 path unless you pass `parents=` or `children=` (`True` or an `int` depth), which runs the Rust engine and returns a `RunResult` (unpacks as a `SuccessTuple`):

```python
run = pipe.sync(parents=True, workers=4)
success, msg = run
print(run.run_id, run.inserted, run.updated, run.duration)

run = pipe.sync(children=True, on_event=lambda e: print(e['event'], e['pipe_ref']))
```

Use `mrsm.Pipeline` to plan, run, or save a reusable selection:

```python
pipeline = mrsm.Pipeline([raw, daily], parents=True)  # or tags=['demo']
plan = pipeline.plan(expected=True)                   # dry run
run = pipeline.sync(workers=8)

pipeline.save('daily')
loaded = mrsm.Pipeline('daily', instance='sql:local')
all_pipelines = mrsm.get_pipelines('sql:local')
```

| `Pipeline(...)` argument | Meaning |
|---|---|
| `pipes` | Saved name (`'daily'`), `pipe_ref` (`'inst|conn|metric|loc'`), `Pipe`, tuple, dict, or mixed list |
| `tags`, `connector_keys`, `metric_keys`, `location_keys` | Filter members like `-t`, `-c`, `-m`, `-l` (`_` negates) |
| `parents`, `children` | `True` or depth (`None` keeps saved traversal; `False` disables) |
| `instance` | Registry and engine state instance |
| `name` | Saved Pipeline name (`Pipeline(name='daily')` loads it) |
| `graph` | Respect member dependencies without traversing (`True` by default for saved Pipelines) |

Saved `Pipeline` methods: `save(name=None, overwrite=False)`, `exists()`, `delete()`, `rename()`, `copy()`, `analyze()`, `apply_analysis()`, `schedule()`, `get_runs()`, `get_run(run_id)`, `get_events(run_id)`.

!!! note "Runtime notes"

    Graph syncs run in the compiled `meerschaum._core` extension and read registered definitions from the instance (`Pipeline.sync()` registers unregistered `Pipe` members; `plan()` does not). Sync DataFrames with bare `pipe.sync(df)`. Unlike bare `pipe.sync()`, `begin=None` leaves bounds to the planner.

## 🧮 How the Planner Decides

The planner checks edge cursors (see [Engine State](/reference/pipelines/state/)), falling back to watermarks, and assigns each node an action:

| Action | When | Sync window |
|---|---|---|
| `root` | No parent in selection | Normal v4 sync (`last sync − backtrack`) |
| `skip` | Parents and definition unchanged since cursor | None |
| `incremental` | Parent watermark advanced, no earlier dirty ranges | Child watermark − backtrack up to min parent watermark |
| `backfill` | Parent changed rows before the child's tail | Dirty ranges widened to the child's chunk grid |
| `rebuild` | `sql`, `fetch`, `dtypes`, or `columns` changed | Clear and full sync (requires `--yes` or confirmation) |
| `observe` | [Streaming source](/reference/pipelines/continuous/#streaming-sources) | Read-only watermark and ledger check |

Skipped/blocked states are recorded as `upstream_failed` or `rebuild_pending`, each with a stored `reason`.

**Capping and bounds.** Children never sync past `min(parent watermarks) + 1 unit` (`system.engine.inclusive_cap: false` for half-open), preventing rollups on partial periods. Explicit `--begin`/`--end` apply verbatim to roots and clamp downstream nodes. Pipes without a datetime column re-sync in full on any parent change; mixed integer/datetime edges convert via [`precision`](/reference/pipes/parameters/#precision) or `axis: {unit: ms}`.

### Windows your plugin receives

The dirty-range ledger buckets changes onto `verify.chunk_minutes` (default 30 days). Late or historical parent changes trigger a `backfill` widened to the child's chunk grid, so `fetch(pipe, begin, end)` may start earlier than the normal backtrack window. Plugins that recompute a window should rebuild the entire requested range.

Dropping, deleting, or fully clearing a pipe clears its ledger entries, edge cursors (including cross-instance), and definition hash while keeping run history.

### Windows for edges: `lookback` and `lookahead`

Declare edge windows when a child's row at `t` reads a broader slice of its parent:

```yaml
parents:
  - {connector: sql:main, metric: raw_events, lookback_days: 7}
```

Keys: `lookback_{seconds,minutes,hours,days,weeks}`, `lookahead_*`, or a bare number (minutes for datetime, raw units for integer).

**Lookup inputs.** Mark dimension/catalog tables with `role: lookup` so they propagate order and failures without capping event progress:

```yaml
children:
  - {connector: sql:main, metric: daily, role: lookup, lookback_days: 1}
```

Changed lookups recheck the declared window (or full history in chunks if omitted).

## 💥 Failures and Resuming

- Failed nodes mark descendants `upstream_failed`; independent branches keep running.
- Configure retries with `parameters['retry'] = {'max': 3, 'backoff_seconds': 30}` (`max` counts retries after the first attempt).
- Resume only failed or unrun nodes:

```bash
mrsm sync pipes -i sql:local --resume 01M4VCTF759XD89GSA6046KPHM
```

### Rebuilds

Edits to `sql`, `fetch`, `dtypes`, or `columns` trigger a `rebuild` (first engine runs record a baseline without rebuilding). Interactive CLI prompts unless `--yes` or `system.engine.auto_rebuild: true` is set; jobs, `--loop`, and API runs mark `rebuild_pending` without blocking. Set `upsert: true` and `rebuild_mode: upsert` in pipe parameters to replay in place without clearing existing rows.

## ⚖️ Bare Syncs and Pipelining

| Behavior | `sync pipes` (flat) | `--parents` / `--children` |
|---|---|---|
| Ordering | Edges if present, else `connector, metric, location` | Dependency order |
| Watermark capping, rebuild detection, failure propagation | No | Yes |
| Cursors & definition hashes written | No | Yes |
| Ledger entries written | Yes (`system.engine.flat_ledger`, default `true`) | Yes |

### Chunk pipelining

With `--chunk-pipelining` (or `system.engine.chunk_pipelining: true`), a child starts syncing committed chunks from a running `incremental` or `backfill` parent before the parent finishes, followed by one final pass. Pipelining runs one level deep, requires the native executor, and never applies to flat syncs or `rebuild`.

## 📦 Saved Pipelines and Jobs

Saved Pipelines store exact instance-qualified member references and traversal defaults on a registry instance (`-i`). Deleting a Pipeline keeps member pipes and data (remove referencing jobs first).

```bash
mrsm register pipelines daily -i sql:local -c sql:local -m daily
mrsm show pipelines -i sql:local
mrsm analyze pipelines daily -i sql:local   # preview/apply pipeline-daily tags
mrsm plan pipelines daily -i sql:local
mrsm sync pipelines daily -i sql:local --parents
```

Additional commands: `bootstrap pipelines`, `edit pipelines`, `copy pipelines SRC DST`, `delete pipelines NAME`, and `drop` / `clear pipelines` (explicit members only).

### Running a Pipeline in the background

```bash
mrsm start pipeline daily -i sql:main
mrsm show jobs
mrsm stop pipeline daily -i sql:main
```

`start pipeline` starts the Pipeline's scheduled [jobs](/reference/background-jobs/) (created via `pipeline.schedule(...)`, `sync pipeline NAME --schedule ... -d`, or the console), or creates `pipeline-NAME` running `sync pipelines NAME -i <instance> --loop --min-seconds 60` (`--min-seconds`, `-s`, `--name` customize it). `sync pipelines NAME --loop` picks up Pipeline edits on the next lap.

API registries (`-i api:registry`) expose saved Pipelines over `/pipelines` routes ([API routes](/reference/api-instance/routes/)) and [MCP](/reference/mcp/) tools (`list_pipelines`, `plan_pipeline`, `sync_pipeline`, `list_runs`, `get_run`).
