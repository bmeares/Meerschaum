# 🗃️ Engine State

The engine adds **no database files** to your root directory. Everything it remembers lives on your **instance**, as internal pipes alongside Meerschaum's own tables (in the internal schema on flavors that have schemas; on SQLite they are plain tables named `mrsm_engine_*`). These pipes use the connector keys `mrsm`, are temporary and static (never registered, never shown by `show pipes`) and are excluded from the graph and the planner.

| Table (pipe) | Holds | Kept on |
|---|---|---|
| `mrsm_engine_state` (`mrsm:engine_state`) | The definition hash per pipe (rebuild detection) | The pipe's instance |
| `mrsm_engine_ledger` | The [dirty-range ledger](#the-dirty-range-ledger) | The child's instance |
| `mrsm_engine_cursors` | The [edge cursors](#cursors) | The child's instance |
| `mrsm_engine_runs`, `mrsm_engine_nodes` | [Run history](#run-history) and per-node results | The home instance |
| `mrsm_engine_events` | Persisted events | The home instance |
| `mrsm_pipe_leases` | [Leases](#leases) (SQL instances) | The pipe's instance |

The first engine run creates them. Graphs, watermarks and duration estimates are never persisted: the graph is rebuilt per run from one batch `fetch_pipes_keys`, and a pipe's watermark is just its `get_sync_time()`.

!!! info "API instances"

    Engine state travels over the dedicated `/engine` route family (`/engine/ledger`, `/engine/cursors`, `/engine/state`, `/engine/runs`, `/engine/events`, `/engine/leases`), which requires the `pipes:write` scope. The generic pipe routes refuse the `mrsm` connector keys. On a read-only instance the engine runs degraded: watermarks only, no ledger-driven backfill, no persisted history, with a warning.

## 📒 The Dirty-Range Ledger

Whenever a sync changes rows, the engine writes one **ledger entry** *before* committing the data. The entry records the range of the datetime column that is about to change (the rows `filter_existing` decided to insert or update, bucketed onto the epoch-aligned chunk grid, i.e. `verify.chunk_minutes`, 30 days by default). A sync with no changed rows writes nothing. Chunked and generator syncs write an entry per chunk, and each entry holds one range per contiguous run of occupied grid cells.

After syncing a late row into `sql:src/raw`, the ledger contained:

```text
created                     pipe_ref                  seq  begin                                       end
2026-10-13 19:46:29.168318  sql:local|sql:src|raw|    2    {"datetime": "2023-12-19T00:00:00+00:00"}   {"datetime": "2024-01-18T00:00:00+00:00"}
2026-10-13 19:46:29.253231  sql:local|sql:local|clean| 2   {"datetime": "2023-12-19T00:00:00+00:00"}   {"datetime": "2024-01-18T00:00:00+00:00"}
```

(`pipe_ref` is `instance|connector|metric|location`.) Each pipe's entries carry an increasing **sequence number**. Entries have no interval for pipes without a datetime column, only a sequence number, which dirties the child's whole range.

Because the entry is written before the commit, a crash between the two leaves a ledger entry for rows that never landed. That is harmless: the child re-syncs a range that did not change. The reverse, committed rows with no entry, cannot happen. Syncs are idempotent, so delivery is at-least-once.

`Pipe.verify()` writes ledger entries for the chunks it repairs, so repairs flow downstream automatically.

**Compaction.** Entries that every child has consumed are pruned. Compaction never waits on a child that has no cursor yet, always keeps each pipe's newest entry (otherwise the sequence would restart and children would silently miss data), and never prunes by age.

## 🔖 Cursors

A cursor is stored per **edge** `(parent, child)`: the last parent sequence number the child consumed, and the parent's watermark when it did (`parent_watermark_at_cursor`). The planner compares the parent's newest ledger entry to the cursor:

```text
parent_ref                 child_ref                  seq  parent_watermark_at_cursor
sql:local|sql:src|raw|     sql:local|sql:local|clean| 2   {"datetime": "2024-01-02T06:00:00+00:00"}
sql:local|sql:local|clean| sql:local|sql:local|daily| 2   {"datetime": "2024-01-02T06:00:00+00:00"}
```

Cursors are why `skip` works for every shape of pipe. Comparing a child's watermark to its parents' assumes both measure the same thing, which a daily rollup does not: its watermark is the day floor and is always below the raw pipe's. A cursor records exactly what the child consumed instead.

**Write ordering.** A parent writes its ledger entry *before* committing; a child advances its cursor only *after* its sync succeeds.

**First run.** A child with no cursor falls back to comparing watermarks and consumes the parent's latest sequence number, so ledger entries that predate the engine are treated as handled.

## 🔒 Leases

Before syncing a pipe the executor acquires a **lease** on it, so two jobs never sync the same pipe at once. Leases are short-lived (60 seconds), renewed every third of that, and released when the node finishes. A node that loses its lease mid-sync is aborted before it commits.

| Instance | How the lease is taken |
|---|---|
| SQL | An `INSERT` into the `mrsm_pipe_leases` table (unique key on the pipe), with conditional `DELETE` for expiry |
| Valkey | `SET NX PX` |
| API | The `/engine/leases` routes |
| Custom instance connectors | Implement the optional `acquire_pipe_lease(pipe, holder, ttl)` / `release_pipe_lease(pipe, holder)` pair. Without it the engine falls back to **machine-local file locks** under `{root}/cache/engine_leases`, with a one-time warning that the lease only protects against processes on this machine. |

## 📜 Run History

Every engine run is recorded: one row in `mrsm_engine_runs`, one row per node in `mrsm_engine_nodes`, and (depending on `system.engine.events`) rows in `mrsm_engine_events`.

```text
run_id                      success  message                                                             workers  num_nodes  selection
01M4VCTF759XD89GSA6046KPHM  1.0      3 succeeded, 0 skipped, 0 observed, 0 failed, 0 upstream_failed, … 4        3          {"targets":{"refs":["sql:local|sql:local|daily|None"]},"parents":"all","children":null}
01M4VCTFHE91JA00NQ0YHHQDMQ  1.0      1 succeeded, 2 skipped, 0 observed, 0 failed, 0 upstream_failed, …  4        3          …
```

A node row has its `action`, `status`, the planner's `reason`, the window it synced (`begin`, `end`), `num_chunks`, `attempts`, timings and the counts (`inserted`, `updated`, `upserted`, `deleted`), plus `planned_start_seconds` and `slack_seconds` from the critical-path calculation that [the Gantt chart](/reference/pipelines/visualization/) draws. The planner's duration estimates are an exponentially weighted moving average per `(pipe, action)` over past runs (30 seconds for a pipe that has never run).

Run IDs are sortable ULIDs. Retention is `clear_pipe(end=now − run_history_days)` on runs, nodes and events (default 30 days); consumed ledger entries are pruned alongside. Unconsumed entries are never pruned by age.

!!! warning "Status in this pre-release"

    The history is written and readable (these tables, and the console's [pipelines page](/reference/pipelines/visualization/)). The `mrsm show runs` action is specified but not yet wired in this build, so read the tables directly, e.g. with `mrsm sql` or the SQL tool for your flavor.

## ⚙️ Settings

All keys live under `system.engine` in your configuration (`mrsm edit config system`), and all have defaults. You do not need to set any of them.

| Key | Default | Meaning |
|---|---|---|
| `workers` | `4` | Global number of nodes syncing at once (`-w` overrides per run) |
| `limits.connectors` | none | Per-connector concurrency, e.g. `{'sql:main': 4}` |
| `limits.instances` | none | Per-instance concurrency |
| `python_workers` | worker pool size | How many nodes may use Python (plugin) connectors at once |
| `implicit_edges` | `true` | Treat `{{ Pipe() }}` references as edges |
| `auto_rebuild` | `false` | Rebuild changed definitions without confirmation |
| `inclusive_cap` | `true` | Cap children at the parents' watermark plus one unit (`false`: half-open) |
| `flat_ledger` | `true` | Write ledger entries for flat (non-graph) syncs too |
| `persist_runs` | `true` | `false` keeps runs, nodes and events in memory only |
| `events` | `nodes` | Persisted event granularity: `off`, `nodes` (run and node transitions) or `all` |
| `state_flush_ms` | `500` | In continuous mode, how often buffered ledger entries and events are flushed |
| `run_history_days` | `30` | Retention for runs, nodes and events |
| `debounce_seconds` | unset | Minimum time between triggered runs of a stream child (see [Continuous Mode](/reference/pipelines/continuous/)) |
| `chunk_pipelining` | `false` | Reserved; accepted but not implemented |

```yaml
system:
  engine:
    workers: 8
    limits:
      connectors:
        'sql:main': 4
    run_history_days: 14
```

## 💸 What it costs

The write path is deliberately small. Per user sync the engine adds **one ledger insert** (before the data commit, parents only) and **one cursor upsert** (after the sync succeeds; a single statement for all of a node's edges). State pipes are written without existence checks or dtype enforcement, and ledger entries and events are batched. Reads and metadata queries are not counted in that budget.
