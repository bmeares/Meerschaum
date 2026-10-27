# 🗃️ Engine State

The engine adds **no database files** to your root directory. Everything it remembers lives on your **instance**, as internal pipes alongside Meerschaum's own tables (on flavors that have schemas, PostgreSQL and MSSQL, in the internal schema `_mrsm_internal`; on MySQL, MariaDB, SQLite, DuckDB and Oracle they are plain tables named `mrsm_engine_*` in the instance's database). When an instance's connector uses a schema other than the default (`public`, `dbo`), its tables carry that schema as a suffix (`_mrsm_internal.mrsm_engine_runs_analytics`; a schema longer than 15 characters is shortened to a 16-character name ending in a hash), so instances that share a database never share engine state. These pipes use the connector keys `mrsm`, are temporary and static (never registered, never shown by `show pipes`) and are excluded from the graph and the planner.

| Table (pipe) | Holds | Kept on |
|---|---|---|
| `mrsm_engine_state` (`mrsm:engine_state`) | The definition hash per pipe (rebuild detection), plus `<pipe_ref>\|remote\|<instance>` markers that remember on which other instances the pipe's ledger entries and cursors were written, so removing the pipe can forget them there | The pipe's instance |
| `mrsm_engine_ledger` | The [dirty-range ledger](#the-dirty-range-ledger) | The child's instance |
| `mrsm_engine_cursors` | The [edge cursors](#cursors) | The child's instance |
| `mrsm_engine_runs`, `mrsm_engine_nodes` | [Run history](#run-history) and per-node results | The home instance |
| `mrsm_engine_events` | Persisted events | The home instance |
| `mrsm_engine_sync_status` | The [sync status](#sync-status) of every pipe: how its latest sync went | The pipe's instance |
| `mrsm_pipe_leases` | [Leases](#leases) (SQL instances) | The pipe's instance |

The first engine run creates them. Graphs, watermarks and duration estimates are never persisted: the graph is rebuilt per run from one batch `fetch_pipes_keys`, and a pipe's watermark is just its `get_sync_time()`.

!!! info "API instances"

    Engine state travels over the dedicated `/engine` route family: the engine reads and writes its state tables through `/engine/pipes/{metric}/read`, `/write`, `/clear` and `/exists` (reads need the `pipes:read` scope, writes `pipes:write`), and leases go through `/engine/leases`; `/engine/ledger`, `/engine/cursors`, `/engine/state`, `/engine/runs` and `/engine/events` expose the same rows by shape (reading needs `pipes:read`, writing `pipes:write`), `GET /engine/runs` filters runs by Pipeline, pipe, status and start time and `GET /engine/runs/{run_id}` returns one run with its pipes ([over HTTP](/reference/api-instance/routes/#runs)), and `GET /engine/runs/{run_id}/events/stream` follows a run's events live as Server-Sent Events ([Following a run over HTTP](/reference/pipelines/visualization/#following-a-run-over-http)). Every `/engine/*` route answers `{"success": ..., "message": ..., ...}` with real status codes: a store the server cannot read is an error (`500`), never an empty history. The server keeps all of it on its own instance (in `_mrsm_internal` on a PostgreSQL or MSSQL instance), so it survives a server restart. The generic pipe routes refuse the `mrsm` connector keys. On a read-only instance the engine runs degraded: watermarks only, no ledger-driven backfill, no persisted history, with a warning.

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
| API | The `/engine/leases` routes, which take the lease on the server's own instance |
| Custom instance connectors | Implement the optional `acquire_pipe_lease(pipe, holder, ttl)` / `release_pipe_lease(pipe, holder)` pair. Without it the engine falls back to **machine-local file locks** under `{root}/cache/engine_leases`, with a one-time warning that the lease only protects against processes on this machine. |

## 📜 Run History

Every engine run is recorded: one row in `mrsm_engine_runs`, one row per node in `mrsm_engine_nodes`, and (with the default `system.engine.events: all`) one row per event in `mrsm_engine_events`, chunk progress included, so a run can be replayed or followed live after the fact.

```text
run_id                      success  message                                                             workers  num_nodes  selection
01M4VCTF759XD89GSA6046KPHM  1.0      3 synced                  4        3          {"targets":{"refs":["sql:local|sql:local|daily|None"]},"parents":"all","children":null}
01M4VCTFHE91JA00NQ0YHHQDMQ  1.0      1 synced, 2 up to date    4        3          …
```

A node row has its `action`, `status`, the planner's `reason`, the window it synced (`begin`, `end`), `num_chunks`, `attempts`, timings and the counts (`inserted`, `updated`, `upserted`, `deleted`), plus `planned_start_seconds` and `slack_seconds` from the critical-path calculation that [the Gantt chart](/reference/pipelines/visualization/) draws. The planner's duration estimates are an exponentially weighted moving average per `(pipe, action)` over past runs (30 seconds for a pipe that has never run).

New run IDs contain the pipeline name, readable UTC start time, and a collision suffix. Earlier ULID run IDs remain valid. Retention is `clear_pipe(end=now − run_history_days)` on runs, nodes and events (default 30 days); consumed ledger entries are pruned alongside. Unconsumed entries are never pruned by age.

From Python, read it through a `Pipeline` (or `mrsm.get_runs()` for every run on an instance):

```python
pipeline = mrsm.Pipeline(pipe)
pipeline.get_runs(limit=10)            # newest first: the run row, `status`, `duration_seconds`, node `counts`
pipeline.get_runs(status='failed')     # 'running', 'succeeded' or 'failed'
pipeline.get_run(run_id)               # one run: node rows plus the planned-vs-actual Gantt bars
pipeline.get_events(run_id)            # the persisted events, in order (also `run.events()` on a `RunResult`)

mrsm.get_runs(instance='sql:main', limit=5)    # every run on the instance
mrsm.get_runs(pipeline='daily')                # the runs of the saved Pipeline 'daily'
mrsm.get_runs(pipe=pipe, status='failed')      # the failed runs that synced this pipe
mrsm.get_runs(since=timedelta(hours=6))        # runs started in the last 6 hours (or a datetime)
```

By default `Pipeline.get_runs()` returns a named Pipeline's own runs (`mrsm.Pipeline('daily').get_runs()`), and for an unnamed selection the runs that targeted or contained its member pipes; pass `all_runs=True` for every run on the instance. Both take `since=` too: a `datetime` (naive means UTC), a `timedelta` (that long ago) or an ISO 8601 string, like `GET /engine/runs?since=...`. Events are read after the run from the instance (granularity set by `system.engine.events`). To follow another process's run as it happens, an API server streams its events over HTTP ([Following a run over HTTP](/reference/pipelines/visualization/#following-a-run-over-http)); `on_event` follows your own run from Python.

!!! tip "Reading the history"

    `mrsm show runs` lists the runs and `mrsm show run <id>` draws one run's Gantt chart. The console's [pipelines page](/reference/pipelines/visualization/) and the Python calls above read the same tables.

A run's `message` lists only the counts that are not zero, in plain words: `synced`, `up to date` (planned `skip`), `listening for new data` (a stream source in a one-time run), `failed`, `not synced because a parent failed`, `waiting for rebuild approval` and `not started`; state warnings follow on `warning:` lines. The node rows keep the machine-readable `status` of each pipe.

## 🩺 Sync Status

Every sync records **how it went** on its pipe's instance, whatever ran it: `mrsm sync pipes`, a background job's laps, a Pipeline run (one record per pipe, after its retries), a `--listen` callback or stream flush, `mrsm copy pipes` (the rows it copies into the new pipe), the API server's `POST /pipes/{connector}/{metric}/{location}/data` and MCP tools, an `api:` client, Python's `pipe.sync()`, and plugins' `pipe.sync()` calls. A pipe's sync status is one row of `mrsm_engine_sync_status`:

| Field | Meaning |
|---|---|
| `pipe_ref`, `connector_keys`, `metric_key`, `location_key`, `instance_keys` | The pipe |
| `last_attempt` | When the latest recorded sync ended (ISO-8601 UTC) |
| `last_success`, `last_failure` | When the latest successful and failed syncs ended, or `null` |
| `success`, `message` | The latest sync's result (messages over 1,000 characters keep their start and end) |
| `rows_inserted`, `rows_updated`, `rows_upserted` | The latest sync's row counts |
| `duration_seconds` | How long the latest sync took |
| `consecutive_failures` | Failed syncs since the last success; `0` after a success |
| `source` | What ran it: `cli`, `job`, `engine` (a Pipeline or `--parents` / `--children` run), `api` (the API server) or `python` |
| `job` | The job's name when the sync ran in a background job, else `null` |

```python
import meerschaum as mrsm

pipe = mrsm.Pipe('sql:main', 'weather', instance='sql:local')
pipe.get_sync_status()
# {'pipe_ref': 'sql:local|sql:main|weather|None', 'success': True, 'consecutive_failures': 0,
#  'last_success': '2026-10-27T14:02:11.418237+00:00', 'rows_inserted': 24, 'source': 'job',
#  'job': 'weather-sync', ...}

statuses = mrsm.get_sync_statuses('sql:local')   # {pipe_ref: status} for every recorded pipe
failing = [s for s in statuses.values() if s['consecutive_failures'] > 0]
```

`pipe.get_sync_status()` is `None` for a pipe whose syncs were never recorded. Over HTTP, `GET /engine/sync-status` (scope `pipes:read`) answers `{"success": true, "message": ..., "statuses": [...]}`, or one pipe's with `?pipe_ref=`; an `api:` client posts its own outcomes to `POST /engine/sync-status` (scope `pipes:write`), so a source that fails on the client is recorded on the server too, and the server records the syncs posted to it by older clients itself. The server keys the rows with its own instance (`sql:main|...`); the client reads them under its `api:` keys. The row counts of a sync to an `api:` instance are the server's, summed over the chunks the client posted.

On the command line, `mrsm show pipes` ends with one line for each selected pipe whose latest sync failed, and prints nothing extra while they all succeed:

```
Pipe('sql:main', 'rollup', 'home') failed its last 3 syncs (latest 2026-10-27 07:39:35 UTC): no such table: raw
```

**Write rate.** Successes of one pipe are written at most every 5 seconds: a success inside that window is held and written when it ends, so a busy `--listen` job costs one write per pipe every few seconds. Failures, and the first success after a failure, are written at once. Each write reads the pipe's row first and then upserts it (one read, one write), so the failure count stays right when several processes sync the same pipe; outcomes that come in while a write runs go out together in the next one.

**Never in the way.** A sync never waits for its status: the writes run in the background, one at a time per pipe, each bounded to a few seconds, and errors are logged at debug level. A read in the same process (`pipe.get_sync_status()`, `mrsm.get_sync_statuses()`, the API server's `GET /engine/sync-status`) first writes what the process still holds for the pipes it reads, so it always sees the latest sync, held successes included. A process writes what it holds before it exits: the command line and its jobs, the API server when it stops, and Python scripts (with `atexit`); only a process that is killed loses a held success, and the success before it is already stored.

**What is not recorded.** Internal (`mrsm`) and temporary pipes have no status. A Pipeline node that never ran because another process held the pipe's lease (it was syncing the pipe, and records that sync itself) records nothing; its node row still says why it failed.

**Forgetting.** Dropping, deleting or fully clearing a pipe forgets its status, like the rest of its engine state: what the process still held or queued for the pipe is dropped first, and a write in progress finishes before the row is cleared, so the status never comes back on its own. A held success whose pipe another process removed within the 5 seconds is dropped as well.

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
| `events` | `all` | Persisted event granularity: `all` (every event, including `node_planned`, `chunk_started`, `chunk_finished` and `stream_flushed`), `nodes` (run and node transitions) or `off` |
| `state_flush_ms` | `500` | In continuous mode, how often buffered ledger entries and events are flushed |
| `run_history_days` | `30` | Retention for runs, nodes and events |
| `debounce_seconds` | unset | Minimum time between triggered runs of a stream child (see [Continuous Mode](/reference/pipelines/continuous/)) |
| `default_duration_seconds` | `30` | Expected duration of a node with no run history: its critical-path duration and the length of its planned Gantt bar (see [Visualization](/reference/pipelines/visualization/)) |
| `chunk_pipelining` | `false` | Let a child pre-sync the chunks its running parents have committed (see [Pipelines](/reference/pipelines/#chunk-pipelining)); `--chunk-pipelining` turns it on for one run |

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

The write path is deliberately small. Per user sync the engine adds **one ledger insert** (before the data commit, parents only) and **one cursor upsert** (after the sync succeeds; a single statement for all of a node's edges). State pipes are written without existence checks or dtype enforcement, and ledger entries and events are batched. Reads and metadata queries are not counted in that budget. The [sync status](#sync-status) adds one read and one upsert per recorded sync, written in the background after the sync returns (outside the data's transaction), with successes written at most every 5 seconds per pipe.

**Event history.** With the default `events: all`, a node that syncs one chunk writes about 6 event rows (`node_planned`, `node_started`, `chunk_started`, `chunk_finished`, `watermark_advanced`, `node_finished`; about 1.5 KB of payload) instead of 3 with `events: nodes` (about 0.7 KB), and each further chunk adds 2 rows (about 0.6 KB); every run adds `run_started` and `run_finished`. The rows are written in the same batches (one insert per `state_flush_ms`), and retention removes them with the rest of the run's history after `run_history_days`. For scale: a `--loop` job syncing 50 single-chunk pipes every minute keeps about 430,000 event rows a day (roughly 100 MB of payload before indexes) and so about 13 million rows at the default 30 days of retention. For such high-frequency loops, set `events: nodes` (the console and the event stream then show node transitions without chunk progress) or shorten `run_history_days`.
