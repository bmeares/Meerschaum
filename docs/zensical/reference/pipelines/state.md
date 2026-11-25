# 🗃️ Engine State

The Syncing Engine creates **no local database files**. All state lives on your **instance** as internal `mrsm` tables—in `_mrsm_internal` on PostgreSQL and MSSQL (suffixed for non-default schemas so instances sharing a database stay isolated), or as `mrsm_engine_*` tables on SQLite, MySQL, MariaDB, DuckDB, and Oracle. Tables are created on first use, hidden from `show pipes`, and excluded from the graph.

| Table | Purpose | Stored on |
|---|---|---|
| `mrsm_engine_state` | Per-pipe definition hash (rebuilds) & cross-instance cleanup markers | Pipe's instance |
| `mrsm_engine_ledger` | [Dirty-range ledger](#dirty-range-ledger-and-cursors) | Child's instance |
| `mrsm_engine_cursors` | Per-edge [cursors](#dirty-range-ledger-and-cursors) | Child's instance |
| `mrsm_engine_runs`, `mrsm_engine_nodes` | [Run history](#run-history) and per-node outcomes | Home instance |
| `mrsm_engine_events` | Persisted run events | Home instance |
| `mrsm_engine_sync_status` | Latest [sync status](#sync-status) per pipe | Pipe's instance |
| `mrsm_engine_metrics` | Numeric [sync metrics](/reference/api-instance/sync-metrics/) | Pipe's instance |
| `mrsm_pipe_leases` | Active [leases](#leases) (SQL instances) | Pipe's instance |

Graphs are rebuilt per run from pipe metadata and watermarks come from `get_sync_time()`. On PostgreSQL, the Stack's [**Engine** Grafana dashboard](/reference/stack/#built-in-dashboards) charts these tables directly.

!!! info "API instances"

    Over `api:` instances, engine state uses `/engine/*` routes (`pipes:read` / `pipes:write`) backed by the server's own instance. Read-only instances degrade to watermarks only with a warning.

## 📒 Dirty-Range Ledger and Cursors

**Ledger.** Before committing changed rows, the engine writes a ledger entry with a monotonic sequence number (`seq`) and the changed datetime ranges bucketed onto `verify.chunk_minutes` (default 30 days), keyed by `pipe_ref` (`instance|connector|metric|location`). Unchanged syncs write nothing; `Pipe.verify()` writes entries for repaired chunks; non-datetime pipes record only `seq` (dirtying full range). Pre-commit writes ensure at-least-once delivery; compaction prunes entries consumed by all children while keeping each pipe's latest entry.

**Cursors.** Each `(parent, child)` edge stores the last consumed parent `seq` and `parent_watermark_at_cursor`, advanced only *after* child sync succeeds. Cursors let `skip` work even when a rollup's watermark trails raw timestamps. First runs fall back to watermarks and record the parent's latest `seq`.

## 🔒 Leases

Before syncing, the executor takes a 60-second lease (renewed every 20s, released on finish) so concurrent jobs never sync the same pipe. Losing a lease aborts before commit.

| Instance | Mechanism |
|---|---|
| SQL | Unique `INSERT` into `mrsm_pipe_leases` with expiry cleanup |
| Valkey | `SET NX PX` |
| API | `/engine/leases` routes on the server's instance |
| Custom | `acquire_pipe_lease` / `release_pipe_lease`; falls back to `{root}/cache/engine_leases` |

## 📜 Run History

Every run records one `mrsm_engine_runs` row, per-node `mrsm_engine_nodes` rows (action, status, reason, bounds, chunks, attempts, counts, timings, Gantt `planned_start_seconds`/`slack_seconds`), and `mrsm_engine_events`. History is pruned after `run_history_days` (default 30; unconsumed ledger entries are never pruned by age). Inspect runs with `mrsm show runs`, `mrsm show run <id>`, the [web console](/reference/pipelines/visualization/), or Python:

```python
from datetime import timedelta
import meerschaum as mrsm

pipeline = mrsm.Pipeline('daily')
pipeline.get_runs(limit=10)
pipeline.get_run(run_id)
pipeline.get_events(run_id)

mrsm.get_runs(instance='sql:main', status='failed', since=timedelta(hours=6))
```

Non-admins see only history matching current Pipe and Pipeline permissions.

## 🩺 Sync Status

Every sync (CLI, job, engine, `--listen`, `copy pipes`, API, MCP, or Python) records its latest outcome in `mrsm_engine_sync_status` (excluding `mrsm` and temporary pipes):

| Field | Meaning |
|---|---|
| `pipe_ref`, `connector_keys`, `metric_key`, `location_key`, `instance_keys` | Pipe identity |
| `last_attempt`, `last_success`, `last_failure` | ISO-8601 UTC timestamps |
| `success`, `message` | Latest outcome |
| `rows_inserted`, `rows_updated`, `rows_upserted`, `duration_seconds` | Latest counts and duration |
| `consecutive_failures` | Failures since last success (`0` after success) |
| `source`, `job` | Caller (`cli`, `job`, `engine`, `api`, `python`) and job name |

```python
pipe = mrsm.Pipe('sql:main', 'weather', instance='sql:local')
pipe.get_sync_status()
statuses = mrsm.get_sync_statuses('sql:local')
```

`mrsm show pipes` appends a line for failing pipes; HTTP uses `GET`/`POST /engine/sync-status`. Writes run asynchronously (failures immediately; successes coalesced to every 5s per pipe, flushed on read/exit). Dropping, deleting, or clearing a pipe removes its status.

## ⚙️ Settings

All keys live under `system.engine` (`mrsm edit config system`):

| Key | Default | Meaning |
|---|---|---|
| `workers` | `4` | Max concurrent nodes (`-w` overrides) |
| `limits.connectors` / `limits.instances` | none | Per-connector / per-instance concurrency caps |
| `python_workers` | pool size | Max concurrent Python plugin workers |
| `python_worker_max_calls` | `10000` | Recycle Python worker after N calls (`0`: never) |
| `python_worker_max_memory_mb` | `1024` | Recycle Python worker above N MB of resident plus swapped private memory (`0`: never; Linux) |
| `python_worker_idle_seconds` | `600` | Stop idle Python workers after N seconds (`0`: never) |
| `python_team_workers` | pool size | Cap across [isolated teams' worker pools](/reference/api-instance/teams/#team-scoped-workers) |
| `python_worker_team_idle_seconds` | `60` | Idle timeout for team workers (`0`: never) |
| `python_team_env_passthrough` | `[]` | Extra env var names passed to team workers/jobs |
| `python_team_sandbox` | `auto` | Linux Landlock mode: `auto`, `require`, `require_sockets`, `off` |
| `implicit_edges` | `true` | Infer edges from `{{ Pipe() }}` references |
| `auto_rebuild` | `false` | Approve definition-change rebuilds automatically |
| `inclusive_cap` | `true` | Cap children at parent watermark + 1 unit (`false`: half-open) |
| `flat_ledger` | `true` | Write ledger entries for flat syncs |
| `persist_runs` | `true` | Persist runs, nodes, and events (`false`: memory only) |
| `persist_skipped_laps` | `true` | Set to `false` to omit loop laps in which every node was skipped from stored history |
| `events` | `all` | Persisted event detail: `all`, `nodes`, or `off` |
| `state_flush_ms` | `500` | Batch flush interval in continuous mode |
| `run_history_days` | `30` | Retention window for runs, nodes, and events |
| `metrics_retention_days` | `run_history_days` | Retention window for sync metrics. Set to `0` to disable expiry |
| `debounce_seconds` | unset | Min delay between triggered runs of a stream child |
| `default_duration_seconds` | `30` | Forecast duration without history |
| `chunk_pipelining` | `false` | Enable [chunk pipelining](/reference/pipelines/#chunk-pipelining) |

```yaml
system:
  engine:
    workers: 8
    limits:
      connectors: {'sql:main': 4}
    events: nodes
    run_history_days: 14
```

Each sync adds one ledger insert (parents only) and one batched cursor upsert. For high-frequency `--loop` jobs, set `events: nodes` or shorten `run_history_days` to keep `mrsm_engine_events` small.
