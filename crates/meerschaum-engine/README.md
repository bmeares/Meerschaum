# meerschaum-engine

> **Internal crate.** `meerschaum-engine` is an implementation detail of [Meerschaum](https://meerschaum.io)
> and may change in any minor release. Depend on the [`meerschaum`](https://crates.io/crates/meerschaum)
> crate instead.

The dependency-aware Syncing Engine (`docs/design/v5.0.0.md` §5). It plans and runs syncs over
the implicit pipe graph against the `InstanceConnector` / `Connector` traits of
`meerschaum-connector`, so SQL, Valkey, API and Python-adapter connectors all plug in.

## Public API

```rust
use std::sync::Arc;
use meerschaum_engine::{
    Depth, Engine, EngineConfig, LoopOptions, RunOptions, Selection, StaticRegistry,
};

let registry: Arc<dyn meerschaum_engine::ConnectorRegistry> = Arc::new(
    StaticRegistry::new()
        .with_instance("sql:main", instance.clone())   // Arc<dyn InstanceConnector>
        .with_source("sql:main", source.clone()),      // Arc<dyn Connector>
);
let engine = Engine::new(registry, EngineConfig::from_config(&config, Some(&root_dir)));

// Live events (§5.7): one typed event per transition, for every run of this engine.
let mut events = engine.subscribe();
// ... or for one run, unbounded and in order: `RunOptions { on_event: Some(sink), .. }`
// (an `Arc<dyn EventSink>`; the Python `Pipeline.sync(on_event=...)` uses it).

// `mrsm sync pipes -m daily_rollup --parents` / `pipe.sync(parents=True)` / `Pipeline.run()`.
let result = engine
    .run(RunOptions {
        selection: Some(Selection::refs([pipe_ref]).with_parents(Some(Depth::All))),
        ..Default::default()
    })
    .await;
result.success;            // §5.10 "Run outcome"
result.outcomes;           // per node, completion order, with `SyncResult` counts
result.record;             // the persisted `RunRecord` (run row plus node rows)

// `mrsm plan pipes ...`: no data movement.
let planned = engine.plan(&RunOptions { /* ... */ ..Default::default() }).await?;
println!("{}", planned.plan);

// `--resume <run_id>`: re-plans only nodes that failed or never ran.
engine.run(RunOptions { resume: Some(run_id), ..Default::default() }).await;

// A bare `pipe.sync()` (R1): v4 behavior, no graph, no capping, no engine state.
engine.sync_pipe(&pipe, None, &Default::default()).await;

// Continuous mode: `--loop --min-seconds N` (laps), `--listen` (stream sources).
engine.run_loop(opts, LoopOptions { run_loop: true, min_seconds: 20.0, ..Default::default() },
                Arc::new(|lap_event| { /* v4 lap output */ })).await;

// Run history for `show runs` / the Gantt view.
engine.state().recent_runs(10).await?;
engine.state().load_run(&run_id).await?;
engine.state().load_events(&run_id).await?;
```

| Item | Purpose |
|---|---|
| `Engine::run(RunOptions) -> RunResult` | One engine run: build the graph (one `fetch_pipes_keys` per instance), resolve the selection, plan, execute under leases, persist the run, compact the ledger. |
| `Engine::plan` | The plan only. |
| `Engine::subscribe() -> broadcast::Receiver<Event>` | The live event stream (`run_started` … `run_finished`, `stream_flushed`, `stream_restarted`). |
| `add_process_event_sink(Arc<dyn EventSink>)` | Also deliver every run's events in this process to a sink, whatever started the run (`mrsm start api` forwards its actions' runs to `GET /engine/runs/{run_id}/events/stream`). |
| `Engine::sync_pipe` | A bare `pipe.sync()` (R1). |
| `Engine::run_loop` | Continuous mode; reports `LapEvent`s in v4's output order. |
| `Engine::state()` | `InstanceState`: the `mrsm:engine_*` pipes (run history, ledger, cursors, hashes). |
| `ConnectorRegistry` | How the engine reaches connectors. Hosts (CLI, `meerschaum-py`, server) implement it; `StaticRegistry` is a fixed map. |
| `EngineConfig::from_config` | `system.engine.*` (`workers`, `limits.connectors`, `limits.instances`, `python_workers`, `auto_rebuild`, `implicit_edges`, `state_flush_ms`, `run_history_days`, `events`, `inclusive_cap`, `flat_ledger`, `persist_runs`, `chunk_pipelining`). |

## Per-pipe schedules and chunk pipelining

- **`parameters['schedule']`** (§5.5). `Engine::with_cadence(Arc<dyn Cadence>)` gives continuous
  mode a schedule parser (the CLI passes the `meerschaum-jobs` grammar; the engine does not depend
  on that crate). With `--loop`, a root node (every node of a flat selection) with a schedule is
  held out of laps until its next fire time (`exclude` for flat selections, `hold` otherwise); a
  lap with nothing due waits for the earliest fire time and the inter-lap sleep is capped by it.
  Without a `Cadence` the key is ignored.
- **Chunk pipelining** (R6; `RunOptions::chunk_pipelining`, `EngineConfig::chunk_pipelining`).
  Needs an executor with `supports_chunk_pipelining()` (the native one) and a non-flat run. A
  node that runs as `incremental`/`backfill` with two or more chunks publishes, after each window's
  data commit, the highest ledger `seq` it has written (`ChunkProgress`). A not-yet-dispatched
  child whose parents are all either succeeded or running in this way is re-planned against a view
  that only shows committed ledger entries (`PipelinedView`) and, if it needs to run, runs an
  *early pass*; early passes advance the edge cursors only to the committed seq, so at-least-once
  delivery holds. The normal finish-to-start pass still follows (usually a `skip`, in which case the
  node is reported `succeeded` with the early passes' counts). One level deep; `rebuild` never runs
  early.

## Seams for other crates

- **Python connectors (worker agent).** A `PythonConnector` is a `Connector` (fetch) and/or an
  `InstanceConnector`; nothing else is needed for plain `fetch` plugins. Additional seams, all
  returned by the host's `ConnectorRegistry`:
  - `PluginSync` — a plugin module with its own `sync(pipe, **kw)` (v4 `Pipe.sync` returns its
    result instead of fetching);
  - `SyncHooks` — `@pre_sync_hook` / `@post_sync_hook`, called around every engine node sync;
  - `StreamSource` — `IS_STREAMING = True` connectors with `subscribe(pipe, sink)`; the engine
    supplies the micro-batching sink (`stream::run_sink`) and acknowledges after commit;
  - `LegacyStream` — the v4 pattern (a `sync()` that subscribes and returns, callbacks call
    `pipe.sync()`): `run(pipe, sink, stop)` keeps the worker alive and sends each callback sync
    through `sink` (a `stream::LegacySink`); the engine's sink (`native::NativeLegacySink`)
    writes the ledger before the commit and reports every sync of a graph node as a
    `stream_flushed{trigger: callback}` flush, and supervises the worker like a `subscribe`
    stream;
  - `is_python(keys)` — counts the node against `system.engine.python_workers`.
- **Leases.** `InstanceConnector::acquire_pipe_lease` / `release_pipe_lease`. An instance that
  answers `NotImplemented` falls back to interprocess file locks under
  `{root}/cache/engine_leases` with a one-time warning. A held lease is polled until
  `lease_wait`; one acquire error is retried after a poll (an acquire can race a crashed
  holder's lease expiring between its statements), a second consecutive one fails the node.
- **Crash injection (tests).** `Engine::with_faults(Arc<dyn FaultInjector>)`: `after_ledger_write`
  (before each data commit), `after_data_commit` (after each window's / flush's commit) and
  `before_cursor_advance` (after the whole node committed, right before its cursors and
  definition hash advance; `SyncExecutor::before_cursor_advance`).

## Engine state

Every durable row lives on an instance as a `temporary`, `static` internal pipe keyed on
`pipe_ref` (R5, R13): `mrsm:engine_state` (definition hashes, the pipe's instance),
`mrsm:engine_ledger` and `mrsm:engine_cursors` (the child's instance), `mrsm:engine_runs`,
`mrsm:engine_nodes` and `mrsm:engine_events` (the home instance; every event by default,
`system.engine.events: all`, so chunk progress can be replayed and streamed by the server's
`GET /engine/runs/{run_id}/events/stream`). Writes go through
`sync_pipe(check_existing=False, enforce=False, _check_temporary_tables=False)`. Per user sync
the engine adds one ledger insert (before the data commit, parents only) and one cursor upsert
for all of a node's edges (after the sync succeeds); `tests/state_cost.rs` measures this.

Dropping, deleting or fully clearing a pipe forgets its state (its ledger entries, the cursors
it is part of, its definition hash) on its own instance: the SQL and Valkey instance connectors
call `meerschaum_connector::pipe_ops::forget_engine_state` after success, and the Python façade
does the same for every `pipe.drop()` / `delete()` / `clear()` (`meerschaum/core/Pipeline/_engine.py`).
The state pipes' parameters live in `pipe_ops::engine_state_parameters` (`EnginePipe::parameters`
delegates to it). Cursors kept on another instance (cross-instance edges) are not forgotten.

## Tests

| Test | What it covers |
|---|---|
| `tests/golden.rs` | The planner golden fixtures (`tests/fixtures/*.json`). |
| `tests/v4_sync.rs` | A six-level chain and a diamond on SQLite over three phases of source changes, `--parents` and flat `-w 1`, against tables recorded from v4.1 (`tests/fixtures/v4_sync/`). |
| `tests/crash.rs` | Crashes between ledger write and data commit, and between data commit and cursor advance; `--resume`. |
| `tests/crash_restart.rs` | The same two points as real process deaths (a child process `exit`s inside the fault hook) followed by a restart, on SQLite and PostgreSQL-family (`MRSM_TEST_PG_URI`, schema `w9_engine`): the ledger holds the interrupted run's entry, no cursor moved, no run record, and the next run converges with every row exactly once. |
| `tests/forget_state.rs` | Drop, delete and full clear forget a pipe's ledger, cursors and hash (partial clears keep them); a reset chain plans like a first run. |
| `tests/w18_remote_state.rs` | Dropping a parent also forgets its ledger entries and parent cursors on its cross-instance children's instances (found through remote-state markers in `mrsm:engine_state`, GAP-002); an unreachable instance keeps its marker and is reported. |
| `tests/pg_state.rs` | On PostgreSQL-family (`MRSM_TEST_POSTGIS_URI`, schema `w9s_state`), a second graph run sees the first run's history, hashes, cursors and watermarks. |
| `tests/legacy_flush.rs` | A v4-pattern (`LegacyStream`) connector's callback syncs are `callback` flushes: events with rows and bounds, ledger entries, children triggered; syncs of pipes outside the graph stay bare. |
| `tests/leases.rs` | Lease contention across two processes on one SQLite instance (engine run level). |
| `tests/leases_backends.rs` | Lease contention across processes through `InstanceLeases` on SQLite, PostgreSQL-family (`MRSM_TEST_POSTGIS_URI`, schema `w7_engine`) and Valkey (`MRSM_TEST_VALKEY_URI`), including a killed holder that blocks only until its TTL. |
| `tests/stream_children.rs` | A stream flush triggers the stream's children, rate-limited by the child's `min_seconds`, coalescing flushes. |
| `tests/mqtt_stream.rs` | QoS 1 persistent-session redelivery and a crash mid-flush against a disposable Mosquitto (`MRSM_TEST_MQTT_URI`), through the `subscribe` path and the v4-pattern adapter path. |
| `tests/chunk_events.rs` | Per-chunk progress events in fetch order (rows, bounds, timing). |
| `tests/source_kwargs.rs` | `source_for_sync` receives the sync kwargs (begin, end, params, chunk settings, extras). |
| `tests/templates.rs` | `{{ Pipe(...) }}` resolution in graph, flat and bare syncs; stored parameters stay raw. |
| `tests/continuous.rs` | Lap-compatible `--loop --min-seconds`, `-l _negation`, `-w 1` order, v4's error cooldown, a supervised in-memory stream under `--listen`. |
| `tests/state_cost.rs` | The engine-state write path. |
| `tests/bare_sync.rs` | A bare `pipe.sync()` (R1). |
| `tests/python_glue.rs` | Manifest routing, native callbacks, scoped ledger observers, real plugin sync and custom fetch kwargs. |
| `tests/pipe_schedule.rs` | `parameters['schedule']` in `--loop`: a scheduled root only syncs when due (via a host-supplied `Cadence`), unscheduled pipes keep the lap cadence, an all-scheduled selection idles until due. |
| `tests/chunk_pipelining.rs` | `--chunk-pipelining`: a child pre-syncs a running parent's committed windows (extra `node_started`), tables identical to a sequential run, cursors advance exactly. |
| `tests/date_bounds.rs` | v4's `parse_date_bounds` on the fetch path: UTC-aware, naive and epoch-integer bounds. |

Regenerate the v4 fixture with
`scripts/parity/run.sh crates/meerschaum-engine/tests/fixtures/v4_sync/gen_v4_sync.py`.

## Worker integration

`python::PythonRegistry::new(native, pool, manifest, config)` adds Python sources, custom
instances, sync overrides, hooks and both stream modes to a native registry. Pass the process's
single `WorkerPool` and its cached startup `Manifest`. `callbacks()` exposes the same native
engine sync handler for plugin actions. Callback syncs preserve bounds, dtype enforcement,
autotime and native diffing. Subscribe pushes are acknowledged only after the engine sink
commits them; the engine owns restart policy for both stream modes.

Callbacks for the currently syncing pipe inherit its enclosing node's pre-commit delta observer;
callbacks targeting another pipe use the bare native sync path (R1). Nested Python calls use
the worker pool's bounded callback reserve, including with `python_workers: 1`. Call
`enable_legacy_streams()` for `--listen` to recognize v4 subscribe-and-return sync overrides;
ordinary batch sync overrides otherwise remain batch sources. `set_stream_kwargs()` carries
CLI flags and plugin arguments into either stream entry point.

Lease renewal failure aborts an in-flight node sync; a regression test checks that it does not
continue to the commit after losing its lease.

## Continuous mode and streams

- **Child triggering.** In `--listen` / `--loop`, every `stream_flushed` of a supervised
  `subscribe` stream marks the stream dirty; between laps the loop runs an ordinary engine run
  over `stream + descendants` (the stream is `observe`, the children read the ledger entries the
  flush wrote). A node runs at most once per `parameters['min_seconds']`, else
  `system.engine.debounce_seconds`, else the loop's `--min-seconds`; nodes still inside their
  window are held back with their descendants (`RunOptions::hold`) and run on a later trigger.
  Flushes that arrive while a trigger run executes or is held are coalesced. Each trigger emits
  `children_triggered{run_id, nodes, held, flushes}`. A v4-pattern (`LegacyStream`) worker's
  callback syncs are flushes too (trigger `callback`), so its children trigger the same way;
  only committed flushes trigger.
- **Flush events.** `stream_flushed` carries `rows`, `bytes`, `trigger` and (wave 9, additive)
  `success`, `begin` / `end` (the smallest and largest datetime-axis value of the changed rows),
  `inserted` / `updated` / `upserted` and `message`. `subscribe` streams only report committed
  flushes; `callback` flushes report failures too (`bytes` is 0).
- **Planned durations (Gantt).** The CPM schedule estimates each node with the EWMA of its
  successful runs (same action, else any action), else `EngineConfig::default_duration`
  (`system.engine.default_duration_seconds`, 30 s); a provisional `skip` is scheduled as an
  `incremental`. `NodeRow::planned_duration_seconds` keeps the estimate in memory (it is not a
  column of the static `mrsm:engine_nodes` schema); `EngineReader::run_detail` recomputes it
  from the history before the run, so every `GanttBar` has a `planned_end`.
- **Chunk events (§5.7).** `chunk_started` / `chunk_finished` carry `source`
  (`window` for planned windows, `fetch` for pieces of a fetched frame in **fetch order**),
  `rows`, `begin`, `end`, `inserted` / `updated` / `upserted` and `duration_ms`. All fields after
  `end` / `message` are additive and default when absent.
- **Source kwargs.** `ConnectorRegistry::source_for_sync(keys, args)` gets the full `SyncArgs`;
  `meerschaum_engine::sync::sync_kwargs(args)` is the map Python plugins receive (`extra` plus
  `begin`, `end`, `params`, `check_existing`, `enforce`, `force`, `workers`, `chunksize`,
  `retries`, `min_seconds`). Registries building native plugin sources use it.
