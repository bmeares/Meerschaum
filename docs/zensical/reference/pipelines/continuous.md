# ♾️ Continuous Mode and Streams

## 🔁 `--loop`

`mrsm sync pipes --loop` keeps the engine running. On a flat selection it behaves **exactly as v4's laps did**: every selected pipe syncs, then the process sleeps `--min-seconds N` *after the lap ends*, then the next lap starts. The `-c` / `-m` / `-l` / `-t` filters, `-l _negation` and `-w 1` work as before, so existing jobs need no change:

```bash
mrsm sync pipes -c plugin:acme -m readings --loop --min-seconds 300 -w 1 -d --name acme-readings
mrsm sync pipes -t nightly -l _staging -s 'daily starting 02:00' -d
```

```text
 ✅ Finished syncing Pipe('sql:src', 'raw', instance='sql:local'):
 …
 💬 Sleeping for 2 seconds.
 ✅ Finished syncing Pipe('sql:src', 'raw', instance='sql:local'):
```

After an error in a lap the loop waits the v4 error cooldown of `2 × (N + 1)` seconds before retrying. `--engine laps` is gone; `--loop` is the only long-running mode.

Add `--parents` / `--children` and every lap becomes an engine run: children only sync when a parent actually changed (a skipped child costs no query against its source). The engine records each lap in the [run history](/reference/pipelines/state/#run-history).

Stop a looping job with `mrsm stop jobs` or Ctrl-C.

!!! note "Cadence per root"

    `--min-seconds` sets the lap cadence of the whole selection. A root node (a pipe with no parent in the selection, or any pipe in a flat selection) can have its own cadence in `parameters['schedule']`, using the same grammar as `--schedule` / `-s` (`every 6 hours`, `daily starting 03:00`, ...). With `--loop`, such a pipe only syncs when its schedule is due: the other pipes keep syncing every lap, a lap in which nothing is due waits for the next fire time, and the sleep between laps is shortened to the next fire time when that is sooner than `--min-seconds`. A schedule that does not parse is ignored with a warning. `--schedule` itself keeps its v4 meaning: it runs the whole action on that schedule.

## 📡 Streaming Sources

Some sources push data continuously (MQTT, Kafka, AMQP, webhooks) instead of being fetched over a bounded window. They are a distinct kind of node.

**Ingestion stays separate from transforms.** A stream source lands raw rows in its own pipe. Downstream pipes read bounded windows from it like any other parent.

Run them with `--listen`:

```bash
mrsm sync pipes -c plugin:mqtt -m sensors --listen -d --name mqtt-sensors
```

Without `--listen`, a stream source is never run by `sync pipes --parents`: it gets the planner action `observe`, so reading its watermark and ledger is all that happens.

### Two ways to write a stream connector

- **`IS_STREAMING = True` with `subscribe(pipe, sink)`** (recommended). The engine owns the micro-batching, acknowledgement and restarts. See the [migration guide](/reference/migrating-to-v5/#plugin-authors) for the plugin side. Rust plugins use the same contract over Arrow streams.
- **The v4 pattern**: a `sync()` that subscribes and returns immediately, while callbacks call `pipe.sync()`. Keep your existing plugin as is. Under `--listen` the engine keeps the worker that ran `sync()` alive and forwards each callback `pipe.sync()` to the native sync path, supervising it like a `subscribe` stream. (The old `--loop --min-seconds 144000000` workaround still works.)

### Micro-batching

The engine's `sink` buffers rows and flushes on whichever trigger fires first. Configure it per pipe:

```yaml
stream:
  idle_ms: 250
  max_latency_ms: 2000
  max_rows: 5000
  max_bytes: 8388608
```

| Key | Default | Purpose |
|---|---|---|
| `idle_ms` | `250` | No message for this long: a burst ended, flush now. `auto` derives it from the moving average of message gaps, bounded to `[50 ms, max_latency_ms]` |
| `max_latency_ms` | `2000` | Age of the oldest buffered row; limits staleness, and loss for sources that cannot acknowledge |
| `max_rows` | `5000` | Caps memory and transaction size |
| `max_bytes` | 8 MB | Likewise |

Each flush performs one sync, writes one combined ledger entry per affected time chunk *before* committing, and emits a `stream_flushed` event with the row count, byte count and trigger.

### Acknowledge after commit

The sink's contract is that `flush()` commits, and only then does the source's acknowledgement run (MQTT QoS 1 or 2 with a persistent session and manual acknowledgement, Kafka offset commits, AMQP `basic.ack`, webhooks answering after the commit). A crash therefore redelivers instead of losing rows, and redelivered messages sync with `check_existing=True` even when the stream otherwise skips it. On a graceful stop the engine flushes and acknowledges before exiting. Sources that cannot acknowledge lose at most one `max_latency_ms` window on a crash.

### Supervision

If the stream stops, the engine restarts it with backoff and emits `stream_restarted` with the error. Throughput and restarts appear on the stream's lane in the [Gantt view](/reference/pipelines/visualization/).

### Children of a stream

In continuous mode a child of a stream source runs when the source flushes, rate-limited. Flushes that arrive while a child run is pending are coalesced into one run. The limit is the child's own `min_seconds`, else `system.engine.debounce_seconds`, else the loop's `--min-seconds`. Late or backloaded data produces ledger entries for historical chunks, and the children backfill exactly those chunks.
