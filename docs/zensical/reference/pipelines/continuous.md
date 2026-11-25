# ♾️ Continuous Mode and Streams

Keep pipes syncing continuously with `--loop` (polling on an interval) or `--listen` (push-based streaming sources like MQTT, Kafka, AMQP, or webhooks).

## 🔁 `--loop`

`mrsm sync pipes --loop` runs each lap, sleeps `--min-seconds N` after the lap finishes, and repeats:

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

- **Graph-aware laps:** Add `--parents` or `--children` (or run a saved Pipeline with `mrsm sync pipelines daily --loop` / `mrsm start pipeline daily`) so unchanged children are skipped automatically and by default every lap is recorded in [run history](/reference/pipelines/state/#run-history).
- **Error cooldown:** After a failed lap, the loop backs off by `2 × (N + 1)` seconds before retrying.
- **Stopping:** Stop background loops with `mrsm stop jobs <name>` or press Ctrl-C in the foreground.

!!! tip "Give root pipes their own cadence"
    Add `schedule` to a root pipe's parameters (using the same syntax as `--schedule`, such as `every 6 hours` or `daily starting 03:00`). Under `--loop`, that root only syncs when due while other pipes keep syncing every lap; children with parents in the run always follow parent updates instead of their own schedule.

    ```yaml
    # In a root pipe's parameters
    schedule: "every 6 hours"
    ```

## 📡 Streaming Sources

Push-based sources (MQTT, Kafka, AMQP, webhooks) land raw rows in their own pipe while downstream pipes read bounded windows from it like any other parent.

Start a streaming source with `--listen`:

```bash
mrsm sync pipes -c plugin:mqtt -m sensors --listen -d --name mqtt-sensors
```

Without `--listen`, `sync pipes --parents` treats a streaming source as `observe` (checks its watermark and ledger without starting a listener).

### Writing a stream connector

There are two ways to build a streaming plugin:

1. **`IS_STREAMING = True` + `subscribe(pipe, sink)` (recommended):** The engine manages micro-batching, commit-then-ack, and restarts. Rust plugins use the same contract over Arrow streams.
2. **Legacy `sync()` callbacks (v4 compatible):** Keep existing plugins that subscribe inside `sync()` and call `pipe.sync()` from callbacks. Under `--listen`, the engine keeps the worker alive, tracks each callback sync as a flush, and triggers downstream children automatically.

### Share a Python stream worker

Set this on each pipe whose connector registers callbacks and returns promptly:

```yaml
stream:
  shared_worker: true
```

One listener then starts one Python process per connector label for those pipes. Each pipe keeps its own sink and acknowledgments. A worker exit or a subscription stopping restarts the remaining subscriptions together. Leave blocking subscriptions on their default dedicated workers.

### Micro-batching

The engine buffers incoming rows and flushes as soon as **any** threshold triggers. Configure `stream` in the pipe's parameters:

```yaml
stream:
  idle_ms: 250
  max_latency_ms: 2000
  max_rows: 5000
  max_bytes: 8388608
```

| Key | Default | Triggers a flush when… |
|---|---|---|
| `idle_ms` | `250` | No message arrives for this long (`auto` adapts between `50 ms` and `max_latency_ms`). |
| `max_latency_ms` | `2000` | The oldest buffered row reaches this age. |
| `max_rows` | `5000` | Buffer reaches this many rows. |
| `max_bytes` | `8 MB` | Buffer reaches this byte size. |

### Acknowledge after commit & supervision

- **At-least-once delivery:** `sink.flush()` commits to the target database *before* acknowledging the broker (MQTT QoS 1/2, Kafka offset, AMQP `basic.ack`, or webhook response). Redelivered rows sync with `check_existing=True` so duplicates are filtered out.
- **Automatic restarts:** If a stream disconnects or errors, the engine restarts it with backoff and logs throughput and restarts in the [Gantt view](/reference/pipelines/visualization/).

### Children of a stream

In continuous mode, downstream children trigger whenever their streaming parent flushes, rate-limited so rapid bursts coalesce into a single child run.

Rate-limit precedence (first set wins):

1. The child pipe's `min_seconds` parameter
2. `system.engine.debounce_seconds`
3. The loop's `--min-seconds`

Late-arriving messages write historical dirty ranges to the ledger so children backfill the exact affected windows.
