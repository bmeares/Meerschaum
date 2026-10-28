# Sync new rows

Your [first Pipe](bootstrap.md) already has one reading. Add another to its source:

```bash
mrsm sql local "INSERT INTO readings VALUES (2, '2026-01-01 13:00:00', 22.0)"
mrsm sync pipes -i sql:local -m temperatures
mrsm show data -i sql:local -m temperatures
```

The Pipe now has **two readings**: 21.5 and 22.0. You used the same sync command; the Pipe picked up the new row.

```mermaid
flowchart LR
    A[New source rows] --> B[Sync the Pipe]
    B --> C[Up-to-date table]
```

## Why the timestamp matters

A Pipe uses its latest timestamp to choose where the next fetch begins. It looks back over a small overlap to catch late arrivals, then compares the fetched rows with the table.

Already-seen rows stay put. New rows are inserted. Changed rows can update their matching keys.

??? tip "What if data arrives late?"
    Increase the overlap with `fetch.backtrack_minutes` in the Pipe's parameters. For older history or a specific correction, sync an explicit window:

    ```bash
    mrsm sync pipes -i sql:local -m temperatures \
      --begin '2026-01-01' --end '2026-01-02'
    ```

    See [Syncing](../reference/pipes/syncing.md) for bounds, backtracking, and update options.

??? example "Watch a row arrive"
    <asciinema-player src="/assets/casts/v5-incremental.cast" size="small" preload="true" rows="18"></asciinema-player>

## Keep it running

For a simple repeating sync, keep the command in the foreground:

```bash
mrsm sync pipes -i sql:local -m temperatures --loop --min-seconds 60
```

Press **Ctrl+C** to stop. Later, a [background job](../reference/background-jobs.md) can give it a name and a schedule.

[Connect a transformation →](pipelines.md){ .md-button .md-button--primary }
