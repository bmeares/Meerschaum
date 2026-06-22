# 🧹 Maintenance

Over time a pipe's target table accumulates dead rows, stale planner statistics, and uncompressed history. Meerschaum provides a family of maintenance actions to keep tables lean and queries fast. Each action selects pipes with the standard `-c` / `-m` / `-l` / `-i` / `-t` filters and prints a per-pipe results table.

| Action | Purpose | `Pipe` method |
|---|---|---|
| [`show sizes`](#disk-usage) | Report on-disk size per pipe | [`Pipe.get_size()`](https://docs.meerschaum.io/meerschaum.html#Pipe.get_size) |
| [`show partitions`](#partitions) | Report partition / chunk counts and width | — |
| [`compress pipes`](#compression) | Reclaim space by compressing history | [`Pipe.compress()`](https://docs.meerschaum.io/meerschaum.html#Pipe.compress) |
| [`decompress pipes`](#compression) | Reverse compression | [`Pipe.decompress()`](https://docs.meerschaum.io/meerschaum.html#Pipe.decompress) |
| [`vacuum pipes`](#vacuuming-and-analyzing) | Reclaim dead-row space | [`Pipe.vacuum()`](https://docs.meerschaum.io/meerschaum.html#Pipe.vacuum) |
| [`analyze pipes`](#vacuuming-and-analyzing) | Refresh planner statistics | [`Pipe.analyze()`](https://docs.meerschaum.io/meerschaum.html#Pipe.analyze) |
| [`partition pipes`](#repartitioning) | Rebuild to a new partition width | [`Pipe.repartition()`](https://docs.meerschaum.io/meerschaum.html#Pipe.repartition) |

!!! note "Instance connector support"
    These operations are backed by instance-connector methods (`get_pipe_size()`, `compress_pipe()`, `vacuum_pipe()`, etc.). The [`SQLConnector`](/reference/connectors/sql-connectors/) implements them per-flavor, and they are wired through `api:` instances. A connector that doesn't support an operation returns an informative failure rather than raising.

## 💾 Disk Usage

Run `show sizes` to list each pipe's target table size on disk, largest first:

```bash
mrsm show sizes -i sql:main
```

In Python, [`Pipe.get_size()`](https://docs.meerschaum.io/meerschaum.html#Pipe.get_size) returns the number of bytes (or `None` if the size can't be determined):

```python
import meerschaum as mrsm

pipe = mrsm.Pipe('demo', 'weather', instance='sql:main')
print(pipe.get_size())
# 1859584
```

Sizes are measured with each flavor's native query — TimescaleDB hypertable size functions, `pg_total_relation_size()` for PostgreSQL / PostGIS (summed across partitions), `data_length + index_length` for MySQL / MariaDB, reserved pages for MSSQL, and `dbstat` for SQLite.

## 🗜️ Compression

For large historical tables, compression can dramatically reduce disk usage. Run `compress pipes` to compress a pipe's history:

```bash
mrsm compress pipes -i sql:main -m weather
```

The mechanism depends on the flavor:

- **TimescaleDB** — enables the [Hypercore columnstore](https://www.tigerdata.com/docs/build/columnar-storage/setup-hypercore), installs a columnstore (compression) policy, and converts existing chunks. By default (the [`hypercore`](/reference/pipes/parameters/#hypercore) parameter) a new hypertable already has the columnstore enabled; the [`compress`](/reference/pipes/parameters/#compress) parameter additionally (re)installs a policy on sync.
- **MySQL / MariaDB** — `ROW_FORMAT=COMPRESSED`.
- **MSSQL** — `DATA_COMPRESSION = PAGE`.

!!! tip "One-shot compression"
    Pass `--no-policy` to compress existing data now **without** installing an ongoing policy (and, for `decompress pipes`, to decompress now while leaving the policy in place):

    ```bash
    # Compress existing chunks once; don't keep auto-compressing new data.
    mrsm compress pipes -m weather --no-policy

    # Decompress for a bulk backfill, then let the policy recompress on schedule.
    mrsm decompress pipes -m weather --no-policy
    ```

Reverse compression with `decompress pipes`, the inverse of `compress pipes`:

```bash
mrsm decompress pipes -i sql:main -m weather
```

For TimescaleDB this removes the columnstore policy, converts compressed chunks back to row-store, and disables the columnstore so future synced chunks stay uncompressed.

??? example "Mark a pipe for automatic compression"

    Set the [`compress`](/reference/pipes/parameters/#compress) parameter (a `bool` or a dict of `after` / `segmentby` / `orderby`) so a policy is installed automatically on sync:

    ```python
    import meerschaum as mrsm

    pipe = mrsm.Pipe(
        'demo', 'compress',
        instance='sql:main',
        columns={'datetime': 'ts', 'id': 'station'},
        compress={'after': '30 days'},
    )
    ```

## 🧽 Vacuuming and Analyzing

`vacuum pipes` reclaims space left by deleted or updated rows:

```bash
mrsm vacuum pipes -i sql:main
```

On the PostgreSQL family a plain `VACUUM` reclaims dead tuples internally but does not return space to the operating system — pass `--full` to run `VACUUM FULL`, which rewrites the table (taking an exclusive lock):

```bash
mrsm vacuum pipes -i sql:main --full
```

Other flavors fall back to their native mechanisms: `OPTIMIZE TABLE` for MySQL / MariaDB, an index rebuild for MSSQL, and `VACUUM` (of the database file) for SQLite.

`analyze pipes` refreshes the database planner's statistics so it chooses better query plans after a large sync. Unlike vacuuming, it does **not** reclaim disk space:

```bash
mrsm analyze pipes -i sql:main -m weather
```

## 🧩 Partitions

For pipes with [native range partitioning](/reference/connectors/sql-connectors/#native-range-partitioning) (or TimescaleDB hypertables), `show partitions` reports the partition / chunk count, the physical partition width, and the approximate number of rows per partition — a useful signal for tuning the width:

```bash
mrsm show partitions -i sql:main
```

### Repartitioning

`verify.chunk_minutes` is the [authoritative partition width](/reference/connectors/sql-connectors/#changing-the-partition-width), read at sync time. To change the width of an **existing** table, use `partition pipes` (don't just edit the parameter — a changed width laid over an existing grid produces overlapping partitions):

```bash
# Rebuild the 'weather' pipes to 7-day partitions.
mrsm partition pipes -i sql:main -m weather --chunk-minutes 10080
```

- **TimescaleDB** applies the new interval to **future** chunks via `set_chunk_time_interval()`; existing chunks keep their size (no rewrite).
- **PostgreSQL / PostGIS, MySQL / MariaDB, MSSQL** rebuild the table at the new width: the data is read, the table is dropped, and the data is re-synced.

!!! warning "Rebuild cost"
    The non-TimescaleDB rebuild reads the whole table into memory and briefly drops it before re-syncing. Run it during a maintenance window for large tables, and choose a sensible width up front to avoid repartitioning later.

## 🤖 Scheduling Maintenance

Like any action, maintenance can be scheduled as a [background job](/reference/background-jobs/). For example, compress and vacuum nightly:

```bash
mrsm compress pipes -i sql:main -f \
    + vacuum pipes -i sql:main -f \
    : -s daily \
    --name nightly_maintenance \
    -d
```
