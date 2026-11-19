<style>
#btm {
  display: block;
  margin-left: auto;
  margin-right: auto;
}
@media screen and (max-width: 76.1875em) {
  #btm {
    width: 100%;
  }
}
@media screen and (min-width: 76.1875em) {
  #btm {
    width: 70%;
  }
}
</style>

# 📥 Syncing

!!! abstract inline end "Want to read more?"
    I wrote my [master's thesis](https://tigerprints.clemson.edu/all_theses/3647/) on comparing syncing strategies.

Meerschaum efficiently syncs time-series data in three basic ETL steps: **fetch**, **filter**, and **upsert**.

## Syncing Stages

The primary reason for syncing in this way is to take advantage of the properties of time-series data. Note that when writing your plugins, you only need to focus on getting data within `begin` and `end` bounds, and Meerschaum will handle the rest.

- **Fetch** (*Extract* and *Transform*)
  Because most new data will be newer than the previous sync, the fetch stage returns rows greater than the previous sync time (minus a backtracking window).

??? example "Example `fetch` query"

    === "SQL"
        ```sql
        WITH "definition" AS (
          SELECT datetime, id, value
          FROM remote_table
        )
        SELECT
          "datetime",
          "id",
          "value"
        FROM "definition"
        WHERE
          "datetime" >= '2021-06-23 14:52:00'
        ```

    === "Python"
        ```python
        sync_time = pipe.get_sync_time()
        fetch(begin=sync_time)
        ```

- **Filter** (*remove duplicates*)
  After fetching the latest rows, the difference is taken to remove duplicates.

!!! tip inline end "Skip filtering"
    To skip the filter stage, use `--skip-check-existing` or `#!python pipe.sync(check_existing=False)`.



??? example "Example `filter` query"

    === "SQL"
        ```sql
        SELECT
          "new"."datetime",
          "new"."id",
          "new"."value"
        FROM "new"
        LEFT JOIN "old" ON
          "new"."id" = "old"."id"
          AND
          "new"."datetime" = "old"."datetime"
        WHERE
          "old"."datetime" IS NULL
          AND
          "old"."id" IS NULL
        ```

    === "Pandas"

        ```python
        joined_df = pd.merge(
            new_df.fillna(pd.NA),
            old_df.fillna(pd.NA),
            how = 'left',
            on = None,
            indicator = True,
        )
        mask = (joined_df['_merge'] == 'left_only')
        delta_df = joined_df[mask]
        ```


  - **Upsert** (*Load*)

    Once new rows are fetched and filtered, they are inserted into the database table via the pipe's [instance connector](/reference/connectors/#instances-and-repositories). If [`upsert`](/reference/pipes/parameters/#upsert) is set in the pipe's parameters, then inserts and updates are performed in one query.

??? example "Example `upsert` queries"

    === "Inserts"
        ```sql
        COPY target_table (datetime, id, value)
        FROM STDIN
        WITH CSV
        ```

    === "Updates"
        ```sql
        UPDATE target_table AS f
        SET value = p.value
        FROM target_table AS t
        INNER JOIN patch_table AS p
          ON p.id = t.id
            AND
            p.datetime = p.datetime
        WHERE
          p.datetime = f.datetime
          AND
          p.id = f.id
        ```

    === "Upserts"
        ```sql
        INSERT INTO target_table (datetime, id, value)
        SELECT datetime, id, value
        FROM patch_table
        ON CONFLICT (datetime, id)
          DO UPDATE
          SET value = EXCLUDED.value
        ```

## Backtracking

Depending on your data source, sometimes data may be missed. When rows are backlogged or a pipe contains multiple data streams (i.e. an ID column), a simple sync might overlook old data.

### Add a backtrack interval

The backtrack interval is the overlapping window between syncs (default 1440 minutes).
<img src="/assets/diagrams/backtrack-minutes.png" alt="Meerschaum backtrack minutes interval" width="75%" style="margin: auto;" id="btm"/>

In the example above, there are four data streams that grow at separate rates, the dotted lines represent remote data which have not yet been synced. By default, only data to the right of the red line will be fetched, which will miss data for the "slower" IDs.

You can modify the backtrack interval under the key `fetch:backtrack_minutes`:

=== "`mrsm edit pipes`"

    ```yaml
    fetch:
      backtrack_minutes: 1440
    ```

=== "Python"

    ```python
    import meerschaum as mrsm
    pipe = mrsm.Pipe(
        'plugin:noaa', 'weather',
        instance = 'sql:local',
        parameters = {
            'fetch': {
                'backtrack_minutes': 1440,
            },
            'noaa': {
                'stations': ['KGMU', 'KCEU'],
            },
        },
    )
    ```

## Immutable synchronization strategies

Use `sync.immutable: true` to declare that each row's key and values are permanent after insertion. New rows may arrive late. For a time series, configure a non-null `datetime` column and a stable unique row key. For several series, configure `id` and use a key that distinguishes rows from each series. The destination must contain a subset of the source rows in the queried scope.

Set the strategy in the Pipe parameters, for example with `mrsm edit pipes`:

```yaml
columns:
  datetime: ts
  id: sensor_id
sync:
  immutable: true
  strategy: unbounded-simple
```

`backtrack` is the default and works for mutable pipes. `naive` also works with `sync.immutable: false`. The other strategies require an explicit immutability declaration. The declaration enables source query planning. You are responsible for keeping the source rows immutable. A normal sync still deduplicates by the configured row key, and the strategies do not delete rows.

The names and diagrams in [Chapter 3 of the thesis](https://meerschaum.io/files/pdf/thesis.pdf#page=52) distinguish incremental **syncs** from historical **verification syncs**. A target watermark is a timestamp already present in the destination. The diagram shows a row that arrives behind the watermark and a verification window that can recover it.

```mermaid
flowchart LR
    Old["Old history"] --> Late["Newly visible late row"]
    Late --> Mark["Target watermark"]
    Mark --> New["Newer rows"]
    Backtrack["Backtrack starts before the watermark"] -.-> Mark
    Repair["Verification revisits a historical day"] -.-> Late
```

### Incremental strategies

| Strategy | Rows requested |
| --- | --- |
| `backtrack` | Starts at the newest target timestamp minus `fetch.backtrack_minutes`. |
| `simple` | Starts at the newest target timestamp, including its boundary. Late rows older than that watermark need a verification sync. |
| `naive` | Reads the entire source query and merges by row key. It picks up edits anywhere in the source history and needs no change-tracking columns in the target. |
| `slow-id` | Starts at the oldest of the per-ID target watermarks for known IDs. A stalled series can widen the read for every known ID. |
| `append` | Builds one SQL `UNION ALL` of per-ID queries using each ID's own target watermark. |
| `join` | Left-joins the source query to a bound SQL `VALUES` table of target watermarks and filters each known ID at its own watermark. |

The three per-ID strategies include all source rows for IDs absent from the destination. They require direct PostgreSQL-compatible source and destination connectors and support at most 256 known IDs per scope. Use consistently typed text or integer IDs on both sides. They materialize the selected source rows before chunking the writes. As with `simple`, older arrivals within a known ID can still require historical verification.

### Historical verification

| Strategy | How it selects data |
| --- | --- |
| `iterative-simple` | Compares exact source and target row counts for each UTC day and fetches days with gaps. |
| `daily-row-count` | Gets the UTC day counts with a grouped query on each side. Fetches days with gaps. |
| `binary-search` | Counts a whole window first. Bisects unequal ranges until it isolates the missing data. |
| `iterative-cpisync` | Finds mismatching days and uses characteristic polynomial interpolation to request missing timestamps by series when the difference is sparse. |

By default these strategies include up to 30 UTC days ending on the latest source day. Set `sync.lookback_days` to change the horizon. Add the `unbounded-` prefix to use the complete source timestamp extent. `unbounded-simple` is an alias for `unbounded-iterative-simple`. If stray rows in the 1970s extend an otherwise recent series by decades, unbounded iterative sync prunes equal-count ranges instead of probing every intervening day. When the query budget is exhausted it fetches the remaining ranges.

Count-based strategies assume unique rows and a target that is a subset of the source. Equal counts can hide changed values or a deletion offset by an insertion. For mutable values, use `backtrack` for edits within a known recent horizon or `naive` to scan the full source history. Use [window replacement](#replacing-a-window) for an authoritative source that can remove rows, and choose a window that covers the changes.

Daily Row Count requires PostgreSQL, SQLite, or DuckDB on both sides. Iterative CPISync uses PostgreSQL-compatible connectors and a unique timestamp per ID at microsecond precision. For each day, both databases evaluate their characteristic polynomials at points outside the timestamp domain. Dividing the samples cancels the timestamps already in the target. Meerschaum fetches only source rows at roots of the remaining polynomial. Days above 1,024 source rows or 16 missing rows, duplicate timestamps within a series, or inconsistent samples fall back to an ordinary window fetch. [Characteristic polynomials and Iterative CPISync](https://meerschaum.io/files/pdf/thesis.pdf#page=57)

### Monthly correction

The monthly strategies do a simple incremental sync on ordinary runs and a historical verification on the first automatic run of each UTC calendar month. A successful correction records a checkpoint in the destination instance. If a correction fails, the next run retries it.

- `simple-monthly-naive`
- `simple-monthly-iterative-simple`
- `simple-monthly-daily-row-count`
- `simple-monthly-binary-search`
- `simple-monthly-iterative-cpisync`

The verification covers all available history. Monthly strategies require a registered pipe on a direct SQL instance and an unfiltered sync. They follow the same database requirements as their verification strategy. [Corrective syncs in the thesis](https://meerschaum.io/files/pdf/thesis.pdf#page=61)

Strategies apply to automatic source reads through `Pipe.sync()` and `mrsm sync pipes`. Explicit begin or end bounds keep their original meaning. The advanced strategies require a connector-managed fetch. Delegated execution uses a window authorized by Main. Automatic strategies other than `backtrack` require merge mode and existing-row checks.

## Replacing a window

A sync normally merges: new rows are inserted, changed rows updated, and rows missing from the
new data are left alone. When the new data is the whole truth for a time range (a corrected
day of readings, a rollup recomputed from its source), replace that range instead: the rows of
`[begin, end)` are deleted and the new rows inserted **in one transaction**, so readers never
see the window half-written and a failed write leaves the old rows in place.

=== "Python"

    ```python
    from datetime import datetime

    pipe.sync(
        df,
        begin=datetime(2024, 1, 1),
        end=datetime(2024, 1, 2),
        replace=True,
    )
    #  🎉 Replaced 96 rows with 94 rows.
    ```

=== "Bash"

    ```bash
    mrsm sync pipes -c sql:main -m weather \
      --begin 2024-01-01 --end 2024-01-02 --replace
    ```

=== "`mrsm edit pipes`"

    ```yaml
    fetch:
      replace: true
    ```

How the window is chosen:

- **With data** (`pipe.sync(df, replace=True)`): `begin` and `end` are the window. A bound you do
  not pass comes from the data: `begin` is the earliest datetime in `df` and `end` just after the
  latest, so every row you pass is inside. Rows outside `[begin, end)` are not written (the message
  says how many were skipped). `params` limits the window to the matching rows.
- **Without data** (a fetch): the window is the fetch's own `[begin, end)`, by default the sync
  time minus the [backtrack interval](#backtracking) onward. Set
  [`fetch: {replace: true}`](/reference/pipes/parameters/#fetchreplace) to replace every fetched
  window, chunked syncs and [Pipelines](/reference/pipelines/) chunks included (each chunk
  replaces its own window). Rows the source returns outside the window are not written. The first
  sync, into a table that does not exist yet, has nothing to replace: its chunks are synced one by
  one like any fetch.
- **An empty frame** with `begin` and `end` clears the window. With neither, the sync is refused,
  so an empty frame never clears a table by accident.
- **A pipe without a `datetime` column** has one window: the whole table (matching `params`).

The connectors' own `sync_pipe(pipe, df, replace=True, begin=..., end=..., params=...)` (SQL,
Valkey, `api:`) and the API's `POST /pipes/{connector}/{metric}/{location}/data?replace=true`
follow the same rules: a bound that is not given comes from the rows (`begin=None` over HTTP is
no bound on that side), and the API answers `400` for an empty body without `begin` and `end`.

In a [Pipeline](/reference/pipelines/), a replacing sync marks its whole window as changed, so its
children re-sync the window and drop the rows it deleted. A child's window ends at its parents'
latest data, so rows newer than a parent's new latest row stay in the child until the child is
rebuilt or cleared.

!!! note "Instances"
    SQL instances delete and insert in one transaction. Valkey has no transactions: it clears the
    window, then writes the rows. `api:` instances pass the window to the server, which replaces it
    in one transaction on its own instance.

## Verification Syncs

Occasionally it may be necessary to perform a more expensive verification sync across the pipe's entire interval. To do so, run `verify pipes` or `sync pipes --verify`:

=== "Bash"

    ```bash
    mrsm verify pipes
    # or
    mrsm sync pipes --verify
    ```

=== "Python"

    ```python
    pipe.verify()
    # or
    pipe.sync(verify=True)
    ```

A verification sync divides a pipe's interval into chunks and resyncs those chunks. Like the backtrack interval, you can configure the chunk interval under the keys `verify:chunk_minutes`:

=== "`mrsm edit pipes`"

    ```yaml
    verify:
      chunk_minutes: 43200
    ```

=== "Python"

    ```python
    import meerschaum as mrsm
    pipe = mrsm.Pipe(
        'plugin:noaa', 'weather',
        instance = 'sql:local',
        parameters = {
            'fetch': {
                'backtrack_minutes': 1440,
            },
            'verify': {
                'chunk_minutes': 43200,
            },
            'noaa': {
                'stations': ['KGMU', 'KCEU'],
            },
        },
    )
    ```

When run without explicit date bounds, verification syncs are bounded to a maximum interval (default 366 days). This value may be set under `verify:bound_days` (or minutes, days, hours, etc.):

=== "`mrsm edit pipes`"

    ```yaml
    verify:
      bound_days: 366
    ```

=== "Python"

    ```python
    import meerschaum as mrsm
    pipe = mrsm.Pipe(
        'plugin:noaa', 'weather',
        instance = 'sql:local',
        parameters = {
            'fetch': {
                'backtrack_minutes': 1440,
            },
            'verify': {
                'chunk_minutes': 43200,
                'bound_days': 366,
            },
            'noaa': {
                'stations': ['KGMU', 'KCEU'],
            },
        },
    )
    ```

## Deduplication Syncs

Although duplicates are removed during the filter stage of a sync, duplicate rows may still slip into your table if your data source returns duplicates.

Just like verification syncs, you can run `deduplicate pipes` to detect and delete duplicate rows. This works by deleting and resyncing chunks which contain duplicates.

!!! note inline end ""
    Your instance connector must provide either `clear_pipe()` or `deduplicate_pipe()` methods to use `pipe.deduplicate()`.

=== "Bash"

    ```bash
    mrsm deduplicate pipes
    # or
    mrsm sync pipes --deduplicate
    ```

=== "Python"

    ```python
    pipe.deduplicate()
    # or
    pipe.sync(deduplicate=True)
    ```

!!! tip "Combine `--verify` and `--deduplicate`."
    You run `mrsm sync pipes --verify --deduplicate` to run verification and deduplication syncs in the same process.
