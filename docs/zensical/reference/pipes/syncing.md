<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>
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
    I wrote my [master's thesis](https://tigerprints.clemson.edu/all_theses/3647/) on comparing syncing strategies. [Here are the presentation slides](https://meerschaum.io/files/pdf/slides.pdf) which summarize my findings.

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

In the example above, there are four data streams that grow at separate rates — the dotted lines represent remote data which have not yet been synced. By default, only data to the right of the red line will be fetched, which will miss data for the "slower" IDs.

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