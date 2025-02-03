# 🪵 Changelog

## 2.7.x Releases

This is the current release cycle, so stay tuned for future releases!

### v2.8.4

- **Allow for pattern matching in `allowed_instance_keys`.**  
  You may now generalize the instances exposed by the API by using Unix-style patterns in the list `system:api:permissions:instances:allowed_instance_keys`:

  ```json
  {
    "api": {
      "permissions": {
        "instances": {
          "allowed_instance_keys": [
            "valkey:*",
            "*_dev"
          ]
        }
      }
    }
  }
  ```

- **Return pipe attributes for the route `/pipes/{connector}/{metric}/{location}`.**  
  The API routes `/pipes/{connector}/{metric}/{location}` and `/pipes/{connector}/{metric}/{location}/attributes` both return pipe attributes.

- **Check entire batches for `verify rowcounts`.**  
  The command `verify rowcounts` will now check batch boundaries before checking row-counts for individual chunks. This should moderately increase performance.

- **Kill orphaned child processes when the parent job is killed.**  
  Jobs created with pipeline arguments should now kill associated child processes.

- **Add `--skip-hooks`.**  
  The flag `--skip-hooks` prevents any sync hooks from firing when syncing pipes.

- **Remove datetime rounding from `parse_schedule()`.**  
  Scheduled actions now behave as expected ― the current timestamp is no longer rounded to the nearest minute, which was causing issues with the `starting in` delay feature.

- **Fix `allowed_instance_keys` enforcement.**

### v2.8.3

- **Increase username limit to 60 characters.**
- **Add chunk retries to `Pipe.verify()`.**
- **Add instance keys to remaining pipes endpoints.**
- **Misc bugfixes.**

### v2.8.0 – v2.8.2

- **Add batches to `Pipe.verify()`.**  
  Verification syncs now run in sequential batches so that they may be interrupted and resumed. See `Pipe.get_chunk_bounds_batches()` for more information:

  ```python
  from datetime import timedelta
  import meerschaum as mrsm
  
  pipe = mrsm.Pipe('demo', 'get_chunk_bounds', instance='sql:local')
  bounds = pipe.get_chunk_bounds(
      chunk_interval=timedelta(hours=10),
      begin='2025-01-10',
      end='2025-01-15',
      bounded=True,
  )
  batches = pipe.get_chunk_bounds_batches(bounds, workers=4)
  mrsm.pprint(
      [
          tuple(
              (str(bounds[0]), str(bounds[1]))
              for bounds in batch
          )
          for batch in batches
      ]
  ) 
  # [
  #     (
  #         ('2025-01-10 00:00:00+00:00', '2025-01-10 10:00:00+00:00'),
  #         ('2025-01-10 10:00:00+00:00', '2025-01-10 20:00:00+00:00'),
  #         ('2025-01-10 20:00:00+00:00', '2025-01-11 06:00:00+00:00'),
  #         ('2025-01-11 06:00:00+00:00', '2025-01-11 16:00:00+00:00')
  #     ),
  #     (
  #         ('2025-01-11 16:00:00+00:00', '2025-01-12 02:00:00+00:00'),
  #         ('2025-01-12 02:00:00+00:00', '2025-01-12 12:00:00+00:00'),
  #         ('2025-01-12 12:00:00+00:00', '2025-01-12 22:00:00+00:00'),
  #         ('2025-01-12 22:00:00+00:00', '2025-01-13 08:00:00+00:00')
  #     ),
  #     (
  #         ('2025-01-13 08:00:00+00:00', '2025-01-13 18:00:00+00:00'),
  #         ('2025-01-13 18:00:00+00:00', '2025-01-14 04:00:00+00:00'),
  #         ('2025-01-14 04:00:00+00:00', '2025-01-14 14:00:00+00:00'),
  #         ('2025-01-14 14:00:00+00:00', '2025-01-15 00:00:00+00:00')
  #     )
  # ]
  ```

- **Add `--skip-chunks-with-greater-rowcounts` to `verify pipes`.**  
  The flag `--skip-chunks-with-greater-rowcounts` will compare a chunk's rowcount with the rowcount of the remote table and skip if the chunk is greater than or equal to the remote count. This is only applicable for connectors which implement `remote=True` support for `get_sync_time()`.

- **Add `verify rowcounts`.**  
  The action `verify rowcounts` (same as passing `--check-rowcounts-only` to `verify pipes`) will compare row-counts for a pipe's chunks against remote rowcounts. This is only applicable for connectors which implement `get_pipe_rowcount()` with support for `remote=True`.

- **Add `remote` to `pipe.get_sync_time()`.**  
  For pipes which support it (i.e. the `SQLConnector`), the option `remote` is intended to return the sync time of a pipe's fetch definition, like the option `remote` in `Pipe.get_rowcount()`.

- **Allow for the Web API to serve pipes from multiple instances.**  
  You can disable this behavior by setting `system:api:permissions:instances:allow_multiple_instances` to `false`. You may also explicitly allow which instances may be accessed by the WebAPI by setting the list `system:api:permissions:instances:allowed_instance_keys` (defaults to `["*"]`).

- **Fix memory leak for retrying failed chunks.**  
  Failed chunks were kept in memory and retried later. In resource-intensive syncs with large chunks and high failures, this would result in large objects not being freed and hogging memory. This situation has been fixed.

- **Add negation to job actions.**  
  Prefix a job name with an underscore to select all other jobs. This is useful for filtering out noise for `show logs`.

- **Add `Pipe.parent`.**  
  As a quality-of-life improvement, the attribute `Pipe.parent` will return the first member of `Pipe.parents` (if available).

- **Use the current instance for new tabs in the Webterm.**  
  Clicking "New Tab" will open a new `tmux` window using the currently selected instance on the Web Console.

- **Other webterm quality-of-life improvements.**  
  Added a size toggle button to allow for the webterm to take the entire page. 

- **Additional refactoring work.**  
  The API endpoints code has been cleaned up.

- **Added system configurations.**  
  New options have been added to the `system` configuration, such as `max_response_row_limit`, `allow_multiple_instances`, `allowed_instance_keys`.

## 2.7.x Releases

The 2.7 series greatly improved indexing, numerics support, added the `bytes` type, and allowed for bypassing dtype enforcement (`Pipe.enforce`) as well as introducing persistent Webterm sessions.

### v2.7.9 – v2.7.10

- **Add persistent Webterm sessions.**  
  On the Web Console, the Webterm will attach to a persistent terminal for the current session's user.

- **Reconnect Webterms after client disconnect.**  
  If a Webterm socket connection is broken, the client logic will attempt to reconnect and attach to the `tmux` session.

- **Add `tmux` sessions to Webterms.**  
  Webterm sessions now connect to `tmux` sessions (tied to the user accounts).
  Set `system:webterm:tmux:enabled` to `false` to disable `tmux` sessions.
  
- **Limit concurrent connections during `verify pipes`.**  
  To keep from exhausting the SQL connection pool, limit the number of concurrent intra-chunk connections.

- **Return the precision and scale from a table's columns and types.**  
  Reading a table's columns and types with `meerschaum.utils.sql.get_table_columns_types()` now returns the precision and scale for `NUMERIC` (`DECIMAL`) columns.

### v2.7.8

- **Add support for user-supplied precision and scale for `numeric` columns.**  
  You may now manually specify a numeric column's precision and scale:

  ```python
  import meerschaum as mrsm

  pipe = mrsm.Pipe(
      'demo', 'numeric', 'precision_scale',
      instance='sql:local',
      dtypes={'val': 'numeric[5,2]'},
  )
  pipe.sync([{'val': '123.456'}])
  print(pipe.get_data())
  #       val
  # 0  123.46
  ```

- **Serialize `numeric` columns to exact values during bulk inserts.**  
  Decimal values are serialized when inserting into `NUMERIC` columns during bulk inserts.

- **Return a generator when fetching with `SQLConnector`.**  
  To alleviate memory pressure, skip loading the entire dataframe when fetching.

- **Add `json_serialize_value()` to handle custom dtypes.**  
  When serializing documents, pass `json_serialize_value` as the default handler:

  ```python
  import json
  from decimal import Decimal
  from datetime import datetime, timezone
  from meerschaum.utils.dtypes import json_serialize_value

  print(json.dumps(
      {
          'bytes': b'hello, world!',
          'decimal': Decimal('1.000000001'),
          'datetime': datetime(2025, 1, 1, tzinfo=timezone.utc),
      },
      default=json_serialize_value,
      indent=4,
  ))
  # {
  #     "bytes": "aGVsbG8sIHdvcmxkIQ==",
  #     "decimal": "1.000000001",
  #     "datetime": "2025-01-01T00:00:00+00:00"
  # }
  ```

- **Fix an issue with the `WITH` keyword in pipe definitions for MSSQL.**  
  Previously, pipes with used with keyword `WITH` but not as a CTE (e.g. to specify an index) were incorrectly parsed.

### v2.7.7

- **Add actions `drop indices` and `index pipes`.**  
  You may now drop and create indices on pipes with the actions `drop indices` and `index pipes` or the pipe methods `drop_indices()` and `create_indices()`:

  ```python
  import meerschaum as mrsm

  pipe = mrsm.Pipe('demo', 'drop_indices', columns=['id'], instance='sql:local')
  pipe.sync([{'id': 1}])
  print(pipe.get_columns_indices())
  # {'id': [{'name': 'IX_demo_drop_indices_id', 'type': 'INDEX'}]}

  pipe.drop_indices()
  print(pipe.get_columns_indices())
  # {}

  pipe.create_indices()
  print(pipe.get_columns_indices())
  # {'id': [{'name': 'IX_demo_drop_indices_id', 'type': 'INDEX'}]}
  ```

- **Remove `CAST()` to datetime with selecting from a pipe's definition.**  
  For some databases, casting to the same dtype causes the query optimizer to ignore the datetime index.

- **Add `INCLUDE` clause to datetime index for MSSQL.**  
  This is to coax the query optimizer into using the datetime axis.

- **Remove redundant unique index.**  
  The two competing unique indices have been combined into a single index (for the key `unique`). The unique constraint (when `upsert` is true) shares the name but has the prefix `UQ_` in place of `IX_`.

- **Add pipe parameter `null_indices`.**  
  Set the pipe parameter `null_indices` to `False` for a performance improvement in situations where null index values are not expected.

- **Apply backtrack minutes when fetching integer datetimes.**  
  Backtrack minutes are now applied to pipes with integer datetimes axes.

### v2.7.6

- **Make temporary table names configurable.**  
  The values for temporary SQL tables may be set in `MRSM{system:connectors:sql:instance:temporary_target}`. The new default prefix is `'_'`, and the new default transaction length is 4. The values have been re-ordered to target, transaction ID, then label.

- **Add connector completions to `copy pipes`.**  
  When copying pipes, the connector keys prompt will offer auto-complete suggestions.

- **Fix stale job results.**  
  When polling for job results, the job result is dropped from in-memory cache to avoid overwriting the on-disk result.

- **Format row counts and seconds into human-friendly text.**  
  Row counts and sync durations are now formatted into human-friendly representations.

- **Add digits to `generate_password()`.**  
  Random strings from `meerschaum.utils.misc.generate_password()` may now contain digits.

### v2.7.3 – v2.7.5

- **Allow for dynamic targets in SQL queries.**  
  Include a pipe definition in double curly braces (à la Jinja) to substitute a pipe's target into a templated query.

  ```python
  import meerschaum as mrsm

  pipe = mrsm.Pipe('demo', 'template', target='foo', instance='sql:local')
  _ = pipe.register()

  downstream_pipe = mrsm.Pipe(
      'sql:local', 'template',
      instance='sql:local',
      parameters={
          'sql': "SELECT *\nFROM {{Pipe('demo', 'template', instance='sql:local')}}"
      },
  )

  conn = mrsm.get_connector('sql:local')
  print(conn.get_pipe_metadef(downstream_pipe))
  # WITH "definition" AS (
  #     SELECT *
  #     FROM "foo"
  # )
  # SELECT *
  # FROM "definition"
  ```

- **Add `--skip-enforce-dtypes`.**  
  To override a pipe's `enforce` parameter, pass `--skip-enforce-dtypes` to a sync.

- **Add bulk inserts for MSSQL.**  
  To disable this behavior, set `system:connectors:sql:bulk_insert:mssql` to `false`. Bulk inserts for PostgreSQL-like flavors may now be disabled as well.

- **Fix altering multiple column types for MSSQL.**  
  When a table has multiple columns to be altered, each column will have its own `ALTER TABLE` query.

- **Skip enforcing custom dtypes when `enforce=False`.**  
  To avoid confusion, special Meerschaum data types (`numeric`, `json`, etc.) are not coerced into objects when `enforce=False`.

- **Fix timezone-aware casts.**  
  A bug has been fixed where it was possible to mix timezone-aware and -naive casts in a single query. This patch ensures that this no longer occurs.

- **Explicitly cast timezone-aware datetimes as UTC in SQL syncs.**  
  By default, timezone-aware columns are now cast as time zone UTC in SQL. This may be skipped by setting `enforce` to `False`.

- **Added virtual environment inter-process locks.**  
  Competing processes now cooperate for virtual environment verification, which protects installed packages.

### v2.7.0 – v2.7.2

- **Introduce the `bytes` data type.**  
  Instance connectors which support binary data (e.g. `SQLConnector`) may now take advantage of the `bytes` dtype. Other connectors (e.g. `ValkeyConnector`) may use `meerschaum.utils.dtypes.serialize_bytes()` to store binary data as a base64-encoded string.

  ```python
  import meerschaum as mrsm

  pipe = mrsm.Pipe(
      'demo', 'bytes',
      instance='sql:memory',
      dtypes={'blob': 'bytes'},
  )
  pipe.sync([
      {'blob': b'hello, world!'},
  ])

  df = pipe.get_data()
  binary_data = df['blob'][0]
  print(binary_data.decode('utf-8'))
  # hello, world!

  from meerschaum.utils.dtypes import serialize_bytes, attempt_cast_to_bytes
  df['encoded'] = df['blob'].apply(serialize_bytes)
  df['decoded'] = df['encoded'].apply(attempt_cast_to_bytes)
  print(df)
  #                blob               encoded           decoded
  # 0  b'hello, world!'  aGVsbG8sIHdvcmxkIQ==  b'hello, world!'
  ```

- **Allow for pipes to use the same column for `datetime`, `primary`, and `autoincrement=True`.**  
  Pipes may now use the same column as the `datetime` axis and `primary` with `autoincrement` set to `True`.

  ```python
  pipe = mrsm.Pipe(
      'demo', 'datetime_primary_key', 'autoincrement',
      instance='sql:local',
      columns={
          'datetime': 'Id',
          'primary': 'Id',
      },
      autoincrement=True,
  )
  ```

- **Only join on `primary` when present.**  
  When the index `primary` is set, use the column as the primary joining index. This will improve performance when syncing tables with a primary key.


- **Add the parameter `enforce`.**  
  The parameter `enforce` (default `True`) toggles data type enforcement behavior. When `enforce` is `False`, incoming data will not be cast to the desired data types. For static datasets where the incoming data is always expected to be of the correct dtypes, then it is recommended to set `enforce` to `False` and `static` to `True`.


  ```python
  from decimal import Decimal
  import meerschaum as mrsm

  pipe = mrsm.Pipe(
      'demo', 'enforce',
      instance='sql:memory',
      enforce=False,
      static=True,
      autoincrement=True,
      columns={
          'primary': 'Id',
          'datetime': 'Id',
      },
      dtypes={
          'Id': 'int',
          'Amount': 'numeric',
      },
  )
  pipe.sync([
      {'Amount': Decimal('1.11')},
      {'Amount': Decimal('2.22')},
  ]) 

  df = pipe.get_data()
  print(df)
  #    Id Amount
  # 0   1   1.11
  # 1   2   2.22
  ```

- **Create the `datetime` axis as a clustered index for MSSQL, even when a `primary` index is specififed.**  
  Specifying a `datetime` and `primary` index will create a nonclustered `PRIMARY KEY`. Specifying the same column as both `datetime` and `primary` will create a clustered primary key (tip: this is useful when `autoincrement=True`).

- **Increase the default chunk interval to 43200 minutes.**  
  New hypertables will use a default chunksize of 30 days (43200 minutes).

- **Virtual environment bugfixes.**  
  Existing virtual environment packages are backed up before re-initializing a virtual environment. This fixes the issue of disappearing dependencies.

- **Store `numeric` as `TEXT` for SQLite and DuckDB.**  
  Due to limited precision, `numeric` columns are now stored as `TEXT`, then parsed into `Decimal` objects upon retrieval.

- **Show the Webterm by default when changing instances.**  
  On the Web Console, changing the instance select will make the Webterm visible.

- **Improve dtype inference.** 

## 2.6.x Releases

The 2.6 series added the `primary` index, `autoincrement`, and migrated to timezone-aware datetimes by default, as well as many quality-of-life improvements, especially for MSSQL.

### v2.6.17

- **Add relative deltas to `starting in ` scheduler syntax.**  
  You may specify a delta in the job scheduler `starting` syntax:

  ```
  mrsm sync pipes -s 'daily starting in 30 seconds'
  ```

- **Fix `drop pipes` for pipes on custom schemas.**  
  Pipes created under a specific schema are now correctly dropped.

- **Enhance editing pipeline jobs.**  
  Pipeline jobs now provide the job label as the default text to be edited. Pipeline arguments are now placed on a separate line to improve legibility.

- **Disable the progress timer for jobs.**  
  The `sync pipes` progress timer will now be hidden when running through a job.

- **Unset `MRSM_NOASK` for daemons.**  
  Now that jobs may accept user input, the environment variable `MRSM_NOASK` is no longer needed for jobs run as daemons (executor `local`).

- **Replace `Cx_Oracle` with `oracledb`.**  
  The Oracle SQL driver is no longer required now that the default Python binding for Oracle is `oracledb`.

- **Fix Oracle auto-incrementing for good.**  
  At long last, the mystery of Oracle auto-incrementing identity columns has been laid to rest. 

### v2.6.15 – v2.6.16

- **Fix inplace syncs without a `datetime` axis.**  
  A bug introduced by a performance optimization has been fixed. Inplace pipes without a `datetime` axis will skip searching for date bounds. Setting `upsert` to `true` will bypass this bug for previous releases.

- **Skip invoking `get_sync_time()` for pipes without a `datetime` axis.**  
  Invoking an instance connector's `get_sync_time()` method will now only occur when `datetime` is set.

- **Remove `guess_datetime()` check from `SQLConnector.get_sync_time()`.**  
  Because sync times are only checked for pipes with a dedicated `datetime` column, the `guess_datetime()` check has been removed from the `SQLConnector.get_sync_time()` method.

- **Skip persisting default `target` to parameters.**  
  The default target table name will no longer be persisted to `parameters`. This helps avoid accidentally setting the wrong target table when copying pipes.

- **Default to "no" for syncing data when copying pipes.**  
  The action `copy pipes` will no longer sync data by default, instead requiring an explicit yes to begin syncing.

- **Fix the "Update query" button behavior on the Web Console.**  
  Existing but null keys are now accounted for when update a SQL pipe's query.

- **Fix another Oracle autoincrement edge case.**  
  Resetting the autoincrementing primary key value on Oracle will now behave as expected.

### v2.6.10 – v2.6.14

- **Improve datetime timezone-awareness enforcement performance.**  
  Datetime columns are only parsed for timezone awareness if the desired awareness differs. This drastically speeds up sync times.

- **Switch to `tz_localize()` when stripping timezone information.**  
  The previous method of using a lambda to replace individual `tzinfo` attributes did not scale well. Using `tz_localize()` can be vectorized and greatly speeds up syncs, especially with large chunks.

- **Add `enforce_dtypes` to `Pipe.filter_existing()`.**  
  You may optionally enforce dtype information during `filter_existing()`. This may be useful when implementing custom syncs for instance connectors. Note this may impact memory and compute performance.

  ```python
  import meerschaum as mrsm
  import pandas as pd

  pipe = mrsm.Pipe('a', 'b', instance='sql:local')
  pipe.sync([{'a': 1}])

  df = pd.DataFrame([{'a': '2'}])

  ### `enforce_dtypes=True` will suppress the differing dtypes warning.
  unseen, update, delta = pipe.filter_existing(df, enforce_dtypes=True)
  print(delta)
  ```

- **Fix `query_df()` for null parameters.**  
  This is useful for when you may use `query_df()` with only `select_columns` or `omit_columns`.

- **Fix autoincrementing IDs for Oracle SQL.**

- **Enforce security settings for creating jobs.**  
  Jobs and remote actions will only be accessible to admin users when running with `--secure` (`system:permissions:actions:non_admin` in config).

### v2.6.6 – v2.6.9

- **Improve metadata performance when syncing.**  
  Syncs via the SQLConnector now cache schema and index metadata, speeding up transactions.
  
- **Fix upserts for MySQL / MariaDB.**  
  Upserts in MySQL and MariaDB now use `ON DUPLICATE` instead of `REPLACE INTO`.

- **Fix dtype detection for index columns.**  
  A bug where new index columns were incorrectly created as `INT` has been fixed.

- **Delete old keys when dropping Valkey pipes.**  
  Dropping a pipe from Valkey now clears all old index keys.

- **Fix timezone-aware enforcement bugs.**

### v2.6.1 – v2.6.5

- **Add `Pipe.tzinfo`.**  
  Check if a pipe is timezone-aware with `tzinfo`:

  ```python
  import meerschaum as mrsm

  pipe = mrsm.Pipe(
      'demo', 'timezone', 'aware',
      columns={'datetime': 'dt'},
  )
  print(pipe.tzinfo)
  # UTC

  pipe = mrsm.Pipe(
      'demo', 'timezone', 'naive',
      columns={'datetime': 'dt'},
      dtypes={'dt': 'datetime64[ns]'},
  )
  print(pipe.tzinfo)
  # None
  ```

- **Improve timezone enforcement when syncing.**
- **Fix inplace syncs with `upsert=True`.**
- **Fix timezone-aware datetime truncation for MSSQL**
- **Fix timezone detection for existing timezone-naive tables.**

### v2.6.0

- **Enforce a timezone-aware `datetime` axis by default.**  
  Pipes now enforce timezone-naive datetimes as UTC, even if the underlying column type is timezone-naive. To use datetime-naive datetime axes, you must explicitly set the `dtype` to `datetime64[ns]`.

- **Designate the index name `primary` for primary keys.**  
  Like the `datetime` index, the `primary` index is used for joins and will be created as the primary key in new tables.

  ```python
  import meerschaum as mrsm

  pipe = mrsm.Pipe(
      'demo', 'primary_key',
      columns={'primary': 'pk'},
      instance='sql:local',
  )
  ### Raises a `UNIQUE` constraint failure:
  pipe.sync([
      {'pk': 1},
      {'pk': 1},
  ])
  ```

- **Add `autoincrement` to `Pipe.parameters`**  
  Like `upsert`, you may designate an incremental integer primary key by setting `autoincrement` to `True` in the pipe parameters. Note that `autoincrement` will be `True` if you specify a `primary` index but do not specify a dtype or pass into the initial dataframe. This is only available for `sql` pipes.

  ```python
  import meerschaum as mrsm

  pipe = mrsm.Pipe(
      'demo', 'autoincrement',
      columns={'primary': 'id'},
      autoincrement=True,
      instance='sql:local',
  )
  pipe.sync([
      {'color': 'red'},
      {'color': 'blue'},
  ])
  print(pipe.get_data())
  #    id color
  # 0   1   red
  # 1   2  blue

  pipe.sync([
      {'color': 'green', 'id': 1},
  ])
  print(pipe.get_data())
  #    id  color
  # 0   1  green
  # 1   2   blue

  ```
  
- **Add option `static` to `Pipe.parameters` to disable schema modification.**  
  Set `static` to `True` in a pipe's parameters to prevent any modification of the column's data types.

  ```python
  import meerschaum as mrsm

  pipe = mrsm.Pipe(
      'demo', 'static',
      columns={
          'primary': 'id',
          'datetime': 'id',
      },
      dtypes={
          'meta': 'json',
          'id': 'int',
      },
      parameters={
          'static': True,
          'upsert': True,
          'autoincrement': True,
      },
  )
  pipe.sync([{'id': 1, 'meta': {'foo': 'bar'}}])

  ### Abort syncing new columns when `static` is set. 
  pipe.sync([{'id': 1, 'color': 'blue'}])
  
  mrsm.pprint(pipe.get_columns_types())
  # {'id': 'BIGINT', 'meta': 'JSONB'}
  ```

- **Add `get_create_table_queries()` to build from `dtypes` dictionaries.**  
  You may get a `CREATE TABLE` query from a `dtypes` dictionary (in addition to a `SELECT` query). The function `meerschaum.utils.sql.get_create_table_query()` now also accepts an arguments `primary_key` and `autoincrement` to designate a primary key column. 

  ```python
  from meerschaum.utils.sql import get_create_table_queries

  queries = get_create_table_queries(
      {
          "id": "int",
          "is_active": "bool",
          "customer_id": "uuid",
          "metadata": "json",
      },
      'new_table',
      'postgresql',
      primary_key="id",
      autoincrement=True,
  )
  print(queries[0])
  # CREATE TABLE "new_table" (
  #     "id" BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY NOT NULL,
  #     "is_active" BOOLEAN,
  #     "customer_id" UUID,
  #     "metadata" JSONB
  # ) 
  ```

- **Create a multi-column index for columns defined in `Pipe.columns`.**  
  To disable this behavior, set `unique` to `None` in `Pipe.indices`. 

- **Default to `BIT` for boolean columns in MSSQL.**  
  The previous workaround was to store `bool` columns as `INT`. This change now defaults to `BIT` when creating new tables. Boolean columns cannot be nullable for MSSQL.

- **Improve file protection in `edit config`.**  
  Writing an invalid config file will now stop you before committing the changes. The previous behavior would lead to data loss.

- **Catch exceptions when creating chunk labels.**  
  If a datetime bound cannot be determined for a chunk, return `pd.NA`.

## 2.5.x Releases

The 2.5.x series was short and sweet, primarily introducing features relating to `Pipe.indices`.

### v2.5.1

- **Update index information in the pipe card.**  
  The `Indices` section of the pipe card on the web console includes more detailed information, such as composite and multi-column indices.

- **Print action results during scheduled jobs.**  
  Scheduled actions now print their result success tuples after firing.

- **Other bugfixes.**  
  A few bugs from the migration of `APScheduler` to internal management have been fixed.

### v2.5.0

- **Add `indices` to `Pipe.parameters`.**  
  You may now explicitly state the indices to be created by defining `indices` (or `indexes`) in `Pipe.parameters` (or the `Pipe` constructor for your convenience).

  ```python
  import meerschaum as mrsm
  
  pipe = mrsm.Pipe(
      'software', 'versions',
      instance='sql:local',
      columns=['major', 'minor', 'patch'],
      indices={
          'all': ['major', 'minor', 'patch'],
      },
  )
  mrsm.pprint(pipe.get_indices())
  # {
  #     'major': 'IX_software_versions_major',
  #     'minor': 'IX_software_versions_minor',
  #     'patch': 'IX_software_versions_patch',
  #     'all': 'IX_software_versions_major_minor_patch'
  # } 
  ```

  You may also use the key `index_template` to change the format of the generated index names (defaults to `IX_{target}_{column_names}`, where `target` is the table name and `column_names` consists of all of the index's columns joined by an underscore).

  ```python
  pipe.parameters['index_template'] = "foo_{target}_{column_names}"
  mrsm.pprint(pipe.get_indices())
  # {'all': 'foo_software_versions_major_minor_patch'} 
  ```

- **Enable chunking for MSSQL**  
  To improve memory usage, `chunksize` is now accepted by `SQLConnector.read()` for the flavor `mssql`.

- **Disable `pyodbc` pooling.**  
  To properly recycle the connection pool in the SQLAlchemy engine, the internal `pyodbc` pooling must be disabled. See the [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/20/dialects/mssql.html#pyodbc-pooling-connection-close-behavior).

- **Bugfixes**  
  Other miscellaneous bugfixes have been included in this release, such as resolving broken imports during certain edge cases.

## 2.4.x Releases

The 2.4.x series added the `ValkeyConnector`, relative `--begin` and `--end`, pipeline timeouts, and improved MSSQL support.

### v2.4.13

- **Add `--timeout` to pipeline arguments.**  
  You may now designate the maximum number of seconds to run a pipeline with `--timeout`. This will run the entire pipeline in a subprocess rather than a persistent session.

  ```bash
  mrsm sync pipes + clear pipes --end '1 month ago' : -s daily --timeout 3600
  ```

- **Add auto-complete to `edit jobs` and `bootstrap jobs`.**

- **Improve the editing experience for `edit jobs` and `bootstrap jobs`.**

- **Fixed plugin detection for Python 3.9.**

### v2.4.12

- **Add the actions `edit jobs` and `bootstrap jobs`.**  
  The action `edit jobs` lets you easily tweak the arguments for an existing job, so there's no need to delete and recreate jobs. The `bootstrap jobs` wizard also gives you a chance to review your changes before starting a job.

- **Fix nested CTEs for MSSQL.**  
  Pipes may now use definitions containing a `WITH` clause for Microsoft SQL Server.

- **Added `wrap_query_with_cte` to `meerschaum.utils.sql`.**  
  Reference a subquery in an encapsulating parent query, even if the subquery contains CTEs itself.

  ```python
  from meerschaum.utils.sql import wrap_query_with_cte
  
  sub_query = """
  WITH [foo] AS (
    SELECT 1 AS [val]
  )
  SELECT ([val] * 2) AS [newval]
  FROM [foo]
  """

  parent_query = "SELECT (newval * 3) FROM [src]"

  query = wrap_query_with_cte(
      sub_query,
      parent_query,
      'mssql',
      cte_name='src',
  )
  print(query)
  # WITH [foo] AS (
  #   SELECT 1 AS [val]
  # ),
  # [src] AS (
  # SELECT ([val] * 2) AS [newval]
  # FROM [foo]
  #
  # )
  # SELECT (newval * 3) FROM [src] 
  ```

- **Fix `--yes` when running in background jobs.**  
  The flags `--yes` and `--noask` are now properly handled when running a background job which contains prompts.

- **Add an external page for jobs to the Web Console.**  
  Like the shareable `/pipes/` links, you may now link to a specific job at the path `/dash/job/{name}`. Click the name of the job on the card to open a job in a new tab.

- **Preserve the original values for `--begin` and `--end`.**  
  When creating jobs in the shell, the original string values for `--begin` and `--end` will be preserved, such as in the case of `--begin 1 month ago`.
  
- **Fix `Pipe` formatting for small terminals.**  
  Pipes with long names are now properly rendered in small terminal windows.

- **Enable shell suggestions for chained actions.**  
  The shell auto-complete now works with chained actions.

### v2.4.9 – v2.4.11

- **Add relative formats to `--begin` and `--end`.**  
  The flags `--begin` and `--end` support values in the format `[N] [unit] ago`:
  
  ```bash
  mrsm sync pipes --begin '3 days ago'
  ```

  Add a second delta format (recommended to be denoted by the keyword `rounded`) to round the timestamp to a clean value:

  ```bash
  mrsm clear pipes --end '1 month ago rounded 1 day'
  ```

  Supported units are `seconds`, `minutes`, `hours`, `days`, `weeks`, `months` (`ago` only), and `years`.

- **Respect `--begin`, `--end`, and `--params` in `show rowcounts`.**  
  The flags `--begin`, `--end`, and `--params` are now handled in the action `show rowcounts`.

- **Fix an issue with `Pipe.get_backtrack_data()`.**  
  An incorrect calculation was fixed the produce the correct backtrack interval.

### v2.4.8

- **Allow for syncing against `DATETIMEOFFSET` columns in MSSQL.**  
  When syncing an existing table with a `DATETIMEOFFSET` column, timestamps correctly coerced into timezone-naive UTC timestamps. Note this behavior will likely change to timezone-aware-by-default in a future release.

- **Default to `DATETIME2` for MSSQL.**  
  To preserve precision, MSSQL now creates datetime columns as `DATETIME2`.

- **Remove temporary tables warnings.**  
  Failure to drop temporary tables no longer raises a warning (added `IF EXISTS` check).

- **Fix an issue with the `sql` action.**

- **Fix UUID support for SQLite, MySQL / MariaDB.**

- **Set `IS_THREAD_SAFE` to `False` for Oracle.**

### v2.4.6 ― v2.4.7

- **Prefix temporary tables with `##`.**  
  Temporary tables are now prefixed with `##` to take advantage of `tempdb` in MSSQL.

- **Add the `uuid` dtype.**  
  The `uuid` dtype adds support for Python `UUID` objects and maps to the appropriate `UUID` data type per SQL flavor (e.g. `UNIQUEIDENTIFIER` for `mssql`).

- **Add `upsert` support to MSSQL.**  
  Setting `upsert` in a pipe's parameters will now upsert rows in a single transaction (via a `MERGE` query).

- **Add `SQLConnector.get_connection()`.**  
  To simplify connection management, you may now obtain an active connection with `SQLConnector.get_connection()`. To force a new connection, pass `rebuild=True`.

- **Improve session management for MSSQL.**  
  Transactions and connections are now more gracefully handled when working with MSSQL.

### v2.4.2 — v2.4.5

- **Fix `bootstrap connectors`.**  
  Revert a breaking change to the `bootstrap connectors` wizard.

- **Respect disabling `uv` for package installation.**  
  Setting `system:experimental:uv_pip` to `false` will now disable `uv` for certain.

- **Default to a query string for `options` when bootstrapping MSSQL connectors.**  
  Although dictionaries are supported for `options`, using a dictionary as a default was breaking serialization. The default for `options` is now the string `driver=ODBC Driver 17 for SQL Server&UseFMTONLY=Yes`.

- **Default to MSSQL ODBC Driver 18.**  
  The default driver to be used by MSSQL connectors is version 18.

### v2.4.1

- **Add `instance` to the external pipe links.**  
  When sharing pipe links on the Web Console, the instance will now be included in the URL.

- **Fix an issue with remote actions.**  
  An import error has been patched.

### v2.4.0

- **Add `valkey` instance connectors.**  
  Introducing a new first-class instance connector: the `ValkeyConnector`. [Valkey](https://valkey.io/), a fork of Redis, is a high-performance in-memory database often used for caching. The `valkey` service has been added to the Meerschaum stack and is accessible via the built-in connector `valkey:main`.

- **Cache Web Console sessions in Valkey when running with `--production`.**  
  Starting the web API with `--production` will now store sessions in `valkey:main`. This results in a smoother experience in the event of a web server restart. By default, sessions expire after 30 days.
  > You may disable this behavior by setting `system:experimental:valkey_session_cache` to `false`.

- **Allow for a default executor.**  
  Setting the key `meerschaum:executor` will set the default executor (overriding the check for `systemd`). This is useful for defaulting to remote actions in a multi-node deployment.

- **Allow querying for `None` in `query_df()`.**  
  You may now query for null rows:

  ```python
  import pandas as pd
  from meerschaum.utils.dataframe import query_df

  df = pd.DataFrame({'a': [1, 2, pd.NA]}).astype('Int64')

  result = query_df(df, {'a': None})
  print(result)
  #       a
  # 2  <NA>
  
  result = query_df(df, {'a': [None, 1]})
  print(result)
  #       a
  # 0     1
  # 2  <NA> 
  ```

- **Improve `query_df()` performance.**  
  Dataframe vlues are no longer serialized by default in `query_df()`, meaning that parameters must match the data type. Pass `coerce_types=True` to restore legacy behavior.

- **Add `Pipe.copy_to()`.**  
  Copy pipes between instances with `Pipe.copy_to()`:

  ```python
  import meerschaum as mrsm

  pipe = mrsm.Pipe('plugin:noaa', 'weather')
  pipe.copy_to('valkey:main')
  ```

- **Add `include_unchanged_columns` to `Pipe.filter_existing()`.**  
  Pass `include_unchanged_columns=True` to return entire documents in the update dataframe. This is useful for situations where you are unable to update individual fields:

  ```python
  import meerschaum as mrsm
  import pandas as pd

  pipe = mrsm.Pipe('a', 'b', columns=['id'])
  pipe.sync([
      {'animal': 'cat', 'name': 'Meowth', 'id': 1},
      {'animal': 'dog', 'name': 'Fluffy', 'id': 2},
  ])

  df = pd.DataFrame([{'id': 1, 'breed': 'tabby'}])

  unseen, update, delta = pipe.filter_existing(df)
  print(update)
  #    id  breed
  # 0   1  tabby
  
  unseen, update, delta = pipe.filter_existing(
      df,
      include_unchanged_columns=True,
  )
  print(update)
  #   animal    name  id  breed
  # 0    cat  Meowth   1  tabby
  ```

- **Add a share button to the Pipe card.**  
  On the web dashboard, you may now more easily share pipes by clicking the "share" icon and copying the URL. This opens the pipe card in a new, dedicated tab.

- **Add `OPTIONAL_ATTRIBUTES` to connectors.**  
  Connectors may now set `OPTIONAL_ATTRIBUTES`, which will add skippable prompts in `bootstrap connector`.

- **Remove progress bar for syncing via remote actions.**  
  Executing `sync pipes` remotely will no longer print the timer progress bar.

- **Fix bug with `stack` in the shell.**  
  Note that `stack` actions may not be chained.

- **Fix scheduler dependency.**  
  To fix the installation of `APScheduler`, `attrs` is now held back to 24.1.0.

## 2.3.x Releases

The 2.3 series was short but brought significant improvements, notably the `Job` API, remote jobs, and action chaining.

### v2.3.5 — v2.3.6

- **Properly handle remote jobs.**  
  Long-running remote jobs are now properly handled, allowing for graceful API shutdown.
  
- **Detect when creating a remote pipeline.**  
  Running a pipeline action with a remote executor will pass through the pipeline to the API server:

  ```bash
  mrsm show version + show arguments : --loop -e api:main
  ```

- **Remove actions websocket endpoint with temporary jobs.**

- **Properly quote environment variables in `systemd` services.**

- **Remove `~/.local` volume from `api` service in the stack.**  
  This was overwriting the new Docker image version and such needed to be removed.


### v2.3.0 – v2.3.4

- **Add the `Job` class.**  
  You may now manage jobs with `Job`:

  ```python
  import meerschaum as mrsm
  job = mrsm.Job('syncing-engine', 'sync pipes --loop')
  job.start()
  ```

  If you are running on `systemd`, jobs will be created as user services. Otherwise (e.g. running in Docker) jobs are created as Unix daemons and kept alive by the API server.

  You may choose the executor with `-e` (`--executor-keys`). Supported values are `local`, `systemd`, and the keys for any API instance. See the jobs documentation for more information.

- **Chain actions with `+`.**  
  Run multiple commands by joining them with `+`, similar to `&&` in `bash` but with better performance (one process).

  ```
  $ mrsm show pipes + sync pipes
  ```

  Adding `-d` (`--daemon`) will escape these joiners and run all of the chained commands in the job:

  ```
  $ mrsm show pipes + sync pipes --loop -d
  ```

- **Run chained actions as a pipeline with `:`.**  
  You can schedule chained actions by adding `:` to the end of your command:

  ```bash
  mrsm sync pipes -i sql:local + sync pipes : -s 'daily starting 00:00' -d
  ```

  Other supported flags are `--loop`, `--min-seconds`, and the number of times to run the pipeline (e.g. `x2` or `2`):

  ```bash
  mrsm sync pipes + verify pipes : --loop --min-seconds 600
  ```

  ```bash
  mrsm show pipes + sync pipes : x2
  ```

- **Add `--restart`.**  
  Your job will be automatically restarted if you use any of flags `--loop`, `--schedule`, or `--restart`.

- **Execute actions remotely.**  
  You may execute an action on an API instance by setting the executor to the connector keys. You may run the `executor` command in the Meercshaum shell (like `instance`) or pass the flag `-e` (`--executor-keys`).

  ```
  mrsm sync pipes -e api:main
  ```

  The output is streamed directly from the API instance (via a websocket).

- **Add `from_plugin_import()`.**  
  You may now easily access attributes from a plugin's submodule with `meerschaum.plugins.from_plugin_import()`.
  
  ```python
  from meerschaum.plugins import from_plugin_import

  get_defined_pipes = from_plugin_import('compose.utils.pipes', 'get_defined_pipes')
  ```

## 2.2.x Releases

The 2.2.x series introduced new features improvements, such as the improved scheduler, the switch to `uv`, the `@dash_plugin` and `@web_page()` decorators, and much more.

### v2.2.7

- **Fix daemon stability.**  
  Broken file handlers are now better handled, and this should keep background jobs from crashing.

- **Improve `show jobs` output.**  
  The `show jobs` table now includes the `SuccessTuple` of the most recent run (when jobs are stopped).

- **Use a plugin's `__doc__` string as the default description.**  
  When registering a new plugin, the `__doc__` string will be used as the default value for the description.

- **Pipes without connectors are no longer considered errors when syncing.**  
  When a pipe has an ordinary string in place of a connector (e.g. externally managed), return early and consider success rather than throwing an error.

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe('a', 'b')
  pipe.sync()
  # (True, "Pipe('a', 'b') does not support fetching; nothing to do.")
  ```

- **Add update announcements.**  
  When new Meerschaum releases become available, you will now be presented with an update message when starting the shell. Update checks may be disabled by setting `shell:updates:check_remote` to `false`.

- **Enforce a 10-minute max timeout for `APIConnectors`.**

### v2.2.6

- **Fix a critical login issue.**  
  The previous release (v2.2.5) broke the login functionality of the Web UI and has been yanked. If you are running v2.2.5, it is urgent that you upgrade immediately.

- **Add environment variable `MRSM_CONFIG_DIR`.**  
  You may now isolate your configuration directory outside of the root (like with `MRSM_PLUGINS_DIR`, and `MRSM_VENVS_DIR`). This will be useful in certain production deployments where secrets need to be segmented and isolated.

- **Add `register connector`.**  
  Like `bootstrap connector`, you may now programmatically create connectors.

- **Allow for job names to contain spaces and parentheses.**  
  Jobs may now be created with more dynamic names. This issue in particular affected Meerschaum Compose.

- **Allow for type annotations in `required`.**  
  Plugins may now annotate `required`:

  ```python
  # plugins/example.py

  __version__: str = '0.0.1'
  required: list[str] = ['requests']
  ```

- **Automatically include `--noask` and `--yes` in remote actions.**  
  For your convenience, the flags `--noask` and `--yes` are included in remote actions sent by `APIConnector.do_action()`.

- **Fixed an issue with URIs for `api` connectors.**  
  Creating an `APIConnector` via a URI connection string now properly handles the protocol.

- **Fixed a formatting issue with `show logs`.**

### v2.2.5

- **Add `bootstrap plugin`.**  
  The `bootstrap plugin` wizard provides a convenient way to create new plugins from templates.

- **Add `edit plugin`.**  
  The action `edit plugin` will open a plugin's source file in your editor (`$EDITOR` or `pyvim`).

- **Allow actions, `fetch()`, and `sync()` to omit `pipe` and `**kwargs`.**  
  Adding `**kwargs` (and `pipe`) is now optional, and you may instead explicitly state only the arguments required.

  ```python
  from meerschaum.actions import make_action

  @make_action
  def do_the_thing(shell=False):
      msg = (
          "You're in the mrsm shell."
          if shell
          else "You called this from bash."
      )
      return True, msg


  # You may now omit the positional argument `pipe`.
  def fetch(begin=None, end=None):
      return [{'ts': '2024-01-01'}]
  ```

- **Fixed minor bug with subaction detection.**  
  Subaction functions must now explicitly begin with the name of the parent action (underscore prefix allowed).

  ```python
  from meerschaum.actions import make_action

  @make_action
  def sing():
      return True, "~la la la~"

  def sing_song():
      return True, "~do re mi~"

  def _sing_tune():
      return True, "~fo so la ti~"
  ```

- **Allow for `--begin None`.**  
  Explicitly setting `--begin None` will now pass `None` to `fetch()`.

- **Persist user packages in stack Docker container.**  
  The stack Docker Compose file now persists user-level packages (under `~/.local`).

- **Throw a warning if a `@dash_plugin` function raises an exception.**

- **Added connector type to `show connectors`.**  
  Append a connector type to the `show connectors` command (e.g. `show connectors sql`) to see only connectors of a certain type.

- **Allow `dprint` to be imported from `meerschaum.utils.warnings`.**  
  For convenience, you may now import `dprint` alongside `info`, `warn`, and `error`.

  ```python
  from meerschaum.utils.warnings import dprint, warn, info, error
  ```

- **Add positional arguments filtering (`filter_positional()` and `filter_arguments()`)**  
  In addition to keyword argument filtering, you may now filter positional arguments.

  ```python
  from meerschaum.utils.misc import filter_arguments, filter_positional

  def foo(a, b, c=0):
      return (a * b) + c

  filter_positional(foo, 1, 2, 3)
  # (1, 2)

  filter_arguments(foo, 1, 2, c=3, d=4)
  # ((1, 2), {'c': 3})
  ```
  
- **Cleaned up OAuth flow (`/login`).**

### v2.2.2 – v2.2.4

- **Speed up package installation in virtual environments.**  
  Dynamic dependencies will now be installed via `uv`, which dramatically speeds up installation times.

- **Add sub-cards for children pipes.**  
  Pipes with `children` defined now include cards for these pipes under the Parameters menu item. This is especially useful when working managing pipeline hierarchies.

- **Add "Open in Python" to pipe cards.**  
  Clicking "Open in Python" on a pipe's card will now launch `ptpython` with the pipe object already created.

  ```python
  # Clicking "Open in Python" executes the following:
  # $ mrsm python "pipe = mrsm.Pipe('plugin:noaa', 'weather', 'gvl', instance='sql:main')"
  >>> import meerschaum as mrsm
  >>> pipe = mrsm.Pipe('plugin:noaa', 'weather', 'gvl', instance='sql:main')
  ```

- **Add the decorators `@web_page` and `@dash_plugin`.**  
  You may now quickly add your own pages to the web console by decorating your layout functions with `@web_page`:

  ```python
  # example.py
  from meerschaum.plugins import dash_plugin, web_page

  @dash_plugin
  def init_dash(dash_app):

      import dash.html as html
      import dash_bootstrap_components as dbc
      from dash import Input, Output, no_update

      @web_page('/my-page', login_required=False)
      def my_page():
          return dbc.Container([
              html.H1("Hello, World!"),
              dbc.Button("Click me", id='my-button'),
              html.Div(id="my-output-div"),
          ])
      
      @dash_app.callback(
          Output('my-output-div', 'children'),
          Input('my-button', 'n_clicks'),
      )
      def my_button_click(n_clicks):
          if not n_clicks:
              return no_update
          return html.P(f'You clicked {n_clicks} times!')
  ```

- **Use `ptpython` for the `python` action.**  
  Rather than opening a classic REPL, the `python` action will now open a `ptpython` shell.

- **Allow passing flags to venv `ptpython` binaries.**  
  You may now pass flags directly to the `ptpython` binary of a virtual environment (by escaping with `[]`):
  
  ```bash
  mrsm python [--help]
  ```

- **Allow for custom connectors to implement a `sync()` method.**  
  Like module-level `sync()` functions for `plugin` connectors, any custom connector may implement `sync()` instead of `fetch()`.

  ```python
  # example.py
  from typing import Any
  import meerschaum as mrsm
  from meerschaum.connectors import Connector, make_connector

  @make_connector
  class ExampleConnector(Connector):

      def register(self, pipe: mrsm.Pipe) -> dict[str, Any]:
          return {
              'columns': {
                  'datetime': 'ts',
                  'id': 'example_id',
              },
          }

      def sync(self, pipe: mrsm.Pipe, **kwargs) -> mrsm.SuccessTuple:
          ### Implement a custom sync.
          return True, f"Successfully synced {pipe}!"
  ```

### v2.2.1

- **Fix `--schedule` in the interactive shell.**  
  The `--schedule` flag may now be used from both the CLI and the Shell.

- **Fix the `SQLConnector` CLI.**  
  The `sql` action now correctly opens an interactive CLI.

- **Bumped `duckdb` to `>=1.0.0`.**  
  The upstream breaking changes that required `duckdb` to be held back have to do with how indices behave. For now, index creation has been disabled so that `duckdb` may be upgraded to 1.0+.

### v2.2.0

**New Features**

- **New job scheduler**  
  The job scheduler has been rewritten with a [simpler syntax](https://meerschaum.io/reference/background-jobs/#-schedules).

  ```bash
  mrsm sync pipes -s 'daily & mon-fri starting 00:00 tomorrow' -d
  ```

- **Add `show schedule`.**  
  Validate your schedules' upcoming timestamps with `show schedule`.

  ```
  mrsm show schedule 'daily & mon-fri starting 2024-05-01'

  Next 5 timestamps for schedule 'daily & mon-fri starting 2024-05-01':

    2024-05-01 00:00:00+00:00
    2024-05-02 00:00:00+00:00
    2024-05-03 00:00:00+00:00
    2024-05-06 00:00:00+00:00
    2024-05-07 00:00:00+00:00
  ```

- **Added timestamps to log file lines.**  
  Log files now prepend the current minute to each line of the file, and the timestamps are also printed when viewing logs with `show logs`.
  To disable this behavio, set `MRSM{jobs:logs:timestamps:enabled}` to `false`.

  You may change the timestamp format under the config keys `MRSM{jobs:logs:timestamps:format}` (timestamp written to disk) and `MRSM{jobs:logs:timestamps:follow_format}` (timestamp printed when following via `show logs`.).

- **Add `--skip-deps`.**  
  When installing plugins, you may skip dependencies with `--skip-deps`. This should improve the iteration loop during development.

  ```bash
  mrsm install plugin noaa --no-deps
  ```

- **Add logs buttons to job cards on the Web UI.**  
  For your convenience, "Follow logs" and "Download logs" buttons have been added to jobs' cards.

- **Add a Delete button to job cards on the Web UI.**  
  You may now delete a job from its card (once stopped, that is).

- **Add management buttons to pipes' cards.**  
  For your convenience, you may now sync, verify, clear, drop, and delete pipes directly from cards.

- **Designate your packages as plugins with the `meerschaum.plugins` entry point.**  
  You may now specify your existing packages as Meerschaum plugins by adding the `meerschaum.plugins` [Entrypoint](https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/#using-package-metadata) to your package metadata:

  ```python
  from setuptools import setup
  
  setup(
      ...,
      entry_points = {
          'meerschaum.plugins': [
              'foo = foo',
          ],
      },
  )
  ```

  or if you are using `pyproject.toml`:

  ```toml
  [project.entry-points."meerschaum.plugins"]
  foo = "foo"
  ```

- **Pre- and post-sync hooks are printed separately.**  
  The results of sync hooks are now printed right after execution rather than after the sync.

**Bugfixes**

- **Fixed a filtering bug on the Web UI when changing instances.**  
  When changing instances on the Web Console, the connector, metric, and location choices will reset appropriately.

- **Ctrl+C when exiting `show logs`.**  
  Pressing Ctrl+C will now exit the `show logs` immediately.

**Breaking Changes**

- **No longer supporting the old scheduler syntax.**  
  If you have jobs with the old scheduler syntax (e.g. using the keyword `before`), you may need to delete and recreate your jobs with an updated schedule.

- **Upgraded to `psycopg` from `psycopg2`.**  
  The upgrade to `psycopg` (version 3) should provide better performance for larger transactions.

- **`Daemon.cleanup()` now returns a `SuccessTuple`.**

**Other changes**

- **Bumped `xterm.js` to v5.5.0.**
- **Added tags to the pipes card.**
- **Replaced `watchgod` with `watchfiles`.**
- **Replaced `rocketry` with `APScheduler`.**
- **Removed `pydantic` from dependencies.**
- **Removed `passlib` from dependencies.**
- **Bumped default TimescaleDB image to `latest-pg16-oss`.**
- **Held back `duckdb` to `<0.10.3`.**


## 2.1.x Releases

The 2.1.x series added high-performance upserts, improved numerics support and temporary tables performance, and many other bugfixes and improvements.

### v2.1.7

- **Add `query_df()` to `meerschaum.utils.dataframe`.**  
  The function `query_df()` allows you to filter dataframes by `params`, `begin`, and `end`.

  ```python
  import pandas as pd
  df = pd.DataFrame([
      {'a': 1, 'color': 'red'},
      {'a': 2, 'color': 'blue'},
      {'a': 3, 'color': 'green'},
      {'a': 4, 'color': 'yellow'},
  ])

  from meerschaum.utils.dataframe import query_df
  query_df(df, {'color': ['red', 'yellow']})
  #    a   color
  # 0  1     red
  # 3  4  yellow
  query_df(df, {'color': '_blue'}, reset_index=True)
  #    a   color
  # 0  1     red
  # 1  3   green
  # 2  4  yellow
  query_df(df, {'a': 2}, select_columns=['color'])
  #   color
  # 1  blue
  ```
- **Add `get_in_ex_params()` to `meerschaum.utils.misc`.**  
  This function parses a standard `params` dictionary into tuples of include and exclude parameters.

  ```python
  from meerschaum.utils.misc import get_in_ex_params
  params = {'color': ['red', '_blue', 'green']}
  in_ex_params = get_in_ex_params(params)
  in_ex_params
  # {'color': (['red', 'green'], ['blue'])}
  in_vals, ex_vals = in_ex_params['color']
  ```

- **Add `coerce_numeric` to `pipe.enforce_dtypes()`.**  
  Setting this to `False` will not cast floats to `Decimal` if the corresponding dtype is `int`.

- **Improve JSON serialization when filtering for updates.**

- **Add `date_bound_only` to `pipe.filter_existing()`.**  
  The argument `date_bound_only` means that samples retrieved by `pipe.get_data()` will only use `begin` and `end` for bounding. This may improve performance for custom instance connectors which have limited searchability.

- **Add `safe_copy` to `pipe.enforce_types()`, `pipe.filter_existing()`, `filter_unseen_df()`.**  
  By default, these functions will create copies of dataframes to avoid mutating the input dataframes. Setting `safe_copy` to `False` may be more memory efficient.

- **Add multiline support to `extract_stats_from_message`.**  
  Multiple messages separated by newlines may be parsed at once.

  ```python
  from meerschaum.utils.formatting import extract_stats_from_message
  extract_stats_from_message("Inserted 10, upserted 3\ninserted 11, upserted 4")
  # {'inserted': 21, 'updated': 0, 'upserted': 7}
  ```

- **Remove `order by` check in SQL queries.**

- **Improve shell startup performance by removing support for `cmd2`.**  
  The package `cmd2` never behaved properly, so support has been removed and only the built-in `cmd` powers the shell. As such, the configuration key `shell:cmd` has been removed.

### v2.1.6

- **Move `success_tuple` from arg to kwarg for `@post_sync_hook` functions.**  
  To match the signature of `@pre_sync_hook` functions, `@post_sync_hook` functions now only accept `pipe` as the positional argument. The return value of the sync will now be passed as the kwarg `success_tuple`. This allows you to use the same callback function as both the pre- and post-sync hooks.

  ```python
  import meerschaum as mrsm
  from meerschaum.plugins import pre_sync_hook, post_sync_hook

  @pre_sync_hook
  @post_sync_hook
  def log_sync(
          pipe: mrsm.Pipe,
          success_tuple: mrsm.SuccessTuple | None = None,
      ):
      if success_tuple is None:
          print(f"About to sync {pipe}!")
      else:
          success, msg = success_tuple
          print(f"Finished syncing {pipe} with message:\n{msg}")
  ```

- **Add `sync_timestamp` and `sync_complete_timestamp` to sync hooks.**  
  The UTC datetime right before the sync is added to the sync hook kwargs, allowing for linking the two callbacks to the same datetime. For convenience, the UTC datetime is also captured at the end of the sync and is passed as `sync_complete_timestamp`.

  ```python
  from datetime import datetime
  import meerschaum as mrsm
  from meerschaum.plugins import pre_sync_hook, post_sync_hook

  @pre_sync_hook
  @post_sync_hook
  def log_sync(
          pipe: mrsm.Pipe,
          sync_timestamp: datetime | None = None,
          sync_complete_timestamp: datetime | None = None,
          sync_duration: float | None = None,
      ) -> mrsm.SuccessTuple:

      if sync_complete_timestamp is None:
          print(f"About to sync {pipe} at {sync_timestamp}.")
          return True, "Success"

      msg = (
          f"It took {sync_duration} seconds to sync {pipe}:\n"
          + "({sync_timestamp} - {sync_complete_timestamp})"
      )
      return True, msg
  ```

- **Improved performance of sync hooks.**  
  Sync hooks are now called asynchronously in their own threads to avoid slowing down or crashing the main thread.

- **Rename `duration` to `sync_duration` for sync hooks.**  
  To avoid potential conflicts, the kwarg `duration` is prefixed with `sync_` to denote that it was specifically added to provide context on the sync.

- **Allow for sync hooks to return `SuccessTuple`.**  
  If a sync hook returns a `SuccessTuple` (`Tuple[bool, str]`), the result will be printed.

  ```python
  import meerschaum as mrsm
  from meerschaum.plugins import post_sync_hook

  @post_sync_hook
  def log_sync(pipe) -> mrsm.SuccessTuple:
      return True, f"Logged sync for {pipe}."
  ```

- **Add `is_success_tuple()` to `meerschaum.utils.typing`.**  
  You can now quickly check whether an object is a `SuccessTuple`:

  ```python
  from meerschaum.utils.typing import is_success_tuple
  assert is_success_tuple((True, "Success"))
  assert not is_success_tuple(("foo", "bar"))
  ```

- **Allow for index-only pipes when `upsert=True`.**  
  If all columns are indices and `upsert` is `True`, then the upsert will insert net-new rows (ignore duplicates).

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe(
      'a', 'b',
      columns = ['a', 'b'],
      parameters = {'upsert': True},
      instance = 'sql:local',
  )
  pipe.sync([{'a': 1, 'b': 2}])
  ```

- **Allow for prelimary null index support for `upsert=True` (inserts only, PostgreSQL only).**  
  Like with regular syncs, upsert syncs now coalesce indices to allow for syncing null values. **NOTE:** the transaction will fail if a null index is synced again, so this is only for the initial insert.

- **Remove automatic instance table renaming.**  
  This patch removes automatic detection and renaming of old instance tables to the new names (e.g. `users` -> `mrsm_users`). Users migrating from an old installation will need to rename the tables manually themselves.

### v2.1.5

- **Add the action `tag pipes`.**  
  Tags may be added or removed with the `tag pipes` action. Note that the flag `--tags` applies to existing tags for filtering; flags to be added or removed are positional arguments.

  ```
  ### Tag all `sql:main` pipes with `production` and `sync-daily`.
  mrsm tag pipes production sync-daily -c sql:main

  ### Remove the `production`` tag from all pipes tagged as `production`.
  mrsm tag pipes _production --tags production
  ```

- **Add `--tags` support to `register pipes`.**  
  The action `register pipe` with the `--tags` flag will auto-tag the new pipes.

- **Clean up warnings on Python 3.12.**  
  All instances of `datetime.utcnow()` have been replaced by `datetime.now(timezone.utc).replace(tzinfo=None)` (to preserve behavior). A full migration to timezone-aware datetimes would have to happen in a minor release.

- **Improve timezone-aware datetime support for MSSQL.**  
  Passing a timezone-aware datetime as a date bound for MSSQL should now be fixed.

- **Add an explicit `VOLUME` to the `Dockerfile`.**  
  The path `/meerschaum` is now explicitly set as a `VOLUME` in the Docker image.

- **Add `--tags` filtering to the `show tags` action.**  

- **Improve global `ThreadPool` handling.**  
  Global pools are now created on a per-worker (and per-class) basis, allowing for switching between workers within the same process. Note that global pools are maintained to allow for nested chunking and the limit the number of connections (e.g. avoid threads creating their own pools).

- **Fix a bug when selecting only by negating tags.**  
  Pipes may now be selected by only specifying negated tags.

- **Rename `meerschaum.utils.get_pipes` to `meerschaum.utils._get_pipes` to avoid namespace collissions.**  
  The functions `get_pipes()` and `fetch_pipes_keys()` are available at the `meerschaum.utils` module namespace.

### v2.1.3 – v2.1.4

- **Add the decorators `@pre_sync_hook` and `@post_sync_hook`.**  
  The new decorators `@pre_sync_hook` and `@post_sync_hook` let you intercept a Pipe immediately before and after a sync, capturing its return tuple and the duration in seconds.

  ```python
  import meerschaum as mrsm
  from meerschaum.plugins import pre_sync_hook, post_sync_hook

  @pre_sync_hook
  def prepare_for_sync(pipe: mrsm.Pipe, **kwargs):
      print(f"About to sync {pipe} with kwargs:\n{kwargs}")

  @post_sync_hook
  def log_sync(
          pipe: mrsm.Pipe,
          return_tuple: mrsm.SuccessTuple,
          duration: float = 0.0
      ):
      print(f"It took {round(duration, 2)} seconds to sync {pipe} with result:")
      mrsm.pprint(return_tuple)
  ```

- **Add the action `show tags`.**  
  The action `show tags` will now display panels of pipes grouped together by common tags. This is useful for large deployments which share common tags.

- **Add dropdowns and inputs for flags with arguments to the Web Console.**  
  Leverage the full power of the Meerschaum CLI in the Web Console with the new dynamic flags dropdowns.

- **Fix shell crashes in Docker containers.**  
  Reloading the running Meerschaum session from an interactive shell via a Docker container will no longer cause crashes on custom commands.

- **Improve reloading times.**  
  Reloading the running Meerschaum session has been sped up by several seconds (due to skipping the internal shell modules).

- **Improve virtual environments in the Docker image.**  
  Initial startup of Docker containers on a fresh persistent volume has been sped up due to preloading the default virtual environment creation. Additionally, the environment variable `$MRSM_VENVS_DIR` has been unset, reverting the virtual environments to be stored under `/meerschaum/venvs`.

### v2.1.1 – v2.1.2

- **Add `upsert` for high-performance pipes.**  
  Setting `upsert` under `pipe.parameters` will create a unique index and combine the insert and update stages into a single upsert. This is particularly useful for pipes with very large tables.

  ```python
  import meerschaum as mrsm

  pipe = mrsm.Pipe(
      'demo', 'upsert',
      instance = 'sql:local',
      columns = ['datetime', 'id'],
      parameters = {'upsert': True},
  )

  mrsm.pprint(
      pipe.sync(
          [
              {'datetime': '2023-01-01', 'id': 1, 'val': 1.1},
              {'datetime': '2023-01-02', 'id': 2, 'val': 2.2},
          ]
      )
  )
  #  🎉 Upserted 2 rows.
  ```

- **Add internal schema `_mrsm_internal` for temporary tables.**  
  To avoid polluting your database schema, temporary tables will now be created within `_mrsm_internal` (for databases which support PostgreSQL-style schemas).

- **Added `mrsm_temporary_tables`.**  
  Temporary tables are logged to `mrsm_temporary_tables` and deleted when dropped.

- **Drop stale temporary tables.**  
  Temporary tables which are more than 24 hours will be dropped automatically (configurable under `system:sql:instance:stale_temporary_tables_minutes`).

- **Prefix instance tables with `mrsm_`.**  
  Internal Meerschaum tables still reside on the default schema but will be denoted with a `mrsm_` prefix. The existing instance tables will be automatically renamed (e.g. `pipes` will become `mrsm_pipes`) to add the prefix (this renaming detection will be removed in later releases).

- **Fix an issue with `bootstrap`.**  
  Refactoring work for 2.1.0 had broken the `bootstrap` action.

- **Fix an issue with `pause jobs`.**

- **Fix an issue when selecting inverse pipes.**  
  Null location keys are now coalesced when selecting pipes to produce expected behavior.

- **Avoid a system exit when exiting the SQL CLI.**

### v2.1.0

- **Replace `term.js` with `xterm.js`.**  
  This has been a long time coming. The webterm has been migrated to `xterm.js` which has continuous support from `term.js` which was last updated almost 10 years ago.

- **Deprecate the legacy web pseudo-terminal.**  
  Clicking the "Execute" button on the web console will now execute the command directly in the webterm. Additionally, changing the instance select will now automatically switch the webterm's context to the desired instance.

- **Fix an issue when starting existing jobs.**  
  A bug has been fixed which prevented jobs from restarting specifically by name.

- **Add `MRSM_VENVS_DIR`.**  
  Like `MRSM_PLUGINS_DIR`, you can now designate a virtual environments directory separate from the root directory. This is particularly useful for production deployments, and `MRSM_VENVS_DIR` has been set to `/home/meerschaum/venvs` in the official Docker images to allow for mounting `/meerschaum` to persistent volumes.

- **Allow syncing `NULL` values into indices.**  
  Syncing `None` within an index will now be coalesced into a magic value when applying updates.

  ```python
  import meerschaum as mrsm

  pipe = mrsm.Pipe(
      'allow', 'null', 'indices',
      instance = 'sql:local',
      columns = ['a', 'b'],
  )
  pipe.sync([{'a': 1, 'b': 1}])
  pipe.sync([{'b': 1}])
  pipe.sync([{'a': 1}])
  pipe.sync([{'c': 1}])

  print(pipe.get_data())
  #       a     b     c
  # 0  <NA>  <NA>     1
  # 1  <NA>     1  <NA>
  # 2     1  <NA>  <NA>
  # 3     1     1  <NA>
  ```

- **Syncing `Decimal` objects will now enforce `numeric` dtypes.**  
  For example, syncing a `Decimal` onto a integer column will update the dtype to `numeric`, like when syncing a float after an integer.

  ```python
  import meerschaum as mrsm
  from decimal import Decimal

  pipe = mrsm.Pipe(
      'demo', 'decimal', 'coersion',
      instance = 'sql:local',
      columns = ['id'],
  )
  pipe.sync([{'id': 1, 'foo': 10}])
  pipe.sync([{'id': 1, 'foo': Decimal('20')}])
  print(pipe.dtypes)
  # {'id': 'int64[pyarrow]', 'foo': 'numeric'}

  df = pipe.get_data()
  print(f"{df['foo'][0]=}")
  # df['foo'][0]=Decimal('20') 
  ```

- **Improve `IS NULL` and `IS NOT NULL` checks for `params`.**  
  Mixing null-like values (e.g. `NaN`, `<NA>`, `None`) in `params` will now separate out nulls.

  ```python
  from meerschaum.utils.sql import build_where
  print(build_where({'a': ['_<NA>', '_1', '_2']}))
  # WHERE
  #   ("a" NOT IN ('1', '2')
  #   AND "a" IS NOT NULL)
  print(build_where({'a': ['NaN', '1', '2']}))
  # WHERE
  #   ("a" IN ('1', '2')
  #   OR "a" IS NULL)
  ```

- **Add colors to `mrsm show columns`.**

- **Fix a unicode decoding error when showing logs.**

- **Remove `xstatic` dependencies.**  
  The `xterm.js` files are now bundled as static assets, so the `term.js` files are no longer needed. Hurray for removing dependencies!

- **Other bugfixes.**  
  A handful of minor bugfixes have been included in this release:
    - Removed non-connector environment variables like `MRSM_WORK_DIR` from the `mrsm show connectors` output.
    - Improving symlinks handling for multi-processed situations (`mrsm start api`).

## 2.0.x Releases

At long last, 2.0 has arrived! The 2.0 releases brought incredible change, from standardizing chunking to adding `Pipe.verify()` and `Pipe.deduplicate()` to introducing first-class `numeric` support. See the full release notes below for the complete picture.

### v2.0.8 – v2.0.9

- **Cast `None` to `Decimal('NaN')` for `numeric` columns.**  
  To allow for all-null numeric columns, `None` (and other null-like types) are coerced to `Decimal('NaN')`.

- **Schema bugfixes.**  
  A few minor edge cases have been addressed when working with custom schemas for pipes.

- **Remove `APIConnector.get_backtrack_data()`.**  
  Since 1.7 released, the `get_backtrack_data()` method for instance connectors has been optional. 
  > **NOTE:** the `backtrack_data` API endpoint has also been removed.

- **Other bugfixes.**  
  Issues with changes made to session authentication have been addressed.

### v2.0.5 – v2.0.7

- **Add the `numeric` dtype (i.e. support for `NUMERIC` columns).**  
  Specifying a column as `numeric` will coerce it into `decimal.Decimal` objects. For `SQLConnectors`, this will be stored as a `NUMERIC` column. This is useful for syncing a mix of integer and float values.

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe(
      'demo', 'numeric',
      instance = 'sql:main',
      columns = ['foo'],
      dtypes = {'foo': 'numeric'},
  )
  pipe.sync([{'foo': '1'}, {'foo': '2.01234567890123456789'}])
  df = pipe.get_data()
  print(df.to_dict(orient='records'))
  # [{'foo': Decimal('1')}, {'foo': Decimal('2.01234567890123456789')}]
  ```

  > **NOTE**: Due to implementation limits, `numeric` has strict precision issues in embedded databases (SQLite and DuckDB: `NUMERIC(15, 4)`). PostgreSQL-like database flavors have the best support for `NUMERIC`; MySQL and MariaDB use a scale and precision of `NUMERIC(38, 20)`, MSSQL uses `NUMERIC(28, 10)`, and Oracle and PostgreSQL are not capped.

- **Mixing `int` and `float` will cast to `numeric`.**  
  Rather than always casting to `TEXT`, a column containing a mix of `int` and `float` will be coerced into `numeric`.

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe('mix', 'int', 'float', instance='sql:local')
  pipe.sync([{'a': 1}])
  pipe.sync([{'a': 1.1}])
  print(pipe.dtypes)
  # {'a': 'numeric'}
  ```

- **Add `schema` to `SQLConnectors`.**  
  Including the key `schema` or as an argument in the URI will use this schema for created tables. The argument `search_path` will also set `schema` (i.e. for PostgreSQL).

  ```bash
  export MRSM_SQL_FOO='{
    "username": "foo",
    "password": "bar",
    "port": 5432,
    "flavor": "timescaledb",
    "host": "localhost",
    "database": "db",
    "schema": "myschema"
  }'

  export MRSM_SQL_FOO='timescaledb://foo:bar@localhost:5432/db?options=--search_path%3Dmyschema'
  ```

- **Add `schema` to `pipe.parameters`.**  
  In addition to the default schema at the connector level, you may override this by setting `schema` under `pipe.parameters`.

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe('a', 'b', parameters={'schema': 'myschema'})
  ```
 
- **Add `schema` to `meerschaum.utils.sql.sql_item_name()`.**  
  You may now pass an optional `schema` when quoting:

  ```python
  from meerschaum.utils.sql import sql_item_name
  print(sql_item_name('foo', 'mssql', schema='dbo'))
  # '[dbo].[foo]'
  ```

- **Add `options` to `SQLConnector`.**  
  The key `options` will now contain a sub-dictionary of connection options, such as `driver`, `search_path`, or any other query parameters.

- **Disable the "Sync Documents" accordion item when the session is not authenticated.**  
  When running the API with `--secure`, only admin users will be able to access the "Sync Documents" accordion items on the pipes' cards.

- **Remove `dtype_backend` from `SQLConnector.read()`.**  
  This argument previously had no effect. When applied, it was coercing JSON columns into strings, so it was removed.

- **Remove `meerschaum.utils.daemon.Log`.**  
  This had been replaced by `meerschaum.utils.daemon.RotatingLog` and had been broken since the 2.0 release.

- **Remove `params` from `Pipe.filter_existing()`.**  
  To avoid confusion, filter parameters are instead derived from the incoming DataFrame. This will improve performance when repeatedly syncing chunks which span the same interval. The default limit of 250 unique values may be configured under `pipes:sync:filter_params_index_limit`.

- **Add `forwarded_allow_ips` and `proxy_headers` to the web API.**  
  The default values `forwarded_allow_ips='*'` and `proxy_headers=True` are set when running Uvicorn or Gunicorn and will help when running Meerschaum behind a proxy.

- **Bump `dash-extensions` to `>=1.0.4`.**  
  The bug that was holding back the version was due to including `enrich.ServersideTransform` in the dash proxy without actually utilizing it.


### v2.0.3 – v2.0.4

- **Fix an issue with `--timeout-seconds`.**  
  Previous refactoring efforts had broken the `--timeout-seconds` polling behavior.

- **Fix a formatting issue when pretty-printing pipes.**  
  Pipes may now be correctly printed if both single and double quotes appear in a message.

- **Allow omitting `port` for `APIConnectors`.**  
  You may now omit the `port` attribute for `APIConnectors` to use the protocol-default port (e.g. 443 for HTTPS). Note you will need to delete the key `api:default:port` via `mrsm edit config` if it's present.

- **Add optional `verify` key to API connectors.**  
  Client API connectors may now be used with self-signed HTTPS instances.

- **Bump `duckdb` to version 0.9.0.**  
  This adds complete support for PyArrow data types to DuckDB.

### v2.0.2

- **Syncing with `--skip-check-existing` will not apply the backtrack interval.**  
  Because `--skip-check-existing` (or `check_existing=False`) is guaranteed to produce duplicates, the backtrack interval will be set to 0 when running in insert-only mode.

- **Allow for `columns` to be a list.**  
  Note that building a pipe with `columns` as a list must have the datetime column named `datetime`.

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe('a', 'b', columns=['a'])
  print(pipe.columns)
  # {'a': 'a'}
  ```

- **Bump default SQLAlchemy pool size to 8 connections.**

- **Consider the number of checked out connections when choosing workers.**  
  For pipes on `sql` instances, `pipe.get_num_workers()` will now consider the number of checked out connections rather than only the number of active threads.

- **Fix `pipe.get_data(as_dask=True)` for JSON columns.**

### v2.0.1

- **Fix syncing bools within in-place SQL pipes.**  
  SQL pipes may now sync bools in-place. For database flavors which lack native `BOOLEAN` support (e.g. `sqlite`, `oracle`, `mysql`), then the boolean columns must be stated in `pipe.dtypes`.

- **Fix an issue with multiple users managing jobs.**  
  Extra validation was added to the web UI to allow for multiple users to interact with jobs.

- **Fix a minor formatting bug with `highlight_pipes()`.**  
  Improved validation logic was added to prevent incorrectly prepending the `Pipe(` prefix.

- **Hold back `pydantic` to `<2.0.0`**  
  Pydantic 2 is supported in all features except `--schedule`. Until `rocketry` supports Pydantic 2, it will be held back.

### v2.0.0

**Breaking Changes**

- **Removed redundant `Pipe.sync_time` property.**  
  Use `pipe.get_sync_time()` instead.

- **Removed `SQLConnector.get_pipe_backtrack_minutes()`.**  
  Use `pipe.get_backtrack_interval()` instead.

- **Replaced `pipe.parameters['chunk_time_interval']` with `pipe.parameters['verify']['chunk_minutes']`**  
  For better security and cohesiveness, the TimescaleDB `chunk_time_interval` value is now derived from the standard `chunk_minutes` value. This also means pipes with integer date axes will be created with a new default chunk interval of 1440 (was previously 100,000).

- **Moved `choose_subaction()` into `meerschaum.actions`.**  
  This function is for internal use and as such should not affect any users.

**Features**

- **Added `verify pipes` and `--verify`.**  
  The command `mrsm verify pipes` or `mrsm sync pipes --verify` will resync pipes' chunks with different rowcounts to catch any backfilled data.

  ```python
  import meerschaum as mrsm
  foo = mrsm.Pipe(
      'foo', 'src',
      target = 'foo',
      columns = {'datetime': 'dt'},
      instance = 'sql:local'
  )
  docs = [
      {'dt': '2023-01-01'},
      {'dt': '2023-01-02'},
  ]
  foo.sync(docs)

  pipe = mrsm.Pipe(
      'sql:local', 'verify', 
      columns = {'datetime': 'dt'},
      parameters = {
          'query': f'SELECT * FROM "{foo.target}"'
      },
      instance = 'sql:local',
  )
  pipe.sync(docs) 

  backfilled_docs = [
      {'dt': '2022-12-30'},
      {'dt': '2022-12-31'},
  ]
  foo.sync(backfilled_docs)
  mrsm.pprint(pipe.verify())
  assert foo.get_rowcount() == pipe.get_rowcount()
  ```

- **Added `deduplicate pipes` and `--deduplicate`.**  
  Running `mrsm deduplicates pipes` or `mrsm sync pipes --deduplicate` will iterate over pipes' entire intervals, chunking at the configured chunk interval (see `pipe.get_chunk_interval()` below) and clearing + resyncing chunks with duplicate rows.

  If your instance connector implements `deduplicate_pipe()` (e.g. `SQLConnector`), then this method will override the default `pipe.deduplicate()`.

  ```python
  pipe = mrsm.Pipe(
      'demo', 'deduplicate',
      columns = {'datetime': 'dt'},
      instance = 'sql:local',
  )
  docs = [
      {'dt': '2023-01-01'},
      {'dt': '2023-01-01'},
  ]
  pipe.sync(docs)
  print(pipe.get_rowcount())
  # 2
  pipe.deduplicate()
  print(pipe.get_rowcount())
  # 1
  ```

- **Added `pyarrow` support.**  
  The dtypes enforcement system was overhauled to add support for `pyarrow` data types.

  ```python
  import meerschaum as mrsm
  import pandas as pd

  df = pd.DataFrame(
      [{'a': 1, 'b': 2.3}]
  ).convert_dtypes(dtype_backend='pyarrow')
  
  pipe = mrsm.Pipe(
      'demo', 'pyarrow',
      instance = 'sql:local',
      columns = {'a': 'a'},
  )
  pipe.sync(df)
  ```

- **Added `bool` support.**  
  Pipes may now sync DataFrames with booleans (even on Oracle and MySQL):

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe(
      'demo', 'bools',
      instance = 'sql:local',
      columns = {'id': 'id'},
  )
  pipe.sync([{'id': 1, 'is_blue': True}])
  assert 'bool' in pipe.dtypes['is_blue']

  pipe.sync([{'id': 1, 'is_blue': False}])
  assert pipe.get_data()['is_blue'][0] == False
  ```


- **Added preliminary `dask` support.**  
  For example, you may now return Dask DataFrames in your plugins, pass into `pipe.sync()`, and `pipe.get_data()` now has the flag `as_dask`.

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe(
      'dask', 'demo',
      columns = {'datetime': 'dt'},
      instance = 'sql:local',
  )
  pipe.sync([
      {'dt': '2023-01-01', 'val': 1},
      {'dt': '2023-01-02', 'val': 2},
      {'dt': '2023-01-03', 'val': 3},
  ])
  ddf = pipe.get_data(as_dask=True)
  print(ddf)
  # Dask DataFrame Structure:
  #                            dt             val
  # npartitions=4
  #                datetime64[ns]  int64[pyarrow]
  #                           ...             ...
  #                           ...             ...
  #                           ...             ...
  #                           ...             ...
  # Dask Name: from-delayed, 5 graph layers

  print(ddf.compute())
  #           dt  val
  # 0 2023-01-01    1
  # 0 2023-01-02    2
  # 0 2023-01-03    3
  
  pipe2 = mrsm.Pipe(
      'dask', 'insert',
      columns = pipe.columns,
      instance = 'sql:local',
  )
  pipe2.sync(ddf)
  assert pipe.get_data().to_dict() == pipe2.get_data().to_dict()

  pipe.sync([{'dt': '2023-01-01', 'val': 10}])
  pipe2.sync(pipe.get_data(as_dask=True))
  assert pipe.get_data().to_dict() == pipe2.get_data().to_dict()
  ```

- **Added `chunk_minutes` to `pipe.parameters['verify']`.**  
  Like `pipe.parameters['fetch']['backtrack_minutes']`, you may now specify the default chunk interval to use for verification syncs and iterating over the datetime axis.

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe(
      'a', 'b',
      instance = 'sql:local',
      columns = {'datetime': 'dt'},
      parameters = {
          'verify': {
              'chunk_minutes': 2880,
          }
      },
  )
  pipe.sync([
      {'dt': '2023-01-01'},
      {'dt': '2023-01-02'},
      {'dt': '2023-01-03'},
      {'dt': '2023-01-04'},
      {'dt': '2023-01-05'},
  ])
  chunk_bounds = pipe.get_chunk_bounds(bounded=True)
  for chunk_begin, chunk_end in chunk_bounds:
      print(chunk_begin, '-', chunk_end)

  # 2023-01-01 00:00:00 - 2023-01-03 00:00:00
  # 2023-01-03 00:00:00 - 2023-01-05 00:00:00
  ```

- **Added `--chunk-minutes`, `--chunk-hours`, and `--chunk-days`.**  
  You may override a pipe's chunk interval during a verification sync with `--chunk-minutes` (or `--chunk-hours` or `--chunk-days`).

  ```python
  mrsm verify pipes --chunk-days 3
  ```

- **Added `pipe.get_chunk_interval()` and `pipe.get_backtrack_interval()`.**  
  Return the `timedelta` (or `int` for integer datetimes) from `verify:chunk_minutes` and `fetch:backtrack_minutes`, respectively.

  ```python
  import meerschaum as mrsm
  dt_pipe = mrsm.Pipe(
      'demo', 'intervals', 'datetime',
      instance = 'sql:local',
      columns = {'datetime': 'dt'},
  )
  print(dt_pipe.get_chunk_interval())
  # 1 day, 0:00:00

  int_pipe = mrsm.Pipe(
      'demo', 'intervals', 'int',
      instance = 'sql:local',
      columns = {'datetime': 'dt'},
      dtypes = {'dt': 'int'},
  )
  print(int_pipe.get_chunk_interval())
  # 1440
  ```

- **Added `pipe.get_chunk_bounds()`.**  
  Return a list of `begin` and `end` values to use when iterating over a pipe's datetime axis.
  
  ```python
  from datetime import datetime
  import meerschaum as mrsm
  pipe = mrsm.Pipe(
      'demo', 'chunk_bounds',
      instance = 'sql:local',
      columns = {'datetime': 'dt'},
      parameters = {
          'verify': {
              'chunk_minutes': 1440,
          }
      },
  )
  pipe.sync([
      {'dt': '2023-01-01'},
      {'dt': '2023-01-02'},
      {'dt': '2023-01-03'},
      {'dt': '2023-01-04'},
  ])

  open_bounds = pipe.get_chunk_bounds()
  for i, (begin, end) in enumerate(open_bounds):
      print(f"Chunk {i}: ({begin}, {end})")

  # Chunk 0: (None, 2023-01-01 00:00:00)
  # Chunk 1: (2023-01-01 00:00:00, 2023-01-02 00:00:00)
  # Chunk 2: (2023-01-02 00:00:00, 2023-01-03 00:00:00)
  # Chunk 3: (2023-01-03 00:00:00, 2023-01-04 00:00:00)
  # Chunk 4: (2023-01-04 00:00:00, None)

  closed_bounds = pipe.get_chunk_bounds(bounded=True)
  for i, (begin, end) in enumerate(closed_bounds):
      print(f"Chunk {i}: ({begin}, {end})")

  # Chunk 0: (2023-01-01 00:00:00, 2023-01-02 00:00:00)
  # Chunk 1: (2023-01-02 00:00:00, 2023-01-03 00:00:00)
  # Chunk 2: (2023-01-03 00:00:00, 2023-01-04 00:00:00)

  sub_bounds = pipe.get_chunk_bounds(
      begin = datetime(2023, 1, 1),
      end = datetime(2023, 1, 3),
  )
  for i, (begin, end) in enumerate(sub_bounds):
      print(f"Chunk {i}: ({begin}, {end})")

  # Chunk 0: (2023-01-01 00:00:00, 2023-01-02 00:00:00)
  # Chunk 1: (2023-01-02 00:00:00, 2023-01-03 00:00:00)
  ```

- **Added `--bounded` to verification syncs.**  
  By default, `verify pipes` is unbounded, meaning it will sync values beyond the existing minimum and maximum datetime values. Running a verification sync with `--bounded` will bound the search to the existing datetime axis.

  ```bash
  mrsm sync pipes --verify --bounded
  ```

- **Added `pipe.get_num_workers()`.**  
  Return the number of concurrent threads to be used with this pipe (with respect to its instance connector's thread safety).

- **Added `select_columns` and `omit_columns` to `pipe.get_data()`.**  
  In situations where not all columns are required, you can now either specify which columns you want to include (`select_columns`) and which columns to filter out (`omit_columns`). You may pass a list of columns or a single column, and the value `'*'` for `select_columns` will be treated as `None` (i.e. `SELECT *`).

  ```python
  pipe = mrsm.Pipe('a', 'b', 'c', instance='sql:local')
  pipe.sync([{'a': 1, 'b': 2, 'c': 3}])
  
  pipe.get_data(['a', 'b'])
  #    a  b
  # 0  1  2
  
  pipe.get_data('*', 'b')
  #    a  c
  # 0  1  3

  pipe.get_data(None, ['a', 'c'])
  #    b
  # 0  2

  pipe.get_data(omit_columns=['b', 'c'])
  #    a
  # 0  1

  pipe.get_data(select_columns=['c', 'a'])
  #    c  a
  # 0  3  1
  ```

- **Replace `daemoniker` with `python-daemon`.**  
  `python-daemon` is a well-maintained and well-behaved daemon process library. However, this migration removes Windows support for background jobs (which was never really fully supported already, so no harm there).

- **Added `pause jobs`.**  
  In addition to `start jobs` and `stop jobs`, the command `pause jobs` will suspend a job's daemon. Jobs may be resumed with `start jobs` (i.e. `Daemon.resume()`).

- **Added job management to the UI.**  
  Now that jobs and logs are much more robust, more job management features have been added to the web UI. Jobs may be started, stopped, paused, and resumed from the web console, and their logs are now available for download.

- **Logs now roll over and are preserved on job restarts.**  
  Spin up long-running job with peace of mind now that logs are automatically rolled over, keeping five 500 KB files on disk at any moment (you can tweak these values with `mrsm edit config jobs`).
  To facilitate this, `meershaum.utils.daemon.RotatingFile` was added to provide a generic file-like object, complete with its own file descriptor.

- **Starting existing jobs with `-d` will not throw an exception if the arguments match.**  
  Similarly, running without any arguments other than `--name` will run the existing job. This matches the behavior of `start jobs`.

- **Allow for colon-separated paths in `MRSM_PLUGINS_DIR`.**  
  Just like `PATH` in `bash`, you may now specify your plugins' paths in a single variable, separated by colons. Unlike `bash`, however, a blank path will not interpreted as the current directory.

  ```bash
  export MRSM_PLUGINS_DIR='./plugins:/app/plugins'
  ```

- **Add `pipe.keys()`**  
  `pipe.keys()` returns the connector, metric, and location keys (i.e. `pipe.meta` without the `instance`).

  ```python
  pipe = mrsm.Pipe('foo', 'bar')
  print(pipe.keys())
  # {'connector': 'foo', 'metric': 'bar', 'location': None}
  ```

- **Pipes are now indexable.**  
  Indexing a pipe directly is the same as accessing `pipe.attributes`:

  ```python
  pipe = mrsm.Pipe('a', 'b', columns={'foo': 'bar'})
  print(pipe['connector'])
  # 'a'
  print(pipe['connector_keys'])
  # 'a'
  print(pipe['columns'])
  # {'foo': 'bar'}
  print(pipe['does_not_exist'])
  # None
  ```

**Other changes**

- **Fixed backtracking being incorrectly applied to `--begin`.**  
  Application of the backtracking interval has been consolidated into `pipe.fetch()`.

- **Improved data type enforcement for SQL pipes.**  
  A pipe's data types are now passed to `SQLConnector.read()` when fetching its data.

- **Added `meerschaum.utils.sql.get_db_version()` and `SQLConnector.db_version`.**
  
- **Moved `print_options()` from `meerschaum.utils.misc` into `meerschaum.utils.formatting`.**  
  This places `print_options()` next to `print_tuple` and `pprint`. A placeholder function is still present in `meerschaum.utils.misc` to preserve existing behavior.

- **`mrsm.pprint()` will now pretty-print `SuccessTuples`.**

- **Added `calm` to `print_tuple()`.**  
  Printing a `SuccessTuple` with `calm=True` will use a more muted color scheme and emoji.

- **Removed `round_down` from `get_sync_time()` for instance connectors.**  
  To avoid confusion, sync times are no longer truncated by default. `round_down` is still an optional keyword argument on `pipe.get_sync_time()`.

- **Created `meerschaum.utils.dtypes`.**  
  - **Added `are_dtypes_equal()` to `meerschaum.utils.dtypes`.**
  - **Added `get_db_type_from_pd_type()` to `meerschaum.utils.dtypes.sql`.**
  - **Added `get_pb_type_from_db_type()` to `meerschaum.utils.dtypes.sql`.**
  - **Moved `to_pandas_dtype()` from `meerschaum.utils.misc` into `meerschaum.utils.dtypes`.**

- **Created `meerschaum.utils.dataframe`.**  
  - **Added `chunksize_to_npartitions()` to `meerschaum.utils.dataframe`.**
  - **Added `get_first_valid_dask_partition()` to `meerschaum.utils.dataframe`.**
  - **Moved `filter_unseen_df()` from `meerschaum.utils.misc` into `meerschaum.utils.dataframe`.**
  - **Moved `add_missing_cols_to_df()` from `meerschaum.utils.misc` into `meerschaum.utils.dataframe`.**
  - **Moved `parse_df_datetimes()` from `meerschaum.utils.misc` into `meerschaum.utils.dataframe`.**
  - **Moved `df_from_literal()` from `meerschaum.utils.misc` into `meerschaum.utils.dataframe`.**
  - **Moved `get_json_cols()` from `meerschaum.utils.misc` into `meerschaum.utils.dataframe`.**
  - **Moved `get_unhashable_cols()` from `meerschaum.utils.misc` into `meerschaum.utils.dataframe`.**
  - **Moved `enforce_dtypes()` from `meerschaum.utils.misc` into `meerschaum.utils.dataframe`.**
  - **Moved `get_datetime_bound_from_df()` from `meerschaum.utils.misc` into `meerschaum.utils.dataframe`.**
  - **Moved `df_is_chunk_generator()` from `meerschaum.utils.misc` into `meerschaum.utils.dataframe`.**

- **Refactored SQL utilities.**  
  - **Added `format_cte_subquery()` to `meerschaum.utils.sql`.**
  - **Added `get_create_table_query()` to `meerschaum.utils.sql`.**
  - **Added `get_db_version()` to `meerschaum.utils.sql`.**
  - **Added `get_rename_table_queries()` to `meerschaum.utils.sql`.**

- **Moved `choices_docstring()` from `meerschaum.utils.misc` into `meerschaum.actions`.**
- **Fixed handling backslashes for `stack` on Windows.**
  


## 1.7.x Releases

The 1.7 series was short and sweet with a big focus on improving the web API. The highlight feature was the integrated webterm, and the series includes many bugfixes and improvements. 

### v1.7.3 – v1.7.4

- **Fix an issue with the local stack healthcheck.**  
  Due to some edge cases, the local stack `docker-compose.yaml` file would not be correctly formatted until `edit config` had been executed. This patch ensures the files are synced with each invocation of `stack`.

- **Fix an issue when running the local stack with non-default ports.**  
  Initializing a local stack with a different database port (e.g. 5433) now routes correctly within the Docker compose network (now patching to internal port to 5432).

- **Fix `upgrade mrsm` behavior.**  
  Recent changes to `stack` broke the automatic `stack pull` within `mrsm upgrade mrsm`.

### v1.7.2

- **Fix `role "root" does not exist` from stack logs.**  
  Although the healthcheck was working as expected, the log output was filled with `Error FATAL: role "root" does not exist`. These errors have been fixed.

- **Fix `MRSM_CONFIG` behavior when running `start api --production`.**  
  Starting the Web API through `gunicorn` (i.e. `--production`) now respects `MRSM_CONFIG`. This is useful for running `stack up` with non-default credentials.

- **Added `--insecure` as an alias for `--no-auth`.**  
  To compliment the newly added `--secure` flag, starting the Web API with `--insecure` will bypass authentication.

- **Bump default TimescaleDB version to PG15.**  
  The default TimescaleDB version for the Meerschaum stack is now `latest-pg15-oss`.

- **Pass sysargs to `docker compose` via `stack`**  
  This patch allows for jumping into the `api` container:

  ```bash
  mrsm stack exec api bash
  ```

- **Added the API endpoint `/healthcheck`.**  
  This is used to determine reachability and the health of the local stack.

### v1.7.0 – v1.7.1 

- **Remove `get_backtrack_data()` for instance connectors.**  
  If provided, this method will still override the new generic implementation.

- **Add `--keyfile` and `--certfile` support.**  
  When starting the Web API, you may now run via HTTPS with `--keyfile` and `--certfile`. Older releases required the keys to be set in `MRSM_CONFIG`. This also brings SSL support for `--production` (Gunicorn).

- **Add the Webterm to the Web Console.**  
  At long last, the webterm is embedded within the web console and is accessible from the Web API at the endpoint `/webterm`. You must provide your active, authorized session ID to access to the Webterm.

- **Add `--secure` to `start api`.**  
  Starting the Web API with `--secure` will now disallow actions from non-administrators. This is recommend for shared deployments.

- **Fixed the registration page on the Web API.**  
  Users should now be able to create accounts from Dockerized deployments.

- **Held back `dash-extensions`**  
  The recent 1.0.2+ releases have shipped some broken changes, so `dash-extensions` is held back to `1.0.1` until newer releases have been tested.

- **Allow for digits in environment connectors.**  
  Connectors defined as environment variables may now have digits in the type.

  ```bash
  export MRSM_A2B_TEST='{"foo": "bar"}'
  ```

- **Fixed `stack` on Windows.**

- **Fixed a false error with background jobs.**

- **Increased the minimum password length to 5.**

## 1.6.x Releases

The biggest features of the 1.6.x series were all about chunking and adding support for syncing generators. The series was also full of minor bugfixes, contributing to an even more polished experience. It also was the first release to drop support for a Python version, formally deprecating Python 3.7.

### v1.6.16 – v1.6.19

- **Add Pydantic v2 support**  
  The only feature which requires Pydantic v1 is the `--schedule` flag, which will throw a warning with a hint to install an older version. The underlying libraries for this feature should have Pydantic v2 support merged soon.

- **Bump dependencies.**  
  This patch bumps the minimum required versions for `typing-extensions`, `rich`, `prompt-toolkit`, `rocketry`, `uvicorn`, `websockets`, and `fastapi` and loosens the minimum version of `pydantic`.

- **Fix shell formatting on Windows 10.**  
  Some edge case issues have been patched for older versions of Windows.
  

### v1.6.15

- **Sync chunks in the `copy pipes` action.**  
   This will help with large out-of-memory pipes.

### v1.6.14

- **Added healthchecks to `mrsm stack up`.**  
  The internal Docker Compose file for `mrsm stack` was bumped to version 3.9, and secrets were replaced with environment variable references.

- **Fixed `--no-auth` when starting the API.**  
  The command `mrsm start api --no-auth` now correctly handles sessions.

### v1.6.13

- **Remove `\\u0000` from strings when inserting into PostgreSQL.**  
  Replace both `\0` and `\\u0000` with empty strings when streaming rows into PostgreSQL.

### v1.6.12

- **Allow nested chunk generators.**  
  This patch more gracefully handles labels for situations with nested chunk generators and adds and explicit test for this scenario.

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe('foo', 'bar', instance='sql:memory')
  
  docs = [{'color': 'red'}, {'color': 'green'}]
  num_chunks = 3
  num_batches = 2

  def build_chunks():
      return (
          [
              {'chunk_ix': chunk_ix, **doc}
              for doc in docs
          ]
          for chunk_ix in range(num_chunks)
      )

  batches = (
      (
          [
              {'batch_ix': batch_ix, **doc}
              for doc in chunk
          ]
          for chunk in build_chunks()
      )
      for batch_ix in range(num_batches)
  )

  pipe.sync(batches)
  print(pipe.get_data())
  #     batch_ix  chunk_ix  color
  # 0          0         0    red
  # 1          0         0  green
  # 2          0         1    red
  # 3          0         1  green
  # 4          0         2    red
  # 5          0         2  green
  # 6          1         0    red
  # 7          1         0  green
  # 8          1         1    red
  # 9          1         1  green
  # 10         1         2    red
  # 11         1         2  green
  ```

### v1.6.11

- **Fix an issue with in-place syncing.**  
  When syncing a SQL pipe in-place with a backtrack interval, the interval is applied to the existing data stage to avoid inserting duplicate rows.

### v1.6.9 — v1.6.10

- **Improve thread safety checks.**  
  Added checks for `IS_THREAD_SAFE` to connectors to determine whether to use mutlithreading.

- **Fix an issue with custom flags while syncing.**  
  This patch includes better handling of custom flags added from plugins during the syncing process.

### v1.6.8

- **Added `as_iterator` to `Pipe.get_data()`.**  
  Passing `as_iterator=True` (or `as_chunks`) to `Pipe.get_data()` returns a generator which returns chunks of Pandas DataFrames.

  Each DataFrame is the result of a `Pipe.get_data()` call with intermediate datetime bounds between `begin` and `end` of size `chunk_interval` (default `datetime.timedelta(days=1)` for time-series / 100,000 IDs for integers).

  ```python
  import meerschaum as mrsm

  pipe = mrsm.Pipe(
      'a', 'b',
      columns={'datetime': 'id'},
      dtypes={'id': 'Int64'},
  )
  pipe.sync([
      {'id': 0, 'color': 'red'},
      {'id': 1, 'color': 'blue'},
      {'id': 2, 'color': 'green'},
      {'id': 3, 'color': 'yellow'},
  ])

  ### NOTE: due to non-inclusive end bounding,
  ###       each chunk contains
  ###       (chunk_interval - 1) rows.
  chunks = pipe.get_data(
      chunk_interval = 2,
      as_iterator = True,
  )
  for chunk in chunks:
      print(chunk)

  #    id color
  # 0   0   red
  # 1   1  blue
  #    id   color
  # 0   2   green
  # 1   3  yellow
  ```

- **Add server-side cursor support to `SQLConnector.read()`.**  
  If `chunk_hook` is provided, keep an open cursor and stream the chunks one-at-a-time. This allows for processing very large out-of-memory data sets.

  To return the results of the `chunk_hook` callable rather than a dataframe, pass `as_hook_result=True` to receive a list of values.

  If `as_iterator` is provided or `chunksize` is `None`, then `SQLConnector.read()` reverts to the default client-side cursor implementation (which loads the entire result set into memory).


  ```python
  import meerschaum as mrsm
  conn = mrsm.get_connector()

  def process_chunk(df: 'pd.DataFrame', **kw) -> int:
      return len(df)
  
  results = conn.read(
      "very_large_table",
      chunk_hook = process_chunk,
      as_hook_results = True,
      chunksize = 100,
  )

  results[:2]
  # [100, 100]
  ```

- **Remove `--sync-chunks` and set its behavior as default.**  
  Due to the above changes to `SQLConnector.read()`, `sync_chunks` now defaults to `True` in `Pipe.sync()`. You may disable this behavior with `--chunksize 0`.

### v1.6.7

- **Improve memory usage when syncing generators.**  
  To more lazily sync chunks from generators, `pool.map()` has been replaced with `pool.imap()`.

### v1.6.6

- **Issue one `ALTER TABLE` query per column for SQLite, MSSQL, DuckDB, and Oracle SQL.**  
  SQLite and other flavors do not support multiple columns in an `ALTER TABLE` query. This patch addresses this behavior and adds a specific test for this scenario.

### v1.6.5

- **Allow pipes to sync DataFrame generators.**  
  If `pipe.sync()` receives a generator (for `DataFrames`, dictionaries, or lists), it will attempt to consume it and sync its chunks in parallel threads (this can be single-threaded with `--workers 1`). For SQL pipes, this will be capped at your configured pool size (default 5) minus the running number of threads.

  This means you may now return generators to large transcations, such as reading a large CSV: 
  ```python
  def fetch(pipe, **kw) -> Iterable['pd.DataFrame']:
      return pd.read_csv('data.csv', chunksize=1000)
  ```

  Any iterator of DataFrame-like chunks will work:

  ```python
  def fetch(pipe, **kw) -> Generator[List[Dict[str, Any]]]:
      return (
          [
              {'id': 1, 'val': 10.0 * i},
              {'id': 2, 'val': 20.0 * i},
          ] for i in range(10)
      )
  ```

  This new behavior has been added to `SQLConnector.fetch()` so you may now confidently sync very large tables between your databases.

  **NOTE:** The default `chunksize` for SQL queries has been lowered to 100,000 from 1,000,000. You may alter this value with `--chunksize` or setting the value in `MRSM{system:connectors:sql:chunksize}` (you can also edit the default pool size here).

- **Fix edge case with SQL in-place syncs.**  
  Occasionally, a few docs would be duplicated when running in-place SQL syncs. This patch increases the fetch window size to mitigate the issue.

- **Remove `begin` and `end` from `filter_existing()`.**  
  The keyword arguments were interfering with the determined datetime bounds, so this patch removes these flags (albeit `begin` was already ignored) to avoid confusion. Date bounds are solely determined from the contents of the DataFrame.

### v1.6.4

- **Allow for mixed UTC offsets in datetimes.**  
  UTC offsets are now applied to datetime values before timezone information is stripped, which should now reflect accurate values. This patch also fixes edge cases when different offsets are synced within the same transcation.

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe('a', 'b', columns={'datetime': 'dt'})
  pipe.sync([
      {'dt': '2023-01-01 00:00:00+00:00'},
      {'dt': '2023-01-02 00:00:00+01:00'},
  ])
  pipe.get_data().to_dict(orient=records)
  # [
  #     {'dt': Timestamp('2023-01-01 00:00:00')},
  #     {'dt': Timestamp('2023-01-01 23:00:00')}
  # ]
  ```

- **Allow skipping datetime detection.**  
  The automatic datetime detection feature now respects a pipe's `dtypes`; columns that aren't of type `datetime64[ns]` will be ignored.

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe('a', 'b', dtypes={'not-date': 'str'})
  pipe.sync([
      {'not-date': '2023-01-01 00:00:00'}
  ])
  pipe.get_data().to_dict(orient=records)
  # [
  #     {'not-date': '2023-01-01 00:00:00'}
  # ]
  ```

- **Added utility method `enforce_dtypes()`.**  
  The DataFrame data type enforcement logic of `pipe.enforce_dtypes()` has been exposed as `meerschaum.utils.misc.enforce_dtypes()`:

  ```python
  from meerschaum.utils.misc import enforce_dtypes
  import pandas as pd
  df = pd.DataFrame([{'a': '1'}])
  enforce_dtypes(df, {'a': 'Int64'}).dtypes
  # a    Int64
  # dtype: object
  ```

- **Performance improvements.**  
  Some of the unnecessarily immutable transformations have been replaced with more memory- and compute-efficient in-place operations. Other small improvements like better caching should also speed things up.

- **Removed noise from debug output.**  
  The virtual environment debug messages have been removed to make `--debug` easier to read.

- **Better handle inferred datetime index.**  
  The inferred datetime index feature may now be disabled by setting `datetime` to `None`. Improvements were made to be handle incorrectly identified indices.

- **Improve dynamic dtypes for SQLite.**  
  SQLite doesn't allow for modifying column types but is usually dynamic with data types. A few edge cases have been solved with a workaround for altering the table's definition.

### v1.6.3

- **Fixed an issue with background jobs.**  
  A change had broken daemon functionality has been reverted.

### v1.6.2

- **Virtual environment and `pip` tweaks.**  
  With upcoming changes to `pip` coming due to PEP 668, this patch sets the environment variable `PIP_BREAK_SYSTEM_PACKAGES` when executing `pip` internally. All packages are installed within virtual environments except `uvicorn`, `gunicorn`, and those explicitly installed with a venv of `None`.

- **Change how pipes are pretty-printed.**  
  Printing the attributes of a single pipe now highlights the keys in blue.

- **Fix an issue with `bootstrap pipes` and plugins.**  
  When bootstrapping a pipe with a plugin connector, the plugin's virtual environment will now be activated while executing its `register()` function.

- **Update dependencies.**  
  The minimum version of `duckdb` was bumped to `0.7.1`, `duckdb-engine` was bumped to `0.7.0`, and `pip` was lowered to `22.0.4` to accept older versions. Additionally, `pandas==2.0.0rc1` was tested and confirmed to work, so version 1.7.x of Meerschaum will likely require 2.0+ of `pandas` to make use of its PyArrow backend.

### v1.6.0 – v1.6.1

**Breaking Changes**

- *Dropped Python 3.7 support.*  
  The latest `pandas` requires 3.8+, so to use Pandas 1.5.x, we have to finally drop Python 3.7.

- *Upgrade SQLAlchemy to 2.0.5+.*  
  This includes better transaction handling with connections. Other packages which use SQLAlchemy may not yet support 2.0+.

- *Removed `MQTTConnector`.*  
  This was one of the original connectors but was never tested or used in production. It may be reintroduced via a future `mqtt` plugin.

**Bugfixes and Improvements**

- **Stop execution when improper command-line arguments are passed in.**  
  Incorrect command-line arguments will now return an error. The previous behavior was to strip the flags and execute the action anyway, which was undesirable.

  ```bash
  $ mrsm show pipes -c

   💢 Invalid arguments:
    show pipes -c

     🛑 argument -c/-C/--connector-keys: expected at least one argument
  ```

- **Allow `bootstrap connector` to create custom connectors.**  
  The `bootstrap connector` wizard can now handle registering custom connectors. It uses the `REQUIRED_ATTRIBUTES` list set in the custom connector class when determining what to ask for.

- **Allow custom connectors to omit `__init__()`**  
  If a connector is created via `@make_connector` and doesn't have an `__init__()` function, the base one is used to create the connector with the correct type (derived from the class name) and verify the `REQUIRED_ATTRIBUTES` values if present.

- **Infer a connector's `type` from its class name.**  
  The `type` of a connector is now determined from its class name (e.g. `FooConnector` would have a type `foo`). When inheriting from `Connector`, it is no longer required to explictly pass the type before the label. For backwards compatability, the legacy method still behaves as expected.

  ```python
  from meerschaum.connectors import (
      Connector,
      make_connector,
      get_connector,
  )

  @make_connector
  class FooConnector:
      REQUIRED_ATTRIBUTES = ['username', 'password']

  conn = get_connector(
      'foo',
      username = 'abc',
      password = 'def',
  )
  ```

- **Allow connectors to omit a `label`.**  
  The default label `main` will be used if `label` is omitted.

- **Add `meta` keys to connectors.**  
  Like pipes, the `meta` property of a connector returns a dictionary with the kwargs needed to reconstruct the connector.

  ```python
  conn = mrsm.get_connector('sql:temp', flavor='sqlite', database=':memory:')
  print(conn.meta)
  # {'type': 'sql', 'label': 'temp', 'database': ':memory:', 'flavor': 'sqlite'}
  ```

- **Remove `NUL` bytes when inserting into PostgreSQL.**  
  PostgreSQL doesn't support `NUL` bytes in text (`'\0'`), so these characters are removed from strings when copying into a table.

- **Cache `pipe.exists()` for 5 seconds.**  
  Repeated calls to `pipe.exists()` will be sped up due to short-term caching. This cache is invalidated when syncing or dropping a pipe.

- **Fix an edge case with subprocesses in headless environments.**  
  Checks were added to subprocesses to prevent using interactive features when no such features may be available (i.e. `termios`).

- **Added `pprint()`, `get_config()`, and `attempt_import()` to the top-level namespace.**  
  Frequently used functions `pprint()`, `get_config()`, and `attempt_import()` have been promoted to the root level of the `meerschaum` namespace, i.e.:

  ```python
  import meerschaum as mrsm
  mrsm.pprint(mrsm.get_config('meerschaum'))

  sqlalchemy = mrsm.attempt_import('sqlalchemy')
  ```

- **Fix CLI for MSSQL.**  
  The interactive CLI has been fixed for Microsoft SQL Server.

## 1.5.x Releases

The 1.5.x series offered many great improvements, namely the ability to use an integer datetime axis and the addition of JSON columns.

### v1.5.8 – v1.5.10

- **Infer JSON columns from the first first non-null value.**  
  When determining complex columns (dictionaries or lists), the first non-null value of the dataframe is checked rather than the first row only. This accounts for documents which contain variable keys in the same sync, e.g.:

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe('a', 'b')
  pipe.sync([
      {'a': {'b': 1}},
      {'c': {'d': 2}},
  ])
  ```

- **Fix a bug when reconstructing JSON columns.**  
  When rebuilding JSON values after merging, a check is first performed if the value is in fact a string (sometimes `NULLS` slip in).

- **Increase the timeout when determining Python versions.**  
  This fixes some difficult-to-reproduce bugs on Windows.


### v1.5.7

- **Replace `ast.literal_eval()` with `json.loads()` when filtering JSON columns.**  
  This patch replaces the use of `str` and `ast.literal_eval()` with `json.dumps()` and `json.loads()` to preserve accuracy.

- **Fix a subtle bug with subprocesses.**  
  The function `run_python_package()` now better handles environment passing and raises a more verbose warning when something goes wrong.

- **Allow columns with `'create'` in the name.**  
  A security measure previously disallowed certain keywords when sanitizing input. Now columns are allowed to contain certain keywords.

### v1.5.3 – v1.5.6

- **Pipes now support syncing dictionaries and lists.**  
  Complex columns (dicts or lists) will now be preserved:

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe('a', 'b')
  pipe.sync([{'a': {'b': 1}}])
  df = pipe.get_data()
  print(df['a'][0])
  # {'b': 1}
  ```

  You can also force strings to be parsed by setting the data type to `json`:

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe(
      'foo', 'bar',
      columns = {'datetime': 'id'},
      dtypes = {'data': 'json', 'id': 'Int64'},
  )
  docs = [{'id': 1, 'data': '{"foo": "bar"}'}]
  pipe.sync(docs)
  df = pipe.get_data()
  print(df['data'][0])
  # {'foo': 'bar'}
  ```

  For PostgreSQL-like databases (e.g. TimescaleDB), this is stored as `JSONB` under the hood. For all others, it's stored as the equivalent for `TEXT`.

- **Fixed determining the version when installing plugins.**  
  Like the `required` list, the `__version__` string must be explicitly set in order for the correct version to be determined.

- **Automatically cast `postgres` to `postgresql`**  
  When a `SQLConnector` is built with a flavor of `postgres`, it will be automatically set to `postgresql`.

### v1.5.0 – v1.5.2

- **Pipes may now use integers for the `datetime` column.**  
  If you use an auto-incrementing integer as your primary key, you may now use that column as your pipe's `datetime` column, just specify the `dtype` as an `Int64`:

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe(
      'foo', 'bar',
      instance = 'sql:memory',
      columns = {
          'datetime': 'id',
      },
      dtypes = {
          'id': 'Int64',
      },
  )
  pipe.sync([{'id': 1, 'foo': 'bar'}])
  pipe.sync([{'id': 2, 'foo': 'baz'}])
  ```

  This applies the same incremental range filtering logic as is normally done on the datetime axis.

- **Allow for multiple plugins directories.**  
  You may now set multiple directories for `MRSM_PLUGINS_DIR`. All of the plugins contained in each directory will be symlinked together into a single `plugins` namespace. To do this, just set `MRSM_PLUGINS_DIR` to a JSON-encoded list:

  ```bash
  export MRSM_PLUGINS_DIR='["./plugins_1", "./plugins_2"]'
  ```

- **Better Windows support.**  
  At long last, the color issues plaguing Windows users have finally been resolved. Additionally, support for background jobs has been fixed on Windows, though the daemonization library I use is pretty hacky and doesn't make for the smoothest experience. But at least it works now!

- **Fixed unsafe TAR extraction.**  
  A [PR about unsafe use of `tar.extractall()`](https://github.com/bmeares/Meerschaum/pull/100) brought this issue to light.

- **Fixed the blank logs bug in `show logs`.**  
  Backtracking a couple lines before following the rest of the logs has been fixed.

- **Requirements may include brackets.**  
  Python packages listed in a plugin's `requirements` list may now include brackets (e.g. `meerschaum[api]`).

- **Enforce 1000 row limit in `SQLConnector.to_sql()` for SQLite.**  
  When inserting rows, the chunksize of 1000 is enforced for SQLite (was previously enforced only for reading).

- **Patch parameters from `--params` in `edit pipes` and `register pipes`.**  
  When editing or registering pipes, the value of `--params` will now be patched into the pipe's parameters. This should be very helpful when scripting.

- **Fixed `edit users`.**  
  This really should have been fixed a long time ago. The action `edit users` was broken due to a stray import left over from a major refactor.

- **Fixed a regex bug when cleaning up packages.**
- **Removed `show gui` and `show modules`.**


## 1.4.x Releases

The 1.4.x series brought some incredible, stable releases, and the highlight feature was in-place SQL syncs for massive performance improvement. The addition of `temporary` to Pipes also made using pipes in projects more accessible.

### v1.4.14

- **Added flag `temporary` to `Pipe` (and `--temporary`).**  
  Pipes built with `temporary=True`, will not create instance tables (`pipes`, `users`, and `plugins`) or be able to modify registration. This is particularly useful when creating pipes from existing tables when automatic registration is not desired.

  ```python
  import meerschaum as mrsm
  import pandas as pd
  conn = mrsm.get_connector('sql:temp', uri='postgresql://user:pass@localhost:5432/db')

  ### Simulating an existing table.
  table_name = 'my_table'
  conn.to_sql(
      pd.DataFrame([{'id_column': 1, 'value': 1.0}]),
      name = table_name,
  )

  ### Create a temporary pipe with the existing table as its target.
  pipe = mrsm.Pipe(
      'foo', 'bar',
      target = table_name,
      temporary = True,
      instance = conn,
      columns = {
          'id': 'id_column',
      },
  )

  docs = [
      {
          "id_column": 1,
          "value": 123.456,
          "new_column": "hello, world!",
      },
  ]

  ### Existing table `my_table` is synced without creating other tables
  ### or affecting pipes' registration.
  pipe.sync(docs)
  ```

- **Fixed potential security of public instance tables.**  
  The API now refuses to sync or serve data if the target is a protected instance table (`pipes`, `users`, or `plugins`).

- **Added not-null check to `pipe.get_sync_time().`**  
  The `datetime` column should never contain null values, but just in case, `pipe.get_sync_time()` now passes a not-null check to `params` for the datetime column.

- **Removed prompt for `value` from `pipe.bootstrap()`.**  
  The prompt for an optional `value` column has been removed from the bootstrapping wizard because `pipe.columns` is now largely used as a collection of indices rather than the original purpose of meta-columns.

- **Pass `--debug` and other flags in `copy pipes`.**  
  Command line flags are now passed to the new pipe when copying an existing pipe.

### v1.4.12 – v1.4.13

- **Fixed an issue when syncing empty DataFrames [(#95)](https://github.com/bmeares/Meerschaum/issues/95).**  
  When syncing an empty list of documents, `Pipe.filter_existing()` would trigger pulling the entire table into memory. This patch adds a check if the dataframe is empty.

- **Allow the `datetime` column to be omitted in the `bootstrap` wizard.**  
  Now that the `datetime` index is optional, the bootstrapping wizard allows users to skip this index.

- **Fixed a small issue when syncing to MySQL.**  
  Due to the addition of MySQL 5.7 support in v1.4.11, a slight edge case arose which broke SQL definitions. This patch fixes MySQL behavior when a `WHERE` clause is present in the definition.

### v1.4.11

- **Add support for older versions of MySQL.**  
  The `WITH` keyword for CTE blocks was not introduced until MySQL 8.0. This patch uses the older syntax for older versions of MySQL and MariaDB. MySQL 5.7 was added to the test suite.

- **Allow for any iterable in `items_str()`**  
  If an iterable other than a list is passed to `items_str()`, it will convert to a list before building the string:

  ```python
  from meerschaum.utils.misc import items_str
  print(items_str({'apples': 1, 'bananas': 2}, quotes=False)
  # apples and bananas
  ```

- **Fixed an edge case with `datetime` set to `None`.**  
  This patch will ignore the datetime index even if it was set explicitly to `None`.

- **Added `Pipe.children`.**  
  To complement `Pipe.parents`, setting the parameters key `children` to a list of pipes' keys will be treated the same as `Pipe.parents`:

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe(
      'a', 'b',
      parameters = {
          'children': [
              {
                  'connector': 'a',
                  'metric': 'b',
                  'location': 'c',
              },
          ]
      }
  )
  print(pipe.children)
  # [Pipe('a', 'b', 'c')]
  ```

- **Added support for `type:label` syntax in `mrsm.get_connector()`.**  
  The factory function `mrsm.get_connector()` expects the type and label as two arguments, but this patch allows for passing a single string with both arguments:

  ```python
  import meerschaum as mrsm
  print(mrsm.get_connector('sql:local'))
  # sql:local
  ```

- **Fixed more edge case bugs.**  
  For example, converting to `Int64` sometimes breaks with older versions of `pandas`. This patch adds a workaround.

### v1.4.10

- **Fixed an issue with syncing background jobs.**  
  The `--name` flag of background jobs with colliding with the `name` keyword argument of `SQLConnector.to_sql()`.

- **Fixed a datetime bounding issue when `datetime` index is omitted.**  
  If the minimum datetime value of the incoming dataframe cannot be determined, do not bound the `get_data()` request.

- **Keep existing parameters when registering plugin pipes.**  
  When a pipe is registered with a plugin as its connector, the return value of the `register()` function will be patched with the existing in-memory parameters.

- **Fixed a data type syncing issue.**  
  In cases where fetched data types do not match the data types in the pipe's table (e.g. automatic datetime columns), a bug has been patched to ensure the correct data types are enforced.

- **Added `Venv` to the root namespace.**  
  Now you can access virtual environments directly from `mrsm`:

  ```python
  import meerschaum as mrsm

  with mrsm.Venv('noaa'):
      import pandas as pd
  ```


### v1.4.9

- **Fixed in-place syncs for aggregate queries.**  
  In-place SQL syncs which use aggregation functions are now handled correctly. This version addresses differences in column types between backtrack and new data. For example, the following query will now be correctly synced:

  ```sql
  WITH days_src AS (
    SELECT *, DATE_TRUNC('day', "datetime") AS days
    FROM plugin_stress_test
  )
  SELECT days, AVG(val) AS avg_value
  FROM days_src
  GROUP BY days
  ```

- **Activate virtual environments for custom instance connectors.**  
  All pipe methods now activate virtual environments for custom instance connectors.

- **Improved database connection performance.**  
  Cold connections to a SQL database have been sped up by replacing `sqlalchemy_utils` with handwritten logic (JSON for PostgreSQL-like and SQLite).

- **Fixed an issue with virtual environment verification in a portable environment.**  
  The portable build has been updated to Python 3.9.15, and this patch includes a check to determine the known `site-package` path for a virtual environment of `None` instead of relying on the default user `site-packages` directory.

- **Fixed some environment warnings when starting the API**

### v1.4.5 – v1.4.8

- **Bugfixes and stability improvements.**  
  These versions included several bugfixes, such as patching `--skip-check-existing` for in-place syncs and fixing the behavior of `--params` ([`build_where()`](https://docs.meerschaum.io/utils/sql.html#meerschaum.utils.sql.build_where)).

### v1.4.0 – v1.4.4

- **Added in-place syncing for SQL pipes.**  
  This feature is big (enough to warrant a new point release). When pipes with the same instance connector and data source connector are synced, the method `sync_pipe_inplace()` is invoked. For SQL pipes, this means the entire syncing process will now happen entirely in SQL, which can lead to massive performance improvements.

  ```python
  import meerschaum as mrsm
  import pandas as pd

  conn = mrsm.get_connector('sql', 'local')
  conn.to_sql(pd.DataFrame([{'a': 1}]), 'foo')
  
  pipe = mrsm.Pipe(
      "sql:local", "foo",
      instance = "sql:local",
      parameters = {
          "query": "SELECT * FROM foo"
      },
  )
  ### This will no longer load table data into memory.
  pipe.sync()
  ```

  This applies even when the source table's schema changes, just like the dynamic columns feature added in v1.3.0.

  > *To disable this behavior, run the command `edit config system` and set the value under the keys `experimental:inplace_sync` to `false`.*

- **Added negation to `--params`.**  
  The [`build_where()`](https://docs.meerschaum.io/utils/sql.html#meerschaum.utils.sql.build_where) function now allows you to negate certain values when prefixed with an underscore (`_`):

  ```bash
  ### Show recent data, excluding where `id` equals `2`.
  mrsm show data --params id:_2
  ```

- **Added `--params` to SQL pipes' queries.**  
  Specifying parameters when syncing SQL pipes will add those constraints to the fetch stage.

- **Skip invalid parameters in `--params`.**  
  If a column does not exist in a pipe's table, the value will be ignored in `--params`.

- **Fixed environment issue when starting the Web API with `gunicorn`.**
- **Added an emoji to the SQL Query option of the web console.**
- **Fixed an edge case with data type enforcement.**
- **Other bugfixes**

## 1.3.x Releases

The 1.3.x series brought a tremendous amount of new features and stability improvements. Read below to see everything that was introduced!

### v1.3.13

- **Fixed an issue when displaying backtrack data on the Web Console.**  
  Certain values like `pd.NA` would break the Recent Data view on the Web Console. Now the values are cast to strings before building the table.

- **Added YAML and JSON support to editing parameters.**  
  YAML is now the default, and toggle buttons have been added to switch the encoding. Line numbers have also been added to the editors.

- **Removed the index column from the CSV downloader.**  
  When the download button is clicked, the dataframe's index column will be omitted from the CSV file.

- **Changed the download filename to `<target>.csv`.**  
  The download process will now name the CSV file after the table rather than the pipe.

- **Web Console improvements.**  
  The items in the actions builder now are presented with a monospace font. Actions and subactions will have underscores represented as spaces.

- **Activating the virtual environment `None` will not override your current working directory.**  
  This is especially useful when testing the API. Activating the virtual environment `None` will insert behind your current working directory or `''` in `sys.path`.

- **Added WebSocket Secure Support.**  
  This has been coming a long time, so I'm proud to announce that the web console can now detect whether the client is connecting via HTTPS and (assuming the server has the appropriate [proxy configuration](http://nginx.org/en/docs/http/websocket.html)) will connect via WSS.

### v1.3.10 – v1.3.12

- **Fixed virtual environment issues when syncing.**  
  This one's a doozy. Before this patch, there would frequently be warnings and sometimes exceptions thrown when syncing a lot of pipes with a lot of threads. This kind of race condition can be hard to pin down, so this patch reworks the virtual environment resolution system by keeping track of which threads have activated the environments and refusing to deactivate if other threads still depend on the environment. To enforce this behavior, most manual ivocations of [`activate_venv()`](https://docs.meerschaum.io/utils/venv/index.html#meerschaum.utils.venv.activate_venv) were replaced with the [`Venv` context manager](https://docs.meerschaum.io/utils/venv/index.html#meerschaum.utils.venv.Venv). Finally, the last stage of each action is to clean up any stray virtual environments. *Note:* You may still run into the wrong version of a package being imported into your plugin if you're syncing a lot of plugins concurrently.

- **Allow custom instance connectors to be selected on the web console.**  
  Provided all of the appropriate interface methods are implemented, selecting a custom instance connector from the instance dropdown should no longer throw an error.

- **Bugfixes and improvements to the virtual environment system.**  
  This patch *should* resolve your virtual environment woes but at a somewhat significant performance penalty. Oh well, 'tis the price we must pay for correct and determinstic code!

- **Fixed custom flags added by `add_plugin_argument()`.**  
  Refactoring work to decouple plugins from the argument parser had the unintended side effect of skipping over custom flags until after sysargs had already been parsed. This patch ensures all plugins with `add_plugin_argument` in their root module will be loaded before parsing.

- **Upgraded `dash-extensions`, `dash`, and `dash-bootstrap-components`.**  
  At long last, `dash-extensions` will no longer need to be held back.

- **Added additional websockets endpoints.**  
  The endpoints `/ws`, `/dash/ws/`, and `/dashws` resolve to the same handler. This is to allow compatability with different versions of `dash-extensions`.

- **Allow for custom arguments to be added from outside plugins.**  
  The function `add_plugin_argument()` will now accept arguments when executed from outside a plugin.

- **Fixed `verify packages`.**

### v1.3.6 – v1.3.9

- **Allow for syncing multiple data types per column.**  
  The highlight of this release is support for syncing multiple data types per column. When different data types are encountered, the underlying column will be converted to `TEXT`:

  ```python
  import meerschaum as mrsm
  pipe = mrsm.Pipe('a', 'b', instance='sql:memory')
  
  pipe.sync([{'a': 1}])
  print(pipe.get_data())
  #    a
  # 0  1

  pipe.sync([{'a': 'foo'}])
  #      a
  # 0    1
  # 1  foo
  ```

- **Cleaned up the Web Console.**  
  The web console's navbar is now more mobile-friendly, and a "sign out" button has been added.

- **Removed plugins import when checking for environment connectors.**  
  This should make some commands feel more snappy by lazy loading custom connectors.
- **Fixed an issue with an updated versin of `uvicorn`.**
- **Fixed an issue with `docker-compose`.**
- **Fixed an issue with `FastAPI` on Python 3.7.**
- **Added support for Python 3.11.**
- **Renamed `meerschaum.actions.arguments` to `meerschaum._internal.arguments`.**


### v1.3.4 – v1.3.5

- **Define environment connectors with JSON or URIs.**  
  Connectors defined as environment variables may now have their attributes set as JSON in addition to a URI string.

- **Custom Connectors may now be defined as environment variables.**  
  You may now set environment variables for custom connectors defined via `@make_connector`, e.g.:

  ```bash
  MRSM_FOO_BAR='{"foo": "bar"}' mrsm show connectors
  ```

- **Allow for custom connectors to be instance connectors.**  
  Add the property `IS_INSTANCE = True` your custom connector to add it to the official list of instance types:

  ```python
  from meerschaum.connectors import make_connector, Connector

  @make_connector
  class FooConnector(Connector):
      IS_INSTANCE = True

      def __init__(label: str, **kw):
          super().__init__('foo', label, **kw)
  ```

- **Install packages for all plugins with `mrsm install required`.**  
  The default behavior for `mrsm install required` with no plugins named is now to install dependencies for all plugins.

- **Syncing bugfixes.**

### v1.3.2 – v1.3.3

- **Fixed a bug with `begin` and `end` bounds in `Pipe.get_data()`.**  
  A safety measure was incorrectly checking if the quoted version of a column was in `pipe.get_columns_types()`, not the unquoted version. This patch restores functionality for `pipe.get_data()`.
- **Fixed an issue with an upgraded version of `SQLAlchemy`.**
- **Added a parameters editor the Web UI.**  
  You may now edit your pipes' parameters in the browser through the Web UI!
- **Added a SQL query editor to the Web UI.**  
  Like the parameters editor, you can edit your pipes' SQL queries in the browser.
- **Added a Sync Documents option to the Web UI.**  
  You can directly sync documents into pipes on the Web UI.
- **Added the arguments `order`, `limit`, `begin_add_minutes`, and `end_add_minutes` to `Pipe.get_data()`.**  
  These new arguments will give you finer control over the data selection behavior.
- **Enforce consistent ordering of indices in `Pipe.get_data()`.**
- **Allow syncing JSON-encoded strings.**  
  This patch allows pipes to sync JSON strings without first needing them to be deserialized.
- **Fixed an environment error with Ubuntu 18.04.**
- **Bumped `duckdb` and `duckdb-engine`.**
- **Added a basic CLI for `duckdb`.**  
  This will probably be replaced later down the line.

### v1.3.1

- **Fixed data type enforcement issues.**  
  A serious bug in data type enforcement has been patched.
- **Allow `Pipe.dtypes` to be edited.**  
  You can now set keys in `Pipe.dtypes` and persist them with `Pipe.edit()`.
- **Added `Pipe.update()`.**  
  `Pipe.update()` is an alias to `Pipe.edit(interactive=False)`.
- **`Pipe.delete()` no longer deletes local attributes.**  
  It still removes `Pipe.id`, but local attributes will now remain intact.
- **Fixed dynamic columns on DuckDB.**  
  DuckDB does not allow for altering tables when indices are created, so this patch will drop and rebuild indices when tables are altered.
- **Replaced `CLOB` with `NVARCHAR(2000)` on Oracle SQL.**  
  This may require migrating existing pipes to use the new data type.
- **Enforce integers are of type `INTEGER` on Oracle SQL.**  
  Lots of data type enforcement has been added for Oracle SQL.
- **Removed datetime warnings when syncing pipes without a datetime column.**
- **Removed grabbing the current time for the sync time if a sync time cannot be determined.**

### v1.3.0: Dynamic Columns

**Improvements**

  - **Syncing now handles dynamic columns.**  
    Syncing a pipe with new columns will trigger an `ALTER TABLE` query to append the columns to your table:

    ```python
    import meerschaum as mrsm
    pipe = mrsm.Pipe('foo', 'bar', instance='sql:memory')
    
    pipe.sync([{'a': 1}])
    print(pipe.get_data())
    #    a
    # 0  1

    pipe.sync([{'b': 1}])
    print(pipe.get_data())
    #       a     b
    # 0     1  <NA>
    # 1  <NA>     1
    ```

    If you've specified index columns, you can use this feature to fill in `NULL` values in your table:

    ```python
    import meerschaum as mrsm
    pipe = mrsm.Pipe(
        'foo', 'bar',
        columns = {'id': 'id_col'},
        instance = 'sql:memory',
    )

    pipe.sync([{'id_col': 1, 'a': 10.0}])
    pipe.sync([{'id_col': 1, 'b': 20.0}])

    print(pipe.get_data())
    #    id_col     a     b
    # 0       1  10.0  20.0
    ```

  - **Add as many indices as you like.**  
    In addition to the special index column labels `datetime`, `id`, and `value`, the values of all keys within the `Pipe.columns` dictionary will be treated as indices when creating and updating tables:

    ```python
    import meerschaum as mrsm
    indices = {'micro': 'station', 'macro': 'country'}
    pipe = mrsm.Pipe('demo', 'weather', columns=indices, instance='sql:memory')

    docs = [{'station': 1, 'country': 'USA', 'temp_f': 80.6}]
    pipe.sync(docs)

    docs = [{'station': 1, 'country': 'USA', 'temp_c': 27.0}]
    pipe.sync(docs)

    print(pipe.get_data())
    #    station  country  temp_f  temp_c
    # 0        1      USA    80.6    27.0
    ```

  - **Added a default 60-second timeout for pipe attributes.**  
    All parameter properties (e.g. `Pipe.columns`, `Pipe.target`, `Pipe.dtypes`, etc.) will sync with the instance every 60 seconds. The in-memory attributes will be patched on top of the database values, so your unsaved state won't be lost (persist your state with `Pipe.edit()`). You can change the timeout duration with `mrsm edit config pipes` under the keys `attributes:local_cache_timeout_seconds`. To disable this caching behavior, set the value to `null`.

  - **Added custom indices and Pandas data types to the Web UI.**

**Breaking Changes**
  
  - **Removed `None` as default for uninitalized properties for pipes.**  
    Parameter properties like `Pipe.columns`, `Pipe.parameters`, etc. will now always return a dictionary, even if a pipe is not registered.
  - **`Pipe.get_columns()` now sets `error` to `False` by default.**  
    Pipes are now mostly index-agnostic, so these checks are no longer needed. This downgrades errors in several functions to just warnings, e.g. `Pipe.get_sync_time()`.

**Bugfixes**

  - **Always quote tables that begin with underscores in Oracle.**
  - **Always refresh the metadata when grabbing `sqlalchemy` tables for pipes to account for dynamic state.**

## 1.2.x Releases

This series brought many industry-ready features, such as the `@make_connector` decorator, improvements to the virtual environment system, the environment variable `MRSM_PLUGINS_DIR`, and much more.

### v1.2.9

- **Added support for Windows junctions for virtual environments.**  
  This included many changes to fix functionality on Windows. For example, the addition of the `MRSM_PLUGINS_DIR` environment variable broke Meerschaum on Windows, because Windows requires administrator rights to create symlinks.

### v1.2.8

- **Custom connectors may now have `register(pipe)` methods.**  
  Just like the module-level `register(pipe)` plugin function, custom connectors may also provide this function as a class member.
- **Print a traceback if `fetch(pipe)` breaks.**  
  A more verbose traceback is printed if a plugin breaks during the syncing process.
- **Cleaned up `sync pipes` output.**  
  This patch cleans up the syncing process's pretty output.
- **Respect `--nopretty` in `sync pipes`.**  
  This flag will only print JSON-encoded dictionaries for `sync pipes`. Tracebacks may still interfere without standard output, however.

### v1.2.5 – v1.2.7

- **`Venv` context managers do not deactivate previously activated venvs.**  
  You can safely use `Venv` without worrying about deactivating your previously activated environments.
- **Better handling of nested plugin dependencies.**
- **`Plugin.get_dependencies()` will not trigger an import.**  
  If you want certainty about a plugin's required list, trigger an import manually. Otherwise, it will use `ast.literal_eval()` to determine the required list from the source itself. This only works for statically set `required` lists.
- **Provide rich traceback for broken plugins.**  
  If a plugin fails to import, a nice traceback is printed out in addition to a warning.
- **Only cache `Pipe.dtypes` if the pipe exists.**
- **Pass current environment to subprocesses.**  
  This should retain any custom configuration you've set in the main process.
- **Hard-code port 5432 as the target DB container port in the stack.**  
  Changing the host port now will not change the target port in the container.
- **Fixed a bug with background jobs and `to_sql()`.**  
  The `--name` flag was conflicting with `to_sql()`.
- **Reimplemented `apply_patch_to_config()`.**  
  This patch removes `cascadict` as a vendored dependency and replaces it with a simpler implementation.
- **Removed network request for shell connectivity status.**  
  The shell now simply checks for the existence of the connector. This may occasionally print an inaccurate connection status, but the speed benefit is worth it.
- **Moved `dill` and other required dependencies into the `sql` dependency group.**
- **Replaced `redengine` with `rocketry`.**
- **Patched `Literal` into `typing` for Python 3.7.**
- **Fixed shell commands.**  
  This includes falling back to `' '.join` instead of `shlex.join` for Python 3.7.

### v1.2.1 – v1.2.4

- **Added the `@make_connector` decorator.**  
  Plugins may now extend the base `Connector` class to provide custom connectors. For most cases, the built-in `plugin` connector should work fine. This addition opens up the internal connector system so that plugin authors may now add new types. See below for a minimal example of a new connector class:

  ```python
  # ~/.config/meerschaum/plugins/example.py

  from meerschaum.connectors import make_connector, Connector

  REQUIRED_ATTRIBUTES = {'username', 'password'}

  @make_connector
  class FooConnector(Connector):
      
      def __init__(self, label: str, **kw):
          """
          Instantiate the base class and verify the required attributes are present.
          """
          kw['label'] = label
          super().__init__('foo', **kw)
          self.verify_attributes(REQUIRED_ATTRIBUTES)

      
      def fetch(self, pipe, **kw):
          """
          Return a dataframe (or list of dicts / dict of lists).
          The same as you would in a regular fetch plugin, except that now
          we can store configuration within the connector itself.
          """
          return [{self.username: '2020-01-01'}]
  ```

- **Allow for omitting `datetime` column index.**  
  The `datetime` column name is still highly recommended, but recent changes have allowed pipes to be synced without a dedicated datetime axis. Plenty of warnings will be thrown if you sync your pipes without specifying a datetime index. If a datetime column can be found, it will be used as the index.

  ```python
  import meerschaum as mrsm
  ### This will work but throw warnings.
  mrsm.Pipe('foo', 'bar').sync([{'a': 1}])
  ```

### v1.2.0

**Improvements**

- **Added the action `start connectors`.**  
  This command allows you to wait until all of the specified connectors are available and accepting connections. This feature is very handy when paired with the new `MRSM_SQL_X` URI environment variables.

- **Added `MRSM_PLUGINS_DIR`.**  
  This one's been on my to-do list for quite a bit! You can now place your plugins in a dedicated, version-controlled directory outside of your root directory.
  
  Like `MRSM_ROOT_DIR`, specify a path with the environment variable `MRSM_PLUGINS_DIR`:

  ```bash
  MRSM_PLUGINS_DIR=plugins \
    mrsm show plugins
  ```

- **Allow for symlinking in URI environment variables.**  
  You may now reference configuration keys within URI variables:

  ```bash
  MRSM_SQL_FOO=postgresql://user:MRSM{meerschaum:connectors:sql:main:password}@localhost:5432/db
  ```

- **Increased token expiration window to 12 hours.**  
  This should reduce the number of login requests needed.

- **Improved virtual environment verification.**  
  More edge cases have been addressed.

- **Symlink Meerschaum into `Plugin` virtual environments.**  
  If plugins do not specify `Meerschaum` in the `required` list, Meerschaum will be symlinked to the currently running package.

**Breaking changes**

- **API endpoints for registering and editing users changed.**  
  To comply with OAuth2 convention, the API endpoint for registering a user is now a url-encoded form submission to `/users/register` (`/user/edit` for editing).

    ***You must upgrade both the server and client to v1.2.0+ to login to your API instances.***

- **Replaced `meerschaum.utils.sql.update_query()` with `meerschaum.utils.sql.get_update_queries()`.**  
  The new function returns a list of query strings rather than a single query. These queries are executed within a single transaction.

**Bugfixes**

- **Removed version enforcement in `pip_install()`.**  
  This changed behavior allows for custom version constraints to be specified in Meerschaum plugins.

- **Backported `UPDATE FROM` query for older versions of SQLite.**  
  The current mutable data logic uses an `UPDATE FROM` query, but this syntax is only present in versions of SQLite greater than 3.33.0 (released 2020-08-14). This releases splits the same logic into `DELETE` and `INSERT` queries for older versions of SQLite.

- **Fixed missing suggestions for shell-only commands.**  
  Completions for commands like `instance` are now suggested.

- **Fixed an issue with killing background jobs.**  
  The signals were not being sent correctly, so this release includes better job process management.


## 1.1.x Releases

The 1.1.x series brought a lot of great new features, notably connector URI parsing (e.g. `MRSM_SQL_<LABEL>`), parsing underscores as spaces in actions, and rewriting the Docker image to run at as a normal user.

### v1.1.9 – v1.1.10

- **Fixed plugins virtual environments.**  
  A typo in v1.1.8 temporarily broke plugins, and this patch fixes that change.
- **Fixed Meerschaum on Windows.**  
  A change in a previous release allowed for dist-packages for the root user (not advised but supported). The check for root (`os.geteuid()`) does not exist on Windows, so this patch accounts for that behavior.
- **Tweaked screen clearing on Windows.**  
  Meerschaum now always uses `clear` or `cls` on Windows instead of ANSI escape sequences.

### v1.1.5 – v1.1.8

- **Fixed `MRSM_PATCH` behavior.**  
  In the docker image, `MRSM_PATCH` is used to overwrite `host` for `sql:main`. This patch restores that behavior (with a performance boost).
- **Fixed virtual environment verification.**  
  This patch prevents circular symlinks.
- **Fixed `manually_import_module()`.**  
  Previous refactoring efforts had broken `manually_import_module()`.
- **Refactoring**  
  While trying to implement multi-thread configuration patching (discarded for the time being), much of the configuration system was cleaned up.

### v1.1.1 – v1.1.4

**Bugfixes**

The first four versions following the initial v1.1.0 release addressed breaking changes and edge cases. Below are some notable issues resolved:

- **Fixed broken Docker images.**  
  Changes to the environment and package systems broke functionality of the Docker images. For example, v1.1.0 switched to a stricter package management policy, but this new policy broke the mechanism behind the Docker images (user-level vs venv-level packages).
- **Verify virtual environments for multiple Python versions.**  
  When a virtual environment is first activated, Meerschaum now verifies that the `python` symlinks point to the correct versions. This is necessary due to a quirk in `venv` and `virtualenv` where activating an existing environment with a different Python version overwrites the existing `python` symlink. It also ensures that the symlinks specify the correct version number, e.g. `python3.10`. This bevavior is now automatic but may be invoked with `mrsm verify venvs`.
- **Fixed inconsistent environment behavior with `gunicorn`.**  
  This one was tricky to troubleshoot. Due to the migration to the user-level Docker image, a subtle bug surfaced where the environment variables for `gunicorn` were incorrectly serialized.
- **Fixed slow-performing edge cases in `determine_version()`.**  
  Inconsistencies in naming conventions in some packages like `pygments` led to failures to quickly determine the version.
- **Fixed Web API actions.**  
  In v1.1.0, the default virtual environment was pinned to `mrsm`, and this broke a function which relied on the old inferred default value of `None`. Always remember: explicit is better than implicit.
- **Fixed `start job` for existing jobs.**  
  The same naming change broke `daemon_action()`. Explcit code is important, folks!


### v1.1.0

**What's New**

- **Underscores in actions may now be parsed as spaces.**  
  This took way more work than expected, but anyway, custom actions with underscores in the function names are now treated as spaces! Consider the following:

  ```python
  @make_action
  def foo_bar(**kw):
      return True, "Huzzah!"
  ```

  The above action may now be executed as `foo bar` or `foo_bar`:

  ```bash
  mrsm foo bar
  ```

- **Create a `SQLConnector` or `APIConnector` directly from a URI.**  
  If you already have a connection string, you can skip providing credentials and build a connector directly from the URI. If you omit a `label`, then the lowercase form of `'<username>@<host>/<database>'` is used:

  ```python
  from meerschaum.connectors import SQLConnector
  uri = 'postgresql://user:pass@localhost:5432/db'

  conn = SQLConnector(uri=uri)
  print(conn)
  # sql:user@localhost/db

  conn = SQLConnector('foo', uri=uri)
  print(conn)
  # sql:foo

  conn = SQLConnector.from_uri(uri)
  print(conn)
  # sql:user@localhost/db
  ```

  The `APIConnector` may also be built from a URI:

  ```python
  from meerschaum.connectors import APIConnector
  uri = 'http://user:pass@localhost:8000'

  conn = APIConnector(uri=uri)
  print(conn)
  # api:username@localhost

  conn = APIConnector('bar', uri=uri)
  print(conn)
  # api:bar

  conn = APIConnector.from_uri(uri)
  print(conn)
  # api:user@localhost
  ```

- **Define temporary connectors from URIs in environment variables.**  
  If you set environment variables with the format `MRSM_SQL_<LABEL>` to valid URIs, new connectors will be available under the keys `sql:<label>`, where `<label>` is the lowercase form of `<LABEL>`:

  ```bash
  export MRSM_SQL_FOO=sqlite://///path/to/sqlite.db
  mrsm show connectors sql:foo
  ```

  You can set as many connectors as you like, and they're treated the same as connectors registered in your permanent configuration.

  ```python
  ### The following environment variable was already exported:
  ### MRSM_SQL_FOO=sqlite://///path/to/sqlite.db

  import meerschaum as mrsm
  conn = mrsm.get_connector('sql', 'foo')
  print(conn.database)
  # /path/to/sqlite.db
  ```

**Bugfixes**

- **Resolved issues with conflicting virtual and base environments.**
- **Only reinstall a package available at the user-level if its version doesn't match.**  
  This was a subtle bug, but now packages are handled strictly in virtual environments except when an appropriate version is available. This may slow down performance, but the change is necessary to ensure a consistent environment.

**Potentially Breaking Changes**

- **The database file path for SQLite and DuckDB is now required.**  
  When creating a `SQLConnector` with the flavors `sqlite` or `duckdb`, the attribute `database` (a file path or `:memory:`) is now required.
- **Removed `--config` and `--root-dir`.**  
  These flags were added very early on but have always caused issues. Instead, please use the environment variables `MRSM_CONFIG` or `MRSM_PATCH` for modifying the runtime configuration, and use `MRSM_ROOT_DIR` to specify a file path to the root Meerschaum directory.
- **Virtual environments must be `None` for standard library packages.**  
  When importing a built-in module with `attempt_import()`, specify `venv=None` to avoid attempting installation.
- **The Docker image now runs as `meerschaum` instead of `root`.**  
  For improved security, the docker image now runs at a lower privilege.

## 1.0.x Releases

The v1.0.0 release was big news. A ton of features, bugfixes, and perfomance improvements were introduced: for example, v1.0.0 brought support for mutable pipes and data type enforcement. Later releases in the v1.0.x series included `--schedule`, the `Venv` context manager, and a whole lot of environment bugfixes.

### v1.0.6

- **Plugins may now have `requirements.txt`.**  
  If a plugin contains a file named `requirements.txt`, the file will be parsed alongside the packages specified in the `required` list.
- **Added the module `meerschaum.utils.venv`.**  
  Functions related to virtual environment management have been migrated from `meerschaum.utils.packages` to [`meerschaum.utils.venv`](https://docs.meerschaum.io/utils/venv/index.html).
- **Added the `Venv` class.**  
  You can now manage your virtual environments with the `Venv` context manager:

  ```python
  from meerschaum.utils.venv import Venv

  with Venv():
      import pandas

  with Venv('foo'):
      import bar
  ```

  You can also activate the environments for a `Plugin`:

  ```python
  from meerschaum.utils.venv import Venv
  from meerschaum.plugins import Plugin

  with Venv(Plugin('noaa')):
      import requests
  ```

- **Removed `--isolated` from `pip_install`.**  
  Virtual environments will now respect environment variables and your global `pip` configuration (`~/.pip/pip.conf`).
- **Fixed issues for Python 3.7**


### v1.0.3 — v1.0.5

- **Fixed environment bugs.**  
  This patch resolves issues with the environment variables `MRSM_ROOT_DIR`, `MRSM_CONFIG`, and `MRSM_PATCH` as well as the configuration directories `patch_config` and `permanent_patch_config`.
- **Fixed package management system.**  
  Meerschaum better handles package metadata, resolving some annoying issues. See [`meerschaum.utils.packages.get_module_path()`](https://docs.meerschaum.io/utils/packages/index.html#meerschaum.utils.packages.get_module_path) for an example of the improved virtual environment management system. Also, `wheel` is automatically installed when new packages are installed into new virtual environments.
- **Set the default venv to `'mrsm'`.**  
  In all functions declared in [`meerschaum.utils.packages`](https://docs.meerschaum.io/utils/packages/index.html), the default value of `venv` is always `'mrsm'`. Use `None` for the `venv` to use the user's site packages.
- **Updated dependencies.**
- **Added `python-dotenv` as a dependency.**
- **Fixed a catalog issue with `duckdb`.**
- **Updated the testing suite.**
- **More refactoring.**  
  Code needs to be beautiful!

### v1.0.2

- **Allow `id` column to be omitted.**  
  When generating the `UPDATE` query, the `id` column may now be omitted (*NOTE:* the datetime column will be assumed to be the primary key in this scenario).
- **Added `--schedule` (`-s` or `--cron`).**  
  The `--schedule` flag (`-s`) now lets you schedule any command to be executed regulary, not unlike crontab. This can come in handy with `--daemon` (`-d`), e.g.:

  ```bash
  mrsm sync pipes -c plugin:foo -s hourly -d
  ```

  Here is more information on the [scheduling syntax](https://red-engine.readthedocs.io/en/stable/condition_syntax/execution.html).

- **Fixed an issue with SQLite.**  
  An issue where the value columns not loading in SQLite has been addressed.

### v1.0.1

- **Added `citus` as an official database flavor.**  
  Citus is a distributed database built on PostgreSQL. When an `id` column is provided, Meerschaum will call `create_distributed_table()` on the pipe's ID index. Citus has also been added to the official test suite.
- **Changed `end` datetimes to be exclusive.**  
  The `end` parameter now generates `<` instead of `<=`. This shouldn't be a major breaking change but is important to be aware of.
- **Bumped `rich` to v12.4.4.**

### v1.0.0: **Mutable at Last**

**What's New**

  - **Inserts and Updates**  
    An additional layer of processing separates new rows from updated rows. Meerschaum uses your `datetime` and `id` columns (if you specified an `id` column) to determine which rows have changed. Therefore a primary key is not required, as long as the `datetime` column is unique or the `datetime` and `id` columns together emulate a composite primary key.

    Meerschaum will insert new rows as before as well as creating a temporary table (same name as the pipe's target but with a leading underscore). The syncing engine then issues the appropriate `MERGE` or `UPDATE` query to update all of the rows in a batch.

    For example, the following lines of code will result in a table with only 1 row:

    ```python
    >>> import meerschaum as mrsm
    >>> pipe = mrsm.Pipe('foo', 'bar', columns={'datetime': 'dt', 'id': 'id'})
    >>>
    >>> ### Insert the first row.
    >>> pipe.sync([{'dt': '2022-06-26', 'id': 1, 'value': 10}])
    >>>
    >>> ### Duplicate row, no change.
    >>> pipe.sync([{'dt': '2022-06-26', 'id': 1, 'value': 10}])
    >>>
    >>> ### Update the value columns of the first row.
    >>> pipe.sync([{'dt': '2022-06-26', 'id': 1, 'value': 100}])
    ```

  - **Data Type Enforcement.**  
    Incoming DataFrames will be cast to the pipe's existing data types, and if you want total control, you can manually specify the Pandas data types for as many columns as you like under the `dtypes` key of `Pipe.parameters`, e.g.:
    ```yaml
    columns:
      datetime: timestamp_utc
      id: station_id
    dtypes:
      timestamp_utc: 'datetime64[ns]'
      station_id: 'Int64'
      station_name: 'object'
    ```
  - **Allow for `NULL` in `INT` columns.**  
    Before pandas v1.3.0, including a null value in an int column would cast it to a float. Now `pd.NA` has been added and is leveraged in Meerschaum's data type inference system.
  - **Plugins respect `.gitignore`**  
    When publishing a plugin that is contained in a git repository, Meerschaum will parse your `.gitignore` file to determine which files to omit.
  - **Private API Mode.**  
    Require authentication on all API endpoints with `start api --private`.
  - **No Authentication API Mode.**  
    Adding `--no-auth` will disable all authentication on all API endpoints, including the web console.

**Bugfixes**

  - **Plugin packaging**  
    A breaking bug in the process of packaging and publishing Meerschaum plugins has been patched.

  - **Correct object names in Oracle SQL.**  
    Oracle has finally been brought up to speed with other flavors.
  - **Multi-module plugins fix.**  
    A small but important fix for multi-module plugins has been applied for their virtual environments.
  - **Improved virtual environment handling for Debian systems.**  
    If `venv` is not available, Meerschaum now better handles falling back to `virtualenv`.
  - **Allow for syncing lists of dicts.**  
    In addition to syncing a dict of lists, `Pipe.sync()` now supports a list of dicts.
  - **Allow for `begin` to equal `None` for `Pipe.fetch()`.**  
    The behavior of determining `begin` from `Pipe.get_sync_time()` only takes place when `begin` is omitted, not when it is `None`. Now `begin=None` will not add a lower bound to the query.

## 0.6.x Releases

The 0.6.x series brought a lot of polish to the package, namely through refactoring and changing some legacy features to a meet expected behaviors.

### v0.6.3 – v0.6.4: **Durable Venvs**

  - **Improved durability of the virtual environments.**  
    The function [`meerschaum.utils.packages.manually_import_module()`](https://docs.meerschaum.io/utils/packages/index.html#meerschaum.utils.packages.manually_import_module) behaves as expected, allowing you to import different versions of modules. More work needs to be done to see if reintroducing import hooks would be beneficial.
  - **Activate plugin virtual environments for API plugins.**  
    If a plugin uses the [`@api_plugin` decorator](https://docs.meerschaum.io/plugins/index.html#meerschaum.plugins.api_plugin), its virtual environment will be activated before starting the API server. This could potentially cause problems if you have many API plugins with conflicting dependencies, but this could be mitigated by isolating environments with `MRSM_ROOT_DIR`.
  - **Changed `name` to `import_name` for `determine_version()`.**  
    The first argument in [`meerschaum.utils.packages.determine_version()`](https://docs.meerschaum.io/utils/packages/index.html#meerschaum.utils.packages.determine_version) has been renamed from `name` to the less-ambiguous `import_name`.
  - **Shortened the IDs for API environments.**  
    Rather than a long UUID, each instance of the API server will have a randomly generated ID of six letters. Keep in mind it is randomly generated, so please excuse any randomly generated words.
  - **Removed version enforcement for uvicorn and gunicorn.**  
    Uvicorn has a lot of hidden imports, and using our home-brewed import system breaks things. Instead, we now use the default [`attempt_import`](https://docs.meerschaum.io/utils/packages/index.html#meerschaum.utils.packages.attempt_import) behavior of `check_update=False`.
  - **Reintroduced `verify packages` to `setup.py`.**  
    Upgrading Meerschaum will check if the virtual environment packages satisfy their required versions.
  - **Moved `pkg_resources` patch from the module-level.**  
    In v0.6.3, the monkey-patching for `flask-compress` happened at the module level, but this was quickly moved to a lazy patch in v0.6.4.
  - **Bugfixes**  
    Versions 0.6.3 and 0.6.4 were yanked due to some unforeseen broken features.
  - **Bumped several dependencies.**

### v0.6.0 – v0.6.2: **Robust Plugins and Beautiful Pipes**

  **Potentially Breaking Changes**

  - **Renamed `meerschaum.connectors.sql.tools` to [`meerschaum.utils.sql`](https://docs.meerschaum.io/utils/sql.html).**  
    A dummy module was created at the old import path, but this will be removed in future releases.
  - **Migrated to `meerschaum.core`.**  
    Important class definitions (e.g. `User`) have been migrated from `meerschaum._internal` to `meerschaum.core`. You can still import `meerschaum.core.Pipe` as `mrsm.Pipe`, however.
  - **Moved `meerschaum.actions.shell` to `meerschaum._internal.shell`.**  
    Finally marked it off the to-do list!
  - **`Pipe.__str__()` and `Pipe.__repr__()` now return stylized strings.**  
    This should make reading logs significantly more pleasant. You can add syntax highlighting back to strings containing `Pipe()` with `meerschaum.utils.formatting.highlight_pipes()`.


  **New Features**

  - **Plugins**  
    Exposed the [`meerschaum.Plugin`](https://docs.meerschaum.io/#meerschaum.Plugin) class, which will make cross-pollinating between plugins simpler.
  - **Uninstall procedure**  
    `Plugins` now ship with a proper `uninstall()` method.
  - **Sharing dependencies**  
    Plugins may now import dependencies from a required plugin's virtual environment. E.g. if plugin `foo` requires `plugin:bar`, and `bar` requires `pandas`, then `foo` will be able to import `pandas`.
  - **Allow other repos for required plugins.**  
    You can now specify the keys of a required plugin's repo following `@`, e.g. `foo` may require `plugin:bar@api:main`.
  - **Isolate package cache.**  
    Each virtual environment now uses an isolated cache folder.
  - **Handle multiple versions of packages in `determine_version()`**  
    When verifying packages, if multiple `dist-info` directories are found in a virtual environment, import the package in a subprocess to determine its `__version__`.
  - **Specify a target table.**  
    `Pipe.target` (`Pipe.parameters['target']`) now governs the name of the underlying SQL table.


  **Bugfixes**

  - **Circular dependency resolver**  
    Multiple plugins may now depend on each other without entering a recursive loop.
  - **Held back `dash_extensions` due to breaking API changes.**  
    Future releases will migrate to `dash_extensions>1.0.0`.
  - **Fixed [`meerschaum.plugins.add_plugin_argument()`](https://docs.meerschaum.io/plugins/index.html#meerschaum.plugins.add_plugin_argument).**  
    Refactoring broke something awhile back; more plugins-focused tests are needed.
  - **Fixed an issue with `fontawesome` and `mkdocs-material`.**
  - **Fixed pickling issue with `mrsm.Pipe`.**


  **Documentation**

  - **`pdoc` changes.**  
    Added `__pdoc__` and `__all__` to public modules to simplify the [package docs](https://docs.meerschaum.io).
  - **Lots of cleanup.**  
    Almost all of the docstrings have been edited.

## 0.5.x Releases

The 0.5.x series tied up many loose ends and brought in new features, such as fulling integrating Oracle SQL, rewriting most of the doctrings, and adding tags and key negation. It also added the `clear pipes` command and introduced the GUI and webterm.

### v0.5.14 – v0.5.15  
  - **Added tags.**  
    Pipes may be grouped together pipes. Check the docs for more information.
  - **Tags may be negated.**  
    Like the key negation added in v0.5.13, you can choose to ignore tags by prefacing them with `_`.
  - **Bugfixes for DuckDB**
  - **Updated documentation.**
  - **Fixed issue with `flask-compress`.**  
  When starting the API for the first time, missing `flask-compress` will not crash the server.

### v0.5.13
- **Key negation when selecting pipes.**  
  Prefix connector, metric, or location with `_` to select pipes that do NOT have that key.
- **Added the `setup plugins` command.**  
  Run the command `setup plugins` followed by a list of plugins to execute their `setup()` functions.
- **Renamed pipes' keys methods function.**  
  The function `meerschaum.utils.get_pipes.methods()` is renamed to `meerschaum.utils.get_pipes.fetch_pipes_keys()`.
- **Improved stability for DuckDB.**
- **Bumped dependencies.**  
  DuckDB, FastAPI, and Uvicorn have been updated to their latest stable versions.


### v0.5.11 — v0.5.12
- **Improved Oracle support.**  
  Oracle SQL has been fully integrated into the testing suite, and critical bugs have been addressed.
- **Added the `install required` command.**  
  When developing plugins, run the command `install required` to install the packages in the plugin's `required` list into its virtual environment.
- **Migrated docstrings.**  
   To improve legibility, many docstrings have been rewritten from reST- to numpy-style. This will make browsing [docs.meerschaum.io](https://docs.meerschaum.io) easier.

### v0.5.10
- **Added the `clear pipes` command.**  
  Users may now delete specific rows within a pipe using `pipe.clear()`. This new method includes support for the `--begin`, `--end`, and `--params` flags.
- **Changed the default behavior of `--begin`.**  
  The `--begin` flag is now only included when the user specifies and no longer defaults to the sync time.
- **Added `--timeout-seconds`.**  
  The flags `--timeout-seconds` and `--timeout` make the syncing engine sync each pipe in a separate subprocess and will kill the process if the sync exceeds the number of provided seconds.
- **Fixed shell argparse bug.**  
  When editing command line arguments within the shell, edge cases no longer cause the shell to exit.

### v0.5.6 — v0.5.9
- **Added support for `gunicorn`.**  
  Gunicorn may be used to manage API processes with the `--production` or `--gunicorn` flags. The `--production` flag is now default in the Docker image of the API server.
- **Updated `bootstrap pipes` flow.**  
  The interactive bootstrapping wizard now makes use of the new `register()` plugins API as well as asking for the `value` column.
- **Fixed edge cases in `Pipe.filter_existing()`.**  
  Better enforcement of `NaT` as well as `--begin` and `--end` now reduces edge-case bugs and unexpected behavior.
- **Re-introduced the `full` Docker image.**  
  Inclusion of the `start gui` command led to the full version of the Docker image requiring GTK and dependencies. Now you can forward the GUI with `docker run -e DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix bmeares/meerschaum:full start gui`
- **Added `ACKNOWLEDGEMENTS.md` to the root directory.**  
  Only dynamic dependencies with BSD, LGPL, MIT, and Apache licenses remain.
- **Fixed plugin installation bug (again).**
- **Allow for plugins with hyphens in the name.**
- **Lots of refactoring and tiny bugfixes.**

### v0.5.3 – v0.5.5
- **Refactored the `start gui` and `start webterm` commands.**  
  The `start gui` command opens a window which displays the webterm. This terminal will be integrated into the dashboard later.
- **Began work on the desktop build.**  
  Work on building with PyOxidizer began on these releases.

### v0.5.1 – v0.5.2
- **Added the experimental commands `start gui` and `start webterm`.**  
  The desktop GUI will be rewritten in the future, but for now it starts a webview for the web console. The webterm is an instance of `xterm` (not `xterm.js`, having issues) and will eventually replace the current web console "terminal" output. The desktop GUI will also be replaced and will include the webterm once I can get it working on Windows.
- **Isolated API processes.**  
  Meerschaum API workers refer to the environment variable `MRSM_SERVER_ID` to determine the location of the configuration file. This results in the ability to run multiple instances of the API concurrently (at last)!
- **Fixed plugin installation bug.**  
  When installing plugins, the expected behavior of checking if it's already installed occurs.
- **Replaced `semver.match()` with `semver.VersionInfo.match()`.**  
  This change resolves depreciation warnings when building the package.
- **Added the `/info` endpoint to the API.**  
  This allows users to scrape tidbits of information about instances. The current dictionary returns the version and numbers of pipes, plugins, and users. More information will be added to this endpoint in future releases.

### v0.5.0
- **New syncing engine.**  
  The `sync pipes` command reduces concurrency issues while nearly halving syncing times for large batches of pipes.
- **Syncing progress bar.**  
  The `sync pipes` command displays a progress bar (only in the shell) to track the number of completed pipes.
- **Bumped default TimescaleDB image to PostgreSQL 14.**  
  You can continue using PostgreSQL 13 if you already have an existing database.
- **Changed API endpoints.**  
  An endpoint for deleting pipes was added, and the editing and registration endpoints were changed to match the connector, metric, location path scheme.
- **Redesigned test suite.**  
  The `pytest` environment now checks syncing, registration, deletion, etc. for pipes and users with many database flavors.
- **Cleanup and small bugfixes.**  
  As a result of the updated testing suite, issues with several database flavors as well as the API have been resolved.

## 0.4.x Releases
The 0.4.x series dramatically updated Meerschaum, such as ensuring compatibility with Python 3.10, migrating to Bootstrap 5, and implementing useful features like the redesigned web console and the shell toolbar.

### v0.4.16 — v0.4.18
- **Rewritten API \`register()\` methods.**
- **MySQL / MariaDB and CockroachDB fixes.**
- **Additional tests.**

### v0.4.11 — v0.4.15
- **Change the number of columns when printing items.**  
  Depending on the lengths of items and size of the terminal, the number of columns is reduced until most items are not truncated.
- **Allow shell jobs with the `-f` flag.**  
  In addition to `--allow-shell-job`, the `--force` flag permits non-Meerschaum commands to be run. If these flags are absent, a more informative error message is printed.
- **Redesigned the bottom toolbar.**  
  The bottom toolbar now uses a black background with white text. Although this technically still prints ANSI when the global ANSI configuration is false, it still does toggle color.
- **More bugfixes.**  
  A warning when installing plugins has been addressed, and other virtual environment and portable bugs have been fixed.

### v0.4.8 — v0.4.10
- **Added the bottom toolbar to the interactive shell.**  
   The includes the current instance, repo, and connection status.
- **Fixed parsing issue with the Docker build.**  
  There is a strange edge case where multiple levels of JSON-encoding needed to be escaped, and this scenario has been accounted for.
- **Enforce `MRSM_CONFIG` and `MRSM_PATCH` in the Web Console actions.**  
  The Docker version of the API uses environment variables to manage instances, so this information is passed along to children threads.
- **Delayed imports when changing instances.**  
   This postpones trying to connect to an instance until as late as possible.

### v0.4.1 — v0.4.7
- **Added features to the Web Console.**  
  Features such as the `Show Pipes` button and others were added to give the Web Console better functionality.
- **Migrated the Web Console to Bootstrap 5.**  
  Many components needed to be modified or rewritten, but ultimately the move to Bootstrap 5 is worth it in the long run.
- **Updated to work on Python 3.10.**  
  This included creating a standalone internal module for `cascadict` since the original project is no longer maintained.
- **Tighter security.**  
  Better enforcement of datetimes in `dateadd_str()` and denying users access to actions if the permissions setting does not allow non-admins to perform actions.
- **Bugfixes for broken dependencies.**  
  In addition to migrating to Bootstrap 5, components like `PyYAML` and `fastapi-login` changed their function signatures which broke things.

### v0.4.0
- **Allow for other plugins to be specified as dependencies.**  
  Other plugins from the same repository may be specified in the `required` list.
- **Added warnings for broken plugins.**  
  When plugins fail to be imported, warnings are thrown to help authors identify the problem.
- **Added registration to the Web Console.**  
  New users may create accounts by clicking the *No account?* link on the login page.
- **Added the `verify` action.**  
  For now, `verify packages` ensures that the installed dependencies meet the stated requirements for the installed version of Meerschaum.
- **Fixed "ghost" background jobs.**  
  Ensure that jobs are *actually* running before marking them as so.

## 0.3.x Releases
Version 0.3.0 introduced the web interface and added more robust SQL support for various flavors, including MSSQL and DuckDB.

### v0.3.12 — v0.3.19
- **Mostly small bugfixes.**  
  Docker-compose fixes, `params` in `get_pipe_rowcount()`, unique index names for pipes.
- **Added `newest` flag to `pipe.get_sync_time()`.**  
  Setting `newest=False` will return the oldest time instead of the newest.
- **Migrated `filter_existing` to a member of `Pipe`.**  
  Although the current implementation for APIConnectors offloads filtering to the SQLConnector, soon filtering will take place locally to save bandwidth.
- **Updated Docker base image.**  
  Bumped base image from Python 3.7 on Debian Buster Slim to Python 3.9 on Debian Bullseye Slim. Also removed ARM images for the sake of passing builds and reducing build times (e.g. DuckDB fails to compile with QEMU).
- **Improved DuckDB support.**  
  `sql:memory` is now the default in-memory DuckDB instance.

### v0.3.1 – v0.3.11
- **Improved Microsoft SQL Server support.**
- **Added plugins page to the dashboard.**  
  Although somewhat hidden away, the path `/dash/plugins` will show the plugins hosted on the API repository. If the user is logged in, the descriptions of plugins belonging to that user become editable.
- **Added locks to resolve race conditions with threading.**
- **Added `--params` when searching for data and backtracked data.**
- **Fixed the `--params` flag for API pipes.**
- **Added experimental multiplexed fetching feature**  
  To enable this feature, run `mrsm edit config system` and under the `experimental` section, set `fetch` to `true`.
- **Bugfixes and stability improvements**


### v0.3.0
- **Introduced the Web Interface.**  
  Added the Meerschaum Web Interface, an interactive dashboard for managing Meerschaum instances. Although not a total replacement for the Meerschaum Shell, the Web Interface allows multiple users to share connectors without needing to remote into the same machine.

- **Background jobs**  
  Actions may be run in the background with the `-d` or `--daemon` flags or with the action `start job`. To assign a name to a job, pass the flag `--name`.

- **Added `duckdb` as a database flavor**  
  The `duckdb` database flavor is a single file, similar to `sqlite`. Future releases may use `duckdb` as the cache store for local pipes' data.

- **Added `uninstall plugins` and `uninstall packages`.**  
  Plugins and virtual environment `pip` packages may now be removed via the `uninstall` command.

- **Delete plugin from repository**  
  The command `delete plugins` now deletes the archive file and database registration of the plugin on the remote repository. This does not uninstall plugins, so deleted plugins may be re-registered if they are still installed on the client.

- **Bound syncing with `--begin` and `--end`**  
  When performing a sync, you can specify `--begin` and `--end` to bound the search for retrieving data.

- **Bugfixes and improvements**  
  Small bugfixes like including the location `None` with other locations and improvements like only searching for plugin auto-complete suggestions when the search term is at least 1 character long.

## 0.2.x Releases
Version 0.2 improved greatly on 0.1, with a greater focus on the user experience, plugins, local performance, and a whole lot more. Read the release notes below for some of the highlights.

### v0.2.22
- **Critical bugfixes.**
  Version 0.2.22 fixes some critical bugs that went unnoticed in v0.2.21 and is another backport from the 0.3.x branch.

### v0.2.21
- **Bugfixes and performance improvements.**
  Improvements that were added to v0.3.0 release candidates were backported to the 0.2.x series prior to the release of v0.3.0. This release is essentially v0.3.0 with the Web Interface disabled.

### v0.2.20
- **Reformatted `show columns` to tables.**  
  The action `show columns` now displays tables rather than dictionaries.
- **SQLConnector bugfixes.**  
  The `debug` flag was breaking functionality of `SQLConnector` objects, but now connectors are more robust and thread safe.
- **Added `instance` as an alias to `mrsm_instance` when creating `Pipe` objects.**  
  For convenience, when building `Pipes`, `instance` may be used in place of `mrsm_instance`.

### v0.2.19
- **Added `show columns` action.**  
  The action `show columns` will now display a pipe's columns and data types.
- **`docker-compose` bugfix.**  
  When `docker-compose` is installed globally, skip using the virtual environment version.
- **Refactoring / linting**  
  A lot of code was cleaned up to conform with cleaner programming practices.

### v0.2.18
- **Added `login` action.**  
  To verify or correct login credentials for API instance, run the `login` action. The action will try to log in with your defined usernames and passwords, and if a connector is missing a username or password is incorrect, it will ask if you would like to try different login credentials, and upon success, it will ask if you would like to save the new credentials to the primary configuration file.

- **Critical bugfix.**  
  Fixed bug where `default` values were being copied over from the active shell `instance`. I finally found, deep in the code, the missing `.copy()`.

- **Reset `api:mrsm` to default repository.**  
  In my task to move everything to the preconfigured instance, I overstepped and made the default repository into the configured `instance`, which by default is a SQLConnector, so that broke things! In case you were affected by this change, you can simply reset the value of `default_repository` to `api:mrsm` (or your `api` server) to return to the desired behavior.

- **🧹 Housekeeping (refactoring)**.  
  I removed nearly all instances of declaring mutable types as optional values, as well as additional `typing` hints. There may still be some additional cleaning to do, but now the functions are neat and tidy!

### v0.2.17
- **Added CockroachDB as a supported database flavor.**  
  CockroachDB may be a data source or a Meerschaum backend. There may be some performance tuning to do, but for now, it is functional. For example, I may implement bulk insert for CockroachDB like what is done for PostgreSQL and TimescaleDB.
- **Only attempt to install readline once in Meerschaum portable.**  
  The first Meerschaum portable launch will attempt to install readline, but even in case of failure, it won't try to reinstall during subsequent launches or reloads.
- **Refactored SQLAlchemy configuration.**  
  Under `system:connectors:sql`, the key `create_engine` has been added to house all the `sqlalchemy` configuration settings. **WARNING:** You might need to run `delete config system` to refresh this portion of the config file in case any old settings break things.
- **Dependency conflict resolution.**
- **As always, more bugfixes :)**

### v0.2.16
- **Hypertable improvements and bugfixes.**  
  When syncing a new pipe, if an `id` column is specified, create partitions for the number of unique `id` values.
- **Only use `api:mrsm` for plugins, resort to default `instance` for everything else.**
- **Fix bug that mirrored changes to `main` under `default`.**

### v0.2.15
- **MySQL/MariaDB bugfixes.**
- **Added `aiomysql` as a driver dependency.**

### v0.2.14
- **Implemented `bootstrap pipes` action.**  
  The `bootstrap pipes` wizard helps guide new users through creating connectors and pipes.
- **Added `edit pipes definition` action.**  
  Adding the word `definition` to the `edit pipes` command will now open a `.sql` file for pipes with `sql` connectors.
- **Changed `api_instance` to symlink to `instance` by default.**
- **Registering users applies to instances, not repositories.**  
  The action `register users` now uses the value of `instance` instead of `default_repository`. For users to make accounts with `api.mrsm.io`, they will have to specify `-i api:mrsm`.

### v0.2.13
- **Fixed symlink handling for nesting dictionaries.**  
  For example, the environment variables for the API service now contain clean references to the `meerschaum` and `system` keys.
- **Added `MRSM_PATCH` environment variable.**  
  The `MRSM_PATCH` environment variable is treated the same as `MRSM_CONFIG` and is loaded after `MRSM_CONFIG` but before patch or permanent patch files. This allows the user to apply a patch on top of a symlinked reference. In the docker-compose configuration, `MRSM_PATCH` is used to change the `sql:main` hostname to `db`, and the entire `meerschaum` config file is loaded from `MRSM_CONFIG`.
- **Bugfixes, improved robustness.**  
  Per usual, many teeny bugs were squashed.

### v0.2.12
- **Improved symlink handling in the configuration dictionary.**  
  Symlinks are now stable and persistent but at this time cannot be chained together.
- **Improved config file syncing.**  
  Generated config files (e.g. Grafana data sources) may only be edited from the main `edit config` process.
- **Upgraded to PostgreSQL 13 TimescaleDB by default.**  
  This may break existing installs, but you can revert back to 12 with `edit config stack` and changing the string `latest-pg13-oss` under the `db` image to `latest-pg12-oss`.
- **Bugfixes.**  
  Like always, this release includes miscellaneous bugfixes.

### v0.2.11 (release notes before this point are back-logged)
- **API Chaining**  
  Set a Meerschaum API as a the parent source connector for a child Meerschaum API, as if it were a SQLConnector.

### v0.2.10
- **MRSM_CONFIG critical bugfix**  
  The environment variable MRSM_CONFIG is patched on top of your existing configuration. MRSM_PATH is also a patch that is added after MRSM_CONFIG.
### v0.2.9
- **API and SQL Chunking**  
  Syncing data via an APIConnector or SQLConnector uploads the dictionary or DataFrame in chunks (defaults to a chunksize of 900). When calling `read()` with a SQLConnector, a `chunk_hook` callable may be passed, and if `as_chunks` is `True`, a list of DataFrames will be returned. If `as_iterator` is `True`, a dataframe iterator will be returned.

### v0.2.8
- **API Chaining introduction**  
  Chaining is first released on v0.2.8, though it is finalized in 0.2.11.

### v0.2.7
- **Shell autocomplete bugfixes**

### v0.2.6
- **Miscellaneous bugfixes and dependency updates**

### v0.2.1 — v0.2.5
- **Shell improvements**  
  Stability, autosuggest, and more.
- **Virtual environments**  
  Isolate dependencies via virtual environments. The primary entrypoint for virtual environments is `meerschaum.utils.packages.attempt_import()`.

### v0.2.0
- **Plugins**  
  Introduced the plugin system, which allows users and developers to easily integrate any data source into Meerschaum. You can read more about plugins [here](/plugins).
- **Repositories**  
  Repositories are Meerschaum APIs that register and serve plugins. To register a plugin, you need a user login for that API instance.
- **Users**  
  A user account is required for most functions of the Meerschaum API (for security reasons). By default, user registration is disabled from the API side (but can be enabled with `edit config system` under `permissions`). You can register users on a direct SQL connection to a Meerschaum instance.
- **Updated shell design**  
  Added a new prompt, intro, and more shell design improvements.
- **SQLite improvements**  
  The connector `sql:local` may be used as as backend for cases such as when running on a low-powered device like a Raspberry Pi.

## 0.1.x Releases

Meerschaum's first point release focused on a lot, but mainly stability and improving important functionality, such as syncing.

## 0.0.x Releases

A lot was accomplished in the first 60 releases of Meerschaum. For the most part, the groundwork for core concepts like pipes, syncing, the config system, SQL and API connectors, bulk inserts, and more was laid.

