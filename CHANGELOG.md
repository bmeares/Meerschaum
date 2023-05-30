# 🪵 Changelog

## 1.6.x Releases

This is the current release cycle, so stay tuned for future releases!

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

