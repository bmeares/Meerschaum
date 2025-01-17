# 🗄️ Instance Connectors

Instance connectors store pipes' registrations and data in addition to the usual `#!python fetch()` functionality of regular connectors, e.g. the [`#!python SQLConnector`](/reference/connectors/sql-connectors/).

To use your custom connector type as an instance connector, implement the following methods, replacing the pseudocode under the `TODO` comments with your connector's equivalent. See the [`MongoDBConnector`](https://github.com/bmeares/mongodb-connector/blob/main/plugins/mongodb-connector/_pipes.py) for  a specific reference.

!!! note ""
    The `#!python SuccessTuple` type annotation is an alias for `Tuple[bool, str]` and may be imported:

    ```python
    from meerschaum.utils.typing import SuccessTuple
    ```

??? tip "Using the `params` Filter"
    Methods which take the `params` argument ([`get_pipe_data()`](#get_pipe_data), [`get_sync_time()`](#get_sync_time), [`get_backtrack_data()`](#get_backtrack_data)) behave similarly to the filters applied to [`fetch_pipes_keys`](#fetch_pipes_keys).

    The easiest way to support `params` is with [meerschaum.utils.dataframe.query_df()](https://docs.meerschaum.io/meerschaum/utils/dataframe.html#query_df):

    ```python
    from meerschaum.utils.dataframe import query_df, parse_df_datetimes
    df = parse_df_datetimes([
        {'ts': '2024-01-01 00:00:00', 'color': 'red'},
        {'ts': '2024-02-02 02:00:00', 'color': 'blue'},
        {'ts': '2024-03-03 03:00:00', 'color': 'green'},
    ])
    print(query_df(df, {'color': 'red'}))
    #           ts color
    # 0 2024-01-01   red 
    print(query_df(df, begin='2024-02-01', datetime_column='ts'))
    #                    ts  color
    # 1 2024-02-02 02:00:00   blue
    # 2 2024-03-03 03:00:00  green
    ```

    For advanced implementations, see the definition for [`MongoDBConnector.build_query()`](https://github.com/bmeares/mongodb-connector/blob/main/plugins/mongodb-connector/_mongo.py#L16) for an example of how to adapt the `params` filter to your connector's query specification.

    ```python
    >>> build_query({'a': 1})
    {'a': {'$eq': 1}}
    >>> 
    >>> build_query({'a': '_b'})
    {'a': {'$ne': 'b'}}
    >>> 
    >>> build_query({'a': ['c', '_d']})
    {'a': {'$eq': 'c', {'$neq': 'd'}}}
    >>> 
    >>> build_query({'a': [1, 2, 3]})
    {'a': {'$nin': [1, 2, 3]}}
    >>> 
    >>> build_query({'a': []})
    {}
    ```

??? warning "`get_backtrack_data()` Deprecation Notice"
    As of v1.7.0+, `get_backtrack_data()` was replaced with a generic alternative. Your connector may still override this method:

    ```python
    def get_backtrack_data(
        self,
        pipe: mrsm.Pipe,
        backtrack_minutes: int = 0,
        begin: datetime | int | None = None,
        params: dict[str, Any] | None = None,
        debug: bool = False,
        **kwargs: Any
    ) -> 'pd.DataFrame':
        """
        Return the most recent interval of data leading up to `begin` (defaults to the sync time).

        Parameters
        ----------
        pipe: mrsm.Pipe,
            The number of minutes leading up to `begin` from which to search.
            If `begin` is an integer, then subtract this value from `begin`.

        backtrack_minutes: int, default 0
            The number of minutes leading up to `begin` from which to search.
            If `begin` is an integer, then subtract this value from `begin`.

        begin: datetime | int | None, default None
            The point from which to begin backtracking.
            If `None`, then use the pipe's sync time (most recent datetime value).

        params: dict[str, Any] | None, default None
            Additional filter parameters.

        Returns
        -------
        A Pandas DataFrame for the interval of size `backtrack_minutes` leading up to `begin`.
        """
        from datetime import datetime, timedelta

        if begin is None:
            begin = pipe.get_sync_time(params=params, debug=debug)

        backtrack_interval = (
            timedelta(minutes=backtrack_minutes)
            if isinstance(begin, datetime)
            else backtrack_minutes
        )

        if begin is not None:
            begin = begin - backtrack_interval

        return self.get_pipe_data(
            pipe,
            begin = begin,
            params = params,
            debug = debug,
            **kwargs
        )
    ```

## `#!python register_pipe()`

Store a pipe's attributes in a `pipes` table.

The attributes row of a pipe includes the pipe's keys (immutable) and parameters dictionary (mutable):

- `connector_keys` (`str`)
- `metric_key` (`str`)
- `location_key` (`str | None`)  
  You may store `"None"` in place of `None`.
- `parameters` (`dict[str, Any]`)  
  You can access the in-memory parameters with `#!python pipe._attributes.get('parameters', {})`.

??? example "`#!python def register_pipe():`"
    ```python
    def register_pipe(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> mrsm.SuccessTuple:
        """
        Insert the pipe's attributes into the internal `pipes` table.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe to be registered.

        Returns
        -------
        A `SuccessTuple` of the result.
        """
        attributes = {
            'connector_keys': str(pipe.connector_keys),
            'metric_key': str(pipe.metric_key),
            'location_key': str(pipe.location_key),
            'parameters': pipe._attributes.get('parameters', {}),
        }

        ### TODO insert `attributes` as a row in the pipes table.
        # self.pipes_collection.insert_one(attributes)

        return True, "Success"
    ```

## `#!python get_pipe_attributes()`

Return the attributes dictionary for a pipe (see [`register_pipe()` above](#register_pipe)).
Note that a pipe's attributes must be JSON-serializable, so objects like MongoDB's `ObjectId` must be converted to strings.

??? example "`#!python def get_pipe_attributes():`"
    ```python
    def get_pipe_attributes(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Return the pipe's document from the internal `pipes` collection.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose attributes should be retrieved.

        Returns
        -------
        The document that matches the keys of the pipe.
        """
        query = {
            'connector_keys': str(pipe.connector_keys),
            'metric_key': str(pipe.metric_key),
            'location_key': str(pipe.location_key),
        }
        ### TODO query the `pipes` table either using these keys or `get_pipe_id()`.
        result = {}
        # result = self.pipes_collection.find_one(query) or {}
        return result
    ```

## `#!python get_pipe_id()`

Return the ID tied to the pipe's connector, metric, and location keys.

??? example "`#!python def get_pipe_id():`"
    ```python
    def get_pipe_id(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> Union[str, int, None]:
        """
        Return the `_id` for the pipe if it exists.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose `_id` to fetch.

        Returns
        -------
        The `_id` for the pipe's document or `None`.
        """
        query = {
            'connector_keys': str(pipe.connector_keys),
            'metric_key': str(pipe.metric_key),
            'location_key': str(pipe.location_key),
        }
        ### TODO fetch the ID mapped to this pipe.
        # oid = (self.pipes_collection.find_one(query, {'_id': 1}) or {}).get('_id', None)
        # return str(oid) if oid is not None else None
    ```

## `#!python edit_pipe()`

Update the `parameters` dictionary of a pipe's registration.

??? example "`#!python def edit_pipe():`"
    ```python
    def edit_pipe(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> mrsm.SuccessTuple:
        """
        Edit the attributes of the pipe.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose in-memory parameters must be persisted.

        Returns
        -------
        A `SuccessTuple` indicating success.
        """
        query = {
            'connector_keys': str(pipe.connector_keys),
            'metric_key': str(pipe.metric_key),
            'location_key': str(pipe.location_key),
        }
        pipe_parameters = pipe._attributes.get('parameters', {})
        ### TODO Update the row with new parameters.
        # self.pipes_collection.update_one(query, {'$set': {'parameters': pipe_parameters}})
        return True, "Success"

    ```

## `#!python delete_pipe()`

Delete a pipe's registration from the `pipes` table.

??? example "`#!python def delete_pipe():`"
    ```python
    def delete_pipe(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> mrsm.SuccessTuple:
        """
        Delete a pipe's registration from the `pipes` collection.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe to be deleted.

        Returns
        -------
        A `SuccessTuple` indicating success.
        """
        pipe_id = self.get_pipe_id(pipe, debug=debug)
        if pipe_id is None:
            return False, f"{pipe} is not registered."

        ### TODO Delete the pipe's row from the pipes table.
        # self.pipes_collection.delete_one({'_id': pipe_id})
        return True, "Success"
    ```

## `#!python fetch_pipes_keys()`

Return a list of tuples for the registered pipes' keys according to the provided filters.

Each filter should only be applied if the given list is not empty.
Values within filters are joined by `OR`, and filters are joined by `AND`.

The function [`separate_negation_values()`](https://docs.meerschaum.io/utils/misc.html#meerschaum.utils.misc.separate_negation_values) returns two sublists: regular values (`IN`) and values preceded by an underscore (`NOT IN`).

??? example "`#!python def fetch_pipes_keys():`"
    ```python
    def fetch_pipes_keys(
        self,
        connector_keys: list[str] | None = None,
        metric_keys: list[str] | None = None,
        location_keys: list[str] | None = None,
        tags: list[str] | None = None,
        debug: bool = False,
        **kwargs: Any
    ) -> List[Tuple[str, str, str]]:
        """
        Return a list of tuples for the registered pipes' keys according to the provided filters.

        Parameters
        ----------
        connector_keys: list[str] | None, default None
            The keys passed via `-c`.

        metric_keys: list[str] | None, default None
            The keys passed via `-m`.

        location_keys: list[str] | None, default None
            The keys passed via `-l`.

        tags: List[str] | None, default None
            Tags passed via `--tags` which are stored under `parameters:tags`.

        Returns
        -------
        A list of connector, metric, and location keys in tuples.
        You may return the string "None" for location keys in place of nulls.

        Examples
        --------
        >>> import meerschaum as mrsm
        >>> conn = mrsm.get_connector('example:demo')
        >>> 
        >>> pipe_a = mrsm.Pipe('a', 'demo', tags=['foo'], instance=conn)
        >>> pipe_b = mrsm.Pipe('b', 'demo', tags=['bar'], instance=conn)
        >>> pipe_a.register()
        >>> pipe_b.register()
        >>> 
        >>> conn.fetch_pipes_keys(['a', 'b'])
        [('a', 'demo', 'None'), ('b', 'demo', 'None')]
        >>> conn.fetch_pipes_keys(metric_keys=['demo'])
        [('a', 'demo', 'None'), ('b', 'demo', 'None')]
        >>> conn.fetch_pipes_keys(tags=['foo'])
        [('a', 'demo', 'None')]
        >>> conn.fetch_pipes_keys(location_keys=[None])
        [('a', 'demo', 'None'), ('b', 'demo', 'None')]
        """
        from meerschaum.utils.misc import separate_negation_values

        in_ck, nin_ck = separate_negation_values([str(val) for val in (connector_keys or [])])
        in_mk, nin_mk = separate_negation_values([str(val) for val in (metric_keys or [])])
        in_lk, nin_lk = separate_negation_values([str(val) for val in (location_keys or [])])
        in_tags, nin_tags = separate_negation_values([str(val) for val in (tags or [])])

        ### TODO build a query like so, only including clauses if the given list is not empty.
        ### The `tags` clause is an OR ("?|"), meaning any of the tags may match.
        ### 
        ### 
        ### SELECT connector_keys, metric_key, location_key
        ### FROM pipes
        ### WHERE connector_keys IN ({in_ck})
        ###   AND connector_keys NOT IN ({nin_ck})
        ###   AND metric_key IN ({in_mk})
        ###   AND metric_key NOT IN ({nin_mk})
        ###   AND location_key IN (in_lk)
        ###   AND location_key NOT IN (nin_lk)
        ###   AND (parameters->'tags')::JSONB ?| ARRAY[{tags}]
        ###   AND NOT (parameters->'tags')::JSONB ?| ARRAY[{nin_tags}]
        return []
    ```

## `#!python pipe_exists()`

Return `True` if the target table exists and has data.

??? example "`#!python def pipe_exists():`"
    ```python
    def pipe_exists(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> bool:
        """
        Check whether a pipe's target table exists.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe to check whether its table exists.

        Returns
        -------
        A `bool` indicating the table exists.
        """
        table_name = pipe.target
        ### TODO write a query to determine the existence of `table_name`.
        table_exists = False
        return table_exists
    ```

## `#!python drop_pipe()`

Drop the pipe's target table.

??? example "`#!python def drop_pipe():`"
    ```python
    def drop_pipe(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> mrsm.SuccessTuple:
        """
        Drop a pipe's collection if it exists.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe to be dropped.

        Returns
        -------
        A `SuccessTuple` indicating success.
        """
        ### TODO write a query to drop `table_name`.
        table_name = pipe.target
        return True, "Success"
    ```

### `#!python drop_pipe_indices()` (optional)

If syncing to your instance connector involves indexing a pipe's target table, you may find it useful to implement the method `drop_pipe_indices()` (for the action `drop indices`). See the [`#!python SQLConnector.drop_pipe_indices()`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector.drop_pipe_indices) method for reference.

## `#!python sync_pipe()`

Upsert new data into the pipe's table.

You may use the built-in method [`pipe.filter_existing()`](https://docs.meerschaum.io/index.html#meerschaum.Pipe.filter_existing) to extract inserts and updates in case the database for this connector does not have upsert functionality.

!!! note ""
    The values of the `pipe.columns` dictionary are immutable indices to be used for upserts. You may improve performance by indexing these columns after an initial sync (i.e. `#!python pipe.exists() is False`).

??? example "`#!python def sync_pipe():`"
    ```python
    def sync_pipe(
        self,
        pipe: mrsm.Pipe,
        df: 'pd.DataFrame' = None,
        debug: bool = False,
        **kwargs: Any
    ) -> mrsm.SuccessTuple:
        """
        Upsert new documents into the pipe's collection.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose collection should receive the new documents.

        df: Union['pd.DataFrame', Iterator['pd.DataFrame']], default None
            The data to be synced.

        Returns
        -------
        A `SuccessTuple` indicating success.
        """
        if df is None:
            return False, f"Received `None`, cannot sync {pipe}."

        ### TODO Write the upsert logic for the target table.
        ### `pipe.filter_existing()` is provided for your convenience to
        ### remove duplicates and separate inserts from updates.
        unseen_df, update_df, delta_df = pipe.filter_existing(df, debug=debug)
        return True, "Success"
    ```

### `#!python sync_pipe_inplace()` (optional)

For situations where the source and instance connectors are the same, the method `#!python sync_pipe_inplace()` allows you to bypass loading DataFrames into RAM and instead handle the syncs remotely. See the [`#!python SQLConnector.sync_pipe_inplace()`](https://docs.meerschaum.io/connectors/sql/SQLConnector.html#meerschaum.connectors.sql.SQLConnector.SQLConnector.sync_pipe_inplace) method for reference.

### `#!python create_pipe_indices()` (optional)

If syncing to your instance connector involves indexing a pipe's target table, you may find it useful to implement the method `create_pipe_indices()`. See the method [`#!python SQLConnector.create_pipe_indices()`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector.create_pipe_indices) for reference.


## `#!python clear_pipe()`

Delete a pipe's data within a bounded or unbounded interval without dropping the table:

??? example "`#!python def clear_pipe():`"
    ```python
    def clear_pipe(
        self,
        pipe: mrsm.Pipe,
        begin: datetime | int | None = None,
        end: datetime | int | None = None,
        params: dict[str, Any] | None = None,
        debug: bool = False,
    ) -> mrsm.SuccessTuple:
        """
        Delete rows within `begin`, `end`, and `params`.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose rows to clear.

        begin: datetime | int | None, default None
            If provided, remove rows >= `begin`.

        end: datetime | int | None, default None
            If provided, remove rows < `end`.
           
        params: dict[str, Any] | None, default None
            If provided, only remove rows which match the `params` filter.

        Returns
        -------
        A `SuccessTuple` indicating success.
        """
        ### TODO Write a query to remove rows which match `begin`, `end`, and `params`.
        return True, "Success"

    ```

### `#!python deduplicate_pipe()` (optional)

Like `sync_pipe_inplace()`, you may choose to implement `deduplicate_pipe()` for a performance boost. Otherwise, the default implementation relies upon `get_pipe_data()`, `clear_pipe()`, and `get_pipe_rowcount()`. See the [`#!python SQLConnector.deduplicate_pipe()`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector.deduplicate_pipe) method for reference.

## `#!python get_pipe_data()`

Return the target table's data according to the filters.

The `begin` and `end` arguments correspond to the designated `datetime` axis (`pipe.columns['datetime']`).

The `params` argument behaves the same as [`fetch_pipes_keys()`](#fetch_pipes_keys) filters but may allow single values as well. See the disclaimer at the top of this page on building queries with `params`.

!!! note ""
    The convenience function [`parse_df_datetimes()`](https://docs.meerschaum.io/utils/misc.html#meerschaum.utils.misc.parse_df_datetimes) casts dataframe-like lists of dictionaries (or dictionaries of lists) into DataFrames, automatically casting ISO strings to datetimes.

??? example "`#!python def get_pipe_data():`"
    ```python
    def get_pipe_data(
        self,
        pipe: mrsm.Pipe,
        select_columns: list[str] | None = None,
        omit_columns: list[str] | None = None,
        begin: datetime | int | None = None,
        end: datetime | int | None = None,
        params: dict[str, Any] | None = None,
        debug: bool = False,
        **kwargs: Any
    ) -> Union['pd.DataFrame', None]:
        """
        Query a pipe's target table and return the DataFrame.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe with the target table from which to read.

        select_columns: list[str] | None, default None
            If provided, only select these given columns.
            Otherwise select all available columns (i.e. `SELECT *`).

        omit_columns: list[str] | None, default None
            If provided, remove these columns from the selection.

        begin: datetime | int | None, default None
            The earliest `datetime` value to search from (inclusive).

        end: datetime | int | None, default None
            The lastest `datetime` value to search from (exclusive).

        params: dict[str | str] | None, default None
            Additional filters to apply to the query.

        Returns
        -------
        The target table's data as a DataFrame.
        """
        if not pipe.exists(debug=debug):
            return None

        table_name = pipe.target
        dt_col = pipe.columns.get("datetime", None)

        ### TODO Write a query to fetch from `table_name`
        ###      and apply the filters `begin`, `end`, and `params`.
        ### 
        ###      To improve performance, add logic to only read from
        ###      `select_columns` and not `omit_columns` (if provided).
        ### 
        ### SELECT {', '.join(cols_to_select)}
        ### FROM "{table_name}"
        ### WHERE "{dt_col}" >= '{begin}'
        ###   AND "{dt_col}" <  '{end}'

        ### The function `parse_df_datetimes()` is a convenience function
        ### to cast a list of dictionaries into a DataFrame and convert datetime columns.
        from meerschaum.utils.dataframe import parse_df_datetimes
        rows = []
        return parse_df_datetimes(rows)
    ```

## `#!python get_sync_time()`

Return the largest (or smallest) value in target table, according to the `params` filter.

??? example "`#!python def get_sync_time():`"
    ```python
    def get_sync_time(
        self,
        pipe: mrsm.Pipe,
        params: dict[str, Any] | None = None,
        newest: bool = True,
        debug: bool = False,
        **kwargs: Any
    ) -> datetime | int | None:
        """
        Return the most recent value for the `datetime` axis.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose collection contains documents.

        params: dict[str, Any] | None, default None
            Filter certain parameters when determining the sync time.

        newest: bool, default True
            If `True`, return the maximum value for the column.

        Returns
        -------
        The largest `datetime` or `int` value of the `datetime` axis. 
        """
		dt_col = pipe.columns.get('dt_col', None)
		if dt_col is None:
			return None

		### TODO write a query to get the largest value for `dt_col`.
        ### If `newest` is `False`, return the smallest value.
        ### Apply the `params` filter in case of multiplexing.
    ```

## `#!python get_pipe_columns_types()`

Return columns and Pandas data types (you may also return PosgreSQL-style types).
You may take advantage of automatic dtype enforcement by implementing this method.

??? example
    ```python
    def get_pipe_columns_types(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> dict[str, str]:
        """
        Return the data types for the columns in the target table for data type enforcement.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose target table contains columns and data types.

        Returns
        -------
        A dictionary mapping columns to data types.
        """
        if not pipe.exists(debug=debug):
            return {}

        table_name = pipe.target
        ### TODO write a query to fetch the columns contained in `table_name`.
        columns_types = {}
        
        ### Return a dictionary mapping the columns
        ### to their Pandas dtypes, e.g.:
        ### `{'foo': 'int64'`}`
        return columns_types
    ```

### `#!python get_pipe_columns_indices()` (optional)

You may choose to implement `get_pipe_columns_indices()`, which returns a dictionary mapping columns to a list of related indices. Additionally, implement the method [`#!python SQLConnector.get_pipe_index_names()`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector.get_pipe_index_names) to return new indices to be created.

??? example

```python
def get_pipe_columns_indices(
    debug: bool = False,
) -> dict[str, list[dict[str, str]]]:
    """
    Return a dictionary mapping columns to metadata about related indices.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table has related indices.

    Returns
    -------
    A list of dictionaries with the keys "type" and "name".

    Examples
    --------
    >>> pipe = mrsm.Pipe('demo', 'shirts', columns={'primary': 'id'}, indices={'size_color': ['color', 'size']})
    >>> pipe.sync([{'color': 'red', 'size': 'M'}])
    >>> pipe.get_columns_indices()
    {'id': [{'name': 'demo_shirts_pkey', 'type': 'PRIMARY KEY'}], 'color': [{'name': 'IX_demo_shirts_color_size', 'type': 'INDEX'}], 'size': [{'name': 'IX_demo_shirts_color_size', 'type': 'INDEX'}]}
    """
```

## `#!python get_pipe_rowcount()`

Return the number of rows in the pipe's target table within the `begin`, `end`, and `params` bounds:

??? example "`#!python def get_pipe_rowcount():`"
    ```python
    def get_pipe_rowcount(
        self,
        pipe: mrsm.Pipe,
        begin: datetime | int | None = None,
        end: datetime | int | None = None,
        params: dict[str, Any] | None = None,
        remote: bool = False,
        debug: bool = False,
        **kwargs: Any
    ) -> int:
        """
        Return the rowcount for the pipe's table.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose table should be counted.

        begin: datetime | int | None, default None
            If provided, only count rows >= `begin`.

        end: datetime | int | None, default None
            If provided, only count rows < `end`.

        params: dict[str, Any] | None
            If provided, only count rows othat match the `params` filter.

        remote: bool, default False
            If `True`, return the rowcount for the pipe's fetch definition.
            In this case, `self` refers to `Pipe.connector`, not `Pipe.instance_connector`.

        Returns
        -------
        The rowcount for this pipe's table according the given parameters.
        """
        ### TODO write a query to count how many rows exist in `table_name` according to the filters.
        table_name = pipe.target
        count = 0
        return count
    ```