#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
This module contains SQLConnector functions for executing SQL queries.
"""

from __future__ import annotations
from meerschaum.utils.typing import (
    Union, Mapping, List, Dict, SuccessTuple, Optional, Any, Iterable, Callable,
    Tuple, Hashable,
)

from meerschaum.utils.debug import dprint
from meerschaum.utils.warnings import warn

### database flavors that can use bulk insert
_bulk_flavors = {'postgresql', 'timescaledb', 'citus'}
### flavors that do not support chunks
_disallow_chunks_flavors = {'duckdb', 'mssql'}
_max_chunks_flavors = {'sqlite': 1000,}

def read(
        self,
        query_or_table: Union[str, sqlalchemy.Query],
        params: Union[Dict[str, Any], List[str], None] = None,
        dtype: Optional[Dict[str, Any]] = None,
        coerce_float: bool = True,
        chunksize: Optional[int] = -1,
        workers: Optional[int] = None,
        chunk_hook: Optional[Callable[[pandas.DataFrame], Any]] = None,
        as_hook_results: bool = False,
        chunks: Optional[int] = None,
        schema: Optional[str] = None,
        as_chunks: bool = False,
        as_iterator: bool = False,
        as_dask: bool = False,
        index_col: Optional[str] = None,
        silent: bool = False,
        debug: bool = False,
        **kw: Any
    ) -> Union[
        pandas.DataFrame,
        dask.DataFrame,
        List[pandas.DataFrame],
        List[Any],
        None,
    ]:
    """
    Read a SQL query or table into a pandas dataframe.

    Parameters
    ----------
    query_or_table: Union[str, sqlalchemy.Query]
        The SQL query (sqlalchemy Query or string) or name of the table from which to select.

    params: Optional[Dict[str, Any]], default None
        `List` or `Dict` of parameters to pass to `pandas.read_sql()`.
        See the pandas documentation for more information:
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql.html

    dtype: Optional[Dict[str, Any]], default None
        A dictionary of data types to pass to `pandas.read_sql()`.
        See the pandas documentation for more information:
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql_query.html

    chunksize: Optional[int], default -1
        How many chunks to read at a time. `None` will read everything in one large chunk.
        Defaults to system configuration.

        **NOTE:** DuckDB does not allow for chunking.

    workers: Optional[int], default None
        How many threads to use when consuming the generator.
        Only applies if `chunk_hook` is provided.

    chunk_hook: Optional[Callable[[pandas.DataFrame], Any]], default None
        Hook function to execute once per chunk, e.g. writing and reading chunks intermittently.
        See `--sync-chunks` for an example.
        **NOTE:** `as_iterator` MUST be False (default).

    as_hook_results: bool, default False
        If `True`, return a `List` of the outputs of the hook function.
        Only applicable if `chunk_hook` is not None.

        **NOTE:** `as_iterator` MUST be `False` (default).

    chunks: Optional[int], default None
        Limit the number of chunks to read into memory, i.e. how many chunks to retrieve and
        return into a single dataframe.
        For example, to limit the returned dataframe to 100,000 rows,
        you could specify a `chunksize` of `1000` and `chunks` of `100`.

    schema: Optional[str], default None
        If just a table name is provided, optionally specify the table schema.
        Defaults to `SQLConnector.schema`.

    as_chunks: bool, default False
        If `True`, return a list of DataFrames. 
        Otherwise return a single DataFrame.

    as_iterator: bool, default False
        If `True`, return the pandas DataFrame iterator.
        `chunksize` must not be `None` (falls back to 1000 if so),
        and hooks are not called in this case.

    index_col: Optional[str], default None
        If using Dask, use this column as the index column.
        If omitted, a Pandas DataFrame will be fetched and converted to a Dask DataFrame.

    silent: bool, default False
        If `True`, don't raise warnings in case of errors.
        Defaults to `False`.

    Returns
    -------
    A `pd.DataFrame` (default case), or an iterator, or a list of dataframes / iterators,
    or `None` if something breaks.

    """
    if chunks is not None and chunks <= 0:
        return []
    from meerschaum.utils.sql import sql_item_name, truncate_item_name
    from meerschaum.utils.dtypes.sql import NUMERIC_PRECISION_FLAVORS
    from meerschaum.utils.packages import attempt_import, import_pandas
    from meerschaum.utils.pool import get_pool
    from meerschaum.utils.dataframe import chunksize_to_npartitions, get_numeric_cols
    import warnings
    import inspect
    import traceback
    from decimal import Decimal
    pd = import_pandas()
    dd = None
    is_dask = 'dask' in pd.__name__
    pd = attempt_import('pandas')
    is_dask = dd is not None
    npartitions = chunksize_to_npartitions(chunksize)
    if is_dask:
        chunksize = None
    schema = schema or self.schema

    sqlalchemy = attempt_import("sqlalchemy")
    default_chunksize = self._sys_config.get('chunksize', None)
    chunksize = chunksize if chunksize != -1 else default_chunksize
    if chunksize is None and as_iterator:
        if not silent and self.flavor not in _disallow_chunks_flavors:
            warn(
                f"An iterator may only be generated if chunksize is not None.\n"
                + "Falling back to a chunksize of 1000.", stacklevel=3,
            )
        chunksize = 1000
    if chunksize is not None and self.flavor in _max_chunks_flavors:
        if chunksize > _max_chunks_flavors[self.flavor]:
            if chunksize != default_chunksize:
                warn(
                    f"The specified chunksize of {chunksize} exceeds the maximum of "
                    + f"{_max_chunks_flavors[self.flavor]} for flavor '{self.flavor}'.\n"
                    + f"    Falling back to a chunksize of {_max_chunks_flavors[self.flavor]}.",
                    stacklevel = 3,
                )
            chunksize = _max_chunks_flavors[self.flavor]

    ### NOTE: A bug in duckdb_engine does not allow for chunks.
    if chunksize is not None and self.flavor in _disallow_chunks_flavors:
        chunksize = None

    if debug:
        import time
        start = time.perf_counter()
        dprint(query_or_table)
        dprint(f"[{self}] Fetching with chunksize: {chunksize}")

    ### This might be sqlalchemy object or the string of a table name.
    ### We check for spaces and quotes to see if it might be a weird table.
    if (
        ' ' not in str(query_or_table)
        or (
            ' ' in str(query_or_table)
            and str(query_or_table).startswith('"')
            and str(query_or_table).endswith('"')
        )
    ):
        truncated_table_name = truncate_item_name(str(query_or_table), self.flavor)
        if truncated_table_name != str(query_or_table) and not silent:
            warn(
                f"Table '{name}' is too long for '{self.flavor}',"
                + f" will instead create the table '{truncated_name}'."
            )

        query_or_table = sql_item_name(str(query_or_table), self.flavor, schema)
        if debug:
            dprint(f"[{self}] Reading from table {query_or_table}")
        formatted_query = sqlalchemy.text("SELECT * FROM " + str(query_or_table))
        str_query = f"SELECT * FROM {query_or_table}"
    else:
        str_query = query_or_table

    formatted_query = (
        sqlalchemy.text(str_query)
        if not is_dask and isinstance(str_query, str)
        else format_sql_query_for_dask(str_query)
    )

    chunk_list = []
    chunk_hook_results = []
    try:
        stream_results = not as_iterator and chunk_hook is not None and chunksize is not None
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', 'case sensitivity issues')

            read_sql_query_kwargs = {
                'params': params,
                'dtype': dtype,
                'coerce_float': coerce_float,
                'index_col': index_col,
            }
            if is_dask:
                if index_col is None:
                    dd = None
                    pd = attempt_import('pandas')
                    read_sql_query_kwargs.update({
                        'chunksize': chunksize,
                    })
            else:
                read_sql_query_kwargs.update({
                    'chunksize': chunksize,
                })

            if is_dask and dd is not None:
                ddf = dd.read_sql_query(
                    formatted_query,
                    self.URI,
                    **read_sql_query_kwargs
                )
            else:

                with self.engine.begin() as transaction:
                    with transaction.execution_options(stream_results=stream_results) as connection:
                        chunk_generator = pd.read_sql_query(
                            formatted_query,
                            connection,
                            **read_sql_query_kwargs
                        )

                        ### `stream_results` must be False (will load everything into memory).
                        if as_iterator or chunksize is None:
                            return chunk_generator

                        ### We must consume the generator in this context if using server-side cursors.
                        if stream_results:

                            pool = get_pool(workers=workers)

                            def _process_chunk(_chunk, _retry_on_failure: bool = True):
                                if not as_hook_results:
                                    chunk_list.append(_chunk)
                                result = None
                                if chunk_hook is not None:
                                    try:
                                        result = chunk_hook(
                                            _chunk,
                                            workers = workers,
                                            chunksize = chunksize,
                                            debug = debug,
                                            **kw
                                        )
                                    except Exception as e:
                                        result = False, traceback.format_exc()
                                        from meerschaum.utils.formatting import get_console
                                        get_console().print_exception()

                                    ### If the chunk fails to process, try it again one more time.
                                    if isinstance(result, tuple) and result[0] is False:
                                        if _retry_on_failure:
                                            return _process_chunk(_chunk, _retry_on_failure=False)

                                return result

                            chunk_hook_results = list(pool.imap(_process_chunk, chunk_generator))
                            if as_hook_results:
                                return chunk_hook_results

    except Exception as e:
        if debug:
            dprint(f"[{self}] Failed to execute query:\n\n{query_or_table}\n\n")
        if not silent:
            warn(str(e), stacklevel=3)
        from meerschaum.utils.formatting import get_console
        get_console().print_exception()

        return None

    if is_dask and dd is not None:
        ddf = ddf.reset_index()
        return ddf

    chunk_list = []
    read_chunks = 0
    chunk_hook_results = []
    if chunksize is None:
        chunk_list.append(chunk_generator)
    elif as_iterator:
        return chunk_generator
    else:
        try:
            for chunk in chunk_generator:
                if chunk_hook is not None:
                    chunk_hook_results.append(
                        chunk_hook(chunk, chunksize=chunksize, debug=debug, **kw)
                    )
                chunk_list.append(chunk)
                read_chunks += 1
                if chunks is not None and read_chunks >= chunks:
                    break
        except Exception as e:
            warn(f"[{self}] Failed to retrieve query results:\n" + str(e), stacklevel=3)
            from meerschaum.utils.formatting import get_console
            get_console().print_exception()

    read_chunks = 0
    try:
        for chunk in chunk_generator:
            if chunk_hook is not None:
                chunk_hook_results.append(
                    chunk_hook(chunk, chunksize=chunksize, debug=debug, **kw)
                )
            chunk_list.append(chunk)
            read_chunks += 1
            if chunks is not None and read_chunks >= chunks:
                break
    except Exception as e:
        warn(f"[{self}] Failed to retrieve query results:\n" + str(e), stacklevel=3)
        from meerschaum.utils.formatting import get_console
        get_console().print_exception()

        return None

    ### If no chunks returned, read without chunks
    ### to get columns
    if len(chunk_list) == 0:
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', 'case sensitivity issues')
            _ = read_sql_query_kwargs.pop('chunksize', None)
            with self.engine.begin() as connection:
                chunk_list.append(
                    pd.read_sql_query(
                        formatted_query,
                        connection,
                        **read_sql_query_kwargs
                    )
                )

    ### call the hook on any missed chunks.
    if chunk_hook is not None and len(chunk_list) > len(chunk_hook_results):
        for c in chunk_list[len(chunk_hook_results):]:
            chunk_hook_results.append(
                chunk_hook(c, chunksize=chunksize, debug=debug, **kw)
            )

    ### chunksize is not None so must iterate
    if debug:
        end = time.perf_counter()
        dprint(f"Fetched {len(chunk_list)} chunks in {round(end - start, 2)} seconds.")

    if as_hook_results:
        return chunk_hook_results
    
    ### Skip `pd.concat()` if `as_chunks` is specified.
    if as_chunks:
        for c in chunk_list:
            c.reset_index(drop=True, inplace=True)
            for col in get_numeric_cols(c):
                c[col] = c[col].apply(lambda x: x.canonical() if isinstance(x, Decimal) else x)
        return chunk_list

    df = pd.concat(chunk_list).reset_index(drop=True)
    ### NOTE: The calls to `canonical()` are to drop leading and trailing zeroes.
    for col in get_numeric_cols(df):
        df[col] = df[col].apply(lambda x: x.canonical() if isinstance(x, Decimal) else x)

    return df


def value(
        self,
        query: str,
        *args: Any,
        use_pandas: bool = False,
        **kw: Any
    ) -> Any:
    """
    Execute the provided query and return the first value.

    Parameters
    ----------
    query: str
        The SQL query to execute.
        
    *args: Any
        The arguments passed to `meerschaum.connectors.sql.SQLConnector.exec`
        if `use_pandas` is `False` (default) or to `meerschaum.connectors.sql.SQLConnector.read`.
        
    use_pandas: bool, default False
        If `True`, use `meerschaum.connectors.SQLConnector.read`, otherwise use
        `meerschaum.connectors.sql.SQLConnector.exec` (default).
        **NOTE:** This is always `True` for DuckDB.

    **kw: Any
        See `args`.

    Returns
    -------
    Any value returned from the query.

    """
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')
    if self.flavor == 'duckdb':
        use_pandas = True
    if use_pandas:
        try:
            return self.read(query, *args, **kw).iloc[0, 0]
        except Exception as e:
            #  import traceback
            #  traceback.print_exc()
            #  warn(e)
            return None

    _close = kw.get('close', True)
    _commit = kw.get('commit', (self.flavor != 'mssql'))
    try:
        result, connection = self.exec(
            query,
            *args,
            with_connection=True,
            close=False,
            commit=_commit,
            **kw
        )
        first = result.first() if result is not None else None
        _val = first[0] if first is not None else None
    except Exception as e:
        warn(e, stacklevel=3)
        return None
    if _close:
        try:
            connection.close()
        except Exception as e:
            warn("Failed to close connection with exception:\n" + str(e))
    return _val


def execute(
        self,
        *args : Any,
        **kw : Any
    ) -> Optional[sqlalchemy.engine.result.resultProxy]:
    """
    An alias for `meerschaum.connectors.sql.SQLConnector.exec`.
    """
    return self.exec(*args, **kw)


def exec(
        self,
        query: str,
        *args: Any,
        silent: bool = False,
        debug: bool = False,
        commit: Optional[bool] = None,
        close: Optional[bool] = None,
        with_connection: bool = False,
        **kw: Any
    ) -> Union[
            sqlalchemy.engine.result.resultProxy,
            sqlalchemy.engine.cursor.LegacyCursorResult,
            Tuple[sqlalchemy.engine.result.resultProxy, sqlalchemy.engine.base.Connection],
            Tuple[sqlalchemy.engine.cursor.LegacyCursorResult, sqlalchemy.engine.base.Connection],
            None
    ]:
    """
    Execute SQL code and return the `sqlalchemy` result, e.g. when calling stored procedures.
    
    If inserting data, please use bind variables to avoid SQL injection!

    Parameters
    ----------
    query: Union[str, List[str], Tuple[str]]
        The query to execute.
        If `query` is a list or tuple, call `self.exec_queries()` instead.

    args: Any
        Arguments passed to `sqlalchemy.engine.execute`.
        
    silent: bool, default False
        If `True`, suppress warnings.

    commit: Optional[bool], default None
        If `True`, commit the changes after execution.
        Causes issues with flavors like `'mssql'`.
        This does not apply if `query` is a list of strings.

    close: Optional[bool], default None
        If `True`, close the connection after execution.
        Causes issues with flavors like `'mssql'`.
        This does not apply if `query` is a list of strings.

    with_connection: bool, default False
        If `True`, return a tuple including the connection object.
        This does not apply if `query` is a list of strings.
    
    Returns
    -------
    The `sqlalchemy` result object, or a tuple with the connection if `with_connection` is provided.

    """
    if isinstance(query, (list, tuple)):
        return self.exec_queries(
            list(query),
            *args,
            silent = silent,
            debug = debug,
            **kw
        )

    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import("sqlalchemy")
    if debug:
        dprint(f"[{self}] Executing query:\n{query}")

    _close = close if close is not None else (self.flavor != 'mssql')
    _commit = commit if commit is not None else (
        (self.flavor != 'mssql' or 'select' not in str(query).lower())
    )

    ### Select and Insert objects need to be compiled (SQLAlchemy 2.0.0+).
    if not hasattr(query, 'compile'):
        query = sqlalchemy.text(query)

    connection = self.engine.connect()
    transaction = connection.begin() if _commit else None
    try:
        result = connection.execute(query, *args, **kw)
        if _commit:
            transaction.commit()
    except Exception as e:
        if debug:
            dprint(f"[{self}] Failed to execute query:\n\n{query}\n\n{e}")
        if not silent:
            warn(str(e))
        result = None
        if _commit:
            transaction.rollback()
    finally:
        if _close:
            connection.close()

        if with_connection:
            return result, connection

    return result


def exec_queries(
        self,
        queries: List[str],
        break_on_error: bool = False,
        rollback: bool = True,
        silent: bool = False,
        debug: bool = False,
    ) -> List[sqlalchemy.engine.cursor.LegacyCursorResult]:
    """
    Execute a list of queries in a single transaction.

    Parameters
    ----------
    queries: List[str]
        The queries in the transaction to be executed.

    break_on_error: bool, default True
        If `True`, stop executing when a query fails.

    rollback: bool, default True
        If `break_on_error` is `True`, rollback the transaction if a query fails.

    silent: bool, default False
        If `True`, suppress warnings.

    Returns
    -------
    A list of SQLAlchemy results.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    results = []
    with self.engine.begin() as connection:
        for query in queries:
            if debug:
                dprint(f"[{self}]\n" + str(query))
            if isinstance(query, str):
                query = sqlalchemy.text(query)

            try:
                result = connection.execute(query)
            except Exception as e:
                msg = (f"Encountered error while executing:\n{e}")
                if not silent:
                    warn(msg)
                elif debug:
                    dprint(f"[{self}]\n" + str(msg))
                result = None
            results.append(result)
            if result is None and break_on_error:
                if rollback:
                    connection.rollback()
                break
    return results


def to_sql(
        self,
        df: pandas.DataFrame,
        name: str = None,
        index: bool = False,
        if_exists: str = 'replace',
        method: str = "",
        chunksize: Optional[int] = -1,
        schema: Optional[str] = None,
        silent: bool = False,
        debug: bool = False,
        as_tuple: bool = False,
        as_dict: bool = False,
        **kw
    ) -> Union[bool, SuccessTuple]:
    """
    Upload a DataFrame's contents to the SQL server.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame to be uploaded.

    name: str
        The name of the table to be created.

    index: bool, default False
        If True, creates the DataFrame's indices as columns.

    if_exists: str, default 'replace'
        Drop and create the table ('replace') or append if it exists
        ('append') or raise Exception ('fail').
        Options are ['replace', 'append', 'fail'].

    method: str, default ''
        None or multi. Details on pandas.to_sql.

    chunksize: Optional[int], default -1
        How many rows to insert at a time.

    schema: Optional[str], default None
        Optionally override the schema for the table.
        Defaults to `SQLConnector.schema`.

    as_tuple: bool, default False
        If `True`, return a (success_bool, message) tuple instead of a `bool`.
        Defaults to `False`.

    as_dict: bool, default False
        If `True`, return a dictionary of transaction information.
        The keys are `success`, `msg`, `start`, `end`, `duration`, `num_rows`, `chunksize`,
        `method`, and `target`.
        
    kw: Any
        Additional arguments will be passed to the DataFrame's `to_sql` function

    Returns
    -------
    Either a `bool` or a `SuccessTuple` (depends on `as_tuple`).
    """
    import time
    import json
    from decimal import Decimal
    from meerschaum.utils.warnings import error, warn
    import warnings
    import functools
    if name is None:
        error(f"Name must not be `None` to insert data into {self}.")

    ### We're requiring `name` to be positional, and sometimes it's passed in from background jobs.
    kw.pop('name', None)

    schema = schema or self.schema

    from meerschaum.utils.sql import (
        sql_item_name,
        table_exists,
        json_flavors,
        truncate_item_name,
    )
    from meerschaum.utils.dataframe import get_json_cols, get_numeric_cols
    from meerschaum.utils.dtypes import are_dtypes_equal
    from meerschaum.utils.dtypes.sql import NUMERIC_PRECISION_FLAVORS
    from meerschaum.connectors.sql._create_engine import flavor_configs
    from meerschaum.utils.packages import attempt_import, import_pandas
    sqlalchemy = attempt_import('sqlalchemy', debug=debug)
    pd = import_pandas()
    is_dask = 'dask' in df.__module__

    stats = {'target': name, }
    ### resort to defaults if None
    if method == "":
        if self.flavor in _bulk_flavors:
            method = functools.partial(psql_insert_copy, schema=self.schema)
        else:
            ### Should resolve to 'multi' or `None`.
            method = flavor_configs.get(self.flavor, {}).get('to_sql', {}).get('method', 'multi')
    stats['method'] = method.__name__ if hasattr(method, '__name__') else str(method)

    default_chunksize = self._sys_config.get('chunksize', None)
    chunksize = chunksize if chunksize != -1 else default_chunksize
    if chunksize is not None and self.flavor in _max_chunks_flavors:
        if chunksize > _max_chunks_flavors[self.flavor]:
            if chunksize != default_chunksize:
                warn(
                    f"The specified chunksize of {chunksize} exceeds the maximum of "
                    + f"{_max_chunks_flavors[self.flavor]} for flavor '{self.flavor}'.\n"
                    + f"    Falling back to a chunksize of {_max_chunks_flavors[self.flavor]}.",
                    stacklevel = 3,
                )
            chunksize = _max_chunks_flavors[self.flavor]
    stats['chunksize'] = chunksize

    success, msg = False, "Default to_sql message"
    start = time.perf_counter()
    if debug:
        msg = f"[{self}] Inserting {len(df)} rows with chunksize: {chunksize}..."
        print(msg, end="", flush=True)
    stats['num_rows'] = len(df)

    ### Check if the name is too long.
    truncated_name = truncate_item_name(name, self.flavor)
    if name != truncated_name:
        warn(
            f"Table '{name}' is too long for '{self.flavor}',"
            + f" will instead create the table '{truncated_name}'."
        )

    ### filter out non-pandas args
    import inspect
    to_sql_params = inspect.signature(df.to_sql).parameters
    to_sql_kw = {}
    for k, v in kw.items():
        if k in to_sql_params:
            to_sql_kw[k] = v

    to_sql_kw.update({
        'name': truncated_name,
        'schema': schema,
        ('con' if not is_dask else 'uri'): (self.engine if not is_dask else self.URI),
        'index': index,
        'if_exists': if_exists,
        'method': method,
        'chunksize': chunksize,
    })
    if is_dask:
        to_sql_kw.update({
            'parallel': True,
        })

    if self.flavor == 'oracle':
        ### For some reason 'replace' doesn't work properly in pandas,
        ### so try dropping first.
        if if_exists == 'replace' and table_exists(name, self, schema=schema, debug=debug):
            success = self.exec(
                "DROP TABLE " + sql_item_name(name, 'oracle', schema)
            ) is not None
            if not success:
                warn(f"Unable to drop {name}")


        ### Enforce NVARCHAR(2000) as text instead of CLOB.
        dtype = to_sql_kw.get('dtype', {})
        for col, typ in df.dtypes.items():
            if are_dtypes_equal(str(typ), 'object'):
                dtype[col] = sqlalchemy.types.NVARCHAR(2000)
            elif are_dtypes_equal(str(typ), 'int'):
                dtype[col] = sqlalchemy.types.INTEGER
        to_sql_kw['dtype'] = dtype
    elif self.flavor == 'mssql':
        dtype = to_sql_kw.get('dtype', {})
        for col, typ in df.dtypes.items():
            if are_dtypes_equal(str(typ), 'bool'):
                dtype[col] = sqlalchemy.types.INTEGER
        to_sql_kw['dtype'] = dtype

    ### Check for JSON columns.
    if self.flavor not in json_flavors:
        json_cols = get_json_cols(df)
        if json_cols:
            for col in json_cols:
                df[col] = df[col].apply(
                    (
                        lambda x: json.dumps(x, default=str, sort_keys=True)
                        if not isinstance(x, Hashable)
                        else x
                    )
                )

    ### Check for numeric columns.
    numeric_scale, numeric_precision = NUMERIC_PRECISION_FLAVORS.get(self.flavor, (None, None))
    if numeric_precision is not None and numeric_scale is not None:
        numeric_cols = get_numeric_cols(df)
        precision_decimal = Decimal((('1' * numeric_scale) + '.' + ('1' * numeric_precision)))
        for col in numeric_cols:
            df[col] = df[col].apply(
                lambda x: (
                    x.quantize(precision_decimal)
                    if isinstance(x, Decimal)
                    else x
                )
            )

    try:
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', 'case sensitivity issues')
            df.to_sql(**to_sql_kw)
        success = True
    except Exception as e:
        if not silent:
            warn(str(e))
        success, msg = False, str(e)

    end = time.perf_counter()
    if success:
        msg = f"It took {round(end - start, 2)} seconds to sync {len(df)} rows to {name}."
    stats['start'] = start
    stats['end'] = end
    stats['duration'] = end - start

    if debug:
        print(f" done.", flush=True)
        dprint(msg)

    stats['success'] = success
    stats['msg'] = msg
    if as_tuple:
        return success, msg
    if as_dict:
        return stats
    return success


def psql_insert_copy(
        table: pandas.io.sql.SQLTable,
        conn: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection],
        keys: List[str],
        data_iter: Iterable[Any],
        schema: Optional[str] = None,
    ) -> None:
    """
    Execute SQL statement inserting data for PostgreSQL.

    Parameters
    ----------
    table: pandas.io.sql.SQLTable
    
    conn: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection]
    
    keys: List[str]
        Column names
    
    data_iter: Iterable[Any]
        Iterable that iterates the values to be inserted

    Returns
    -------
    None
    """
    import csv
    from io import StringIO
    import json

    from meerschaum.utils.sql import sql_item_name

    ### NOTE: PostgreSQL doesn't support NUL chars in text, so they're removed from strings.
    data_iter = (
        (
            (
                (
                    json.dumps(item).replace('\0', '').replace('\\u0000', '')
                    if isinstance(item, (dict, list))
                    else (
                        item
                        if not isinstance(item, str)
                        else item.replace('\0', '').replace('\\u0000', '')
                    )
                )
            ) if item is not None
            else r'\N'
            for item in row
        ) for row in data_iter
    )

    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(f'"{k}"' for k in keys)
        table_name = sql_item_name(table.name, 'postgresql', table.schema)
        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV NULL '\\N'"
        cur.copy_expert(sql=sql, file=s_buf)


def format_sql_query_for_dask(query: str) -> 'sqlalchemy.sql.selectable.Select':
    """
    Given a `SELECT` query, return a `sqlalchemy` query for Dask to use.
    This may only work with specific database flavors like PostgreSQL.

    Parameters
    ----------
    query: str
        The `SELECT` query to be parsed.

    Returns
    -------
    A `sqlalchemy` selectable.
    """
    if 'sqlalchemy' in str(type(query)):
        return query

    if 'select ' not in query.lower():
        raise ValueError(f"Cannot convert query to SQLAlchemy:\n\n{query}")

    def _remove_leading_select(q: str) -> str:
        return q.replace("SELECT ", "", 1)

    from meerschaum.utils.packages import attempt_import
    sqlalchemy_sql = attempt_import("sqlalchemy.sql")
    select, text = sqlalchemy_sql.select, sqlalchemy_sql.text

    parts = query.rsplit('ORDER BY', maxsplit=1)
    meta_query = f"SELECT * FROM (\n{query}\n) AS s"
    #  if parts[1]:
        #  meta_query += "\nORDER BY " + parts[1]
    return select(text(_remove_leading_select(meta_query)))
