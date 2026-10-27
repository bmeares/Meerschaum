#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interact with Pipes metadata via SQLConnector.
"""
from __future__ import annotations
from datetime import datetime
from typing import TYPE_CHECKING

import meerschaum as mrsm
from meerschaum.utils.typing import (
    Union, Any, SuccessTuple, Tuple, Dict, Optional, List
)

if TYPE_CHECKING:
    import pandas as pd
    import sqlalchemy


def register_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> SuccessTuple:
    """
    Register a new pipe.
    A pipe's attributes must be set before registering.
    """
    return self._rust_method('register_pipe')(pipe)


def edit_pipe(
    self,
    pipe: mrsm.Pipe,
    patch: bool = False,
    debug: bool = False,
    **kw : Any
) -> SuccessTuple:
    """
    Persist a Pipe's parameters to its database.

    Parameters
    ----------
    pipe: mrsm.Pipe, default None
        The pipe to be edited.
    patch: bool, default False
        If patch is `True`, update the existing parameters by cascading.
        Otherwise overwrite the parameters (default).
    debug: bool, default False
        Verbosity toggle.
    """
    return self._rust_method('edit_pipe')(pipe, patch=patch)


def fetch_pipes_keys(
    self,
    connector_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    location_keys: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
) -> Dict[
        int, Tuple[str, str, Union[str, None], Dict[str, Any]]
    ]:
    """
    Return a dictionary mapping pipe IDs to key tuples corresponding to the parameters provided.

    Parameters
    ----------
    connector_keys: Optional[List[str]], default None
        List of connector_keys to search by.

    metric_keys: Optional[List[str]], default None
        List of metric_keys to search by.

    location_keys: Optional[List[str]], default None
        List of location_keys to search by.

    tags: Optional[List[str]], default None
        List of pipes to search by.

    params: Optional[Dict[str, Any]], default None
        Dictionary of additional parameters to search by.
        E.g. `--params pipe_id:1`

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A list of tuples of pipes' keys and parameters (connector_keys, metric_key, location_key, parameters).
    """
    return self._rust_method('fetch_pipes_keys')(
        connector_keys=connector_keys,
        metric_keys=metric_keys,
        location_keys=location_keys,
        tags=tags,
        params=params,
    )


def create_pipe_indices(
    self,
    pipe: mrsm.Pipe,
    columns: Optional[List[str]] = None,
    debug: bool = False,
) -> SuccessTuple:
    """
    Create a pipe's indices.
    """
    return self._rust_method('create_pipe_indices')(pipe, columns=columns)


def create_indices(
    self,
    pipe: mrsm.Pipe,
    columns: Optional[List[str]] = None,
    indices: Optional[List[str]] = None,
    debug: bool = False
) -> bool:
    """
    Create a pipe's indices.
    """
    if pipe.__dict__.get('_skip_check_indices', False):
        return True
    return self._rust_method('create_indices')(pipe, columns=columns, indices=indices)


def drop_pipe_indices(
    self,
    pipe: mrsm.Pipe,
    columns: Optional[List[str]] = None,
    debug: bool = False,
) -> SuccessTuple:
    """
    Drop a pipe's indices.
    """
    return self._rust_method('drop_pipe_indices')(pipe, columns=columns)


def drop_indices(
    self,
    pipe: mrsm.Pipe,
    columns: Optional[List[str]] = None,
    indices: Optional[List[str]] = None,
    debug: bool = False
) -> bool:
    """
    Drop a pipe's indices.
    """
    return self._rust_method('drop_indices')(pipe, columns=columns, indices=indices)


def get_pipe_index_names(self, pipe: mrsm.Pipe) -> Dict[str, str]:
    """
    Return a dictionary mapping index keys to their names on the database.

    Returns
    -------
    A dictionary of index keys to column names.
    """
    return self._rust_method('get_pipe_index_names')(pipe)

def get_create_index_queries(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Dict[str, List[str]]:
    """
    Return a dictionary mapping columns to a `CREATE INDEX` or equivalent query.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to which the queries will correspond.

    Returns
    -------
    A dictionary of index names mapping to lists of queries.
    """
    return self._rust_method('get_create_index_queries')(pipe)


def get_drop_index_queries(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Dict[str, List[str]]:
    """
    Return a dictionary mapping columns to a `DROP INDEX` or equivalent query.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to which the queries will correspond.

    Returns
    -------
    A dictionary of column names mapping to lists of queries.
    """
    return self._rust_method('get_drop_index_queries')(pipe)


def delete_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> SuccessTuple:
    """
    Delete a Pipe's registration.
    """
    return self._rust_method('delete_pipe')(pipe)


def get_pipe_data(
    self,
    pipe: mrsm.Pipe,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[datetime, str, None] = None,
    end: Union[datetime, str, None] = None,
    params: Optional[Dict[str, Any]] = None,
    order: str = 'asc',
    limit: Optional[int] = None,
    begin_add_minutes: int = 0,
    end_add_minutes: int = 0,
    chunksize: Optional[int] = -1,
    as_iterator: bool = False,
    as_polars: bool = False,
    debug: bool = False,
    **kw: Any
) -> Union[pd.DataFrame, None]:
    """
    Access a pipe's data from the SQL instance.

    Parameters
    ----------
    pipe: mrsm.Pipe:
        The pipe to get data from.

    select_columns: Optional[List[str]], default None
        If provided, only select these given columns.
        Otherwise select all available columns (i.e. `SELECT *`).

    omit_columns: Optional[List[str]], default None
        If provided, remove these columns from the selection.

    begin: Union[datetime, str, None], default None
        If provided, get rows newer than or equal to this value.

    end: Union[datetime, str, None], default None
        If provided, get rows older than or equal to this value.

    params: Optional[Dict[str, Any]], default None
        Additional parameters to filter by.
        See `meerschaum.connectors.sql.build_where`.

    order: Optional[str], default 'asc'
        The selection order for all of the indices in the query.
        If `None`, omit the `ORDER BY` clause.

    limit: Optional[int], default None
        If specified, limit the number of rows retrieved to this value.

    begin_add_minutes: int, default 0
        The number of minutes to add to the `begin` datetime (i.e. `DATEADD`).

    end_add_minutes: int, default 0
        The number of minutes to add to the `end` datetime (i.e. `DATEADD`).

    chunksize: Optional[int], default -1
        The size of dataframe chunks to load into memory.

    as_iterator: bool, default False
        If `True`, return the chunks iterator directly.

    as_polars: bool, default False
        If `True`, use an Arrow-native driver when available and return a Polars DataFrame.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A Pandas DataFrame, or a Polars DataFrame when an Arrow-native read is available.

    """
    return self._rust_method('get_pipe_data')(
        pipe,
        select_columns=select_columns,
        omit_columns=omit_columns,
        begin=begin,
        end=end,
        params=params,
        order=order,
        limit=limit,
        begin_add_minutes=begin_add_minutes,
        end_add_minutes=end_add_minutes,
        chunksize=chunksize,
        as_iterator=as_iterator,
        as_polars=as_polars,
        **kw
    )


def get_pipe_docs(
    self,
    pipe: mrsm.Pipe,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[datetime, str, None] = None,
    end: Union[datetime, str, None] = None,
    params: Optional[Dict[str, Any]] = None,
    order: str = 'asc',
    limit: Optional[int] = None,
    debug: bool = False,
    **kw: Any
) -> List[Dict[str, Any]]:
    """
    Return a pipe's data as a list of dictionaries, bypassing pandas overhead.
    """
    return self._rust_method('get_pipe_docs')(
        pipe,
        select_columns=select_columns,
        omit_columns=omit_columns,
        begin=begin,
        end=end,
        params=params,
        order=order,
        limit=limit,
    )


def get_pipe_data_query(
    self,
    pipe: mrsm.Pipe,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[datetime, int, str, None] = None,
    end: Union[datetime, int, str, None] = None,
    params: Optional[Dict[str, Any]] = None,
    order: Optional[str] = 'asc',
    sort_datetimes: bool = False,
    limit: Optional[int] = None,
    begin_add_minutes: int = 0,
    end_add_minutes: int = 0,
    replace_nulls: Optional[str] = None,
    skip_existing_cols_check: bool = False,
    debug: bool = False,
    **kw: Any
) -> Union[str, None]:
    """
    Return the `SELECT` query for retrieving a pipe's data from its instance.

    Parameters
    ----------
    pipe: mrsm.Pipe:
        The pipe to get data from.

    select_columns: Optional[List[str]], default None
        If provided, only select these given columns.
        Otherwise select all available columns (i.e. `SELECT *`).

    omit_columns: Optional[List[str]], default None
        If provided, remove these columns from the selection.

    begin: Union[datetime, int, str, None], default None
        If provided, get rows newer than or equal to this value.

    end: Union[datetime, str, None], default None
        If provided, get rows older than or equal to this value.

    params: Optional[Dict[str, Any]], default None
        Additional parameters to filter by.
        See `meerschaum.connectors.sql.build_where`.

    order: Optional[str], default None
        The selection order for all of the indices in the query.
        If `None`, omit the `ORDER BY` clause.

    sort_datetimes: bool, default False
        Alias for `order='desc'`.

    limit: Optional[int], default None
        If specified, limit the number of rows retrieved to this value.

    begin_add_minutes: int, default 0
        The number of minutes to add to the `begin` datetime (i.e. `DATEADD`).

    end_add_minutes: int, default 0
        The number of minutes to add to the `end` datetime (i.e. `DATEADD`).

    chunksize: Optional[int], default -1
        The size of dataframe chunks to load into memory.

    replace_nulls: Optional[str], default None
        If provided, replace null values with this value.

    skip_existing_cols_check: bool, default False
        If `True`, do not verify that querying columns are actually on the table.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SELECT` query to retrieve a pipe's data.
    """
    return self._rust_method('get_pipe_data_query')(
        pipe,
        select_columns=select_columns,
        omit_columns=omit_columns,
        begin=begin,
        end=end,
        params=params,
        order=order,
        sort_datetimes=sort_datetimes,
        limit=limit,
        begin_add_minutes=begin_add_minutes,
        end_add_minutes=end_add_minutes,
        replace_nulls=replace_nulls,
        skip_existing_cols_check=skip_existing_cols_check,
    )


def get_pipe_id(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Any:
    """
    Get a Pipe's ID from the pipes table.
    """
    return self._rust_method('get_pipe_id')(pipe)


def get_pipe_attributes(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Get a Pipe's attributes dictionary.
    """
    return self._rust_method('get_pipe_attributes')(pipe)


def sync_pipe(
    self,
    pipe: mrsm.Pipe,
    df: Union[pd.DataFrame, str, Dict[Any, Any], None] = None,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    chunksize: Optional[int] = -1,
    check_existing: bool = True,
    blocking: bool = True,
    debug: bool = False,
    _check_temporary_tables: bool = True,
    **kw: Any
) -> SuccessTuple:
    """
    Sync a pipe using a database connection.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The Meerschaum Pipe instance into which to sync the data.

    df: Union[pandas.DataFrame, str, Dict[Any, Any], List[Dict[str, Any]]]
        An optional DataFrame or equivalent to sync into the pipe.
        Defaults to `None`.

    begin: Union[datetime, int, None], default None
        Optionally specify the earliest datetime to search for data.
        Defaults to `None`.

    end: Union[datetime, int, None], default None
        Optionally specify the latest datetime to search for data.
        Defaults to `None`.

    chunksize: Optional[int], default -1
        Specify the number of rows to sync per chunk.
        If `-1`, resort to system configuration (default is `900`).
        A `chunksize` of `None` will sync all rows in one transaction.
        Defaults to `-1`.

    check_existing: bool, default True
        If `True`, pull and diff with existing data from the pipe. Defaults to `True`.

    blocking: bool, default True
        If `True`, wait for sync to finish and return its result, otherwise asyncronously sync.
        Defaults to `True`.

    debug: bool, default False
        Verbosity toggle. Defaults to False.

    kw: Any
        Catch-all for keyword arguments. `replace=True` replaces the pipe's rows of
        `[begin, end)` (matching `params`) with the rows of `df` in one transaction; a bound
        that is not given is the rows' own datetime range (`begin` = the earliest, `end` = just
        after the latest), as for `pipe.sync(df, replace=True)`.

    Returns
    -------
    A `SuccessTuple` of success (`bool`) and message (`str`).
    """
    if df is not None and type(df).__module__.split('.', maxsplit=1)[0] != 'pandas':
        df = pipe.enforce_dtypes(
            df,
            chunksize=chunksize,
            safe_copy=kw.get('safe_copy', False),
            dtypes=pipe.get_dtypes(debug=debug),
            debug=debug,
        )
    replace_kw = (
        {'replace': True, 'params': kw.get('params', None)}
        if kw.get('replace', False)
        else {}
    )
    result = self._rust_method('sync_pipe')(
        pipe,
        df,
        begin=begin,
        end=end,
        chunksize=chunksize,
        check_existing=check_existing,
        **replace_kw
    )
    if hasattr(pipe, '_clear_cache_key'):
        for cache_key in (
            '_exists', '_columns_types', '_columns_types_timestamp',
            '_columns_indices', '_columns_indices_timestamp',
        ):
            pipe._clear_cache_key(cache_key, debug=debug)
    return result


def sync_pipe_inplace(
    self,
    pipe: 'mrsm.Pipe',
    params: Optional[Dict[str, Any]] = None,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    chunksize: Optional[int] = -1,
    check_existing: bool = True,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    If a pipe's connector is the same as its instance connector,
    it's more efficient to sync the pipe in-place rather than reading data into Pandas.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose connector is the same as its instance.

    params: Optional[Dict[str, Any]], default None
        Optional params dictionary to build the `WHERE` clause.
        See `meerschaum.utils.sql.build_where`.

    begin: Union[datetime, int, None], default None
        Optionally specify the earliest datetime to search for data.
        Defaults to `None`.

    end: Union[datetime, int, None], default None
        Optionally specify the latest datetime to search for data.
        Defaults to `None`.

    chunksize: Optional[int], default -1
        Specify the number of rows to sync per chunk.
        If `-1`, resort to system configuration (default is `900`).
        A `chunksize` of `None` will sync all rows in one transaction.
        Defaults to `-1`.

    check_existing: bool, default True
        If `True`, pull and diff with existing data from the pipe.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A SuccessTuple.
    """
    if self.flavor == 'duckdb':
        return pipe.sync(
            params=params,
            begin=begin,
            end=end,
            chunksize=chunksize,
            check_existing=check_existing,
            debug=debug,
            _inplace=False,
            **kw
        )
    from meerschaum import _core
    result = _core.sync_pipe_inplace(
        self._core_connector(),
        pipe,
        params,
        begin,
        end,
        check_existing,
    )
    ### The in-place sync may have added or retyped columns behind the pipe's cache.
    pipe._clear_cache_key('_columns_types', debug=debug)
    pipe._clear_cache_key('_columns_indices', debug=debug)
    return result


def get_sync_time(
    self,
    pipe: 'mrsm.Pipe',
    params: Optional[Dict[str, Any]] = None,
    newest: bool = True,
    remote: bool = False,
    debug: bool = False,
) -> Union[datetime, int, None]:
    """Get a Pipe's most recent datetime value.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to get the sync time for.

    params: Optional[Dict[str, Any]], default None
        Optional params dictionary to build the `WHERE` clause.
        See `meerschaum.utils.sql.build_where`.

    newest: bool, default True
        If `True`, get the most recent datetime (honoring `params`).
        If `False`, get the oldest datetime (ASC instead of DESC).

    remote: bool, default False
        If `True`, return the sync time for the remote fetch definition.

    Returns
    -------
    A `datetime` object (or `int` if using an integer axis) if the pipe exists, otherwise `None`.
    """
    return self._rust_method('get_sync_time')(pipe, params=params, newest=newest, remote=remote)


def pipe_exists(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False
) -> bool:
    """
    Check that a Pipe's table exists.

    Parameters
    ----------
    pipe: mrsm.Pipe:
        The pipe to check.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `bool` corresponding to whether a pipe's table exists.

    """
    return self._rust_method('pipe_exists')(pipe)


def get_pipe_rowcount(
    self,
    pipe: mrsm.Pipe,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    remote: bool = False,
    debug: bool = False
) -> Union[int, None]:
    """
    Get the rowcount for a pipe in accordance with given parameters.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to query with.

    begin: Union[datetime, int, None], default None
        The begin datetime value.

    end: Union[datetime, int, None], default None
        The end datetime value.

    params: Optional[Dict[str, Any]], default None
        See `meerschaum.utils.sql.build_where`.

    remote: bool, default False
        If `True`, get the rowcount for the remote table.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    An `int` for the number of rows if the `pipe` exists, otherwise `None`.

    """
    return self._rust_method('get_pipe_rowcount')(pipe, begin=begin, end=end, params=params, remote=remote)


def drop_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    Drop a pipe's tables but maintain its registration.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to drop.

    Returns
    -------
    A `SuccessTuple` indicated success.
    """
    return self._rust_method('drop_pipe')(pipe)


def clear_pipe(
    self,
    pipe: mrsm.Pipe,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    Delete a pipe's data within a bounded or unbounded interval without dropping the table.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to clear.
        
    begin: Union[datetime, int, None], default None
        Beginning datetime. Inclusive.

    end: Union[datetime, int, None], default None
         Ending datetime. Exclusive.

    params: Optional[Dict[str, Any]], default None
         See `meerschaum.utils.sql.build_where`.

    """
    return self._rust_method('clear_pipe')(pipe, begin=begin, end=end, params=params)


def get_pipe_table(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Union['sqlalchemy.Table', None]:
    """
    Return the `sqlalchemy.Table` object for a `mrsm.Pipe`.

    Parameters
    ----------
    pipe: mrsm.Pipe:
        The pipe in question.

    Returns
    -------
    A `sqlalchemy.Table` object. 

    """
    from meerschaum.utils.sql import get_sqlalchemy_table
    if not pipe.exists(debug=debug):
        return None

    return get_sqlalchemy_table(
        pipe.target,
        connector=self,
        schema=self.get_pipe_schema(pipe),
        debug=debug,
        refresh=True,
    )


def get_pipe_columns_types(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Dict[str, str]:
    """
    Get the pipe's columns and types.

    Parameters
    ----------
    pipe: mrsm.Pipe:
        The pipe to get the columns for.

    Returns
    -------
    A dictionary of columns names (`str`) and types (`str`).

    Examples
    --------
    >>> conn.get_pipe_columns_types(pipe)
    {
      'dt': 'TIMESTAMP WITHOUT TIMEZONE',
      'id': 'BIGINT',
      'val': 'DOUBLE PRECISION',
    }
    >>> 
    """
    return self._rust_method('get_pipe_columns_types')(pipe)


def get_pipe_columns_indices(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Dict[str, List[Dict[str, str]]]:
    """
    Return a dictionary mapping columns to the indices created on those columns.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to be queried against.

    Returns
    -------
    A dictionary mapping columns names to lists of dictionaries.
    The dictionaries in the lists contain the name and type of the indices.
    """
    if pipe.__dict__.get('_skip_check_indices', False):
        return {}
    return self._rust_method('get_pipe_columns_indices')(pipe)


def deduplicate_pipe(
    self,
    pipe: mrsm.Pipe,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Delete duplicate values within a pipe's table.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose table to deduplicate.

    begin: Union[datetime, int, None], default None
        If provided, only deduplicate values greater than or equal to this value.

    end: Union[datetime, int, None], default None
        If provided, only deduplicate values less than this value.

    params: Optional[Dict[str, Any]], default None
        If provided, further limit deduplication to values which match this query dictionary.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    return self._rust_method('deduplicate_pipe')(pipe, begin=begin, end=end, params=params)


def get_pipe_schema(self, pipe: mrsm.Pipe) -> Union[str, None]:
    """
    Return the schema to use for this pipe.
    First check `pipe.parameters['schema']`, then check `self.schema`.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe which may contain a configured schema.

    Returns
    -------
    A schema string or `None` if nothing is configured.
    """
    if self.flavor in ('sqlite', 'geopackage'):
        return self.schema
    return pipe.parameters.get('schema', self.schema)


@staticmethod
def get_temporary_target(
    target: str,
    transact_id: Optional[str] = None,
    label: Optional[str] = None,
    separator: Optional[str] = None,
) -> str:
    """
    Return a unique(ish) temporary target for a pipe.
    """
    from meerschaum.utils.misc import generate_password
    temp_target_cf = (
        mrsm.get_config('system', 'connectors', 'sql', 'instance', 'temporary_target') or {}
    )
    transaction_id_len = temp_target_cf.get('transaction_id_length', 3)
    transact_id = transact_id or generate_password(transaction_id_len)
    temp_prefix = temp_target_cf.get('prefix', '_')
    separator = separator or temp_target_cf.get('separator', '_')
    return (
        temp_prefix
        + target
        + separator
        + transact_id
        + ((separator + label) if label else '')
    )


