#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define function signatures for pipes' methods.
"""

from __future__ import annotations

import abc
from typing import Any, Union, Dict, List, Tuple, Optional
from datetime import datetime

import meerschaum as mrsm

@abc.abstractmethod
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

@abc.abstractmethod
def get_pipe_attributes(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> Dict[str, Any]:
    """
    Return the pipe's document from the internal `pipes` table.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose attributes should be retrieved.

    Returns
    -------
    The document that matches the keys of the pipe.
    """

@abc.abstractmethod
def get_pipe_id(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> Union[str, int, None]:
    """
    Return the `id` for the pipe if it exists.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose `id` to fetch.

    Returns
    -------
    The `id` for the pipe's document or `None`.
    """

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
    raise NotImplementedError

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
    raise NotImplementedError

@abc.abstractmethod
def fetch_pipes_keys(
    self,
    connector_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    location_keys: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    debug: bool = False,
    **kwargs: Any
) -> Union[
    List[Tuple[str, str, str]],
    List[Tuple[str, str, str, Union[Dict[str, Any], List[str]]]],
    Dict[Union[int, str], Tuple[str, str, str]],
    Dict[Union[int, str], Tuple[str, str, str, Union[Dict[str, Any], List[str]]]],
]:
    """
    Return registered pipes' keys according to the provided filters.

    May return either a list of key tuples or a dictionary mapping pipe IDs to key tuples.
    When returning a dictionary, the key is the pipe's unique ID (int or str).
    Tuples may be length 3 `(connector_keys, metric_key, location_key)` or length 4
    with parameters or tags appended as the fourth element.

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
    A list of tuples or a dictionary mapping pipe IDs to tuples.
    You may return the string `"None"` for location keys in place of nulls.

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

@abc.abstractmethod
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

@abc.abstractmethod
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
    raise NotImplementedError

def drop_pipe_indices(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Drop a pipe's indices.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose indices need to be dropped.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    return False, f"Cannot drop indices for instance connectors of type '{self.type}'."

@abc.abstractmethod
def sync_pipe(
    self,
    pipe: mrsm.Pipe,
    df: 'pd.DataFrame' = None,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    chunksize: Optional[int] = -1,
    check_existing: bool = True,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Sync a pipe using a database connection.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The Meerschaum Pipe instance into which to sync the data.

    df: Optional[pd.DataFrame]
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

    debug: bool, default False
        Verbosity toggle. Defaults to False.

    Returns
    -------
    A `SuccessTuple` of success (`bool`) and message (`str`).
    """

def create_pipe_indices(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Create a pipe's indices.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose indices need to be created.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    return False, f"Cannot create indices for instance connectors of type '{self.type}'."

def clear_pipe(
    self,
    pipe: mrsm.Pipe,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    **kwargs: Any
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
    raise NotImplementedError

def get_pipe_data(
    self,
    pipe: mrsm.Pipe,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
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
    if type(self).get_pipe_docs is get_pipe_docs:
        raise NotImplementedError(
            f"Missing `get_pipe_data()` or `get_pipe_docs()` for {type(self)}."
        )

    docs = self.get_pipe_docs(
        pipe=pipe,
        select_columns=select_columns,
        omit_columns=omit_columns,
        begin=begin,
        end=end,
        params=params,
        debug=debug,
        **kwargs
    )
    if not docs:
        return None

    pd = mrsm.attempt_import('pandas')
    try:
        return pd.DataFrame(docs)
    except Exception as e:
        from meerschaum.utils.warnings import warn
        warn(f"Cannot build DataFrame from pipe docs:\n{e}")
    
    return None

def get_pipe_docs(
    self,
    pipe: mrsm.Pipe,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    **kwargs: Any
) -> list[dict[str, Any]]:
    """
    Return a pipe's data as a list of documents.
    Defaults to `get_pipe_data().to_dict(orient='records')`.

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
    The target table's data as a list of dictionaries.
    """
    df = self.get_pipe_data(
        pipe=pipe,
        select_columns=select_columns,
        omit_columns=omit_columns,
        begin=begin,
        end=end,
        params=params,
        debug=debug,
        **kwargs
    )
    if df is None or df.empty:
        return []
    return df.to_dict(orient='records')

@abc.abstractmethod
def get_sync_time(
    self,
    pipe: mrsm.Pipe,
    params: Optional[Dict[str, Any]] = None,
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

@abc.abstractmethod
def get_pipe_columns_types(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> Dict[str, str]:
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

def get_pipe_columns_indices(
    self,
    debug: bool = False,
) -> Dict[str, List[Dict[str, str]]]:
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
    return {}

def get_pipe_size(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> Union[int, None]:
    """
    Return the on-disk size of a pipe's target table in bytes.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table size to measure.

    Returns
    -------
    An `int` of the number of bytes occupied by the target table,
    or `None` if the size cannot be determined.
    """
    raise NotImplementedError(
        f"`get_pipe_size()` is not implemented for instance connectors of type '{self.type}'."
    )

def compress_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Compress a pipe's target table to reduce disk usage.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table to compress.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    return False, (
        f"Compression is not supported for instance connectors of type '{self.type}'."
    )
