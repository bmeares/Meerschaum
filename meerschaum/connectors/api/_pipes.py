#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register or fetch Pipes from the API.

Every method is the Rust client (`meerschaum._core.ApiConnector`): the same routes, query parameters
and request bodies as v4.
"""

from __future__ import annotations

import json
from datetime import datetime

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Union, Any, Optional, List, Dict, Tuple


def pipe_r_url(pipe: mrsm.Pipe) -> str:
    """Return a relative URL path from a Pipe's keys."""
    from meerschaum._internal.static import STATIC_CONFIG
    location_key = pipe.location_key
    if location_key is None:
        location_key = '[None]'
    return (
        f"{STATIC_CONFIG['api']['endpoints']['pipes']}/"
        + f"{pipe.connector_keys}/{pipe.metric_key}/{location_key}"
    )


def get_pipe_instance_keys(self, pipe: mrsm.Pipe) -> Union[str, None]:
    """
    Return the configured instance keys for a pipe if set,
    else fall back to the default `instance_keys` for this `APIConnector`.
    """
    return self._rust_client().get_pipe_instance_keys(pipe)


def register_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> SuccessTuple:
    """
    Submit a POST to the API to register a new Pipe object.
    Returns a tuple of (success_bool, response_dict).
    """
    return self._rust_client().register_pipe(pipe)


def edit_pipe(
    self,
    pipe: mrsm.Pipe,
    patch: bool = False,
    debug: bool = False,
) -> SuccessTuple:
    """
    Submit a PATCH to the API to edit an existing Pipe object.
    Returns a tuple of (success_bool, response_dict).
    """
    return self._rust_client().edit_pipe(pipe, patch=patch)


def fetch_pipes_keys(
    self,
    connector_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    location_keys: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False
) -> Union[
    Dict[Union[int, str], Tuple[str, str, Union[str, None], Dict[str, Any]]],
    List[Tuple[str, str, Union[str, None]]],
]:
    """
    Fetch registered Pipes' keys from the API.

    Parameters
    ----------
    connector_keys: Optional[List[str]], default None
        The connector keys for the query.

    metric_keys: Optional[List[str]], default None
        The metric keys for the query.

    location_keys: Optional[List[str]], default None
        The location keys for the query.

    tags: Optional[List[str]], default None
        A list of tags for the query.

    params: Optional[Dict[str, Any]], default None
        A parameters dictionary for filtering against the `pipes` table
        (e.g. `{'connector_keys': 'plugin:foo'}`).
        Not recommeded to be used.

    Returns
    -------
    A dictionary mapping pipe IDs to key tuples, or a list of key tuples for older servers.
    """
    return self._rust_client().fetch_pipes_keys(
        connector_keys,
        metric_keys,
        location_keys,
        tags,
        params,
    )


def sync_pipe(
    self,
    pipe: mrsm.Pipe,
    df: Optional[Union['pd.DataFrame', Dict[Any, Any], str]] = None,
    chunksize: Optional[int] = -1,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Sync a DataFrame (or documents, or a dictionary of lists) into a Pipe.
    Keyword arguments are sent to the API as query parameters.
    """
    if isinstance(df, str):
        df = json.loads(df)
    return self._rust_client().sync_pipe(pipe, df, chunksize=chunksize, **kw)


def delete_pipe_cache(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Invalidate the server-side cache for a pipe."""
    return self._rust_client().delete_pipe_cache(pipe)


def delete_pipe(
    self,
    pipe: Optional[mrsm.Pipe] = None,
    debug: bool = False,
) -> SuccessTuple:
    """Delete a Pipe and drop its table."""
    if pipe is None:
        raise ValueError("Pipe cannot be None.")
    return self._rust_client().delete_pipe(pipe)


def get_pipe_data(
    self,
    pipe: mrsm.Pipe,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[str, datetime, int, None] = None,
    end: Union[str, datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    as_chunks: bool = False,
    debug: bool = False,
    **kw: Any
) -> Union['pd.DataFrame', None]:
    """Fetch data from the API."""
    return self._rust_client().get_pipe_data(
        pipe,
        select_columns=select_columns,
        omit_columns=omit_columns,
        begin=begin,
        end=end,
        params=params,
        as_chunks=as_chunks,
    )


def get_pipe_id(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Union[int, str, None]:
    """Get a Pipe's ID from the API."""
    return self._rust_client().get_pipe_id(pipe)


def get_pipe_attributes(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Get a Pipe's attributes from the API.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe whose attributes we are fetching.

    Returns
    -------
    A dictionary of a pipe's attributes.
    If the pipe does not exist, return an empty dictionary.
    """
    return self._rust_client().get_pipe_attributes(pipe)


def get_sync_time(
    self,
    pipe: mrsm.Pipe,
    params: Optional[Dict[str, Any]] = None,
    newest: bool = True,
    debug: bool = False,
) -> Union[datetime, int, None]:
    """
    Get a Pipe's most recent datetime value from the API.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to select from.

    params: Optional[Dict[str, Any]], default None
        Optional params dictionary to build the WHERE clause.

    newest: bool, default True
        If `True`, get the most recent datetime (honoring `params`).
        If `False`, get the oldest datetime (ASC instead of DESC).

    Returns
    -------
    The most recent (or oldest if `newest` is `False`) datetime of a pipe,
    rounded down to the closest minute.
    """
    return self._rust_client().get_sync_time(pipe, params=params, newest=newest)


def pipe_exists(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False
) -> bool:
    """
    Check the API to see if a Pipe exists.

    Parameters
    ----------
    pipe: 'meerschaum.Pipe'
        The pipe which were are querying.

    Returns
    -------
    A bool indicating whether a pipe's underlying table exists.
    """
    return self._rust_client().pipe_exists(pipe)


def create_metadata(
    self,
    debug: bool = False
) -> bool:
    """
    Create metadata tables.

    Returns
    -------
    A bool indicating success.
    """
    return self._rust_client().create_metadata()


def get_pipe_rowcount(
    self,
    pipe: mrsm.Pipe,
    begin: Union[str, datetime, int, None] = None,
    end: Union[str, datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    remote: bool = False,
    debug: bool = False,
) -> int:
    """
    Get a pipe's row count from the API.

    Parameters
    ----------
    pipe: 'meerschaum.Pipe':
        The pipe whose row count we are counting.

    begin: Union[str, datetime, int, None], default None
        If provided, bound the count by this datetime.

    end: Union[str, datetime, int, None], default None
        If provided, bound the count by this datetime.

    params: Optional[Dict[str, Any]], default None
        If provided, bound the count by these parameters.

    remote: bool, default False
        If `True`, return the rowcount for the fetch definition.

    Returns
    -------
    The number of rows in the pipe's table, bound the given parameters.
    If the table does not exist, return 0.
    """
    return self._rust_client().get_pipe_rowcount(
        pipe,
        begin=begin,
        end=end,
        params=params,
        remote=remote,
    )


def drop_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False
) -> SuccessTuple:
    """
    Drop a pipe's table but maintain its registration.

    Parameters
    ----------
    pipe: meerschaum.Pipe:
        The pipe to be dropped.

    Returns
    -------
    A success tuple (bool, str).
    """
    if pipe is None:
        raise ValueError("Pipe cannot be None.")
    return self._rust_client().drop_pipe(pipe)


def clear_pipe(
    self,
    pipe: mrsm.Pipe,
    begin: Union[str, datetime, int, None] = None,
    end: Union[str, datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    Delete rows in a pipe's table.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe with rows to be deleted.

    begin: Union[str, datetime, int, None], default None
        Delete rows at or after this datetime.

    end: Union[str, datetime, int, None], default None
        Delete rows before this datetime.

    params: Optional[Dict[str, Any]], default None
        Delete rows matching these parameters.

    Returns
    -------
    A success tuple.
    """
    return self._rust_client().clear_pipe(pipe, begin=begin, end=end, params=params)


def get_pipe_columns_types(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Union[Dict[str, str], None]:
    """
    Fetch the columns and types of the pipe's table.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe whose columns to be queried.

    Returns
    -------
    A dictionary mapping column names to their database types, or `None` on failure.
    """
    return self._rust_client().get_pipe_columns_types(pipe)


def get_pipe_columns_indices(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Union[Dict[str, List[Dict[str, str]]], None]:
    """
    Fetch the index information for a pipe.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose columns to be queried.

    Returns
    -------
    A dictionary mapping column names to lists of dictionaries with index `name` and `type`,
    or `None` on failure.
    """
    return self._rust_client().get_pipe_columns_indices(pipe)


def get_pipe_docs(
    self,
    pipe: mrsm.Pipe,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[str, datetime, int, None] = None,
    end: Union[str, datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    order: str = 'asc',
    limit: Optional[int] = None,
    debug: bool = False,
    **kw: Any
) -> List[Dict[str, Any]]:
    """Fetch a pipe's data as a list of documents from the API."""
    return self._rust_client().get_pipe_docs(
        pipe,
        select_columns=select_columns,
        omit_columns=omit_columns,
        begin=begin,
        end=end,
        params=params,
        order=order,
        limit=limit,
    )


def get_pipe_size(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kw: Any
) -> Union[int, None]:
    """
    Return the size of the pipe's table in bytes, or `None` if it is not available.
    """
    return self._rust_client().get_pipe_size(pipe)


def compress_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Compress a pipe's table on the API instance."""
    return self._rust_client().compress_pipe(pipe)


def decompress_pipe(
    self,
    pipe: mrsm.Pipe,
    no_policy: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Decompress a pipe's table on the API instance."""
    return self._rust_client().decompress_pipe(pipe, no_policy=no_policy)


def vacuum_pipe(
    self,
    pipe: mrsm.Pipe,
    full: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Vacuum a pipe's table on the API instance."""
    return self._rust_client().vacuum_pipe(pipe, full=full)


def partition_pipe(
    self,
    pipe: mrsm.Pipe,
    chunk_minutes: Optional[int] = None,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Repartition a pipe's table on the API instance."""
    return self._rust_client().partition_pipe(pipe, chunk_minutes=chunk_minutes)


def analyze_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Analyze a pipe's table on the API instance."""
    return self._rust_client().analyze_pipe(pipe)


def get_pipe_index_names(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Dict[str, str]:
    """Return the index names of a pipe's table."""
    return self._rust_client().get_pipe_index_names(pipe)
