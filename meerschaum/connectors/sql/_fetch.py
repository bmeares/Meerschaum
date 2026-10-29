#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the Connector fetch() method
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Union, Any, List, Dict

if TYPE_CHECKING:
    import pandas as pd


_JOIN_FETCH_WARNED = False


def _warn_join_fetch_ignored() -> None:
    """Warn, once per process, that the experimental `join_fetch` query shape was removed."""
    global _JOIN_FETCH_WARNED
    if _JOIN_FETCH_WARNED:
        return
    _JOIN_FETCH_WARNED = True
    from meerschaum.utils.warnings import warn
    warn(
        "`system:experimental:join_fetch` was removed in Meerschaum 5 and is ignored: "
        "the fetch query is not narrowed to the pipe's newest rows per ID.",
        stack=False,
    )


def _check_bounds_are_comparable(
    pipe: mrsm.Pipe,
    parent: Optional[mrsm.Pipe],
    begin: Any,
    end: Any,
    sync_time: Any,
) -> None:
    """
    v4 compared `begin >= end` before building the query, so a timezone-naive bound against an
    aware one (or text against a datetime) raised `TypeError`. The Rust builder does not compare,
    so the façade keeps the error (integer axes convert datetimes first and never raised).
    """
    effective_begin = sync_time if begin == '' else begin
    if effective_begin in (None, '') or end is None:
        return
    parent_dt_col = parent.columns.get('datetime', None) if parent is not None else None
    dt_col = parent_dt_col or pipe.columns.get('datetime', None) or pipe.guess_datetime()
    if not dt_col:
        return
    dt_typ = (
        parent.dtypes.get(parent_dt_col, 'datetime')
        if parent_dt_col
        else pipe.dtypes.get(dt_col, 'datetime')
    )
    if 'int' in str(dt_typ).lower():
        return
    effective_begin >= end


def fetch(
    self,
    pipe: mrsm.Pipe,
    begin: Union[datetime, int, str, None] = '',
    end: Union[datetime, int, str, None] = None,
    check_existing: bool = True,
    chunksize: Optional[int] = -1,
    workers: Optional[int] = None,
    debug: bool = False,
    **kw: Any
) -> Union['pd.DataFrame', List[Any], None]:
    """Execute the SQL definition and return a Pandas DataFrame.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe object which contains the `fetch` metadata.

        - pipe.columns['datetime']: str
            - Name of the datetime column for the remote table.
        - pipe.parameters['fetch']: Dict[str, Any]
            - Parameters necessary to execute a query.
        - pipe.parameters['fetch']['definition']: str
            - Raw SQL query to execute to generate the pandas DataFrame.
        - pipe.parameters['fetch']['backtrack_minutes']: Union[int, float]
            - How many minutes before `begin` to search for data (*optional*).

    begin: Union[datetime, int, str, None], default None
        Most recent datatime to search for data.
        If `backtrack_minutes` is provided, subtract `backtrack_minutes`.

    end: Union[datetime, int, str, None], default None
        The latest datetime to search for data.
        If `end` is `None`, do not bound 

    check_existing: bool, defult True
        If `False`, use a backtrack interval of 0 minutes.

    chunksize: Optional[int], default -1
        How many rows to load into memory at once.
        Otherwise the entire result set is loaded into memory.

    workers: Optional[int], default None
        How many threads to use when consuming the generator.
        Defaults to the number of cores.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A pandas DataFrame generator.
    """
    meta_def = self.get_pipe_metadef(
        pipe,
        begin=begin,
        end=end,
        check_existing=check_existing,
        debug=debug,
        **kw
    )
    chunks = self.read(
        meta_def,
        chunksize=chunksize,
        workers=workers,
        as_iterator=True,
        debug=debug,
    )
    return chunks


def get_pipe_metadef(
    self,
    pipe: mrsm.Pipe,
    params: Optional[Dict[str, Any]] = None,
    begin: Union[datetime, int, str, None] = '',
    end: Union[datetime, int, str, None] = None,
    check_existing: bool = True,
    debug: bool = False,
    **kw: Any
) -> Union[str, None]:
    """
    Return a pipe's meta definition fetch query.

    params: Optional[Dict[str, Any]], default None
        Optional params dictionary to build the `WHERE` clause.
        See `meerschaum.utils.sql.build_where`.

    begin: Union[datetime, int, str, None], default None
        Most recent datatime to search for data.
        If `backtrack_minutes` is provided, subtract `backtrack_minutes`.

    end: Union[datetime, int, str, None], default None
        The latest datetime to search for data.
        If `end` is `None`, do not bound 

    check_existing: bool, default True
        If `True`, apply the backtrack interval.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A pipe's meta definition fetch query string.
    """
    from meerschaum import _core
    from meerschaum.config import get_config
    if (pipe.columns or {}).get('id', None) and get_config('system', 'experimental', 'join_fetch'):
        _warn_join_fetch_ignored()

    parent = pipe.parent
    parent_spec = None
    if parent is not None:
        parent_dt_col = parent.columns.get('datetime', None)
        parent_instance = parent.instance_connector
        parent_spec = {
            'target': parent.target,
            'schema': (
                parent_instance.get_pipe_schema(parent)
                if parent_instance.type == 'sql'
                else None
            ),
            'dt_col': parent_dt_col,
            'dt_typ': (
                parent.dtypes.get(parent_dt_col, 'datetime')
                if parent_dt_col
                else None
            ),
            'precision_unit': (parent.precision or {}).get('unit', 'second'),
        }
    sync_time = pipe.get_sync_time(debug=debug) if begin == '' else None
    _check_bounds_are_comparable(pipe, parent, begin, end, sync_time)
    meta_def = _core.get_pipe_metadef(
        self._core_connector(),
        pipe,
        params,
        begin,
        end,
        check_existing,
        sync_time,
        parent_spec,
    )
    if meta_def is None:
        raise ValueError(f"No SQL definition could be found for {pipe}.")
    return meta_def


def get_pipe_query(
    pipe: mrsm.Pipe,
    apply_symlinks: bool = True,
    warn: bool = True,
) -> Union[str, None]:
    """
    Run through the possible keys for a pipe's query and return the first match.

    - fetch, definition
    - definition
    - query
    - sql
    """
    import textwrap
    from meerschaum.utils.warnings import warn as _warn
    from meerschaum.utils.pipes import replace_pipes_syntax

    parameters = pipe.get_parameters(apply_symlinks=apply_symlinks)

    if parameters.get('fetch', {}).get('definition', None):
        definition = parameters['fetch']['definition']
    elif pipe.parameters.get('definition', None):
        definition = parameters['definition']
    elif pipe.parameters.get('query', None):
        definition = parameters['query']
    elif pipe.parameters.get('sql', None):
        definition = parameters['sql']
    else:
        if warn:
            _warn(
                f"Could not determine a SQL definition for {pipe}.\n"
                + "    Set the key `query` in `pipe.parameters` to a valid SQL query."
            )
        return None

    if apply_symlinks:
        definition = replace_pipes_syntax(definition)

    return textwrap.dedent(definition.lstrip().rstrip())


