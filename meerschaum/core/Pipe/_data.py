#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Retrieve Pipes' data from instances.
"""

from __future__ import annotations
from datetime import datetime, timedelta

import meerschaum as mrsm
from meerschaum.utils.typing import (
    Optional, Dict, Any, Union, List, Tuple, Iterator, TYPE_CHECKING,
)
from meerschaum.config import get_config

if TYPE_CHECKING:
    pd = mrsm.attempt_import('pandas')


def get_data(
    self,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[datetime, int, str, None] = None,
    end: Union[datetime, int, str, None] = None,
    params: Optional[Dict[str, Any]] = None,
    as_iterator: bool = False,
    as_chunks: bool = False,
    as_dask: bool = False,
    chunk_interval: Union[timedelta, int, None] = None,
    order: Optional[str] = 'asc',
    limit: Optional[int] = None,
    fresh: bool = False,
    debug: bool = False,
    **kw: Any
) -> Union['pd.DataFrame', Iterator['pd.DataFrame'], None]:
    """
    Get a pipe's data from the instance connector.

    Parameters
    ----------
    select_columns: Optional[List[str]], default None
        If provided, only select these given columns.
        Otherwise select all available columns (i.e. `SELECT *`).

    omit_columns: Optional[List[str]], default None
        If provided, remove these columns from the selection.

    begin: Union[datetime, int, str, None], default None
        Lower bound datetime to begin searching for data (inclusive).
        Translates to a `WHERE` clause like `WHERE datetime >= begin`.
        Defaults to `None`.

    end: Union[datetime, int, str, None], default None
        Upper bound datetime to stop searching for data (inclusive).
        Translates to a `WHERE` clause like `WHERE datetime < end`.
        Defaults to `None`.

    params: Optional[Dict[str, Any]], default None
        Filter the retrieved data by a dictionary of parameters.
        See `meerschaum.utils.sql.build_where` for more details. 

    as_iterator: bool, default False
        If `True`, return a generator of chunks of pipe data.

    as_chunks: bool, default False
        Alias for `as_iterator`.

    as_dask: bool, default False
        If `True`, return a `dask.DataFrame`
        (which may be loaded into a Pandas DataFrame with `df.compute()`).

    chunk_interval: Union[timedelta, int, None], default None
        If `as_iterator`, then return chunks with `begin` and `end` separated by this interval.
        This may be set under `pipe.parameters['chunk_minutes']`.
        By default, use a timedelta of 1440 minutes (1 day).
        If `chunk_interval` is an integer and the `datetime` axis a timestamp,
        the use a timedelta with the number of minutes configured to this value.
        If the `datetime` axis is an integer, default to the configured chunksize.
        If `chunk_interval` is a `timedelta` and the `datetime` axis an integer,
        use the number of minutes in the `timedelta`.

    order: Optional[str], default 'asc'
        If `order` is not `None`, sort the resulting dataframe by indices.

    limit: Optional[int], default None
        If provided, cap the dataframe to this many rows.

    fresh: bool, default False
        If `True`, skip local cache and directly query the instance connector.

    debug: bool, default False
        Verbosity toggle.
        Defaults to `False`.

    Returns
    -------
    A `pd.DataFrame` for the pipe's data corresponding to the provided parameters.

    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.utils.misc import iterate_chunks, items_str
    from meerschaum.utils.dtypes import to_pandas_dtype, coerce_timezone
    from meerschaum.utils.dataframe import add_missing_cols_to_df, df_is_chunk_generator
    from meerschaum.utils.packages import attempt_import
    dd = attempt_import('dask.dataframe') if as_dask else None
    dask = attempt_import('dask') if as_dask else None
    dateutil_parser = attempt_import('dateutil.parser')

    if select_columns == '*':
        select_columns = None
    elif isinstance(select_columns, str):
        select_columns = [select_columns]

    if isinstance(omit_columns, str):
        omit_columns = [omit_columns]

    begin, end = self.parse_date_bounds(begin, end)
    as_iterator = as_iterator or as_chunks
    dt_col = self.columns.get('datetime', None)

    def _sort_df(_df):
        if df_is_chunk_generator(_df):
            return _df
        indices = [] if dt_col not in _df.columns else [dt_col]
        non_dt_cols = [
            col
            for col_ix, col in self.columns.items()
            if col_ix != 'datetime' and col in _df.columns
        ]
        indices.extend(non_dt_cols)
        if 'dask' not in _df.__module__:
            _df.sort_values(
                by=indices,
                inplace=True,
                ascending=(str(order).lower() == 'asc'),
            )
            _df.reset_index(drop=True, inplace=True)
        else:
            _df = _df.sort_values(
                by=indices,
                ascending=(str(order).lower() == 'asc'),
            )
            _df = _df.reset_index(drop=True)
        if limit is not None and len(_df) > limit:
            return _df.head(limit)
        return _df

    if as_iterator or as_chunks:
        df = self._get_data_as_iterator(
            select_columns=select_columns,
            omit_columns=omit_columns,
            begin=begin,
            end=end,
            params=params,
            chunk_interval=chunk_interval,
            limit=limit,
            order=order,
            fresh=fresh,
            debug=debug,
        )
        return _sort_df(df)

    if as_dask:
        from multiprocessing.pool import ThreadPool
        dask_pool = ThreadPool(self.get_num_workers())
        dask.config.set(pool=dask_pool)
        chunk_interval = self.get_chunk_interval(chunk_interval, debug=debug)
        bounds = self.get_chunk_bounds(
            begin=begin,
            end=end,
            bounded=False,
            chunk_interval=chunk_interval,
            debug=debug,
        )
        dask_chunks = [
            dask.delayed(self.get_data)(
                select_columns=select_columns,
                omit_columns=omit_columns,
                begin=chunk_begin,
                end=chunk_end,
                params=params,
                chunk_interval=chunk_interval,
                order=order,
                limit=limit,
                fresh=fresh,
                debug=debug,
            )
            for (chunk_begin, chunk_end) in bounds
        ]
        dask_meta = {
            col: to_pandas_dtype(typ)
            for col, typ in self.dtypes.items()
        }
        return _sort_df(dd.from_delayed(dask_chunks, meta=dask_meta))

    if not self.exists(debug=debug):
        return None

    if self.cache_pipe is not None:
        if not fresh:
            _sync_cache_tuple = self.cache_pipe.sync(
                begin=begin,
                end=end,
                params=params,
                debug=debug,
                **kw
            )
            if not _sync_cache_tuple[0]:
                warn(f"Failed to sync cache for {self}:\n" + _sync_cache_tuple[1])
                fresh = True
            else: ### Successfully synced cache.
                return self.enforce_dtypes(
                    self.cache_pipe.get_data(
                        select_columns=select_columns,
                        omit_columns=omit_columns,
                        begin=begin,
                        end=end,
                        params=params,
                        order=order,
                        limit=limit,
                        debug=debug,
                        fresh=True,
                        **kw
                    ),
                    debug=debug,
                )

    with Venv(get_connector_plugin(self.instance_connector)):
        df = self.instance_connector.get_pipe_data(
            pipe=self,
            select_columns=select_columns,
            omit_columns=omit_columns,
            begin=begin,
            end=end,
            params=params,
            limit=limit,
            order=order,
            debug=debug,
            **kw
        )
        if df is None:
            return df

        if not select_columns:
            select_columns = [col for col in df.columns]

        cols_to_omit = [
            col
            for col in df.columns
            if (
                col in (omit_columns or [])
                or
                col not in (select_columns or [])
            )
        ]
        cols_to_add = [
            col
            for col in select_columns
            if col not in df.columns
        ]
        if cols_to_omit:
            warn(
                (
                    f"Received {len(cols_to_omit)} omitted column"
                    + ('s' if len(cols_to_omit) != 1 else '')
                    + f" for {self}. "
                    + "Consider adding `select_columns` and `omit_columns` support to "
                    + f"'{self.instance_connector.type}' connectors to improve performance."
                ),
                stack=False,
            )
            _cols_to_select = [col for col in df.columns if col not in cols_to_omit]
            df = df[_cols_to_select]

        if cols_to_add:
            warn(
                (
                    f"Specified columns {items_str(cols_to_add)} were not found on {self}. "
                    + "Adding these to the DataFrame as null columns."
                ),
                stack=False,
            )
            df = add_missing_cols_to_df(df, {col: 'string' for col in cols_to_add})

        enforced_df = self.enforce_dtypes(df, debug=debug)

        if order:
            return _sort_df(enforced_df)
        return enforced_df


def _get_data_as_iterator(
    self,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    params: Optional[Dict[str, Any]] = None,
    chunk_interval: Union[timedelta, int, None] = None,
    limit: Optional[int] = None,
    order: Optional[str] = 'asc',
    fresh: bool = False,
    debug: bool = False,
    **kw: Any
) -> Iterator['pd.DataFrame']:
    """
    Return a pipe's data as a generator.
    """
    from meerschaum.utils.misc import round_time
    begin, end = self.parse_date_bounds(begin, end)
    if not self.exists(debug=debug):
        return

    _ = kw.pop('as_chunks', None)
    _ = kw.pop('as_iterator', None)
    dt_col = self.columns.get('datetime', None)
    min_dt = (
        begin
        if begin is not None
        else self.get_sync_time(round_down=False, newest=False, params=params, debug=debug)
    ) if dt_col else None
    max_dt = (
        end
        if end is not None
        else self.get_sync_time(round_down=False, newest=True, params=params, debug=debug)
    ) if dt_col else None

    ### We want to search just past the maximum value.
    if end is None:
        if isinstance(max_dt, int):
            max_dt += 1
        elif isinstance(max_dt, datetime):
            max_dt = round_time(max_dt + timedelta(minutes=1))

    chunk_interval = self.get_chunk_interval(chunk_interval, debug=debug)

    ### If we can't determine bounds
    ### or if chunk_interval exceeds the max,
    ### return a single chunk.
    if (
        (min_dt is None and max_dt is None)
        or
        (min_dt + chunk_interval) > max_dt
    ):
        yield self.get_data(
            select_columns=select_columns,
            omit_columns=omit_columns,
            begin=begin,
            end=end,
            params=params,
            limit=limit,
            order=order,
            fresh=fresh,
            debug=debug,
        )
        return

    chunk_bounds = self.get_chunk_bounds(
        begin=min_dt,
        end=max_dt,
        chunk_interval=chunk_interval,
        debug=debug,
    )

    for chunk_begin, chunk_end in chunk_bounds:
        chunk = self.get_data(
            select_columns=select_columns,
            omit_columns=omit_columns,
            begin=chunk_begin,
            end=chunk_end,
            params=params,
            limit=limit,
            order=order,
            fresh=fresh,
            debug=debug,
        )
        if len(chunk) > 0:
            yield chunk


def get_backtrack_data(
    self,
    backtrack_minutes: Optional[int] = None,
    begin: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    fresh: bool = False,
    debug: bool = False,
    **kw: Any
) -> Optional['pd.DataFrame']:
    """
    Get the most recent data from the instance connector as a Pandas DataFrame.

    Parameters
    ----------
    backtrack_minutes: Optional[int], default None
        How many minutes from `begin` to select from.
        If `None`, use `pipe.parameters['fetch']['backtrack_minutes']`.

    begin: Optional[datetime], default None
        The starting point to search for data.
        If begin is `None` (default), use the most recent observed datetime
        (AKA sync_time).

        ```
        E.g. begin = 02:00

        Search this region.           Ignore this, even if there's data.
        /  /  /  /  /  /  /  /  /  |
        -----|----------|----------|----------|----------|----------|
        00:00      01:00      02:00      03:00      04:00      05:00

        ```

    params: Optional[Dict[str, Any]], default None
        The standard Meerschaum `params` query dictionary.

    limit: Optional[int], default None
        If provided, cap the number of rows to be returned.

    fresh: bool, default False
        If `True`, Ignore local cache and pull directly from the instance connector.
        Only comes into effect if a pipe was created with `cache=True`.

    debug: bool default False
        Verbosity toggle.

    Returns
    -------
    A `pd.DataFrame` for the pipe's data corresponding to the provided parameters. Backtrack data
    is a convenient way to get a pipe's data "backtracked" from the most recent datetime.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    if not self.exists(debug=debug):
        return None

    begin = self.parse_date_bounds(begin)

    backtrack_interval = self.get_backtrack_interval(debug=debug)
    if backtrack_minutes is None:
        backtrack_minutes = (
            (backtrack_interval.total_seconds() / 60)
            if isinstance(backtrack_interval, timedelta)
            else backtrack_interval
        )

    if self.cache_pipe is not None:
        if not fresh:
            _sync_cache_tuple = self.cache_pipe.sync(begin=begin, params=params, debug=debug, **kw)
            if not _sync_cache_tuple[0]:
                warn(f"Failed to sync cache for {self}:\n" + _sync_cache_tuple[1])
                fresh = True
            else: ### Successfully synced cache.
                return self.enforce_dtypes(
                    self.cache_pipe.get_backtrack_data(
                        fresh=True,
                        begin=begin,
                        backtrack_minutes=backtrack_minutes,
                        params=params,
                        limit=limit,
                        order=kw.get('order', 'desc'),
                        debug=debug,
                        **kw
                    ),
                    debug=debug,
                )

    if hasattr(self.instance_connector, 'get_backtrack_data'):
        with Venv(get_connector_plugin(self.instance_connector)):
            return self.enforce_dtypes(
                self.instance_connector.get_backtrack_data(
                    pipe=self,
                    begin=begin,
                    backtrack_minutes=backtrack_minutes,
                    params=params,
                    limit=limit,
                    debug=debug,
                    **kw
                ),
                debug=debug,
            )

    if begin is None:
        begin = self.get_sync_time(params=params, debug=debug)

    backtrack_interval = (
        timedelta(minutes=backtrack_minutes)
        if isinstance(begin, datetime)
        else backtrack_minutes
    )
    if begin is not None:
        begin = begin - backtrack_interval

    return self.get_data(
        begin=begin,
        params=params,
        debug=debug,
        limit=limit,
        order=kw.get('order', 'desc'),
        **kw
    )


def get_rowcount(
    self,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    remote: bool = False,
    debug: bool = False
) -> int:
    """
    Get a Pipe's instance or remote rowcount.

    Parameters
    ----------
    begin: Optional[datetime], default None
        Count rows where datetime > begin.

    end: Optional[datetime], default None
        Count rows where datetime < end.

    remote: bool, default False
        Count rows from a pipe's remote source.
        **NOTE**: This is experimental!

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    An `int` of the number of rows in the pipe corresponding to the provided parameters.
    Returned 0 if the pipe does not exist.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.utils.misc import filter_keywords

    begin, end = self.parse_date_bounds(begin, end)
    connector = self.instance_connector if not remote else self.connector
    try:
        with Venv(get_connector_plugin(connector)):
            if not hasattr(connector, 'get_pipe_rowcount'):
                warn(
                    f"Connectors of type '{connector.type}' "
                    "do not implement `get_pipe_rowcount()`.",
                    stack=False,
                )
                return 0
            kwargs = filter_keywords(
                connector.get_pipe_rowcount,
                begin=begin,
                end=end,
                params=params,
                remote=remote,
                debug=debug,
            )
            if remote and 'remote' not in kwargs:
                warn(
                    f"Connectors of type '{connector.type}' do not support remote rowcounts.",
                    stack=False,
                )
                return 0
            rowcount = connector.get_pipe_rowcount(
                self,
                begin=begin,
                end=end,
                params=params,
                remote=remote,
                debug=debug,
            )
            if rowcount is None:
                return 0
            return rowcount
    except AttributeError as e:
        warn(e)
        if remote:
            return 0
    warn(f"Failed to get a rowcount for {self}.")
    return 0


def get_chunk_interval(
    self,
    chunk_interval: Union[timedelta, int, None] = None,
    debug: bool = False,
) -> Union[timedelta, int]:
    """
    Get the chunk interval to use for this pipe.

    Parameters
    ----------
    chunk_interval: Union[timedelta, int, None], default None
        If provided, coerce this value into the correct type.
        For example, if the datetime axis is an integer, then
        return the number of minutes.

    Returns
    -------
    The chunk interval (`timedelta` or `int`) to use with this pipe's `datetime` axis.
    """
    default_chunk_minutes = get_config('pipes', 'parameters', 'verify', 'chunk_minutes')
    configured_chunk_minutes = self.parameters.get('verify', {}).get('chunk_minutes', None)
    chunk_minutes = (
        (configured_chunk_minutes or default_chunk_minutes)
        if chunk_interval is None
        else (
            chunk_interval
            if isinstance(chunk_interval, int)
            else int(chunk_interval.total_seconds() / 60)
        )
    )

    dt_col = self.columns.get('datetime', None)
    if dt_col is None:
        return timedelta(minutes=chunk_minutes)

    dt_dtype = self.dtypes.get(dt_col, 'datetime64[ns, UTC]')
    if 'int' in dt_dtype.lower():
        return chunk_minutes
    return timedelta(minutes=chunk_minutes)


def get_chunk_bounds(
    self,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    bounded: bool = False,
    chunk_interval: Union[timedelta, int, None] = None,
    debug: bool = False,
) -> List[
    Tuple[
        Union[datetime, int, None],
        Union[datetime, int, None],
    ]
]:
    """
    Return a list of datetime bounds for iterating over the pipe's `datetime` axis.

    Parameters
    ----------
    begin: Union[datetime, int, None], default None
        If provided, do not select less than this value.
        Otherwise the first chunk will be unbounded.

    end: Union[datetime, int, None], default None
        If provided, do not select greater than or equal to this value.
        Otherwise the last chunk will be unbounded.

    bounded: bool, default False
        If `True`, do not include `None` in the first chunk.

    chunk_interval: Union[timedelta, int, None], default None
        If provided, use this interval for the size of chunk boundaries.
        The default value for this pipe may be set
        under `pipe.parameters['verify']['chunk_minutes']`.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A list of chunk bounds (datetimes or integers).
    If unbounded, the first and last chunks will include `None`.
    """
    from datetime import timedelta
    from meerschaum.utils.dtypes import are_dtypes_equal
    from meerschaum.utils.misc import interval_str
    include_less_than_begin = not bounded and begin is None
    include_greater_than_end = not bounded and end is None
    if begin is None:
        begin = self.get_sync_time(newest=False, debug=debug)
    if end is None:
        end = self.get_sync_time(newest=True, debug=debug)
        if end is not None and hasattr(end, 'tzinfo'):
            end += timedelta(minutes=1)
        elif are_dtypes_equal(str(type(end)), 'int'):
            end += 1
    if begin is None and end is None:
        return [(None, None)]

    begin, end = self.parse_date_bounds(begin, end)

    ### Set the chunk interval under `pipe.parameters['verify']['chunk_minutes']`.
    chunk_interval = self.get_chunk_interval(chunk_interval, debug=debug)
    
    ### Build a list of tuples containing the chunk boundaries
    ### so that we can sync multiple chunks in parallel.
    ### Run `verify pipes --workers 1` to sync chunks in series.
    chunk_bounds = []
    begin_cursor = begin
    num_chunks = 0
    max_chunks = 1_000_000
    while begin_cursor < end:
        end_cursor = begin_cursor + chunk_interval
        chunk_bounds.append((begin_cursor, end_cursor))
        begin_cursor = end_cursor
        num_chunks += 1
        if num_chunks >= max_chunks:
            raise ValueError(
                f"Too many chunks of size '{interval_str(chunk_interval)}' between '{begin}' and '{end}'."
            )

    ### The chunk interval might be too large.
    if not chunk_bounds and end >= begin:
        chunk_bounds = [(begin, end)]

    ### Truncate the last chunk to the end timestamp.
    if chunk_bounds[-1][1] > end:
        chunk_bounds[-1] = (chunk_bounds[-1][0], end)

    ### Pop the last chunk if its bounds are equal.
    if chunk_bounds[-1][0] == chunk_bounds[-1][1]:
        chunk_bounds = chunk_bounds[:-1]

    if include_less_than_begin:
        chunk_bounds = [(None, begin)] + chunk_bounds
    if include_greater_than_end:
        chunk_bounds = chunk_bounds + [(end, None)]

    return chunk_bounds


def get_chunk_bounds_batches(
    self,
    chunk_bounds: List[Tuple[Union[datetime, int, None], Union[datetime, int, None]]],
    batchsize: Optional[int] = None,
    workers: Optional[int] = None,
    debug: bool = False,
) -> List[
    Tuple[
        Tuple[
            Union[datetime, int, None],
            Union[datetime, int, None],
        ], ...
    ]
]:
    """
    Return a list of tuples of chunk bounds of size `batchsize`.

    Parameters
    ----------
    chunk_bounds: List[Tuple[Union[datetime, int, None], Union[datetime, int, None]]]
        A list of chunk_bounds (see `Pipe.get_chunk_bounds()`).

    batchsize: Optional[int], default None
        How many chunks to include in a batch. Defaults to `Pipe.get_num_workers()`.

    workers: Optional[int], default None
        If `batchsize` is `None`, use this as the desired number of workers.
        Passed to `Pipe.get_num_workers()`.

    Returns
    -------
    A list of tuples of chunk bound tuples.
    """
    from meerschaum.utils.misc import iterate_chunks
    
    if batchsize is None:
        batchsize = self.get_num_workers(workers=workers)

    return [
        tuple(
            _batch_chunk_bounds
            for _batch_chunk_bounds in batch
            if _batch_chunk_bounds is not None
        )
        for batch in iterate_chunks(chunk_bounds, batchsize)
        if batch
    ]


def parse_date_bounds(self, *dt_vals: Union[datetime, int, None]) -> Union[
    datetime,
    int,
    str,
    None,
    Tuple[Union[datetime, int, str, None]]
]:
    """
    Given a date bound (begin, end), coerce a timezone if necessary.
    """
    from meerschaum.utils.misc import is_int
    from meerschaum.utils.dtypes import coerce_timezone
    from meerschaum.utils.warnings import warn
    dateutil_parser = mrsm.attempt_import('dateutil.parser')

    def _parse_date_bound(dt_val):
        if dt_val is None:
            return None

        if isinstance(dt_val, int):
            return dt_val

        if dt_val == '':
            return ''

        if is_int(dt_val):
            return int(dt_val)

        if isinstance(dt_val, str):
            try:
                dt_val = dateutil_parser.parse(dt_val)
            except Exception as e:
                warn(f"Could not parse '{dt_val}' as datetime:\n{e}")
                return None

        dt_col = self.columns.get('datetime', None)
        dt_typ = str(self.dtypes.get(dt_col, 'datetime64[ns, UTC]'))
        if dt_typ == 'datetime':
            dt_typ = 'datetime64[ns, UTC]'
        return coerce_timezone(dt_val, strip_utc=('utc' not in dt_typ.lower()))

    bounds = tuple(_parse_date_bound(dt_val) for dt_val in dt_vals)
    if len(bounds) == 1:
        return bounds[0]
    return bounds
