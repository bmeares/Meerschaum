#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Retrieve Pipes' data from instances.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from meerschaum.utils.typing import Optional, Dict, Any, Union, Generator
from meerschaum.config import get_config

def get_data(
        self,
        begin: Union[datetime, int, None] = None,
        end: Union[datetime, int, None] = None,
        params: Optional[Dict[str, Any]] = None,
        as_iterator: bool = False,
        as_chunks: bool = False,
        chunk_interval: Union[timedelta, int, None] = None,
        fresh: bool = False,
        debug: bool = False,
        **kw: Any
    ) -> Union['pd.DataFrame', Generator['pd.DataFrame'], None]:
    """
    Get a pipe's data from the instance connector.

    Parameters
    ----------
    begin: Union[datetime, int, None], default None
        Lower bound datetime to begin searching for data (inclusive).
        Translates to a `WHERE` clause like `WHERE datetime >= begin`.
        Defaults to `None`.

    end: Union[datetime, int, None], default None
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

    chunk_interval: Union[timedelta, int, None], default None
        If `as_iterator`, then return chunks with `begin` and `end` separated by this interval.
        This may be set under `pipe.parameters['chunk_minutes']`.
        By default, use a timedelta of 1440 minutes (1 day).
        If `chunk_interval` is an integer and the `datetime` axis a timestamp,
        the use a timedelta with the number of minutes configured to this value.
        If the `datetime` axis is an integer, default to the configured chunksize.
        If `chunk_interval` is a `timedelta` and the `datetime` axis an integer,
        use the number of minutes in the `timedelta`.

    fresh: bool, default True
        If `True`, skip local cache and directly query the instance connector.
        Defaults to `True`.

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
    from meerschaum.utils.misc import iterate_chunks
    kw.update({'begin': begin, 'end': end, 'params': params,})

    as_iterator = as_iterator or as_chunks

    if as_iterator or as_chunks:
        return self._get_data_as_iterator(
            begin = begin,
            end = end,
            params = params,
            chunk_interval = chunk_interval,
            fresh = fresh,
            debug = debug,
        )

    if not self.exists(debug=debug):
        return None
       
    if self.cache_pipe is not None:
        if not fresh:
            _sync_cache_tuple = self.cache_pipe.sync(debug=debug, **kw)
            if not _sync_cache_tuple[0]:
                warn(f"Failed to sync cache for {self}:\n" + _sync_cache_tuple[1])
                fresh = True
            else: ### Successfully synced cache.
                return self.enforce_dtypes(
                    self.cache_pipe.get_data(debug=debug, fresh=True, **kw),
                    debug = debug,
                )

    ### If `fresh` or the syncing failed, directly pull from the instance connector.
    with Venv(get_connector_plugin(self.instance_connector)):
        return self.enforce_dtypes(
            self.instance_connector.get_pipe_data(
                pipe = self,
                debug = debug,
                **kw
            ),
            debug = debug,
        )


def _get_data_as_iterator(
        self,
        begin: Optional[datetime] = None,
        end: Optional[datetime] = None,
        params: Optional[Dict[str, Any]] = None,
        chunk_interval: Union[timedelta, int, None] = None,
        fresh: bool = False,
        debug: bool = False,
        **kw: Any
    ) -> Generator['pd.DataFrame']:
    """
    Return a pipe's data as a generator.
    """
    from meerschaum.utils.misc import round_time
    parse_begin = isinstance(begin, str)
    parse_end = isinstance(end, str)
    if parse_begin or parse_end:
        from meerschaum.utils.packages import attempt_import
        dateutil_parser = attempt_import('dateutil.parser')
    if parse_begin:
        begin = dateutil_parser.parse(begin)
    if parse_end:
        end = dateutil_parser.parse(end)

    if not self.exists(debug=debug):
        return

    _ = kw.pop('as_chunks', None)
    _ = kw.pop('as_iterator', None)
    min_dt = (
        begin if begin is not None
        else self.get_sync_time(round_down=False, newest=False, params=params, debug=debug)
    )
    max_dt = (
        end if end is not None
        else self.get_sync_time(round_down=False, newest=True, params=params, debug=debug)
    )

    ### We want to search just past the maximum value.
    if end is None:
        if isinstance(max_dt, int):
            max_dt += 1
        elif isinstance(max_dt, datetime):
            max_dt = round_time(max_dt + timedelta(minutes=1))

    if chunk_interval is None:
        chunk_interval = self.get_chunk_interval(debug=debug)
    elif isinstance(chunk_interval, int) and isinstance(min_dt, datetime):
        chunk_interval = timedelta(minutes=1)
    elif isinstance(chunk_interval, timedelta) and isinstance(min_dt, int):
        chunk_interval = int(chunk_interval.total_seconds() / 60)

    ### If we can't determine bounds
    ### or if chunk_interval exceeds the max,
    ### return a single chunk.
    if (
        (min_dt is None and max_dt is None)
        or
        (min_dt + chunk_interval) > max_dt
    ):
        yield self.get_data(
            begin = begin,
            end = end,
            params = params,
            fresh = fresh,
            debug = debug,
        )
        return

    chunk_begin = min_dt
    chunk_end = min_dt + chunk_interval
    while chunk_end < max_dt:
        yield self.get_data(
            begin = chunk_begin,
            end = chunk_end,
            params = params,
            fresh = fresh,
            debug = debug,
        )
        chunk_begin = chunk_end
        chunk_end += chunk_interval

    if chunk_begin <= max_dt:
        yield self.get_data(
            begin = chunk_begin,
            end = max_dt,
            params = params,
            fresh = fresh,
            debug = debug,
        )


def get_backtrack_data(
        self,
        backtrack_minutes: int = 0,
        begin: Optional[datetime] = None,
        params: Optional[Dict[str, Any]] = None,
        fresh: bool = False,
        debug: bool = False,
        **kw: Any
    ) -> Optional['pd.DataFrame']:
    """
    Get the most recent data from the instance connector as a Pandas DataFrame.

    Parameters
    ----------
    backtrack_minutes: int, default 0
        How many minutes from `begin` to select from.
        Defaults to 0. This may return a few rows due to a rounding quirk.

    begin: Optional[datetime], default None
        The starting point to search for data.
        If begin is `None` (default), use the most recent observed datetime
        (AKA sync_time).

    params: Optional[Dict[str, Any]], default None
        The standard Meerschaum `params` query dictionary.
        
        
    ```
    E.g. begin = 02:00

    Search this region.           Ignore this, even if there's data.
    /  /  /  /  /  /  /  /  /  |
    -----|----------|----------|----------|----------|----------|
    00:00      01:00      02:00      03:00      04:00      05:00

    ```

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

    if self.cache_pipe is not None:
        if not fresh:
            _sync_cache_tuple = self.cache_pipe.sync(begin=begin, params=params, debug=debug, **kw)
            if not _sync_cache_tuple[0]:
                warn(f"Failed to sync cache for {self}:\n" + _sync_cache_tuple[1])
                fresh = True
            else: ### Successfully synced cache.
                return self.enforce_dtypes(
                    self.cache_pipe.get_backtrack_data(
                        fresh = True,
                        begin = begin,
                        backtrack_minutes = backtrack_minutes,
                        params = params,
                        debug = deubg,
                        **kw
                    ),
                    debug = debug,
                )

    if hasattr(self.instance_connector, 'get_backtrack_data'):
        with Venv(get_connector_plugin(self.instance_connector)):
            return self.enforce_dtypes(
                self.instance_connector.get_backtrack_data(
                    pipe = self,
                    begin = begin,
                    backtrack_minutes = backtrack_minutes,
                    params = params,
                    debug = debug,
                    **kw
                ),
                debug = debug,
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
        begin = begin,
        params = params,
        debug = debug,
        **kw
    )


def get_rowcount(
        self,
        begin: Optional[datetime] = None,
        end: Optional['datetime'] = None,
        remote: bool = False,
        params: Optional[Dict[str, Any]] = None,
        debug: bool = False
    ) -> Union[int, None]:
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
    `None` is returned if the pipe does not exist.

    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    connector = self.instance_connector if not remote else self.connector
    try:
        with Venv(get_connector_plugin(connector)):
            return connector.get_pipe_rowcount(
                self, begin=begin, end=end, remote=remote, params=params, debug=debug
            )
    except AttributeError as e:
        warn(e)
        if remote:
            return None
    warn(f"Failed to get a rowcount for {self}.")
    return None


def get_chunk_interval(
        self,
        debug: bool = False,
    ) -> Union[timedelta, int]:
    """
    Get the chunk interval to use for this pipe.
    """
    default_chunk_minutes = get_config('pipes', 'parameters', 'chunk_minutes')
    configured_chunk_minutes = self.parameters.get('chunk_minutes', None)
    chunk_minutes = configured_chunk_minutes or default_chunk_minutes

    dt_col = self.columns.get('datetime', None)
    if dt_col is None:
        return timedelta(minutes=chunk_minutes)

    dt_dtype = self.dtypes.get(dt_col, 'datetime64[ns]')
    if 'datetime' in dt_dtype.lower():
        return timedelta(minutes=chunk_minutes)

    return chunk_minutes
