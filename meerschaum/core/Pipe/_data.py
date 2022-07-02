#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Retrieve Pipes' data from instances.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any, Union

def get_data(
        self,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        params: Optional[Dict[str, Any]] = None,
        fresh: bool = False,
        debug: bool = False,
        **kw: Any
    ) -> Union['pd.DataFrame', None]:
    """
    Get a pipe's data from the instance connector.

    Parameters
    ----------
    begin: Optional[datetime.datetime], default None
        Lower bound datetime to begin searching for data (inclusive).
        Translates to a `WHERE` clause like `WHERE datetime >= begin`.
        Defaults to `None`.

    end: Optional[datetime.datetime], default None
        Upper bound datetime to stop searching for data (inclusive).
        Translates to a `WHERE` clause like `WHERE datetime < end`.
        Defaults to `None`.

    params: Optional[Dict[str, Any]], default None
        Filter the retrieved data by a dictionary of parameters.
        See `meerschaum.utils.sql.build_where` for more details. 

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
    kw.update({'begin': begin, 'end': end, 'params': params,})

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
    return self.enforce_dtypes(
        self.instance_connector.get_pipe_data(
            pipe = self,
            debug = debug,
            **kw
        ),
        debug = debug,
    )


def get_backtrack_data(
        self,
        backtrack_minutes: int = 0,
        begin: Optional['datetime.datetime'] = None,
        fresh: bool = False,
        debug : bool = False,
        **kw : Any
    ) -> Optional['pd.DataFrame']:
    """
    Get the most recent data from the instance connector as a Pandas DataFrame.

    Parameters
    ----------
    backtrack_minutes: int, default 0
        How many minutes from `begin` to select from.
        Defaults to 0. This may return a few rows due to a rounding quirk.
    begin: Optional[datetime.datetime], default None
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
    kw.update({'backtrack_minutes': backtrack_minutes, 'begin': begin,})

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
                    self.cache_pipe.get_backtrack_data(debug=debug, fresh=True, **kw),
                    debug = debug,
                )

    ### If `fresh` or the syncing failed, directly pull from the instance connector.
    return self.enforce_dtypes(
        self.instance_connector.get_backtrack_data(
            pipe = self,
            debug = debug,
            **kw
        ),
        debug = debug,
    )


def get_rowcount(
        self,
        begin: Optional['datetime.datetime'] = None,
        end: Optional['datetime.datetime'] = None,
        remote: bool = False,
        params: Optional[Dict[str, Any]] = None,
        debug: bool = False
    ) -> Union[int, None]:
    """
    Get a Pipe's instance or remote rowcount.

    Parameters
    ----------
    begin: Optional[datetime.datetime], default None
        Count rows where datetime > begin.

    end: Optional[datetime.datetime], default None
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
    connector = self.instance_connector if not remote else self.connector
    try:
        return connector.get_pipe_rowcount(
            self, begin=begin, end=end, remote=remote, params=params, debug=debug
        )
    except AttributeError as e:
        warn(e)
        if remote:
            return None
    warn(f"Failed to get a rowcount for {self}.")
    return None
