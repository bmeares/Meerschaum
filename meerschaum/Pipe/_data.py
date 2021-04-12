#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for getting Pipe data
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any

def get_data(
        self,
        begin : Optional[datetime.datetime] = None,
        end : Optional[datetime.datetime] = None,
        params : Optional[Dict[str, Any]] = None,
        refresh : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> Optional[pandas.DataFrame]:
    """
    Get a pipe's data from the instance connector.

    :param begin:
        Lower bound datetime to begin searching for data (inclusive).
        Translates to a `WHERE` clause like `WHERE datetime >= begin`.
        Defaults to `None`.

    :param end:
        Upper bound datetime to stop searching for data (inclusive).
        Translates to a `WHERE` clause like `WHERE datetime <= end`.
        Defaults to `None`.

    :param params:
        Filter the retrieved data by a dictionary of parameters.
        E.g. to retrieve data for only certain values of `id`,
        the `params` dictionary would look like the following:
        
        ```
        >>> params = {
        ...   'id' : [1, 2, 3],
        ... }
        >>> 
        ```

    :param refresh:
        If True, skip local cache and directly query the instance connector.
        Currently has no effect (until caching features are merged into the stable release).
        Defaults to `True`.

    :param debug:
        Verbosity toggle.
        Defaults to `False`.
    """
    if refresh or True: ### TODO remove `or True`
        self._data = self.instance_connector.get_pipe_data(
            pipe = self,
            begin = begin,
            end = end,
            params = params,
            debug = debug,
            **kw
        )
    ### TODO caching / sync logic
    return self._data

def get_backtrack_data(
        self,
        backtrack_minutes : int = 0,
        begin : 'datetime.datetime' = None,
        debug : bool = False,
        **kw : Any
    ) -> Optional['pd.DataFrame']:
    """
    Get the most recent data from the instance connector as a Pandas DataFrame.

    :param backtrack_minutes:
        How many minutes from `begin` to select from.
        Defaults to 0. This may return a few rows due to a rounding quirk.

    :param begin:
        The starting point from which to search for data.
        If begin is None (default), use the most recent observed datetime
        (AKA sync_time).

        E.g. begin = 02:00

        ```

        Search this region.           Ignore this, even if there's data.
        /  /  /  /  /  /  /  /  /  |
        -----|----------|----------|----------|----------|----------|
           00:00      01:00      02:00      03:00      04:00      05:00

        ```

    :param debug: Verbosity toggle.
    """
    return self.instance_connector.get_backtrack_data(
        pipe = self,
        begin = begin,
        backtrack_minutes = backtrack_minutes,
        debug = debug,
        **kw
    )

def get_rowcount(
        self,
        begin : Optional['datetime.datetime'] = None,
        end : Optional['datetime.datetime'] = None,
        remote : bool = False,
        params : Optional[Dict[str, Any]] = None,
        debug : bool = False
    ) -> Optional[int]:
    """
    Get a Pipe's cached or remote rowcount.

    :param begin:
        Count rows where datetime > begin.

    :param end:
        Count rows where datetime <= end.

    :param remote:
        Count rows from a pipe's remote source.
        NOTE: This is experimental!

    :param debug: Verbosity toggle.
    """
    connector = self.instance_connector if not remote else self.connector
    try:
        return connector.get_pipe_rowcount(
            self, begin=begin, end=end, remote=remote, params=params, debug=debug
        )
    except AttributeError:
        if remote:
            return None
    from meerschaum.utils.warnings import warn
    warn(f"Failed to get a rowcount for pipe '{self}'.")
    return None
