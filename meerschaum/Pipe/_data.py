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
        refresh : bool = False,
        params : Optional[Dict[str, Any]] = None,
        debug : bool = False
    ) -> Optional[pandas.DataFrame]:
    """
    Get data from the instance connector.
    """
    if refresh or True: ### TODO remove `or True`
        self._data = self.instance_connector.get_pipe_data(
            pipe = self,
            begin = begin,
            end = end,
            params = params,
            debug = debug
        )
    ### TODO caching / sync logic
    return self._data

def get_backtrack_data(
        self,
        backtrack_minutes : int = 0,
        begin : 'datetime.datetime' = None,
        debug : bool = False
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
        debug = debug
    )

### NOTE: removed this
#  @property
#  def data(
        #  self
    #  ) -> 'pd.DataFrame':
    #  return self.get_data()

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
    connector = self.instance_connector
    if remote: connector = self.connector
    return connector.get_pipe_rowcount(self, begin=begin, end=end, remote=remote, params=params, debug=debug)
