#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Delete duplicate rows within a pipe's table.
"""

from __future__ import annotations
from datetime import datetime
from meerschaum.utils.typing import SuccessTuple, Any, Optional, Dict, Union


def deduplicate(
    self,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    chunk_interval: Union[datetime, int, None] = None,
    bounded: Optional[bool] = None,
    workers: Optional[int] = None,
    debug: bool = False,
    _use_instance_method: bool = True,
    **kwargs: Any
) -> SuccessTuple:
    """
    Call the Pipe's instance connector's `delete_duplicates` method to delete duplicate rows.

    Parameters
    ----------
    begin: Union[datetime, int, None], default None:
        If provided, only deduplicate rows newer than this datetime value.

    end: Union[datetime, int, None], default None:
        If provided, only deduplicate rows older than this datetime column (not including end).

    params: Optional[Dict[str, Any]], default None
        Restrict deduplication to this filter (for multiplexed data streams).
        See `meerschaum.utils.sql.build_where`.

    chunk_interval: Union[timedelta, int, None], default None
        If provided, use this for the chunk bounds.
        Defaults to the value set in `pipe.parameters['verify']['chunk_minutes']` (43200 — 30 days).

    bounded: Optional[bool], default None
        Only check outside the oldest and newest sync times if bounded is explicitly `False`.

    workers: Optional[int], default None
        If the instance connector is thread-safe, limit concurrenct syncs to this many threads.

    debug: bool, default False:
        Verbositity toggle.

    kwargs: Any
        All other keyword arguments are passed to
        `pipe.sync()`, `pipe.clear()`, and `pipe.get_data().

    Returns
    -------
    A `SuccessTuple` corresponding to whether all of the chunks were successfully deduplicated.
    """
    from meerschaum import _core
    from meerschaum.core.Pipe._data import _rust_context
    context = _rust_context(self, debug=debug)
    begin, end = self.parse_date_bounds(begin, end)
    return _core.pipe_deduplicate(
        self,
        context,
        begin=begin,
        end=end,
        params=params,
        chunk_interval=chunk_interval,
        bounded=bounded,
        use_instance_method=_use_instance_method,
        workers=self.get_num_workers(workers=workers),
        kwargs=kwargs,
        debug=debug,
    )
