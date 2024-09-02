#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Clear pipe data within a bounded or unbounded interval.
"""

from __future__ import annotations

from datetime import datetime
from meerschaum.utils.typing import SuccessTuple, Any, Optional, Dict


def clear(
    self,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Call the Pipe's instance connector's `clear_pipe` method.

    Parameters
    ----------
    begin: Optional[datetime], default None:
        If provided, only remove rows newer than this datetime value.

    end: Optional[datetime], default None:
        If provided, only remove rows older than this datetime column (not including end).

    params: Optional[Dict[str, Any]], default None
         See `meerschaum.utils.sql.build_where`.

    debug: bool, default False:
        Verbositity toggle.

    Returns
    -------
    A `SuccessTuple` corresponding to whether this procedure completed successfully.

    Examples
    --------
    >>> pipe = mrsm.Pipe('test', 'test', columns={'datetime': 'dt'}, instance='sql:local')
    >>> pipe.sync({'dt': [datetime(2020, 1, 1, 0, 0)]})
    >>> pipe.sync({'dt': [datetime(2021, 1, 1, 0, 0)]})
    >>> pipe.sync({'dt': [datetime(2022, 1, 1, 0, 0)]})
    >>> 
    >>> pipe.clear(begin=datetime(2021, 1, 1, 0, 0))
    >>> pipe.get_data()
              dt
    0 2020-01-01

    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    if self.cache_pipe is not None:
        success, msg = self.cache_pipe.clear(
            begin = begin,
            end = end,
            params = params,
            debug = debug,
            **kwargs
        )
        if not success:
            warn(msg)

    with Venv(get_connector_plugin(self.instance_connector)):
        return self.instance_connector.clear_pipe(
            self,
            begin = begin,
            end = end,
            params = params,
            debug = debug,
            **kwargs
        )
