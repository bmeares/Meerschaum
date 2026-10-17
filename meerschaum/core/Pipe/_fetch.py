#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for fetching new data into the Pipe
"""

from __future__ import annotations
from datetime import timedelta, datetime

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Union, Iterator, TYPE_CHECKING

if TYPE_CHECKING:
    pd = mrsm.attempt_import('pandas')


def fetch(
    self,
    begin: Union[datetime, int, str, None] = '',
    end: Union[datetime, int, None] = None,
    check_existing: bool = True,
    sync_chunks: bool = False,
    debug: bool = False,
    **kw: Any
) -> Union['pd.DataFrame', Iterator['pd.DataFrame'], None]:
    """
    Fetch a Pipe's latest data from its connector.

    Parameters
    ----------
    begin: Union[datetime, str, None], default '':
        If provided, only fetch data newer than or equal to `begin`.

    end: Optional[datetime], default None:
        If provided, only fetch data older than or equal to `end`.

    check_existing: bool, default True
        If `False`, do not apply the backtrack interval.

    sync_chunks: bool, default False
        If `True` and the pipe's connector is of type `'sql'`, begin syncing chunks while fetching
        loads chunks into memory.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `pd.DataFrame` of the newest unseen data.

    """
    from meerschaum import _core
    from meerschaum.core.Pipe._data import _rust_context
    try:
        context = _rust_context(self, debug=debug)
    except Exception as e:
        ### v4 checked the source before it touched the instance: a source without `fetch()`
        ### answers `None` with a warning even when the instance cannot be parsed.
        if type(e) is Exception and 'fetch' not in dir(self.connector):
            from meerschaum.utils.warnings import warn
            warn(f"No `fetch()` function defined for connector '{self.connector}'")
            return None
        raise
    begin, end = self.parse_date_bounds(begin, end)
    fetched = _core.pipe_fetch(
        self,
        context,
        begin=begin,
        end=end,
        check_existing=check_existing,
        params=kw.get('params', None),
        kwargs={
            key: val
            for key, val in kw.items()
            if key not in ('params', 'chunk_hook')
        },
        sync_chunks=sync_chunks,
        chunk_hook=kw.get('chunk_hook', None),
        debug=debug,
    )
    ### A SQL source's rows come in chunks, as a generator.
    return (chunk for chunk in fetched) if hasattr(fetched, '__next__') else fetched


def get_backtrack_interval(
    self,
    check_existing: bool = True,
    debug: bool = False,
) -> Union[timedelta, int]:
    """
    Get the chunk interval to use for this pipe.

    Parameters
    ----------
    check_existing: bool, default True
        If `False`, return a backtrack_interval of 0 minutes.

    Returns
    -------
    The backtrack interval (`timedelta` or `int`) to use with this pipe's `datetime` axis.
    """
    from meerschaum import _core
    return _core.pipe_get_backtrack_interval(self, check_existing, self.dtypes)


