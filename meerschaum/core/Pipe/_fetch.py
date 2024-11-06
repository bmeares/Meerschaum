#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for fetching new data into the Pipe
"""

from __future__ import annotations
from datetime import timedelta, datetime

import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Any, Union, SuccessTuple, Iterator, TYPE_CHECKING
from meerschaum.config import get_config
from meerschaum.utils.warnings import warn

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
    if 'fetch' not in dir(self.connector):
        warn(f"No `fetch()` function defined for connector '{self.connector}'")
        return None

    from meerschaum.connectors import custom_types, get_connector_plugin
    from meerschaum.utils.debug import dprint, _checkpoint
    from meerschaum.utils.misc import filter_arguments

    _chunk_hook = kw.pop('chunk_hook', None)
    kw['workers'] = self.get_num_workers(kw.get('workers', None))
    if sync_chunks and _chunk_hook is None:

        def _chunk_hook(chunk, **_kw) -> SuccessTuple:
            """
            Wrap `Pipe.sync()` with a custom chunk label prepended to the message.
            """
            from meerschaum.config._patch import apply_patch_to_config
            kwargs = apply_patch_to_config(kw, _kw)
            chunk_success, chunk_message = self.sync(chunk, **kwargs)
            chunk_label = self._get_chunk_label(chunk, self.columns.get('datetime', None))
            if chunk_label:
                chunk_message = '\n' + chunk_label + '\n' + chunk_message
            return chunk_success, chunk_message

    begin, end = self.parse_date_bounds(begin, end)

    with mrsm.Venv(get_connector_plugin(self.connector)):
        _args, _kwargs = filter_arguments(
            self.connector.fetch,
            self,
            begin=_determine_begin(
                self,
                begin,
                check_existing=check_existing,
                debug=debug,
            ),
            end=end,
            chunk_hook=_chunk_hook,
            debug=debug,
            **kw
        )
        df = self.connector.fetch(*_args, **_kwargs)
    return df


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
    default_backtrack_minutes = get_config('pipes', 'parameters', 'fetch', 'backtrack_minutes')
    configured_backtrack_minutes = self.parameters.get('fetch', {}).get('backtrack_minutes', None)
    backtrack_minutes = (
        configured_backtrack_minutes
        if configured_backtrack_minutes is not None
        else default_backtrack_minutes
    ) if check_existing else 0

    backtrack_interval = timedelta(minutes=backtrack_minutes)
    dt_col = self.columns.get('datetime', None)
    if dt_col is None:
        return backtrack_interval

    dt_dtype = self.dtypes.get(dt_col, 'datetime64[ns, UTC]')
    if 'int' in dt_dtype.lower():
        return backtrack_minutes

    return backtrack_interval


def _determine_begin(
    pipe: mrsm.Pipe,
    begin: Union[datetime, int, str, None] = '',
    check_existing: bool = True,
    debug: bool = False,
) -> Union[datetime, int, None]:
    """
    Apply the backtrack interval if `--begin` is not provided.

    Parameters
    ----------
    begin: Union[datetime, int, str, None], default ''
        The provided begin timestamp.

    check_existing: bool, default True
        If `False`, do not apply the backtrack interval.

    Returns
    -------
    A datetime (or int) value from which to fetch.
    Returns `None` if no begin may be determined.
    """
    if begin != '':
        return begin
    sync_time = pipe.get_sync_time(debug=debug)
    if sync_time is None:
        return sync_time
    backtrack_interval = pipe.get_backtrack_interval(check_existing=check_existing, debug=debug)
    if isinstance(sync_time, datetime) and isinstance(backtrack_interval, int):
        backtrack_interval = timedelta(minutes=backtrack_interval)
    try:
        return sync_time - backtrack_interval
    except Exception:
        warn(f"Unable to substract backtrack interval {backtrack_interval} from {sync_time}.")
    return sync_time
