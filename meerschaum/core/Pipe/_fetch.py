#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for fetching new data into the Pipe
"""

from __future__ import annotations
from datetime import timedelta, datetime
from meerschaum.utils.typing import Optional, Any, Union
from meerschaum.config import get_config

def fetch(
        self,
        begin: Optional[datetime.datetime, str] = '',
        end: Optional[datetime.datetime] = None,
        sync_chunks: bool = False,
        deactivate_plugin_venv: bool = True,
        debug: bool = False,
        **kw: Any
    ) -> 'pd.DataFrame or None':
    """
    Fetch a Pipe's latest data from its connector.

    Parameters
    ----------
    begin: Optional[datetime.datetime, str], default '':
        If provided, only fetch data newer than or equal to `begin`.

    end: Optional[datetime.datetime], default None:
        If provided, only fetch data older than or equal to `end`.

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
        from meerschaum.utils.warnings import warn
        warn(f"No `fetch()` function defined for connector '{self.connector}'")
        return None

    from meerschaum.connectors import custom_types
    from meerschaum.utils.debug import dprint, _checkpoint
    if (
        self.connector.type == 'plugin'
        or
        self.connector.type in custom_types
    ):
        from meerschaum.plugins import Plugin
        from meerschaum.utils.packages import activate_venv, deactivate_venv
        plugin_name = (
            self.connector.label if self.connector.type == 'plugin'
            else self.connector.__module__.replace('plugins.', '').split('.')[0]
        )
        connector_plugin = Plugin(plugin_name)
        connector_plugin.activate_venv(debug=debug)
    
    _chunk_hook = kw.pop('chunk_hook', None)
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

    workers = kw.get('workers', None)
    if workers is None and not getattr(self.instance_connector, 'IS_THREAD_SAFE', False):
        workers = 1
    kw['workers'] = workers

    df = self.connector.fetch(
        self,
        begin = _determine_begin(self, begin, debug=debug),
        end = end,
        chunk_hook = _chunk_hook,
        debug = debug,
        **kw
    )
    return df


def get_backtrack_interval(self, debug: bool = False) -> Union[timedelta, int]:
    """
    Get the chunk interval to use for this pipe.

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
    )

    backtrack_interval = timedelta(minutes=backtrack_minutes)
    dt_col = self.columns.get('datetime', None)
    if dt_col is None:
        return backtrack_interval

    dt_dtype = self.dtypes.get(dt_col, 'datetime64[ns]')
    if 'int' in dt_dtype.lower():
        return backtrack_minutes

    return backtrack_interval


def _determine_begin(
        pipe: meerschaum.Pipe,
        begin: Union[datetime, int, str] = '',
        debug: bool = False,
    ) -> Union[datetime, int, None]:
    """
    Apply the backtrack interval if `--begin` is not provided.
    """
    if begin != '':
        return begin
    sync_time = pipe.get_sync_time(debug=debug)
    if sync_time is None:
        return sync_time
    backtrack_interval = pipe.get_backtrack_interval(debug=debug)
    if isinstance(sync_time, datetime) and isinstance(backtrack_interval, int):
        backtrack_interval = timedelta(minutes=backtrack_interval)
    try:
        return sync_time - backtrack_interval
    except Exception as e:
        warn(f"Unable to substract backtrack interval {backtrack_interval} from {sync_time}.")
        return sync_time
