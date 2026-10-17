#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage the daemons behind jobs (`meerschaum.jobs.Job` is the way to run one).

The daemon functions are façades over the native jobs (`meerschaum-jobs`). Running an arbitrary
Python callable as a daemon (`Daemon(target=...)`, `run_daemon`, `daemon_action`, `daemon_entry`)
and the file-descriptor interception of v4 are removed in Meerschaum v5.
"""

from __future__ import annotations

import os

from meerschaum.utils.typing import List, Optional, Union
from meerschaum.utils.daemon.StdinFile import StdinFile
from meerschaum.utils.daemon.Daemon import Daemon
from meerschaum.utils.daemon.RotatingFile import RotatingFile
from meerschaum.utils.daemon._names import get_new_daemon_name


__all__ = (
    'get_daemons',
    'get_daemon_ids',
    'get_running_daemons',
    'get_stopped_daemons',
    'get_paused_daemons',
    'get_filtered_daemons',
    'get_new_daemon_name',
    'get_current_daemon',
    'running_in_daemon',
    'Daemon',
    'StdinFile',
    'RotatingFile',
)

_daemons = {}


def get_daemon_ids() -> List[str]:
    """
    Return the IDs of all daemons on disk.
    """
    from meerschaum.jobs import _native
    return _native.core().jobs_daemon_ids()


def get_daemons() -> List[Daemon]:
    """
    Return all existing Daemons, sorted by end time.
    """
    daemons = [Daemon(daemon_id=d_id) for d_id in get_daemon_ids()]
    statuses = {daemon: daemon.status for daemon in daemons}
    key_by_status = {'stopped': 'ended', 'paused': 'paused', 'running': 'began'}

    def _sorted(status: str) -> List[Daemon]:
        key = key_by_status[status]
        selected = [daemon for daemon in daemons if statuses[daemon] == status]
        return sorted(
            selected,
            key=lambda daemon: daemon.properties.get('process', {}).get(key, '9999'),
        )

    return _sorted('stopped') + _sorted('paused') + _sorted('running')


def get_running_daemons(daemons: Optional[List[Daemon]] = None) -> List[Daemon]:
    """
    Return a list of currently running daemons.
    """
    if daemons is None:
        daemons = get_daemons()
    return [d for d in daemons if d.status == 'running']


def get_paused_daemons(daemons: Optional[List[Daemon]] = None) -> List[Daemon]:
    """
    Return a list of active but paused daemons.
    """
    if daemons is None:
        daemons = get_daemons()
    return [d for d in daemons if d.status == 'paused']


def get_stopped_daemons(daemons: Optional[List[Daemon]] = None) -> List[Daemon]:
    """
    Return a list of stopped daemons.
    """
    if daemons is None:
        daemons = get_daemons()
    return [d for d in daemons if d.status == 'stopped']


def get_filtered_daemons(
    filter_list: Optional[List[str]] = None,
    warn: bool = False,
) -> List[Daemon]:
    """
    Return a list of `Daemons` filtered by a list of `daemon_ids`.
    Only `Daemons` that exist are returned.

    If `filter_list` is `None` or empty, return all `Daemons` (from `get_daemons()`).

    Parameters
    ----------
    filter_list: Optional[List[str]], default None
        List of `daemon_ids` to include. If `daemon_ids` is `None` or empty,
        return all `Daemons`.

    warn: bool, default False
        If `True`, raise warnings for non-existent `daemon_ids`.

    Returns
    -------
    A list of Daemon objects.
    """
    if not filter_list:
        daemons = get_daemons()
        return [d for d in daemons if not d.hidden]

    from meerschaum.utils.warnings import warn as _warn
    daemons = []
    for d_id in filter_list:
        try:
            d = Daemon(daemon_id=d_id)
            _exists = d.path.exists()
        except Exception:
            _exists = False
        if not _exists:
            if warn:
                _warn(f"Daemon '{d_id}' does not exist.", stack=False)
            continue
        daemons.append(d)
    return daemons


def running_in_daemon() -> bool:
    """
    Return whether the current thread is running in a Daemon context.
    """
    from meerschaum._internal.static import STATIC_CONFIG
    daemon_env_var = STATIC_CONFIG['environment']['daemon_id']
    return daemon_env_var in os.environ


def get_current_daemon() -> Union[Daemon, None]:
    """
    If running withing a daemon context, return the corresponding `Daemon`.
    Otherwise return `None`.
    """
    from meerschaum._internal.static import STATIC_CONFIG
    daemon_env_var = STATIC_CONFIG['environment']['daemon_id']
    daemon_id = os.environ.get(daemon_env_var, None)
    if daemon_id is None:
        return None

    if daemon_id in _daemons:
        return _daemons[daemon_id]
    try:
        _daemons[daemon_id] = Daemon(daemon_id=daemon_id)
    except Exception:
        return None
    return _daemons[daemon_id]
