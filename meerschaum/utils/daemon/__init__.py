#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Daemonize processes via daemoniker.
"""

from __future__ import annotations
import os, pathlib, shutil, json, datetime, threading
from meerschaum.utils.typing import SuccessTuple, List, Optional, Callable, Any, Dict
from meerschaum.config._paths import DAEMON_RESOURCES_PATH
from meerschaum.utils.daemon.Daemon import Daemon

def daemon_entry(sysargs : Optional[List[str]] = None) -> SuccessTuple:
    """
    Parse sysargs and execute a Meerschaum action as a daemon.
    """
    from meerschaum.actions._entry import _entry as entry
    _args = None
    if '--name' in sysargs or '--job-name' in sysargs:
        from meerschaum.actions.arguments._parse_arguments import parse_arguments
        _args = parse_arguments(sysargs)
    success_tuple = run_daemon(
        entry,
        sysargs,
        daemon_id = _args.get('name', None) if _args else None,
        label = (' '.join(sysargs) if sysargs else None),
        keep_daemon_output = ('--rm' not in sysargs)
    )
    if not isinstance(success_tuple, tuple):
        success_tuple = False, str(success_tuple)
    return success_tuple

def daemon_action(**kw) -> SuccessTuple:
    """
    Execute a Meerschaum action as a daemon.
    """
    from meerschaum.utils.packages import run_python_package
    from meerschaum.utils.threading import Thread
    from meerschaum.actions.arguments._parse_arguments import parse_dict_to_sysargs
    from meerschaum.actions import actions

    kw['daemon'] = True
    kw['shell'] = False

    if kw.get('action', None) and kw.get('action')[0] not in actions:
        if not kw.get('allow_shell_job'):
            return False, (
                f"Action '{kw.get('action')[0]}' isn't recognized.\n\n"
                + "Pass `--allow-shell-job` to enable shell commands to run as Meerschaum jobs."
            )

    sysargs = parse_dict_to_sysargs(kw)
    rc = run_python_package('meerschaum', sysargs, debug=kw.get('debug', False))
    msg = "Success" if rc == 0 else f"Daemon for '{' '.join(sysargs)}' returned code: {rc}"
    return rc == 0, msg

def run_daemon(
        func : Callable[[Any], Any],
        *args,
        daemon_id : Optional[str] = None,
        keep_daemon_output : bool = False,
        allow_dirty_run : bool = False,
        label : Optional[str] = None,
        **kw
    ) -> Any:
    """
    Execute a function as a daemon.
    """
    daemon = Daemon(func, daemon_id=daemon_id, target_args=args, target_kw=kw, label=label)
    return daemon.run(
        keep_daemon_output = keep_daemon_output,
        allow_dirty_run = allow_dirty_run
    )

def get_daemons() -> List[Daemon]:
    """
    Return a list of daemons.
    """
    return [Daemon(daemon_id=d_id) for d_id in get_daemon_ids()]

def get_daemon_ids() -> List[str]:
    """
    Return a list of daemon IDs.
    """
    return os.listdir(DAEMON_RESOURCES_PATH)

def get_running_daemons(daemons : Optional[List[Daemon]] = None) -> List[Daemon]:
    """
    Return a list of currently running daemons.
    """
    if daemons is None:
        daemons = get_daemons()
    return [
        d for d in daemons if d.pid_path.exists()
    ]

def get_stopped_daemons(
        daemons : Optional[List[Daemon]] = None,
        running_daemons : Optional[List[Daemon]] = None,
    ) -> List[Daemon]:
    """
    Return a list of stopped daemons.
    """
    if daemons is None:
        daemons = get_daemons()
    if running_daemons is None:
        running_daemons = get_running_daemons(daemons)

    return [d for d in daemons if d not in running_daemons]

def get_filtered_daemons(
        filter_list : Optional[List[str]] = None,
        warn : bool = False,
    ) -> List[Daemon]:
    """
    Return a list of `Daemons` filtered by a list of `daemon_ids`.
    Only `Daemons` that exist are returned.

    If `filter_list` is `None` or empty, return all `Daemons` (from `get_daemons()`).

    :param filter_list:
        List of `daemon_ids` to include. If `daemon_ids` is `None` or empty,
        return all `Daemons`.

    :param warn:
        If `True`, raise warnings for non-existent `daemon_ids`.
        Defaults to `True`.
    """
    if not filter_list:
        return get_daemons()
    from meerschaum.utils.warnings import warn as _warn
    daemons = []
    for d in [Daemon(daemon_id=d_id) for d_id in filter_list]:
        if not d.path.exists():
            if warn:
                _warn(f"Daemon '{d.daemon_id}' does not exist.", stack=False)
            continue
        daemons.append(d)
    return daemons
