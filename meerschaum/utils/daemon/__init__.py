#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage Daemons via the `Daemon` class.
"""

from __future__ import annotations
import os, pathlib, shutil, json, datetime, threading, shlex
from meerschaum.utils.typing import SuccessTuple, List, Optional, Callable, Any, Dict
from meerschaum.config._paths import DAEMON_RESOURCES_PATH
from meerschaum.utils.daemon.Daemon import Daemon
from meerschaum.utils.daemon.RotatingFile import RotatingFile


def daemon_entry(sysargs: Optional[List[str]] = None) -> SuccessTuple:
    """Parse sysargs and execute a Meerschaum action as a daemon.

    Parameters
    ----------
    sysargs: Optional[List[str]], default None
        The command line arguments used in a Meerschaum action.

    Returns
    -------
    A SuccessTuple.
    """
    from meerschaum._internal.entry import entry
    _args = {}
    if '--name' in sysargs or '--job-name' in sysargs:
        from meerschaum._internal.arguments._parse_arguments import parse_arguments
        _args = parse_arguments(sysargs)
    filtered_sysargs = [arg for arg in sysargs if arg not in ('-d', '--daemon')]
    try:
        label = shlex.join(filtered_sysargs) if sysargs else None
    except Exception as e:
        label = ' '.join(filtered_sysargs) if sysargs else None

    name = _args.get('name', None)
    daemon = None
    if name:
        try:
            daemon = Daemon(daemon_id=name)
        except Exception as e:
            daemon = None

    if daemon is not None:
        existing_sysargs = daemon.properties['target']['args'][0]
        existing_kwargs = parse_arguments(existing_sysargs)

        ### Remove sysargs because flags are aliased.
        _ = _args.pop('daemon', None)
        _ = _args.pop('sysargs', None)
        _ = _args.pop('filtered_sysargs', None)
        debug = _args.pop('debug', None)
        _args['sub_args'] = sorted(_args.get('sub_args', []))
        _ = existing_kwargs.pop('daemon', None)
        _ = existing_kwargs.pop('sysargs', None)
        _ = existing_kwargs.pop('filtered_sysargs', None)
        _ = existing_kwargs.pop('debug', None)
        existing_kwargs['sub_args'] = sorted(existing_kwargs.get('sub_args', []))

        ### Only run if the kwargs equal or no actions are provided.
        if existing_kwargs == _args or not _args.get('action', []):
            return daemon.run(
                debug = debug,
                allow_dirty_run = True,
            )

    success_tuple = run_daemon(
        entry,
        filtered_sysargs,
        daemon_id = _args.get('name', None) if _args else None,
        label = label,
        keep_daemon_output = ('--rm' not in sysargs)
    )
    if not isinstance(success_tuple, tuple):
        success_tuple = False, str(success_tuple)
    return success_tuple


def daemon_action(**kw) -> SuccessTuple:
    """Execute a Meerschaum action as a daemon."""
    from meerschaum.utils.packages import run_python_package
    from meerschaum.utils.threading import Thread
    from meerschaum._internal.arguments._parse_arguments import parse_dict_to_sysargs
    from meerschaum.actions import get_action

    kw['daemon'] = True
    kw['shell'] = False

    action = kw.get('action', None)
    if action and get_action(action) is None:
        if not kw.get('allow_shell_job') and not kw.get('force'):
            return False, (
                f"Action '{action}' isn't recognized.\n\n"
                + "  Include `--allow-shell-job`, `--force`, or `-f`\n  " 
                + "to enable shell commands to run as Meerschaum jobs."
            )

    sysargs = parse_dict_to_sysargs(kw)
    rc = run_python_package('meerschaum', sysargs, venv=None, debug=False)
    msg = "Success" if rc == 0 else f"Daemon returned code: {rc}"
    return rc == 0, msg


def run_daemon(
        func: Callable[[Any], Any],
        *args,
        daemon_id: Optional[str] = None,
        keep_daemon_output: bool = False,
        allow_dirty_run: bool = False,
        label: Optional[str] = None,
        **kw
    ) -> Any:
    """Execute a function as a daemon."""
    daemon = Daemon(
        func,
        daemon_id = daemon_id,
        target_args = [arg for arg in args],
        target_kw = kw,
        label = label,
    )
    return daemon.run(
        keep_daemon_output = keep_daemon_output,
        allow_dirty_run = allow_dirty_run,
    )


def get_daemons() -> List[Daemon]:
    """
    Return all existing Daemons, sorted by end time.
    """
    daemons = [Daemon(daemon_id=d_id) for d_id in get_daemon_ids()]
    daemons_status = {daemon: daemon.status for daemon in daemons}
    running_daemons = {
        daemon: daemons_status[daemon]
        for daemon in daemons
        if daemons_status[daemon] == 'running'
    }
    paused_daemons = {
        daemon: daemons_status[daemon]
        for daemon in daemons
        if daemons_status[daemon] == 'paused'
    }
    stopped_daemons = {
        daemon: daemons_status[daemon]
        for daemon in daemons
        if daemons_status[daemon] == 'stopped'
    }
    daemons_began = {
        daemon: daemon.properties.get('process', {}).get('began', '9999')
        for daemon in daemons
    }
    daemons_paused = {
        daemon: daemon.properties.get('process', {}).get('paused', '9999')
        for daemon in daemons
    }
    daemons_ended = {
        daemon: daemon.properties.get('process', {}).get('ended', '9999')
        for daemon in daemons
    }
    sorted_stopped_daemons = [
        daemon
        for daemon in sorted(stopped_daemons, key=lambda x: daemons_ended[x])
    ]
    sorted_paused_daemons = [
        daemon
        for daemon in sorted(paused_daemons, key=lambda x: daemons_paused[x])
    ]
    sorted_running_daemons = [
        daemon
        for daemon in sorted(running_daemons, key=lambda x: daemons_began[x])
    ]
    return sorted_stopped_daemons + sorted_paused_daemons + sorted_running_daemons


def get_daemon_ids() -> List[str]:
    """
    Return the IDs of all daemons on disk.
    """
    return sorted(os.listdir(DAEMON_RESOURCES_PATH))


def get_running_daemons(daemons: Optional[List[Daemon]] = None) -> List[Daemon]:
    """
    Return a list of currently running daemons.
    """
    if daemons is None:
        daemons = get_daemons()
    return [
        d
        for d in daemons
        if d.status == 'running'
    ]


def get_paused_daemons(daemons: Optional[List[Daemon]] = None) -> List[Daemon]:
    """
    Return a list of active but paused daemons.
    """
    if daemons is None:
        daemons = get_daemons()
    return [
        d
        for d in daemons
        if d.status == 'paused'
    ]


def get_stopped_daemons(daemons: Optional[List[Daemon]] = None) -> List[Daemon]:
    """
    Return a list of stopped daemons.
    """
    if daemons is None:
        daemons = get_daemons()

    return [
        d
        for d in daemons
        if d.status == 'stopped'
    ]


def get_filtered_daemons(
        filter_list: Optional[List[str]] = None,
        warn: bool = False,
    ) -> List[Daemon]:
    """Return a list of `Daemons` filtered by a list of `daemon_ids`.
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
        except Exception as e:
            _exists = False
        if not _exists:
            if warn:
                _warn(f"Daemon '{d_id}' does not exist.", stack=False)
            continue
        if d.hidden:
            pass
        daemons.append(d)
    return daemons
