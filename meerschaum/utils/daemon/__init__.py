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
    NOTE: The parent process WILL EXIT when this function is called!
    """
    from meerschaum.actions._entry import _entry as entry
    success_tuple = run_daemon(
        entry,
        sysargs,
        label=(' '.join(sysargs) if sysargs else None),
        keep_daemon_output = (
            '--keep-daemon-output' in sysargs
                or '--skip-daemon-cleanup' in sysargs
                or '--keep-logs' in sysargs
        )
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

    kw['daemon'] = True
    kw['shell'] = False

    sysargs = parse_dict_to_sysargs(kw)
    rc = run_python_package('meerschaum', sysargs, debug=kw.get('debug', False))
    msg = "Success" if rc == 0 else f"Daemon for '{' '.join(sysargs)}' returned code: {rc}"
    return rc == 0, msg

def run_daemon(
        func : Callable[[Any], Any],
        *args,
        keep_daemon_output : bool = False,
        allow_dirty_run : bool = False,
        label : Optional[str] = None,
        **kw
    ) -> Any:
    """
    Execute a function as a daemon.
    NOTE: This WILL EXIT the parent process!
    """
    daemon = Daemon(func, target_args=args, target_kw=kw, label=label)
    return daemon.run(keep_daemon_output=keep_daemon_output, allow_dirty_run=allow_dirty_run)

def get_daemons() -> List[Daemon]:
    """
    Return a list of daemons.
    """
    return [Daemon(daemon_id=d_id) for d_id in os.listdir(DAEMON_RESOURCES_PATH)]

