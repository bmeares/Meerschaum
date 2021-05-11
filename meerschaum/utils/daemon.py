#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Daemonize processes via daemoniker.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, List, Optional

def daemon_entry(sysargs : Optional[List[str]] = None) -> SuccessTuple:
    """
    Run a Meerschaum action as a daemon.
    """
    import uuid, os
    from meerschaum.actions._entry import _entry as entry
    from meerschaum.config._paths import DAEMON_RESOURCES_PATH
    from meerschaum.utils.packages import attempt_import
    daemoniker = attempt_import('daemoniker')

    daemon_id = str(uuid.uuid4())
    daemon_path = DAEMON_RESOURCES_PATH / daemon_id
    daemon_path.mkdir(parents=True, exist_ok=True)

    pid_path = daemon_path / f'{daemon_id}.pid'
    stdout_path = daemon_path / f'stdout.txt'
    stderr_path = daemon_path / f'stderr.txt'

    with daemoniker.Daemonizer() as (is_setup, daemonizer):
        is_parent, _daemon_id, _sysargs = daemonizer(
            str(pid_path),
            daemon_id,
            sysargs,
            stdout_goto = str(stdout_path),
            stderr_goto = str(stderr_path),
            strip_cmd_args = True
        )

    sighandler = daemoniker.SignalHandler1(str(pid_path))
    sighandler.start()

    try:
        success_tuple = entry(_sysargs)
    except Exception as e:
        success_tuple = False, str(e)

    if (not '--keep-daemon-output' in sysargs) and (not '--skip-daemon-cleanup' in sysargs):
        _cleanup(_daemon_id)
    return success_tuple

def _cleanup(daemon_id : str):
    import shutil
    from meerschaum.config._paths import DAEMON_RESOURCES_PATH
    daemon_path = DAEMON_RESOURCES_PATH / daemon_id
    if daemon_path.exists():
        try:
            shutil.rmtree(daemon_path)
        except Exception as e:
            from meerschaum.utils.warnings import warn
            warn(e)

