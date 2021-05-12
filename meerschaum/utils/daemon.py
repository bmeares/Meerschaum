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
        keep_daemon_output=('--keep-daemon-output' in sysargs or '--skip-daemon-cleanup' in sysargs)
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
        label : Optional[str] = None,
        **kw
    ) -> Any:
    """
    Execute a function as a daemon.
    NOTE: This WILL EXIT the parent process!
    """
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.warnings import warn
    daemoniker = attempt_import('daemoniker')

    began = datetime.datetime.utcnow()
    daemon_id = (
        began.isoformat() + ' ' +
        (func.__name__ if label is None else label) +
        ' ' + str(threading.current_thread().ident)
    )
    daemon_path = DAEMON_RESOURCES_PATH / daemon_id
    daemon_path.mkdir(parents=True, exist_ok=True)
    daemon_properties = {
        'function' : {
            'name' : func.__name__,
            'args' : args,
            'kw' : kw,
        },
        'began' : began.isoformat(),
    }

    pid_path = daemon_path / 'process.pid'
    stdout_path = daemon_path / 'stdout.txt'
    stderr_path = daemon_path / 'stderr.txt'
    properties_path = daemon_path / 'properties.json'
    _write_properties(daemon_properties, properties_path)

    with daemoniker.Daemonizer() as (is_setup, daemonizer):
        is_parent, _daemon_id, _properties, _properties_path, _args, _kw = daemonizer(
            str(pid_path),
            daemon_id,
            daemon_properties,
            properties_path,
            args,
            kw,
            stdout_goto = str(stdout_path),
            stderr_goto = str(stderr_path),
            strip_cmd_args = True
        )

    sighandler = daemoniker.SignalHandler1(str(pid_path))
    sighandler.start()

    try:
        result = func(*_args, **_kw)
    except Exception as e:
        warn(e, stacklevel=3)
        result = e

    if keep_daemon_output:
        _properties['ended'] = datetime.datetime.utcnow().isoformat()
        _write_properties(_properties, _properties_path)
    else:
        _cleanup(_daemon_id)
    return result

def _cleanup(daemon_id : str):
    daemon_path = DAEMON_RESOURCES_PATH / daemon_id
    if daemon_path.exists():
        try:
            shutil.rmtree(daemon_path)
        except Exception as e:
            from meerschaum.utils.warnings import warn
            warn(e)

def _write_properties(properties, properties_path):
    """
    Write daemon properties to a path.
    """
    with open(properties_path, 'w+') as properties_file:
        json.dump(properties, properties_file)

def get_daemon_ids() -> List[str]:
    """
    Return a list of daemon IDs.
    """
    return os.listdir(DAEMON_RESOURCES_PATH)

def get_daemon_path(daemon_id : str) -> pathlib.Path:
    """
    Return the `pathlib.Path` for a daemon.
    """
    return DAEMON_RESOURCES_PATH / daemon_id

def get_daemon_properties(daemon_id : str) -> Dict[str, Any]:
    """
    Return the properties dictionary for a daemon.
    """
    with open(get_daemon_path(daemon_id), 'r') as file:
        return json.load(file)

def get_running_daemon_ids() -> List[str]:
    """
    Return a list of daemons which have not yet completed.
    """
    daemon_ids = get_daemon_ids()
