#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions regarding the webterm.
"""

from typing import List, Optional

import meerschaum as mrsm


def is_webterm_running(
    host: Optional[str] = None,
    port: Optional[int] = None,
    protocol: str = 'http',
    session_id: str = 'mrsm',
) -> int:
    """
    Determine whether the webterm service is running on a given host and port.
    """
    requests = mrsm.attempt_import('requests', lazy=False)
    host = host or mrsm.get_config('api', 'webterm', 'host')
    port = port or mrsm.get_config('api', 'webterm', 'port')
    url = f'{protocol}://{host}:{port}/webterm/{session_id}'
    try:
        r = requests.get(url, timeout=3)
    except Exception:
        return False
    if not r:
        return False
    return '<title>Meerschaum Shell</title>' in r.text


def kill_tmux_session(session: str) -> bool:
    """
    Kill a tmux session if it exists.
    """
    from meerschaum.utils.process import run_process
    command = ['tmux', 'kill-session', '-t', session]
    return run_process(command, capture_output=True) == 0


def get_mrsm_tmux_sessions(port: Optional[int] = None) -> List[str]:
    """
    Return a list of tmux sessions created by Meerschaum.
    """
    from meerschaum.utils.process import run_process
    tmux_suffix = mrsm.get_config('api', 'webterm', 'tmux', 'session_suffix')
    command = ['tmux', 'ls']
    proc = run_process(command, capture_output=True, as_proc=True)
    if proc.returncode != 0:
        return []

    port = port or mrsm.get_config('meerschaum', 'webterm', 'port')

    sessions = [
        line.split(':', maxsplit=1)[0]
        for line in proc.stdout.read().decode('utf-8').split('\n')
    ]
    mrsm_sessions_ports = []
    for session in sessions:
        if '--' not in session:
            continue

        parts = session.split('--', maxsplit=1)
        if len(parts) != 2:
            continue

        if not parts[0].endswith(tmux_suffix):
            continue

        try:
            session_port = int(parts[1])
        except Exception:
            continue

        mrsm_sessions_ports.append((session, session_port))

    return [
        session
        for session, session_port in mrsm_sessions_ports
        if session_port == port
    ]
