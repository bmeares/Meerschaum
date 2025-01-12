#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions regarding the webterm.
"""

import meerschaum as mrsm
from meerschaum.utils.typing import List


def is_webterm_running(
    host: str,
    port: int,
    protocol: str = 'http',
    session_id: str = 'mrsm',
) -> int:
    """
    Determine whether the webterm service is running on a given host and port.
    """
    requests = mrsm.attempt_import('requests', lazy=False)
    url = f'{protocol}://{host}:{port}/webterm/{session_id}'
    try:
        r = requests.get(url, timeout=3)
    except Exception as e:
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


def get_mrsm_tmux_sessions() -> List[str]:
    """
    Return a list of tmux sessions created by Meerschaum.
    """
    from meerschaum.utils.process import run_process
    tmux_suffix = mrsm.get_config('system', 'webterm', 'tmux', 'session_suffix')
    command = ['tmux', 'ls']
    proc = run_process(command, capture_output=True, as_proc=True)
    if proc.returncode != 0:
        return []
    sessions = [
        line.split(':', maxsplit=1)[0]
        for line in proc.stdout.read().decode('utf-8').split('\n')
    ]
    return [
        session
        for session in sessions
        if session.endswith(tmux_suffix)
    ]
