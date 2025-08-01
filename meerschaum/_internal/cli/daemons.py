#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the CLI daemons utilities.
"""

import os
import threading
import pathlib
from typing import Optional, List

import meerschaum as mrsm


def get_cli_daemon(ix: Optional[int] = None):
    """
    Get the CLI daemon.
    """
    from meerschaum._internal.cli.workers import ActionWorker
    ix = ix if ix is not None else get_available_cli_daemon_ix()
    return ActionWorker(ix)


def get_cli_lock_path(ix: int) -> pathlib.Path:
    """
    Return the path to a CLI daemon's lock file.
    """
    from meerschaum.config.paths import CLI_RESOURCES_PATH
    return CLI_RESOURCES_PATH / f"ix-{ix}.lock"


def get_cli_session_id() -> str:
    """
    Return the session ID to use for the current process and thread.
    """
    return f"{os.getpid()}.{threading.current_thread().ident}"


def get_available_cli_daemon_ix() -> int:
    """
    Return the index for an available CLI daemon.
    """
    max_ix = mrsm.get_config('system', 'cli', 'max_daemons') - 1
    ix = 0
    while True:
        lock_path = get_cli_lock_path(ix)
        if not lock_path.exists():
            return ix
        
        ix += 1
        if ix > max_ix:
            raise EnvironmentError("Too many CLI daemons are running.")


def get_existing_cli_daemon_indices() -> List[int]:
    """
    Return a list of the existing CLI daemons.
    """
    from meerschaum.utils.daemon import get_daemon_ids
    daemon_ids = [daemon_id for daemon_id in get_daemon_ids() if daemon_id.startswith('.cli.')]
    indices = []

    for daemon_id in daemon_ids:
        try:
            ix = int(daemon_id[len('.cli.'):])
            indices.append(ix)
        except Exception:
            pass

    return indices


def get_existing_cli_daemons() -> 'List[ActionWorker]':
    """
    Return a list of the existing CLI daemons.
    """
    from meerschaum._internal.cli.workers import ActionWorker
    indices = get_existing_cli_daemon_indices()
    return [
        ActionWorker(ix)
        for ix in indices
    ]


def _get_cli_session_dir_path(session_id: str) -> pathlib.Path:
    """
    Return the path to the file handles for the CLI session.
    """
    from meerschaum.config.paths import CLI_RESOURCES_PATH
    return CLI_RESOURCES_PATH / session_id


def _get_cli_stream_path(
    session_id: str,
    action_id: str,
    stream: str = 'stdout',
) -> pathlib.Path:
    """
    Return the file path for the stdout / stderr file stream.
    """
    session_dir_path = _get_cli_session_dir_path(session_id)
    return session_dir_path / f'{action_id}.{stream}'
