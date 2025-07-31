#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the CLI daemons utilities.
"""

import os
import sys
import threading
import pathlib
import uuid
import json
import time
import shlex
import traceback
from typing import Optional, List, Dict, Any

import meerschaum as mrsm



def get_cli_daemon(ix: Optional[int] = None):
    """
    Get the CLI daemon.
    """
    from meerschaum.utils.daemon import Daemon
    ix = ix if ix is not None else get_available_cli_daemon_ix()
    return Daemon(
        cli_daemon_loop,
        env=dict(os.environ),
        daemon_id=f'.cli.{ix}',
        label=f'Internal Meerschaum CLI Daemon ({ix})',
        properties={
            'logs': {
                'write_timestamps': False,
                'refresh_files_seconds': 31557600,
                'max_file_size': 10_000_000,
                'num_files_to_keep': 1,
                'redirect_streams': True,
            },
        },
    )


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


def get_existing_cli_daemons() -> 'List[Daemon]':
    """
    Return a list of the existing CLI daemons.
    """
    from meerschaum.utils.daemon import Daemon
    indices = get_existing_cli_daemon_indices()
    return [
        Daemon(daemon_id=f".cli.{ix}")
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


def cli_daemon_loop():
    """
    The target function to run inside the CLI daemon.
    """
    import json
    import signal
    import uuid
    import sys
    import os
    import traceback
    from typing import Dict, Any, Optional

    from meerschaum.utils.daemon import get_current_daemon
    from meerschaum._internal.cli.workers import ActionWorker

    daemon = get_current_daemon()
    daemon_ix = int(daemon.daemon_id[len('.cli.'):])
    worker = ActionWorker(daemon_ix)

    session_ids_action_ids = {}

    def _cleanup(result, _session_id: Optional[str] = None):
        session_ids_action_ids.pop(_session_id, None)

        daemon.rotating_log.increment_subfiles()
        if daemon.log_offset_path.exists():
            daemon.log_offset_path.unlink()

    def _print_line(
        session_id: str,
        action_id: str,
        state: str,
        result=None,
    ):
        line_data = {
            'state': state,
            'session_id': session_id,
            'action_id': action_id,
        }
        if result is not None:
            _success, _message = result
            line_data['success'] = _success
            line_data['message'] = _message

        sys.stdout.write(json.dumps(line_data, separators=(',', ':')) + '\n')
        sys.stdout.flush()

    def _write_to_worker(data):
        worker.input_file.write(json.dumps(data) + '\n')

    def _entry_from_input_data(input_data: Dict[str, Any]):
        _write_to_worker(input_data)

    while True:
        try:
            input_data_str = input()
        except (KeyboardInterrupt, EOFError, SystemExit):
            running_session_id = list(session_ids_action_ids)[0] if session_ids_action_ids else None
            exc_str = traceback.format_exc().splitlines()[-1]
            action_success, action_msg = True, f"Exiting CLI daemon process:\n{exc_str}"

            _cleanup((action_success, action_msg), running_session_id)
            return action_success, action_msg

        try:
            input_data = json.loads(input_data_str)
        except Exception as e:
            input_data = {"error": str(e)}

        error_msg = input_data.get('error', None)
        session_id = input_data.get('session_id', None)
        if session_id and session_id not in session_ids_action_ids:
            session_ids_action_ids[session_id] = []

        action_id = input_data.get('action_id', uuid.uuid4().hex)
        if session_id and action_id:
            session_ids_action_ids[session_id].append(action_id)

        if error_msg:
            _print_line(
                session_id,
                action_id,
                state='completed',
                result=(False, error_msg),
                error=error_msg,
            )
            _cleanup(None, None)
            return False, error_msg

        if session_id is None:
            session_id = uuid.uuid4()
            input_data['session_id'] = session_id

        _signal = input_data.get('signal', None)
        if _signal:
            signal_to_send = getattr(signal, _signal)
            worker.send_signal(signal_to_send)
            continue

        _do_cleanup = input_data.get('cleanup', False)
        if _do_cleanup:
            _cleanup(None, session_id)
            continue

        _print_line(
            session_id,
            action_id,
            state='accepted',
        )

        _entry_from_input_data(input_data)
