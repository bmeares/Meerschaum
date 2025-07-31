#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the CLI daemon entrypoint.
"""

import os
import time
import uuid
import json
import shlex
import traceback
import signal
from typing import Optional, Dict, List, Any

import meerschaum as mrsm


def entry_with_daemon(
    sysargs: Optional[List[str]] = None,
    _patch_args: Optional[Dict[str, Any]] = None,
    _use_cli_daemon: bool = True,
    _session_id: Optional[str] = None,
) -> mrsm.SuccessTuple:
    """
    Parse arguments and launch a Meerschaum action.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    from meerschaum._internal.entry import entry_without_daemon
    from meerschaum._internal.cli.workers import ActionWorker
    from meerschaum._internal.cli.daemons import (
        get_available_cli_daemon_ix,
        get_cli_session_id,
    )
    daemon_is_ready = True

    found_acceptable_prefix = False
    allowed_prefixes = mrsm.get_config('system', 'cli', 'allowed_prefixes')
    for prefix in allowed_prefixes:
        sysargs_str = sysargs if isinstance(sysargs, str) else shlex.join(sysargs or [])
        if sysargs_str.startswith(prefix):
            found_acceptable_prefix = True
            break

    if not found_acceptable_prefix:
        daemon_is_ready = False

    try:
        daemon_ix = get_available_cli_daemon_ix() if daemon_is_ready else -1
    except EnvironmentError as e:
        from meerschaum.utils.warnings import warn
        warn(e, stack=False)
        daemon_ix = -1
        daemon_is_ready = False

    worker = ActionWorker(daemon_ix) if daemon_ix != -1 else None
    if worker and worker.lock_path.exists():
        daemon_is_ready = False

    start_success, start_msg = (
        worker.job.start()
        if worker and daemon_is_ready
        else (False, "Lock exists.")
    )

    if not start_success:
        daemon_is_ready = False

    if start_success and worker:
        start = time.perf_counter()
        while (start - time.perf_counter()) < 3:
            if not worker.is_ready():
                time.sleep(0.01)
            else:
                break

    if not daemon_is_ready:
        return entry_without_daemon(sysargs, _patch_args=_patch_args)

    session_id = _session_id or get_cli_session_id()
    action_id = uuid.uuid4().hex
    
    worker.write_input_data({
        'session_id': session_id,
        'action_id': action_id,
        'sysargs': sysargs,
        'patch_args': _patch_args,
        'env': dict(os.environ),
    })

    accepted = False
    exit_data = None
    worker_data = None

    while not accepted:
        state = worker.read_output_data(block=True).get('state', None)
        if state == 'accepted':
            accepted = True
            break

        time.sleep(0.1)

    try:
        worker.job.monitor_logs(
            stop_on_exit=True,
            callback_function=worker.monitor_callback,
        )
        worker_data = worker.read_output_data(block=True)
    except KeyboardInterrupt:
        exit_data = {
            'session_id': session_id,
            'action_id': action_id,
            'signal': signal.SIGINT,
            'traceback': traceback.format_exc(),
            'success': True,
            'message': 'Exiting due to keyboard interrupt.',
        }
    except Exception as e:
        exit_data = {
            'session_id': session_id,
            'action_id': action_id,
            'signal': signal.SIGINT,
            'traceback': traceback.format_exc(),
            'success': False,
            'message': f'Encountered exception: {e}',
        }
    except SystemExit:
        exit_data = {
            'session_id': session_id,
            'action_id': action_id,
            'signal': signal.SIGTERM,
            'traceback': traceback.format_exc(),
            'success': True,
            'message': 'Exiting on SIGTERM.',
        }
    except BrokenPipeError:
        return False, "Connection to daemon is broken."

    if exit_data:
        exit_success = bool(exit_data['success'])
        exit_message = str(exit_data['message'])
        exit_signal = exit_data['signal']
        worker.send_signal(exit_signal)
        if not exit_success:
            print(exit_data['traceback'])
        return exit_success, exit_message

    worker.write_input_data({'increment': True})

    success = (worker_data or {}).get('success', False)
    message = (worker_data or {}).get('message', "Failed to retrieve message from CLI worker.")
    return success, message
