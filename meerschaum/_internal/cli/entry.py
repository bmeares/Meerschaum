#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the CLI daemon entrypoint.
"""

import os
import time
import uuid
import shlex
import shutil
import traceback
import signal
from typing import Optional, Dict, List, Any, Union

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
    found_unacceptable_prefix = False
    allowed_prefixes = mrsm.get_config('system', 'cli', 'allowed_prefixes')
    disallowed_prefixes = mrsm.get_config('system', 'cli', 'disallowed_prefixes')
    sysargs_str = sysargs if isinstance(sysargs, str) else shlex.join(sysargs or [])
    for prefix in allowed_prefixes:
        if sysargs_str.startswith(prefix) or prefix == '*':
            found_acceptable_prefix = True
            break
    for prefix in disallowed_prefixes:
        if sysargs_str.startswith(prefix) or prefix == '*':
            found_unacceptable_prefix = True

    if not found_acceptable_prefix or found_unacceptable_prefix:
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
        while (time.perf_counter() - start) < 3:
            if worker.is_ready():
                break
            time.sleep(0.01)

    if not daemon_is_ready or worker is None:
        return entry_without_daemon(sysargs, _patch_args=_patch_args)

    refresh_seconds = mrsm.get_config('system', 'cli', 'refresh_seconds')
    session_id = _session_id or get_cli_session_id()
    action_id = uuid.uuid4().hex

    terminal_size = shutil.get_terminal_size()
    env = {
        **{
            'LINES': str(terminal_size.lines),
            'COLUMNS': str(terminal_size.columns),
        },
        **dict(os.environ),
    }
    
    worker.write_input_data({
        'session_id': session_id,
        'action_id': action_id,
        'sysargs': sysargs,
        'patch_args': _patch_args,
        'env': env,
    })

    accepted = False
    exit_data = None
    worker_data = None

    while not accepted:
        state = worker.read_output_data(block=True).get('state', None)
        if state == 'accepted':
            accepted = True
            break

        time.sleep(refresh_seconds)

    start_cli_logs_refresh_thread(daemon_ix)

    try:
        log = worker.job.daemon.rotating_log
        worker.job.monitor_logs(
            stop_on_exit=True,
            callback_function=worker.monitor_callback,
            _log=log,
            _wait_if_stopped=False,
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
        exit_signal = exit_data['signal']
        worker.send_signal(exit_signal)
        try:
            worker.job.monitor_logs(
                stop_on_exit=True,
                callback_function=worker.monitor_callback,
                _wait_if_stopped=False,
                _log=log,
            )
        except (KeyboardInterrupt, SystemExit):
            worker.send_signal(signal.SIGTERM)
        worker_data = worker.read_output_data(block=True, timeout_seconds=3)

        if not exit_success:
            print(exit_data['traceback'])

    worker.write_input_data({'increment': True})
    success = (worker_data or {}).get('success', False)
    message = (worker_data or {}).get('message', "Failed to retrieve message from CLI worker.")
    return success, message


def touch_cli_logs_loop(ix: int, refresh_seconds: Union[int, float] = 0.01):
    """
    Touch the CLI daemon's logs to refresh the logs monitoring.
    """
    from meerschaum._internal.cli.workers import ActionWorker
    worker = ActionWorker(ix)

    while True:
        worker.job.daemon.rotating_log.touch()
        time.sleep(refresh_seconds)


def start_cli_logs_refresh_thread(ix: int):
    """
    Spin up a daemon thread to refresh the CLI's logs.
    """
    from meerschaum.utils.threading import Thread
    refresh_seconds = mrsm.get_config('system', 'cli', 'refresh_seconds')
    thread = Thread(
        target=touch_cli_logs_loop,
        args=(ix,),
        kwargs={'refresh_seconds': refresh_seconds},
        daemon=True,
    )
    thread.start()
