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
    from meerschaum.actions import get_action
    from meerschaum.plugins import load_plugins, _actions_daemon_enabled
    from meerschaum._internal.entry import entry_without_daemon
    from meerschaum._internal.cli.workers import ActionWorker
    from meerschaum._internal.cli.daemons import (
        get_available_cli_daemon_ix,
        get_cli_session_id,
    )
    from meerschaum.config import get_possible_keys
    from meerschaum._internal.arguments import split_pipeline_sysargs, split_chained_sysargs
    daemon_is_ready = True

    load_plugins()

    found_acceptable_prefix = False
    found_unacceptable_prefix = False
    found_disabled_action = False
    allowed_prefixes = (
        mrsm.get_config('system', 'cli', 'allowed_prefixes')
    )
    disallowed_prefixes = (
        mrsm.get_config('system', 'cli', 'disallowed_prefixes')
    )
    refresh_seconds = mrsm.get_config('system', 'cli', 'refresh_seconds')
    sysargs_str = sysargs if isinstance(sysargs, str) else shlex.join(sysargs or [])
    debug = ' --debug' in sysargs_str
    _sysargs = shlex.split(sysargs_str)
    _sysargs, _pipeline_args = split_pipeline_sysargs(_sysargs)
    _chained_sysargs = split_chained_sysargs(_sysargs)
    _action_functions = {
        _action_func.__name__: _action_func
        for _step_sysargs in _chained_sysargs
        if (_action_func := get_action(_step_sysargs))
    }
    for action_name, enabled in _actions_daemon_enabled.items():
        if action_name not in _action_functions:
            continue
        if enabled:
            continue
        found_disabled_action = True
        break

    for prefix in allowed_prefixes:
        if sysargs_str.startswith(prefix) or prefix == '*':
            found_acceptable_prefix = True
            break

    for prefix in disallowed_prefixes:
        if sysargs_str.startswith(prefix) or prefix == '*':
            found_unacceptable_prefix = True
            break

    if not found_acceptable_prefix or found_unacceptable_prefix or found_disabled_action:
        daemon_is_ready = False

    try:
        daemon_ix = get_available_cli_daemon_ix() if daemon_is_ready else -1
    except EnvironmentError as e:
        from meerschaum.utils.warnings import warn
        warn(e, stack=False)
        daemon_ix = -1
        daemon_is_ready = False

    worker = ActionWorker(daemon_ix, refresh_seconds=refresh_seconds) if daemon_ix != -1 else None
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
            time.sleep(refresh_seconds)

    if not daemon_is_ready or worker is None:
        if debug:
            print("Revert to entry without daemon.")
        return entry_without_daemon(sysargs, _patch_args=_patch_args)

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
    for key in get_possible_keys():
        _ = mrsm.get_config(key)
    config = mrsm.get_config()
    
    worker.write_input_data({
        'session_id': session_id,
        'action_id': action_id,
        'sysargs': sysargs,
        'patch_args': _patch_args,
        'env': env,
        'config': config,
    })

    accepted = False
    exit_data = None
    worker_data = None

    while not accepted:
        state = worker.read_output_data().get('state', None)
        if state == 'accepted':
            accepted = True
            break

        time.sleep(refresh_seconds)

    worker.start_cli_logs_refresh_thread()

    try:
        log = worker.job.daemon.rotating_log
        worker.job.monitor_logs(
            stop_on_exit=True,
            callback_function=worker.monitor_callback,
            _log=log,
            _wait_if_stopped=False,
        )
        worker_data = worker.read_output_data()
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
        worker_data = worker.read_output_data()

        if not exit_success:
            print(exit_data['traceback'])

    worker.stop_cli_logs_refresh_thread()
    worker.write_input_data({'increment': True})
    success = (worker_data or {}).get('success', False)
    message = (worker_data or {}).get('message', "Failed to retrieve message from CLI worker.")
    return success, message
