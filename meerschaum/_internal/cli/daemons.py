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

    daemon = get_cli_daemon(daemon_ix) if daemon_ix != -1 else None
    cli_lock_path = get_cli_lock_path(daemon_ix)
    if cli_lock_path.exists():
        daemon_is_ready = False

    start_success, start_msg = (
        daemon.run(allow_dirty_run=True, wait=True)
        if daemon_is_ready
        else (False, "Lock exists.")
    )

    if not start_success:
        daemon_is_ready = False

    if start_success:
        start = time.perf_counter()
        while (start - time.perf_counter()) < 3:
            if not daemon.blocking_stdin_file_path.exists():
                time.sleep(0.01)
            else:
                break

    if not daemon_is_ready:
        return entry_without_daemon(sysargs, _patch_args=_patch_args)

    from meerschaum._internal.cli.workers import ActionWorker
    cli_lock_path.touch()
    worker = ActionWorker(daemon_ix)
    _ = worker.job.start()

    session_id = _session_id or get_cli_session_id()
    action_id = uuid.uuid4().hex
    
    entry_data = {
        'session_id': session_id,
        'action_id': action_id,
        'sysargs': sysargs,
        'patch_args': _patch_args,
        'env': dict(os.environ),
    }

    daemon.stdin_file.write(json.dumps(entry_data, separators=(',', ':')) + '\n')

    def _parse_line(line: str) -> Dict[str, Any]:
        try:
            line_text = line
            return json.loads(line_text)
        except Exception as e:
            from meerschaum.utils.warnings import warn
            warn(f"Failed to parse line from CLI daemon:\n{e}")
            return {}

    def _cleanup():
        try:
            if cli_lock_path.exists():
                cli_lock_path.unlink()

            cleanup_data = {
                'session_id': session_id,
                'action_id': action_id,
                'cleanup': True,
            }
            daemon.stdin_file.write(json.dumps(cleanup_data, separators=(',', ':')) + '\n')
        except Exception:
            traceback.print_exc()

    accepted = False
    exit_data = None
    worker_data = None

    while not accepted:
        daemon_lines = daemon.readlines()
        if not daemon_lines:
            time.sleep(0.01)
            continue

        for line in daemon_lines:
            if line.startswith('{') and action_id in line:
                line_data = _parse_line(line)
                state = line_data.get('state', None)
                if state == 'accepted':
                    accepted = True
                    break

    try:
        worker.job.monitor_logs(
            stop_on_exit=True,
            stop_event=worker.stop_event,
            callback_function=worker.monitor_callback,
        )
        while True:
            worker_data_str = worker.output_file.readline()
            if not worker_data_str:
                time.sleep(0.01)
                continue
            worker_data = json.loads(worker_data_str)
            break
    except KeyboardInterrupt:
        exit_data = {
            'session_id': session_id,
            'action_id': action_id,
            'signal': 'SIGINT',
            'traceback': traceback.format_exc(),
            'success': True,
            'message': 'Exiting due to keyboard interrupt.',
        }
    except Exception as e:
        exit_data = {
            'session_id': session_id,
            'action_id': action_id,
            'signal': 'SIGINT',
            'traceback': traceback.format_exc(),
            'success': False,
            'message': f'Encountered exception: {e}',
        }
    except SystemExit:
        exit_data = {
            'session_id': session_id,
            'action_id': action_id,
            'signal': 'SIGTERM',
            'traceback': traceback.format_exc(),
            'success': True,
            'message': 'Exiting on SIGTERM.',
        }
    except BrokenPipeError:
        _cleanup()
        return False, "Connection to daemon is broken."

    if exit_data:
        exit_success = bool(exit_data['success'])
        exit_message = str(exit_data['message'])
        if not exit_success:
            print(exit_data['traceback'])
        try:
            daemon.stdin_file.write(json.dumps(exit_data, separators=(',', ':')) + '\n')
        except BrokenPipeError:
            pass
        time.sleep(0.1)
        _cleanup()
        return exit_success, exit_message

    _cleanup()
    success = (worker_data or {}).get('success', False)
    message = (worker_data or {}).get('message', "Failed to retrieve message from CLI worker.")
    return success, message


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
