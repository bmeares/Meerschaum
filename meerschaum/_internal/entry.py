#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
# type: ignore

"""
The entry point for launching Meerschaum actions.
"""

from __future__ import annotations

import os
import sys
import pathlib
import threading

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, List, Optional, Dict, Callable, Any, Union
from meerschaum._internal.static import STATIC_CONFIG as _STATIC_CONFIG

_systemd_result_path = None
if (_STATIC_CONFIG['environment']['systemd_log_path']) in os.environ:
    from meerschaum.utils.daemon import RotatingFile as _RotatingFile, StdinFile as _StdinFile
    from meerschaum.config import get_config as _get_config

    _systemd_result_path = pathlib.Path(
        os.environ[_STATIC_CONFIG['environment']['systemd_result_path']]
    )
    _systemd_log_path = pathlib.Path(
        os.environ[_STATIC_CONFIG['environment']['systemd_log_path']]
    )
    _systemd_delete_job = (
        (os.environ.get(_STATIC_CONFIG['environment']['systemd_delete_job'], None) or '0')
        not in (None, '0', 'false')
    )
    _job_name = os.environ[_STATIC_CONFIG['environment']['daemon_id']]
    _systemd_log = _RotatingFile(
        _systemd_log_path,
        write_timestamps=True,
        timestamp_format=_get_config('jobs', 'logs', 'timestamps', 'format'),
    )
    sys.stdout = _systemd_log
    sys.stderr = _systemd_log
    _systemd_stdin_path = os.environ.get(_STATIC_CONFIG['environment']['systemd_stdin_path'], None)
    if _systemd_stdin_path:
        sys.stdin = _StdinFile(_systemd_stdin_path)


def entry(
    sysargs: Optional[List[str]] = None,
    _patch_args: Optional[Dict[str, Any]] = None,
    _use_cli_daemon: bool = True,
    _session_id: Optional[str] = None,
) -> SuccessTuple:
    """
    Parse arguments and launch a Meerschaum action.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    if not _use_cli_daemon or not mrsm.get_config('system', 'experimental', 'cli_daemon'):
        return entry_without_daemon(sysargs, _patch_args=_patch_args)

    import uuid
    import json
    import time
    import shlex
    import traceback

    daemon_is_ready = True

    found_acceptable_prefix = False
    allowed_prefixes = mrsm.get_config('system', 'cli', 'allowed_prefixes')
    for prefix in allowed_prefixes:
        sysargs_str = sysargs if isinstance(sysargs, str) else shlex.join(sysargs)
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
        while True:
            if not daemon.blocking_stdin_file_path.exists():
                time.sleep(0.01)
            else:
                break

    if not daemon_is_ready:
        return entry_without_daemon(sysargs, _patch_args=_patch_args)

    cli_lock_path.touch()

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

    while True:
        try:
            lines = daemon.readlines()
            if not lines:
                time.sleep(0.01)
                continue

            for line in lines:
                if line.startswith('{') and action_id in line:
                    line_data = _parse_line(line)
                    state = line_data.get('state', None)
                    if state == 'completed':
                        success, msg = line_data['success'], line_data['message']
                        _cleanup()
                        return success, msg
                    elif state == 'accepted':
                        accepted = True
                        continue

                elif line and accepted:
                    sys.stdout.write(line)
                    sys.stdout.flush()

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
            exit_success = exit_data['success']
            exit_message = exit_data['message']
            if not exit_success:
                print(exit_data['traceback'])
            try:
                daemon.stdin_file.write(json.dumps(exit_data, separators=(',', ':')) + '\n')
            except BrokenPipeError:
                pass
            time.sleep(0.1)
            _cleanup()
            return exit_success, exit_message


def entry_without_daemon(
    sysargs: Union[List[str], str, None] = None,
    _patch_args: Optional[Dict[str, Any]] = None,
) -> SuccessTuple:
    """
    Parse arguments and launch a Meerschaum action.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    import shlex
    import json
    from meerschaum.utils.formatting import make_header
    from meerschaum._internal.arguments import (
        parse_arguments,
        split_chained_sysargs,
        split_pipeline_sysargs,
        sysargs_has_api_executor_keys,
        get_pipeline_sysargs,
    )
    from meerschaum._internal.static import STATIC_CONFIG
    if sysargs is None:
        sysargs = []
    if not isinstance(sysargs, list):
        sysargs = shlex.split(sysargs)

    pipeline_key = STATIC_CONFIG['system']['arguments']['pipeline_key']
    escaped_pipeline_key = STATIC_CONFIG['system']['arguments']['escaped_pipeline_key']
    sysargs, pipeline_args = split_pipeline_sysargs(sysargs)

    has_daemon = '-d' in sysargs or '--daemon' in sysargs
    has_start_job = sysargs[:2] == ['start', 'job']
    pipeline_has_api_executor_keys = sysargs_has_api_executor_keys(pipeline_args)

    chained_sysargs = (
        [sysargs]
        if has_daemon or has_start_job or pipeline_has_api_executor_keys
        else split_chained_sysargs(sysargs)
    )
    if pipeline_args:
        chained_sysargs = [get_pipeline_sysargs(sysargs, pipeline_args, _patch_args=_patch_args)]

    results: List[SuccessTuple] = []

    for _sysargs in chained_sysargs:
        if escaped_pipeline_key in _sysargs:
            _sysargs = [
                pipeline_key
                if _arg == escaped_pipeline_key
                else _arg
                for _arg in _sysargs
            ]

        args = parse_arguments(_sysargs)
        if _patch_args:
            args.update(_patch_args)
        argparse_exception = args.get(
            STATIC_CONFIG['system']['arguments']['failure_key'],
            None,
        )
        if argparse_exception is not None:
            args_text = args.get('text', '')
            if not args_text.startswith('show arguments'):
                return (
                    False,
                    (
                        "Invalid arguments:"
                        + (f"\n{args_text}" if args_text else '')
                        + f"\n    {argparse_exception}"
                    )
                )

        entry_success, entry_msg = entry_with_args(_patch_args=_patch_args, **args)
        results.append((entry_success, entry_msg))

        if not entry_success:
            break

    success = all(_success for _success, _ in results)
    any_success = any(_success for _success, _ in results)
    success_messages = [_msg for _success, _msg in results if _success]

    successes_msg = (
        success_messages[0]
        if len(success_messages) and len(results) == 1
        else (
            (
                'Successfully c'
                if success
                else (
                    'Failed pipeline after '
                    + f"{len(success_messages)} step"
                    + ('s' if len(success_messages) != 1 else '')
                    + '.\n\nC'
                )
            ) + 'ompleted step'
                + ('s' if len(success_messages) != 1 else '') 
            + ':\n\n'
            + '\n'.join(
                [
                    (
                        make_header(shlex.join(_sysargs))
                        + '\n    ' + _msg + '\n'
                    )
                    for i, (_msg, _sysargs) in enumerate(zip(success_messages, chained_sysargs))
                ]
            )
        )
    )
    has_fail = results[-1][0] is False
    fail_ix = len(results) - 1
    fail_sysargs = chained_sysargs[fail_ix] if has_fail else None
    fail_msg = results[-1][1] if has_fail else ''
    fails_msg = (
        'Failed to complete step:\n\n'
        + make_header(shlex.join(fail_sysargs))
        + '\n    '
        + fail_msg

    ) if not results[-1][0] else ''

    msg = (
        successes_msg
        + ('\n\n' if any_success else '')
        + fails_msg
    ).rstrip() if len(chained_sysargs) > 1 else results[0][1]

    if _systemd_result_path:
        import json
        from meerschaum.utils.warnings import warn
        import meerschaum as mrsm

        job = mrsm.Job(_job_name, executor_keys='systemd')
        if job.delete_after_completion:
            delete_success, delete_msg = job.delete()
            mrsm.pprint((delete_success, delete_msg))
        else:
            try:
                if _systemd_result_path.parent.exists():
                    with open(_systemd_result_path, 'w+', encoding='utf-8') as f:
                        json.dump((success, msg), f)
            except Exception as e:
                warn(f"Failed to write job result:\n{e}")

    return success, msg


def entry_with_args(
    _actions: Optional[Dict[str, Callable[[Any], SuccessTuple]]] = None,
    _patch_args: Optional[Dict[str, Any]] = None,
    **kw
) -> SuccessTuple:
    """Execute a Meerschaum action with keyword arguments.
    Use `_entry()` for parsing sysargs before executing.
    """
    import functools
    import inspect
    from meerschaum.actions import get_action
    from meerschaum._internal.arguments import remove_leading_action
    from meerschaum.utils.venv import active_venvs, deactivate_venv
    from meerschaum._internal.static import STATIC_CONFIG
    from meerschaum.utils.typing import is_success_tuple

    if _patch_args:
        kw.update(_patch_args)

    and_key = STATIC_CONFIG['system']['arguments']['and_key']
    escaped_and_key = STATIC_CONFIG['system']['arguments']['escaped_and_key']
    if and_key in (sysargs := kw.get('sysargs', [])):
        if '-d' in sysargs or '--daemon' in sysargs:
            sysargs = [(arg if arg != and_key else escaped_and_key) for arg in sysargs]
        return entry(sysargs, _patch_args=_patch_args)

    if kw.get('trace', None):
        from meerschaum.utils.misc import debug_trace
        debug_trace()
    if (
        len(kw.get('action', [])) == 0
        or
        (kw['action'][0] == 'mrsm' and len(kw['action'][1:]) == 0)
    ):
        _ = get_shell(**kw).cmdloop()
        return True, "Success"

    _skip_schedule = False

    executor_keys = kw.get('executor_keys', None)
    if executor_keys is None:
        executor_keys = 'local'

    if executor_keys.startswith('api:'):
        intended_action_function = get_action(kw['action'], _actions=_actions)
        function_accepts_executor_keys = (
            'executor_keys' in inspect.signature(intended_action_function).parameters
            if intended_action_function is not None
            else False
        )
        if not function_accepts_executor_keys:
            api_label = executor_keys.split(':')[-1]
            kw['action'].insert(0, 'api')
            kw['action'].insert(1, api_label)
            _skip_schedule = True

    ### If the `--daemon` flag is present, prepend 'start job'.
    if kw.get('daemon', False) and kw['action'][0] != 'stack':
        kw['action'] = ['start', 'jobs'] + kw['action']

    action_function = get_action(kw['action'], _actions=_actions)

    ### If action does not exist, execute in a subshell.
    if action_function is None:
        kw['action'].insert(0, 'sh')
        action_function = get_action(['sh'], _actions=_actions)

    ### Check if the action is a plugin, and if so, activate virtual environment.
    plugin_name = (
        action_function.__module__.split('.')[1] if (
            action_function.__module__.startswith('plugins.')
        ) else None
    )

    if (
        kw['action']
        and kw['action'][0] == 'start'
        and kw['action'][1] in ('job', 'jobs')
    ):
        _skip_schedule = True

    kw['action'] = remove_leading_action(kw['action'], _actions=_actions)

    do_action = functools.partial(
        _do_action_wrapper,
        action_function,
        plugin_name,
        _skip_schedule=_skip_schedule,
        **kw
    )

    if kw.get('schedule', None) and not _skip_schedule:
        from meerschaum.utils.schedule import schedule_function
        from meerschaum.utils.misc import interval_str
        import time
        from datetime import timedelta
        start_time = time.perf_counter()
        schedule_function(do_action, **kw)
        delta = timedelta(seconds=(time.perf_counter() - start_time))
        result = True, f"Exited scheduler after {interval_str(delta)}."
    else:
        result = do_action()

    ### Clean up stray virtual environments.
    for venv in [venv for venv in active_venvs]:
        deactivate_venv(venv, debug=kw.get('debug', False), force=True)

    if not is_success_tuple(result):
        return True, str(result)

    return result


def _do_action_wrapper(
    action_function,
    plugin_name,
    _skip_schedule: bool = False,
    **kw
):
    from meerschaum.plugins import Plugin
    from meerschaum.utils.venv import Venv
    from meerschaum.utils.misc import filter_keywords
    plugin = Plugin(plugin_name) if plugin_name else None
    with Venv(plugin, debug=kw.get('debug', False)):
        action_name = ' '.join(action_function.__name__.split('_') + kw.get('action', []))
        try:
            result = action_function(**filter_keywords(action_function, **kw))
        except Exception as e:
            if kw.get('debug', False):
                import traceback
                traceback.print_exception(type(e), e, e.__traceback__)
            result = False, (
                f"Failed to execute `{action_name.strip()}` "
                + f"with `{type(e).__name__}`"
                + (f':\n\n{e}' if str(e) else '.')
                + (
                    "\n\nRun again with '--debug' to see a full stacktrace."
                    if not kw.get('debug', False) else ''
                )
            )
        except KeyboardInterrupt:
            result = False, f"Cancelled action `{action_name.lstrip()}`."

    if kw.get('schedule', None) and not _skip_schedule:
        mrsm.pprint(result)

    return result

_shells = []
_shell = None
def get_shell(
    sysargs: Optional[List[str]] = None,
    reload: bool = False,
    debug: bool = False,
    mrsm_instance: Optional[str] = None,
    **kwargs: Any
):
    """Initialize and return the Meerschaum shell object."""
    global _shell
    from meerschaum.utils.debug import dprint
    import meerschaum._internal.shell as shell_pkg
    from meerschaum.actions import actions
    if sysargs is None:
        sysargs = []

    if _shell is None or reload:
        if debug:
            dprint("Loading the shell...")

        if _shell is None:
            shell_pkg._insert_shell_actions()
            _shell = shell_pkg.Shell(actions, sysargs=sysargs, instance_keys=mrsm_instance)
        elif reload:
            _shell.__init__(instance_keys=mrsm_instance)

        _shells.append(_shell)
    return _shell


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

    from meerschaum._internal.entry import entry as _entry
    from meerschaum.utils.daemon import get_current_daemon
    from meerschaum.utils.threading import Thread
    from meerschaum.utils.misc import set_env

    daemon = get_current_daemon()

    session_ids_threads = {}
    session_ids_action_ids = {}
    thread_idents_session_ids = {} 

    def _cleanup(result, _session_id: Optional[str] = None):
        session_thread = (
            session_ids_threads.get(_session_id, None)
            if _session_id is not None
            else None
        )
        if session_thread is not None:
            if session_thread.ident in thread_idents_session_ids:
                del thread_idents_session_ids[session_thread.ident]
            if _session_id in session_ids_threads:
                del session_ids_threads[_session_id]
            if _session_id in session_ids_action_ids:
                del session_ids_action_ids[_session_id]

        daemon.rotating_log.increment_subfiles()
        if daemon.log_offset_path.exists():
            daemon.log_offset_path.unlink()

    def _print_line(
        text,
        session_id: str,
        action_id: str,
        state: str,
        result=None,
    ):
        line_data = {
            'state': state,
            'session_id': session_id,
            'action_id': action_id,
            'text': text,
        }
        if result is not None:
            _success, _message = result
            line_data['success'] = _success
            line_data['message'] = _message

        sys.stdout.write(json.dumps(line_data, separators=(',', ':')) + '\n')
        sys.stdout.flush()

    def _entry_from_input_data(input_data: Dict[str, Any]):
        sysargs = input_data.get('sysargs', None)
        _patch_args = input_data.get('patch_args', None)
        session_id = input_data.get('session_id', None)
        action_id = input_data.get('action_id', None)
        env = input_data.get('env', os.environ)

        try:
            with set_env(env):
                action_success, action_msg = _entry(
                    sysargs,
                    _patch_args=_patch_args,
                    _use_cli_daemon=False,
                )
        except Exception as e:
            action_success, action_msg = False, f"Failed to execute:\n{e}"

        _print_line(
            None,
            session_id,
            action_id,
            state='completed',
            result=(action_success, action_msg),
        )

        return action_success, action_msg

    def send_exc_to_session_thread(session_id: str, exc):
        session_thread = session_ids_threads.get(session_id, None)
        if session_thread is None:
            return

        session_thread.raise_exception(exc)

    while True:
        try:
            input_data_str = input()
        except (KeyboardInterrupt, EOFError, SystemExit) as e:
            running_session_id = list(session_ids_threads)[0] if session_ids_threads else None
            if running_session_id:
                send_exc_to_session_thread(running_session_id, e)

            running_action_id = (
                session_ids_action_ids[running_session_id][-1]
                if (running_session_id and session_ids_action_ids)
                else None
            )
            running_thread = (
                session_ids_threads.get(running_session_id, None)
                if running_session_id
                else None
            )
            exc_str = traceback.format_exc().splitlines()[-1]

            action_success, action_msg = True, f"Exiting CLI daemon process:\n{exc_str}"

            import time
            start = time.perf_counter()
            thread_is_dead = False
            while (time.perf_counter() - start) <= 8:
                if running_thread is None or not running_thread.is_alive():
                    thread_is_dead = True
                    break
                time.sleep(0.1)

            if not thread_is_dead:
                _print_line(
                    None,
                    running_session_id,
                    running_action_id,
                    state='completed',
                    result=(action_success, action_msg),
                )
            _cleanup((action_success, action_msg), running_session_id)
            return action_success, action_msg

        try:
            input_data = json.loads(input_data_str)
        except Exception as e:
            input_data = {"error": str(e)}

        error_msg = input_data.get('error', None)
        session_id = input_data.get('session_id', None)
        action_id = input_data.get('action_id', uuid.uuid4().hex)

        if error_msg:
            _print_line(
                {"error": error_msg},
                session_id,
                action_id,
                state='completed',
                result=(False, error_msg),
            )
            _cleanup(None, None)
            return False, error_msg

        if session_id is None:
            session_id = uuid.uuid4()
            input_data['session_id'] = session_id

        _signal = input_data.get('signal', None)
        if _signal:
            signal_to_send = getattr(signal, _signal)
            thread_to_stop = session_ids_threads.get(session_id, None)
            if thread_to_stop:
                thread_to_stop.send_signal(signal_to_send)
            continue

        _do_cleanup = input_data.get('cleanup', False)
        if _do_cleanup:
            _cleanup(None, session_id)
            continue

        _print_line(
            None,
            session_id,
            action_id,
            state='accepted',
        )

        thread = Thread(
            target=_entry_from_input_data,
            daemon=True,
            args=(input_data,),
        )
        session_ids_threads[session_id] = thread
        if session_id not in session_ids_action_ids:
            session_ids_action_ids[session_id] = []
        session_ids_action_ids[session_id].append(action_id)
        thread_idents_session_ids[thread.ident] = session_id
        thread.start()


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
