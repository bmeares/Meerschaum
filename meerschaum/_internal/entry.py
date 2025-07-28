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
    _use_cli_daemon: bool = False,
    _session_id: Optional[str] = None,
) -> SuccessTuple:
    """
    Parse arguments and launch a Meerschaum action.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    if not _use_cli_daemon:
        return entry_without_daemon(sysargs, _patch_args=_patch_args)

    import uuid
    import json
    import time
    import threading
    import shutil
    from meerschaum.utils.daemon import StdinFile
    from meerschaum.utils.prompt import prompt

    daemon_is_ready = True
    daemon = get_cli_daemon()
    start_success, start_msg = daemon.run(allow_dirty_run=True, wait=True)

    if not start_success:
        daemon_is_ready = False

    if start_success:
        while True:
            if not daemon.blocking_stdin_file_path.exists():
                print("Not yet accepting input, wait...")
                time.sleep(0.01)
            else:
                break

    ### TODO: Find other conditions which may fail the daemon.

    if not daemon_is_ready:
        return entry_without_daemon(sysargs, _patch_args=_patch_args)

    session_id = _session_id or (
        f"{os.getpid()}.{threading.current_thread().ident}"
    )
    action_id = uuid.uuid4().hex
    session_dir_path = _get_cli_session_dir_path(session_id)
    
    entry_data = {
        'session_id': session_id,
        'action_id': action_id,
        'sysargs': sysargs,
        'patch_args': _patch_args,
        'env': dict(os.environ),
    }

    daemon.stdin_file.write(json.dumps(entry_data, separators=(',', ':')) + '\n')

    def _parse_line(line: bytes) -> Dict[str, Any]:
        try:
            line_text = line.decode('utf-8')
            return json.loads(line_text)
        except Exception as e:
            from meerschaum.utils.warnings import warn
            warn(f"Failed to parse line from CLI daemon:\n{e}")
            return {}

    stdout_file_path = _get_cli_stream_path(session_id, action_id, 'stdout')
    stdout_file = StdinFile(stdout_file_path, decode=False)
    stdin_file_path = _get_cli_stream_path(session_id, action_id, 'stdin')
    stdin_file = StdinFile(stdin_file_path, decode=False)

    def _cleanup():
        stdout_file.close()
        stdin_file.close()
        shutil.rmtree(session_dir_path)

    while True:
        try:
            exit_data = None
            line = stdout_file.readline()
            if action_id.encode('utf-8') in line:
                line_data = _parse_line(line)

                if line_data.get('completed', False):
                    success, msg = line_data['success'], line_data['message']
                    _cleanup()
                    return success, msg

            if line:
                sys.stdout.write(line.decode('utf-8') + '\n')
                sys.stdout.flush()

            if stdin_file.blocking_file_path.exists():
                data_to_send_back = prompt('', silent=True)
                daemon.stdin_file.write(data_to_send_back + '\n')
                continue
        except (Exception, KeyboardInterrupt):
            exit_data = {
                'session_id': session_id,
                'action_id': action_id,
                'signal': 'SIGINT',
            }
        except SystemExit:
            exit_data = {
                'session_id': session_id,
                'action_id': action_id,
                'signal': 'SIGTERM',
            }

        if exit_data:
            daemon.stdin_file.write(json.dumps(exit_data, separators=(',', ':')) + '\n')
            time.sleep(0.1)
            _cleanup()
            return True, "Exiting due to interrupt."


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
    from meerschaum.config.paths import CLI_CACHE_RESOURCES_PATH
    return CLI_CACHE_RESOURCES_PATH / session_id


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
    import threading
    import builtins
    from typing import Dict, Any
    from contextlib import redirect_stdout, redirect_stderr

    from meerschaum.utils.prompt import prompt
    from meerschaum._internal.entry import entry as _entry
    from meerschaum.utils.daemon import StdinFile
    from meerschaum.utils.threading import Thread
    from meerschaum.utils.misc import set_env
    from meerschaum.config.paths import CLI_CACHE_RESOURCES_PATH

    session_ids_threads = {}
    session_ids_stdout_files = {}
    thread_idents_session_ids = {} 

    def _print_monkey_patch(*args, **kwargs):
        current_thread_ident = threading.current_thread().ident
        session_id = thread_idents_session_ids.get(current_thread_ident, None)
        stdout_file = session_ids_stdout_files.get(session_id, None)
        file = stdout_file if stdout_file is not None else sys.stdout
        if 'file' not in kwargs:
            kwargs['file'] = file
        print(*args, **kwargs)

    #  builtins.print = _print_monkey_patch

    def _print_line(
        text,
        session_id: str,
        action_id: str,
        completed: bool = False,
        result=None,
        output_stream=None,
    ):
        line_data = {
            'completed': completed,
            'session_id': session_id,
            'action_id': action_id,
            'text': text,
        }
        if result is not None:
            _success, _message = result
            line_data['success'] = _success
            line_data['message'] = _message

        (output_stream or sys.stdout).write(json.dumps(line_data, separators=(',', ':')) + '\n')
        (output_stream or sys.stdout).flush()

    def _entry_from_input_data(input_data: Dict[str, Any], output_stream=None):
        sysargs = input_data.get('sysargs', None)
        _patch_args = input_data.get('patch_args', None)
        session_id = input_data.get('session_id', None)
        action_id = input_data.get('action_id', None)
        error_msg = input_data.get('error', "No sysargs provided.") if not sysargs else None
        env = input_data.get('env', os.environ)

        if error_msg:
            _print_line(
                {"error": error_msg},
                session_id,
                action_id,
                completed=True,
                result=(False, error_msg),
                output_stream=output_stream,
            )
            return

        #  with redirect_stdout(output_stream):
            #  with redirect_stderr(output_stream):
            with set_env(env):
                action_success, action_msg = _entry(
                    sysargs,
                    _patch_args=_patch_args,
                    _use_cli_daemon=False,
                )

        _print_line(
            None,
            session_id,
            action_id,
            completed=True,
            result=(action_success, action_msg),
            output_stream=output_stream,
        )

    while True:
        try:
            input_data_str = prompt('', silent=True)
        except (KeyboardInterrupt, EOFError):
            return True, "Exiting CLI daemon process."

        try:
            input_data = json.loads(input_data_str)
        except Exception as e:
            input_data = {"error": str(e)}

        session_id = input_data.get('session_id', None)
        if session_id is None:
            session_id = uuid.uuid4()
            input_data['session_id'] = session_id

        _signal = input_data.get('signal', None)
        if _signal:
            signal_to_send = getattr(signal, _signal)
            thread_to_stop = session_ids_threads.get(session_id, None)
            thread_to_stop.send_signal(signal_to_send)
            continue

        action_id = input_data.get('action_id', uuid.uuid4().hex)
        session_dir_path = CLI_CACHE_RESOURCES_PATH / session_id
        session_dir_path.mkdir(parents=True, exist_ok=True)
        stdout_file_path = _get_cli_stream_path(session_id, action_id, 'stdout')
        stdout_file = StdinFile(stdout_file_path, decode=False)

        thread = Thread(
            target=_entry_from_input_data,
            daemon=True,
            args=(input_data, stdout_file),
        )
        session_ids_threads[session_id] = thread
        thread_idents_session_ids[thread.ident] = session_id
        session_ids_stdout_files[session_id] = stdout_file
        thread.start()


def get_cli_daemon():
    """
    Get the CLI daemon.
    """
    from meerschaum.utils.daemon import Daemon
    return Daemon(
        cli_daemon_loop,
        env=dict(os.environ),
        daemon_id='.cli',
        label='Internal Meerschaum CLI Daemon',
        properties={
            'logs': {
                'write_timestamps': False,
                'refresh_files_seconds': 86400,
                'max_file_size': 1_000_000,
            },
        },
    )
