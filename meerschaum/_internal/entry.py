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

from meerschaum.utils.typing import SuccessTuple, List, Optional, Dict, Callable, Any
from meerschaum.config.static import STATIC_CONFIG as _STATIC_CONFIG

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
    from meerschaum.config.static import STATIC_CONFIG
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
    from meerschaum.config.static import STATIC_CONFIG
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

    skip_schedule = False

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
            skip_schedule = True

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
        skip_schedule = True

    kw['action'] = remove_leading_action(kw['action'], _actions=_actions)

    do_action = functools.partial(
        _do_action_wrapper,
        action_function,
        plugin_name,
        **kw
    )

    if kw.get('schedule', None) and not skip_schedule:
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


def _do_action_wrapper(action_function, plugin_name, **kw):
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
                f"Failed to execute `{action_name}` "
                + "with exception:\n\n" +
                f"{e}."
                + (
                    "\n\nRun again with '--debug' to see a full stacktrace."
                    if not kw.get('debug', False) else ''
                )
            )
        except KeyboardInterrupt:
            result = False, f"Cancelled action `{action_name.lstrip()}`."
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
