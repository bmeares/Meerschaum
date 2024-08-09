#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Attach to running jobs.
"""

import meerschaum as mrsm
from meerschaum.utils.typing import Optional, List, Any, SuccessTuple

def attach(
    action: Optional[List[str]] = None,
    executor_keys: Optional[str] = None,
    **kwargs: Any
) -> SuccessTuple:
    """
    Attach to a job, and prompt the user when blocking on input.
    """
    from meerschaum.actions import choose_subaction
    attach_options = {
        'jobs': _attach_jobs,
        'logs': _attach_logs,
    }
    return choose_subaction(action, attach_options, executor_keys=executor_keys, **kwargs)


def _complete_attach(
    action: Optional[List[str]] = None,
    **kw: Any
) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    from meerschaum.actions.delete import _complete_delete_jobs

    if action is None:
        action = []

    options = {
        'job': _complete_delete_jobs,
        'jobs': _complete_delete_jobs,
        'log': _complete_delete_jobs,
        'logs': _complete_delete_jobs,
    }

    if (
        len(action) > 0 and action[0] in options
            and kw.get('line', '').split(' ')[-1] != action[0]
    ):
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum._internal.shell import default_action_completer
    return default_action_completer(action=(['attach'] + action), **kw)


def _attach_jobs(
    action: Optional[List[str]] = None,
    name: Optional[str] = None,
    executor_keys: Optional[str] = None,
    **kwargs: Any
) -> SuccessTuple:
    """
    Attach to a job, and prompt the user when blocking on input.
    """
    action = action or []
    if not action and not name:
        return False, "Provide the name of the job to attach to."

    name = name or action[0]
    job = mrsm.Job(name, executor_keys=executor_keys)
    other_executor_keys = 'systemd' if executor_keys in (None, 'local') else 'local'
    if not job.exists():
        other_job = mrsm.Job(name, executor_keys=other_executor_keys)
        if not other_job.exists():
            return False, f"Job '{job.name}' does not exist."

        job = other_job

    success, message = True, "Success"

    def _capture_result(result: SuccessTuple):
        nonlocal success, message
        success, message = result

    try:
        job.monitor_logs(
            stop_callback_function=_capture_result,
            accept_input=True,
            stop_on_exit=True,
            strip_timestamps=True,
        )
    except KeyboardInterrupt:
        pass

    return success, message


def _attach_logs(*args, **kwargs) -> SuccessTuple:
    """
    Attach to jobs' logs.
    """
    from meerschaum.actions.show import _show_logs
    return _show_logs(*args, **kwargs)


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
attach.__doc__ += _choices_docstring('attach')
