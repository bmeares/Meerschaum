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
    **kwargs: Any
) -> SuccessTuple:
    """
    Attach to a job.
    """
    from meerschaum.actions import choose_subaction
    attach_options = {
        'jobs': _attach_jobs,
        'logs': _attach_logs,
    }
    return choose_subaction(action, attach_options, **kwargs)


def _complete_attach(
    action: Optional[List[str]] = None,
    **kw: Any
) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    from meerschaum.actions.start import _complete_start_jobs

    if action is None:
        action = []

    options = {
        'job': _complete_start_jobs,
        'jobs': _complete_start_jobs,
        'log': _complete_start_jobs,
        'logs': _complete_start_jobs,
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
    if not action and not name:
        return False, "Provide the name of the job to attach to."

    name = name or action[0]
    job = mrsm.Job(name, executor_keys=executor_keys)
    if not job.exists():
        return False, f"Job '{job.name}' does not exist."

    job.monitor_logs(
        stop_on_exit=True,
        strip_timestamps=True,
    )

    return True, "Success"


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
