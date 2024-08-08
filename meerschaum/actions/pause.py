#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Pause running jobs that were started with `-d` or `start job`.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, List, Dict, SuccessTuple, Any

def pause(action: Optional[List[str]] = None, **kw) -> SuccessTuple:
    """
    Pause running jobs.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'jobs': _pause_jobs,
    }
    return choose_subaction(action, options, **kw)


def _complete_pause(
        action: Optional[List[str]] = None,
        **kw: Any
    ) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    from meerschaum.actions.delete import _complete_delete_jobs
    from functools import partial

    if action is None:
        action = []

    _complete_pause_jobs = partial(
        _complete_delete_jobs,
        _get_job_method='running',
    )

    options = {
        'job': _complete_pause_jobs,
        'jobs': _complete_pause_jobs,
    }

    if (
        len(action) > 0 and action[0] in options
            and kw.get('line', '').split(' ')[-1] != action[0]
    ):
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum._internal.shell import default_action_completer
    return default_action_completer(action=(['start'] + action), **kw)


def _pause_jobs(
    action: Optional[List[str]] = None,
    executor_keys: Optional[str] = None,
    noask: bool = False,
    force: bool = False,
    yes: bool = False,
    nopretty: bool = False,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    Pause (suspend) running jobs.
    """
    from meerschaum.utils.formatting._jobs import pprint_jobs
    from meerschaum.utils.daemon import (
        get_filtered_daemons, get_running_daemons, get_stopped_daemons, get_paused_daemons,
    )
    from meerschaum.jobs import (
        get_filtered_jobs,
        get_running_jobs,
        get_stopped_jobs,
        get_paused_jobs,
    )
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.misc import items_str

    jobs = get_filtered_jobs(executor_keys, action, debug=debug, warn=(not nopretty))
    running_jobs = get_running_jobs(executor_keys, jobs, debug=debug)
    paused_jobs = get_paused_jobs(executor_keys, jobs, debug=debug)
    stopped_jobs = get_stopped_jobs(executor_keys, jobs, debug=debug)

    if action and stopped_jobs and not nopretty:
        warn(
            f"Skipping stopped job" + ("s" if len(stopped_jobs) > 1 else '')
            + " '"
            + ("', '".join(stopped_jobs.keys()))
            + "'.",
            stack=False,
        )

    jobs_to_pause = {**running_jobs, **paused_jobs}
    if not jobs_to_pause:
        return False, "No running jobs to pause. You can start jobs with `-d` or `start jobs`."

    if not action:
        if not force:
            pprint_jobs(running_jobs)
            if not yes_no(
                "Pause all running jobs?",
                noask=noask, yes=yes, default='n'
            ):
                return False, "No jobs were paused."

    successes, fails = [], []
    for name, job in jobs_to_pause.items():
        pause_success, pause_msg = job.pause()
        (successes if pause_success else fails).append(name)

    msg = (
        (
            (
                "Successfully paused job" + ("s" if len(successes) != 1 else '')
                + f" {items_str(successes)}."
                + (
                    '\n'
                    if fails
                    else ''
                )
            )
            if successes
            else ''
        )
        + (
            "Failed to pause job" + ("s" if len(fails) != 1 else '')
            + f" {items_str(fails)}."
            if fails
            else ''
        )
    )
    return len(fails) == 0, msg


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
pause.__doc__ += _choices_docstring('pause')
