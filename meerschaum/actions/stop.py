#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Stop running jobs that were started with `-d` or `start job`.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, List, Dict, SuccessTuple, Any

def stop(action: Optional[List[str]] = None, **kw) -> SuccessTuple:
    """
    Stop running jobs.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'jobs': _stop_jobs,
    }
    return choose_subaction(action, options, **kw)


def _complete_stop(
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

    _complete_stop_jobs = partial(
        _complete_delete_jobs,
        _get_job_method=('running', 'paused', 'restart'),
    )

    options = {
        'job' : _complete_stop_jobs,
        'jobs' : _complete_stop_jobs,
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


def _stop_jobs(
    action: Optional[List[str]] = None,
    executor_keys: Optional[str] = None,
    timeout_seconds: Optional[int] = None,
    noask: bool = False,
    force: bool = False,
    yes: bool = False,
    nopretty: bool = False,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    Stop running jobs that were started with `-d` or `start job`.
    
    To see running processes, run `show jobs`.
    """
    from meerschaum.jobs import (
        get_filtered_jobs,
        get_running_jobs,
        get_paused_jobs,
        get_stopped_jobs,
        get_restart_jobs,
    )
    from meerschaum.utils.formatting._jobs import pprint_jobs
    from meerschaum.utils.daemon import (
        get_filtered_daemons, get_running_daemons, get_stopped_daemons, get_paused_daemons,
    )
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.prompt import yes_no

    jobs = get_filtered_jobs(executor_keys, action, warn=(not nopretty))
    running_jobs = get_running_jobs(executor_keys, jobs)
    paused_jobs = get_paused_jobs(executor_keys, jobs)
    restart_jobs = get_restart_jobs(executor_keys, jobs)
    stopped_jobs = {
        name: job
        for name, job in get_stopped_jobs(executor_keys, jobs).items()
        if name not in restart_jobs
    }

    jobs_to_stop = {
        **running_jobs,
        **paused_jobs,
        **restart_jobs,
    }

    if action and stopped_jobs and not nopretty:
        warn(
            "Skipping stopped job"
            + ("s" if len(stopped_jobs) != 1 else '')
            + " '"
            + ("', '".join(name for name in stopped_jobs)) + "'.",
            stack=False,
        )

    if not jobs_to_stop:
        return False, "No running, paused or restarting jobs to stop."

    if not action:
        if not force:
            pprint_jobs(jobs_to_stop)
            if not yes_no(
                "Stop the above jobs?",
                noask=noask, yes=yes, default='n'
            ):
                return False, "No jobs were stopped."

    job_success_tuples = {}
    for name, job in jobs_to_stop.items():
        stop_success, stop_msg = job.stop(
            timeout_seconds=timeout_seconds,
            debug=debug,
        )
        job_success_tuples[name] = (stop_success, stop_msg)

    num_success = sum(
        (
            1
            for name, (stop_success, stop_msg) in job_success_tuples.items()
            if stop_success
        )
    )
    num_fail = sum(
        (
            1
            for name, (stop_success, stop_msg) in job_success_tuples.items()
            if not stop_success
        )
    )
    success = num_success > 0
    msg = (
        f"Stopped {num_success} job"
        + ('s' if num_success != 1 else '')
        + '.'
    )
    if num_fail > 0:
        msg += (
            f"\nFailed to stop {num_fail} job"
            + ('s' if num_fail != 1 else '')
            + '.'
        )

    return success, msg


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
stop.__doc__ += _choices_docstring('stop')
