#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Restart stopped jobs which have not been manually stopped.
"""

from meerschaum.utils.typing import SuccessTuple, Optional, List, Any

def restart(
    action: Optional[List[str]] = None,
    executor_keys: Optional[str] = None,
    **kwargs: Any
) -> SuccessTuple:
    """
    Restart stopped jobs which have not been manually stopped.
    """
    from meerschaum.actions import choose_subaction
    attach_options = {
        'jobs': _restart_jobs,
    }
    return choose_subaction(action, attach_options, executor_keys=executor_keys, **kwargs)


def _complete_restart(
    action: Optional[List[str]] = None,
    **kw: Any
) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    from functools import partial
    from meerschaum.actions.delete import _complete_delete_jobs

    if action is None:
        action = []

    _complete_restart_jobs = partial(
        _complete_delete_jobs,
        _get_job_method='restart',
    )

    options = {
        'job': _complete_restart_jobs,
        'jobs': _complete_restart_jobs,
    }

    if (
        len(action) > 0 and action[0] in options
            and kw.get('line', '').split(' ')[-1] != action[0]
    ):
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum._internal.shell import default_action_completer
    return default_action_completer(action=(['restart'] + action), **kw)


def _restart_jobs(
    action: Optional[List[str]] = None,
    executor_keys: Optional[str] = None,
    loop: bool = False,
    min_seconds: Optional[float] = 1.0,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Restart stopped jobs which have not been manually stopped.
    """
    import time
    from meerschaum.jobs import (
        get_restart_jobs,
        get_filtered_jobs,
        check_restart_jobs,
    )
    from meerschaum.utils.misc import items_str
    from meerschaum.utils.warnings import info
    action = action or []

    while True:
        jobs = get_filtered_jobs(
            executor_keys or 'local',
            action,
            include_hidden=True,
            combine_local_and_systemd=False,
            debug=debug,
        )
        restart_jobs = get_restart_jobs(executor_keys, jobs, debug=debug) if not action else jobs
        if not restart_jobs and not loop:
            return True, "No jobs need to be restarted."

        info(
            "Checking job"
            + ('s' if len(restart_jobs) != 1 else '')
            + ' '
            + items_str(list(restart_jobs.keys()))
            + '...'
        )

        check_success, check_msg = check_restart_jobs(
            executor_keys,
            restart_jobs,
            debug=debug,
        )
        if not loop:
            break

        if min_seconds is not None and min_seconds != 0.0:
            info(f"Sleeping for {min_seconds} seconds...")
            time.sleep(min_seconds)

    return check_success, check_msg
