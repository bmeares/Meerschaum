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
    from meerschaum.actions.start import _complete_start_jobs
    if action is None:
        action = []

    options = {
        'job' : _complete_start_jobs,
        'jobs' : _complete_start_jobs,
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
        timeout_seconds: Optional[int] = None,
        noask: bool = False,
        force: bool = False,
        yes: bool = False,
        nopretty: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Stop running jobs that were started with `-d` or `start job`.
    
    To see running processes, run `show jobs`.
    """
    from meerschaum.utils.formatting._jobs import pprint_jobs
    from meerschaum.utils.daemon import (
        get_filtered_daemons, get_running_daemons, get_stopped_daemons, get_paused_daemons,
    )
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.prompt import yes_no
    daemons = get_filtered_daemons(action, warn=(not nopretty))
    _running_daemons = get_running_daemons(daemons)
    _paused_daemons = get_paused_daemons(daemons)
    _stopped_daemons = get_stopped_daemons(daemons)
    if action and _stopped_daemons and not nopretty:
        warn(
            f"Skipping stopped job" + ("s" if len(_stopped_daemons) > 1 else '') + " '" +
                ("', '".join(d.daemon_id for d in _stopped_daemons)) + "'.",
            stack = False
        )

    daemons_to_stop = _running_daemons + _paused_daemons
    if not daemons_to_stop:
        return False, "No running or paused jobs to stop."

    if not action:
        if not force:
            pprint_jobs(daemons_to_stop)
            if not yes_no(
                "Stop the above jobs?",
                noask=noask, yes=yes, default='n'
            ):
                return False, "No jobs were stopped."

    _quit_daemons, _kill_daemons = [], []
    for d in daemons_to_stop:
        quit_success, quit_msg = d.quit(timeout=timeout_seconds)
        if quit_success:
            _quit_daemons.append(d)
            continue
        else:
            warn(
                f"Failed to gracefully quit job '{d.daemon_id}', attempting to terminate:\n   "
                + f"{quit_msg}",
                stack = False,
            )

        kill_success, kill_msg = d.kill(timeout=timeout_seconds)
        if kill_success:
            _kill_daemons.append(d)
            continue
        if not nopretty:
            warn(f"Failed to kill job '{d.daemon_id}' (PID {d.pid}):\n{kill_msg}", stack=False)

    msg = (
        (("Stopped job" + ("s" if len(_quit_daemons) != 1 else '') +
            " '" + "', '".join([d.daemon_id for d in _quit_daemons]) + "'.")
            if _quit_daemons else '')
        + (("\n" if _quit_daemons else "")
           + ("Killed job" + ("s" if len(_kill_daemons) != 1 else '') +
            " '" + "', '".join([d.daemon_id for d in _kill_daemons]) + "'.")
            if _kill_daemons else '')
    )
    return (len(_quit_daemons + _kill_daemons) > 0), msg


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
stop.__doc__ += _choices_docstring('stop')
