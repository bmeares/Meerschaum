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


def _pause_jobs(
        action: Optional[List[str]] = None,
        noask: bool = False,
        force: bool = False,
        yes: bool = False,
        nopretty: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Pause running jobs that were started with `-d` or `start job`.
    
    To see running jobs, run `show jobs`.
    """
    from meerschaum.utils.formatting._jobs import pprint_jobs
    from meerschaum.utils.daemon import (
        get_filtered_daemons, get_running_daemons, get_stopped_daemons, get_paused_daemons,
    )
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.misc import items_str
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

    daemons_to_pause = _running_daemons + _paused_daemons
    if not daemons_to_pause:
        return False, "No running jobs to pause. You can start jobs with `-d` or `start jobs`."

    if not action:
        if not force:
            pprint_jobs(_running_daemons)
            if not yes_no(
                "Pause all running jobs?",
                noask=noask, yes=yes, default='n'
            ):
                return False, "No jobs were paused."

    successes, fails = [], []
    for d in daemons_to_pause:
        pause_success, pause_msg = d.pause()
        (successes if pause_success else fails).append(d.daemon_id)

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
