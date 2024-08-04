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
    }
    return choose_subaction(action, attach_options, **kwargs)


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

    while True:
        try:
            pass
            #  job.mon
        except KeyboardInterrupt:
            break

    return True, "Success"

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
attach.__doc__ += _choices_docstring('attach')
