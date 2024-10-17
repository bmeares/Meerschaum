#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Functions for clearing pipes.
"""

from __future__ import annotations

from datetime import datetime
import meerschaum as mrsm
from meerschaum.utils.typing import List, SuccessTuple, Any, Optional


def clear(
    action: Optional[List[str]] = None,
    **kw: Any
) -> SuccessTuple:
    """
    Clear pipes of their data, or clear the screen.

    Usage:
    - clear
        - Clear the screen.
    - clear pipes --begin 2022-01-01
        - For all pipes, remove rows newer than or equal to `2022-01-01 00:00:00`.

    """
    from meerschaum.actions import choose_subaction
    if not action:
        from meerschaum.utils.formatting._shell import clear_screen
        return clear_screen(kw.get('debug', False)), ''

    options = {
        'pipes': _clear_pipes,
    }
    return choose_subaction(action, options, **kw)


def _clear_pipes(
    action: Optional[List[str]] = None,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    connector_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    mrsm_instance: Optional[str] = None,
    location_keys: Optional[List[str]] = None,
    yes: bool = False,
    force: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Clear pipes' data without dropping any tables.

    """
    from meerschaum import get_pipes
    from meerschaum.utils.formatting import print_tuple

    successes = {}
    fails = {}

    pipes = get_pipes(
        as_list=True, connector_keys=connector_keys, metric_keys=metric_keys,
        location_keys=location_keys, mrsm_instance=mrsm_instance, debug=debug, **kw
    )

    if not force:
        if not _ask_with_rowcounts(pipes, begin=begin, end=end, debug=debug, yes=yes, **kw):
            return False, "No rows were deleted."

    for pipe in pipes:
        clear_success, clear_msg = pipe.clear(
            begin=begin,
            end=end,
            yes=yes,
            debug=debug,
            **kw
        )
        print_tuple((clear_success, clear_msg))
        (successes if clear_success else fails)[pipe] = clear_msg

    success = len(successes) > 0
    msg = (
        f"Finished clearing {len(pipes)} pipe" + ('s' if len(pipes) != 1 else '')
        + f'\n    ({len(successes)} succeeded, {len(fails)} failed).'
    )

    return success, msg


def _ask_with_rowcounts(
    pipes: List[mrsm.Pipe],
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    yes: bool = False,
    nopretty: bool = False,
    noask: bool = False,
    debug: bool = False,
    **kw
) -> bool:
    """
    Count all of the pipes' rowcounts and confirm with the user that these rows need to be deleted.

    """
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.misc import print_options
    from meerschaum.utils.warnings import warn
    for pipe in pipes:
        if not pipe.columns or 'datetime' not in pipe.columns:
            _dt = pipe.guess_datetime()
            is_guess = True
        else:
            _dt = pipe.get_columns('datetime')
            is_guess = False

        if begin is not None or end is not None:
            if is_guess:
                if _dt is None:
                    warn(
                        f"No datetime could be determined for {pipe}!\n"
                        + "    THIS WILL DELETE THE ENTIRE TABLE!",
                        stack=False
                    )
                else:
                    warn(
                        f"A datetime wasn't specified for {pipe}.\n"
                        + f"    Using column \"{_dt}\" for datetime bounds...",
                        stack=False
                    )


    pipes_rowcounts = {p: p.get_rowcount(begin=begin, end=end, debug=debug) for p in pipes} 
    print_options(
        [str(p) + f'\n{rc}\n' for p, rc in pipes_rowcounts.items()],
        header='Number of Rows to be Deleted'
    )
    total_num_rows = sum([rc for p, rc in pipes_rowcounts.items()])
    question = (
        f"Are you sure you want to delete {total_num_rows} rows across {len(pipes)} pipe"
        + ('s' if len(pipes) != 1 else '')
        + " in the following range?\n"
    )
    range_text = (
        (f"\n    Newer than (>=) {begin}" if begin is not None else '')
        + ("\n    Older than (<)" + (' ' if begin else '') + f" {end}" if end is not None else '')
    ) if (begin is not None or end is not None) else '\n    Unbounded (delete all rows)!'
    question += range_text + '\n\n'

    return yes_no(question, yes=yes, nopretty=nopretty, noask=noask, default='n')

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
clear.__doc__ += _choices_docstring('clear')
