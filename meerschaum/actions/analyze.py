#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Analyze pipes' target tables to refresh planner statistics.
"""

from meerschaum.utils.typing import SuccessTuple, Any, List, Optional


def analyze(action: Optional[List[str]] = None, **kw: Any) -> SuccessTuple:
    """
    Analyze pipes' target tables to refresh the database planner's statistics.

    This does not reclaim disk space; it helps the query planner choose better plans
    after large syncs.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes': _analyze_pipes,
    }
    return choose_subaction(action, options, **kw)


def _analyze_pipes(
    action: Optional[List[str]] = None,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Analyze the target tables for the selected pipes.
    """
    from meerschaum import get_pipes
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.formatting import pprint

    pipes = get_pipes(as_list=True, debug=debug, **kw)
    if len(pipes) == 0:
        return False, "No pipes to analyze."

    question = "Are you sure you want to analyze the target tables for these pipes?\n\n"
    for pipe in pipes:
        question += f"    - {pipe}\n"
    question += "\n"

    if force:
        answer = True
    else:
        answer = yes_no(question, default='n', noask=noask, yes=yes)

    if not answer:
        return False, "No pipes were analyzed."

    success_dict = {}
    successes, fails = 0, 0
    for pipe in pipes:
        analyze_success, analyze_msg = pipe.analyze(debug=debug)
        success_dict[pipe] = analyze_msg
        if analyze_success:
            successes += 1
        else:
            fails += 1
            warn(analyze_msg, stack=False)

    if debug:
        dprint("Results for analyzing pipes.")
        pprint(success_dict)

    msg = (
        f"Finished analyzing {len(pipes)} pipe"
        + ('s' if len(pipes) != 1 else '')
        + f"\n    ({successes} succeeded, {fails} failed)."
    )
    return successes > 0, msg
