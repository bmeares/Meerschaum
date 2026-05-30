#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Compress pipes' target tables to reduce disk usage.
"""

from meerschaum.utils.typing import SuccessTuple, Any, List, Optional


def compress(action: Optional[List[str]] = None, **kw: Any) -> SuccessTuple:
    """
    Compress pipes' target tables to reduce disk usage.

    For TimescaleDB hypertables this enables native compression and installs a
    compression policy so future synced chunks are compressed automatically.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes': _compress_pipes,
    }
    return choose_subaction(action, options, **kw)


def _compress_pipes(
    action: Optional[List[str]] = None,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Compress the target tables for the selected pipes.
    """
    from meerschaum import get_pipes
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.formatting import pprint

    pipes = get_pipes(as_list=True, debug=debug, **kw)
    if len(pipes) == 0:
        return False, "No pipes to compress."

    question = (
        "Are you sure you want to compress the target tables for these pipes?\n"
        "    This rewrites table data and may take a while.\n\n"
    )
    for pipe in pipes:
        question += f"    - {pipe}\n"
    question += "\n"

    if force:
        answer = True
    else:
        answer = yes_no(question, default='n', noask=noask, yes=yes)

    if not answer:
        return False, "No pipes were compressed."

    success_dict = {}
    successes, fails = 0, 0
    for pipe in pipes:
        compress_success, compress_msg = pipe.compress(debug=debug)
        success_dict[pipe] = compress_msg
        if compress_success:
            successes += 1
        else:
            fails += 1
            warn(compress_msg, stack=False)

    if debug:
        dprint("Results for compressing pipes.")
        pprint(success_dict)

    msg = (
        f"Finished compressing {len(pipes)} pipe"
        + ('s' if len(pipes) != 1 else '')
        + f"\n    ({successes} succeeded, {fails} failed)."
    )
    return successes > 0, msg
