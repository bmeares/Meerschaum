#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Functions for dropping elements
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any, Optional, List


def drop(
    action: Optional[List[str]] = None,
    **kw: Any
) -> SuccessTuple:
    """
    Drop pipe data (maintaining registration) or tables.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes'  : _drop_pipes,
        'indices': _drop_indices,
        'index': _drop_indices,
        'indexes': _drop_indices,
    }
    return choose_subaction(action, options, **kw)


def _drop_pipes(
    action: Optional[List[str]] = None,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Drop pipes' tables but keep pipe metadata registration.
    """
    from meerschaum.utils.prompt import yes_no
    from meerschaum import get_pipes
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.formatting import pprint

    pipes = get_pipes(as_list=True, debug=debug, **kw)
    if len(pipes) == 0:
        return False, "No pipes to drop."

    question = (
        "Are you sure you want to drop these tables?\n    "
        "Data will be lost and will need to be resynced.\n\n"
    )
    seen_targets = set()
    for pipe in pipes:
        target = pipe.target
        if target in seen_targets:
            continue
        question += f"    - {target}" + "\n"
        seen_targets.add(target)

    question += '\n'
    if force:
        answer = True
    else:
        answer = yes_no(question, default='n', noask=noask, yes=yes)

    if not answer:
        return False, "No pipes were dropped."

    success_dict = {}
    successes, fails = 0, 0
    msg = ""

    for pipe in pipes:
        drop_success, drop_msg = pipe.drop(debug=debug)
        success_dict[pipe] = drop_msg
        if drop_success:
            successes += 1
        else:
            fails += 1
            warn(drop_msg, stack=False)
    
    if debug:
        dprint("Results for dropping pipes.")
        pprint(success_dict)

    msg = (
        f"Finished dropping {len(pipes)} pipe"
        + ('s' if len(pipes) != 1 else '')
        + f"\n    ({successes} succeeded, {fails} failed)."
    )
    return successes > 0, msg


def _drop_indices(
    action: Optional[List[str]] = None,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Drop pipes' tables but keep pipe metadata registration.
    """
    from meerschaum.utils.prompt import yes_no
    from meerschaum import get_pipes
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.formatting import pprint

    pipes = get_pipes(as_list=True, debug=debug, **kw)
    if len(pipes) == 0:
        return False, "No pipes to drop."

    question = "Are you sure you want to drop these indices?\n"
    for pipe in pipes:
        indices = pipe.get_indices()
        if not indices:
            continue
        question += f"\n{pipe}\n"
        for ix_key, index_name in indices.items():
            question += f"    - {index_name}\n"

    question += '\n'
    if force:
        answer = True
    else:
        answer = yes_no(question, default='n', noask=noask, yes=yes)

    if not answer:
        return False, "No pipes were dropped."

    success_dict = {}
    successes, fails = 0, 0
    msg = ""

    for pipe in pipes:
        drop_success, drop_msg = pipe.drop_indices(debug=debug)
        success_dict[pipe] = drop_msg
        if drop_success:
            successes += 1
        else:
            fails += 1
            warn(drop_msg, stack=False)
    
    if debug:
        dprint("Results for dropping indices.")
        pprint(success_dict)

    msg = (
        f"Finished dropping indices for {len(pipes)} pipe"
        + ('s' if len(pipes) != 1 else '')
        + f"\n    ({successes} succeeded, {fails} failed)."
    )
    return successes > 0, msg


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
drop.__doc__ += _choices_docstring('drop')
