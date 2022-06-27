#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Functions for dropping elements
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Union, Any, Optional, Sequence, List

def drop(
        action: Optional[List[str]] = None,
        **kw: Any
    ) -> SuccessTuple:
    """
    Drop pipe data (maintaining registration) or tables.
    """
    from meerschaum.utils.misc import choose_subaction

    options = {
        'pipes'  : _drop_pipes,
        'tables' : _drop_tables,
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
    from meerschaum.utils.prompt import prompt, yes_no
    from meerschaum import get_pipes
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.formatting import pprint
    pipes = get_pipes(as_list=True, debug=debug, **kw)
    if len(pipes) == 0:
        return False, "No pipes to drop."
    question = (
        "Are you sure you want to drop these tables?\n    "
        + "Data will be lost and will need to be resynced.\n\n"
    )
    for p in pipes:
        question += f"    - {p.target}" + "\n"
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

    for p in pipes:
        success_tup = p.drop(debug=debug)
        success_dict[p] = success_tup[1]
        if success_tup[0]:
            successes += 1
        else:
            fails += 1
            warn(success_tup[1], stack=False)
    
    if debug:
        dprint("Results for dropping pipes.")
        pprint(success_dict)

    msg = (
        f"Finished dropping {len(pipes)} pipes\n" + 
        f"    ({successes} succeeded, {fails} failed)."
    )
    return successes > 0, msg

def _drop_tables(
        action: Optional[List[str]] = None,
        **kw: Any
    ) -> SuccessTuple:
    """
    Drop SQL tables. WARNING: This is very dangerous!
    """
    return False, "Not implemented"

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
drop.__doc__ += _choices_docstring('drop')
