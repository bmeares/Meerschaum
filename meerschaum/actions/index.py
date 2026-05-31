#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for indexing tables.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any, Optional, List


def index(
    action: Optional[List[str]] = None,
    **kw: Any
) -> SuccessTuple:
    """
    Create pipes' indices.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes'  : _index_pipes,
    }
    return choose_subaction(action, options, **kw)


def _index_pipes(
    action: Optional[List[str]] = None,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    nopretty: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Create pipes' indices.
    """
    import os
    import contextlib
    from meerschaum import get_pipes
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.formatting._shell import progress
    from meerschaum.utils.daemon import running_in_daemon
    from meerschaum._internal.static import STATIC_CONFIG

    pipes = get_pipes(as_list=True, debug=debug, **kw)
    if len(pipes) == 0:
        return False, "No pipes to index."

    noninteractive_val = os.environ.get(STATIC_CONFIG['environment']['noninteractive'], None)
    noninteractive = str(noninteractive_val).lower() in ('1', 'true', 'yes')
    _progress = (
        progress()
        if (
            kw.get('shell', False)
            and not noninteractive
            and not running_in_daemon()
            and not nopretty
            and not debug
        )
        else None
    )

    success_dict = {}
    successes, fails = 0, 0

    cm = _progress if _progress is not None else contextlib.nullcontext()
    with cm:
        task = (
            _progress.add_task("Indexing pipes...", total=len(pipes))
            if _progress is not None
            else None
        )
        for pipe in pipes:
            if not nopretty:
                info(f"Creating indices for {pipe}...")
            index_success, index_msg = pipe.create_indices(columns=(action or None), debug=debug)
            success_dict[pipe] = index_msg
            if index_success:
                successes += 1
            else:
                fails += 1
                warn(index_msg, stack=False)

            if _progress is not None:
                _progress.advance(task)

    if debug:
        dprint("Results for indexing pipes.")
        pprint(success_dict)

    msg = (
        f"Finished indexing {len(pipes)} pipe"
        + ('s' if len(pipes) != 1 else '')
        + f"\n    ({successes} succeeded, {fails} failed)."
    )
    return successes > 0, msg


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
index.__doc__ += _choices_docstring('index')
