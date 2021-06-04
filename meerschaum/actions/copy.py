#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for copying elements.
"""

from __future__ import annotations
from meerschaum.utils.typing import Union, Any, Sequence, SuccessTuple, Optional, Tuple, List

def copy(
        action : Optional[List[str]] = None,
        **kw : Any
    ) -> SuccessTuple:
    """
    Copy pipes' attributes and make new pipes.

    Command:
        `copy {option}`

    Example:
        `copy pipes`
    """
    from meerschaum.utils.misc import choose_subaction
    if action is None:
        action = []
    options = {
        'pipes'      : _copy_pipes,
    }
    return choose_subaction(action, options, **kw)

def _copy_pipes(
        yes: bool = False,
        noask: bool = False,
        force: bool = False,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Copy pipes' attributes and make new pipes.
    """
    from meerschaum import get_pipes, Pipe
    from meerschaum.utils.prompt import prompt, yes_no
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.formatting import print_tuple
    from meerschaum.utils.formatting._shell import clear_screen
    pipes = get_pipes(as_list=True, **kw)
    successes = 0
    for p in pipes:
        ck = prompt(f"Connector keys for copy of pipe '{p}':", default=p.connector_keys)
        mk = prompt(f"Metric key for copy of pipe '{p}':", default=p.metric_key)
        lk = prompt(f"Location key for copy of pipe '{p}' (empty to omit):")
        if lk == '':
            lk = None
        _new_pipe = Pipe(
            ck, mk, lk,
            parameters=p.parameters.copy(),
        )
        instance_keys = prompt(f"Meerschaum instance to store new pipe '{_new_pipe}':", default=p.instance_keys)
        _new_pipe.instance_keys = instance_keys
        if _new_pipe.id is not None:
            warn(f"New pipe '{_new_pipe}' already exists. Skipping...", stack=False)
            continue
        _register_success_tuple = _new_pipe.register(debug=debug)
        if not _register_success_tuple[0]:
            warn(f"Failed to register new pipe '{_new_pipe}'.", stack=False)
            continue

        clear_screen(debug=debug)
        successes += 1
        print_tuple(
            (True, f"Successfully copied attributes of pipe '{p}' "
                + f"({p.instance_keys}) into '{_new_pipe}' ({_new_pipe.instance_keys}).")
        )
        if (
            force or yes_no(
                (
                    f"Do you want to copy data from pipe '{p}' into new pipe '{_new_pipe}'?\n\n"
                    + "If you specified `--begin`, `--end` or `--params`, data will be filtered."
                ),
                    noask=noask, yes=yes
                )
        ):
            _new_pipe.sync(p.get_data(debug=debug, **kw))

    msg = (
        "No pipes were copied." if successes == 0
        else (f"Copied {successes} pipe" + ("s" if successes != 1 else '') + '.')
    )

    return successes > 0, msg


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
copy.__doc__ += _choices_docstring('copy')
