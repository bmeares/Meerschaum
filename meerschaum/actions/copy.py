#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for copying elements.
"""

from __future__ import annotations
from meerschaum.utils.typing import Union, Any, Sequence, SuccessTuple, Optional, Tuple, List

def copy(
        action: Optional[List[str]] = None,
        **kw : Any
    ) -> SuccessTuple:
    """
    Duplicate connectors or pipes.
    
    Command:
        `copy {pipes, connectors}`
    
    Example:
        `copy pipes`

    """
    from meerschaum.utils.misc import choose_subaction
    if action is None:
        action = []
    options = {
        'pipes'      : _copy_pipes,
        'connectors' : _copy_connectors,
    }
    return choose_subaction(action, options, **kw)


def _complete_copy(
        action : Optional[List[str]] = None,
        **kw : Any
    ) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.

    """
    from meerschaum.actions.start import _complete_start_jobs
    from meerschaum.actions.edit import _complete_edit_config
    if action is None:
        action = []
    options = {
        'connector': _complete_copy_connectors,
        'connectors': _complete_copy_connectors,
    }

    if (
        len(action) > 0 and action[0] in options
            and kw.get('line', '').split(' ')[-1] != action[0]
    ):
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum._internal.shell import default_action_completer
    return default_action_completer(action=(['copy'] + action), **kw)


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
        ck = prompt(f"Connector keys for copy of {p}:", default=p.connector_keys)
        mk = prompt(f"Metric key for copy of {p}:", default=p.metric_key)
        lk = prompt(f"Location key for copy of {p} ('None' to omit):", default=str(p.location_key))
        if lk in ('', 'None', '[None]'):
            lk = None
        _new_pipe = Pipe(
            ck, mk, lk,
            parameters=p.parameters.copy(),
        )
        instance_keys = prompt(
            f"Meerschaum instance to store the new {_new_pipe}:",
            default=p.instance_keys
        )
        _new_pipe.instance_keys = instance_keys
        if _new_pipe.get_id(debug=debug) is not None:
            warn(f"New {_new_pipe} already exists. Skipping...", stack=False)
            continue
        _register_success_tuple = _new_pipe.register(debug=debug)
        if not _register_success_tuple[0]:
            warn(f"Failed to register new {_new_pipe}.", stack=False)
            continue

        clear_screen(debug=debug)
        successes += 1
        print_tuple(
            (True, f"Successfully copied attributes of {p} " + f" into {_new_pipe}.")
        )
        if (
            force or yes_no(
                (
                    f"Do you want to copy data from {p} into {_new_pipe}?\n\n"
                    + "If you specified `--begin`, `--end` or `--params`, data will be filtered."
                ),
                    noask=noask, yes=yes
                )
        ):
            _new_pipe.sync(p.get_data(debug=debug, **kw), debug=debug, **kw)

    msg = (
        "No pipes were copied." if successes == 0
        else (f"Copied {successes} pipe" + ("s" if successes != 1 else '') + '.')
    )

    return successes > 0, msg

def _copy_connectors(
        action: Optional[List[str]] = None,
        connector_keys: Optional[List[str]] = None,
        nopretty: bool = False,
        yes: bool = False,
        force: bool = False,
        noask: bool = False,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Create a new connector from an existing one.

    """
    import os, pathlib
    from meerschaum.utils.prompt import yes_no, prompt
    from meerschaum.connectors.parse import parse_connector_keys
    from meerschaum.config import _config, get_config
    from meerschaum.config._edit import write_config
    from meerschaum.utils.warnings import info, warn
    from meerschaum.utils.formatting import pprint
    cf = _config()
    if action is None:
        action = []
    if connector_keys is None:
        connector_keys = []

    _keys = list(set(action + connector_keys))

    if not _keys:
        return False, "No connectors to copy."

    for ck in _keys:
        try:
            conn = parse_connector_keys(ck)
        except Exception as e:
            warn(f"Unable to parse connector '{ck}'. Skipping...", stack=False)
            continue

        attrs = get_config('meerschaum', 'connectors', conn.type, conn.label)
        pprint(attrs, nopretty=nopretty)

        asking = True
        #  while asking:
            #  new_ck = prompt("Please enter a new label for the new connector ():")


    return False, "Not implemented."

def _complete_copy_connectors(
        action : Optional[List[str]] = None,
        line : str = '',
        **kw : Any
    ) -> List[str]:
    from meerschaum.config import get_config
    from meerschaum.utils.misc import get_connector_labels
    types = list(get_config('meerschaum', 'connectors').keys())
    if line.split(' ')[-1] == '' or not action:
        search_term = ''
    else:
        search_term = action[-1]
    return get_connector_labels(*types, search_term=search_term)


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
copy.__doc__ += _choices_docstring('copy')
