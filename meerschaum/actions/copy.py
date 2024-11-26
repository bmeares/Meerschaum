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
    from meerschaum.actions import choose_subaction
    options = {
        'pipes'      : _copy_pipes,
        'connectors' : _copy_connectors,
    }
    return choose_subaction(action, options, **kw)


def _complete_copy(
    action: Optional[List[str]] = None,
    **kw: Any
) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
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
    from meerschaum.utils.warnings import warn
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
                    noask=noask,
                    yes=yes,
                    default='n',
                )
        ):
            _new_pipe.sync(
                p.get_data(
                    debug=debug,
                    as_iterator=True,
                    **kw
                ),
                debug=debug,
                **kw
            )

    msg = (
        "No pipes were copied." if successes == 0
        else (f"Copied {successes} pipe" + ("s" if successes != 1 else '') + '.')
    )

    return successes > 0, msg


def _copy_connectors(
    action: Optional[List[str]] = None,
    connector_keys: Optional[List[str]] = None,
    nopretty: bool = False,
    force: bool = False,
    debug: bool = False,
    **kwargs: Any
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
    from meerschaum.actions import get_action
    cf = _config()
    if action is None:
        action = []
    if connector_keys is None:
        connector_keys = []

    _keys = (action or []) + connector_keys

    if not _keys:
        return False, "No connectors to copy."

    if len(_keys) < 1 or len(_keys) > 2:
        return False, "Provide one set of connector keys."

    ck = _keys[0]

    try:
        conn = parse_connector_keys(ck)
    except Exception as e:
        return False, f"Unable to parse connector '{ck}'."

    if len(_keys) == 2:
        new_ck = _keys[1] if ':' in _keys[1] else None
        new_label = _keys[1].split(':')[-1]
    else:
        new_ck = None
        new_label = None

    try:
        if new_label is None:
            new_label = prompt(f"Enter a label for the new '{conn.type}' connector:")
    except KeyboardInterrupt:
        return False, "Nothing was copied."

    if new_ck is None:
        new_ck = f"{conn.type}:{new_label}"

    info(f"Registering connector '{new_ck}' from '{ck}'...")

    attrs = get_config('meerschaum', 'connectors', conn.type, conn.label)
    pprint(attrs, nopretty=nopretty)
    if not force and not yes_no(
        f"Register connector '{new_ck}' with the above attributes?",
        default='n',
        **kwargs
    ):
        return False, "Nothing was copied."

    register_connector = get_action(['register', 'connector'])
    register_success, register_msg = register_connector(
        [new_ck],
        params=attrs,
        **kwargs
    )
    return register_success, register_msg


def _complete_copy_connectors(
    action: Optional[List[str]] = None,
    line: str = '',
    **kw: Any
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
