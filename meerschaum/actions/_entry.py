#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The entry point for launching Meerschaum actions.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, List, Optional

def _entry(sysargs : Optional[List[str]] = None) -> SuccessTuple:
    """
    Parse arguments and launch a Meerschaum action.
    The `action` list removes the first element.

    Examples of action:
        'show actions' -> ['actions']
        'show' -> []
    """
    from meerschaum.actions.arguments import parse_arguments
    if sysargs is None:
        sysargs = []
    if not isinstance(sysargs, list):
        import shlex
        sysargs = shlex.split(sysargs)
    args = parse_arguments(sysargs)
    return _entry_with_args(**args)

def _entry_with_args(**kw) -> SuccessTuple:
    """
    Execute a Meerschaum action with keyword arguments.
    Use `_entry()` for parsing sysargs before executing.
    """
    from meerschaum.actions import actions, original_actions, get_shell
    from meerschaum.utils.packages import activate_venv, deactivate_venv
    import sys
    if kw.get('trace', None):
        from meerschaum.utils.misc import debug_trace
        debug_trace()
    if len(kw.get('action', [])) == 0:
        return get_shell().cmdloop()
    #  if kw.get('daemon', False):
        #  from meerschaum.utils.daemon import daemon_action
        #  return daemon_action(**kw)

    main_action = kw['action'][0]

    ### if action does not exist, execute in bash
    if main_action not in actions:
        main_action = 'sh'
        kw['action'].insert(0, main_action)

    ### Check if the action is a plugin, and if so, activate virtual environment.
    plugin_name = (
        actions[main_action].__module__.split('.')[-1] if (
            actions[main_action].__module__.startswith('plugins.')
        ) else None
    )

    del kw['action'][0]

    activate_venv(venv=plugin_name, debug=kw.get('debug', False))

    try:
        result = actions[main_action](**kw)
    except Exception as e:
        if kw.get('debug', False):
            import traceback
            traceback.print_exception(type(e), e, e.__traceback__)
        result = False, (
            f"Failed to execute '{' '.join([main_action] + kw['action'])}' with exception:\n\n" +
            f"'{e}'.\n\nRun again with '--debug' to see a full stacktrace."
        )

    deactivate_venv(venv=plugin_name, debug=kw.get('debug', False))

    return result

