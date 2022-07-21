#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The entry point for launching Meerschaum actions.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, List, Optional

def _entry(sysargs: Optional[List[str]] = None) -> SuccessTuple:
    """Parse arguments and launch a Meerschaum action.
    The `action` list removes the first element.
    
    Examples of action:
        'show actions' -> ['actions']
        'show' -> []

    Parameters
    ----------
    sysargs : Optional[List[str]] :
         (Default value = None)

    Returns
    -------
    A `SuccessTuple` indicating success. If `schedule` is provided, this will never return.

    """
    from meerschaum.actions.arguments import parse_arguments
    if sysargs is None:
        sysargs = []
    if not isinstance(sysargs, list):
        import shlex
        sysargs = shlex.split(sysargs)
    args = parse_arguments(sysargs)
    if args.get('schedule', None):
        from meerschaum.utils.schedule import schedule_function
        return schedule_function(_entry_with_args, args['schedule'], **args)
    return _entry_with_args(**args)

def _entry_with_args(
        _actions: Optional[Dict[str, Callable[[Any], SuccessTuple]]] = None,
        **kw
    ) -> SuccessTuple:
    """Execute a Meerschaum action with keyword arguments.
    Use `_entry()` for parsing sysargs before executing.
    """
    import sys
    from meerschaum.plugins import Plugin
    from meerschaum.actions import get_shell, get_action
    from meerschaum.utils.venv import Venv
    if kw.get('trace', None):
        from meerschaum.utils.misc import debug_trace
        debug_trace()
    if (
        len(kw.get('action', [])) == 0
        or
        (kw['action'][0] == 'mrsm' and len(kw['action']) == 0)
    ):
        return get_shell().cmdloop()

    action_function = get_action(kw['action'], _actions=_actions)

    ### If action does not exist, execute in a subshell.
    if action_function is None:
        kw['action'].insert(0, 'sh')

    ### Check if the action is a plugin, and if so, activate virtual environment.
    plugin_name = (
        action_function.__module__.split('.')[1] if (
            action_function.__module__.startswith('plugins.')
        ) else None
    )
    plugin = Plugin(plugin_name) if plugin_name else None

    del kw['action'][0]

    with Venv(plugin, debug=kw.get('debug', False)):
        try:
            result = action_function(**kw)
        except Exception as e:
            if kw.get('debug', False):
                import traceback
                traceback.print_exception(type(e), e, e.__traceback__)
            result = False, (
                f"Failed to execute '{' '.join([action_function.__name__] + kw['action'])}' "
                + "with exception:\n\n" +
                f"{e}."
                + (
                    "\n\nRun again with '--debug' to see a full stacktrace."
                    if not kw.get('debug', False) else ''
                )
            )

    return result

