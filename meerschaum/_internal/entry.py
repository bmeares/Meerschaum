#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The entry point for launching Meerschaum actions.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, List, Optional, Dict

def entry(sysargs: Optional[List[str]] = None) -> SuccessTuple:
    """Parse arguments and launch a Meerschaum action.
    The `action` list removes the first element.
    
    Examples of action:
        'show actions' -> ['actions']
        'show' -> []

    Returns
    -------
    A `SuccessTuple` indicating success. If `schedule` is provided, this will never return.

    """
    from meerschaum._internal.arguments import parse_arguments
    from meerschaum.config.static import STATIC_CONFIG
    if sysargs is None:
        sysargs = []
    if not isinstance(sysargs, list):
        import shlex
        sysargs = shlex.split(sysargs)
    args = parse_arguments(sysargs)
    argparse_exception = args.get(
        STATIC_CONFIG['system']['arguments']['failure_key'],
        None,
    )
    if argparse_exception is not None:
        args_text = args.get('text', '')
        if not args_text.startswith('show arguments'):
            return (
                False,
                (
                    "Invalid arguments:"
                    + (f"\n{args_text}" if args_text else '')
                    + f"\n    {argparse_exception}"
                )
            )

    if args.get('schedule', None):
        from meerschaum.utils.schedule import schedule_function
        return schedule_function(entry_with_args, args['schedule'], **args)
    return entry_with_args(**args)


def entry_with_args(
        _actions: Optional[Dict[str, Callable[[Any], SuccessTuple]]] = None,
        **kw
    ) -> SuccessTuple:
    """Execute a Meerschaum action with keyword arguments.
    Use `_entry()` for parsing sysargs before executing.
    """
    import sys
    from meerschaum.plugins import Plugin
    from meerschaum.actions import get_shell, get_action, get_main_action_name
    from meerschaum._internal.arguments import remove_leading_action
    from meerschaum.utils.venv import Venv, active_venvs, deactivate_venv
    if kw.get('trace', None):
        from meerschaum.utils.misc import debug_trace
        debug_trace()
    if (
        len(kw.get('action', [])) == 0
        or
        (kw['action'][0] == 'mrsm' and len(kw['action'][1:]) == 0)
    ):
        return get_shell().cmdloop()

    action_function = get_action(kw['action'], _actions=_actions)

    ### If action does not exist, execute in a subshell.
    if action_function is None:
        kw['action'].insert(0, 'sh')
        action_function = get_action(['sh'], _actions=_actions)

    ### Check if the action is a plugin, and if so, activate virtual environment.
    plugin_name = (
        action_function.__module__.split('.')[1] if (
            action_function.__module__.startswith('plugins.')
        ) else None
    )
    plugin = Plugin(plugin_name) if plugin_name else None

    kw['action'] = remove_leading_action(kw['action'], _actions=_actions)

    with Venv(plugin, debug=kw.get('debug', False)):
        action_name = ' '.join(action_function.__name__.split('_') + kw.get('action', []))
        try:
            result = action_function(**kw)
        except Exception as e:
            if kw.get('debug', False):
                import traceback
                traceback.print_exception(type(e), e, e.__traceback__)
            result = False, (
                f"Failed to execute `{action_name}` "
                + "with exception:\n\n" +
                f"{e}."
                + (
                    "\n\nRun again with '--debug' to see a full stacktrace."
                    if not kw.get('debug', False) else ''
                )
            )
        except KeyboardInterrupt:
            result = False, f"Cancelled action `{action_name}`."

    ### Clean up stray virtual environments.
    for venv in [venv for venv in active_venvs]:
        deactivate_venv(venv, debug=kw.get('debug', False), force=True)

    return result


_shell = None
def get_shell(
        sysargs: Optional[List[str]] = None,
        reload: bool = False,
        debug: bool = False
    ):
    """Initialize and return the Meerschaum shell object."""
    global _shell
    from meerschaum.utils.debug import dprint
    import meerschaum._internal.shell as shell_pkg
    from meerschaum.actions import actions
    if sysargs is None:
        sysargs = []

    if _shell is None or reload:
        if debug:
            dprint("Loading the shell...")

        if _shell is None:
            shell_pkg._insert_shell_actions()
            _shell = shell_pkg.Shell(actions, sysargs=sysargs)
        elif reload:
            _shell.__init__()
    return _shell
