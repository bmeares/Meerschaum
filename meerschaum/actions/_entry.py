#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
The entry point for launching Meerschaum actions.
"""

def _entry(sysargs=[]):
    """
    Parse arguments and launch a Meerschaum action.
    The `action` list removes the first element.

    Examples of action:
    'show actions' -> ['actions']
    'show' -> []
    """
    from meerschaum.actions.arguments import parse_arguments
    from meerschaum.actions import actions, original_actions, get_shell
    from meerschaum.utils.packages import activate_venv, deactivate_venv
    import sys
    if not isinstance(sysargs, list):
        import shlex
        sysargs = shlex.split(sysargs)
    args = parse_arguments(sysargs)
    if len(args['action']) == 0:
        return get_shell().cmdloop()
    main_action = args['action'][0]

    ### if action does not exist, execute in bash
    if main_action not in actions:
        main_action = 'sh'
        args['action'].insert(0, main_action)

    ### Check if the action is a plugin, and if so, activate virtual environment.
    plugin_name = (
        actions[main_action].__module__.split('.')[-1] if actions[main_action].__module__.startswith('plugins.')
        else None
    )

    del args['action'][0]

    activate_venv(venv=plugin_name)
    result = actions[main_action](**args)
    deactivate_venv(venv=plugin_name)

    return result
