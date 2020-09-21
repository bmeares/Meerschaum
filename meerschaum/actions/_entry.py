#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
The entry point for launching Meerschaum actions.
"""

def _entry(sysargs=[]):
    """
    Parse arguments and launch a Meerschaum action.
    The `action` list removes the first element and ensures a minimum
    length of 1.

    Examples of action:
    'show actions' -> ['actions']
    'show' -> ['']
    """
    from meerschaum.actions.arguments import parse_arguments
    from meerschaum.actions import actions
    import sys
    if not isinstance(sysargs, list):
        import shlex
        sysargs = shlex.split(sysargs)
    args = parse_arguments(sysargs)
    main_action = args['action'][0]

    ### if action does not exist, execute in bash
    if main_action not in actions:
        main_action = 'bash'
        args['action'].insert(0, main_action)

    del args['action'][0]
    if len(args['action']) == 0: args['action'] = ['']

    return actions[main_action](**args, sysargs=sysargs)
