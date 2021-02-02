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
    from meerschaum.actions import actions, get_shell
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
        main_action = 'bash'
        args['action'].insert(0, main_action)

    del args['action'][0]

    ### monkey patch socket if async is specified
    #  if 'unblock' in args:
        #  from meerschaum.utils.misc import enforce_gevent_monkey_patch
        #  enforce_gevent_monkey_patch()

    return actions[main_action](**args, sysargs=sysargs)
