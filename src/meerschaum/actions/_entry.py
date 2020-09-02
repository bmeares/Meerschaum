#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
The entry point for launching Meerschaum actions.
"""

def _entry(sysargs=[]):
    from meerschaum.actions.arguments import parse_arguments
    from meerschaum.actions import actions
    import sys
    args = parse_arguments(sysargs)
    main_action = args['action'][0]
    del args['action'][0]
    if main_action not in actions:
        print(f"Action '{main_action}' is not valid.")
        main_action = 'show'
        args['action'] = ['actions']
        args['pretty'] = True

    return actions[main_action](**args, sysargs=sysargs)
