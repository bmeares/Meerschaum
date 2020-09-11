#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains functions for printing elements.
"""

def show(
        action:list=[],
        **kw
    ) -> tuple:
    """
    Show elements of a certain type.
    
    command: `show actions`

    Options:
        - actions
        - pipes

    params:
        action : list
            What items to show 
            The 'action' list contains strings following the 'show' command'.
            e.g.: `show actions` becomes ['actions']
    """
    
    from meerschaum.utils.misc import choose_subaction
    show_options = {
        'actions'    : _show_actions,
        'pipes'      : _show_pipes,
        'config'     : _show_config,
        'modules'    : _show_modules,
        'version'    : _show_version,
        'connectors' : _show_connectors,
        'arguments'  : _show_arguments,
    }
    return choose_subaction(action, show_options, **kw)

def _show_actions(**kw) -> tuple:
    """
    Show available actions
    """
    from meerschaum.actions import actions
    return _show_dict(options=actions, **kw)

def _show_config(**kw) -> tuple:
    """
    Show the configuration dictionary
    """
    import pprint
    from meerschaum.config import config as cf
    pprint.pprint(cf)
    return (True, "Success")

def _show_modules(**kw) -> tuple:
    """
    Show the currently imported modules
    """
    import sys, pprint
    pprint.pprint(list(sys.modules.keys()))
    return (True, "Success")

def _show_dict(
        options={},
        nopretty=False,
        **kw
    ) -> tuple:
    """
    Show available options from an iterable
    """
    from meerschaum.actions import actions
    if not nopretty:
        header = "Available options:"
        print("\n" + header)
        ### calculate underline length
        underline_len = len(header)
        for a in actions:
            if len(a) + 4 > underline_len:
                underline_len = len(a) + 4
        ### print underline
        for i in range(underline_len): print('-', end="")
        print("\n", end="")
    ### print actions
    for action in sorted(actions):
        if not nopretty: print("  - ", end="")
        print(action)
    return (True, "Success")

### TODO
def _show_pipes(**kw) -> tuple:
    return (True, "Success")

def _show_version(**kw) -> tuple:
    """
    Show the Meerschaum doc string
    """
    from meerschaum import __doc__ as doc
    print(doc)
    return (True, "Success")

def _show_connectors(
        debug : bool = False,
        **kw
    ) -> tuple:
    from meerschaum.connectors import connectors
    from meerschaum.config import config
    from meerschaum.utils.formatting import make_header
    import pprint
    print(make_header("\nConfigured connectors:"))
    pprint.pprint(config['meerschaum']['connectors'])
    print(make_header("\nActive connectors:"))
    pprint.pprint(connectors)
    return True, "Success"

def _show_arguments(
        **kw
    ):
    import pprint
    pprint.pprint(kw)
    return True, "Success"
