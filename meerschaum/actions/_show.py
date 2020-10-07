#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains functions for printing elements.
"""


def show(
        action : list = [''],
        **kw
    ) -> tuple:
    """
    Show elements of a certain type.
    
    Command:
        `show {option}`

    Example:
        `show pipes`
    """
    
    from meerschaum.utils.misc import choose_subaction, sorted_dict
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
    from meerschaum.utils.misc import print_options
    print_options(options=actions, **kw)
    return True, "Success"

def _show_config(
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Show the configuration dictionary
    """
    from pprintpp import pprint
    from meerschaum.config import config
    from meerschaum.config._paths import CONFIG_PATH
    from meerschaum.utils.debug import dprint
    if debug: dprint(f"Configuration loaded from {CONFIG_PATH}")
    pprint(config)
    return (True, "Success")

def _show_modules(**kw) -> tuple:
    """
    Show the currently imported modules
    """
    import sys, pprintpp
    pprintpp.pprint(list(sys.modules.keys()))
    return (True, "Success")

def _show_pipes(
        nopretty : bool = False,
        debug : bool = False,
        **kw
    ) -> tuple:
    from meerschaum import get_pipes
    from meerschaum.utils.misc import flatten_pipes_dict
    pipes = get_pipes(debug=debug, **kw)

    if len(pipes) == 1:
        return flatten_pipes_dict(pipes)[0].show(debug=debug, nopretty=nopretty, **kw)

    if not nopretty:
        import pprintpp
        pprintpp.pprint(pipes)
    else:
        pipes_list = flatten_pipes_dict(pipes)
        for p in pipes_list:
            print(p)

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
    from pprintpp import pprint
    print(make_header("\nConfigured connectors:"))
    pprint(config['meerschaum']['connectors'])
    print(make_header("\nActive connectors:"))
    pprint(connectors)
    return True, "Success"

def _show_arguments(
        **kw
    ):
    from pprintpp import pprint
    pprint(kw)
    return True, "Success"

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
show.__doc__ += _choices_docstring('show')
