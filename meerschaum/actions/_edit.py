#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for editing elements belong here.
"""


def edit(
        action : list = [''],
        **kw
    ) -> tuple:
    """
    Edit an existing element.
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
            'config'  : _edit_config,
            'pipes'    : _edit_pipes,
            'stack'   : _edit_stack,
            'grafana' : _edit_grafana,
    }
    return choose_subaction(action, options, **kw)

def _edit_stack(*args, **kw):
    from meerschaum.config.stack import edit_stack
    return edit_stack(*args, **kw)

def _edit_config(*args, **kw):
    from meerschaum.config._edit import edit_config
    return edit_config(*args, **kw)

def _edit_grafana(*args, **kw):
    from meerschaum.config.stack.grafana import edit_grafana
    return edit_grafana(*args, **kw)

def _edit_pipes(
        action : list = [''],
        debug : bool = False,
        **kw
    ):
    from meerschaum import get_pipes
    pipes = get_pipes(debug=debug, as_list=True, **kw)
    for p in pipes:
        try:
            text = input(f"Press [Enter] to edit '{p}' or [CTRL-C] to quit: ")
        except KeyboardInterrupt:
            return False, "User pressed CTRL+C"
        if text != 'pass':
            p.edit(debug=debug, **kw)
    return (True, "Success")

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
edit.__doc__ += _choices_docstring('edit')

