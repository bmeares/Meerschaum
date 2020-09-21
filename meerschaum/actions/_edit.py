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
    Command:
        edit [config, pipe, stack, grafana]
    """
    from meerschaum.config._edit import edit_config as _edit_config
    from meerschaum.config.stack import edit_stack as _edit_stack
    from meerschaum.config.stack.grafana import edit_grafana as _edit_grafana
    from meerschaum.utils.misc import choose_subaction
    options = {
            'config'  : _edit_config,
            'pipe'    : _edit_pipe,
            'stack'   : _edit_stack,
            'grafana' : _edit_grafana,
    }
    return choose_subaction(action, options, **kw)

def _edit_pipe(**kw):
    return (True, "Success")

   
