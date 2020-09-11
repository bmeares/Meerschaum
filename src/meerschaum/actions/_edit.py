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
        edit [config, pipe]
    """
    from meerschaum.config._edit import edit_config as _edit_config
    from meerschaum.utils.misc import choose_subaction
    options = {
            'config'    : _edit_config,
            'pipe'      : _edit_pipe,
            #  'connector' : _edit_connector,
    }
    return choose_subaction(action, options, **kw)

def _edit_pipe(**kw):
    return (True, "Success")

#  def _edit_connector(
        #  action
        #  debug : bool = False,
        #  **kw
    #  ) -> tuple:
    #  from meerschaum.config import config
    #  label = action[0]
    
