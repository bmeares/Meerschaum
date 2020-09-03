#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Functions for bootstrapping elements
(pipes, server connections, etc)
"""

def bootstrap(
        action : list = [''],
        **kw
    ):
    """
    Bootstrap an element (pipe, server connection)
    Command:
        bootstrap [pipe, sql]
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
        'pipe' : _bootstrap_pipe,
        'sql'  : _bootstrap_sql,
    }
    return choose_subaction(action, options)

def _bootstrap_sql(**kw):
    """
    Initiate the main Meerschaum SQL backend
    """
    return (True, "Success")

def _bootstrap_pipe(**kw):
    """
    Create a new Pipe
    """
    return (True, "Success")
