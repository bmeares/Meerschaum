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
        'pipe'   : _bootstrap_pipe,
        'sql'    : _bootstrap_sql,
        'config' : _bootstrap_config,
    }
    return choose_subaction(action, options, **kw)

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

def _bootstrap_config(
        yes : bool = False,
        force : bool = False,
        debug : bool = False,
        **kw
    ):
    """
    Delete and regenerate the default Meerschaum configuration
    """
    import meerschaum.config._default as default
    import importlib, os
    from meerschaum.config._read_yaml import config_path
    answer = False
    if not yes:
        from meerschaum.utils.misc import yes_no
        answer = yes_no(f"Delete {config_path}?", default='n')

    if answer or force:
        if debug: print(f"Removing {config_path}...")
        try:
            os.remove(config_path)
        except Exception as e:
            print(e)

    if not default.write_default_config(debug=debug, **kw):
        return (False, "Failed to write default config")
    return (True, "Success")
