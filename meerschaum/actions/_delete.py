#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for deleting elements
"""

def delete(
        action : list = [''],
        debug : bool = False,
        **kw
    ):
    """
    Delete an element.

    delete [config, pipe]
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
        'config' : _delete_config, 
        'pipe' : _delete_pipe,
    }
    return choose_subaction(action, options, **kw)

def _delete_pipe(
        #  action : list = [''],
        debug : bool = False,
        **kw
    ):
    return False, "TODO IMPLEMENT"

def _delete_config(
        #  action : list = [''],
        yes : bool = False,
        force : bool = False,
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Delete configuration files
    """
    import os
    from meerschaum.utils.misc import yes_no
    from meerschaum.config._paths import CONFIG_PATH, STACK_COMPOSE_PATH, DEFAULT_CONFIG_PATH
    paths = [CONFIG_PATH, STACK_COMPOSE_PATH, DEFAULT_CONFIG_PATH]
    answer = False
    if not yes:
        sep = '\n' + '  - '
        answer = yes_no(f"Delete files?{sep + sep.join([str(p) for p in paths])}\n", default='n')

    if answer or force:
        for path in paths:
            if debug: print(f"Removing {path}...")
            try:
                os.remove(path)
            except Exception as e:
                print(e)
    else:
        msg = "Nothing deleted."
        if debug: print(msg)
        return False, msg
    
    return True, "Successfully deleted configuration files"


