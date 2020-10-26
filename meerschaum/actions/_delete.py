#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for deleting elements
"""

def delete(
        action : list = [''],
        **kw
    ):
    """
    Delete an element.
    """
    from meerschaum.utils.misc import choose_subaction
    from meerschaum.utils.debug import dprint
    options = {
        'config' : _delete_config, 
        'pipes' : _delete_pipes,
    }
    return choose_subaction(action, options, **kw)

def _delete_pipes(
        debug : bool = False,
        yes : bool = False,
        force : bool = False,
        **kw
    ) -> tuple:
    from meerschaum import get_pipes
    from meerschaum.utils.misc import yes_no
    import pprintpp
    pipes = get_pipes(as_list=True, debug=debug, **kw)
    if len(pipes) == 0:
        return False, "No pipes to delete"
    question = "Are you sure you want to delete these Pipes? THIS CANNOT BE UNDONE!\n"
    for p in pipes:
        question += f" - {p}" + "\n"
    answer = force
    if not yes and not force:
        answer = yes_no(question, default='n')
    if not answer:
        return False, "No pipes deleted."
    for p in pipes:
        success_tuple = p.delete(debug=debug)
        if not success_tuple[0]:
            return success_tuple

    return True, "Success"

def _delete_config(
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
    from meerschaum.utils.debug import dprint
    paths = [CONFIG_PATH, STACK_COMPOSE_PATH, DEFAULT_CONFIG_PATH]
    answer = False
    if not yes:
        sep = '\n' + '  - '
        answer = yes_no(f"Delete files?{sep + sep.join([str(p) for p in paths])}\n", default='n')

    if answer or force:
        for path in paths:
            if debug: dprint(f"Removing {path}...")
            if os.path.isfile(path):
                os.remove(path)
    else:
        msg = "Nothing deleted."
        if debug: dprint(msg)
        return False, msg
    
    return True, "Successfully deleted configuration files"

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
delete.__doc__ += _choices_docstring('delete')
