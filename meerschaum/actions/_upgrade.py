#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Upgrade your current Meerschaum environment
"""

def upgrade(
        action : list = [''],
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Upgrade the current Meerschaum instance

    Example:
        mrsm upgrade full
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.actions import actions
    from meerschaum.utils.misc import run_python_package

    if debug: dprint("Taking stack down...")
    actions['stack'](['down'], debug=debug)

    dependencies = None
    if action != [''] and len(action) > 0:
        dependencies = action[0]

    command = ['install', '--upgrade', 'meerschaum'] + ([dependencies] if dependencies else [])

    if debug: dprint('Upgrade meerschaum with dependencies: \"' + f'{dependencies}' + '\"')
    run_python_package('pip', command)

    if debug: dprint("Pulling new Docker images...")
    actions['stack'](['pull'], debug=debug)

    return True, "Success"
