#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Upgrade your current Meerschaum environment
"""

def upgrade(
        action : list = [''],
        yes : bool = True,
        force : bool = False,
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Upgrade the current Meerschaum instance. Optionally specify dependency versions (such as [full])

    Example:
        mrsm upgrade full
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.actions import actions
    from meerschaum.utils.misc import run_python_package, yes_no, attempt_import

    is_stack_running = False
    client = None
    docker = attempt_import('docker', warn=False)
    if docker:
        try:
            client = docker.from_env()
            containers = client.containers.list()
            is_stack_running = len(containers) > 0
        except:
            pass

    if is_stack_running:
        answer = True or force
        if not yes and not force:
            answer = yes_no(f"Take down the stack?", default='y')

        if answer:
            if debug: dprint("Taking stack down...")
            actions['stack'](['down'], debug=debug)

    dependencies = None
    if action != [''] and len(action) > 0:
        dependencies = action[0]

    command = [
        'install',
        '--upgrade',
        'meerschaum' + (('[' + ','.join(dependencies.split(',')) + ']') if dependencies else ''),
    ]

    if debug: dprint('Upgrade meerschaum with dependencies: \"' + f'{dependencies}' + '\"')
    run_python_package('pip', command)

    if debug: dprint("Pulling new Docker images...")
    if client: actions['stack'](['pull'], debug=debug)

    return True, "Success"
