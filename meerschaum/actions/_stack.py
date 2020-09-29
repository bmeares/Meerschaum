#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for running the Docker Compose stack
"""

#  custom_subactions = {
    #  'build' : _stack_build,
#  }


def stack(
        action : list = [''],
        sub_args : list = [],
        yes : bool = False,
        force : bool = False,
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Control the Meerschaum stack with Docker Compose.
    Usage: `stack {command}`
    
    command : action[0] : default 'up'
        Docker Compose command to run. E.g. 'config' will print Docker Compose configuration
    """
    from subprocess import call
    from meerschaum.config.stack import get_necessary_files, write_stack
    from meerschaum.config._paths import STACK_COMPOSE_PATH
    from meerschaum.utils.misc import yes_no, reload_package
    import meerschaum.config
    import os

    bootstrap = False
    for fp in get_necessary_files():
        if not os.path.isfile(fp):
            if not yes and not force:
                if not yes_no(
                    f"Missing file {fp}.\n\nBootstrap stack configuration?\n\n"
                    f"NOTE: The following files will be overwritten: {list(get_necessary_files())}"
                ):
                    bootstrap = True
            else: ### yes or force is True
                bootstrap = True
            break
    ### if bootstrap flag was set, create files
    if bootstrap:
        write_stack(debug=debug)

    compose_command = ['up']
    ### default: alias stack as docker-compose
    if action[0] != '':
        compose_command = action

    ### if command is just `stack`, add --build
    elif '--build' not in sub_args:
        sub_args.append('--build')

    cmd_list = ['docker-compose'] + compose_command + sub_args
    if debug: print(cmd_list)
    call(cmd_list, cwd=STACK_COMPOSE_PATH.parent)
    reload_package(meerschaum.config)
    reload_package(meerschaum.config)
    return True, "Success"

#  def _stack_build(
        
    #  ):
