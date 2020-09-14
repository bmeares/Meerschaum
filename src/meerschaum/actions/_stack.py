#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for running the Docker Compose stack
"""

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
    from meerschaum.config.stack import stack_resources_path, necessary_files, write_stack
    from meerschaum.utils.misc import yes_no
    import os

    bootstrap = False
    for fp in necessary_files:
        if not os.path.isfile(fp):
            if not yes and not force:
                if not yes_no(
                            f"Missing file {fp}.\n\nBootstrap stack configuration?\n\n"
                            f"NOTE: The following files will be overwritten: {list(necessary_files)}",

                ):
                    bootstrap = True
            else: ### yes or force is True
                bootstrap = True
            break
    ### if bootstrap flag was set, create files
    if bootstrap:
        write_stack(debug=debug)

    compose_command = ['up']
    if action[0] != '': compose_command = action

    cmd_list = ['docker-compose'] + compose_command + sub_args
    if debug: print(cmd_list)
    call(cmd_list, cwd=stack_resources_path)
    return True, "Success"
