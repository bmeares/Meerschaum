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
    from meerschaum.config.stack import get_necessary_files, write_stack
    from meerschaum.config._paths import STACK_COMPOSE_PATH
    from meerschaum.utils.misc import yes_no, reload_package
    import meerschaum.config
    from meerschaum.config import get_config
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.formatting import ANSI
    import os

    bootstrap_question = (
        "Bootstrap configuration?\n\n"
        "The following files will be overwritten:"
    )
    for f in get_necessary_files():
        bootstrap_question += "\n  - " + str(f)
    bootstrap_question += "\n"

    bootstrap = False
    for fp in get_necessary_files():
        if not os.path.isfile(fp):
            if not yes and not force:
                if yes_no(bootstrap_question):
                    bootstrap = True
                else:
                    warn_message = "Cannot start stack without bootstrapping"
                    warn(warn_message)
                    return False, warn_message
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

    ### define project name when starting containers
    project_name_list = ['--project-name', get_config('stack', 'project_name', patch=True)]
    
    ### disable ANSI if the user sets ANSI mode to False
    ansi_list = [] if ANSI else ['--no-ansi']

    debug_list = ['--log-level', 'DEBUG'] if debug else []

    ### prepend settings before the docker-compose action
    settings_list = project_name_list + ansi_list + debug_list

    cmd_list = ['docker-compose'] + settings_list + compose_command + sub_args
    if debug: dprint(cmd_list)
    call(cmd_list, cwd=STACK_COMPOSE_PATH.parent)

    ### not sure why I decided to reload the config here...
    #  reload_package(meerschaum.config)
    #  reload_package(meerschaum.config)
    return True, "Success"

