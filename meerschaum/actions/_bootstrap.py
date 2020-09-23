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
    Bootstrap an element (pipe, configuration).

    Command:
        bootstrap [pipe, config {stack, grafana}]
    Example:
        bootstrap config stack
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
        'pipe'    : _bootstrap_pipe,
        'config'  : _bootstrap_config,
        #  'stack'   : _bootstrap_stack,
    }
    return choose_subaction(action, options, **kw)

def _bootstrap_pipe(**kw):
    """
    Create a new Pipe
    """
    return (True, "Success")

def _bootstrap_config(
        action : list = [''],
        yes : bool = False,
        force : bool = False,
        debug : bool = False,
        **kw
    ):
    """
    Delete and regenerate the default Meerschaum configuration
    """
    possible_config_funcs = {
        'stack'   : _bootstrap_stack,
        'grafana' : _bootstrap_grafana,
    }
    if len(action) > 1:
        if action[1] in possible_config_funcs:
            return possible_config_funcs[action[0]](**kw)

    from meerschaum.config._edit import write_default_config, write_config
    from meerschaum.config._default import default_config

    from meerschaum.actions import actions
    if not actions['delete'](['config'], debug=debug, yes=yes, force=force, **kw)[0]:
        return False, "Aborting bootstrap"

    if not write_config(default_config, debug=debug, **kw):
        return (False, "Failed to write default configuration")

    if not write_default_config(debug=debug, **kw):
        return (False, "Failed to write default configuration")

    from meerschaum.config.stack import write_stack
    if not write_stack(debug=debug):
        return False, "Failed to write stack"

    return (True, "Successfully bootstrapped configuration files")

def _bootstrap_stack(
        yes : bool = False,
        force : bool = False,
        debug : bool = False,
        **kw
    ):
    """
    Delete and regenerate the default Meerschaum stack configuration
    """
    from meerschaum.utils.misc import yes_no
    from meerschaum.config._paths import STACK_COMPOSE_PATH, STACK_ENV_PATH
    from meerschaum.config.stack import write_stack
    answer = False
    if not yes:
        answer = yes_no(f"Delete {STACK_COMPOSE_PATH} and {STACK_ENV_PATH}?", default='n')

    if answer or force:
        write_stack(debug=debug)
    else:
        msg = "No edits made"
        if debug: print(msg)
        return True, msg
    return True, "Success"

def _bootstrap_grafana(
        yes : bool = False,
        force : bool = False,
        debug : bool = False,
        **kw
    ):
    """
    Delete and regenerate the default Meerschaum stack configuration
    """
    from meerschaum.utils.misc import yes_no
    from meerschaum.config._paths import GRAFANA_DATASOURCE_PATH, GRAFANA_DASHBOARD_PATH
    from meerschaum.config._edit import general_write_config
    from meerschaum.config import config as cf
    answer = False
    if not yes:
        answer = yes_no(f"Delete {GRAFANA_DATASOURCE_PATH} and {GRAFANA_DASHBOARD_PATH}?", default='n')

    if answer or force:
        general_write_config(
            {
                GRAFANA_DATASOURCE_PATH : cf['grafana']['datasource'],
                GRAFANA_DASHBOARD_PATH : cf['grafana']['dashboard'],
            },
            debug=debug
        )
    else:
        msg = "No edits made"
        if debug: print(msg)
        return True, msg
    return True, "Success"

