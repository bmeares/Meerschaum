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
        'stack'   : _bootstrap_stack,
        'grafana' : _bootstrap_grafana,
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

    from meerschaum.utils.misc import reload_package, yes_no
    import meerschaum.config
    import meerschaum.config._default as default
    import importlib, os
    from meerschaum.config._read_yaml import config_path
    answer = False
    if not yes:
        answer = yes_no(f"Delete {config_path}?", default='n')

    if answer or force:
        if debug: print(f"Removing {config_path}...")
        try:
            os.remove(config_path)
        except Exception as e:
            print(e)
    else:
        msg = "No edits made"
        if debug: print(msg)
        return True, msg

    if not default.write_default_config(debug=debug, **kw):
        return (False, "Failed to write default config")
    reload_package(meerschaum.config, debug=debug, **kw)
    reload_package(meerschaum.config, debug=debug, **kw)
    return (True, "Success")

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
    from meerschaum.config.stack import write_stack, compose_path, env_path
    answer = False
    if not yes:
        answer = yes_no(f"Delete {compose_path} and {env_path}?", default='n')

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
    from meerschaum.config.stack.grafana import grafana_datasource_yaml_path, datasource, grafana_dashboard_yaml_path, dashboard
    from meerschaum.config._edit import general_write_config
    answer = False
    if not yes:
        answer = yes_no(f"Delete {grafana_datasource_yaml_path} and {grafana_dashboard_yaml_path}?", default='n')

    if answer or force:
        general_write_config(
            {
                grafana_datasource_yaml_path : datasource,
                grafana_dashboard_yaml_path : dashboard,
            },
            debug=debug
        )
    else:
        msg = "No edits made"
        if debug: print(msg)
        return True, msg
    return True, "Success"

