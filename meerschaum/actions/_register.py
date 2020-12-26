#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register new Pipes. Requires the API to be running.
"""

def register(
        action : list = [''],
        **kw
    ) -> tuple:
    """
    Register new elements.
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
        'pipes'     : _register_pipes,
        'metrics'   : _register_metrics,
        'locations' : _register_locations,
        'plugins'   : _register_plugins,
    }
    return choose_subaction(action, options, **kw)

def _register_pipes(
        connector_keys : list = [],
        metric_keys : list = [],
        location_keys : list = [],
        params : dict = dict(),
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Create and register Pipe objects.
    Required: connector_keys and metric_keys. If location_keys is empty, assume [None]
    """
    from meerschaum import get_pipes, get_connector
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn

    pipes = get_pipes(
        connector_keys = connector_keys,
        metric_keys = metric_keys,
        location_keys = location_keys,
        params = params,
        as_list = True,
        method = 'explicit',
        debug = debug,
        **kw
    )

    success, message = True, "Success"
    failed_message = ""
    for p in pipes:
        if debug: dprint(f"Registering pipe '{p}'...")
        ss, msg = p.register(debug=debug)
        if not ss:
            warn(f"{msg}")
            success = False
            failed_message += f"{p}, "

    if len(failed_message) > 0:
        message = "Failed to register pipes: " + failed_message[:(-1 * len(', '))]

    return success, message


def _register_metrics(**kw):
    pass

def _register_locations(**kw):
    pass

def _register_plugins(
        action : list = [],
        debug : bool = False,
        mrsm_instance : str = None,
        **kw
    ) -> tuple:
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.misc import parse_instance_keys, reload_plugins
    from meerschaum.config import get_config
    from meerschaum.utils.warnings import warn, error
    from meerschaum import Plugin
    from meerschaum.connectors.api import APIConnector
    from meerschaum import get_connector
    from meerschaum.utils.formatting import print_tuple

    reload_plugins(debug=debug)

    if mrsm_instance is None: mrsm_instance = get_config('meerschaum', 'instance', patch=True)
    instance_connector = parse_instance_keys(mrsm_instance)
    default_connector = get_connector('api', 'mrsm')
    if not isinstance(instance_connector, APIConnector):
        instance_connector = default_connector

    if len(action) == 0: return False, "No plugins to register"

    plugins_to_register = dict()
    from meerschaum.actions import _plugins_names
    for p in action:
        if p not in _plugins_names:
            warn(f"Plugin '{p}' is not installed and cannot be registered. Ignoring...")
        else:
            plugins_to_register[p] = Plugin(p)

    successes = dict()

    for name, plugin in plugins_to_register.items():
        print(f"Registering plugin '{plugin}' to Meerschaum API '{instance_connector}'..." + '\n')
        success, msg = instance_connector.register_plugin(plugin, debug=debug)
        print_tuple((success, msg + '\n'))
        successes[name] = (success, msg)

    total_success, total_fail = 0, 0
    for p, tup in successes.items():
        if tup[0]: total_success += 1
        else: total_fail += 1

    if debug:
        from pprintpp import pprint
        dprint("Return values for each plugin:")
        pprint(successes)

    msg = (
        f"Finished processing {len(plugins_to_register)} plugins." + '\n' +
        f"  {total_success} succeeded, {total_fail} failed."
    )
    print(msg)
    reload_plugins(debug=debug)
    return True, msg


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
register.__doc__ += _choices_docstring('register')

