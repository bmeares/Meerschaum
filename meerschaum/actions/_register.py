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
    }
    return choose_subaction(action, options, **kw)

def _register_pipes(
        connector_keys : list = [],
        metric_keys : list = [],
        location_keys : list = [],
        params : dict = dict(),
        api_label : str = 'main',
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Create and register Pipe objects.
    Required: connector_keys and metric_keys. If location_keys is empty, assume [None]
    """
    from meerschaum import get_pipes, get_connector
    from meerschaum.utils.debug import dprint

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
    api_connector = get_connector(type='api', label=api_label)

    for p in pipes:
        if debug: dprint(f"Registering pipe '{p}'...")
        p.register(api_connector=api_connector, debug=debug)

    return True, "Success"


def _register_metrics(**kw):
    pass

def _register_locations(**kw):
    pass


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
register.__doc__ += _choices_docstring('register')

