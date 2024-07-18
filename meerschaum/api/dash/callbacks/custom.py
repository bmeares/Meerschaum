#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Import custom callbacks created by plugins.
"""

import traceback
from meerschaum.api.dash import dash_app
from meerschaum.plugins import _dash_plugins, _plugin_endpoints_to_pages
from meerschaum.utils.warnings import warn
from meerschaum.api.dash.callbacks.dashboard import _paths, _required_login


def init_dash_plugins():
    """
    Fire the initial callbacks for Dash plugins.
    """
    for _module_name, _functions in _dash_plugins.items():
        for _function in _functions:
            try:
                _function(dash_app)
            except Exception:
                warn(
                    f"Failed to load function '{_function.__name__}' "
                    + f"from plugin '{_module_name}':\n"
                    + traceback.format_exc()
                )


def add_plugin_pages():
    """
    Allow users to add pages via the `@web_page` decorator.
    """
    for _endpoint, _page_dict in _plugin_endpoints_to_pages.items():
        _paths[_endpoint] = _page_dict['function']()
        if _page_dict['login_required']:
            _required_login.add(_endpoint)
