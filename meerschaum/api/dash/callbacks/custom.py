#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Import custom callbacks created by plugins.
"""

import traceback
from typing import Any, Dict

from meerschaum.api.dash import dash_app
from meerschaum.plugins import _dash_plugins, _plugin_endpoints_to_pages
from meerschaum.utils.warnings import warn, dprint
from meerschaum.api.dash.callbacks.dashboard import _paths, _required_login, _pages
from meerschaum.api.dash.components import pages_navbar


def init_dash_plugins(debug: bool = False):
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


def add_plugin_pages(debug: bool = False):
    """
    Allow users to add pages via the `@web_page` decorator.
    """
    for plugin_name, pages_dicts in _plugin_endpoints_to_pages.items():
        if debug:
            dprint(f"Adding pages from plugin '{plugin_name}'...")
        for _endpoint, _page_dict in pages_dicts.items():
            page_layout = _page_dict['function']()
            if not _page_dict['skip_navbar']:
                if isinstance(page_layout, list):
                    page_layout = [pages_navbar] + page_layout
                else:
                    page_layout = [pages_navbar, page_layout]
            _pages[_page_dict['page_key']] = _endpoint
            _paths[_endpoint] = page_layout
            if _page_dict['login_required']:
                _required_login.add(_endpoint)
