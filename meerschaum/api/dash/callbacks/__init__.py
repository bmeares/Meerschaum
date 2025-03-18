#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define callbacks for pages.
"""

from meerschaum.api import debug as _debug
import meerschaum.api.dash.callbacks.dashboard
import meerschaum.api.dash.callbacks.login
import meerschaum.api.dash.callbacks.plugins
import meerschaum.api.dash.callbacks.jobs
import meerschaum.api.dash.callbacks.register
import meerschaum.api.dash.callbacks.pipes
import meerschaum.api.dash.callbacks.settings
from meerschaum.api.dash.callbacks.custom import init_dash_plugins, add_plugin_pages

init_dash_plugins(debug=_debug)
add_plugin_pages(debug=_debug)

