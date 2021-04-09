#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The PluginConnector imports fetch and sync methods from a Plugin (if possible) and
allows Pipes to have connectors of type 'plugin'.
"""

from meerschaum.connectors import Connector

class PluginConnector(Connector):

    def __init__(
        self,
        label : str,
        debug : bool = False,
        **kw
    ):
        """
        The PluginConnector imports fetch and sync methods from a Plugin (if possible) and
        allows Pipes to have connectors of type 'plugin'.
        """
        super().__init__('plugin', label=label, **kw)

        import os, pathlib, sys
        from meerschaum.utils.warnings import error, warn
        from meerschaum.config._paths import PLUGINS_RESOURCES_PATH
        if PLUGINS_RESOURCES_PATH not in sys.path:
            sys.path.append(str(PLUGINS_RESOURCES_PATH))

        self.resource_path = None
        for _plugin in os.listdir(PLUGINS_RESOURCES_PATH):
            plugin = _plugin.replace('.py', '')
            if plugin == self.label:
                self.resource_path = pathlib.Path(os.path.join(PLUGINS_RESOURCES_PATH, plugin))
                break
        if not self.resource_path:
            error(f"Plugin '{self.label}' cannot be found. Is it installed?")

        self.fetch = None
        try:
            exec(f'from plugins.{self.label} import fetch; self.fetch = fetch')
        except Exception as e:
            pass

        self.sync = None
        try:
            exec(f'from plugins.{self.label} import sync; self.sync = sync')
        except Exception as e:
            pass

        if self.fetch is None and self.sync is None:
            error(f"Could not import `fetch()` or `sync()` methods for plugin '{self.label}'")
