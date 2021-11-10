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
        from meerschaum._internal.Plugin import Plugin
        from meerschaum.utils.warnings import error, warn
        #  from meerschaum.config._paths import PLUGINS_RESOURCES_PATH
        #  if str(PLUGINS_RESOURCES_PATH.parent) not in sys.path:
            #  sys.path.append(str(PLUGINS_RESOURCES_PATH.parent))
        self._plugin = Plugin(self.label)

        #  self.resource_path = None
        #  for _plugin in os.listdir(PLUGINS_RESOURCES_PATH):
            #  plugin = _plugin.replace('.py', '')
            #  if plugin == self.label:
                #  self.resource_path = pathlib.Path(os.path.join(PLUGINS_RESOURCES_PATH, plugin))
                #  break
        #  if not self.resource_path:
        if self._plugin.module is None:
            error(f"Plugin '{self.label}' cannot be found. Is it installed?")

        ### Attempt to import a `fetch()` method.
        self.fetch = None
        try:
            self.fetch = self._plugin.module.fetch
        except Exception as e:
            pass

        ### Attempt to import a `sync()` method.
        self.sync = None
        try:
            self.sync = self._plugin.module.sync
        except Exception as e:
            pass

        ### Attempt to import a `register()` method.
        self.register = None
        try:
            self.register = self._plugin.module.register
        except Exception as e:
            pass

        if self.fetch is None and self.sync is None:
            error(f"Could not import `fetch()` or `sync()` methods for plugin '{self.label}'.")
