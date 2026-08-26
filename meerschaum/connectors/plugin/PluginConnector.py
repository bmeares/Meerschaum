#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The PluginConnector imports fetch and sync methods from a Plugin (if possible) and
allows Pipes to have connectors of type 'plugin'.
"""

from meerschaum.connectors import Connector

class PluginConnector(Connector):
    """
    The `PluginConnector` allows plugins' modules to act as connectors by implementing
    a `fetch()` or `sync()` class + other special functions.
    See the documentation for more details:
    https://meerschaum.io/reference/plugins/writing-plugins/#functions
    """

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
        from meerschaum.core import Plugin
        from meerschaum.plugins import _import_first_party_plugin
        from meerschaum.utils.warnings import error, warn
        from meerschaum.utils.venv import Venv

        self._plugin = Plugin(self.label)
        with Venv(self._plugin):
            plugin_module = self._plugin.module or _import_first_party_plugin(self.label)
            if plugin_module is None:
                error(f"Plugin '{self.label}' cannot be found. Is it installed?")

            ### Attempt to import a `fetch()` method.
            self.fetch = None
            try:
                self.fetch = plugin_module.fetch
            except Exception as e:
                pass

            ### Attempt to import a `sync()` method.
            self.sync = None
            try:
                self.sync = plugin_module.sync
            except Exception as e:
                pass

            ### Attempt to import a `register()` method.
            self.register = None
            try:
                self.register = plugin_module.register
            except Exception as e:
                pass

            if self.fetch is None and self.sync is None:
                error(f"Could not import `fetch()` or `sync()` methods for plugin '{self.label}'.")
