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

        ### A native (Rust) plugin is not a Python module: the core runs its hooks.
        self.is_native = _is_native_plugin(self.label)
        if self.is_native:
            self.fetch = self._native_fetch
            self.sync = None
            self.register = self._native_register
            return

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

    def _native_fetch(self, pipe, **kw):
        """
        Fetch a native plugin's rows through the core (`pipe.fetch()`).
        """
        return pipe.fetch(**kw)

    def _native_register(self, pipe, **kw):
        """
        Run a native plugin's `register(pipe)` in the core and return its parameters
        (`None` without a `register` hook; a non-dict return value is passed through so that
        `Pipe.register()` warns about it, as for a Python plugin).
        """
        from meerschaum import _core
        outcome = _core.native_connector_register(pipe)
        if outcome is None:
            return None
        valid, parameters, repr_ = outcome
        return parameters if valid else repr_


def _is_native_plugin(name: str) -> bool:
    """
    Whether `name` is a native (Rust) plugin in the configured plugins directories.
    """
    from meerschaum import _core
    try:
        return name in _core.native_plugin_names()
    except Exception:
        return False
