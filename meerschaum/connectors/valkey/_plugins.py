#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define methods for registering plugins (the Rust client, `meerschaum._core.ValkeyConnector`).
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import Any, SuccessTuple, Dict, Union

PLUGINS_TABLE: str = "mrsm_plugins"
PLUGIN_PREFIX: str = "mrsm_plugin"


def get_plugins_pipe(self) -> mrsm.Pipe:
    """
    Return the pipe to store the plugins.
    """
    return mrsm.Pipe(
        'mrsm', 'plugins',
        columns=['plugin_name'],
        temporary=True,
        target=PLUGINS_TABLE,
        instance=self,
    )


@classmethod
def get_plugin_key(cls, plugin_name: str, sub_key: str) -> str:
    """
    Return the key for a plugin's attribute.
    """
    from meerschaum import _core
    return _core.ValkeyConnector.get_plugin_key(plugin_name, sub_key)


@classmethod
def get_plugin_keys_vals(
    cls,
    plugin: 'mrsm.core.Plugin',
    mutable_only: bool = False,
) -> Dict[str, str]:
    """
    Return a dictionary containing keys and values to set for the plugin.

    Parameters
    ----------
    plugin: mrsm.core.Plugin
        The plugin for which to generate the keys.

    mutable_only: bool, default False
        If `True`, only return keys which may be edited.

    Returns
    -------
    A dictionary mapping a plugins's keys to values.
    """
    from meerschaum import _core
    return _core.ValkeyConnector.get_plugin_keys_vals(plugin, mutable_only)


def register_plugin(
    self,
    plugin: 'mrsm.core.Plugin',
    force: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Register a new plugin to the `mrsm_plugins` "table".
    """
    return self._rust_client().register_plugin(plugin, force)


def get_plugin_id(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Union[str, None]:
    """
    Return a plugin's ID (its name when it is registered).
    """
    return self._rust_client().get_plugin_id(plugin)


def get_plugin_version(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Union[str, None]:
    """
    Return the version for a plugin.
    """
    return self._rust_client().get_plugin_version(plugin)


def get_plugin_user_id(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Union[str, None]:
    """
    Return the user ID for a plugin's owner.
    """
    return self._rust_client().get_plugin_user_id(plugin)


def get_plugin_username(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Union[str, None]:
    """
    Return the username for a plugin's owner.
    """
    return self._rust_client().get_plugin_username(plugin)


def get_plugin_attributes(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Dict[str, Any]:
    """
    Return the attributes of a plugin.
    """
    return self._rust_client().get_plugin_attributes(plugin)


def delete_plugin(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Delete a plugin's registration.
    """
    return self._rust_client().delete_plugin(plugin)
