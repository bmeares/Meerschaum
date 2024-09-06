#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define methods for registering plugins.
"""

from __future__ import annotations

import json

import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Any, List, SuccessTuple, Dict, Union

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
    return cls.get_entity_key(PLUGIN_PREFIX, plugin_name, sub_key)


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
    plugin_attributes_str = json.dumps(plugin.attributes, separators=(',', ':'))
    mutable_keys_vals = {
        cls.get_plugin_key(plugin.name, 'attributes'): plugin_attributes_str,
        cls.get_plugin_key(plugin.name, 'version'): plugin.version,
    }
    if mutable_only:
        return mutable_keys_vals

    immutable_keys_vals = {
        cls.get_plugin_key(plugin.name, 'user_id'): plugin.user_id,
    }

    return {**immutable_keys_vals, **mutable_keys_vals}


def register_plugin(
    self,
    plugin: 'mrsm.core.Plugin',
    force: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Register a new plugin to the `mrsm_plugins` "table"."""
    from meerschaum.utils.misc import generate_password

    plugins_pipe = self.get_plugins_pipe()
    keys_vals = self.get_plugin_keys_vals(plugin)

    try:
        sync_success, sync_msg = plugins_pipe.sync(
            [
                {
                    'plugin_name': plugin.name,
                    'user_id': plugin.user_id,
                },
            ],
            check_existing=False,
            debug=debug,
        )
        if not sync_success:
            return sync_success, sync_msg

        for key, val in keys_vals.items():
            if val is not None:
                self.set(key, val)

        success, msg = True, "Success"
    except Exception as e:
        success = False
        msg = f"Failed to register plugin '{plugin.name}':\n{e}"

    if not success:
        for key in keys_vals:
            try:
                self.client.delete(key)
            except Exception:
                pass

    return success, msg


def get_plugin_id(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Union[str, None]:
    """
    Return a plugin's ID.
    """
    return plugin.name


def get_plugin_version(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False,
) -> Union[str, None]:
    """
    Return a plugin's version.
    """
    version_key = self.get_plugin_key(plugin.name, 'version')

    try:
        return self.get(version_key)
    except Exception:
        return None


def get_plugin_user_id(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Union[str, None]:
    """
    Return a plugin's user ID.
    """
    user_id_key = self.get_plugin_key(plugin.name, 'user_id')

    try:
        return self.get(user_id_key)
    except Exception:
        return None


def get_plugin_username(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Union[str]:
    """
    Return the username of a plugin's owner.
    """
    user_id = self.get_plugin_user_id(plugin, debug=debug)
    if user_id is None:
        return None

    username_key = self.get_user_key(user_id, 'username')
    try:
        return self.get(username_key)
    except Exception:
        return None


def get_plugin_attributes(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Dict[str, Any]:
    """
    Return the attributes of a plugin.
    """
    attributes_key = self.get_plugin_key(plugin.name, 'attributes')
    try:
        attributes_str = self.get(attributes_key)
        if not attributes_str:
            return {}
        return json.loads(attributes_str)
    except Exception:
        return {}


def get_plugins(
    self,
    user_id: Optional[int] = None,
    search_term: Optional[str] = None,
    debug: bool = False,
    **kw: Any
) -> List[str]:
    """
    Return a list of plugin names.
    """
    plugins_pipe = self.get_plugins_pipe()
    params = {}
    if user_id:
        params['user_id'] = user_id

    df = plugins_pipe.get_data(['plugin_name'], params=params, debug=debug)
    docs = df.to_dict(orient='records')

    return [
        doc['plugin_name']
        for doc in docs
        if (plugin_name := doc['plugin_name']).startswith(search_term or '')
    ]


def delete_plugin(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Delete a plugin from the plugins table.
    """
    plugins_pipe = self.get_plugins_pipe()
    clear_success, clear_msg = plugins_pipe.clear(params={'plugin_name': plugin.name}, debug=debug)
    if not clear_success:
        return clear_success, clear_msg

    keys_vals = self.get_plugin_keys_vals(plugin)
    try:
        old_keys_vals = {
            key: self.get(key)
            for key in keys_vals
        }
    except Exception as e:
        return False, f"Failed to delete plugin '{plugin.name}':\n{e}"

    try:
        for key in keys_vals:
            self.client.delete(key)
        success, msg = True, "Success"
    except Exception as e:
        success = False
        msg = f"Failed to delete plugin '{plugin.name}':\n{e}"

    if not success:
        try:
            for key, old_val in old_keys_vals.items():
                self.set(key, old_val)
        except Exception:
            pass

    return success, msg
