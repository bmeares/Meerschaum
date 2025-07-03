#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define high-level plugins methods for instance connectors.
"""

import uuid
from typing import Union, Optional, List, Dict, Any

import meerschaum as mrsm
from meerschaum.core import Plugin


def get_plugins_pipe(self) -> 'mrsm.Pipe':
    """
    Return the internal pipe for syncing plugins metadata.
    """
    users_pipe = self.get_users_pipe()
    user_id_dtype = users_pipe.dtypes.get('user_id', 'uuid')
    return mrsm.Pipe(
        'mrsm', 'plugins',
        instance=self,
        target='mrsm_plugins',
        temporary=True,
        static=True,
        null_indices=False,
        columns={
            'primary': 'plugin_name',
            'user_id': 'user_id',
        },
        dtypes={
            'plugin_name': 'string',
            'user_id': user_id_dtype,
            'attributes': 'json',
            'version': 'string',
        },
    )


def register_plugin(self, plugin: Plugin, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Register a new plugin to the plugins table.
    """
    plugins_pipe = self.get_plugins_pipe()
    users_pipe = self.get_users_pipe()
    user_id = self.get_plugin_user_id(plugin)
    if user_id is not None:
        username = self.get_username(user_id, debug=debug)
        return False, f"{plugin} is already registered to '{username}'."

    doc = {
        'plugin_name': plugin.name,
        'version': plugin.version,
        'attributes': plugin.attributes,
        'user_id': plugin.user_id,
    }

    sync_success, sync_msg = plugins_pipe.sync(
        [doc],
        check_existing=False,
        debug=debug,
    )
    if not sync_success:
        return False, f"Failed to register {plugin}:\n{sync_msg}"

    return True, "Success"


def get_plugin_user_id(self, plugin: Plugin, debug: bool = False) -> Union[uuid.UUID, None]:
    """
    Return the user ID for plugin's owner.
    """
    plugins_pipe = self.get_plugins_pipe() 
    return plugins_pipe.get_value('user_id', {'plugin_name': plugin.name}, debug=debug)


def get_plugin_username(self, plugin: Plugin, debug: bool = False) -> Union[uuid.UUID, None]:
    """
    Return the username for plugin's owner.
    """
    user_id = self.get_plugin_user_id(plugin, debug=debug)
    if user_id is None:
        return None
    return self.get_username(user_id, debug=debug)


def get_plugin_id(self, plugin: Plugin, debug: bool = False) -> Union[str, None]:
    """
    Return a plugin's ID.
    """
    user_id = self.get_plugin_user_id(plugin, debug=debug)
    return plugin.name if user_id is not None else None


def delete_plugin(self, plugin: Plugin, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Delete a plugin's registration.
    """
    plugin_id = self.get_plugin_id(plugin, debug=debug)
    if plugin_id is None:
        return False, f"{plugin} is not registered."
    
    plugins_pipe = self.get_plugins_pipe()
    clear_success, clear_msg = plugins_pipe.clear(params={'plugin_name': plugin.name}, debug=debug)
    if not clear_success:
        return False, f"Failed to delete {plugin}:\n{clear_msg}"
    return True, "Success"


def get_plugin_version(self, plugin: Plugin, debug: bool = False) -> Union[str, None]:
    """
    Return the version for a plugin.
    """
    plugins_pipe = self.get_plugins_pipe() 
    return plugins_pipe.get_value('version', {'plugin_name': plugin.name}, debug=debug)


def get_plugin_attributes(self, plugin: Plugin, debug: bool = False) -> Dict[str, Any]:
    """
    Return the attributes for a plugin.
    """
    plugins_pipe = self.get_plugins_pipe() 
    return plugins_pipe.get_value('attributes', {'plugin_name': plugin.name}, debug=debug) or {}


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
    if df is None:
        return []

    docs = df.to_dict(orient='records')
    return [
        plugin_name
        for doc in docs
        if (plugin_name := doc['plugin_name']).startswith(search_term or '')
    ]
