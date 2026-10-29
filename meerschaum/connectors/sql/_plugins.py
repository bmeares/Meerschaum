#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for managing plugins registration via the SQL connector
"""

from __future__ import annotations


import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Any, List, SuccessTuple, Dict


def get_plugins_pipe(self) -> mrsm.Pipe:
    """
    Return the internal metadata plugins pipe.
    """
    users_pipe = self.get_users_pipe()
    user_id_dtype = users_pipe.dtypes.get('user_id', 'int')
    return mrsm.Pipe(
        'mrsm', 'plugins',
        instance=self,
        temporary=True,
        static=True,
        null_indices=False,
        columns={
            'primary': 'plugin_id',
            'user_id': 'user_id',    
        },
        dtypes={
            'plugin_name': 'string',
            'user_id': user_id_dtype,
            'attributes': 'json',
            'version': 'string',
        },
        indices={
            'unique': 'plugin_name',
        },
    )


def register_plugin(
    self,
    plugin: 'mrsm.core.Plugin',
    force: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Register a new plugin to the plugins table."""
    return self._rust_method('register_plugin')(plugin, force=force)

def get_plugin_id(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Optional[int]:
    """
    Return a plugin's ID.
    """
    return self._rust_method('get_plugin_id')(plugin)


def get_plugin_version(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Optional[str]:
    """
    Return a plugin's version.
    """
    return self._rust_method('get_plugin_version')(plugin)

def get_plugin_user_id(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Optional[int]:
    """
    Return a plugin's user ID.
    """
    return self._rust_method('get_plugin_user_id')(plugin)

def get_plugin_username(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Optional[str]:
    """
    Return the username of a plugin's owner.
    """
    return self._rust_method('get_plugin_username')(plugin)


def get_plugin_attributes(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Dict[str, Any]:
    """
    Return the attributes of a plugin.
    """
    return self._rust_method('get_plugin_attributes')(plugin)

def get_plugins(
    self,
    user_id: Optional[int] = None,
    search_term: Optional[str] = None,
    debug: bool = False,
    **kw: Any
) -> List[str]:
    """
    Return a list of all registered plugins.

    Parameters
    ----------
    user_id: Optional[int], default None
        If specified, filter plugins by a specific `user_id`.

    search_term: Optional[str], default None
        If specified, add a `WHERE plugin_name LIKE '{search_term}%'` clause to filter the plugins.


    Returns
    -------
    A list of plugin names.
    """
    return self._rust_method('get_plugins')(user_id=user_id, search_term=search_term)


def delete_plugin(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Delete a plugin from the plugins table."""
    return self._rust_method('delete_plugin')(plugin)
