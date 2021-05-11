#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for managing plugins registration via the SQL connector
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Any, List, SuccessTuple, Dict

def register_plugin(
        self,
        plugin : 'meerschaum._internal.Plugin.Plugin',
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Register a new plugin to the plugins table.
    """

    from meerschaum.utils.warnings import warn, error
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')
    from meerschaum.connectors.sql.tables import get_tables
    plugins = get_tables(mrsm_instance=self, debug=debug)['plugins']

    old_id = self.get_plugin_id(plugin, debug=debug)

    if old_id is not None:
        old_version = self.get_plugin_version(plugin, debug=debug)
        new_version = plugin.version
        if old_version is None:
            old_version = ''
        if new_version is None:
            new_version = ''

        ### verify that the new version is greater than the old
        from packaging import version as packaging_version
        if packaging_version.parse(old_version) >= packaging_version.parse(new_version):
            return False, (
                f"Version '{new_version}' of plugin '{plugin}' " +
                f"must be greater than existing version '{old_version}'."
            )

    import json
    bind_variables = {
        'plugin_name' : plugin.name,
        'version' : plugin.version,
        'attributes' : json.dumps(plugin.attributes),
        'user_id' : plugin.user_id,
    }

    if old_id is None:
        query = sqlalchemy.insert(plugins).values(**bind_variables)
    else:
        query = (
            sqlalchemy.update(plugins).
            values(**bind_variables).
            where(plugins.c.plugin_id == old_id)
        )

    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to register plugin '{plugin}'."
    return True, f"Successfully registered plugin '{plugin}'."

def get_plugin_id(
        self,
        plugin : 'meerschaum._internal.Plugin.Plugin',
        debug : bool = False
    ) -> Optional[int]:
    """
    Return a plugin's id if it is registered, else return None.
    """
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    plugins = get_tables(mrsm_instance=self, debug=debug)['plugins']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    query = sqlalchemy.select([plugins.c.plugin_id]).where(plugins.c.plugin_name == plugin.name)
    
    try:
        return int(self.value(query, debug=debug))
    except Exception as e:
        return None

def get_plugin_version(
        self,
        plugin : 'meerschaum._internal.Plugin.Plugin',
        debug : bool = False
    ) -> Optional[str]:
    """
    Return a plugin's version if it exists.
    """
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    plugins = get_tables(mrsm_instance=self, debug=debug)['plugins']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    query = sqlalchemy.select([plugins.c.version]).where(plugins.c.plugin_name == plugin.name)

    return self.value(query, debug=debug)

def get_plugin_user_id(
        self,
        plugin : 'meerschaum._internal.Plugin.Plugin',
        debug : bool = False
    ) -> Optional[int]:
    """
    Return a plugin's user_id if it exists.
    """
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    plugins = get_tables(mrsm_instance=self, debug=debug)['plugins']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    query = sqlalchemy.select([plugins.c.user_id]).where(plugins.c.plugin_name == plugin.name)

    try:
        return int(self.value(query, debug=debug))
    except Exception as e:
        return None

def get_plugin_username(
        self,
        plugin : 'meerschaum._internal.Plugin.Plugin',
        debug : bool = False
    ) -> Optional[str]:
    """
    Return the username of a plugin's user_id, if it exists.
    """
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    plugins = get_tables(mrsm_instance=self, debug=debug)['plugins']
    users = get_tables(mrsm_instance=self, debug=debug)['users']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    query = (
        sqlalchemy.select([users.c.username]).
        where(
            users.c.users_id == plugins.c.user_id
            and plugins.c.plugin_name == plugin.name
        )
    )

    return self.value(query, debug=debug)

def get_plugin_attributes(
        self,
        plugin : 'meerschaum._internal.Plugin.Plugin',
        debug : bool = False
    ) -> Optional[Dict[str, Any]]:
    """
    Return attributes for a plugin, if it exists.
    """
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    plugins = get_tables(mrsm_instance=self, debug=debug)['plugins']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    query = sqlalchemy.select([plugins.c.attributes]).where(plugins.c.plugin_name == plugin.name)

    return self.value(query, debug=debug)

def get_plugins(
        self,
        user_id : Optional[int] = None,
        search_term : Optional[str] = None,
        debug : bool = False,
        **kw : Any
    ) -> List[str]:
    """
    Return a list of all registered plugins.

    :param user_id:
        If specified, filter plugins by a specific `user_id`.

    :param search_term:
        If specified, add a `WHERE plugin_name LIKE '{search_term}%'` clause to filter the plugins.
    """
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    plugins = get_tables(mrsm_instance=self, debug=debug)['plugins']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    query = sqlalchemy.select([plugins.c.plugin_name])
    if user_id is not None:
        query = query.where(plugins.c.user_id == user_id)
    if search_term is not None:
        query = query.where(plugins.c.plugin_name.like(search_term + '%'))

    return [row['plugin_name'] for row in self.engine.execute(query).fetchall()]

def delete_plugin(
        self,
        plugin : 'meerschaum._internal.Plugin.Plugin',
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Delete a plugin from the plugins table.
    """

    from meerschaum.utils.warnings import warn, error
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')
    from meerschaum.connectors.sql.tables import get_tables
    plugins = get_tables(mrsm_instance=self, debug=debug)['plugins']

    plugin_id = self.get_plugin_id(plugin, debug=debug)
    if plugin_id is None:
        return True, f"Plugin '{plugin}' was not registered."

    bind_variables = {
        'plugin_id' : plugin_id,
    }

    query = sqlalchemy.delete(plugins).where(plugins.c.plugin_id == plugin_id)
    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to delete plugin '{plugin}'."
    return True, f"Successfully deleted plugin '{plugin}'."


