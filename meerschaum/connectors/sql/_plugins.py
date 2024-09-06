#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for managing plugins registration via the SQL connector
"""

from __future__ import annotations

import json

import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Any, List, SuccessTuple, Dict

def register_plugin(
    self,
    plugin: 'mrsm.core.Plugin',
    force: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Register a new plugin to the plugins table."""
    from meerschaum.utils.warnings import warn, error
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')
    from meerschaum.utils.sql import json_flavors
    from meerschaum.connectors.sql.tables import get_tables
    plugins_tbl = get_tables(mrsm_instance=self, debug=debug)['plugins']

    old_id = self.get_plugin_id(plugin, debug=debug)

    ### Check for version conflict. May be overridden with `--force`.
    if old_id is not None and not force:
        old_version = self.get_plugin_version(plugin, debug=debug)
        new_version = plugin.version
        if old_version is None:
            old_version = ''
        if new_version is None:
            new_version = ''

        ### verify that the new version is greater than the old
        packaging_version = attempt_import('packaging.version')
        if (
            old_version and new_version
            and packaging_version.parse(old_version) >= packaging_version.parse(new_version)
        ):
            return False, (
                f"Version '{new_version}' of plugin '{plugin}' " +
                f"must be greater than existing version '{old_version}'."
            )

    bind_variables = {
        'plugin_name': plugin.name,
        'version': plugin.version,
        'attributes': (
            json.dumps(plugin.attributes) if self.flavor not in json_flavors else plugin.attributes
        ),
        'user_id': plugin.user_id,
    }

    if old_id is None:
        query = sqlalchemy.insert(plugins_tbl).values(**bind_variables)
    else:
        query = (
            sqlalchemy.update(plugins_tbl)
            .values(**bind_variables)
            .where(plugins_tbl.c.plugin_id == old_id)
        )

    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to register plugin '{plugin}'."
    return True, f"Successfully registered plugin '{plugin}'."

def get_plugin_id(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Optional[int]:
    """
    Return a plugin's ID.
    """
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    plugins_tbl = get_tables(mrsm_instance=self, debug=debug)['plugins']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    query = (
        sqlalchemy
        .select(plugins_tbl.c.plugin_id)
        .where(plugins_tbl.c.plugin_name == plugin.name)
    )
    
    try:
        return int(self.value(query, debug=debug))
    except Exception as e:
        return None

def get_plugin_version(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Optional[str]:
    """
    Return a plugin's version.
    """
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    plugins_tbl = get_tables(mrsm_instance=self, debug=debug)['plugins']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')
    query = sqlalchemy.select(plugins_tbl.c.version).where(plugins_tbl.c.plugin_name == plugin.name)
    return self.value(query, debug=debug)

def get_plugin_user_id(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Optional[int]:
    """
    Return a plugin's user ID.
    """
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    plugins_tbl = get_tables(mrsm_instance=self, debug=debug)['plugins']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    query = (
        sqlalchemy
        .select(plugins_tbl.c.user_id)
        .where(plugins_tbl.c.plugin_name == plugin.name)
    )

    try:
        return int(self.value(query, debug=debug))
    except Exception as e:
        return None

def get_plugin_username(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Optional[str]:
    """
    Return the username of a plugin's owner.
    """
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    plugins_tbl = get_tables(mrsm_instance=self, debug=debug)['plugins']
    users = get_tables(mrsm_instance=self, debug=debug)['users']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    query = (
        sqlalchemy.select(users.c.username)
        .where(
            users.c.user_id == plugins_tbl.c.user_id
            and plugins_tbl.c.plugin_name == plugin.name
        )
    )

    return self.value(query, debug=debug)


def get_plugin_attributes(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False
) -> Dict[str, Any]:
    """
    Return the attributes of a plugin.
    """
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    plugins_tbl = get_tables(mrsm_instance=self, debug=debug)['plugins']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    query = (
        sqlalchemy
        .select(plugins_tbl.c.attributes)
        .where(plugins_tbl.c.plugin_name == plugin.name)
    )

    _attr = self.value(query, debug=debug)
    if isinstance(_attr, str):
        _attr = json.loads(_attr)
    elif _attr is None:
        _attr = {}
    return _attr

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
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    plugins_tbl = get_tables(mrsm_instance=self, debug=debug)['plugins']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    query = sqlalchemy.select(plugins_tbl.c.plugin_name)
    if user_id is not None:
        query = query.where(plugins_tbl.c.user_id == user_id)
    if search_term is not None:
        query = query.where(plugins_tbl.c.plugin_name.like(search_term + '%'))

    rows = (
        self.execute(query).fetchall()
        if self.flavor != 'duckdb'
        else [
            (row['plugin_name'],)
            for row in self.read(query).to_dict(orient='records')
        ]
    )
    
    return [row[0] for row in rows]


def delete_plugin(
    self,
    plugin: 'mrsm.core.Plugin',
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Delete a plugin from the plugins table."""
    from meerschaum.utils.warnings import warn, error
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')
    from meerschaum.connectors.sql.tables import get_tables
    plugins_tbl = get_tables(mrsm_instance=self, debug=debug)['plugins']

    plugin_id = self.get_plugin_id(plugin, debug=debug)
    if plugin_id is None:
        return True, f"Plugin '{plugin}' was not registered."

    bind_variables = {
        'plugin_id' : plugin_id,
    }

    query = sqlalchemy.delete(plugins_tbl).where(plugins_tbl.c.plugin_id == plugin_id)
    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to delete plugin '{plugin}'."
    return True, f"Successfully deleted plugin '{plugin}'."
