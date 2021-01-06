#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for managing plugins registration via the SQL connector
"""

def register_plugin(
        self,
        plugin : 'meerschaum.Plugin',
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Register a new plugin
    """

    from meerschaum.utils.warnings import warn, error

    old_id = self.get_plugin_id(plugin, debug=debug)

    if old_id is not None:
        old_version = self.get_plugin_version(plugin, debug=debug)
        new_version = plugin.version
        if old_version is None: old_version = ''
        if new_version is None: new_version = ''

        ### verify that the new version is greater than the old
        from packaging import version as packaging_version
        if packaging_version.parse(old_version) >= packaging_version.parse(new_version):
            return False, (
                f"Version '{new_version}' of plugin '{plugin}' must be greater than existing version '{old_version}'."
            )

    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    tables = get_tables(mrsm_instance=self, debug=debug)

    import json
    bind_variables = {
        'plugin_name' : plugin.name,
        'version' : plugin.version,
        'attributes' : json.dumps(plugin.attributes),
        'user_id' : plugin.user_id,
        'plugin_id' : old_id,
    }

    if old_id is None:
        query = f"""
        INSERT INTO plugins (
            plugin_name,
            version,
            user_id,
            attributes
        ) VALUES (
            %(plugin_name)s,
            %(version)s,
            %(user_id)s,
            %(attributes)s
        );
        """
    else:
        query = f"""
        UPDATE plugins
        SET plugin_name = %(plugin_name)s',
            version = %(version)s,
            attributes = %(attributes)s'
        WHERE plugin_id = %(plugin_id)s
        """

    result = self.exec(query, bind_variables, debug=debug)
    if result is None:
        return False, f"Failed to register plugin '{plugin}'"
    return True, f"Successfully registered plugin '{plugin}'"

def get_plugin_id(
        self,
        plugin : 'meerschaum.Plugin',
        debug : bool = False
    ) -> int:
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    tables = get_tables(mrsm_instance=self, debug=debug)

    query = f"""
    SELECT plugin_id
    FROM plugins
    WHERE plugin_name = %s
    """
    return self.value(query, (plugin.name,), debug=debug)

def get_plugin_version(
        self,
        plugin : 'meerschaum.Plugin',
        debug : bool = False
    ) -> str:
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    tables = get_tables(mrsm_instance=self, debug=debug)

    query = f"""
    SELECT version
    FROM plugins
    WHERE plugin_name = %s
    """
    return self.value(query, (plugin.name,), debug=debug)

def get_plugin_user_id(
        self,
        plugin : 'meerschaum.Plugin',
        debug : bool = False
    ) -> str:
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    tables = get_tables(mrsm_instance=self, debug=debug)

    query = """
    SELECT user_id
    FROM plugins
    WHERE plugin_name = %s
    """
    return self.value(query, (plugin.name,), debug=debug)

def get_plugin_username(
        self,
        plugin : 'meerschaum.Plugin',
        debug : bool = False
    ) -> str:
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    tables = get_tables(mrsm_instance=self, debug=debug)

    bind_variables = { 'plugin_name' : plugin.name, }

    query = f"""
    SELECT users.username
    FROM plugins
    INNER JOIN users ON users.user_id = plugins.user_id
    WHERE plugin_name = %(plugin_name)s
    """
    return self.value(query, bind_variables, debug=debug)

def get_plugins(
        self,
        user_id : int = None,
        debug : bool = False,
        **kw
    ) -> list:
    ### ensure plugins table exists
    from meerschaum.connectors.sql.tables import get_tables
    tables = get_tables(mrsm_instance=self, debug=debug)

    bind_variables = {'user_id' : user_id}

    q = f"""
    SELECT plugin_name
    FROM plugins
    """ + ("""
    WHERE user_id = %(user_id)s
    """ if user_id is not None else "")
    return list(self.read(q, bind_variables, debug=debug)['plugin_name'])

