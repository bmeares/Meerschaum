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

    version = 'NULL' if plugin.version is None else f"'{plugin.version}'"

    import json
    attributes = json.dumps(plugin.attributes).replace("'", "''")
    if old_id is None:
        query = f"""
        INSERT INTO plugins (
            plugin_name,
            version,
            attributes
        ) VALUES (
            '{plugin.name}',
            {version},
            '{attributes}'
        );
        """
    else:
        query = f"""
        UPDATE plugins
        SET plugin_name = '{plugin.name}', version = {version}, attributes = '{attributes}'
        WHERE plugin_id = {old_id}
        """

    result = self.exec(query, debug=debug)
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
    WHERE plugin_name = '{plugin.name}'
    """
    return self.value(query, debug=debug)

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
    WHERE plugin_name = '{plugin.name}'
    """
    return self.value(query, debug=debug)

