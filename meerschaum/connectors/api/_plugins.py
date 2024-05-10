#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage plugins via the API connector
"""

from __future__ import annotations
from meerschaum.utils.typing import Union, Any, Optional, SuccessTuple, Mapping, Sequence

def plugin_r_url(
        plugin: Union[meerschaum.core.Plugin, str]
    ) -> str:
    """Generate a relative URL path from a Plugin."""
    from meerschaum.config.static import STATIC_CONFIG
    return f"{STATIC_CONFIG['api']['endpoints']['plugins']}/{plugin}"

def register_plugin(
        self,
        plugin: meerschaum.core.Plugin,
        make_archive: bool = True,
        debug: bool = False,
    ) -> SuccessTuple:
    """Register a plugin and upload its archive."""
    import json
    archive_path = plugin.make_tar(debug=debug) if make_archive else plugin.archive_path
    file_pointer = open(archive_path, 'rb')
    files = {'archive': file_pointer}
    metadata = {
        'version': plugin.version,
        'attributes': json.dumps(plugin.attributes),
    }
    r_url = plugin_r_url(plugin)
    try:
        response = self.post(r_url, files=files, params=metadata, debug=debug)
    except Exception as e:
        return False, f"Failed to register plugin '{plugin}'."
    finally:
        file_pointer.close()

    try:
        success, msg = json.loads(response.text)
    except Exception as e:
        return False, response.text

    return success, msg

def install_plugin(
        self,
        name: str,
        skip_deps: bool = False,
        force: bool = False,
        debug: bool = False
    ) -> SuccessTuple:
    """Download and attempt to install a plugin from the API."""
    import os, pathlib, json
    from meerschaum.core import Plugin
    from meerschaum.config._paths import PLUGINS_TEMP_RESOURCES_PATH
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    binaryornot_check = attempt_import('binaryornot.check', lazy=False)
    r_url = plugin_r_url(name)
    dest = pathlib.Path(os.path.join(PLUGINS_TEMP_RESOURCES_PATH, name + '.tar.gz'))
    if debug:
        dprint(f"Fetching from '{self.url + r_url}' to '{dest}'...")
    archive_path = self.wget(r_url, dest, debug=debug) 
    is_binary = binaryornot_check.is_binary(str(archive_path))
    if not is_binary:
        fail_msg = f"Failed to download binary for plugin '{name}'."
        try:
            with open(archive_path, 'r') as f:
                j = json.load(f)
            if isinstance(j, list):
                success, msg = tuple(j)
            elif isinstance(j, dict) and 'detail' in j:
                success, msg = False, fail_msg
        except Exception as e:
            success, msg = False, fail_msg
        return success, msg
    plugin = Plugin(name, archive_path=archive_path, repo_connector=self)
    return plugin.install(skip_deps=skip_deps, force=force, debug=debug)

def get_plugins(
        self,
        user_id : Optional[int] = None,
        search_term : Optional[str] = None,
        debug : bool = False
    ) -> Sequence[str]:
    """Return a list of registered plugin names.

    Parameters
    ----------
    user_id :
        If specified, return all plugins from a certain user.
    user_id : Optional[int] :
         (Default value = None)
    search_term : Optional[str] :
         (Default value = None)
    debug : bool :
         (Default value = False)

    Returns
    -------

    """
    import json
    from meerschaum.utils.warnings import warn, error
    from meerschaum.config.static import STATIC_CONFIG
    response = self.get(
        STATIC_CONFIG['api']['endpoints']['plugins'],
        params = {'user_id' : user_id, 'search_term' : search_term},
        use_token = True,
        debug = debug
    )
    if not response:
        return []
    plugins = json.loads(response.text)
    if not isinstance(plugins, list):
        error(response.text)
    return plugins

def get_plugin_attributes(
        self,
        plugin: meerschaum.core.Plugin,
        debug: bool = False
    ) -> Mapping[str, Any]:
    """
    Return a plugin's attributes.
    """
    import json
    from meerschaum.utils.warnings import warn, error
    r_url = plugin_r_url(plugin) + '/attributes'
    response = self.get(r_url, use_token=True, debug=debug)
    attributes = response.json()
    if isinstance(attributes, str) and attributes and attributes[0] == '{':
        try:
            attributes = json.loads(attributes)
        except Exception as e:
            pass
    if not isinstance(attributes, dict):
        error(response.text)
    elif not response and 'detail' in attributes:
        warn(attributes['detail'])
        return {}
    return attributes

def delete_plugin(
        self,
        plugin: meerschaum.core.Plugin,
        debug: bool = False
    ) -> SuccessTuple:
    """Delete a plugin from an API repository."""
    import json
    r_url = plugin_r_url(plugin)
    try:
        response = self.delete(r_url, debug=debug)
    except Exception as e:
        return False, f"Failed to delete plugin '{plugin}'."

    try:
        success, msg = json.loads(response.text)
    except Exception as e:
        return False, response.text

    return success, msg

