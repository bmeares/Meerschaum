#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage plugins via the API connector
"""

from __future__ import annotations
from meerschaum.utils.typing import Union, Any, Optional, SuccessTuple, Mapping, Sequence

def plugin_r_url(
        plugin : Union[meerschaum._internal.Plugin, str]
    ) -> str:
    """
    Generate a relative URL path from a Pipe's keys.
    """
    return f'/mrsm/plugins/{plugin}'

def register_plugin(
        self,
        plugin : meerschaum._internal.Plugin,
        make_archive : bool = True,
        debug : bool = False
    ) -> SuccessTuple:
    """
    Register a plugin and upload its archive.
    """
    import json
    if make_archive: archive_path = plugin.make_tar(debug=debug)
    else: archive_path = plugin.archive_path
    file_pointer = open(archive_path, 'rb')
    files = {'archive' : file_pointer}
    metadata = {
        'version' : plugin.version,
        'attributes': json.dumps(plugin.attributes),
    }
    r_url = plugin_r_url(plugin)
    try:
        response = self.post(r_url, files=files, params=metadata)
    except:
        success, msg = False, f"Failed to register plugin '{plugin}'"
    finally:
        file_pointer.close()

    try:
        success, msg = json.loads(response.text)
    except:
        success, msg = False, response.text

    return success, msg

def install_plugin(
        self,
        name : str,
        debug : bool = False
    ) -> SuccessTuple:
    """
    Download and attempt to install a plugin from the API.

    :param name:
        The name of the plugin to be installed.
    """
    import os, pathlib
    from meerschaum._internal import Plugin
    from meerschaum.config._paths import PLUGINS_TEMP_RESOURCES_PATH
    from meerschaum.utils.debug import dprint
    r_url = plugin_r_url(name)
    dest = pathlib.Path(os.path.join(PLUGINS_TEMP_RESOURCES_PATH, name + '.tar.gz'))
    if debug: dprint(f"Fetching from '{r_url}' to '{dest}'")
    archive_path = self.wget(r_url, dest) 
    plugin = Plugin(name, archive_path=archive_path)
    return plugin.install(debug=debug)

def get_plugins(
        self,
        user_id : Optional[int] = None,
        debug : bool = False
    ) -> Sequence[str]:
    """
    Return a list of registered plugin names.

    :param user_id:
        If specified, return all plugins from a certain user.
    """
    import json
    from meerschaum.utils.warnings import warn, error
    response = self.get('/mrsm/plugins', params={'user_id' : user_id})
    plugins = json.loads(response.text)
    if not isinstance(plugins, list): error(response.text)
    return plugins

def get_plugin_attributes(
        self,
        plugin : meerschaum._internal.Plugin,
        debug : bool = False
    ) -> Mapping[str, Any]:
    """
    Return attributes of a registered plugin.
    """
    import json
    from meerschaum.utils.warnings import warn, error
    response = self.get(f'/mrsm/plugins/{plugin.name}/attributes')
    attributes = json.loads(response.text)
    if not isinstance(attributes, dict):
        error(response.text)
    return attributes

