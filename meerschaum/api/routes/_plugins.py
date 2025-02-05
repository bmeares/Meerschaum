#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Routes for managing plugins
"""

from __future__ import annotations
import json
import shutil
import pathlib
import os

from meerschaum.utils.typing import Optional, List, SuccessTuple, Any, Dict

from meerschaum.api import (
    fastapi,
    app,
    endpoints,
    get_api_connector,
    manager,
    debug,
    private,
    no_auth,
    default_instance_keys,
)
from meerschaum.api.tables import get_tables
from fastapi import File, UploadFile
from meerschaum.utils.packages import attempt_import
from meerschaum.core import Plugin
starlette_responses = attempt_import('starlette.responses', warn=False, lazy=False)
FileResponse = starlette_responses.FileResponse

sqlalchemy = attempt_import('sqlalchemy', lazy=False)
plugins_endpoint = endpoints['plugins']

PLUGINS_INSTANCE_KEYS = default_instance_keys


@app.post(plugins_endpoint + '/{name}', tags=['Plugins'])
def register_plugin(
    name: str,
    version: str = None,
    attributes: str = None,
    archive: UploadFile = File(...),
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> SuccessTuple:
    """
    Register a plugin and save its archive file.

    Parameters
    ----------
    name: str :
        The name of the plugin.
        
    version: str, default None
        The version of the plugin.

    attributes: str, default None
        JSON-encoded string of the attributes dictionary.

    archive: UploadFile :
        The archive file of the plugin.

    curr_user: 'meerschaum.core.User'
        The logged-in user.

    Returns
    -------
    A `SuccessTuple` of success, message.

    """
    from meerschaum.config import get_config
    allow_plugins = get_config('system', 'api', 'permissions', 'registration', 'plugins')
    if not allow_plugins:
        return False, (
            "The administrator for this server has not allowed plugin registration.\n\n" +
            "Please contact the system administrator, or if you are running this server, " +
            "open the configuration file with `edit config system` and search for 'permissions'. " +
            " Under the keys `api:permissions:registration`, " +
            "you can toggle various registration types."
        )

    get_tables()
    if attributes is None:
        attributes = json.dumps({})
    attributes = json.loads(attributes)
    if isinstance(attributes, str) and attributes[0] == '{':
        try:
            attributes = json.loads(attributes)
        except Exception:
            pass

    plugin = Plugin(name, version=version, attributes=attributes)
    if curr_user is None:
        return (
            False,
            "Cannot register a plugin without logging in (are you running with `--insecure`)?"
        )

    if curr_user is not None:
        plugin_user_id = get_api_connector(PLUGINS_INSTANCE_KEYS).get_plugin_user_id(plugin)
        curr_user_id = get_api_connector(PLUGINS_INSTANCE_KEYS).get_user_id(curr_user) if curr_user is not None else -1
        if plugin_user_id is not None and plugin_user_id != curr_user_id:
            return False, f"User '{curr_user.username}' cannot edit plugin '{plugin}'."
        plugin.user_id = curr_user_id

    success, msg = get_api_connector(
        PLUGINS_INSTANCE_KEYS
    ).register_plugin(plugin, make_archive=False, debug=debug)

    if success:
        archive_path = plugin.archive_path
        temp_archive_path = pathlib.Path(str(archive_path) + '.tmp')
        with temp_archive_path.open('wb') as buff:
            shutil.copyfileobj(archive.file, buff)
        temp_archive_path.chmod(0o775)
        os.rename(temp_archive_path, archive_path)

    return success, msg

@app.get(plugins_endpoint + '/{name}', tags=['Plugins'])
def get_plugin(
    name: str,
    curr_user = (
        fastapi.Depends(manager) if private else None
    ),
) -> Any:
    """
    Download a plugin's archive file.
    """
    plugin = Plugin(name)
    if plugin.archive_path.exists():
        return FileResponse(plugin.archive_path, filename=f'{plugin.name}.tar.gz')
    return False, f"Archive for plugin '{plugin}' could not be found."


@app.get(plugins_endpoint + '/{name}/attributes', tags=['Plugins'])
def get_plugin_attributes(
    name: str,
    curr_user = (
        fastapi.Depends(manager) if private else None
    ),
) -> Dict[str, Any]:
    """
    Get a plugin's attributes.
    """
    return get_api_connector(PLUGINS_INSTANCE_KEYS).get_plugin_attributes(Plugin(name))


@app.get(plugins_endpoint, tags=['Plugins'])
def get_plugins(
    user_id: Optional[int] = None,
    search_term: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if private else None
    ),
) -> List[str]:
    """
    Get a list of plugins.

    Parameters
    ----------
    user_id: Optional[int], default None
        The `user_id` to search by.

    search_term : Optional[str], default None
        Search for plugins with this search term.

    Returns
    -------
    A list of strings.
    """
    return get_api_connector(
        PLUGINS_INSTANCE_KEYS
    ).get_plugins(user_id=user_id, search_term=search_term)


@app.delete(plugins_endpoint + '/{name}', tags=['Plugins'])
def delete_plugin(
    name: str,
    curr_user = (
        fastapi.Depends(manager) if private else None
    ),
) -> SuccessTuple:
    """
    Delete a plugin and its archive file from the repository.
    """
    get_tables()
    plugin = Plugin(name)
    plugin_user_id = get_api_connector(PLUGINS_INSTANCE_KEYS).get_plugin_user_id(plugin)
    if plugin_user_id is None:
        return False, f"Plugin '{plugin}' is not registered."

    if curr_user is not None:
        curr_user_id = get_api_connector(PLUGINS_INSTANCE_KEYS).get_user_id(curr_user)
        if plugin_user_id != curr_user_id:
            return False, f"User '{curr_user.username}' cannot delete plugin '{plugin}'."
    else:
        curr_user_id = -1
    plugin.user_id = curr_user_id

    _remove_success = plugin.remove_archive(debug=debug)
    if not _remove_success[0]:
        return _remove_success

    _delete_success = get_api_connector(PLUGINS_INSTANCE_KEYS).delete_plugin(plugin, debug=debug)
    if not _delete_success[0]:
        return _delete_success

    return True, f"Successfully deleted plugin '{plugin}'."
