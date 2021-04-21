#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Routes for managing plugins
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, List, SuccessTuple, Union

from meerschaum.api import (
    fastapi,
    app,
    endpoints,
    get_api_connector,
    pipes,
    get_pipe,
    manager,
    debug,
)
from meerschaum.api.tables import get_tables
from fastapi import FastAPI, File, UploadFile
from meerschaum.utils.packages import attempt_import
import meerschaum._internal.User
starlette_responses = attempt_import('starlette.responses', warn=False)
FileResponse = starlette_responses.FileResponse

sqlalchemy = attempt_import('sqlalchemy')
plugins_endpoint = endpoints['plugins']

@app.post(plugins_endpoint + '/{name}')
def register_plugin(
        name : str,
        version : str = None,
        attributes : str = None,
        archive : UploadFile = File(...),
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ) -> SuccessTuple:
    """
    Register a plugin and save its archive file.
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

    import json, shutil, pathlib, os
    from meerschaum.config._paths import PLUGINS_RESOURCES_PATH, PLUGINS_ARCHIVES_RESOURCES_PATH
    from meerschaum._internal.Plugin import Plugin
    get_tables()
    if attributes is None:
        attributes = json.dumps({})
    attributes = json.loads(attributes)

    plugin = Plugin(name, version=version, attributes=attributes)
    plugin_user_id = get_api_connector().get_plugin_user_id(plugin)
    if plugin_user_id is not None and plugin_user_id != get_api_connector().get_user_id(curr_user):
        return False, f"User '{curr_user.username}' cannot edit plugin '{plugin}'."
    plugin.user_id = get_api_connector().get_user_id(curr_user)

    success, msg = get_api_connector().register_plugin(plugin, make_archive=False, debug=debug)

    ### TODO delete and install new version of plugin on success
    if success:
        archive_path = plugin.archive_path
        temp_archive_path = pathlib.Path(str(archive_path) + '.tmp')
        with temp_archive_path.open('wb') as buff:
            shutil.copyfileobj(archive.file, buff)
        temp_archive_path.chmod(0o775)
        os.rename(temp_archive_path, archive_path)

    return success, msg

@app.get(plugins_endpoint + '/{name}')
def get_plugin(
        name : str
    ) -> Union[FileResponse, SuccessTuple]:
    """
    Download a plugin's archive file
    """
    from meerschaum._internal.Plugin import Plugin
    plugin = Plugin(name)
    if plugin.archive_path.exists():
        return FileResponse(plugin.archive_path, filename=f'{plugin.name}.tar.gz')
    return False, f"Archive for plugin '{plugin}' could not be found."

@app.get(plugins_endpoint + '/{name}/attributes')
def get_plugin_attributes(
        name : str
    ) -> dict:
    """
    Download a plugin's archive file
    """
    from meerschaum._internal.Plugin import Plugin
    return get_api_connector().get_plugin_attributes(Plugin(name))

@app.get(plugins_endpoint)
def get_plugins(
        user_id : Optional[int] = None,
        search_term : Optional[str] = None,
    ) -> List[str]:
    """
    Return a list of registered plugins
    """
    return get_api_connector().get_plugins(user_id=user_id, search_term=search_term)
