#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Routes for managing plugins
"""

from meerschaum.api import fastapi, fast_api, endpoints, get_connector, pipes, get_pipe, get_pipes_sql
from meerschaum.api.tables import get_tables
from fastapi import FastAPI, File, UploadFile
from meerschaum.utils.misc import attempt_import
from starlette.responses import FileResponse

sqlalchemy = attempt_import('sqlalchemy')
plugins_endpoint = endpoints['mrsm'] + '/plugins'

@fast_api.post(plugins_endpoint + '/{name}')
def register_plugin(
        name : str,
        version : str = None,
        attributes : str = None,
        archive : UploadFile = File(...)
    ) -> tuple:
    """
    Register a plugin and save its archive file
    """
    import json, shutil, pathlib, os
    from meerschaum.config._paths import PLUGINS_RESOURCES_PATH, PLUGINS_ARCHIVES_RESOURCES_PATH
    from meerschaum import Plugin
    get_tables()
    if attributes is None: attributes = json.dumps(dict())
    attributes = json.loads(attributes)
    
    plugin = Plugin(name, version=version, attributes=attributes)

    success, msg = get_connector().register_plugin(plugin, make_archive=False, debug=True)

    ### TODO delete and install new version of plugin on success
    if success:
        archive_path = plugin.archive_path
        temp_archive_path = pathlib.Path(str(archive_path) + '.tmp')
        with temp_archive_path.open('wb') as buff:
            shutil.copyfileobj(archive.file, buff)
        temp_archive_path.chmod(0o775)
        os.rename(temp_archive_path, archive_path)

    return success, msg

@fast_api.get(plugins_endpoint + '/{name}')
def get_plugin(
        name : str
    ) -> FileResponse:
    """
    Download a plugin's archive file
    """
    from meerschaum import Plugin
    plugin = Plugin(name)
    if plugin.archive_path.exists():
        return FileResponse(plugin.archive_path, filename=f'{plugin.name}.tar.gz')
    return False, f"Archive for plugin '{plugin}' could not be found"
