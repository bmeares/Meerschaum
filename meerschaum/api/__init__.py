#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Meerschaum API backend. Start an API instance with `api start`.
"""
from __future__ import annotations
import pathlib, os
from meerschaum.utils.typing import Dict, Any, Optional

from meerschaum import __version__ as version
__version__ = version

from meerschaum.config import get_config
from meerschaum.config.static import _static_config
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.get_pipes import get_pipes as _get_pipes
from meerschaum.config._paths import API_UVICORN_CONFIG_PATH
endpoints = _static_config()['api']['endpoints']
aiofiles = attempt_import('aiofiles', lazy=False)
fastapi = attempt_import('fastapi', lazy=False)
starlette_reponses = attempt_import('starlette.responses', warn=False, lazy=False)
python_multipart = attempt_import('multipart', lazy=False)
packaging_version = attempt_import('packaging.version')
from meerschaum.api._chain import check_allow_chaining, DISALLOW_CHAINING_MESSAGE

uvicorn_config = None
sys_config = get_config('system', 'api')
permissions_config = get_config('system', 'api', 'permissions')

def get_uvicorn_config() -> Dict[str, Any]:
    """
    Read the Uvicorn configuration JSON and return a dictionary.
    """
    global uvicorn_config
    import json
    if uvicorn_config is None:
        try:
            with open(API_UVICORN_CONFIG_PATH, 'r') as f:
                uvicorn_config = json.load(f)
        except Exception as e:
            uvicorn_config = sys_config.get('uvicorn', None)

        if uvicorn_config is None:
            uvicorn_config = dict()

        ### Default: main SQL connector
        if 'mrsm_instance' not in uvicorn_config:
            uvicorn_config['mrsm_instance'] = get_config('meerschaum', 'api_instance', patch=True)
    return uvicorn_config

debug = get_uvicorn_config().get('debug', False) if API_UVICORN_CONFIG_PATH.exists() else False
no_dash = get_uvicorn_config().get('no_dash', False)
### NOTE: Disable dash unless version is at least 0.3.0.
_include_dash = (
    (not no_dash)
    and (packaging_version.parse(version) >= packaging_version.parse('0.3.0.rc1'))
)

connector = None
def get_api_connector(instance_keys : Optional[str] = None):
    """
    Create the instance connector.
    """
    from meerschaum.utils.debug import dprint
    global connector
    if connector is None:
        if instance_keys is None:
            instance_keys = get_uvicorn_config().get('mrsm_instance', None)

        from meerschaum.connectors.parse import parse_instance_keys
        connector = parse_instance_keys(instance_keys, debug=debug)
    if debug:
        dprint(f"API instance connector: {connector}")
    return connector

database = None
def get_database(instance_keys : str = None):
    """
    Return a databases object.
    NOTE: Not used!
    """
    global database
    if database is None:
        database = get_api_connector(instance_keys).db
    return database

_pipes = None
def pipes(refresh=False):
    """
    Return the pipes dictionary.
    """
    global _pipes
    if _pipes is None or refresh:
        _pipes = _get_pipes(mrsm_instance=get_api_connector())
    return _pipes

def get_pipe(connector_keys, metric_key, location_key, refresh=False):
    """
    Index the pipes dictionary or create a new Pipe object.
    """
    from meerschaum.utils.misc import is_pipe_registered
    from meerschaum import Pipe
    if location_key in ('[None]', 'None', 'null'):
        location_key = None
    p = Pipe(connector_keys, metric_key, location_key, mrsm_instance=get_api_connector())
    if is_pipe_registered(p, pipes()):
        return pipes(refresh=refresh)[connector_keys][metric_key][location_key]
    return p

app = fastapi.FastAPI(title='Meerschaum API')

(
    fastapi_responses,
    fastapi_templating,
    fastapi_staticfiles,
) = attempt_import(
    'fastapi.responses',
    'fastapi.templating',
    'fastapi.staticfiles',
)
#  jinja2 = attempt_import('jinja2')

HTMLResponse = fastapi_responses.HTMLResponse
Request = fastapi.Request

from meerschaum.config._paths import API_RESOURCES_PATH, API_STATIC_PATH, API_TEMPLATES_PATH
app.mount('/static', fastapi_staticfiles.StaticFiles(directory=str(API_STATIC_PATH)), name='static')
#  templates = fastapi_templating.Jinja2Templates(directory=str(API_TEMPLATES_PATH))


### Import everything else within the API.
from meerschaum.api._oauth2 import manager
import meerschaum.api.routes as routes
import meerschaum.api._events
import meerschaum.api._websockets

if _include_dash:
    import meerschaum.api.dash
