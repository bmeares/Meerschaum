#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Meerschaum API backend. Start an API instance with `api start`.
"""

__version__ = "0.0.14"
from meerschaum.config import get_config
from meerschaum.config.static import _static_config
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.get_pipes import get_pipes as get_pipes_sql
import pathlib, os
endpoints = _static_config()['api']['endpoints']
aiofiles = attempt_import('aiofiles', lazy=False)
fastapi = attempt_import('fastapi', lazy=False)
starlette_reponses = attempt_import('starlette.responses', warn=False, lazy=False)
python_multipart = attempt_import('multipart', lazy=False)

fastapi_login = attempt_import('fastapi_login')
LoginManager = fastapi_login.LoginManager
def generate_secret_key():
    """
    Read or generate the keyfile
    """
    from meerschaum.config._paths import API_SECRET_KEY_PATH
    if not API_SECRET_KEY_PATH.exists():
        secret_key = os.urandom(24).hex()
        with open(API_SECRET_KEY_PATH, 'w') as f:
            f.write(secret_key)
    else:
        with open(API_SECRET_KEY_PATH, 'r') as f:
            secret_key = f.read()

    return secret_key

SECRET = generate_secret_key()
manager = LoginManager(SECRET, tokenUrl=endpoints['login'])

uvicorn_config = None
def get_uvicorn_config() -> dict:
    global uvicorn_config
    from meerschaum.config._paths import API_UVICORN_CONFIG_PATH
    from meerschaum.utils.yaml import yaml
    if uvicorn_config is None:
        try:
            with open(API_UVICORN_CONFIG_PATH, 'r') as f:
                uvicorn_config = yaml.load(f)
        except:
            uvicorn_config = dict()

        if uvicorn_config is None: uvicorn_config = dict()

        ### Default: main SQL connector
        if 'mrsm_instance' not in uvicorn_config:
            uvicorn_config['mrsm_instance'] = get_config('meerschaum', 'api_instance', patch=True)
    return uvicorn_config

debug = get_uvicorn_config().get('debug', False)

connector = None
def get_connector(instance_keys : str = None):
    """
    Create the connector
    """
    from meerschaum.utils.debug import dprint
    global connector
    if connector is None:
        if instance_keys is None:
            instance_keys = get_uvicorn_config()['mrsm_instance']

        from meerschaum.connectors.parse import parse_instance_keys
        connector = parse_instance_keys(instance_keys, debug=debug)
    if debug: dprint(f"API instance connector: {connector}")
    return connector

database = None
def get_database(instance_keys : str = None):
    #  if instance_keys is None
    global database
    if database is None:
        database = get_connector(instance_keys).db
    return database

_pipes = None
def pipes(refresh=False):
    global _pipes
    if _pipes is None or refresh:
        _pipes = get_pipes_sql(mrsm_instance=get_connector())
    return _pipes

def get_pipe(connector_keys, metric_key, location_key, refresh=False):
    """
    Index the pipes dictionary or create a new Pipe object
    """
    from meerschaum.utils.misc import is_pipe_registered
    from meerschaum import Pipe
    if location_key in ('[None]', 'None', 'null'): location_key = None
    p = Pipe(connector_keys, metric_key, location_key, mrsm_instance=get_connector())
    if is_pipe_registered(p, pipes()):
        return pipes(refresh=refresh)[connector_keys][metric_key][location_key]
    return p

sys_config = get_config('system', 'api')
app = fastapi.FastAPI(title='Meerschaum API')

(
    fastapi_responses,
    fastapi_templating,
    fastapi_staticfiles,
    fastapi_middleware_wsgi
) = attempt_import(
    'fastapi.responses',
    'fastapi.templating',
    'fastapi.staticfiles',
    'fastapi.middleware.wsgi'
)
jinja2 = attempt_import('jinja2')

### TODO Add Django integration


HTMLResponse = fastapi_responses.HTMLResponse
Request = fastapi.Request

from meerschaum.config._paths import API_RESOURCES_PATH, API_STATIC_PATH, API_TEMPLATES_PATH
app.mount('/static', fastapi_staticfiles.StaticFiles(directory=str(API_STATIC_PATH)), name='static')
templates = fastapi_templating.Jinja2Templates(directory=str(API_TEMPLATES_PATH))


### import WebAPI routes
import meerschaum.api.routes as routes
import meerschaum.api._events
