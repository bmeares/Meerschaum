#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Meerschaum API backend. Start an API instance with `start api`.
"""
from __future__ import annotations
import pathlib, os
from meerschaum.utils.typing import Dict, Any, Optional

from meerschaum import __version__ as version
__version__ = version
__doc__ = """
The Meerschaum Web API lets you access and control your data over the Internet.
"""

from meerschaum.config import get_config
from meerschaum.config.static import _static_config
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.get_pipes import get_pipes as _get_pipes
from meerschaum.config._paths import API_UVICORN_CONFIG_PATH, API_UVICORN_RESOURCES_PATH
from meerschaum.plugins import _api_plugins
from meerschaum.utils.warnings import warn
endpoints = _static_config()['api']['endpoints']
aiofiles = attempt_import('aiofiles', lazy=False)
fastapi = attempt_import('fastapi', lazy=False)
starlette_reponses = attempt_import('starlette.responses', warn=False, lazy=False)
python_multipart = attempt_import('multipart', lazy=False)
packaging_version = attempt_import('packaging.version')
from meerschaum.api._chain import check_allow_chaining, DISALLOW_CHAINING_MESSAGE
from meerschaum.config.static import SERVER_ID
uvicorn_config_path = API_UVICORN_RESOURCES_PATH / SERVER_ID / '.config.json'

uvicorn_workers = attempt_import('uvicorn.workers', venv=None)
uvicorn_config = None
sys_config = get_config('system', 'api')
permissions_config = get_config('system', 'api', 'permissions')

def get_uvicorn_config() -> Dict[str, Any]:
    """Read the Uvicorn configuration JSON and return a dictionary."""
    global uvicorn_config
    import json
    runtime = os.environ.get(_static_config()['environment']['runtime'], None)
    if runtime == 'api':
        return get_config('system', 'api', 'uvicorn')
    _uvicorn_config = uvicorn_config
    if uvicorn_config is None:
        try:
            with open(uvicorn_config_path, 'r') as f:
                uvicorn_config = json.load(f)
            _uvicorn_config = uvicorn_config
        except Exception as e:
            _uvicorn_config = sys_config.get('uvicorn', None)

        if _uvicorn_config is None:
            _uvicorn_config = dict()

        ### Default: main SQL connector
        if 'mrsm_instance' not in _uvicorn_config:
            _uvicorn_config['mrsm_instance'] = get_config('meerschaum', 'api_instance', patch=True)
    return _uvicorn_config

debug = get_uvicorn_config().get('debug', False)
no_dash = get_uvicorn_config().get('no_dash', False)
no_auth = get_uvicorn_config().get('no_auth', False)
private = get_uvicorn_config().get('private', False)
_include_dash = (not no_dash)

connector = None
def get_api_connector(instance_keys : Optional[str] = None):
    """Create the instance connector.

    Parameters
    ----------
    instance_keys : Optional[str] :
         (Default value = None)

    Returns
    -------

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

    Parameters
    ----------
    instance_keys : str :
         (Default value = None)

    Returns
    -------
    type
        NOTE: Not used!

    """
    global database
    if database is None:
        database = get_api_connector(instance_keys).db
    return database

_pipes = None
def pipes(refresh=False):
    """

    Parameters
    ----------
    refresh :
         (Default value = False)

    Returns
    -------
    type
        

    """
    global _pipes
    if _pipes is None or refresh:
        _pipes = _get_pipes(mrsm_instance=get_api_connector())
    return _pipes

def get_pipe(connector_keys, metric_key, location_key, refresh=False):
    """Index the pipes dictionary or create a new Pipe object.

    Parameters
    ----------
    connector_keys :
        
    metric_key :
        
    location_key :
        
    refresh :
         (Default value = False)

    Returns
    -------

    """
    from meerschaum.utils.misc import is_pipe_registered
    from meerschaum import Pipe
    if location_key in ('[None]', 'None', 'null'):
        location_key = None
    p = Pipe(connector_keys, metric_key, location_key, mrsm_instance=get_api_connector())
    if is_pipe_registered(p, pipes()):
        return pipes(refresh=refresh)[connector_keys][metric_key][location_key]
    return p

app = fastapi.FastAPI(
    title = 'Meerschaum API',
    description = __doc__,
    version = __version__,
    contact = {
        'name': 'Bennett Meares',
        'url': 'https://meerschaum.io',
    },
    license_info = {
        'name': 'Apache 2.0',
        'url': 'https://www.apache.org/licenses/LICENSE-2.0.html',
    },
    open_api_tags = [{
            'name': 'Pipes',
            'description': 'Access pipes by indexing their keys.',
        }, {
            'name': 'Actions',
            'description': 'Perform actions via the API.',
        }, {
            'name': 'Connectors',
            'description': 'Get information about the registered connectors.'
        }, {
            'name': 'Users',
            'description': 'Access, register, and delete users.',
        }, {
            'name': 'Plugins',
            'description': 'Access, register, and delete plugins.',
        }, {
            'name': 'Misc',
            'description': 'Miscellaneous endpoints.',
        }, {
            'name': 'Version',
            'description': 'Version information.'
    }],
)

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

_custom_kwargs = {'mrsm_instance'}

def __getattr__(name: str):
    ucf = get_uvicorn_config()
    if name in ucf:
        return ucf[name]
    if name in globals():
        return globals()[name]
    raise AttributeError(f"Could not import '{name}'.")

### Import everything else within the API.
from meerschaum.api._oauth2 import manager
import meerschaum.api.routes as routes
import meerschaum.api._events
import meerschaum.api._websockets

### Skip importing the dash if `--no-dash` is provided.
if _include_dash:
    import meerschaum.api.dash

### Execute the API plugins functions.
import meerschaum as mrsm
for module_name, functions_list in _api_plugins.items():
    plugin_name = module_name.split('.')[-1] if module_name.startswith('plugins.') else None
    plugin = mrsm.Plugin(plugin_name) if plugin_name else None

    if plugin is not None:
        plugin.activate_venv(debug=debug)

    for function in functions_list:
        try:
            function(app)
        except Exception as e:
            import traceback
            traceback.print_exc()
            warn(
                f"Failed to load API plugin '{plugin}' "
                + f"when executing function '{function.__name__}' with exception:\n{e}",
                stack=False,
            )

