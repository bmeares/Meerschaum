#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Meerschaum API backend. Start an API instance with `start api`.
"""
from __future__ import annotations

import os
from collections import defaultdict
from fnmatch import fnmatch

import meerschaum as mrsm
from meerschaum.utils.typing import Dict, Any, Optional, PipesDict
from meerschaum.config import get_config
from meerschaum.config.static import STATIC_CONFIG, SERVER_ID
from meerschaum.utils.packages import attempt_import
from meerschaum.utils import get_pipes as _get_pipes
from meerschaum.config._paths import API_UVICORN_CONFIG_PATH, API_UVICORN_RESOURCES_PATH
from meerschaum.plugins import _api_plugins
from meerschaum.utils.warnings import warn, dprint
from meerschaum.utils.threading import RLock
from meerschaum.utils.misc import is_pipe_registered
from meerschaum.connectors.parse import parse_instance_keys

from meerschaum import __version__ as version
__version__ = version
__doc__ = """The Meerschaum Web API lets you manage your pipes over the Internet."""


_locks = defaultdict(lambda: RLock())

### Skip verifying packages in the docker image.
CHECK_UPDATE = os.environ.get(STATIC_CONFIG['environment']['runtime'], None) != 'docker'

endpoints = STATIC_CONFIG['api']['endpoints']

uv = attempt_import('uv', lazy=False, check_update=CHECK_UPDATE)
(
    fastapi,
    aiofiles,
    starlette_responses,
    multipart,
    packaging_version,
) = attempt_import(
    'fastapi',
    'aiofiles',
    'starlette.responses',
    'multipart',
    'packaging.version',
    lazy=False,
    check_update=CHECK_UPDATE,
)
(
    typing_extensions,
    uvicorn_workers,
) = attempt_import(
    'typing_extensions',
    'uvicorn.workers',
    lazy=False,
    check_update=CHECK_UPDATE,
    venv=None,
)
from meerschaum.api._chain import check_allow_chaining, DISALLOW_CHAINING_MESSAGE
from meerschaum.api._exceptions import APIPermissionError
uvicorn_config_path = API_UVICORN_RESOURCES_PATH / SERVER_ID / 'config.json'

uvicorn_config = None
sys_config = get_config('system', 'api')
permissions_config = get_config('system', 'api', 'permissions')

def get_uvicorn_config() -> Dict[str, Any]:
    """Read the Uvicorn configuration JSON and return a dictionary."""
    global uvicorn_config
    import json
    runtime = os.environ.get(STATIC_CONFIG['environment']['runtime'], None)
    if runtime == 'api':
        return get_config('system', 'api', 'uvicorn')
    _uvicorn_config = uvicorn_config
    with _locks['uvicorn_config']:
        if uvicorn_config is None:
            try:
                with open(uvicorn_config_path, 'r', encoding='utf-8') as f:
                    uvicorn_config = json.load(f)
                _uvicorn_config = uvicorn_config
            except Exception:
                _uvicorn_config = sys_config.get('uvicorn', None)

            if _uvicorn_config is None:
                _uvicorn_config = {}
            if 'mrsm_instance' not in _uvicorn_config:
                _uvicorn_config['mrsm_instance'] = get_config('meerschaum', 'api_instance')
    return _uvicorn_config

debug = get_uvicorn_config().get('debug', False)
no_dash = get_uvicorn_config().get('no_dash', False)
no_auth = get_uvicorn_config().get('no_auth', False)
private = get_uvicorn_config().get('private', False)
production = get_uvicorn_config().get('production', False)
_include_dash = (not no_dash)
docs_enabled = not production or sys_config.get('endpoints', {}).get('docs_in_production', True)

default_instance_keys = None
_instance_connectors = defaultdict(lambda: None)
def get_api_connector(instance_keys: Optional[str] = None):
    """Create the instance connectors."""
    global default_instance_keys
    if instance_keys is None:
        if default_instance_keys is None:
            default_instance_keys = get_uvicorn_config().get('mrsm_instance', None)
        instance_keys = default_instance_keys

    allow_multiple_instances = permissions_config.get(
        'instances', {}
    ).get('allow_multiple_instances', False)
    if not allow_multiple_instances and instance_keys != default_instance_keys:
        raise APIPermissionError(
            "This API instance does not allow for accessing additional instances."
        )

    allowed_instance_keys = permissions_config.get(
        'instances', {}
    ).get(
        'allowed_instance_keys',
        ['*']
    )
    found_match: bool = False
    for allowed_keys_pattern in allowed_instance_keys:
        if fnmatch(instance_keys, allowed_keys_pattern):
            found_match = True
            break
    if not found_match:
        raise APIPermissionError(
            f"Instance keys '{instance_keys}' does not match the allowed instances patterns."
        )

    with _locks[f'instance-{instance_keys}']:
        if _instance_connectors[instance_keys] is None:
            try:
                is_valid_connector = True
                _instance_connectors[instance_keys] = parse_instance_keys(instance_keys, debug=debug)
            except Exception:
                is_valid_connector = False

            if not is_valid_connector:
                raise fastapi.HTTPException(
                    status_code=422,
                    detail="Invalid instance keys.",
                )
    return _instance_connectors[instance_keys]


cache_connector = None
def get_cache_connector(connector_keys: Optional[str] = None):
    """Return the `valkey` connector if running in production."""
    global cache_connector
    if cache_connector is not None:
        return cache_connector

    if not production:
        return None

    enable_valkey_cache = get_config('system', 'experimental', 'valkey_session_cache')
    if not enable_valkey_cache:
        return None

    connector_keys = connector_keys or get_config(
        'system', 'api', 'cache', 'connector',
        warn=False,
    )
    if connector_keys is None:
        return None

    if not connector_keys.startswith('valkey'):
        warn(f"Invalid cache connector '{connector_keys}'.")
        return None

    if cache_connector is None:
        cache_connector = parse_instance_keys(connector_keys)

    if debug:
        dprint(f"Cache connector: {cache_connector}")

    return cache_connector


_instance_pipes = defaultdict(lambda: None)
def pipes(instance_keys: Optional[str] = None, refresh: bool = False) -> PipesDict:
    """
    Manage the global pipes dictionaries.
    """
    instance_keys = str(get_api_connector(instance_keys))
    with _locks['pipes-' + instance_keys]:
        pipes = _instance_pipes[instance_keys]
        if pipes is None or refresh:
            pipes = _get_pipes(mrsm_instance=instance_keys)
            _instance_pipes[instance_keys] = pipes
    return pipes


def get_pipe(
    connector_keys: str,
    metric_key: str,
    location_key: Optional[str],
    instance_keys: Optional[str] = None,
    refresh: bool = False
) -> mrsm.Pipe:
    """Index the pipes dictionary or create a new Pipe object."""
    if location_key in ('[None]', 'None', 'null'):
        location_key = None
    instance_keys = str(get_api_connector(instance_keys))
    pipe = mrsm.Pipe(connector_keys, metric_key, location_key, mrsm_instance=instance_keys)
    if is_pipe_registered(pipe, pipes(instance_keys)):
        return pipes(instance_keys, refresh=refresh)[connector_keys][metric_key][location_key]
    return pipe


app = fastapi.FastAPI(
    title = 'Meerschaum API',
    description=__doc__,
    version=__version__,
    contact={
        'name': 'Bennett Meares',
        'url': 'https://meerschaum.io',
    },
    license_info={
        'name': 'Apache 2.0',
        'url': 'https://www.apache.org/licenses/LICENSE-2.0.html',
    },
    docs_url=(None if not docs_enabled else endpoints['docs']),
    redoc_url=(None if not docs_enabled else endpoints['redoc']),
    openapi_url=endpoints['openapi'],
    open_api_tags=[
        {
            'name': 'Pipes',
            'description': 'Access pipes by indexing their keys.',
        },
        {
            'name': 'Actions',
            'description': 'Perform actions via the API.',
        },
        {
            'name': 'Connectors',
            'description': 'Get information about the registered connectors.',
        },
        {
            'name': 'Users',
            'description': 'Access, register, and delete users.',
        },
        {
            'name': 'Plugins',
            'description': 'Access, register, and delete plugins.',
        },
        {
            'name': 'Misc',
            'description': 'Miscellaneous endpoints.',
        },
        {
            'name': 'Version',
            'description': 'Version information.',
        },
    ],
)

(
    fastapi_responses,
    fastapi_templating,
    fastapi_staticfiles,
) = attempt_import(
    'fastapi.responses',
    'fastapi.templating',
    'fastapi.staticfiles',
    check_update=CHECK_UPDATE,
)

HTMLResponse = fastapi_responses.HTMLResponse
Request = fastapi.Request

from meerschaum.config.paths import API_RESOURCES_PATH, API_STATIC_PATH, API_TEMPLATES_PATH
app.mount('/static', fastapi_staticfiles.StaticFiles(directory=str(API_STATIC_PATH)), name='static')

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
