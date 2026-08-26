#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Meerschaum API backend. Start an API instance with `start api`.
"""
from __future__ import annotations

import os
import time
from collections import defaultdict
from fnmatch import fnmatch

import meerschaum as mrsm
import meerschaum.config.paths as paths
from meerschaum.utils.typing import Dict, Any, Optional, PipesDict
from meerschaum.config import get_config
from meerschaum._internal.static import STATIC_CONFIG, SERVER_ID
from meerschaum.utils.packages import attempt_import
from meerschaum.utils import get_pipes as _get_pipes
from meerschaum.plugins import _api_plugins
from meerschaum.utils.warnings import warn, dprint
from meerschaum.utils.threading import RLock
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
    importlib_metadata,
) = attempt_import(
    'fastapi',
    'aiofiles',
    'starlette.responses',
    'multipart',
    'packaging.version',
    'importlib_metadata',
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
uvicorn_config_path = paths.API_UVICORN_RESOURCES_PATH / SERVER_ID / 'config.json'

uvicorn_config = None
sys_config = get_config('api')
permissions_config = get_config('api', 'permissions')

def get_uvicorn_config() -> Dict[str, Any]:
    """Read the Uvicorn configuration JSON and return a dictionary."""
    global uvicorn_config
    import json
    _uvicorn_config = uvicorn_config
    with _locks['uvicorn_config']:
        if uvicorn_config is None:
            try:
                with open(uvicorn_config_path, 'r', encoding='utf-8') as f:
                    uvicorn_config = json.load(f)
                _uvicorn_config = uvicorn_config
            except Exception:
                import traceback
                traceback.print_exc()
                _uvicorn_config = sys_config.get('uvicorn', None)

            if _uvicorn_config is None:
                _uvicorn_config = {}
            if 'mrsm_instance' not in _uvicorn_config:
                _uvicorn_config['mrsm_instance'] = get_config('meerschaum', 'api_instance')
    return _uvicorn_config

debug = get_uvicorn_config().get('debug', False)
no_dash = get_uvicorn_config().get('no_dash', False)
no_webterm = get_uvicorn_config().get('no_webterm', False)
no_auth = get_uvicorn_config().get('no_auth', False)
private = get_uvicorn_config().get('private', False)
production = get_uvicorn_config().get('production', False)
_include_dash = (not no_dash)
_include_webterm = (not no_webterm) and _include_dash
docs_enabled = not production or sys_config.get('endpoints', {}).get('docs_in_production', True)

### Meerschaum's own MCP server at `/mcp`, authenticated per-tool by OAuth2 scope.
mcp_enabled = sys_config.get('mcp', {}).get('enabled', True)

### Plotly Dash's MCP server, which exposes the web console's layout and callbacks.
### Off by default: it is served from inside the Dash WSGI app, where FastAPI's
### token auth does not apply, so every Dash callback it advertises is reachable
### without credentials.
dash_mcp_enabled = sys_config.get('dash', {}).get('mcp', {}).get('enabled', False)
webterm_port = (
    get_uvicorn_config().get('webterm_port', None)
    or mrsm.get_config('api', 'webterm', 'port')
)

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
        if fnmatch(str(instance_keys), allowed_keys_pattern):
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
                instance_connector = parse_instance_keys(instance_keys, debug=debug)
                instance_connector._cache_connector = get_cache_connector()
                _instance_connectors[instance_keys] = instance_connector
            except Exception:
                is_valid_connector = False

            if not is_valid_connector:
                raise fastapi.HTTPException(
                    status_code=422,
                    detail="Invalid instance keys.",
                )
    return _instance_connectors[instance_keys]


cache_connector = None
_cache_connector_probe_failed_at = None
CACHE_CONNECTOR_PROBE_RETRY_SECONDS: float = 60.0
def get_cache_connector(connector_keys: Optional[str] = None):
    """Return the `valkey` connector if running in production."""
    global cache_connector, _cache_connector_probe_failed_at
    if cache_connector is not None:
        return cache_connector

    if not production:
        return None

    enable_valkey_cache = get_config('system', 'experimental', 'valkey_session_cache')
    if not enable_valkey_cache:
        return None

    connector_keys = connector_keys or get_config(
        'api', 'cache', 'connector',
        warn=False,
    )
    if connector_keys is None:
        return None

    if not connector_keys.startswith('valkey'):
        warn(f"Invalid cache connector '{connector_keys}'.")
        return None

    with _locks['cache_connector']:
        if cache_connector is not None:
            return cache_connector

        ### After a failed probe, wait before probing again
        ### so that a dead cache server is not hammered on every request.
        if (
            _cache_connector_probe_failed_at is not None
            and (time.monotonic() - _cache_connector_probe_failed_at)
                < CACHE_CONNECTOR_PROBE_RETRY_SECONDS
        ):
            return None

        _cache_connector = parse_instance_keys(connector_keys)

        ### Probe the connector before memoizing it: a misconfigured or unreachable
        ### cache must degrade to the documented no-cache path (`None`), not return
        ### a connector whose every operation fails at the call site.
        try:
            probe_success = bool(_cache_connector.test_connection())
        except Exception:
            probe_success = False

        if not probe_success:
            _cache_connector_probe_failed_at = time.monotonic()
            warn(
                f"Cache connector '{connector_keys}' is unreachable. "
                "Continuing without a cache; will probe again in "
                f"{CACHE_CONNECTOR_PROBE_RETRY_SECONDS} seconds.",
                stack=False,
            )
            return None

        _cache_connector_probe_failed_at = None
        cache_connector = _cache_connector

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
            pipes = _get_pipes(
                mrsm_instance=instance_keys,
                cache=(get_config('api', 'cache', 'pipes', warn=False) or False),
                cache_connector_keys=get_cache_connector(),
            )
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
    if connector_keys == 'mrsm':
        raise fastapi.HTTPException(
            status_code=403,
            detail="Unable to serve any pipes with connector keys `mrsm` over the API.",
        )

    pipes_dict = pipes(instance_keys)
    if (
        not refresh
        and connector_keys in pipes_dict
        and metric_key in pipes_dict[connector_keys]
        and location_key in pipes_dict[connector_keys][metric_key]
    ):
        return pipes_dict[connector_keys][metric_key][location_key]

    pipe = mrsm.Pipe(
        connector_keys,
        metric_key,
        location_key,
        mrsm_instance=instance_keys,
        cache=(get_config('api', 'cache', 'pipes', warn=False) or False),
        cache_connector_keys=get_cache_connector(),
    )
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
        {
            'name': 'MCP',
            'description': (
                "Model Context Protocol server (Streamable HTTP transport). "
                "Each tool requires the same scope as its equivalent REST route."
            ),
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

app.mount('/static', fastapi_staticfiles.StaticFiles(directory=paths.API_STATIC_PATH.as_posix()), name='static')

_custom_kwargs = {'mrsm_instance'}

def __getattr__(name: str):
    ucf = get_uvicorn_config()
    if name in ucf:
        return ucf[name]
    if name in globals():
        return globals()[name]
    raise AttributeError(f"Could not import '{name}'.")

### Import everything else within the API.
from meerschaum.api._oauth2 import manager, ScopedAuth
import meerschaum.api.routes as routes
import meerschaum.api._events
import meerschaum.api._websockets

### Skip importing the dash if `--no-dash` is provided.
if _include_dash:
    import meerschaum.api.dash

### The `mcp` plugin predates the built-in `/mcp` endpoint and registers a route
### at the same path. Routes registered above win, so the plugin's route is
### unreachable rather than conflicting — but say so plainly instead of leaving
### two implementations installed.
if mcp_enabled:
    from meerschaum.mcp import has_mcp_plugin
    if has_mcp_plugin():
        warn(
            "The 'mcp' plugin is superseded by Meerschaum's built-in MCP endpoint, "
            "which is now serving '/mcp'. The plugin's route is unreachable; "
            "uninstall it with `mrsm uninstall plugin mcp`.",
            stack=False,
        )

### Execute the API plugins functions.
for module_name, functions_list in _api_plugins.items():
    plugin_name = (
        module_name.split('.')[1]
        if module_name and module_name.startswith('plugins.')
        else module_name
    )
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
