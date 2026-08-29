#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Import the routes submodules to register them to the FastAPI app.
"""

from fnmatch import fnmatch
from importlib import import_module

from meerschaum.api import _include_dash, mcp_enabled, permissions_config

_allowed_route_group_patterns = (
    permissions_config.get('routes', {}).get('allowlist', None) or ['*']
)

#: Core route groups which `api:permissions:routes:allowlist` may restrict.
#: Each name matches a submodule of `meerschaum.api.routes`.
_core_route_groups = (
    'actions',
    'connectors',
    'index',
    'jobs',
    'misc',
    'pipes',
    'plugins',
    'tokens',
    'users',
    'version',
)


def route_group_is_allowed(route_group: str) -> bool:
    """
    Return whether a group of core routes may be registered,
    according to the patterns in `api:permissions:routes:allowlist`.
    """
    return any(
        fnmatch(route_group, pattern)
        for pattern in _allowed_route_group_patterns
    )


### The login routes are always registered:
### tokens issued by `/login` authenticate the routes added by API plugins,
### and the token manager's user loader is defined alongside them.
import meerschaum.api.routes._login

for _route_group in _core_route_groups:
    if route_group_is_allowed(_route_group):
        import_module(f'meerschaum.api.routes._{_route_group}')

### Registered before the API plugins run so that the built-in `/mcp` route
### takes precedence over the route registered by the third-party `mcp` plugin
### (which this endpoint supersedes).
if mcp_enabled and route_group_is_allowed('mcp'):
    import meerschaum.api.routes._mcp

if _include_dash and route_group_is_allowed('webterm'):
    import meerschaum.api.routes._webterm

### The web console's own pages are gated by the `dash` route group
### (see `meerschaum.api.dash.callbacks.dashboard`); pages registered by
### plugins via `@web_page` are always served when the Dash app is mounted.
