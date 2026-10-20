#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Python API host (v5 design §7.3, D17).

The Rust server serves Meerschaum's own routes, console and MCP server. What only Python can
run is hosted here, in one FastAPI app served on a Unix socket and reverse-proxied by the Rust
server for every request it does not match itself:

- `@api_plugin` functions, run against the app in plugin order (routes, middleware, static
  mounts, background threads);
- the web console: the Plotly Dash app of v4 (`meerschaum.api.dash`, design D21), mounted at `/dash`
  with the plugins' `@dash_plugin` functions and `@web_page` pages at their v4 paths
  (`meerschaum.api.host.dash`);
- MCP tools, resources and prompts registered by plugins with `meerschaum.mcp.tool(...)`,
  federated into the Rust MCP server through two internal routes (`/_mrsm/host/registry`,
  `/_mrsm/host/mcp`) that require a token signed with the API secret.

`meerschaum.api` keeps its surface (`app`, `endpoints`, `get_api_connector`, `ScopedAuth`,
`CurrentScopes`, `_oauth2.SECRET`, `_tokens.get_token_from_authorization`, `get_pipe`, ...): in host
mode (`MRSM_API_HOST=1`) `meerschaum.api.app` *is* the host's app, without the core routes.

Usage:

    python -m meerschaum.api.host --socket /path/to/api_host.sock
"""

from __future__ import annotations

import os

HOST_ENV: str = 'MRSM_API_HOST'
HOST_CONFIG_ENV: str = 'MRSM_API_HOST_CONFIG'
INTERNAL_PREFIX: str = '/_mrsm/'
REGISTRY_PATH: str = '/_mrsm/host/registry'
STATUS_PATH: str = '/_mrsm/host/status'
MCP_PATH: str = '/_mrsm/host/mcp'
TOKEN_HEADER: str = 'x-mrsm-host-token'
TOKEN_CLAIM: str = 'mrsm_host'
PROXY_TOKEN_ENV: str = 'MRSM_API_HOST_PROXY_TOKEN'
PROXY_TOKEN_HEADER: str = 'x-mrsm-host-proxy-token'

__all__ = (
    'build_app',
    'run',
    'run_tcp',
    'is_host_mode',
    'get_host_config',
    'dash_enabled',
    'console_available',
    'HOST_ENV',
    'HOST_CONFIG_ENV',
)


def is_host_mode() -> bool:
    """
    Return whether this process is the API host (`MRSM_API_HOST` is set).
    """
    return os.environ.get(HOST_ENV, '').lower() not in ('', '0', 'false')


def get_host_config() -> dict:
    """
    Return the host's configuration (`MRSM_API_HOST_CONFIG`, a JSON object the Rust server fills
    from its own flags: `mrsm_instance`, `no_auth`, `production`, `private`, `debug`, `no_dash`,
    `no_webterm`, and `webterm`, whether the Rust server serves the terminal; the host never
    serves the terminal, the Rust server does).
    """
    import json
    try:
        config = json.loads(os.environ.get(HOST_CONFIG_ENV, '') or '{}')
    except ValueError:
        return {}
    return config if isinstance(config, dict) else {}


def dash_enabled() -> bool:
    """
    Return whether the host serves the web console and the plugin pages in it.
    As in v4, `--no-dash` serves none of them; the default is to serve them.
    """
    return not get_host_config().get('no_dash', False)


def console_available() -> bool:
    """
    Return whether the web console can run in this interpreter: it is enabled (`--no-dash` not
    given) and its packages (`dash`, `dash-bootstrap-components`, `dash-ace`, `a2wsgi`) are
    installed. This only looks for the packages (no import), so the Rust server can ask cheaply
    before it starts the host (design decision 32); `host.dash.console_status()` is what actually
    mounted.
    """
    if not dash_enabled():
        return False
    from meerschaum.api.dash import console_available as _console_available
    return _console_available()


def build_app(load_plugins: bool = True, debug: bool = False):
    """
    Build the host's FastAPI app: run the API plugins, mount the Dash console (the one app plugin pages register into), and add
    the internal MCP routes. Requires host mode (`MRSM_API_HOST=1` before `meerschaum.api` is
    imported).

    Returns
    -------
    The FastAPI app (`meerschaum.api.app`).
    """
    from meerschaum.api.host._app import build_app as _build_app
    return _build_app(load_plugins=load_plugins, debug=debug)


def run(socket_path: str, debug: bool = False) -> None:
    """
    Serve the host on a Unix socket with uvicorn until interrupted.
    """
    from meerschaum.api.host._app import run as _run
    _run(socket_path, debug=debug)


def run_tcp(host: str, port: int, debug: bool = False) -> None:
    """
    Serve the host on loopback TCP with uvicorn until interrupted (Windows has no Unix sockets in
    the proxy). If `MRSM_API_HOST_PROXY_TOKEN` is set, every request and websocket handshake
    without the matching `X-Mrsm-Host-Proxy-Token` header is refused.
    """
    from meerschaum.api.host._app import run_tcp as _run_tcp
    _run_tcp(host, port, debug=debug)
