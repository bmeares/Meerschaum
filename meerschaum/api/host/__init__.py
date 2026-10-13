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
- plugin-owned Dash apps (`@dash_plugin`, legacy `@web_page` layouts), each mounted at
  `/dash/<plugin>/` with the core stores pre-populated (§8.4, `meerschaum.api.host.dash`);
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
MCP_PATH: str = '/_mrsm/host/mcp'
TOKEN_HEADER: str = 'x-mrsm-host-token'
TOKEN_CLAIM: str = 'mrsm_host'

__all__ = (
    'build_app',
    'run',
    'is_host_mode',
    'HOST_ENV',
    'HOST_CONFIG_ENV',
)


def is_host_mode() -> bool:
    """
    Return whether this process is the API host (`MRSM_API_HOST` is set).
    """
    return os.environ.get(HOST_ENV, '').lower() not in ('', '0', 'false')


def build_app(load_plugins: bool = True, debug: bool = False):
    """
    Build the host's FastAPI app: run the API plugins, mount the plugin-owned Dash apps, and add
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
