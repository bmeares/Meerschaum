#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Meerschaum's Model Context Protocol (MCP) server.

The tools, resources, and prompts are declared once in `meerschaum.mcp._tools`,
`._resources`, and `._prompts`, and served over two transports:

- **HTTP** (Streamable HTTP) at `/mcp` on the Meerschaum API, where each tool
  requires the same OAuth2 scope as its equivalent REST route.
- **stdio**, via `mrsm start mcp`, for a local Meerschaum install with no API
  server. stdio inherits the privileges of the shell which launched it — the
  same trust model as the CLI — so no token is involved.

Examples
--------
>>> from meerschaum.mcp import handle_message
>>> response = handle_message({'jsonrpc': '2.0', 'id': 1, 'method': 'tools/list'}, ['pipes:read'])
>>> [tool['name'] for tool in response['result']['tools']]
['get_pipe_attributes', 'get_pipe_data', 'get_pipe_stats', 'list_pipes']
"""

from __future__ import annotations

import meerschaum as mrsm

from meerschaum.mcp._registry import (
    MCPTool,
    MCPResource,
    MCPPrompt,
    tool,
    resource,
    prompt,
    get_tools,
    get_resources,
    get_resource_templates,
    get_all_resources,
    get_prompts,
    encode_cursor,
    decode_cursor,
    paginate,
)
from meerschaum.mcp._dispatch import (
    handle_message,
    handle_payload,
    has_scope,
    get_visible_tools,
    get_visible_resources,
    get_visible_prompts,
    jsonrpc_error,
    jsonrpc_result,
    LATEST_PROTOCOL_VERSION,
    SUPPORTED_PROTOCOL_VERSIONS,
    SERVER_NAME,
)
from meerschaum.mcp._primer import MRSM_PRIMER

__all__ = (
    'MCPTool',
    'MCPResource',
    'MCPPrompt',
    'tool',
    'resource',
    'prompt',
    'get_tools',
    'get_resources',
    'get_resource_templates',
    'get_all_resources',
    'get_prompts',
    'get_visible_tools',
    'get_visible_resources',
    'get_visible_prompts',
    'handle_message',
    'handle_payload',
    'has_scope',
    'has_mcp_plugin',
    'is_mcp_enabled',
    'is_read_only_server',
    'encode_cursor',
    'decode_cursor',
    'paginate',
    'jsonrpc_error',
    'jsonrpc_result',
    'LATEST_PROTOCOL_VERSION',
    'SUPPORTED_PROTOCOL_VERSIONS',
    'SERVER_NAME',
    'MRSM_PRIMER',
)


def is_mcp_enabled() -> bool:
    """
    Return whether the `/mcp` endpoint should be served on the API.
    """
    return bool(mrsm.get_config('api', 'mcp', 'enabled', warn=False))


def is_read_only_server() -> bool:
    """
    Return whether this MCP server refuses to expose tools which modify data.

    Read-only mode is a deployment-level switch independent of any token's
    scopes: it hides every non-read-only tool even from a `*`-scoped caller.
    """
    return bool(mrsm.get_config('api', 'mcp', 'read_only', warn=False))


def has_mcp_plugin() -> bool:
    """
    Return whether the third-party `mcp` plugin is installed.

    The plugin predates this module and registers its own `/mcp` route via
    `@api_plugin`. The built-in route is registered first and therefore wins, so
    the plugin's route becomes unreachable rather than conflicting — but leaving
    it installed is confusing, so the API warns about it at startup.
    """
    try:
        plugin = mrsm.Plugin('mcp')
        return plugin.is_installed()
    except Exception:
        return False
