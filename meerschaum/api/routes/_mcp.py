#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Serve Meerschaum's MCP server over the Streamable HTTP transport.

The tools themselves live in `meerschaum.mcp` and are shared with the stdio
transport (`mrsm start mcp`). This module only handles HTTP: parsing the
JSON-RPC payload, resolving the caller's scopes, and returning the response.

Each tool requires the same OAuth2 scope as its equivalent REST route, so a
token minted for `pipes:read` can read pipes here and nowhere else. Tools the
caller's scopes do not cover are omitted from `tools/list` rather than
advertised and then refused.
"""

from __future__ import annotations

import fastapi
from fastapi import Request
from fastapi.concurrency import run_in_threadpool

import meerschaum as mrsm
from meerschaum.api import app, endpoints
from meerschaum.api._oauth2 import CurrentScopes
from meerschaum.mcp._security import set_mcp_context, reset_mcp_context
from meerschaum.mcp import (
    handle_payload,
    get_visible_tools,
    get_visible_resources,
    get_visible_prompts,
    is_read_only_server,
    jsonrpc_error,
    LATEST_PROTOCOL_VERSION,
    SERVER_NAME,
    MRSM_PRIMER,
)
from meerschaum.mcp._dispatch import PARSE_ERROR
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.typing import List

fastapi_responses = attempt_import('fastapi.responses', lazy=False)
JSONResponse = fastapi_responses.JSONResponse
Response = fastapi_responses.Response

endpoint = endpoints['mcp']


@app.post(endpoint, tags=['MCP'])
async def mcp_endpoint(
    request: Request,
    current_scopes: List[str] = fastapi.Depends(CurrentScopes),
):
    """
    Meerschaum's Model Context Protocol endpoint (Streamable HTTP transport).

    Point any MCP client at `<api-url>/mcp` with a bearer token. Each tool
    requires the same scope as its equivalent REST route: reads need
    `pipes:read`, writes need `pipes:write`, `drop_pipe` needs `pipes:drop`,
    `delete_pipe` needs `pipes:delete`, `execute_action` needs
    `actions:execute`, and `read_sql` needs `sql:read`.
    """
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(
            status_code=400,
            content=jsonrpc_error(None, PARSE_ERROR, "Parse error"),
        )

    ### Tools do blocking work (syncs, queries, actions) which can run for
    ### minutes, so run them off the event loop or every other request stalls.
    ### The context marks this as an API call so tools apply the server's
    ### instance and action permissions; it rides along because `contextvars`
    ### are copied into the threadpool worker.
    user_or_token = getattr(request.state, 'user_or_token', None)
    context_token = set_mcp_context({'api': True, 'user': user_or_token})
    try:
        response = await run_in_threadpool(handle_payload, payload, current_scopes)
    finally:
        reset_mcp_context(context_token)

    ### Notifications get no body.
    if response is None:
        return Response(status_code=202)

    return JSONResponse(content=response)


@app.get(endpoint, tags=['MCP'])
async def mcp_info(
    request: Request,
    current_scopes: List[str] = fastapi.Depends(CurrentScopes),
):
    """
    Return this MCP server's capabilities and everything the caller is scoped
    for.

    Not part of the MCP specification — a convenience for humans checking what a
    token can actually reach.
    """
    return {
        'protocolVersion': LATEST_PROTOCOL_VERSION,
        'serverInfo': {
            'name': SERVER_NAME,
            'title': 'Meerschaum',
            'version': mrsm.__version__,
        },
        'capabilities': {
            'tools': {'listChanged': False},
            'resources': {'listChanged': False, 'subscribe': False},
            'prompts': {'listChanged': False},
        },
        'readOnly': is_read_only_server(),
        'instructions': MRSM_PRIMER,
        'tools': [
            tool.to_dict() for tool in get_visible_tools(current_scopes).values()
        ],
        'resources': [
            res.to_dict() for res in get_visible_resources(current_scopes)
        ],
        'prompts': [
            prompt.to_dict() for prompt in get_visible_prompts(current_scopes).values()
        ],
    }
