#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The host's internal routes, called only by the Rust server (§7.2 federation):

- `GET /_mrsm/host/registry`: the MCP tools, resources and prompts registered by plugins (not
  the built-ins, which the Rust server implements), and the paths of the app's routes;
- `POST /_mrsm/host/mcp`: a JSON-RPC message, dispatched by `meerschaum.mcp` with the caller's
  scopes from the internal token.

Both require `X-Mrsm-Host-Token` (see `_auth.decode_internal_token`). The Rust server never
proxies `/_mrsm/` from clients.
"""

from meerschaum.utils.typing import Any, Dict, List


def app_route_paths(app) -> List[str]:
    """
    Return the paths of the app's routes and mounts, minus the internal ones.
    """
    from meerschaum.api.host import INTERNAL_PREFIX
    paths = []
    for route in getattr(app, 'routes', []):
        path = getattr(route, 'path', None)
        if not path or path.startswith(INTERNAL_PREFIX):
            continue
        if path not in paths:
            paths.append(path)
    return paths


def web_page_registry() -> List[Dict[str, Any]]:
    """
    Return every `@web_page` with its mode: `html` (page contract, served at `route`) or `dash`
    (legacy layout, served by the plugin's Dash app at `/dash/<plugin>/...`).
    """
    from meerschaum.plugins import _plugin_endpoints_to_pages
    from meerschaum.api.host.dash import get_page_contract_pages, plugin_path

    html_pages = get_page_contract_pages()
    out = []
    for page_group, pages in list(_plugin_endpoints_to_pages.items()):
        for page_str, page in list(pages.items()):
            contract = html_pages.get(page_str, None)
            plugin_name = contract['plugin'] if contract else page_group
            entry = {
                'path': page_str,
                'page_group': page_group,
                'page_key': page.get('page_key', None),
                'login_required': bool(page.get('login_required', True)),
                'skip_navbar': bool(page.get('skip_navbar', False)),
                'dark_theme': bool(page.get('dark_theme', True)),
                'function': getattr(page.get('function'), '__name__', None),
                'mode': 'html' if contract else 'dash',
            }
            if contract:
                entry['route'] = contract['route']
            else:
                entry['route'] = plugin_path(plugin_name, page_str)
            out.append(entry)
    return out


def registry_document(app) -> Dict[str, Any]:
    """
    Return the plugin registry the Rust server federates.
    """
    from meerschaum.mcp._registry import get_plugin_registry
    return {
        **get_plugin_registry(),
        'routes': app_route_paths(app),
        'web_pages': web_page_registry(),
    }


def register(app) -> None:
    """
    Add the internal routes to the host app.
    """
    import fastapi
    from fastapi.concurrency import run_in_threadpool
    from starlette.responses import JSONResponse, Response
    from meerschaum.api.host import REGISTRY_PATH, MCP_PATH, TOKEN_HEADER
    from meerschaum.api.host._auth import decode_internal_token, load_user_or_token_from_manager

    def _forbidden():
        return JSONResponse(status_code=403, content={'detail': 'Forbidden.'})

    @app.get(REGISTRY_PATH, include_in_schema=False)
    async def _host_registry(request: fastapi.Request):
        if decode_internal_token(request.headers.get(TOKEN_HEADER)) is None:
            return _forbidden()
        return JSONResponse(content=registry_document(app))

    @app.post(MCP_PATH, include_in_schema=False)
    async def _host_mcp(request: fastapi.Request):
        claims = decode_internal_token(request.headers.get(TOKEN_HEADER))
        if claims is None:
            return _forbidden()
        from meerschaum.mcp import handle_payload, jsonrpc_error
        from meerschaum.mcp._dispatch import PARSE_ERROR
        from meerschaum.mcp._security import set_mcp_context, reset_mcp_context
        try:
            payload = await request.json()
        except Exception:
            return JSONResponse(status_code=400, content=jsonrpc_error(None, PARSE_ERROR, "Parse error"))

        scopes = claims.get('scopes') or []
        sub = claims.get('sub')

        def _dispatch():
            user = None
            if sub:
                try:
                    user = load_user_or_token_from_manager(str(sub))
                except Exception:
                    user = None
            token = set_mcp_context({'api': True, 'user': user})
            try:
                return handle_payload(payload, scopes)
            finally:
                reset_mcp_context(token)

        response = await run_in_threadpool(_dispatch)
        if response is None:
            return Response(status_code=202)
        return JSONResponse(content=response)
