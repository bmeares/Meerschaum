"""Exact declared extension invocation in a native private Api execution.

No Main application, cookie jar, request authentication, module name, or attribute lookup
is accepted here. The native launcher validates its sealed descriptor before this fixed op.
"""

from __future__ import annotations

MAX_REPLY = 2 * 1024 * 1024


def _require_execution():
    import os
    from meerschaum._internal import team_sandbox
    if (
        os.environ.get('MRSM_API_EXECUTION') != '1'
        or os.environ.get('MRSM_METADATA_EXECUTION') == '1'
        or not team_sandbox.has_required_socket_boundary()
    ):
        raise PermissionError('Extension invocation requires its native private Api context.')


def _api_app(plugin):
    from fastapi import FastAPI
    import meerschaum.plugins as plugins
    from meerschaum.worker._plugin_registry import _hooks
    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)
    for hook in _hooks(plugins._api_plugins, plugin):
        hook(app)
    return app


async def _api(plugin, admitted, request, registry, caller):
    from starlette.routing import Match
    from urllib.parse import urlencode
    app = _api_app(plugin)
    selector = admitted['selector']
    from meerschaum.worker._plugin_declarations import require_api_declaration
    require_api_declaration(app, admitted, registry)
    if caller is not None:
        # Override only the already imported core SDK manager dependency in this private
        # app. It receives public data, never a Main cookie, JWT, signer or auth loader.
        import sys
        oauth = sys.modules.get('meerschaum.api._oauth2')
        manager = getattr(oauth, 'manager', None)
        if manager is not None:
            from fastapi import Request
            async def projected_manager(request):
                from meerschaum.api._extension_user import extension_principal
                handled, projected, _ = extension_principal(request)
                if not handled:
                    raise PermissionError('The exact private caller is unavailable.')
                return projected
            projected_manager.__annotations__ = {'request': Request}
            app.dependency_overrides[manager] = projected_manager
    scope = {
        'type': 'http', 'asgi': {'version': '3.0'}, 'http_version': '1.1',
        'method': request['method'], 'scheme': 'http', 'path': request['path'],
        'raw_path': request['path'].encode('utf-8'), 'root_path': '',
        'query_string': urlencode(request['query']).encode('ascii'),
        'headers': [(key.encode('ascii'), value.encode('utf-8')) for key, value in request['headers'].items()],
        'client': ('extension.invalid', 0), 'server': ('extension.invalid', 0),
        'app': app,
    }
    if caller is not None:
        from meerschaum.api._extension_user import SCOPE_KEY
        scope[SCOPE_KEY] = caller
    matches = [route for route in app.routes if getattr(route, 'path', None) == selector['path']
               and selector['method'] in (getattr(route, 'methods', None) or ())
               and route.matches(scope)[0] == Match.FULL]
    if len(matches) != 1:
        raise PermissionError('The declared API handler is unavailable in this exact source view.')
    body = request['body'].encode('utf-8')
    used, status, parts, count = False, None, [], 0

    async def receive():
        nonlocal used
        if used:
            return {'type': 'http.disconnect'}
        used = True
        return {'type': 'http.request', 'body': body, 'more_body': False}

    async def send(message):
        nonlocal status, count
        if message['type'] == 'http.response.start':
            if status is not None:
                raise ValueError('The extension sent more than one response.')
            status = int(message['status'])
            # Headers are deliberately not transported. In particular, Set-Cookie and
            # Location from a plugin cannot change Main's sessions or select navigation.
        elif message['type'] == 'http.response.body':
            part = message.get('body', b'')
            count += len(part)
            if count > MAX_REPLY:
                raise ValueError('The extension response exceeds its byte bound.')
            parts.append(part)
        else:
            raise ValueError('Unsupported extension ASGI response.')

    # Running the private app preserves dependencies and middleware while its router can
    # reach only the one positively matched declared method/path above.
    await app(scope, receive, send)
    if status is None:
        raise ValueError('The extension produced no API response.')
    return {'kind': 'api', 'status': status, 'body': b''.join(parts).decode('utf-8')}


def _page(plugin, selector, request):
    import json
    from meerschaum.plugins import _get_parent_plugin, _plugin_endpoints_to_pages
    from meerschaum.utils.misc import filter_keywords
    matches = []
    for pages in _plugin_endpoints_to_pages.values():
        for path, info in pages.items():
            function = info.get('function')
            normalized = str(path).strip('/').removeprefix('dash/')
            if normalized == selector['path'] and callable(function) and _get_parent_plugin(function=function) == plugin:
                matches.append(function)
    if len(matches) != 1:
        raise PermissionError('The declared page is unavailable in this exact source view.')
    result = matches[0](**filter_keywords(matches[0], **dict(request['query'])))
    status = 200
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], int):
        result, status = result
    if selector['renderer'] == 'html':
        if not isinstance(result, str):
            raise ValueError('The page no longer returns its declared HTML renderer.')
        return {'kind': 'page', 'fragment': result, 'status': status}
    from plotly.utils import PlotlyJSONEncoder
    return {'kind': 'page', 'fragment': json.dumps(result, cls=PlotlyJSONEncoder, ensure_ascii=False), 'status': status}


def _dash(plugin, admitted, request):
    import json
    import meerschaum.plugins as plugins
    selector = admitted['selector']
    from dash import Dash, html, _callback
    from meerschaum.plugins import _get_parent_plugin
    from meerschaum.worker._plugin_registry import _hooks
    app = Dash(__name__ + '_' + plugin, requests_pathname_prefix='/_isolated/',
               routes_pathname_prefix='/_isolated/', use_pages=False,
               suppress_callback_exceptions=True, serve_locally=True, include_assets_files=False)
    app.layout = html.Div()
    for hook in _hooks(plugins._dash_plugins, plugin):
        hook(app)
    before_map, before_list = dict(_callback.GLOBAL_CALLBACK_MAP), list(_callback.GLOBAL_CALLBACK_LIST)
    try:
        app._setup_server()
    finally:
        _callback.GLOBAL_CALLBACK_MAP.clear()
        _callback.GLOBAL_CALLBACK_MAP.update(before_map)
        _callback.GLOBAL_CALLBACK_LIST[:] = before_list
    declared = app.callback_map.get(selector['output'], {})
    callback = declared.get('callback')
    if not callable(callback) or _get_parent_plugin(function=callback) != plugin:
        raise PermissionError('The declared Dash callback is unavailable.')
    from meerschaum.worker._plugin_declarations import require_dash_declaration
    require_dash_declaration(declared, admitted)
    data = request['inputs']
    if not isinstance(data, dict) or data.get('output') != selector['output']:
        raise PermissionError('A Dash request cannot select another callback.')
    response = app.server.test_client().post('/_isolated/_dash-update-component', json=data)
    if response.status_code == 204:
        return {'kind': 'dash_callback', 'updates': {}}
    if response.status_code != 200 or len(response.data) > MAX_REPLY:
        raise ValueError('The declared Dash callback failed or exceeded its byte bound.')
    return {'kind': 'dash_callback', 'updates': json.loads(response.data)}


async def _mcp(plugin, admitted, request, metadata):
    import inspect
    selector = admitted['selector']
    from meerschaum.mcp import _registry
    from meerschaum.plugins import _get_parent_plugin
    from meerschaum.utils.misc import filter_keywords
    entries = {
        'mcp_tool': list(_registry._TOOLS.values()),
        'mcp_resource': list(_registry._RESOURCES),
        'mcp_prompt': list(_registry._PROMPTS.values()),
    }[selector['kind']]
    matches = [entry for entry in entries if entry.name == selector['name']
               and _get_parent_plugin(function=entry.handler) == plugin]
    arguments = request['arguments']
    if len(matches) != 1 or not isinstance(arguments, dict):
        raise PermissionError('The exact declared MCP handler is unavailable.')
    group = {'mcp_tool':'mcp_tools','mcp_resource':'mcp_resources','mcp_prompt':'mcp_prompts'}[selector['kind']]
    declared = [entry for entry in metadata[group] if entry['name'] == selector['name']]
    if len(declared) != 1:
        raise PermissionError('The exact MCP input declaration is unavailable.')
    from meerschaum.worker._plugin_declarations import require_mcp_declaration
    require_mcp_declaration(matches[0], admitted, declared[0])
    if selector['kind'] == 'mcp_resource':
        arguments = arguments['variables']
    function = matches[0].handler
    result = function(**filter_keywords(function, **arguments))
    if inspect.isawaitable(result):
        result = await result
    if selector['kind'] == 'mcp_resource':
        import json
        result = {'text': result if isinstance(result, str) else json.dumps(result, ensure_ascii=False),
                  'mime_type': matches[0].mime_type}
    elif selector['kind'] == 'mcp_prompt' and isinstance(result, list):
        result = {'description': matches[0].description, 'messages': result}
    return {'kind': 'json', 'value': result}


def invoke(operation):
    import asyncio
    import json
    import os
    import time
    from meerschaum.plugins import load_plugins
    _require_execution()
    if operation.get('deadline', 0) <= time.time():
        raise PermissionError('The extension operation expired.')
    catalog, invocation = operation['catalog'], operation['invocation']
    if sorted(catalog['plugins']) != sorted(json.loads(os.environ['MRSM_EXECUTOR_PLUGINS'])):
        raise PermissionError('The invocation has another selected source set.')
    source = catalog['plugins'][invocation['plugin']]
    registry = source['metadata']['registry']
    candidates = [entry for group in ('api', 'mcp', 'pages', 'dash_callbacks')
                  if registry[group]['state'] == 'complete' for entry in registry[group]['entries']
                  if entry['id'] == invocation['handler_id']]
    if len(candidates) != 1:
        raise PermissionError('The invocation has no exact declared handler.')
    selector, request = candidates[0]['selector'], invocation['request']
    load_plugins(skip_if_loaded=True)
    kind = selector['kind']
    if kind == 'api' and request['kind'] == 'api':
        result = asyncio.run(_api(invocation['plugin'], candidates[0], request, registry, operation.get('caller')))
    elif kind == 'page' and request['kind'] == 'page':
        result = _page(invocation['plugin'], selector, request)
    elif kind == 'dash_callback' and request['kind'] == 'dash_callback':
        result = _dash(invocation['plugin'], candidates[0], request)
    elif kind.startswith('mcp_') and request['kind'] == 'mcp':
        result = asyncio.run(_mcp(invocation['plugin'], candidates[0], request, source['metadata']))
    else:
        raise PermissionError('The invocation does not match its exact handler kind.')
    encoded = json.dumps(result, ensure_ascii=False)
    if len(encoded.encode('utf-8')) > MAX_REPLY:
        raise ValueError('The extension response exceeds its byte bound.')
    return encoded
