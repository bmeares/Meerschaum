#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
A synthetic plugin exercising every extension point of the Python API host (v5 design §7.3,
§8.4, §7.2): API routes (plain, scoped, streaming, websocket), an HTTP middleware, a static
mount, Dash console registrations (`@dash_plugin` and a `@web_page` Dash layout), and an MCP
tool, resource and prompt.

Used by `crates/meerschaum-server/tests/host_e2e.rs` (through the Rust server) and
`tests/test_api_host.py` (against the host app directly). All data is synthetic.
"""

import os

from meerschaum.plugins import api_plugin, dash_plugin, web_page
from meerschaum.mcp import tool, resource, prompt

__version__ = '0.0.1'
required = []

HERE = os.path.dirname(os.path.abspath(__file__))


@api_plugin
def init_routes(app):
    """Routes, a middleware and a static mount."""
    import asyncio
    import fastapi
    from fastapi.responses import StreamingResponse, PlainTextResponse
    from fastapi.staticfiles import StaticFiles
    from meerschaum.api import ScopedAuth, CurrentScopes, manager

    @app.middleware('http')
    async def tag_responses(request, call_next):
        response = await call_next(request)
        response.headers['x-hostdemo'] = 'middleware'
        return response

    @app.get('/hostdemo/hello')
    def hello():
        return {'message': 'Hello from hostdemo!'}

    @app.api_route('/hostdemo/echo', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'])
    async def echo(request: fastapi.Request):
        body = await request.body()
        return {
            'method': request.method,
            'path': request.url.path,
            'query': request.url.query,
            'cookie': request.cookies.get('flavor'),
            'header': request.headers.get('x-demo'),
            'body': body.decode('utf-8', errors='replace'),
        }

    @app.get('/hostdemo/whoami')
    def whoami(user=fastapi.Depends(manager)):
        return {'username': user.username}

    @app.get('/hostdemo/scoped')
    def scoped(
        principal=fastapi.Depends(ScopedAuth(['hostdemo:read'])),
        scopes=fastapi.Depends(CurrentScopes),
    ):
        return {'scopes': sorted(scopes)}

    @app.get('/hostdemo/stream')
    def stream():
        async def chunks():
            for i in range(3):
                yield f'chunk {i}\n'
                await asyncio.sleep(0.01)
        return StreamingResponse(chunks(), media_type='text/plain')

    @app.post('/hostdemo/upload')
    async def upload(request: fastapi.Request):
        size = 0
        async for chunk in request.stream():
            size += len(chunk)
        return PlainTextResponse(str(size))

    @app.websocket('/hostdemo/ws')
    async def ws(websocket: fastapi.WebSocket):
        await websocket.accept()
        try:
            while True:
                text = await websocket.receive_text()
                await websocket.send_text(f'echo: {text}')
        except fastapi.WebSocketDisconnect:
            pass

    app.mount(
        '/hostdemo/static',
        StaticFiles(directory=os.path.join(HERE, 'static')),
        name='hostdemo_static',
    )


@dash_plugin
def init_dash(dash_app):
    """A callback that reads the core `session-store`."""
    import dash
    from dash import Input, Output

    @dash_app.callback(
        Output('hostdemo-session', 'children'),
        Input('session-store', 'data'),
    )
    def show_session(data):
        return 'logged in' if (data or {}).get('session-id') else 'anonymous'


@web_page('hostdemo/page', login_required=False)
def demo_page():
    """A legacy Dash page."""
    from dash import html
    return html.Div(
        [html.H1('Hostdemo'), html.Div(id='hostdemo-session')],
        className='hostdemo-page',
    )


@tool(
    'hostdemo_add',
    'Add two numbers (a synthetic plugin tool).',
    scopes=['hostdemo:read'],
    read_only=True,
    idempotent=True,
    input_schema={
        'type': 'object',
        'properties': {
            'a': {'type': 'number'},
            'b': {'type': 'number'},
        },
        'required': ['a', 'b'],
    },
    output_schema={
        'type': 'object',
        'properties': {'sum': {'type': 'number'}},
    },
)
def hostdemo_add(a, b):
    return {'sum': a + b}


@tool(
    'hostdemo_reset',
    'Reset the synthetic counter (a plugin write tool).',
    scopes=['hostdemo:write'],
    read_only=False,
    destructive=True,
)
def hostdemo_reset():
    return {'reset': True}


@resource(
    'hostdemo_readme',
    'A synthetic plugin resource.',
    uri='hostdemo://readme',
    mime_type='text/plain',
    scopes=['hostdemo:read'],
)
def hostdemo_readme():
    return 'Hostdemo is a synthetic plugin.'


@prompt(
    'hostdemo_greet',
    'Greet someone (a synthetic plugin prompt).',
    arguments=[{'name': 'who', 'description': 'Whom to greet.', 'required': True}],
    scopes=['hostdemo:read'],
)
def hostdemo_greet(who):
    return [{'role': 'user', 'content': {'type': 'text', 'text': f'Say hello to {who}.'}}]
