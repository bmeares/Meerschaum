#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Routes to the Webterm proxy.
"""

import asyncio
from meerschaum.utils.typing import Optional
from meerschaum.api import app, endpoints
from meerschaum.utils.packages import attempt_import
from meerschaum.api.dash.sessions import is_session_authenticated, get_username_from_session
fastapi, fastapi_responses = attempt_import('fastapi', 'fastapi.responses')
import starlette

httpcore = attempt_import('httpcore', lazy=False)
httpx = attempt_import('httpx', lazy=False)
websockets = attempt_import('websockets', lazy=False)
Request = fastapi.Request
WebSocket = fastapi.WebSocket
HTMLResponse = fastapi_responses.HTMLResponse
Response = fastapi_responses.Response
PlainTextResponse = fastapi_responses.PlainTextResponse


@app.get(endpoints['webterm'], tags=["Webterm"])
async def get_webterm(
    request: Request,
    session_id: str,
) -> HTMLResponse:
    """
    Get the main HTML template for the Webterm.
    """
    if not is_session_authenticated(session_id):
        return HTMLResponse(
            """
            <html>
                <head>
                    <style>
                    body {
                        background-color: #222222;
                    }
                    #message-div {
                        padding: 15px;
                        font-size: x-large;
                        font-family: sans-serif;
                    }
                    h2 {
                        color: #FFFFFF;
                    }
                    p {
                        color: #FFFFFF;
                    }
                    a {
                        color: #FFFFFF;
                        text-decoration: underline;
                    }
                    </style>
                </head>
                <body>
                    <div id="message-div">
                        <h2>Unauthorized</h2>
                        <p>Please <a href="/dash/login">log in</a> and try again.</p>
                    </div>
                </body>
            </html>
            """,
            status_code = 401,
        )

    username = get_username_from_session(session_id)
    async with httpx.AsyncClient() as client:
        webterm_url = f"http://localhost:8765/webterm/{username or session_id}"
        response = await client.get(webterm_url)
        text = response.text
        if request.url.scheme == 'https':
            text = text.replace('ws://', 'wss://')
        text = text.replace(f'websocket/{username}', f'websocket/{session_id}')
        return HTMLResponse(
            content=text,
            status_code=response.status_code,
            headers=request.headers,
        )


@app.websocket(endpoints['webterm_websocket'])
async def webterm_websocket(websocket: WebSocket, session_id: str):
    """
    Connect to the Webterm's websocket.
    """
    try:
        await websocket.accept()
    except starlette.websockets.WebSocketDisconnect:
        return

    if not is_session_authenticated(session_id):
        await websocket.close()
        return

    username = get_username_from_session(session_id)

    ws_url = f"ws://localhost:8765/websocket/{username or session_id}"
    async with websockets.connect(ws_url) as ws:
        async def forward_messages():
            try:
                while True:
                    client_message = await websocket.receive_text()
                    await ws.send(client_message)
            except (
                websockets.exceptions.ConnectionClosed,
                starlette.websockets.WebSocketDisconnect
            ):
                await ws.close()

        async def backward_messages():
            try:
                while True:
                    server_response = await ws.recv()
                    await websocket.send_text(server_response)
            except (
                websockets.exceptions.ConnectionClosed,
                starlette.websockets.WebSocketDisconnect
            ):
                pass

        await asyncio.gather(
            forward_messages(),
            backward_messages(),
        )
