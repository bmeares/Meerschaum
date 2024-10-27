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
from meerschaum.api.dash.sessions import is_session_authenticated
fastapi, fastapi_responses = attempt_import('fastapi', 'fastapi.responses')
import starlette
httpcore = attempt_import('httpcore')
httpx = attempt_import('httpx')
websockets = attempt_import('websockets')
Request = fastapi.Request
WebSocket = fastapi.WebSocket
HTMLResponse = fastapi_responses.HTMLResponse
Response = fastapi_responses.Response
PlainTextResponse = fastapi_responses.PlainTextResponse


@app.get(endpoints['webterm'], tags=["Webterm"])
async def get_webterm(
    request: Request,
    s: Optional[str] = None,
) -> HTMLResponse:
    """
    Get the main HTML template for the Webterm.
    """
    if not is_session_authenticated(s):
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

    async with httpx.AsyncClient() as client:
        response = await client.get("http://localhost:8765/")
        text = response.text
        if request.url.scheme == 'https':
            text = text.replace('ws://', 'wss://')
        return HTMLResponse(
            content = text,
            status_code = response.status_code,
            headers = request.headers,
        )


@app.websocket(endpoints['webterm_websocket'])
async def webterm_websocket(websocket: WebSocket):
    """
    Connect to the Webterm's websocket.
    """
    try:
        await websocket.accept()
        session_doc = await websocket.receive_json()
    except starlette.websockets.WebSocketDisconnect:
        return
    session_id = (session_doc or {}).get('session-id', 'no-auth')

    if not is_session_authenticated(session_id):
        await websocket.close()
        return

    async with websockets.connect("ws://localhost:8765/websocket") as ws:
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
