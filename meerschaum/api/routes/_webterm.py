#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Routes to the Webterm proxy.
"""

import asyncio
from meerschaum.utils.typing import Optional
from meerschaum.api import app, no_auth, manager
from meerschaum.utils.packages import attempt_import
from meerschaum.api.dash import active_sessions
fastapi, fastapi_responses = attempt_import('fastapi', 'fastapi.responses')
import starlette
httpx = attempt_import('httpx')
websockets = attempt_import('websockets')
Request = fastapi.Request
WebSocket = fastapi.WebSocket
HTMLResponse = fastapi_responses.HTMLResponse
Response = fastapi_responses.Response
PlainTextResponse = fastapi_responses.PlainTextResponse


@app.get("/webterm", tags=["Webterm"])
async def get_webterm(
        request: Request,
        s: Optional[str] = 'no-auth',
    ) -> HTMLResponse:
    """
    Get the main HTML template for the Webterm.
    """
    session_id = s
    if session_id not in active_sessions:
        return HTMLResponse(
            """
            <h2>Unauthorized</h2>
            <p>Please <a href="/dash/login">log in</a> and try again.</p>
            """,
            status_code = 401,
        )

    async with httpx.AsyncClient() as client:
        response = await client.get("http://localhost:8765/")
        return HTMLResponse(
            content = response.text,
            status_code = response.status_code,
            headers = request.headers,
        )


@app.get("/xstatic/termjs/term.js", tags=['Webterm'])
async def get_termjs(
        v: str,
        request: Request,
    ):
    """
    Fetch the `term.js` source file.
    """

    async with httpx.AsyncClient() as client:
        response = await client.get(
            "http://localhost:8765/xstatic/termjs/term.js",
            params = {'v': v},
            headers = request.headers,
        )
        return Response(
            content = response.text,
            status_code = response.status_code,
            headers = response.headers,
        )


@app.websocket("/websocket")
async def webterm_websocket(websocket: WebSocket):
    """
    Connect to the Webterm's websocket.
    """
    
    await websocket.accept()

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
                pass

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
