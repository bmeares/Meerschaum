#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement WebSockets for the Meerschaum API via FastAPI.
"""

import time, uuid
from datetime import datetime, timezone
from meerschaum.api import (
    app, get_api_connector, get_uvicorn_config, debug, fastapi, endpoints
)
from meerschaum.api.dash.users import is_session_authenticated
from meerschaum.utils.typing import Optional

_websocket_endpoint = endpoints['websocket']

websockets = {}
sessions = {}

@app.websocket('/dashws')
@app.websocket('/dash/ws')
@app.websocket(_websocket_endpoint)
async def websocket_endpoint(
        websocket: fastapi.WebSocket,
    ):
    """
    Communicate with the Web Interface over a websocket.
    """
    await websocket.accept()
    try:
        initial_data = await websocket.receive_json()
    except Exception as e:
        initial_data = {'session-id': None}
    session_id = initial_data.get('session-id', None)
    if not is_session_authenticated(str(session_id)):
        await websocket.close()
        return
    now = datetime.now(timezone.utc)
    join_msg = str(now)
    await websocket.send_text(join_msg)
    websockets[session_id] = websocket
    while True:
        try:
            data = await websocket.receive_text()
            await websocket.send_text(str(now))
        except fastapi.WebSocketDisconnect:
            delete_websocket_session(session_id)
            break


def delete_websocket_session(session_id: str) -> None:
    """
    Delete a websocket session if it exists.
    """
    try:
        del websockets[session_id]
    except KeyError:
        pass
