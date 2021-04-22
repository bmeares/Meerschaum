#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement WebSockets for the Meerschaum API via FastAPI.
"""

import time, datetime, uuid
from meerschaum.api import (
    app, get_api_connector, get_uvicorn_config, debug, fastapi, endpoints
)
from meerschaum.config.static import _static_config
from meerschaum.utils.typing import Optional

#  _websocket_endpoint = _static_config()['api']['endpoints']['websocket']
_websocket_endpoint = endpoints['websocket']

websockets = {}
sessions = {}

@app.websocket(_websocket_endpoint)
async def websocket_endpoint(
        websocket : fastapi.WebSocket,
    ):
    """
    Communicate with the Web Interface over a websocket.
    """
    global websockets
    await websocket.accept()
    initial_data = await websocket.receive_json()
    session_id = initial_data['session-id']
    now = datetime.datetime.utcnow()
    join_msg = ""
    await websocket.send_text(join_msg)
    websockets[session_id] = websocket
    ### NOTE: remove the below statement!
    websockets['debug'] = websocket
    while True:
        try:
            data = await websocket.receive_text()
            await websocket.send_text(str(datetime.datetime.utcnow()))
        except fastapi.WebSocketDisconnect:
            del websockets[session_id]
            break
