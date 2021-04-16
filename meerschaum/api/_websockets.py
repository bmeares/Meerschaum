#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement WebSockets for the Meerschaum API via FastAPI.
"""

from meerschaum.api import (
    app, get_connector as get_api_connector, get_uvicorn_config, debug, fastapi,
    manager,
)
from meerschaum.config.static import _static_config
from meerschaum.utils.typing import Optional
import time, datetime

_websocket_endpoint = _static_config()['api']['endpoints']['websocket']

class ConnectionManager:
    def __init__(self):
        self.active_connections

@app.websocket(_websocket_endpoint)
async def websocket_endpoint(
        websocket : fastapi.WebSocket,
        #  curr_user = fastapi.Depends(manager),
    ):
    await websocket.accept()

    #  print('DATA:', data)
    while True:
        try:
            data = await websocket.receive_text()
            await websocket.send_text(str(datetime.datetime.utcnow()))
        except fastapi.WebSocketDisconnect:
            break
            pass
    #  print('sending...')
    #  await websocket.send_text('message')
