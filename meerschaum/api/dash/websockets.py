#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting via WebSockets.
"""

import asyncio, sys
from meerschaum.api._websockets import websockets
from meerschaum.config.static import _static_config

def ws_url_from_href(href : str):
    """
    Generate the websocket URL from the webpage href.
    """
    http_protocol = href.split('://')[0]
    ws_protocol = 'wss' if http_protocol == 'https' else 'ws'
    host_and_port = href.replace(http_protocol + '://', '').split('/')[0]
    return (
        ws_protocol + '://' +
        host_and_port +
        _static_config()['api']['endpoints']['websocket']
    )

def ws_send(msg: str, session_id : str):
    """
    Send a string to a client over the websocket.
    """
    if session_id not in websockets:
        return
    async def do_send():
        await websockets[session_id].send_text(msg)
    asyncio.run(do_send())
