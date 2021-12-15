#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Declare FastAPI events in this module (startup, shutdown, etc.).
"""

import sys, os, time
from meerschaum.api import app, get_api_connector, get_uvicorn_config, debug, uvicorn_config_path
from meerschaum.utils.debug import dprint
from meerschaum.utils.misc import retry_connect
from meerschaum.utils.threading import Thread

@app.on_event("startup")
async def startup():
    conn = get_api_connector()
    try:
        connected = retry_connect(
            get_api_connector(),
            workers = get_uvicorn_config().get('workers', None),
            debug = debug
        )
    except Exception as e:
        connected = False
    if not connected:
        await shutdown()
        os._exit(1)

@app.on_event("shutdown")
async def shutdown():
    if debug:
        dprint("Closing connection...")
    if get_api_connector().type == 'sql':
        get_api_connector().engine.dispose()

