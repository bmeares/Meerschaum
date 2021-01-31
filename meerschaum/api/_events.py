#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Declare FastAPI events in this module (startup, shutdown, etc)
"""

from meerschaum.api import app, get_connector
from meerschaum.utils.debug import dprint

@app.on_event("startup")
async def startup():
    from meerschaum.utils.misc import retry_connect
    import sys
    conn = get_connector()
    await retry_connect(get_connector(), debug=True)

@app.on_event("shutdown")
async def shutdown():
    from meerschaum.config._paths import API_UVICORN_CONFIG_PATH
    try:
        dprint(f"Removing Uvicorn configuration ({API_UVICORN_CONFIG_PATH})")
        os.remove(API_UVICORN_CONFIG_PATH)
    except:
        pass
    dprint("Closing database connection...")
    await get_connector().db.disconnect()


