#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Declare FastAPI events in this module (startup, shutdown, etc)
"""

from meerschaum.api import fast_api, connector
from meerschaum.utils.debug import dprint

@fast_api.on_event("startup")
async def startup():
    from meerschaum.utils.misc import retry_connect
    await retry_connect(connector, debug=True)

@fast_api.on_event("shutdown")
async def startup():
    print("Closing database connection...")
    await connector.db.disconnect()


