#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Declare FastAPI events in this module (startup, shutdown, etc)
"""

from meerschaum.api import fast_api, database

@fast_api.on_event("startup")
async def startup():
    await database.connect()

@fast_api.on_event("shutdown")
async def startup():
    print("Closing database connection...")
    await database.disconnect()


