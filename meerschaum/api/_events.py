#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Declare FastAPI events in this module (startup, shutdown, etc)
"""

from meerschaum.api import fast_api, database
from meerschaum.utils.debug import dprint

@fast_api.on_event("startup")
async def startup():
    async def connect(
        max_retries : int = 40,
        retry_wait : int = 3,
        debug : bool = False
    ):
        import time
        retries = 0
        while retries < max_retries:
            if debug:
                dprint(f"Trying to connect to the database")
                dprint(f"Attempt ({retries + 1} / {max_retries})")
            try:
                await database.connect()
            except Exception as e:
                print(f"Connection failed. Retrying in {retry_wait} seconds...")
                time.sleep(retry_wait)
                retries += 1
            else:
                if debug: dprint("Connection established!")
                break
    await connect(debug=True)

@fast_api.on_event("shutdown")
async def startup():
    print("Closing database connection...")
    await database.disconnect()


