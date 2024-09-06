#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Declare FastAPI events in this module (startup, shutdown, etc.).
"""

import sys
import os
import time
from meerschaum.api import (
    app,
    get_api_connector,
    get_cache_connector,
    get_uvicorn_config,
    debug,
    no_dash,
)
from meerschaum.utils.debug import dprint
from meerschaum.connectors.poll import retry_connect
from meerschaum.utils.warnings import warn
from meerschaum.jobs import (
    get_jobs,
    start_check_jobs_thread,
    stop_check_jobs_thread,
)
from meerschaum.config.static import STATIC_CONFIG

TEMP_PREFIX: str = STATIC_CONFIG['api']['jobs']['temp_prefix']


@app.on_event("startup")
async def startup():
    """
    Connect to the instance database and begin monitoring jobs.
    """
    try:
        if not no_dash:
            from meerschaum.api.dash.webterm import start_webterm
            start_webterm()

        connected = retry_connect(
            get_api_connector(),
            workers=get_uvicorn_config().get('workers', None),
            debug=debug
        )
        cache_connector = get_cache_connector()
        if cache_connector is not None:
            connected = retry_connect(
                cache_connector,
                workers=get_uvicorn_config().get('workers', None),
                debug=debug,
            )
    except Exception:
        import traceback
        traceback.print_exc()
        connected = False

    if not connected:
        await shutdown()
        os._exit(1)

    start_check_jobs_thread()


@app.on_event("shutdown")
async def shutdown():
    """
    Close the database connection and stop monitoring jobs.
    """
    if debug:
        dprint("Closing connection...")
    if get_api_connector().type == 'sql':
        get_api_connector().engine.dispose()

    stop_check_jobs_thread()

    temp_jobs = {
        name: job
        for name, job in get_jobs(include_hidden=True).items()
        if name.startswith(TEMP_PREFIX)
    }
    for job in temp_jobs.values():
        job.delete()

    ### Terminate any running jobs left over.
    if 'meerschaum.api.dash' in sys.modules:
        from meerschaum.api.dash.webterm import stop_webterm
        stop_webterm()
