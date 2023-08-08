#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Declare FastAPI events in this module (startup, shutdown, etc.).
"""

import sys, os, time
from meerschaum.api import (
    app,
    get_api_connector,
    get_uvicorn_config,
    debug,
    no_dash,
    uvicorn_config_path,
)
from meerschaum.utils.debug import dprint
from meerschaum.connectors.poll import retry_connect
from meerschaum.utils.warnings import warn
from meerschaum._internal.term.tools import is_webterm_running

@app.on_event("startup")
async def startup():
    conn = get_api_connector()
    try:
        if not no_dash:
            from meerschaum.api.dash.webterm import start_webterm
            start_webterm()
        connected = retry_connect(
            get_api_connector(),
            workers = get_uvicorn_config().get('workers', None),
            debug = debug
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
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

    ### Terminate any running jobs left over.
    if 'meerschaum.api.dash' in sys.modules:
        from meerschaum.api.dash.actions import running_jobs, stop_action
        from meerschaum.api.dash.webterm import stop_webterm
        stop_webterm()
        for session_id in running_jobs:
            stop_action({'session-store.data': {'session-id': session_id}})
