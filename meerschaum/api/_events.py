#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Declare FastAPI events in this module (startup, shutdown, etc.).
"""

import sys, os, time
from meerschaum.api import app, get_api_connector, get_uvicorn_config, debug, uvicorn_config_path
from meerschaum.utils.debug import dprint
from meerschaum.connectors.poll import retry_connect
from meerschaum.utils.threading import Thread
from meerschaum.utils.warnings import warn

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

    ### Terminate any running jobs left over.
    if 'meerschaum.api.dash' in sys.modules:
        from meerschaum.api.dash.actions import running_jobs, stop_action
        from meerschaum.utils.packages import run_python_package
        from meerschaum.config.static import STATIC_CONFIG
        from meerschaum._internal.term.tools import is_webterm_running
        run_python_package(
            'meerschaum', [
                'delete', 'job',
                STATIC_CONFIG['api']['webterm_job_name'], '-y'
            ],
            venv = None,
            foreground = False,
            capture_output = True,
        )
        for session_id in running_jobs:
            stop_action({'session-store.data': {'session-id': session_id}})
