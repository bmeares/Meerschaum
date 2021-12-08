#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Miscellaneous routes
"""

import os
from meerschaum.api import app, endpoints, check_allow_chaining, SERVER_ID, debug
from starlette.responses import FileResponse

@app.get(endpoints['favicon'], tags=['Misc'])
def get_favicon() -> FileResponse:
    from meerschaum.config._paths import API_STATIC_PATH
    return FileResponse(os.path.join(API_STATIC_PATH, 'ico', 'logo.ico'))

@app.get(endpoints['chaining'], tags=['Misc'])
def get_chaining_status() -> bool:
    """
    Return whether this API instance allows chaining.
    """
    return check_allow_chaining()

if debug:
    @app.get('/id', tags=['Misc'])
    def get_ids() -> str:
        """
        Return the server and process IDs.
        """
        return {
            'server': SERVER_ID,
            'process': os.getpid(),
        }
