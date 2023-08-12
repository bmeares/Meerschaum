#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Miscellaneous routes
"""

from __future__ import annotations
from meerschaum.utils.typing import Dict, Union, Any

import os
from meerschaum import get_pipes
from meerschaum.api import (
    app, endpoints, check_allow_chaining, SERVER_ID, debug, get_api_connector,
    private, no_auth, manager
)
import fastapi
from starlette.responses import FileResponse, JSONResponse
from meerschaum.connectors.poll import _wrap_retry_connect

@app.get(endpoints['favicon'], tags=['Misc'])
def get_favicon() -> Any:
    """
    Return the favicon file.
    """
    from meerschaum.config._paths import API_STATIC_PATH
    return FileResponse(os.path.join(API_STATIC_PATH, 'ico', 'logo.ico'))


@app.get(endpoints['chaining'], tags=['Misc'])
def get_chaining_status(
        curr_user = (
            fastapi.Depends(manager) if private else None
        ),
    ) -> bool:
    """
    Return whether this API instance may be chained.
    """
    return check_allow_chaining()


@app.get(endpoints['info'], tags=['Misc'])
def get_instance_info(
        curr_user = (
            fastapi.Depends(manager) if private else None
        ),
    ) -> Dict[str, Union[str, int]]:
    """
    Return information about this API instance.
    """
    from meerschaum import __version__ as version
    num_plugins = len(get_api_connector().get_plugins(debug=debug))
    num_users = len(get_api_connector().get_users(debug=debug))
    num_pipes = len(get_pipes(mrsm_instance=get_api_connector(), as_list=True))
    return {
        'version': version,
        'num_plugins': num_plugins,
        'num_users': num_users,
        'num_pipes': num_pipes,
    }


@app.get(endpoints['healthcheck'], tags=['Misc'])
def get_healtheck() -> Dict[str, Any]:
    """
    Return a success message to confirm this API is reachable.
    """
    conn = get_api_connector()
    success = _wrap_retry_connect(
        conn.meta,
        max_retries = 1, 
    )
    message = "Success" if success else "Failed to connect to instance connector."
    status_code = 200 if success else 503
    return JSONResponse({"success": success, "message": message}, status_code=status_code)


if debug:
    @app.get('/id', tags=['Misc'])
    def get_ids(
        curr_user = (
            fastapi.Depends(manager) if private else None
        ),
    ) -> str:
        return {
            'server': SERVER_ID,
            'process': os.getpid(),
        }
