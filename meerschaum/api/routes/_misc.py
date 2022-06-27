#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Miscellaneous routes
"""

from __future__ import annotations
from meerschaum.utils.typing import Dict, Union

import os
from meerschaum import get_pipes
from meerschaum.api import (
    app, endpoints, check_allow_chaining, SERVER_ID, debug, get_api_connector,
    private, no_auth, manager
)
import fastapi
from starlette.responses import FileResponse

@app.get(endpoints['favicon'], tags=['Misc'])
def get_favicon() -> FileResponse:
    from meerschaum.config._paths import API_STATIC_PATH
    return FileResponse(os.path.join(API_STATIC_PATH, 'ico', 'logo.ico'))


@app.get(endpoints['chaining'], tags=['Misc'])
def get_chaining_status(
        curr_user = (
            fastapi.Depends(manager) if private else None
        ),
    ) -> bool:
    return check_allow_chaining()


@app.get(endpoints['info'], tags=['Misc'])
def get_instance_info(
        curr_user = (
            fastapi.Depends(manager) if private else None
        ),
    ) -> Dict[str, str]:
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
