#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Return version information
"""

import fastapi
from meerschaum.api import app, endpoints, private, manager
from meerschaum.utils.typing import Union

@app.get(endpoints['version'], tags=['Version'])
def get_api_version(
        curr_user = (
            fastapi.Depends(manager) if private else None
        ),
    ):
    """
    Get the Meerschaum API version.
    """
    from meerschaum.api import __version__ as version
    return version

@app.get(endpoints['version'] + "/mrsm", tags=['Version'])
def get_meerschaum_version(
        curr_user = (
            fastapi.Depends(manager) if private else None
        ),
    ):
    """
    Get the Meerschaum instance version.
    """
    from meerschaum import __version__ as version
    return version

