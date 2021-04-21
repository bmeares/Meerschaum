#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Default route
"""

import starlette.responses
from meerschaum.api import (
    app,
    endpoints,
    HTMLResponse,
    Request,
)
from meerschaum.utils.packages import attempt_import
RedirectResponse = starlette.responses.RedirectResponse

@app.get(endpoints['index'], response_class=HTMLResponse)
def index(request : Request):
    """
    Meerschaum WebAPI index page
    """
    dash_endpoint = endpoints['dash']
    return RedirectResponse(url=dash_endpoint)
