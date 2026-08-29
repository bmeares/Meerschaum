#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Redirect the index path to `/dash` if applicable.
"""

import starlette.responses
from meerschaum.api import (
    app,
    endpoints,
    HTMLResponse,
    Request,
    docs_enabled,
    _include_dash,
)
from meerschaum.api.routes import route_group_is_allowed
RedirectResponse = starlette.responses.RedirectResponse

INDEX_REDIRECT_URL: str = (
    (
        endpoints['dash']
        if route_group_is_allowed('dash')
        else endpoints['dash'] + '/login'
    )
    if _include_dash
    else (
        endpoints['docs']
        if docs_enabled
        else endpoints['openapi']
    )
)


@app.get(endpoints['index'], response_class=HTMLResponse)
def index(request: Request):
    """
    Meerschaum Web API index page.
    """
    return RedirectResponse(url=INDEX_REDIRECT_URL)
