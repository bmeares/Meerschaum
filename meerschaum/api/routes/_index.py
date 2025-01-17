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
RedirectResponse = starlette.responses.RedirectResponse

INDEX_REDIRECT_URL: str = (
    endpoints['dash']
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
