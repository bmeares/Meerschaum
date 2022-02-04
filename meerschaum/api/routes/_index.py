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
    version,
    _include_dash,
)
from meerschaum.utils.packages import attempt_import
RedirectResponse = starlette.responses.RedirectResponse

@app.get(endpoints['index'], response_class=HTMLResponse)
def index(request : Request):
    """
    Meerschaum WebAPI index page.
    """
    _url = endpoints['dash'] if _include_dash else '/docs'
    return RedirectResponse(url=_url)
