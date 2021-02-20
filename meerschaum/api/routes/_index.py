#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Default route
"""

from meerschaum.api import (
    app,
    endpoints,
    database,
    connector,
    HTMLResponse,
    Request,
    templates
)
from meerschaum.utils.packages import attempt_import
import starlette.responses
RedirectResponse = starlette.responses.RedirectResponse

@app.get(endpoints['index'], response_class=HTMLResponse)
def index(request : Request):
    """
    Meerschaum WebAPI index page
    """
    return RedirectResponse(url='/docs')
    #  return templates.TemplateResponse("index.html", {"request" : request})
