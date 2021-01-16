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

@app.get("/", response_class=HTMLResponse)
def index(request : Request):
    """
    Meerschaum WebAPI index page
    """
    return templates.TemplateResponse("index.html", {"request" : request})
