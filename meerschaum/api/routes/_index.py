#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Default route
"""

from meerschaum.api import (
    fast_api,
    endpoints,
    database,
    connector,
    HTMLResponse,
    Request,
    templates
)

@fast_api.get("/", response_class=HTMLResponse)
def index(request : Request):
    """
    Meerschaum WebAPI index page
    """
    return templates.TemplateResponse("index.html", {"request" : request})
