#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Routes which serve HTML
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


@fast_api.get("/secret", response_class=HTMLResponse)
async def secret(request : Request):
    return templates.TemplateResponse("secret.html", {"request" : request})
