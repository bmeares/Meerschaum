#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Default route
"""

from meerschaum.api import fast_api

@fast_api.get("/")
def index():
    """
    Meerschaum WebAPI index page
    """
    from meerschaum import __doc__ as doc
    return doc
