#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Default route
"""

from meerschaum.api import flask_app

@flask_app.route("/")
def index():
    """
    Meerschaum WebAPI index page
    """
    from meerschaum import __doc__ as doc
    return doc
