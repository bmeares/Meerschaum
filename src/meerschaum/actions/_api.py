#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Start the Meerschaum WebAPI with the `api` action.
"""

def api(debug=False, **kw):
    from meerschaum.WebAPI import flask_app
    print(flask_app)
