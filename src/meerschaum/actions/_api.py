#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Start the Meerschaum WebAPI with the `api` action.
"""

def api(debug=False, **kw):
    """
    Run the Meerschaum WebAPI
    """
    from meerschaum.api import flask_app, gunicorn_app, port
    if debug:
        flask_app.run(port=port)
    else:
        gunicorn_app(flask_app)
