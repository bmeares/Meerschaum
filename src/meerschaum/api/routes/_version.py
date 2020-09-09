#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Return version information
"""

from meerschaum.api import flask_app

@flask_app.route("/version")
def get_version():
    from meerschaum.api import __version__ as version
    return {'Meerschaum API version' : version}
