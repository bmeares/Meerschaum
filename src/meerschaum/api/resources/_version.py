#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Return version information
"""

from flask_restful import Resource

class Version(Resource):
    urls = ['/meerschaum/version', '/mrsm/version']
    def get(self):
        from meerschaum import __version__ as version
        return {"Meerschaum version" : version}

