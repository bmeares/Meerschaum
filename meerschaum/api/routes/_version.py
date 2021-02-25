#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Return version information
"""

from meerschaum.api import app, endpoints

@app.get(endpoints['version'])
def get_api_version():
    """
    Get the Meerschaum API version
    """
    from meerschaum.api import __version__ as version
    return version

@app.get(endpoints['version'] + "/mrsm")
def get_meerschaum_version():
    """
    Get the Meerschaum instance version
    """
    from meerschaum import __version__ as version
    return version
