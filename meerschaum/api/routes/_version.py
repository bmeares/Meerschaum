#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Return version information
"""

from meerschaum.api import fast_api

@fast_api.get("/version")
def get_api_version():
    """
    Get the Meerschaum API version
    """
    from meerschaum.api import __version__ as version
    return { 'Meerschaum API version' : version }

@fast_api.get("/mrsm/version")
def get_meerschaum_version():
    """
    Get the Meerschaum instance version
    """
    from meerschaum import __version__ as version
    return { 'Meerschaum version' : version }
