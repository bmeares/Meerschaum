#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Reload the meerschaum library (mostly used in the interactive shell for development)
"""

def reload(
        action : list = [''],
        debug : bool = False,
        **kw
    ):
    """
    Reload the meerschaum library
    """
    import imp
    import importlib, meerschaum
    print(importlib.reload(meerschaum))
    #  meerschaum = importlib.reload(meerschaum)
    #  imp.reload(meerschaum)

    return (True, "Success")
