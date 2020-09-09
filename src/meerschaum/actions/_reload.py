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
    import sys
    from meerschaum.utils.misc import reload_package
    reload_package(sys.modules[f'meerschaum'], debug=debug)
    reload_package(sys.modules[f'meerschaum'], debug=debug)

    return (True, "Success")

#  def deleteme(**kw): pass
