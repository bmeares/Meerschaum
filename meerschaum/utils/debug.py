#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions to handle debug statements
"""

import sys, logging, inspect
logging.basicConfig(format='%(message)s')
log = logging.getLogger(__name__)

def dprint(
        msg : str,
        leader : bool = True,
        package: bool = True,
        color : 'str or list' = None,
        attrs : list = [],
        **kw
    ):
    try:
        from meerschaum.utils.formatting import CHARSET, ANSI, colored
    except ImportError:
        from meerschaum.utils.formatting import colored_fallback
        CHARSET, ANSI, colored = 'ascii', False, colored_fallback
    from meerschaum.config._paths import CONFIG_PATH, PERMANENT_PATCH_PATH
    ### NOTE: We can't import get_config for some reason
    from meerschaum.config import config as cf

    parent_globals = inspect.stack()[1][0].f_globals
    parent_package = parent_globals['__name__']
    msg = str(msg)
    premsg = ""
    if package:
        premsg = parent_package + ':\n'
    if leader:
        try:
            debug_leader = cf['system']['debug'][CHARSET]['icon']
        except KeyError:
            print("Failed to load config. Please delete the following files and restart Meerschaum:")
            for p in [CONFIG_PATH, PERMANENT_PATCH_PATH]:
                print('  - ' + str(p))
            debug_leader = ''
            ### crash if we can't load the leader
            #  sys.exit(1)
        premsg = ' ' + debug_leader + ' ' + premsg
    if ANSI:
        if color is not None:
            if isinstance(color, str):
                color = [color]
        else:
            try:
                color = cf['system']['debug']['ansi']['color']
            except KeyError:
                color = []
        premsg = colored(premsg, *color)
    log.warning(premsg + msg, **kw)
