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
    ):
    from meerschaum.utils.formatting import CHARSET, ANSI, colored
    from meerschaum.config import config as cf, get_config
    parent_globals = inspect.stack()[1][0].f_globals
    parent_package = parent_globals['__name__']
    msg = str(msg)
    premsg = ""
    if package:
        premsg = parent_package + ':\n'
    if leader:
        debug_leader = get_config('system', 'debug', CHARSET, 'leader', patch=True)
        premsg = debug_leader + ' ' + premsg
    if ANSI:
        if color is not None:
            if isinstance(color, str):
                color = [color]
        else:
            color = get_config('system', 'debug', 'ansi', 'color', patch=True)
        premsg = colored(premsg, *color)
    log.warning(premsg + msg)
