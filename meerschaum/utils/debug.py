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
    from meerschaum.utils.formatting import CHARSET, ANSI, colored
    from meerschaum.config import config as cf
    parent_globals = inspect.stack()[1][0].f_globals
    parent_package = parent_globals['__name__']
    msg = str(msg)
    premsg = ""
    if package:
        premsg = parent_package + ':\n'
    if leader:
        debug_leader = cf['system']['debug'][CHARSET]['leader']
        premsg = debug_leader + ' ' + premsg
    if ANSI:
        if color is not None:
            if isinstance(color, str):
                color = [color]
        else:
            color = cf['system']['debug']['ansi']['color']
        premsg = colored(premsg, *color)
    log.warning(premsg + msg, **kw)
