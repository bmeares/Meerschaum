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
        color : str = 'cyan',
        attrs : list = [],
    ):
    from meerschaum.utils.formatting import UNICODE, ANSI
    parent_globals = inspect.stack()[1][0].f_globals
    parent_package = parent_globals['__name__']
    msg = str(msg)
    premsg = ""
    if package:
        premsg = parent_package + ':\n'
    if leader:
        debug_leader = "DEBUG:"
        if UNICODE: debug_leader = "🐞"
        premsg = debug_leader + ' ' + premsg
    if ANSI:
        from more_termcolor import colored
        premsg = colored(premsg, color)
    log.warning(premsg + msg)
