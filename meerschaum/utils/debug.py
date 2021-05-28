#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions to handle debug statements
"""

from __future__ import annotations
from meerschaum.utils.typing import Union, Optional, List

def dprint(
        msg : str,
        leader : bool = True,
        package: bool = True,
        color : Optional[Union[str, List[str]]] = None,
        attrs : Optional[List[str]] = None,
        nopretty : bool = False,
        **kw
    ) -> None:
    """
    Print a debug message.
    """
    if attrs is None:
        attrs = []
    if not isinstance(color, bool) and not nopretty:
        try:
            from meerschaum.utils.formatting import CHARSET, ANSI, colored
        except ImportError:
            CHARSET, ANSI, colored = 'ascii', False, None
        from meerschaum.config._paths import CONFIG_DIR_PATH, PERMANENT_PATCH_DIR_PATH
        from meerschaum.config import _config
        cf = _config('formatting')
        _color = color
    else:
        CHARSET, ANSI, colored, _color, cf = 'ascii', False, None, None, None

    import logging, sys, inspect
    logging.basicConfig(format='%(message)s')
    log = logging.getLogger(__name__)

    parent_frame = inspect.stack()[1][0]
    parent_info = inspect.getframeinfo(parent_frame)
    parent_lineno = parent_info.lineno
    parent_globals = parent_frame.f_globals
    parent_package = parent_globals['__name__']
    msg = str(msg)
    premsg = ""
    if package:
        premsg = parent_package + ':' + str(parent_lineno) + '\n'
    if leader and cf is not None:
        try:
            debug_leader = cf['formatting']['debug'][CHARSET]['icon'] if cf is not None else ''
        except KeyError:
            print(
                "Failed to load config. " +
                "Please delete the following directories and restart Meerschaum:"
            )
            for p in [CONFIG_DIR_PATH, PERMANENT_PATCH_DIR_PATH]:
                print('  - ' + str(p))
            debug_leader = ''
            ### crash if we can't load the leader
            #  sys.exit(1)
        premsg = ' ' + debug_leader + ' ' + premsg
    if ANSI:
        if _color is not None:
            if isinstance(_color, str):
                _color = [_color]
        else:
            if cf is not None and not nopretty:
                try:
                    _color = cf['formatting']['debug']['ansi']['rich'] if cf is not None else {}
                except KeyError:
                    _color = {}
            else:
                _color = []
        if colored is not None:
            premsg = colored(premsg, **_color)
    #  log.warning(premsg + msg, **kw)
    print(premsg + msg)
