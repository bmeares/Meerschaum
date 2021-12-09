#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Build the web console virtual terminal using Tornado and xterm.js.
"""

from __future__ import annotations
import os
import platform
import sys
from meerschaum.utils.packages import attempt_import
from meerschaum._internal.term.TermPageHandler import TermPageHandler
from meerschaum.config._paths import API_TEMPLATES_PATH

tornado, tornado_ioloop, terminado, tornado_xstatic = attempt_import(
    'tornado', 'tornado.ioloop', 'terminado', 'tornado_xstatic', lazy=False, venv=None,
)
try:
    from xstatic.pkg import termjs
except ImportError:
    from meerschaum.utils.packages import pip_install
    if not pip_install('XStatic-term.js', venv=None):
        raise ImportError("Failed to install and import Xterm.js.")
    from xstatic.pkg import termjs

STATIC_DIR = os.path.join(os.path.dirname(terminado.__file__), "_static")

commands = [sys.executable, '-m', 'meerschaum']

term_manager = terminado.UniqueTermManager(shell_command=commands)
handlers = [
    (
        r"/websocket",
        terminado.TermSocket,
        {'term_manager': term_manager}
    ),
    (
        r"/", 
        TermPageHandler
    ),
    (
        r"/xstatic/(.*)",
        tornado_xstatic.XStaticFileHandler,
        {'allowed_modules': ['termjs']}
    ),
]
tornado_app = tornado.web.Application(
    handlers,
    static_path = STATIC_DIR,
    template_path = API_TEMPLATES_PATH,
    xstatic_url = tornado_xstatic.url_maker('/xstatic/')
)

