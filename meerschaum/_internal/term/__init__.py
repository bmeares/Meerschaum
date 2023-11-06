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
from typing import List, Tuple
from meerschaum.utils.packages import attempt_import
from meerschaum._internal.term.TermPageHandler import TermPageHandler
from meerschaum.config._paths import API_TEMPLATES_PATH, API_STATIC_PATH

tornado, tornado_ioloop, terminado = attempt_import(
    'tornado', 'tornado.ioloop', 'terminado', lazy=False, venv=None,
)

def get_webterm_app_and_manager() -> Tuple[
        tornado.web.Application,
        terminado.UniqueTermManager, 
    ]:
    """
    Construct the Tornado web app and term manager from the provided sysargs.

    Returns
    -------
    A tuple of the Tornado web application and term manager.
    """
    commands = [
        sys.executable,
        '-c',
        "import os; _ = os.environ.pop('COLUMNS', None); _ = os.environ.pop('LINES', None); "
        "from meerschaum._internal.entry import get_shell; "
        "get_shell([]).cmdloop()"
    ]

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
    ]
    tornado_app = tornado.web.Application(
        handlers,
        static_path = API_STATIC_PATH,
        template_path = API_TEMPLATES_PATH,
    )
    return tornado_app, term_manager
