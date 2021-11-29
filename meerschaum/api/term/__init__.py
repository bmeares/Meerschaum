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
from meerschaum.api import app as fastapi_app, endpoints
from meerschaum.api.term.TermPageHandler import TermPageHandler
from meerschaum.config._paths import API_TEMPLATES_PATH

tornado, terminado, tornado_xstatic, uvicorn = attempt_import(
    'tornado', 'terminado', 'tornado_xstatic', 'uvicorn', lazy=False,
)
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



#  fastapi_app.add_middleware(tornado_app)
#  fastapi_middleware_wsgi = attempt_import('fastapi.middleware.wsgi')
#  fastapi_app.mount(endpoints['term'], fastapi_middleware_wsgi.WSGIMiddleware(tornado_app))
