#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the terminal page handler class.
"""

from __future__ import annotations
from meerschaum.utils.packages import attempt_import
tornado, tornado_xstatic = attempt_import('tornado', 'tornado_xstatic', lazy=False)

class TermPageHandler(tornado.web.RequestHandler):
    def get(self):
        from meerschaum.api import endpoints
        return self.render(
            "termpage.html",
            static = self.static_url,
            xstatic = self.application.settings['xstatic_url'],
            ws_url_path = "/websocket"
        )

