#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the terminal page handler class.
"""

from __future__ import annotations
from meerschaum.utils.packages import attempt_import
tornado_web = attempt_import('tornado.web', lazy=False)

class TermPageHandler(tornado_web.RequestHandler):
    def get(self):
        return self.render(
            "termpage.html",
            static=self.static_url,
            ws_url_path="/websocket",
        )

