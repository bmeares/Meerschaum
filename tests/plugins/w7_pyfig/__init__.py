#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synthetic wave-7 console test plugin (Python): a page-contract page (design §8.3) embedding a
Plotly figure, an HTMX fragment route, and API-plugin routes that read the console session.

Inert unless `MRSM_W7_CONSOLE=1` (so the default test suites that load `tests/plugins` do not
mount it). Used by `tests/console/`. All data is synthetic.
"""

import os
import json
import html

__version__ = '0.0.1'
required = []

#: The figure both this plugin and the Rust plugin `w7_native_fig` emit, byte for byte.
FIGURE = {
    'data': [{'type': 'bar', 'x': ['a', 'b', 'c'], 'y': [3, 1, 2], 'name': 'w7'}],
    'layout': {'title': {'text': 'W7 figure'}},
}
FIGURE_JSON = json.dumps(FIGURE, separators=(',', ':'))

if os.environ.get('MRSM_W7_CONSOLE', '').lower() not in ('', '0', 'false'):
    from meerschaum.plugins import api_plugin, web_page

    @web_page('w7-figure', login_required=True, page_group='W7 Python')
    def w7_figure_page():
        return (
            '<section id="w7-py-page"><h2>W7 Python page</h2>'
            f"<div id=\"w7-figure\" data-mrsm-figure='{html.escape(FIGURE_JSON, quote=False)}'></div>"
            '<button id="w7-load" hx-get="/w7/fragment" hx-target="#w7-slot">Load fragment</button>'
            '<div id="w7-slot"></div></section>'
        )

    @api_plugin
    def w7_routes(app):
        import fastapi
        from starlette.responses import HTMLResponse

        @app.get('/w7/fragment')
        def w7_fragment():
            return HTMLResponse('<p id="w7-fragment">Fragment from a Python plugin route.</p>')

        @app.get('/w7/session')
        def w7_session(request: fastapi.Request):
            """The username behind the console's `mrsm-session-id` cookie, through the shared store."""
            from meerschaum.api.dash.sessions import (
                get_session_id_from_request,
                is_session_active,
                get_username_from_session,
            )
            session_id = get_session_id_from_request(request)
            if not session_id or not is_session_active(session_id):
                return {'active': False, 'username': None}
            return {'active': True, 'username': get_username_from_session(session_id)}
