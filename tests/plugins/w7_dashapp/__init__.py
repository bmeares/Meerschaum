#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synthetic wave-7 console test plugin (legacy Dash, design §8.4): a plugin-owned Dash app with a
`@dash_plugin` callback and a `@web_page` Dash layout that reads the core `session-store`.

Inert unless `MRSM_W7_CONSOLE=1`. Used by `tests/console/`. All data is synthetic.
"""

import os

__version__ = '0.0.1'
required = []

if os.environ.get('MRSM_W7_CONSOLE', '').lower() not in ('', '0', 'false'):
    from meerschaum.plugins import dash_plugin, web_page

    @dash_plugin
    def w7_dash_callbacks(dash_app):
        from dash import Input, Output, State

        @dash_app.callback(
            Output('w7-out', 'children'),
            Input('w7-btn', 'n_clicks'),
            prevent_initial_call=True,
        )
        def _clicked(n_clicks):
            return f"clicked {n_clicks}"

        @dash_app.callback(
            Output('w7-session', 'children'),
            Input('session-store', 'data'),
        )
        def _session(data):
            return 'session: active' if (data or {}).get('session-id') else 'session: none'

    @web_page('w7-dash', login_required=True, page_group='W7 Dash')
    def w7_dash_page():
        from dash import html
        return html.Div([
            html.H2('W7 Dash page', id='w7-dash-title'),
            html.Button('Click', id='w7-btn'),
            html.Div(id='w7-out'),
            html.Div(id='w7-session'),
        ])
