#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define callbacks for the `/dash/pipes/` page.
"""

from dash.dependencies import Input, Output
from dash import no_update

import meerschaum as mrsm
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.pipes import build_pipe_card
from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)


@dash_app.callback(
    Output('pipe-output-div', 'children'),
    Input('pipes-location', 'pathname'),
)
def render_page_from_url(pathname):
    if not str(pathname).startswith('/dash/pipes'):
        return no_update

    keys = pathname.replace('/dash/pipes', '').lstrip('/').rstrip('/').split('/')
    if len(keys) not in (2, 3):
        return no_update

    ck = keys[0]
    mk = keys[1]
    lk = keys[2] if len(keys) == 3 else None

    pipe = mrsm.Pipe(ck, mk, lk)
    ### TODO Check if logged in
    return [
        html.Br(),
        build_pipe_card(pipe, authenticated=False),
        html.Br(),
    ]
