#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define callbacks for the `/dash/pipes/` page.
"""

from urllib.parse import parse_qs

from dash.dependencies import Input, Output, State
from dash import no_update

import meerschaum as mrsm
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.pipes import build_pipe_card
from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.api.dash.sessions import is_session_authenticated
from meerschaum.utils.typing import Optional, Dict, Any
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)


@dash_app.callback(
    Output('pipe-output-div', 'children'),
    Input('pipes-location', 'pathname'),
    State('pipes-location', 'search'),
    State('session-store', 'data'),
)
def render_page_from_url(
    pathname: str,
    pipe_search: str,
    session_data: Optional[Dict[str, Any]],
):
    if not str(pathname).startswith('/dash/pipes'):
        return no_update

    session_id = (session_data or {}).get('session-id', None)
    authenticated = is_session_authenticated(str(session_id))

    keys = pathname.replace('/dash/pipes', '').lstrip('/').rstrip('/').split('/')
    if len(keys) not in (2, 3):
        return no_update

    ck = keys[0]
    mk = keys[1]
    lk = keys[2] if len(keys) == 3 else None
    query_params = parse_qs(pipe_search.lstrip('?')) if pipe_search else {}
    instance = query_params.get('instance', [None])[0]

    pipe = mrsm.Pipe(ck, mk, lk, instance=instance)
    return [
        html.Br(),
        build_pipe_card(pipe, authenticated=authenticated, include_manage=False),
        html.Br(),
    ]
