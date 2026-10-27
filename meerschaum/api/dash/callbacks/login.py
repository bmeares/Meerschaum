#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the login page.
"""

from __future__ import annotations
import uuid

from meerschaum.api import CHECK_UPDATE, endpoints
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.utils.typing import Optional

dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State

from meerschaum.api.dash import dash_app, debug, pipes, _get_pipes
from meerschaum.api.dash.sessions import set_session, set_session_cookie, public_callback
from meerschaum.api.dash.connectors import get_web_connector
from meerschaum.api.dash._support import authenticate_user
from meerschaum.api.dash.components import alert_from_success_tuple
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)


def safe_next_path(location_href: Optional[str]) -> Optional[str]:
    """
    Return the `next` path of the login page's URL (`/dash/login?next=/dash/pipes`), where the
    browser goes after signing in, if it is a path on this server (`/...`, never `//host` or a
    URL with a scheme). The console sends a browser here when a callback needs a session.
    """
    if not location_href:
        return None
    from urllib.parse import urlsplit, parse_qs
    try:
        query = parse_qs(urlsplit(str(location_href)).query)
    except ValueError:
        return None
    target = (query.get('next', None) or [None])[0]
    if not target or not target.startswith('/') or target.startswith('//'):
        return None
    if '\\' in target or any(ord(char) < 32 for char in target):
        return None
    if urlsplit(target).scheme or urlsplit(target).netloc:
        return None
    return target


@public_callback
@dash_app.callback(
    Output('user-registration-disabled-collapse', 'is_open'),
    Input('show-user-registration-disabled-button', 'n_clicks'),
    State('user-registration-disabled-collapse', 'is_open'),
)
def show_registration_disabled_collapse(n_clicks, is_open):
    """
    Toggle the registration info div.
    """
    if n_clicks:
        return not is_open
    return is_open


@public_callback
@dash_app.callback(
    Output('session-store', 'data'),
    Output('username-input', 'className'),
    Output('mrsm-location', 'pathname'),
    Output('login-alert-div', 'children'),
    Output('location-login', 'href'),
    Input('username-input', 'n_submit'),
    Input('password-input', 'n_submit'),
    Input('login-button', 'n_clicks'),
    State('username-input', 'value'),
    State('password-input', 'value'),
    State('mrsm-location', 'href'),
    State('mrsm-location', 'pathname'),
)
def login_button_click(
    username_submit,
    password_submit,
    n_clicks,
    username,
    password,
    location_href,
    location_pathname,
):
    """
    When the user submits the login form, check the login.
    On successful login, set the session id, then open the page the visitor came for: the `next`
    page of `/dash/login?next=...`, the console from the login page itself, else the page that
    showed the login form stays and renders for the new session.
    """
    form_class = 'form-control'
    ctx = dash.callback_context
    if not username or not password or not ctx.triggered:
        raise PreventUpdate

    next_href = dash.no_update
    if authenticate_user(username, password):
        session_id = str(uuid.uuid4())
        session_data = {
            'session-id': session_id,
            'mrsm-location.href': location_href,
        }
        set_session(session_id, {'username': username})
        ### Mirror the session ID into an `HttpOnly` cookie (scoped to `/`)
        ### so plain HTTP routes outside the `/dash` WSGI mount can read it.
        from flask import request as flask_request
        set_session_cookie(ctx.response, session_id, secure=flask_request.is_secure)
        alerts = []
        next_path = safe_next_path(location_href)
        dash_prefix = endpoints['dash'].rstrip('/')
        if next_path:
            next_href = next_path
        elif str(location_pathname or '').rstrip('/') in (
            dash_prefix + '/login', dash_prefix + '/register',
        ):
            next_href = dash_prefix
    else:
        form_class += ' is-invalid'
        session_data = dash.no_update
        alerts = alert_from_success_tuple((False, "Invalid login."))
    return session_data, form_class, dash.no_update, alerts, next_href
