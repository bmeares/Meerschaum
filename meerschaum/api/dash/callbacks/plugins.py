#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the plugins page.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional
from meerschaum.api.dash import dash_app, debug
from meerschaum.utils.packages import attempt_import
dash = attempt_import('dash', lazy=False)
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State

@dash_app.callback(
    Output('plugins-cards-div', 'children'),
    Input('search-plugins-input', 'value'),
)
def search_plugins(text: Optional[str] = None):
    from meerschaum.api.dash.pages.plugins import build_cards_div
    return build_cards_div(search_term=text)
