#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Connectors page's callbacks (`pages/connectors.py`): the filter box and the administrators'
Test connection button.
"""

from __future__ import annotations

import dash
from dash.dependencies import Input, Output, State, MATCH
from dash.exceptions import PreventUpdate

from meerschaum.utils.typing import Any, Dict, Optional
from meerschaum.api import CHECK_UPDATE
from meerschaum.api.dash import dash_app
from meerschaum.utils.packages import import_html
html = import_html(check_update=CHECK_UPDATE)


dash_app.clientside_callback(
    """
    function(value){
        const query = (value || '').trim().toLowerCase();
        document.querySelectorAll('#connectors-table tbody tr').forEach(function(row){
            row.hidden = Boolean(query) && !(row.dataset.search || '').includes(query);
        });
        return dash_clientside.no_update;
    }
    """,
    Output('connectors-filter-dummy', 'children'),
    Input('connectors-filter', 'value'),
)


@dash_app.callback(
    Output({'type': 'connector-test-result', 'index': MATCH}, 'children'),
    Input({'type': 'connector-test-button', 'index': MATCH}, 'n_clicks'),
    State('session-store', 'data'),
    running=[
        (Output({'type': 'connector-test-button', 'index': MATCH}, 'disabled'), True, False),
    ],
    prevent_initial_call=True,
)
def test_connection_click(n_clicks: Optional[int], session_data: Optional[Dict[str, Any]]):
    """Test a connector (administrators only) and show the result under its button."""
    if not n_clicks:
        raise PreventUpdate
    from meerschaum.api.dash.pages.connectors import may_test
    session_id = (session_data or {}).get('session-id', None) if isinstance(session_data, dict) else None
    if not may_test(session_id):
        return html.Span(
            'Only administrators can test connections.',
            className='connectors-test-failure',
        )
    triggered = dash.ctx.triggered_id
    keys = triggered.get('index', None) if isinstance(triggered, dict) else None
    from meerschaum.api.dash.connector_listing import list_connectors, test_connector
    known = {row['keys']: row for row in list_connectors()}
    if not keys or keys not in known or not known[keys].get('testable', False):
        return html.Span("This connector cannot be tested.", className='connectors-test-failure')
    result = test_connector(keys)
    if result['success']:
        return html.Span(
            f"Connected in {result['seconds']:.2f} s",
            className='connectors-test-success',
        )
    return html.Span(result['message'], className='connectors-test-failure')
