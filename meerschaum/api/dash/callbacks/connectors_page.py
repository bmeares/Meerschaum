#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Connector filtering, freshly authorized inspection, and connection tests.
"""

from __future__ import annotations

import dash
from dash.dependencies import Input, Output, State, MATCH, ALL
from dash.exceptions import PreventUpdate

from meerschaum.utils.typing import Any, Dict, Optional
from meerschaum.api import CHECK_UPDATE
from meerschaum.api.dash import dash_app
from meerschaum.utils.packages import import_html
html = import_html(check_update=CHECK_UPDATE)


def _connector_row(keys, session, details=False):
    from meerschaum.api.dash.pages.connectors import may_view, may_see_details
    from meerschaum.api.dash.connector_listing import list_connectors
    from meerschaum.api.dash.users import viewer
    sid = session.get('session-id') if isinstance(session,dict) else None
    if not may_view(sid) or (details and not may_see_details(sid)):
        raise PermissionError('Connector details are unavailable.')
    who = viewer(session)
    if not who['active'] or not who['viewer'].can('connectors:read') or (details and not who['admin']):
        raise PermissionError('Connector details are unavailable.')
    rows = list_connectors(viewer=who['viewer'])
    row = next((row for row in rows if row['keys']==keys),None)
    if row is None:
        raise PermissionError('Connector is unavailable.')
    return row,who


@dash_app.callback(
    Output('connector-info-modal','is_open'),
    Output('connector-info-title','children'),
    Output('connector-info-body','children'),
    Output('connector-info-footer','children'),
    Input({'type':'connector-info-button','index':ALL},'n_clicks'),
    Input('connector-info-close','n_clicks'),
    State('session-store','data'),
    prevent_initial_call=True,
)
def connector_info_click(clicks, close, session):
    import dash_bootstrap_components as dbc
    from meerschaum.api.dash.pages.connectors import connector_info
    trigger = dash.ctx.triggered_id
    if trigger=='connector-info-close':
        if not close:
            raise PreventUpdate
        return False,dash.no_update,dash.no_update,dash.no_update
    if not isinstance(trigger,dict) or trigger.get('type')!='connector-info-button' or not any(clicks or []):
        raise PreventUpdate
    try:
        row,who = _connector_row(trigger.get('index'),session)
        body,footer = connector_info(row,who['admin'],who['admin'])
        return True,html.Code(row['keys']),body,footer
    except Exception:
        return True,'Connector',dbc.Alert('This connector is not available.',color='secondary'),[
            dbc.Button('Close',id='connector-info-close',color='secondary',n_clicks=0)]


@dash_app.callback(
    Output({'type':'connector-inspect-content','index':MATCH},'children'),
    Input({'type':'connector-inspect-button','index':MATCH},'n_clicks'),
    State('session-store','data'),
    prevent_initial_call=True,
)
def inspect_connector_click(clicks, session):
    if not clicks:
        raise PreventUpdate
    from meerschaum.api.dash.pages.connectors import connector_inspection
    trigger = dash.ctx.triggered_id
    try:
        row,_ = _connector_row(trigger.get('index') if isinstance(trigger,dict) else None,session,details=True)
    except Exception:
        return html.P('Connection settings are not available to your account.')
    return connector_inspection(row)


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
