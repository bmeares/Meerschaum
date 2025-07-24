#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define callbacks for the `/dash/pipes/` page.
"""

from urllib.parse import parse_qs

from dash.dependencies import Input, Output, State
from dash import no_update
from dash.exceptions import PreventUpdate

import meerschaum as mrsm
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.components import alert_from_success_tuple, build_cards_grid
from meerschaum.api.dash.pipes import (
    build_pipe_card,
    build_pipes_dropdown_keys_row,
    build_pipes_tags_dropdown,
)
from meerschaum.api import CHECK_UPDATE, get_api_connector
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
def render_pipe_page_from_url(
    pathname: str,
    pipe_search: str,
    session_data: Optional[Dict[str, Any]],
):
    if not str(pathname).startswith('/dash/pipes'):
        return no_update

    session_id = (session_data or {}).get('session-id', None)
    authenticated = is_session_authenticated(str(session_id))
    query_params = parse_qs(pipe_search.lstrip('?')) if pipe_search else {}
    instance = query_params.get('instance', [None])[0] or str(get_api_connector())
    tags = query_params.get('tags', [None])[0] or []
    if isinstance(tags, str):
        tags = tags.split(',')

    connector_keys = query_params.get('connector_keys', [None])[0] or []
    if isinstance(connector_keys, str):
        connector_keys = connector_keys.split(',')

    metric_keys = query_params.get('metric_keys', [None])[0] or []
    if isinstance(metric_keys, str):
        metric_keys = metric_keys.split(',')

    location_keys = query_params.get('location_keys', [None])[0] or []
    if isinstance(location_keys, str):
        location_keys = location_keys.split(',')

    instance_connector = mrsm.get_connector(instance)
    if instance_connector is None:
        return [
            html.Br(),
            alert_from_success_tuple((False, f"Invalid instance keys '{instance}'.")),
            html.Br(),
        ]

    keys = pathname.replace('/dash/pipes', '').lstrip('/').rstrip('/').split('/')
    if len(keys) not in (2, 3):
        pipes = mrsm.get_pipes(
            as_list=True,
            connector_keys=connector_keys,
            metric_keys=metric_keys,
            location_keys=location_keys,
            tags=tags,
            instance=instance_connector,
        )
        cards = [
            build_pipe_card(pipe, authenticated=authenticated, include_manage=False)
            for pipe in pipes
        ]
        return [
            html.Div([
                html.Br(),
                build_pipes_dropdown_keys_row(
                    connector_keys,
                    metric_keys,
                    location_keys,
                    tags,
                    pipes,
                    instance_connector,
                ),
                html.Br(),
                build_pipes_tags_dropdown(
                    connector_keys,
                    metric_keys,
                    location_keys,
                    tags,
                    instance,
                ),
            ]),
            html.Br(),
            build_cards_grid(cards, 1),
            html.Br(),
        ]

    ck = keys[0]
    mk = keys[1]
    lk = keys[2] if len(keys) == 3 else None

    pipe = mrsm.Pipe(ck, mk, lk, instance=instance)
    return [
        html.Br(),
        build_pipe_card(pipe, authenticated=authenticated, include_manage=False),
        html.Br(),
    ]


@dash_app.callback(
    Output('pipes-location', 'search'),
    Input('pipes-connector-keys-dropdown', 'value'),
    Input('pipes-metric-keys-dropdown', 'value'),
    Input('pipes-location-keys-dropdown', 'value'),
    Input('pipes-tags-dropdown', 'value'),
)
def update_location_on_pipes_filter_change(connector_keys, metric_keys, location_keys, tags):
    """
    Update the URL parameters when clicking the dropdowns.
    """
    if not any((connector_keys or []) + (metric_keys or []) + (location_keys or []) + (tags or [])):
        return ''

    search_str = "?"
    
    if connector_keys:
        search_str += "connector_keys=" + ','.join(connector_keys)
        if metric_keys or location_keys or tags:
            search_str += '&'

    if metric_keys:
        search_str += "metric_keys=" + ','.join(metric_keys)
        if location_keys or tags:
            search_str += '&'

    if location_keys:
        search_str += "location_keys=" + ','.join(location_keys)
        if tags:
            search_str += '&'

    if tags:
        search_str += "tags=" + ','.join(tags)

    return search_str
