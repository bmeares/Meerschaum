#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define callbacks for the `/dash/pipes/` page.
"""

from urllib.parse import parse_qs, quote_plus
from typing import List, Optional, Dict, Any

import dash
from dash.dependencies import Input, Output, State
from dash import no_update
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

import meerschaum as mrsm
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.components import (
    alert_from_success_tuple,
    build_cards_grid,
)
from meerschaum.api.dash.pipes import (
    build_pipe_card,
    build_pipes_dropdown_keys_row,
    build_pipes_tags_dropdown,
    build_pipes_navbar,
)
from meerschaum.api import CHECK_UPDATE, get_api_connector
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.api.dash.sessions import is_session_authenticated
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)


@dash_app.callback(
    Output('pipes-navbar-div', 'children'),
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
        raise PreventUpdate

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

    keys = pathname.replace('/dash/pipes', '').lstrip('/').rstrip('/').split('/')
    instance_connector = mrsm.get_connector(instance)
    viewing_single_pipe = len(keys) in (2, 3)
    if instance_connector is None:
        return (
            build_pipes_navbar(instance, with_instance_select=(not viewing_single_pipe)),
            [
                html.Br(),
                alert_from_success_tuple((False, f"Invalid instance keys '{instance}'.")),
                html.Br(),
            ]
        )

    if not viewing_single_pipe:
        try:
            pipes = mrsm.get_pipes(
                as_list=True,
                connector_keys=connector_keys,
                metric_keys=metric_keys,
                location_keys=location_keys,
                tags=tags,
                instance=instance_connector,
            )
        except Exception as e:
            return (
                build_pipes_navbar(instance, with_instance_select=False),
                [
                    html.Br(),
                    alert_from_success_tuple(
                        (False, f"Failed to get pipes for instance '{instance}':\n{e}")
                    ),
                    html.Br(),
                    dbc.Row(
                        [
                            dbc.Button(
                                "Reload",
                                id='pipes-reload-button',
                                size='lg',
                                href=(
                                    "/dash/pipes"
                                    if pathname.startswith('/dash/pipes/')
                                    else "/dash/pipes/" 
                                )
                            ),
                        ],
                        justify='center',
                        align='center',
                        className='h-50',
                    ),
                ]
            )

        cards = [
            build_pipe_card(pipe, authenticated=authenticated, include_manage=False)
            for pipe in pipes
        ]
        return (
            build_pipes_navbar(instance, with_instance_select=True),
            [
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
        )

    ck = keys[0]
    mk = keys[1]
    lk = keys[2] if len(keys) == 3 else None

    pipe = mrsm.Pipe(ck, mk, lk, instance=instance)
    return (
        build_pipes_navbar(instance, with_instance_select=False),
        [
            html.Br(),
            build_pipe_card(pipe, authenticated=authenticated, include_manage=False),
            html.Br(),
        ]
    )


@dash_app.callback(
    Output('pipes-location', 'search'),
    Input('pipes-connector-keys-dropdown', 'value'),
    Input('pipes-metric-keys-dropdown', 'value'),
    Input('pipes-location-keys-dropdown', 'value'),
    Input('pipes-tags-dropdown', 'value'),
    Input('instance-select', 'value'),
    Input('pipes-clear-all-button', 'n_clicks'),
)
def update_location_on_pipes_filter_change(
    connector_keys: Optional[List[str]],
    metric_keys: Optional[List[str]],
    location_keys: Optional[List[str]],
    tags: Optional[List[str]],
    instance_keys: str,
    clear_all_button_n_clicks: Optional[int],
):
    """
    Update the URL parameters when clicking the dropdowns.
    """
    ctx = dash.callback_context.triggered
    if len(ctx) != 1:
        raise PreventUpdate

    if not any(
        (connector_keys or [])
        + (metric_keys or [])
        + (location_keys or [])
        + (tags or [])
        + ([instance_keys] if instance_keys else [])
    ):
        return ''

    if ctx[0].get('prop_id', None) == 'pipes-clear-all-button.n_clicks':
        connector_keys = []
        metric_keys = []
        location_keys = []
        tags = []

    include_instance_keys = instance_keys and instance_keys != str(get_api_connector())
    search_str = ""
    
    if connector_keys:
        search_str += "connector_keys=" + ','.join((quote_plus(ck) for ck in connector_keys))
        if metric_keys or location_keys or tags or include_instance_keys:
            search_str += '&'

    if metric_keys:
        search_str += "metric_keys=" + ','.join((quote_plus(mk) for mk in metric_keys))
        if location_keys or tags or include_instance_keys:
            search_str += '&'

    if location_keys:
        search_str += "location_keys=" + ','.join((quote_plus(str(lk)) for lk in location_keys))
        if tags or include_instance_keys:
            search_str += '&'

    if tags:
        search_str += "tags=" + ','.join((quote_plus(tag) for tag in tags))
        if include_instance_keys:
            search_str += '&'

    if instance_keys:
        if include_instance_keys:
            search_str += "instance=" + quote_plus(instance_keys)

    return ('?' + search_str) if search_str else ''
