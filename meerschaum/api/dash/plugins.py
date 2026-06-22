#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with plugins via the web interface.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Tuple, SuccessTuple, Optional, WebState, Dict, Any
from meerschaum.utils.packages import import_dcc, import_html
from meerschaum.api import get_api_connector, endpoints, CHECK_UPDATE
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.core import Plugin
from meerschaum.api.dash import dash_app, debug
from meerschaum.api.dash.sessions import get_username_from_session
from meerschaum.api.dash.components import build_cards_grid

PLUGINS_PER_PAGE: int = 12
PLUGINS_GRID_COLUMNS: int = 3


def _build_plugin_card(
    plugin_name: str,
    session_data: Dict[str, Any],
    *,
    detail: bool = False,
) -> dbc.Card:
    """
    Build a single plugin card. If ``detail`` is True, render the larger,
    self-contained plugin detail view with an install snippet.
    """
    conn = get_api_connector()
    plugin = Plugin(plugin_name)
    attrs = conn.get_plugin_attributes(plugin, debug=debug) or {}
    desc = attrs.get('description', 'No description provided.')
    plugin_username = conn.get_plugin_username(plugin, debug=debug)
    plugin_version = (conn.get_plugin_version(plugin, debug=debug) or '').strip()

    owner = is_plugin_owner(plugin_name, session_data)
    desc_textarea_kw = {
        'value': desc,
        'readOnly': not owner,
        'debounce': False,
        'className': 'plugin-description',
        'draggable': False,
        'wrap': 'overflow',
        'placeholder': "Edit the plugin's description",
        'id': {'type': 'description-textarea', 'index': plugin_name},
    }
    if detail:
        desc_textarea_kw['style'] = {'min-height': '12em'}

    title_el = (
        html.H3(plugin_name, style={'margin': 0})
        if detail
        else html.H5(plugin_name, style={'margin': 0})
    )
    title_link = html.A(
        title_el,
        href='/dash/plugins/' + plugin_name,
        style={'text-decoration': 'none', 'color': 'inherit'},
    )

    version_badge = html.Span(
        plugin_version or '—',
        className='plugin-version-badge',
    )

    card_header = dbc.CardHeader(
        dbc.Row(
            [
                dbc.Col(title_link),
                dbc.Col(
                    version_badge,
                    width='auto',
                    style={'text-align': 'right'},
                ),
            ],
            justify='between',
            align='center',
            className='g-2',
        ),
    )

    body_children: List[Any] = [
        html.Div(
            [
                html.Span('👤 ', className='text-muted'),
                html.A(
                    str(plugin_username),
                    href='/dash/users/' + str(plugin_username),
                    className='text-muted',
                ),
            ],
            className='plugin-author',
        ),
        dbc.Textarea(**desc_textarea_kw),
    ]
    if owner:
        body_children += [
            dbc.Button(
                'Update description',
                size='sm',
                color='link',
                id={'type': 'edit-button', 'index': plugin_name},
                style={'padding-left': 0},
            ),
            html.Div(id={'type': 'edit-alert-div', 'index': plugin_name}),
        ]

    if detail:
        install_cmd = f"mrsm install plugin {plugin_name}"
        body_children += [
            html.Hr(),
            html.H5('Install', style={'margin-bottom': '0.5em'}),
            html.P(
                [
                    'Run the following command in a shell with Meerschaum installed. ',
                    'Add ',
                    html.Code('-r api:<label>'),
                    ' to target this repository.',
                ],
                className='text-muted',
                style={'margin-bottom': '0.75em'},
            ),
            dcc.Markdown(
                f"```bash\n{install_cmd}\n```",
                style={'margin-bottom': 0},
            ),
        ]

    download_link = html.A(
        '⬇️ Download',
        href=(endpoints['plugins'] + '/' + plugin_name),
        className='plugin-card-link',
    )
    if detail:
        footer = dbc.CardFooter(download_link)
    else:
        footer = dbc.CardFooter(
            html.Div(
                [
                    download_link,
                    html.A(
                        '🔗 Share',
                        href='/dash/plugins/' + plugin_name,
                        className='plugin-card-link',
                    ),
                ],
                className='plugin-card-footer-row',
            ),
        )

    return dbc.Card(
        [
            card_header,
            dbc.CardBody(body_children),
            footer,
        ],
        id=plugin_name + '_card',
        className='plugin-card' + (' plugin-card-detail' if detail else ''),
    )


def get_plugins_cards(
    state: Optional[WebState] = None,
    search_term: Optional[str] = None,
    session_data: Optional[Dict[str, Any]] = None,
    page: int = 1,
    per_page: int = PLUGINS_PER_PAGE,
) -> Tuple[List[dbc.Card], List[SuccessTuple], int, int]:
    """
    Return the cards, alerts, total page count, and total plugin count
    for the plugins listing.
    """
    alerts: List[SuccessTuple] = []
    if session_data is None:
        session_data = {}

    all_names = sorted(get_api_connector().get_plugins(search_term=search_term) or [])
    total = len(all_names)
    if per_page <= 0:
        per_page = PLUGINS_PER_PAGE
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    page_names = all_names[start:start + per_page]

    cards = [_build_plugin_card(name, session_data) for name in page_names]
    return cards, alerts, total_pages, total


def build_plugins_listing(
    search_term: Optional[str],
    session_data: Optional[Dict[str, Any]],
    page: int = 1,
) -> Tuple[Any, int, int]:
    """
    Build the plugins listing body (summary + cards grid) and return it
    along with the total page count and total plugin count. The
    ``plugins-pagination`` control is rendered persistently by the page layout.
    """
    cards, _alerts, total_pages, total = get_plugins_cards(
        search_term=search_term,
        session_data=session_data,
        page=page,
    )

    if not cards and not search_term:
        return _build_empty_plugins_placeholder(), total_pages, total

    if not cards:
        body = html.Div(
            [
                html.H4(
                    f"No plugins match '{search_term}'.",
                    className='text-muted',
                    style={'text-align': 'center'},
                ),
                html.P(
                    'Try a different search term or clear the search to browse all plugins.',
                    className='text-muted',
                    style={'text-align': 'center'},
                ),
            ],
            className='plugins-empty-state',
        )
    else:
        body = build_cards_grid(cards, num_columns=PLUGINS_GRID_COLUMNS)

    summary_text = (
        f"Showing page {page} of {total_pages} ({total} plugin"
        + ('s' if total != 1 else '')
        + ')'
    )

    content = html.Div([
        html.P(summary_text, className='text-muted', style={'text-align': 'center'}),
        body,
    ])
    return content, total_pages, total


def _build_empty_plugins_placeholder() -> Any:
    """
    Friendly empty-state shown when the instance has zero plugins published.
    """
    return html.Div(
        [
            html.Div('🧩', className='plugins-empty-icon'),
            html.H3('No plugins published yet', style={'margin-bottom': '0.5em'}),
            html.P(
                [
                    'This Meerschaum instance does not host any plugins. ',
                    'Plugins extend Meerschaum with custom connectors, actions, '
                    'and dashboard pages.',
                ],
                className='text-muted',
                style={'margin-bottom': '1.25em'},
            ),
            html.H6('Publish a plugin from your shell:'),
            dcc.Markdown(
                "```bash\nmrsm register plugin <name> -r api:<label>\n```",
                style={'margin-bottom': '1em'},
            ),
            html.P(
                [
                    'See the ',
                    html.A(
                        'plugins documentation',
                        href='https://meerschaum.io/reference/plugins/',
                        rel='noreferrer noopener',
                        target='_blank',
                    ),
                    ' to learn how to build and publish a plugin.',
                ],
                className='text-muted',
            ),
        ],
        className='plugins-empty-state',
    )


def build_plugin_detail(
    plugin_name: str,
    session_data: Optional[Dict[str, Any]],
) -> Any:
    """
    Build the detail view for a single plugin (shareable URL).
    """
    if session_data is None:
        session_data = {}

    back_button = dbc.Button(
        '← Back to plugins',
        href='/dash/plugins',
        color='link',
        size='lg',
        className='plugin-back-button',
    )

    existing = get_api_connector().get_plugins(search_term=plugin_name) or []
    if plugin_name not in existing:
        return html.Div([
            back_button,
            html.Br(),
            html.Br(),
            html.H3(f"404: Plugin '{plugin_name}' not found."),
        ], className='plugin-detail-wrapper')

    return html.Div([
        back_button,
        html.Br(),
        html.Br(),
        _build_plugin_card(plugin_name, session_data, detail=True),
        html.Br(),
    ], className='plugin-detail-wrapper')


def is_plugin_owner(plugin_name: str, session_data: Dict['str', Any]) -> bool:
    """
    Check whether the currently logged in user is the owner of a plugin.
    """
    plugin = Plugin(plugin_name)
    session_id = (session_data or {}).get('session-id', None)
    if session_id is None:
        return False
    _username = get_username_from_session(session_id)
    _plugin_username = get_api_connector().get_plugin_username(plugin, debug=debug)
    return (
        _username is not None
        and _username == _plugin_username
    )
