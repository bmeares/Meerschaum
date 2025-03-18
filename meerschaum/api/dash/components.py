#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Custom components are defined here.
"""

from __future__ import annotations

from meerschaum.utils.packages import attempt_import, import_dcc, import_html
from meerschaum.utils.typing import SuccessTuple, List
from meerschaum.config.static import STATIC_CONFIG
from meerschaum.utils.misc import remove_ansi
from meerschaum._internal.shell.Shell import get_shell_intro
from meerschaum.api import endpoints, CHECK_UPDATE, docs_enabled
from meerschaum.connectors import instance_types, _load_builtin_custom_connectors
from meerschaum.utils.misc import get_connector_labels
from meerschaum.config import __doc__ as doc
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
dex = attempt_import('dash_extensions', lazy=False, check_update=CHECK_UPDATE)
dash_ace = attempt_import('dash_ace', lazy=False, check_update=CHECK_UPDATE)
_load_builtin_custom_connectors()

go_button = dbc.Button('Exec', id='go-button', color='primary', style={'width': '100%'})
test_button = dbc.Button('Test', id='test-button', color='danger', style={'display': 'none'})
get_items_menu = dbc.DropdownMenu(
    label='More', id='get-items-menu', children=[
        dbc.DropdownMenuItem("Plugins", id='get-plugins-button'),
        dbc.DropdownMenuItem("Users", id='get-users-button'),
        dbc.DropdownMenuItem("Graphs", id='get-graphs-button'),
    ],
    style={'width': '100%', 'font-size': '0.5em'},
    menu_variant='dark',
    toggle_style={'width': '100%'},
    color='secondary',
    size='sm',
)
show_pipes_button = dbc.Button(
    'Pipes',
    id='get-pipes-button',
    color='info',
    style={'width': '100%'},
)
show_jobs_button = dbc.Button(
    'Jobs',
    id='get-jobs-button',
    color='success',
    style={'width': '100%'},
)
cancel_button = dbc.Button(
    'Term',
    id='cancel-button',
    color='dark',
    style={'width': '100%', 'background-color': 'black', 'display': 'none'},
)
show_webterm_button = dbc.Button(
    'Term',
    id='show-webterm-button',
    color='dark',
    style={'width': '100%', 'background-color': 'black'},
)
bottom_buttons_content = dbc.Card(
    dbc.CardBody(
        dbc.Row([
            dbc.Col(go_button, xl=2, lg=3, md=3, sm=12),
            dbc.Col(show_pipes_button, xl=2, lg=3, md=3, sm=12),
            dbc.Col(show_jobs_button, xl=2, lg=3, md=3, sm=12),
            dbc.Col(show_webterm_button, xl=2, lg=3, md=3, sm=12),
            dbc.Col(lg=True, md=False, sm=False),
            dbc.Col(get_items_menu, xl=2, lg=12, md=12, sm=12),
        ])
    )
)
console_div = html.Div(
    id='console-div',
    children=[html.Pre(get_shell_intro(), id='console-pre')],
)

location = dcc.Location(id='mrsm-location', refresh=False)

search_parameters_editor = dash_ace.DashAceEditor(
    id='search-parameters-editor',
    theme='monokai',
    mode='json',
    tabSize=2,
    placeholder=(
        'Additional search parameters. ' +
        'Simple dictionary format or JSON accepted.'
    ),
    style={'height': 100},
)

pages_offcanvas = dbc.Offcanvas(
    title='',
    id='pages-offcanvas',
)

download_dataframe = dcc.Download(id='download-dataframe-csv')
download_logs = dcc.Download(id='download-logs')

instance_select = dbc.Select(
    id='instance-select',
    size='sm',
    options=[
        {'label': i, 'value': i}
        for i in get_connector_labels(*instance_types)
    ],
    class_name='dbc_dark custom-select custom-select-sm',
)

sign_out_button = dbc.Button(
    "Sign out",
    color='link',
    style={'margin-left': '30px'},
    id='sign-out-button',
)

logo_row = dbc.Row(
    [
        dbc.Col(
            html.Img(
                src=endpoints['dash'] + "/assets/logo_48x48.png",
                title=doc,
                id="logo-img",
                style={'cursor': 'pointer'},
            ),
        ),
    ],
    align='center',
    className='g-0 navbar-logo-row',
)

pages_navbar = html.Div(
    [
        pages_offcanvas,
        dbc.Navbar(
            dbc.Container(
                [
                    logo_row,
                    dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
                    dbc.Collapse(
                        dbc.Row(
                            [
                                dbc.Col(
                                    sign_out_button,
                                    className="ms-auto",
                                ),
                            ],
                            className="g-0 ms-auto flex-nowrap mt-3 mt-md-0",
                        ),
                        id='navbar-collapse',
                        is_open=False,
                        navbar=True,
                    ),
                ]
            ),
            dark=True,
            color='dark'
        ),
    ],
    id='pages-navbar-div',
)

navbar = dbc.Navbar(
    dbc.Container(
        [
            logo_row,
            dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
            dbc.Collapse(
                dbc.Row(
                    [
                        dbc.Col(instance_select),
                        dbc.Col(
                            sign_out_button,
                            className="ms-auto",
                        ),
                    ],
                    className="g-0 ms-auto flex-nowrap mt-3 mt-md-0",
                ),
                id='navbar-collapse',
                is_open=False,
                navbar=True,
            ),
        ],
        style={'max-width': '96%'},
    ),
    color='dark', dark=True,
    style={'width': '100% !important'},
)

refresh_jobs_interval = dcc.Interval(
    id='refresh-jobs-interval',
    interval=(1 * 1000),
    n_intervals=0,
    disabled=False,
)

def alert_from_success_tuple(success: SuccessTuple) -> dbc.Alert:
    """
    Return a `dbc.Alert` from a `SuccessTuple`.
    """
    return dbc.Alert('', is_open=False) if not isinstance(success, tuple) else (
        dbc.Alert(
            remove_ansi(success[1]),
            id='success-alert',
            dismissable=True,
            fade=True,
            is_open=(success[1] not in STATIC_CONFIG['system']['success']['ignore']),
            color='success' if success[0] else 'danger',
        )
    )


def build_cards_grid(cards: List[dbc.Card], num_columns: int = 3) -> html.Div:
    """
    Because `CardColumns` were removed in Bootstrap 5, this function recreates a similar effect.
    """
    rows_childrens = []
    for i, card in enumerate(cards):
        if i % num_columns == 0:
            rows_childrens.append([])
        rows_childrens[-1].append(dbc.Col(card, sm=12, md=12, lg=int(12/num_columns)))
    ### Append mising columns to keep the grid shape.
    if rows_childrens and len(rows_childrens[-1]) != num_columns:
        for i in range(num_columns - len(rows_childrens[-1])):
            rows_childrens[-1].append(dbc.Col(sm=12, md=12, lg=int(12/num_columns)))
    _rows = [dbc.Row(children) for children in rows_childrens]
    rows = []
    for r in _rows:
        rows.append(r)
        rows.append(html.Br())
    return html.Div(rows)


def build_pages_offcanvas_children():
    """
    Return the contents of the pages offcanvas.
    """
    from meerschaum.api.dash.callbacks.dashboard import _pages
    from meerschaum.api.dash.callbacks.custom import _plugin_endpoints_to_pages
    pages_listgroup_items = []
    custom_pages = []
    for pages_dicts in _plugin_endpoints_to_pages.values():
        for page_dict in pages_dicts.values():
            custom_pages.append(page_dict['page_key'])

    for page_key, page_href in _pages.items():
        if page_key in custom_pages:
            continue
        pages_listgroup_items.append(
            dbc.ListGroupItem(
                dbc.Button(
                    ' '.join([word.capitalize() for word in page_key.split(' ')]),
                    style={
                        'width': '100%',
                        'text-align': 'left',
                        'text-decoration': 'none',
                        'padding-left': '0',
                    },
                    href=page_href,
                    color='dark',
                )
            )
        )

    plugins_accordion_items = []
    for page_group, pages_dicts in _plugin_endpoints_to_pages.items():
        plugin_listgroup_items = [
            dbc.ListGroupItem(
                dbc.Button(
                    ' '.join([word.capitalize() for word in page_dict['page_key'].split(' ')]),
                    style={
                        'width': '100%',
                        'text-align': 'left',
                        'text-decoration': 'none',
                        'padding-left': '0',
                    },
                    href=page_href,
                    color='dark',
                )
            )
            for page_href, page_dict in pages_dicts.items()
        ]
        plugin_listgroup = dbc.ListGroup(plugin_listgroup_items, flush=True)
        plugin_accordion_item = dbc.AccordionItem(
            plugin_listgroup,
            title=(
                page_group.capitalize()
                if page_group and not page_group[0].isupper()
                else page_group
            ),
            class_name='pages-offcanvas-accordion',
        )
        plugins_accordion_items.append(plugin_accordion_item)
    plugins_accordion = dbc.Accordion(
        plugins_accordion_items,
        start_collapsed=True,
        flush=True,
        always_open=True,
    )
    pages_listgroup_items.append(plugins_accordion)

    pages_children = dbc.Card(
        dbc.ListGroup(
            pages_listgroup_items,
            flush=True,
        ),
        outline=True,
    )
    return pages_children
