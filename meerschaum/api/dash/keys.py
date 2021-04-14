#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define components for choosing keys.
"""

from __future__ import annotations
from meerschaum.utils.packages import attempt_import
dash = attempt_import('dash', lazy=False)
dbc = attempt_import('dash_bootstrap_components', lazy=False)
dcc = attempt_import('dash_core_components', warn=False)
html = attempt_import('dash_html_components', warn=False)

placeholders = {
    'ck' : 'Connectors',
    'mk' : 'Metrics',
    'lk' : 'Locations',
    'params' : (
        'Additional search parameters. ' +
        'Simple dictionary format or JSON accepted.'
    ),
}
widths = {
    'ck' : {'size' : 4},
    'mk' : {'size' : 4},
    'lk' : {'size' : 4},
    'params' : {'size' : 12},
}
input_group_sizes = {
    'ck' : 'sm',
    'mk' : 'sm',
    'lk' : 'sm',
    'params' : 'sm',
}

dropdown_tab_content = dbc.Card(
    dbc.CardBody(
        [
            dbc.Row(
                [
                    dbc.Col(
                        html.Div(
                            [
                                dcc.Dropdown(
                                    id = 'connector-keys-dropdown',
                                    options = [],
                                    placeholder = placeholders['ck'],
                                    multi = True,
                                ),
                            ],
                            className = 'dbc_dark',
                        ),
                        width = widths['ck'],
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                dcc.Dropdown(
                                    id = 'metric-keys-dropdown',
                                    options = [],
                                    placeholder = placeholders['mk'],
                                    multi = True,
                                ),
                            ],
                            className = 'dbc_dark'
                        ),
                        width = widths['mk'],
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                dcc.Dropdown(
                                    id = 'location-keys-dropdown',
                                    options = [],
                                    placeholder = placeholders['lk'],
                                    multi = True,
                                ),
                            ],
                            className = 'dbc_dark'
                        ),
                        width = widths['lk'],
                    ),
                ]
            )
        ]
    )
)

text_tab_content = dbc.Card(
    dbc.CardBody(
        [
            dbc.Row(
                [
                    dbc.Col(html.Div(className='dbc_dark', children=[
                        dbc.InputGroup(
                            [
                                dbc.InputGroupAddon(
                                    dbc.Button(
                                        'Clear',
                                        id = 'clear-connector-keys-input-button',
                                        color = 'link'
                                    ),
                                    addon_type = 'prepend',
                                ),
                                dbc.Input(
                                    id = 'connector-keys-input',
                                    placeholder = placeholders['ck'],
                                    type = 'text',
                                    value = '',
                                    list = 'connector-keys-list',
                                    className = 'dbc_dark'
                                ),
                            ],
                            size = input_group_sizes['ck'],
                        )]),
                        width = widths['ck'],
                    ),
                    dbc.Col(
                        dbc.InputGroup(
                            [
                                dbc.InputGroupAddon(
                                    dbc.Button(
                                        'Clear',
                                        id = 'clear-metric-keys-input-button',
                                        color = 'link'
                                    ),
                                    addon_type = 'prepend',
                                ),
                                dbc.Input(
                                    id = 'metric-keys-input',
                                    placeholder = placeholders['mk'],
                                    type = 'text',
                                    value = '',
                                    list = 'metric-keys-list',
                                ),
                            ],
                            size = input_group_sizes['mk'],
                        ),
                        width = widths['mk'],
                    ),
                    dbc.Col(
                        dbc.InputGroup(
                            [
                                dbc.InputGroupAddon(
                                    dbc.Button(
                                        'Clear',
                                        id = 'clear-location-keys-input-button',
                                        color = 'link'
                                    ),
                                    addon_type = 'prepend',
                                ),
                                dbc.Input(
                                    id = 'location-keys-input',
                                    placeholder = placeholders['lk'],
                                    type = 'text',
                                    value = '',
                                    list = 'location-keys-list',
                                ),
                            ],
                            size = input_group_sizes['lk'],
                        ),
                        width = widths['lk'],
                    ),
                ]
            ),
            html.Br(),
            dbc.Row(
                dbc.Col(
                    dbc.InputGroup(
                        [
                            dbc.InputGroupAddon(
                                dbc.Button(
                                    'Clear',
                                    id = 'clear-params-textarea-button',
                                    color = 'link'
                                ),
                                addon_type = 'prepend',
                            ),
                            dbc.Textarea(
                                id = 'params-textarea',
                                placeholder = placeholders['params'],
                                value = '',
                            )
                        ],
                        size = input_group_sizes['params'],
                    )
                )
            ),
        ]
    )
)

keys_lists_content = html.Div([
    html.Datalist(id='connector-keys-list'),
    html.Datalist(id='metric-keys-list'),
    html.Datalist(id='location-keys-list'),
], hidden=True)

