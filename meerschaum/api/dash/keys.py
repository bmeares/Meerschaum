#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define components for choosing keys.
"""

from __future__ import annotations
from meerschaum.utils.packages import attempt_import
from meerschaum.actions import actions
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
    'action' : {'size' : 4},
    'subaction' : {'size' : 4},
    'flags' : {'size' : 4},
    'params' : {'size' : 12},
}
input_group_sizes = {
    'ck' : 'sm',
    'mk' : 'sm',
    'lk' : 'sm',
    'params' : 'sm',
}

#  show_pipes_button = dbc.Button(
    #  "Show Pipes", id='show-pipes-button',
    #  color='secondary', className='mr-1', style={'float' : 'right'}
#  )

action_dropdown_row = dbc.Row(
    id = 'action-row',
    children = [
        dbc.Col(
            dbc.InputGroup(
                id = 'action-dropdown-group',
                children = [
                    dbc.Col(
                        html.Div(
                            dcc.Dropdown(
                                id = 'action-dropdown',
                                options = [],
                                clearable = False,
                            ),
                            id = 'action-dropdown-div',
                            className = 'dbc_dark'
                        ),
                        width = widths['action']
                    ),
                    dbc.Col(
                        html.Div(
                            dcc.Dropdown(
                                id = 'subaction-dropdown',
                                options = [],
                                clearable = False,
                            ),
                            id = 'subaction-dropdown-div',
                            className = 'dbc_dark'
                        ),
                        width = widths['subaction']
                    ),
                    dbc.Col(
                        html.Div(
                            dcc.Dropdown(
                                id = 'flags-dropdown',
                                multi = True,
                                placeholder = 'Additional flags',
                                options = [],
                            ),
                            id = 'flags-dropdown-div',
                            className = 'dbc_dark',
                        ),
                        width = widths['flags'],
                    )
                ]
            ),
            ### Action buttons width
            #  width = {'size' : 7, 'offset' : 4},
        ),
    ]
)


dropdown_keys_row = dbc.Row(
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
    ] ### end of filters row children
)
dropdown_tab_content = html.Div([
    dbc.Card(
        dbc.CardBody(
            [
                #  html.P('Pipe Keys'),
                dropdown_keys_row,
            ], ### end of card children
            className = 'card-text',
        )
    ),
    html.Br(),
    dbc.Card(
        dbc.CardBody(
            [
                action_dropdown_row,
            ],
            className = 'card-text',
        ),
    ),
])

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

