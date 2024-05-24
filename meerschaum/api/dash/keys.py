#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define components for choosing keys.
"""

from __future__ import annotations
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.actions import actions
from meerschaum.api import CHECK_UPDATE
from meerschaum.api.dash.components import search_parameters_editor
dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)

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
    'flags' : {'size' : 12},
    'params' : {'size' : 12},
    'begin_end' : {'size' : 8},
    'arguments' : {'size' : 8},
}
input_group_sizes = {
    'ck' : 'sm',
    'mk' : 'sm',
    'lk' : 'sm',
    'params' : 'sm',
}

action_dropdown_row = html.Div(
        children = [
        dbc.Row(
            id = 'action-row',
            children = [
                dbc.Col(
                    html.Div(
                        dbc.Select(
                            id = 'action-dropdown',
                            options = [],
                            className = 'custom-select input-text'
                            #  clearable = False,
                            #  style = {'height' : '150px'},
                        ),
                        id = 'action-dropdown-div',
                        className = 'dbc_dark',
                    ),
                    sm = 12,
                    md = 6,
                    lg = 3,
                ),
                dbc.Col(
                    html.Div(
                        dbc.Select(
                            id = 'subaction-dropdown',
                            options = [],
                            className = 'custom-select input-text'
                        ),
                        id = 'subaction-dropdown-div',
                        className = 'dbc_dark input-text'
                    ),
                    sm = 12,
                    md = 6,
                    lg = 3,
                ),
                dbc.Col(
                    html.Div(
                        dbc.InputGroup(
                            children = [
                                dbc.Button(
                                    'Clear',
                                    color = 'link',
                                    id = 'clear-subaction-dropdown-text-button',
                                    size = 'sm',
                                    style = {'text-decoration': 'none'},
                                ),
                                dbc.Input(
                                    id = 'subaction-dropdown-text',
                                    placeholder = 'Positional arguments',
                                    className = 'input-text',
                                ),
                            ],
                        ),
                    ),
                    id = 'subaction-dropdown-text-div',
                    lg = 6,
                    md = 12,
                    sm = 12,
                    className = 'dbc_dark',
                ),
            ]
        ),
        html.Br(),
        dbc.Row(
            children = [
                dbc.Col(
                    html.Div(
                        dcc.Dropdown(
                            id = 'flags-dropdown',
                            multi = True,
                            placeholder = 'Boolean flags',
                            options = ['--yes'],
                            value = ['--yes'],
                        ),
                        id = 'flags-dropdown-div',
                        className = 'dbc_dark input-text',
                    ),
                    width = widths['flags'],
                ),
            ],
        ),
        html.Br(),
        html.Div(id='input-flags-div'),
        dbc.Row(
            children = [
                dbc.Col(
                    children = [
                        html.Div(
                            children = [
                                dbc.Button(
                                    'Additional parameters',
                                    id = 'show-arguments-collapse-button',
                                    color = 'link',
                                    size = 'md',
                                    outline = True,
                                    style = {'display': 'none'},
                                ),
                                #  html.Br(),
                                dbc.Collapse(
                                    children = [
                                        dbc.Button(
                                            'Clear',
                                            id = 'clear-begin-end-datepicker-button',
                                            color = 'link',
                                            size = 'sm',
                                        ),
                                        dcc.DatePickerRange(
                                            id = 'begin-end-datepicker',
                                        ),
                                    ],
                                    id = 'arguments-collapse',
                                ),
                            ], ### end of div children
                        ),
                    ], ### end of col children
                    width = widths['arguments'],
                ),
            ], ### end of row children
        ),
    ], ### end of parent div children
    id = 'action-div',
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
            lg = 4,
            md = 12,
            sm = 12,
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
            lg = 4,
            md = 12,
            sm = 12,
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
            lg = 4,
            md = 12,
            sm = 12,
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
                                #  dbc.InputGroupAddon(
                                    dbc.Button(
                                        'Clear',
                                        id = 'clear-connector-keys-input-button',
                                        color = 'link',
                                        size = 'sm',
                                    ),
                                    #  addon_type = 'prepend',
                                #  ),
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
                        width = 4,
                    ),
                    dbc.Col(
                        dbc.InputGroup(
                            [
                                #  dbc.InputGroupAddon(
                                    dbc.Button(
                                        'Clear',
                                        id = 'clear-metric-keys-input-button',
                                        color = 'link',
                                        size = 'sm',
                                    ),
                                    #  addon_type = 'prepend',
                                #  ),
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
                        width = 4,
                    ),
                    dbc.Col(
                        dbc.InputGroup(
                            [
                                #  dbc.InputGroupAddon(
                                    dbc.Button(
                                        'Clear',
                                        id = 'clear-location-keys-input-button',
                                        color = 'link',
                                        size = 'sm',
                                    ),
                                    #  addon_type = 'prepend',
                                #  ),
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
                        width = 4,
                    ),
                ]
            ),
            html.Br(),
            dbc.Row(
                dbc.Col(
                    dbc.InputGroup(
                        [
                            #  dbc.InputGroupAddon(
                                #  dbc.Button(
                                    #  'Clear',
                                    #  id = 'clear-params-textarea-button',
                                    #  color = 'link',
                                    #  size = 'sm',
                                #  ),
                                #  addon_type = 'prepend',
                            #  ),
                            search_parameters_editor,
                            #  dbc.Textarea(
                                #  id = 'params-textarea',
                                #  placeholder = placeholders['params'],
                                #  value = '',
                            #  )
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

