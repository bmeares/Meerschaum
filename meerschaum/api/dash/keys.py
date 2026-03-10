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
from meerschaum._internal.arguments._parser import parser
dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)

placeholders = {
    'ck': 'Connectors',
    'mk': 'Metrics',
    'lk': 'Locations',
    'tags': 'Tags',
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
omit_flags = {
    'help',
    'gui',
    'version',
    'shell',
    'use_bash',
    'trace',
    'allow_shell_job',
    'action',
    'mrsm_instance',
}

def build_flags_options(is_input: bool = False):
    _flags_options = []
    for a in parser._actions:
        acceptable_args = (a.nargs != 0 if not is_input else a.nargs == 0)
        if acceptable_args or a.dest in omit_flags:
            continue
        _op = {'title': a.help}
        for _trigger in a.option_strings:
            if _trigger.startswith('--'):
                _op['value'] = _trigger
                break
        if not _op.get('value', None):
            _op['value'] = a.dest
        _op['label'] = _op['value']
        _flags_options.append(_op)
    return sorted(_flags_options, key=lambda k: k['label'])

def build_flags_row(
    index: int,
    val: str | None,
    val_text: str | None,
    taken_input_flags: list[str] | None = None,
):
    taken_input_flags = taken_input_flags or []
    options = [
        op
        for op in build_flags_options(is_input=True)
        if op['value'] == val or op['value'] not in taken_input_flags
    ]
    row_children = [
        dbc.Col(
            html.Div(
                dbc.InputGroup([
                    dbc.Button(
                        '❌',
                        color = 'link',
                        id = {'type': 'input-flags-remove-button', 'index': index},
                        size = 'sm',
                        style = {'text-decoration': 'none'},
                    ),
                    dcc.Dropdown(
                        id = {'type': 'input-flags-dropdown', 'index': index},
                        multi = False,
                        placeholder = 'Input flags',
                        options = options,
                        value = val,
                        style = {'flex': 1},
                    ),
                ]),
                id = {'type': 'input-flags-dropdown-div', 'index': index},
                className = 'dbc_dark',
            ),
            sm = 12,
            md = 5,
            lg = 5,
            id = 'input-flags-left-col',
        ),
        dbc.Col(
            html.Div(
                dbc.Input(
                    id = {'type': 'input-flags-dropdown-text', 'index': index},
                    placeholder = 'Flag value',
                    className = 'input-text',
                    value = val_text,
                ),
                id = {'type': 'input-flags-text-div', 'index': index},
                className = 'dbc_dark input-text',
            ),
            sm = 12,
            md = 7,
            lg = 7,
        )
    ]
    return dbc.Row(
        row_children,
        id = {'type': 'input-flags-row', 'index': index},
        className = 'input-text',
    )


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
                            id='flags-dropdown',
                            multi=True,
                            placeholder='Boolean flags',
                            options=[],
                            value=[],
                        ),
                        id='flags-dropdown-div',
                        className='dbc_dark input-text',
                    ),
                    width=widths['flags'],
                ),
            ],
        ),
        html.Br(),
        html.Div([build_flags_row(1, None, None)], id='input-flags-div'),
        dbc.Row(
            children=[
                dbc.Col(
                    children=[
                        html.Div(
                            children=[
                                dbc.Button(
                                    'Additional parameters',
                                    id='show-arguments-collapse-button',
                                    color='link',
                                    size='md',
                                    outline=True,
                                    style={'display': 'none'},
                                ),
                                dbc.Collapse(
                                    children=[
                                        dbc.Button(
                                            'Clear',
                                            id='clear-begin-end-datepicker-button',
                                            color='link',
                                            size='sm',
                                        ),
                                        dcc.DatePickerRange(
                                            id='begin-end-datepicker',
                                        ),
                                    ],
                                    id='arguments-collapse',
                                ),
                            ], ### end of div children
                        ),
                    ], ### end of col children
                    width=widths['arguments'],
                ),
            ], ### end of row children
        ),
    ], ### end of parent div children
    id='action-div',
)


dropdown_keys_row = dbc.Row(
    [
        dbc.Col(
            html.Div(
                [
                    dcc.Dropdown(
                        id='connector-keys-dropdown',
                        options=[],
                        placeholder=placeholders['ck'],
                        multi=True,
                    ),
                ],
                className='dbc_dark',
            ),
            lg=4,
            md=12,
            sm=12,
        ),
        dbc.Col(
            html.Div(
                [
                    dcc.Dropdown(
                        id='metric-keys-dropdown',
                        options=[],
                        placeholder=placeholders['mk'],
                        multi=True,
                    ),
                ],
                className='dbc_dark'
            ),
            lg=4,
            md=12,
            sm=12,
        ),
        dbc.Col(
            html.Div(
                [
                    dcc.Dropdown(
                        id='location-keys-dropdown',
                        options=[],
                        placeholder=placeholders['lk'],
                        multi=True,
                    ),
                ],
                className='dbc_dark'
            ),
            lg=4,
            md=12,
            sm=12,
        ),
    ] ### end of filters row children
)
tags_dropdown = html.Div(
    dcc.Dropdown(
        id='tags-dropdown',
        options=[],
        placeholder=placeholders['tags'],
        multi=True,
        searchable=True,
    ),
    className="dbc_dark",
    id="tags-dropdown-div",
)

dropdown_tab_content = html.Div([
    dbc.Card(
        dbc.CardBody(
            [
                dropdown_keys_row,
                html.Br(),
                dbc.Row(
                    [
                        dbc.Col(tags_dropdown, width=True),
                        dbc.Col(
                            dbc.Button(
                                "Clear all",
                                id='clear-all-keys-button',
                                color='link',
                                size='sm',
                                style={'text-decoration': 'none'},
                            ),
                            width='auto',
                        ),
                    ],
                    className='g-0',
                    align='center',
                ),
            ], ### end of card children
            className='card-text',
        )
    ),
    html.Br(),
    dbc.Card(
        dbc.CardBody(
            [
                action_dropdown_row,
            ],
            className='card-text',
        ),
    ),
])

keys_lists_content = html.Div([
    html.Datalist(id='connector-keys-list'),
    html.Datalist(id='metric-keys-list'),
    html.Datalist(id='location-keys-list'),
    html.Datalist(id='tags-list'),
], hidden=True)
