#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The pipelines page (`/dash/pipelines`, new in v5): the dependency graph with each pipe's planned
action and ledger lag, a Gantt per run (planned against actual), the runs table and a run's
events. The data is the Syncing Engine's (`meerschaum.api.dash.engine`).
"""

from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.api.dash.components import build_pages_navbar
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

STATUS_OPTIONS = [
    {'label': 'Any status', 'value': 'any'},
    {'label': 'Running', 'value': 'running'},
    {'label': 'Succeeded', 'value': 'succeeded'},
    {'label': 'Failed', 'value': 'failed'},
]

layout = [
    build_pages_navbar(),
    dcc.Location('pipelines-location'),
    dcc.Interval(id='pipelines-interval', interval=(15 * 1000), n_intervals=0),
    dbc.Container(
        [
            html.Br(),
            html.Div(
                [
                    html.H3('Pipelines', style={'display': 'inline-block'}),
                    dbc.Button(
                        "⟳",
                        color='black',
                        size='sm',
                        id='pipelines-refresh-button',
                        title='Refresh',
                        style={'margin-left': '10px'},
                    ),
                ],
            ),
            html.Div(id='pipelines-alert-div'),
            dbc.Card(
                dbc.CardBody(
                    dbc.Row(
                        [
                            dbc.Col(
                                dbc.Input(
                                    id='pipelines-run-id-input',
                                    placeholder='Run ID',
                                    type='text',
                                    debounce=True,
                                    size='sm',
                                ),
                                md=4,
                            ),
                            dbc.Col(
                                dbc.Input(
                                    id='pipelines-pipe-input',
                                    placeholder='Pipe (connector|metric|location)',
                                    type='text',
                                    debounce=True,
                                    size='sm',
                                ),
                                md=4,
                            ),
                            dbc.Col(
                                dbc.Select(
                                    id='pipelines-status-select',
                                    options=STATUS_OPTIONS,
                                    value='any',
                                    size='sm',
                                    class_name='dbc_dark custom-select custom-select-sm',
                                ),
                                md=2,
                            ),
                            dbc.Col(
                                dbc.Button(
                                    'Filter runs',
                                    id='pipelines-filter-button',
                                    size='sm',
                                    color='primary',
                                    style={'width': '100%'},
                                ),
                                md=2,
                            ),
                        ],
                        className='g-2',
                    ),
                ),
            ),
            html.Br(),
            html.H4('Pipeline dependencies'),
            dcc.Loading(
                html.Div(id='pipelines-graph-div'),
                id='pipelines-graph-loading',
                type='circle',
                delay_show=1000,
            ),
            html.Br(),
            html.H4('Timeline'),
            html.Div(id='pipelines-gantt-div'),
            html.Br(),
            html.H4('Runs'),
            html.Div(id='pipelines-runs-div'),
            html.Br(),
            html.H4('Events'),
            html.Pre('', id='pipelines-events-pre', className='pipelines-events'),
            dcc.Store(id='pipelines-events-store', data={}),
            html.Br(),
        ],
        fluid=True,
        style={'max-width': '96%'},
    ),
]
