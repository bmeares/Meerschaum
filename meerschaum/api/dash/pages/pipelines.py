#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The pipelines page (`/dash/pipelines`, new in v5): the dependency graph with each pipe's planned
action and dependencies, a Gantt per run (planned against actual), the runs table and a run's
events. The data is the Syncing Engine's (`meerschaum.api.dash.engine`).
"""

from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.api.dash.components import build_pages_navbar
from meerschaum.api.dash.pipeline_management import build_pipeline_controls
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
            build_pipeline_controls(),
            dcc.Store(id='pipeline-visible-refs', data=[]),
            html.Div([
                html.Details([
                    html.Summary('Find a pipe’s pipeline'),
                    html.Div([
                        dcc.Dropdown(id='pipeline-focus-connector', options=[],
                                     placeholder='Connector keys', clearable=True),
                        dcc.Dropdown(id='pipeline-focus-metric', options=[],
                                     placeholder='Metric keys', clearable=True),
                        dcc.Dropdown(id='pipeline-focus-location', options=[],
                                     placeholder='Location keys', clearable=True),
                    ], className='pipeline-key-filters'),
                    dcc.Dropdown(id='pipeline-focus', options=[],
                                 placeholder='Select a pipe to focus its pipeline',
                                 className='pipeline-focus', clearable=True),
                ], className='pipeline-find'),
                dbc.Checkbox(id='pipeline-standalone', label='Include standalone pipes', value=False),
            ], className='pipeline-filter-toolbar'),
            dcc.Store(id='pipeline-focus-options', data=[]),
            html.Div([
                dcc.Loading(html.Div(id='pipelines-graph-div'), type='circle', delay_show=1000),
                html.Aside(id='pipeline-inspector', hidden=True),
            ], className='pipeline-workspace', id='pipeline-workspace'),
            html.H4('Sync history'),
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
                                    placeholder="Find a pipe, e.g. Pipe('sql:main', 'counts')",
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
