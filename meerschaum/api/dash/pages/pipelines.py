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
    dcc.Location('pipelines-location', refresh=False),
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
            dcc.Store(id='pipeline-visible-refs', data=[]),
            dcc.Store(id='pipeline-selection'),
            dcc.Store(id='pipeline-inspector-result'),
            html.Div(id='pipeline-inspector-guard', hidden=True),
            dcc.Store(id='pipeline-context'),
            dcc.Store(id='pipeline-node-run'),
            dcc.Store(id='pipeline-node-runs', data=[]),
            dcc.Store(id='pipeline-node-events'),
            dcc.Store(id='pipeline-node-statuses', data={}),
            dcc.Store(id='pipeline-analysis'),
            dcc.Store(id='pipeline-stage-statuses', data={}),
            html.Div(id='pipeline-stage-status-dummy', hidden=True),
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
                html.Div([
                    build_pipeline_controls(),
                    html.Div(id='pipeline-analysis-alert'),
                    html.Div(id='pipeline-analysis-preview'),
                ], className='pipeline-workspace-management'),
                html.Details([
                    html.Summary('Edit Pipeline draft / add Pipe'),
                    html.P('Membership edits affect this Pipeline. Pipe registration is a separate explicit action.'),
                    dcc.Dropdown(id='pipeline-draft-members', options=[], multi=True,
                                 placeholder='Explicit members (instance-qualified)'),
                    dbc.Row([
                        dbc.Col(dbc.Checkbox(id='pipeline-draft-parents', label='Include all parents', value=False)),
                        dbc.Col(dbc.Checkbox(id='pipeline-draft-children', label='Include all children', value=False)),
                    ]),
                    dbc.Button('Undo', id='pipeline-draft-undo', size='sm', class_name='me-2'),
                    dbc.Button('Redo', id='pipeline-draft-redo', size='sm', class_name='me-2'),
                    dbc.Button('Save draft', id='pipeline-draft-save', size='sm', class_name='me-2'),
                    dbc.Button('Discard draft', id='pipeline-draft-discard', size='sm', color='secondary'),
                    html.Div(id='pipeline-draft-notice'),
                    dbc.Button('Save shared layout', id='pipeline-save-layout', n_clicks=0, size='sm',
                               title='Alt-drag Pipes to arrange, then save for everyone', class_name='mt-2'),
                    html.Div(id='pipeline-layout-alert'),
                    html.Details([
                        html.Summary('Find existing Pipes on an instance / register a new Pipe'),
                        dbc.Input(id='pipeline-add-instance', placeholder='Instance keys, e.g. sql:main', size='sm'),
                        dbc.Button('Find existing Pipes', id='pipeline-add-find', size='sm'),
                        dbc.Input(id='pipeline-add-connector', placeholder='Connector keys', size='sm'),
                        dbc.Input(id='pipeline-add-metric', placeholder='Metric key', size='sm'),
                        dbc.Input(id='pipeline-add-location', placeholder='Location key (optional)', size='sm'),
                        dbc.Textarea(id='pipeline-add-parameters', value='{}', placeholder='Parameters JSON'),
                        dbc.Button('Register new Pipe and add to draft', id='pipeline-add-register', size='sm'),
                        html.Div(id='pipeline-add-alert'),
                    ]),
                ], id='pipeline-draft-editor'),
                dcc.Store(id='pipeline-draft'),
                dcc.Store(id='pipeline-layout-draft'),
                html.Div(id='pipelines-graph-div'),
                html.Aside(id='pipeline-inspector', hidden=True),
                html.Div([
                    html.Div([html.H5('Pipe output'),
                        html.Button('Expand output', id='pipeline-output-expand'),
                        dbc.Button('Cancel selected execution', id='pipeline-cancel-node-run', color='danger', outline=True, size='sm'),
                    ], className='pipeline-output-header'),
                    dcc.Dropdown(id='pipeline-output-tab', options=[], placeholder='Select Pipe output', clearable=False),
                    html.Div(id='pipeline-node-run-label'),
                    html.Div(id='pipeline-cancel-alert'),
                    html.Div(id='pipeline-node-stream-dummy', hidden=True),
                    html.Pre(id='pipeline-node-output', className='pipelines-events'),
                    html.Div(id='pipeline-node-alert'),
                    html.Details([html.Summary('Pipeline command output'),
                        html.Pre(id='pipeline-command-output', className='pipelines-events')]),
                ], id='pipeline-output-dock', className='pipeline-output-empty'),
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
            html.H4('Actual execution timeline'),
            html.Div(id='pipelines-gantt-div'),
            html.Br(),
            html.H4('Runs'),
            html.Div(id='pipelines-runs-div'),
            html.Br(),
            html.H4('Sync activity'),
            dcc.Dropdown(id='pipeline-log-stage', options=[], value='',
                         placeholder='All stages', clearable=True),
            html.Pre('', id='pipelines-events-pre', className='pipelines-events'),
            dcc.Store(id='pipelines-events-store', data={}),
            html.Br(),
        ],
        fluid=True,
        style={'max-width': '96%'},
    ),
]
