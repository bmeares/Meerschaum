#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The Pipeline workspace and run history, backed by the Syncing Engine.
"""

from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.api.dash.components import build_pages_navbar
from meerschaum.api.dash.pipeline_management import build_pipeline_controls
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

from meerschaum.connectors import instance_types
from meerschaum.utils.misc import get_connector_labels
from meerschaum.api.dash.engine import get_instance

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
    html.Main(
        [
            html.Div(
                [
                    html.H3('Pipelines', style={'display': 'inline-block'}),
                    dbc.Button(
                        'Refresh',
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
            dcc.Store(id='pipeline-selected-run'),
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
            dcc.Store(id='pipeline-focus-options', data=[]),
            build_pipeline_controls(html.Div([
                html.Details([
                    html.Summary('Find pipes'),
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
                    dbc.Checkbox(id='pipeline-standalone', label='Include standalone pipes', value=False),
                ], className='pipeline-find'),
            ], className='pipeline-filter-toolbar')),
            html.Div([
                html.Button('Workspace', id='pipeline-view-workspace', role='tab', **{'aria-selected':'true'}),
                html.Button('Runs', id='pipeline-view-runs', role='tab', **{'aria-selected':'false'}),
            ], className='pipeline-view-tabs', role='tablist', **{'aria-label':'Pipeline views'}),
            html.Section([
                html.Div([
                html.Div([
                    html.Div([
                        html.Button(label, id='pipeline-mobile-' + value, role='tab',
                                    **{'data-pane':value, 'aria-selected':str(value == 'graph').lower()})
                        for value, label in [('graph','Graph'),('details','Pipe details'),('output','Output')]
                    ], className='pipeline-mobile-tabs', role='tablist', **{'aria-label':'Workspace panels'}),
                    html.Button('Exit fullscreen', id='pipeline-exit-fullscreen', className='pipeline-exit-fullscreen'),
                ], className='pipeline-workspace-management'),
                html.Details([
                    html.Summary('Pipeline members'),
                    html.P('Select existing Pipes, then save membership.'),
                    dcc.Dropdown(id='pipeline-draft-members', options=[], multi=True, closeOnSelect=True,
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
                    html.Details([
                        html.Summary('Find Pipes on another instance'),
                        dcc.Dropdown(id='pipeline-add-instance', options=[{'label': key, 'value': key} for key in get_connector_labels(*instance_types)], value=str(get_instance()), clearable=False, placeholder='Instance'),
                        dbc.Button('Find existing Pipes', id='pipeline-add-find', size='sm'),
                        html.Details([html.Summary('Register a new Pipe'),
                        dbc.Input(id='pipeline-add-connector', placeholder='Connector keys', size='sm'),
                        dbc.Input(id='pipeline-add-metric', placeholder='Metric key', size='sm'),
                        dbc.Input(id='pipeline-add-location', placeholder='Location key (optional)', size='sm'),
                        dbc.Textarea(id='pipeline-add-parameters', value='{}', placeholder='Parameters JSON'),
                        dbc.Button('Register new Pipe and add to draft', id='pipeline-add-register', size='sm'),
                        ]),
                        html.Div(id='pipeline-add-alert'),
                    ]),
                ], id='pipeline-draft-editor'),
                dcc.Store(id='pipeline-draft'),
                dcc.Store(id='pipeline-member-options', data={'options': []}),
                dcc.Store(id='pipeline-layout-draft'),
                html.Div(id='pipelines-graph-div'),
                html.Aside(id='pipeline-inspector', hidden=True),
                html.Div([
                    html.Div([
                        dbc.Button('Cancel sync', id='pipeline-cancel-node-run', color='danger', outline=True, size='sm', style={'display': 'none'}),
                        html.Button('×', id='pipeline-output-close', title='Close output'),
                    ], className='pipeline-output-header'),
                    html.Div([
                    dcc.Dropdown(id='pipeline-output-tab', options=[], placeholder='Select Pipe output', clearable=False),
                    html.Div(id='pipeline-node-run-label'),
                    html.Div(id='pipeline-cancel-alert'),
                    html.Div(id='pipeline-node-stream-dummy', hidden=True),
                    html.Pre(id='pipeline-node-output', className='pipelines-events'),
                    html.Div(id='pipeline-node-alert'),
                    ], id='pipeline-pipe-output'),
                    html.Div([html.Div(id='pipeline-command-label'),
                        html.Pre(id='pipeline-command-output', className='pipelines-events')], id='pipeline-whole-output'),
                ], id='pipeline-output-dock', className='pipeline-output-empty pipeline-output-expanded'),
                ], className='pipeline-workspace', id='pipeline-workspace', **{'data-pane':'graph'}),
            ], id='pipeline-workspace-panel', role='tabpanel', **{'aria-labelledby':'pipeline-view-workspace'}),
            html.Section([
                html.H4('Runs'),
                html.Details([html.Summary('Filters'), dbc.Card(
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
            )], className='pipeline-run-filters'),
                html.Div(id='pipelines-runs-div'),
                html.H5('Selected run'),
                html.Div(id='pipeline-run-status'),
                html.Div(id='pipelines-gantt-div'),
                html.Details([
                    html.Summary('Sync activity'),
                    dcc.Dropdown(id='pipeline-log-stage', options=[], value='',
                                 placeholder='All stages', clearable=True),
                    html.Pre('', id='pipelines-events-pre', className='pipelines-events'),
                ], className='pipeline-activity'),
            ], id='pipeline-runs-panel', role='tabpanel', **{'aria-labelledby':'pipeline-view-runs'}),
            dcc.Store(id='pipelines-events-store', data={}),
            html.Br(),
        ],
        id='pipelines-page', className='pipeline-page', **{'data-view':'workspace'},
    ),
]
