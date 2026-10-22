"""Controls for saved pipeline selections and their ordinary scheduled Jobs."""
from meerschaum.utils.packages import import_html, import_dcc
import dash_bootstrap_components as dbc
html, dcc = import_html(), import_dcc()


def build_pipeline_controls():
    return html.Div([
        dbc.Row([
            dbc.Col(dcc.Dropdown(id='pipeline-saved-name', options=[], clearable=True,
                                placeholder='All pipelines'), md=5),
            dbc.Col(dbc.Button('Analyze', id='pipeline-analyze', size='sm'), width='auto'),
            dbc.Col(dbc.Button('Apply proposed tags', id='pipeline-apply-tags', color='warning', size='sm', disabled=True), width='auto'),
            dbc.Col(dbc.Button('Manage pipeline', id='pipeline-manage-toggle', size='sm',
                              color='secondary'), width='auto'),
        ], className='g-2 align-items-center mb-2'),
        dbc.Collapse(dbc.Card(dbc.CardBody([
            html.H5('Pipeline definition'),
            html.P('Save the visible pipes under a name. Select a pipeline to edit its definition.',
                   className='text-muted'),
            dbc.Row([
                dbc.Col(dbc.Input(id='pipeline-name-input', placeholder='Pipeline name', size='sm'), md=5),
                dbc.Col(dbc.Button('Save pipeline', id='pipeline-save', size='sm'), width='auto'),
                dbc.Col(dbc.Button('Delete pipeline', id='pipeline-delete', color='danger',
                                  outline=True, size='sm'), width='auto'),
            ], className='g-2 mb-3'),
            html.Div(id='pipeline-manage-alert'),
            html.H5('Schedule'),
            html.P('Create a recurring job using this pipeline’s saved membership. The job starts immediately.', className='text-muted'),
            html.Div(id='pipeline-job-settings-notice'),
            dbc.Row([
                dbc.Col([dbc.Label('Job name'), dbc.Input(id='pipeline-job-name', placeholder='New job name', size='sm')], md=4),
                dbc.Col([dbc.Label('Schedule'), dbc.Input(id='pipeline-job-schedule', placeholder='every 5 minutes', size='sm')], md=4),
                dbc.Col(dbc.Button('Create scheduled job', id='pipeline-job-create', size='sm'), md=4),
            ], className='g-2'),
            dbc.Row([
                dbc.Col([dbc.Label('Workers', size='sm'), dbc.Input(id='pipeline-job-workers',
                         type='number', min=1, step=1, value=4, size='sm')], md=3),
                dbc.Col([dbc.Label('Retries', size='sm'), dbc.Input(id='pipeline-job-retries',
                         type='number', min=0, step=1, value=0, size='sm')], md=3),
                dbc.Col(dbc.Checkbox(id='pipeline-job-force', label='Allow rebuilds', value=False), md=4),
            ], className='g-2 mt-1 mb-3'),
            html.Div(id='pipeline-jobs'),
        ])), id='pipeline-manage-collapse', is_open=False, class_name='mb-2'),
        dcc.Store(id='pipeline-definition', data=None),
        dcc.Store(id='pipeline-definitions', data={}),
        dcc.Store(id='pipeline-live-run', data=None),
        html.Div(id='pipeline-summary'),
        html.Div(id='pipeline-run-alert'),
        html.Div(id='pipeline-run-stream-dummy', hidden=True),
        html.Div(id='pipeline-run-status'),

    ])
