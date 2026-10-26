"""
The pieces of the Pipelines pages: the styled `Pipeline('name')`, the member panel shared by the
New pipeline stepper and Edit mode, the detail page's header, Edit bar and Manage dialogs.
"""
from meerschaum.utils.packages import import_html, import_dcc
import dash_bootstrap_components as dbc
html, dcc = import_html(), import_dcc()


def pipeline_title(name, registry=None, link=False):
    """
    `Pipeline('name')` styled like the CLI's `pipeline_repr()` (a bold `Pipeline(` and `)`, the
    name like a metric key), with the registry as a badge.
    """
    title = html.Span([
        html.Span('Pipeline(', className='mrsm-repr-type'),
        html.Span(repr(str(name)), className='mrsm-repr-name'),
        html.Span(')', className='mrsm-repr-type'),
    ], className='mrsm-pipeline-repr')
    if link:
        from urllib.parse import quote
        title = html.A(title, href='/dash/pipelines/' + quote(str(name), safe=''),
                       className='pipeline-title-link')
    parts = [title]
    if registry:
        parts.append(dbc.Badge(str(registry), color='secondary', className='pipeline-registry-badge'))
    return parts


def runs_placeholder():
    """A card's Runs before they load: a fixed-height slot, replaced in place."""
    return html.Div(dbc.Spinner(size='sm', color='secondary'), className='pipeline-runs-placeholder',
                    role='status', **{'aria-label': 'Loading runs'})


def count_label(count, noun):
    """`1 member`, `2 members`."""
    return f"{count} {noun}" + ('' if count == 1 else 's')


def build_member_panel(prefix, registry, instance_options):
    """
    Search existing Pipes on any instance and register new ones (applied when the draft is
    saved or created). `prefix` keeps the stepper's and Edit mode's ids apart.
    """
    return html.Div([
        dcc.Dropdown(id=f'{prefix}-draft-members', options=[], multi=True, closeOnSelect=True,
                     placeholder='Members'),
        html.Div([
            dcc.Dropdown(id=f'{prefix}-add-instance', options=instance_options, value=str(registry),
                         clearable=False, placeholder='Instance', className='pipeline-add-instance'),
            dbc.Button('Find Pipes', id=f'{prefix}-add-find', size='sm', color='secondary'),
        ], className='pipeline-add-find-row'),
        html.Details([
            html.Summary('New Pipe'),
            dbc.Input(id=f'{prefix}-add-connector', placeholder='Connector keys', size='sm'),
            dbc.Input(id=f'{prefix}-add-metric', placeholder='Metric key', size='sm'),
            dbc.Input(id=f'{prefix}-add-location', placeholder='Location key', size='sm'),
            html.Details([
                html.Summary('Parameters'),
                dbc.Textarea(id=f'{prefix}-add-parameters', value='{}', placeholder='{}'),
            ], className='pipeline-add-parameters'),
            dbc.Button('Add new Pipe', id=f'{prefix}-add-register', size='sm'),
        ], className='pipeline-add-new'),
        html.Div(id=f'{prefix}-add-alert'),
        dcc.Store(id=f'{prefix}-member-options', data={'options': []}),
    ], className='pipeline-member-panel')


def build_detail_header(name, registry):
    """The detail page's header: identity, then Sync, Plan, Edit and Manage."""
    return html.Div([
        html.Div([
            html.H3(pipeline_title(name), id='pipeline-title'),
            dbc.Badge(str(registry), color='secondary', className='pipeline-registry-badge'),
            dbc.Badge('Draft', id='pipeline-draft-badge', color='warning', className='pipeline-draft-badge'),
            html.Span(id='pipeline-draft-notice', className='pipeline-unsaved-count'),
        ], className='pipeline-identity'),
        html.Div([
            dbc.Button('Sync', id='pipeline-run-now', n_clicks=0, size='sm'),
            dbc.Button('Cancel run', id='pipeline-cancel-current', n_clicks=0, size='sm', color='danger',
                       outline=True, style={'display': 'none'}),
            dbc.Button('Plan', id='pipeline-plan-open', n_clicks=0, size='sm', color='secondary'),
            dbc.Button('Edit', id='pipeline-edit-toggle', n_clicks=0, size='sm', color='secondary',
                       className='pipeline-read-only'),
            dbc.DropdownMenu([
                dbc.DropdownMenuItem('Analyze tags', id='pipeline-analyze', n_clicks=0),
                dbc.DropdownMenuItem('Schedule', id='pipeline-manage-schedule', n_clicks=0),
                dbc.DropdownMenuItem('Copy', id='pipeline-manage-copy', n_clicks=0),
                dbc.DropdownMenuItem('Rename', id='pipeline-manage-rename', n_clicks=0),
                dbc.DropdownMenuItem(divider=True),
                dbc.DropdownMenuItem('Clear', id='pipeline-manage-clear', n_clicks=0),
                dbc.DropdownMenuItem('Drop', id='pipeline-manage-drop', n_clicks=0),
                dbc.DropdownMenuItem('Delete', id='pipeline-delete', n_clicks=0),
            ], label='Manage', direction='up', size='sm', color='secondary', menu_variant='dark',
                id='pipeline-manage-menu', className='pipeline-read-only'),
            html.Div([
                dbc.Button('Undo', id='pipeline-draft-undo', size='sm', color='secondary'),
                dbc.Button('Redo', id='pipeline-draft-redo', size='sm', color='secondary'),
                dbc.Button('Discard', id='pipeline-draft-discard', size='sm', color='secondary'),
                dbc.Button('Save', id='pipeline-draft-save', size='sm', color='primary'),
            ], className='pipeline-edit-actions pipeline-edit-only'),
            dbc.Button('↻', id='pipelines-refresh-button', size='sm', color='black', title='Refresh'),
        ], className='pipeline-header-actions'),
        html.Div([
            dbc.Button('Resume draft', id='pipeline-draft-resume', size='sm', color='warning'),
            dbc.Button('Start over', id='pipeline-draft-start-over', size='sm', color='secondary'),
        ], id='pipeline-draft-restore', className='pipeline-draft-restore', hidden=True),
    ], className='pipeline-detail-header')


def build_manage_dialogs():
    """Plan, Analyze, Schedule, Copy and Rename dialogs, and the destructive confirmations."""
    modal = lambda id, title, body, footer=None, size='lg': dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle(title)),
        dbc.ModalBody(body),
        *([dbc.ModalFooter(footer)] if footer else []),
    ], id=id, is_open=False, size=size, scrollable=True, className='pipeline-dialog')
    return html.Div([
        modal('pipeline-plan-modal', 'Plan', html.Div(id='pipeline-plan-body'), size='xl'),
        modal('pipeline-analyze-modal', 'Analyze tags', [
            html.Div(id='pipeline-analysis-alert'),
            html.Div(id='pipeline-analysis-preview'),
        ], dbc.Button('Apply proposed tags', id='pipeline-apply-tags', color='warning', size='sm',
                      disabled=True)),
        modal('pipeline-schedule-modal', 'Schedule', [
            html.Div(id='pipeline-job-settings-notice'),
            dbc.Row([
                dbc.Col([dbc.Label('Job name', size='sm'),
                         dbc.Input(id='pipeline-job-name', placeholder='Job name', size='sm')], md=6),
                dbc.Col([dbc.Label('Schedule', size='sm'),
                         dbc.Input(id='pipeline-job-schedule', placeholder='every 5 minutes', size='sm')], md=6),
            ], className='g-2'),
            dbc.Row([
                dbc.Col([dbc.Label('Workers', size='sm'), dbc.Input(id='pipeline-job-workers',
                         type='number', min=1, step=1, value=4, size='sm')], md=4),
                dbc.Col([dbc.Label('Retries', size='sm'), dbc.Input(id='pipeline-job-retries',
                         type='number', min=0, step=1, value=0, size='sm')], md=4),
                dbc.Col(dbc.Checkbox(id='pipeline-job-force', label='Allow rebuilds', value=False),
                        md=4, className='pipeline-job-force'),
            ], className='g-2 mt-1 mb-3'),
            dbc.Button('Create scheduled job', id='pipeline-job-create', size='sm'),
            html.Div(id='pipeline-jobs', className='mt-3'),
        ]),
        modal('pipeline-copy-modal', 'Copy', [
            dbc.Input(id='pipeline-copy-name', placeholder='New Pipeline name', size='sm'),
            html.Div(id='pipeline-copy-alert'),
        ], dbc.Button('Copy', id='pipeline-copy-confirm', size='sm')),
        modal('pipeline-rename-modal', 'Rename', [
            dbc.Input(id='pipeline-rename-name', placeholder='New Pipeline name', size='sm'),
            html.Div(id='pipeline-rename-alert'),
        ], dbc.Button('Rename', id='pipeline-rename-confirm', size='sm')),
        dcc.ConfirmDialog(id='pipeline-destructive-confirm'),
        dcc.Store(id='pipeline-destructive-action'),
        dcc.ConfirmDialog(id='pipeline-delete-confirm'),
        dcc.Store(id='pipeline-delete-target'),
    ])
