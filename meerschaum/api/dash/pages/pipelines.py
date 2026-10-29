#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The Pipelines pages, backed by the Syncing Engine:

- `/dash/pipelines`: the Pipelines registered on the console's global instance (the registry);
- `/dash/pipelines/new`: the New pipeline stepper (Name, Members, Traversal, Review);
- `/dash/pipelines/explore?pipe=…`: the connected graph of one Pipe, read-only;
- `/dash/pipelines/<name>`: one Pipeline (Canvas and Runs, read-only until Edit).

Each is built per request (`route`), so it carries its registry and name.
"""

from urllib.parse import parse_qs, urlparse, unquote, urlencode, quote

from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.api.dash.components import build_pages_navbar
from meerschaum.api.dash.pipeline_management import (
    build_member_panel, build_detail_header, build_manage_dialogs,
)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

from meerschaum.connectors import instance_types
from meerschaum.utils.misc import get_connector_labels

STATUS_OPTIONS = [
    {'label': 'Any status', 'value': 'any'},
    {'label': 'Running', 'value': 'running'},
    {'label': 'Succeeded', 'value': 'succeeded'},
    {'label': 'Failed', 'value': 'failed'},
]

#: Path segments that are pages, not Pipeline names.
RESERVED = ('new', 'explore')


def _query(href):
    return parse_qs(urlparse(href or '').query)


def _registry(query):
    from meerschaum.api.dash.engine import get_instance
    return str(get_instance(query.get('instance', [None])[0]))


def _instance_options():
    return [{'label': key, 'value': key} for key in get_connector_labels(*instance_types)]


def _navbar(registry):
    navbar = build_pages_navbar(instance_select=True)
    for component in _walk(navbar):
        if getattr(component, 'id', None) == 'pages-instance-select':
            component.value = registry
    return navbar


def _walk(node):
    if isinstance(node, (list, tuple)):
        for child in node:
            yield from _walk(child)
        return
    if node is None or isinstance(node, (str, int, float)):
        return
    yield node
    yield from _walk(getattr(node, 'children', None))


def route(pathname, href=None):
    """The layout of a Pipelines path. Legacy `?pipeline=` / `?name=` links redirect."""
    query = _query(href)
    rest = (pathname or '')[len('/dash/pipelines'):].strip('/')
    if not rest:
        legacy = (query.get('pipeline') or query.get('name') or [None])[0]
        # Old standalone lineage links carried a pipe and sometimes the literal `None` name.
        # They describe a Pipe graph, not a saved Pipeline.
        if query.get('pipe') and legacy in (None, '', 'None', 'null'):
            keep = {key: values[0] for key, values in query.items()
                    if key in ('pipe', 'instance') and values and values[0]}
            return [html.Div(**{'data-mrsm-redirect': '/dash/pipelines/explore?' + urlencode(keep)})]
        if legacy:
            keep = {key: values[0] for key, values in query.items()
                    if key in ('instance', 'run_id', 'run') and values and values[0]}
            if 'run' in keep:
                keep['run_id'] = keep.pop('run')
            target = '/dash/pipelines/' + quote(legacy, safe='') + (('?' + urlencode(keep)) if keep else '')
            return [html.Div(**{'data-mrsm-redirect': target})]
    if rest == 'explore' and not query.get('instance'):
        ref = (query.get('pipe') or [''])[0]
        if ref.count('|') == 3:
            query['instance'] = [ref.split('|', 1)[0]]
    registry = _registry(query)
    if not rest:
        layout = list_layout(registry)
    elif rest == 'new':
        layout = new_layout(registry, query)
    elif rest == 'explore':
        layout = explore_layout(registry, query)
    else:
        layout = detail_layout(unquote(rest.split('/', 1)[0]), registry)
    return _with_instance_alert(layout, query)


def _with_instance_alert(layout, query):
    """A refused (or invalid) requested or global instance is named above the page."""
    from meerschaum.api.dash.engine import instance_problem
    problem = instance_problem(query.get('instance', [None])[0])
    if not problem:
        return layout
    main = layout[-1]
    main.children = [dbc.Alert(problem, color='danger', id='pipelines-instance-alert',
                               className='pipelines-instance-alert')] + list(main.children)
    return layout


def _may_register() -> bool:
    """Whether the viewer may create Pipelines (`pipes:register`)."""
    from meerschaum.api.dash import access
    try:
        return access.current_viewer().can('pipes:register')
    except Exception:
        return False


def list_layout(registry):
    """The registry's runbook, with local search and sorting above its rows."""
    return [
        _navbar(registry),
        dcc.Location('pipelines-list-location', refresh=False),
        html.Main([
            html.Div([
                html.H3('Pipelines'),
                (dbc.Button('New pipeline', href='/dash/pipelines/new', size='sm', id='pipelines-new-button')
                 if _may_register() else html.Span()),
                dcc.Dropdown(id='pipelines-find-pipe', options=[], placeholder='Find Pipe',
                             className='pipelines-find-pipe', clearable=True),
            ], className='pipelines-list-header'),
            html.Div(id='pipelines-list-alert'),
            html.Div([
                html.Label('Search Pipelines', htmlFor='pipelines-search', className='visually-hidden'),
                dbc.Input(id='pipelines-search', type='search', placeholder='Search Pipelines'),
                html.Select([
                    html.Option('Name A to Z', value='name'),
                    html.Option('Name Z to A', value='name-desc'),
                    html.Option('Most recent run', value='recent'),
                    html.Option('Run status', value='status'),
                    html.Option('Most members', value='members'),
                ], id='pipelines-sort', **{'aria-label': 'Sort Pipelines'}),
                html.Span(id='pipelines-visible-count', role='status', **{'aria-live': 'polite'}),
            ], className='pipelines-list-tools'),
            ### Lazy row history never hides or replaces the runbook.
            dcc.Loading(html.Div(id='pipelines-list', className='pipelines-list'), type='dot',
                        target_components={'pipelines-list': 'children'}),
        ], id='pipelines-list-page', className='pipeline-page', **{'data-registry': registry}),
    ]


def _access_fields():
    """The New pipeline stepper's Team and Visibility, prefilled with the viewer's primary team."""
    from meerschaum.api.dash import access
    from meerschaum.api.dash.new_pipe import team_options
    try:
        teams, team = team_options(access.current_viewer())
    except Exception:
        teams, team = [{'label': access.EVERYONE, 'value': access.EVERYONE}], access.EVERYONE
    return [
        html.Div([
            dbc.Label('Team', html_for='pipeline-new-team', size='sm'),
            dbc.Select(id='pipeline-new-team', options=teams, value=team, size='sm',
                       class_name='dbc_dark custom-select custom-select-sm'),
        ], className='pipeline-new-field'),
        html.Div([
            dbc.Label('Visibility', html_for='pipeline-new-visibility', size='sm'),
            dbc.Select(id='pipeline-new-visibility', size='sm', value='team',
                       options=[{'label': label, 'value': value} for value, label in access.VISIBILITIES],
                       class_name='dbc_dark custom-select custom-select-sm'),
        ], className='pipeline-new-field'),
    ]


def new_layout(registry, query):
    """The New pipeline stepper; nothing is saved before Create."""
    members = [ref for value in query.get('members', []) for ref in value.split(',') if ref]
    steps = [('name', 'Name'), ('members', 'Members'), ('traversal', 'Traversal'), ('review', 'Review')]
    return [
        _navbar(registry),
        dcc.Location('pipeline-new-location', refresh=False),
        dcc.Store(id='pipelines-navigate'),
        dcc.Store(id='pipeline-new-prefill', data={'members': members, 'registry': registry}),
        dcc.Store(id='pipeline-new-drafts', storage_type='local'),
        dcc.Store(id='pipeline-new-draft', data=None),
        html.Main([
            html.Div([
                html.H3('New pipeline'),
                dbc.Badge(registry, color='secondary', className='pipeline-registry-badge'),
            ], className='pipelines-list-header'),
            html.Div([
                dbc.Button('Resume draft', id='pipeline-new-resume', size='sm', color='warning'),
                dbc.Button('Start over', id='pipeline-new-start-over', size='sm', color='secondary'),
            ], id='pipeline-new-restore', className='pipeline-draft-restore', hidden=True),
            html.Ol([
                html.Li(html.Button(label, id={'type': 'pipeline-new-step', 'index': key}, n_clicks=0),
                        **{'data-step': key})
                for key, label in steps
            ], className='pipeline-stepper', id='pipeline-new-stepper'),
            html.Section([
                dbc.Input(id='pipeline-new-name', placeholder='Pipeline name', size='sm', debounce=False),
                ### Who the new Pipeline belongs to: the viewer's primary team, visible to it.
                html.Div(_access_fields(), className='pipeline-new-access'),
            ], className='pipeline-new-step', **{'data-step': 'name'}),
            html.Section([
                build_member_panel('pipeline-new', registry, _instance_options()),
            ], className='pipeline-new-step', **{'data-step': 'members'}),
            html.Section([
                dbc.Row([
                    dbc.Col([dbc.Checkbox(id='pipeline-new-parents', label='Include parents', value=False),
                             dbc.Input(id='pipeline-new-parents-depth', type='number', min=1, step=1,
                                       placeholder='All levels', size='sm')], md=6),
                    dbc.Col([dbc.Checkbox(id='pipeline-new-children', label='Include children', value=False),
                             dbc.Input(id='pipeline-new-children-depth', type='number', min=1, step=1,
                                       placeholder='All levels', size='sm')], md=6),
                ], className='g-3'),
            ], className='pipeline-new-step', **{'data-step': 'traversal'}),
            html.Section([
                dcc.Loading(html.Div(id='pipeline-new-review'), type='dot',
                            target_components={'pipeline-new-review': 'children'}),
            ], className='pipeline-new-step', **{'data-step': 'review'}),
            html.Div(id='pipeline-new-alert'),
            html.Div([
                dbc.Button('Back', id='pipeline-new-back', size='sm', color='secondary'),
                dbc.Button('Next', id='pipeline-new-next', size='sm'),
                dbc.Checkbox(id='pipeline-new-apply-tags', label='Apply these tags', value=True,
                             className='pipeline-new-review-only'),
                dbc.Button('Create', id='pipeline-new-create', size='sm', color='success',
                           className='pipeline-new-review-only'),
            ], className='pipeline-new-actions'),
        ], id='pipeline-new-page', className='pipeline-page', **{'data-step': 'name', 'data-registry': registry}),
    ]


def explore_layout(registry, query):
    """One Pipe's connected graph, read-only, with Save as Pipeline."""
    ref = (query.get('pipe') or [''])[0]
    return [
        _navbar(registry),
        dcc.Location('pipeline-explore-location', refresh=False),
        dcc.Store(id='pipeline-explore-ref', data=ref),
        html.Main([
            html.Div([
                html.H3(id='pipeline-explore-title'),
                dbc.Button('Save as Pipeline', id='pipeline-explore-save', size='sm', href='/dash/pipelines/new',
                           style=(None if _may_register() else {'display': 'none'})),
            ], className='pipelines-list-header'),
            dcc.Loading(html.Div(id='pipeline-explore-graph', className='pipeline-workspace pipeline-explore'),
                        target_components={'pipeline-explore-graph': 'children'},
                        type='dot'),
        ], id='pipeline-explore-page', className='pipeline-page', **{'data-registry': registry}),
    ]


def _pipeline_rights(name, registry):
    from meerschaum.api.dash import access
    from meerschaum.api.dash.pipeline_management import pipeline_rights
    try:
        return pipeline_rights(access.current_viewer(), name, registry)
    except Exception:
        return {}


def detail_layout(name, registry):
    """One Pipeline: header, Canvas | Runs, Edit mode, output and Manage dialogs."""
    return [
        _navbar(registry),
        dcc.Location('pipelines-location', refresh=False),
        dcc.Store(id='pipelines-navigate'),
        dcc.Interval(id='pipelines-interval', interval=(15 * 1000), n_intervals=0),
        html.Main(
            [
                build_detail_header(name, registry, rights=_pipeline_rights(name, registry)),
                html.Div(id='pipelines-alert-div'),
                html.Div(id='pipeline-run-alert'),
                html.Div(id='pipeline-manage-alert'),
                html.Div(id='pipeline-edit-alert'),
                ### Compatibility: the selected Pipeline and its saved name, fixed by the path.
                dcc.Dropdown(id='pipeline-saved-name', options=[{'label': name, 'value': name}],
                             value=name, clearable=False, style={'display': 'none'}),
                dbc.Input(id='pipeline-name-input', value=name, type='hidden'),
                dcc.Store(id='pipeline-definition', data=None),
                dcc.Store(id='pipeline-definitions', data={}),
                dcc.Store(id='pipeline-live-run', data=None),
                html.Div(id='pipeline-run-stream-dummy', hidden=True),
                dcc.Store(id='pipeline-edit-mode', data=False),
                html.Div(id='pipeline-edit-mode-dummy', hidden=True),
                dcc.Store(id='pipeline-edge-proposal', data=None),
                dcc.Store(id='pipeline-canvas-prefs', storage_type='local'),
                dcc.Store(id='pipeline-visible-refs', data=[]),
                dcc.Store(id='pipeline-selection'),
                dcc.Store(id='pipeline-selected-run'),
                dcc.Store(id='pipeline-inspector-result'),
                html.Div(id='pipeline-inspector-guard', hidden=True),
                dcc.Store(id='pipeline-context'),
                dcc.Store(id='pipeline-node-run'),
                dcc.Store(id='pipeline-node-runs', data=[]),
                dcc.Store(id='pipeline-node-events'),
                ### The selected run of this Pipeline: its members' outputs are scoped to it.
                dcc.Store(id='pipeline-current-run', data=None),
                dcc.Store(id='pipeline-member-output', data=None),
                html.Div(id='pipeline-current-run-dummy', hidden=True),
                dcc.Store(id='pipeline-node-statuses', data={}),
                dcc.Store(id='pipeline-analysis'),
                dcc.Store(id='pipeline-stage-statuses', data={}),
                html.Div(id='pipeline-stage-status-dummy', hidden=True),
                ### Live runs: the run to follow over `/engine/runs/{run_id}/events/stream`, which
                ### runs stream (`assets/pipeline_events.js`), and the refresh its events ask for.
                dcc.Store(id='pipeline-run-follow', data=None),
                dcc.Store(id='pipeline-run-streams', data={}),
                dcc.Store(id='pipeline-run-tick', data=0),
                html.Div(id='pipeline-run-follow-dummy', hidden=True),
                html.Div([
                    html.Div([
                        html.Button('Canvas', id='pipeline-view-workspace', role='tab', **{'aria-selected': 'true'}),
                        html.Button('Runs', id='pipeline-view-runs', role='tab', **{'aria-selected': 'false'}),
                    ], className='pipeline-view-tabs', role='tablist', **{'aria-label': 'Pipeline views'}),
                    dbc.Button('Output', id='pipeline-view-output', size='sm', color='secondary', disabled=True),
                ], className='pipeline-tabs-row'),
                html.Section([
                    html.Div([
                        html.Details([
                            html.Summary('Add Pipe', title='Close'),
                            build_member_panel('pipeline', registry, _instance_options()),
                            dbc.Row([
                                dbc.Col(dbc.Checkbox(id='pipeline-draft-parents', label='Include parents', value=False)),
                                dbc.Col(dbc.Checkbox(id='pipeline-draft-children', label='Include children', value=False)),
                            ]),
                        ], id='pipeline-draft-editor'),
                        dcc.Store(id='pipeline-draft'),
                        dcc.Store(id='pipeline-layout-draft'),
                        html.Div(id='pipelines-graph-div'),
                        html.Aside(id='pipeline-inspector', hidden=True),
                        html.Div([
                            html.Div([
                                dbc.Button('Cancel sync', id='pipeline-cancel-node-run', color='danger', outline=True,
                                           size='sm', style={'display': 'none'}),
                                html.Button('×', id='pipeline-output-close', title='Close output'),
                            ], className='pipeline-output-header'),
                            html.Div([
                                dcc.Dropdown(id='pipeline-output-tab', options=[], placeholder='Pipe output',
                                             clearable=False),
                                html.Div(id='pipeline-node-run-label'),
                                html.Div(id='pipeline-cancel-alert'),
                                html.Div(id='pipeline-node-stream-dummy', hidden=True),
                                html.Pre(id='pipeline-node-output', className='pipelines-events'),
                                html.Div(id='pipeline-node-alert'),
                            ], id='pipeline-pipe-output'),
                            html.Div([html.Div(id='pipeline-command-label'),
                                      html.Pre(id='pipeline-command-output', className='pipelines-events')],
                                     id='pipeline-whole-output'),
                        ], id='pipeline-output-dock', className='pipeline-output-empty pipeline-output-expanded'),
                    ], className='pipeline-workspace', id='pipeline-workspace',
                        **{'data-pane': 'graph', 'data-edit': 'false', 'data-dirty': 'false'}),
                ], id='pipeline-workspace-panel', role='tabpanel', **{'aria-labelledby': 'pipeline-view-workspace'}),
                html.Section([
                    html.Details([html.Summary('Filters'), dbc.Card(dbc.CardBody(dbc.Row([
                        dbc.Col(dbc.Input(id='pipelines-run-id-input', placeholder='Run ID', type='text',
                                          debounce=True, size='sm'), md=4),
                        dbc.Col(dbc.Input(id='pipelines-pipe-input', placeholder="Pipe('sql:main', 'counts')",
                                          type='text', debounce=True, size='sm'), md=4),
                        dbc.Col(dbc.Select(id='pipelines-status-select', options=STATUS_OPTIONS, value='any',
                                           size='sm', class_name='dbc_dark custom-select custom-select-sm'), md=2),
                        dbc.Col(dbc.Button('Filter runs', id='pipelines-filter-button', size='sm',
                                           color='primary', style={'width': '100%'}), md=2),
                    ], className='g-2')))], className='pipeline-run-filters'),
                    html.Div(id='pipelines-runs-div'),
                    html.Div(id='pipeline-run-status'),
                    html.Div(id='pipelines-gantt-div'),
                    html.Details([
                        html.Summary('Output'),
                        dcc.Dropdown(id='pipeline-log-stage', options=[], value='',
                                     placeholder='All members', clearable=True),
                        html.Pre('', id='pipelines-events-pre', className='pipelines-events'),
                    ], className='pipeline-activity', open=True),
                ], id='pipeline-runs-panel', role='tabpanel', **{'aria-labelledby': 'pipeline-view-runs'}),
                dcc.Store(id='pipelines-events-store', data={}),
                build_manage_dialogs(),
            ],
            id='pipelines-page', className='pipeline-page pipeline-detail-page',
            **{'data-view': 'workspace', 'data-pipeline': name, 'data-registry': registry},
        ),
    ]
