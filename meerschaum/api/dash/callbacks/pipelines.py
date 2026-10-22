#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Callbacks of the pipelines page (`/dash/pipelines`).
"""

from __future__ import annotations

import json

from urllib.parse import parse_qs, urlencode

import dash
from dash.dependencies import Input, Output, State, ALL
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional
from meerschaum.utils.warnings import warn
from meerschaum.api import CHECK_UPDATE
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.api.dash.sessions import is_session_authenticated
from meerschaum.api.dash import engine as _engine
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)

#: The most lines of events the page shows.
MAX_EVENT_LINES: int = 500


def _filters_from_search(search: Optional[str]) -> Dict[str, str]:
    query = parse_qs((search or '').lstrip('?'))
    filters = {key: (query.get(key, [''])[0] or '') for key in ('run_id', 'pipe', 'status')}
    if filters['status'] == 'any':
        filters['status'] = ''
    return filters


def build_graph_children(graph: Dict[str, Any], failed_refs: List[str], definitions=None) -> List[Any]:
    """Named membership groups with independently selectable pipe cards."""
    visible = {node['pipe_ref'] for node in graph['nodes']}
    groups = []
    covered = set()
    for name, definition in sorted((definitions or {}).items()):
        refs = visible.intersection(definition.get('targets', []))
        for direction, key in (('parent', 'parents'), ('child', 'children')):
            depth = definition.get(key)
            frontier = set(refs)
            for _ in range(len(visible) if depth is True else int(depth or 0)):
                added = {e[direction] for e in graph['edges']
                         if e['child' if direction == 'parent' else 'parent'] in frontier}
                added = (added & visible) - refs
                if not added:
                    break
                refs.update(added)
                frontier = added
        if refs:
            groups.append((name, refs))
            covered.update(refs)
    if visible - covered:
        groups.append(('', visible - covered))
    cards, connections = [], []
    offset, width = 0, 390
    for name, refs in groups:
        subset = {**graph, 'nodes': [n for n in graph['nodes'] if n['pipe_ref'] in refs],
                  'edges': [e for e in graph['edges'] if e['parent'] in refs and e['child'] in refs]}
        positions = _engine.canvas_positions(subset)
        group_width = max(x for x, _ in positions.values()) * 380 + 390
        group_height = max(y for _, y in positions.values()) * 190 + 240
        if name:
            cards.append(html.Div([
                html.Button(f"{name} · {len(refs)} pipes",
                            id={'type': 'pipeline-group', 'index': name}, n_clicks=0,
                            className='pipeline-group-title', title='Inspect pipeline'),
            ], className='pipeline-group', style={'left': '12px', 'top': f'{offset + 8}px',
                 'width': f'{group_width - 12}px', 'height': f'{group_height - 16}px'},
                 **{'data-pipeline': name}))
        for node in subset['nodes']:
            ref = node['pipe_ref']
            x, y = positions[ref]
            status = 'failed' if ref in failed_refs else node['status']
            key = json.dumps([name, ref])
            cards.append(html.Button([
                _engine.action_indicator(node['action'], failed=ref in failed_refs),
                html.Span('▤', className='pipeline-node-output-icon', role='button', tabIndex=0, title='View Pipe output', **{'aria-label': 'View Pipe output'}),
                html.Code(node['label']),
                html.Small(_engine.split_ref(ref)[0], className='pipeline-instance-badge'),
                html.Small(' · explicit member' if ref in (definitions or {}).get(name, {}).get('targets', [])
                           else ' · detected traversal' if name else ' · unsaved selection'),
            ], id={'type': 'pipeline-stage', 'index': key}, n_clicks=0,
                className='pipeline-stage', title=node['label'],
                style={'left': f'{32 + x * 380}px', 'top': f'{offset + 64 + y * 190}px',
                       '--stage-color': _engine.STATUS_COLORS.get(status, '#9aa5b1')},
                **{'data-ref': ref, 'data-node': key, 'data-grouped': 'true' if name else 'false', 'aria-label': f"Inspect {node['label']}"}))
        connections.extend({'parent': json.dumps([name, e['parent']]),
                            'child': json.dumps([name, e['child']])} for e in subset['edges'])
        width = max(width, group_width)
        offset += group_height + 32
    node_keys = {}
    for card in cards:
        props = card.to_plotly_json()['props']
        if props.get('data-ref'):
            node_keys.setdefault(props['data-ref'], props['data-node'])
    for edge in graph['edges']:
        if not any(json.loads(e['parent'])[1] == edge['parent']
                   and json.loads(e['child'])[1] == edge['child'] for e in connections):
            if edge['parent'] in node_keys and edge['child'] in node_keys:
                connections.append({'parent': node_keys[edge['parent']], 'child': node_keys[edge['child']]})
    plan_positions = _engine.canvas_positions(graph)
    return [
        html.Div([
            html.Div([
                html.Span(f"{len(visible)} pipes · {len(graph['edges'])} connections"),
                html.Div([
                    dbc.Input(id='pipeline-canvas-search', placeholder='Find Pipe', type='search', size='sm'),
                    html.Button('Fit', id='pipeline-fit', title='Fit pipeline to view'),
                    html.Button('Fullscreen', id='pipeline-fullscreen', title='Toggle fullscreen workspace'),
                    html.Button('Add Pipe', id='pipeline-add-node', title='Add a Pipe to the draft'),
                    dbc.Button('Save layout', id='pipeline-save-layout', n_clicks=0, size='sm', title='Save positions for everyone'),
                    html.Div(id='pipeline-layout-alert'),
                    html.Details([
                        html.Summary('View options'),
                        html.Div([
                            html.Button('−', id='pipeline-zoom-out', title='Zoom out'),
                            html.Button('+', id='pipeline-zoom-in', title='Zoom in'),
                            html.Button('Center selected', id='pipeline-center'),
                            html.Button('Collapse groups', id='pipeline-collapse-groups', disabled=not any(name for name, refs in groups)),
                            html.Select([html.Option(label, value=value) for label, value in [
                                ('All statuses', ''), ('Queued', 'pending'), ('Running', 'running'),
                                ('Succeeded', 'succeeded'), ('Failed', 'failed'), ('Skipped', 'skipped'),
                                ('Blocked', 'upstream_failed'),
                            ]], id='pipeline-canvas-status', **{'aria-label':'Filter Pipe status'}),
                        ], className='pipeline-view-options-body'),
                    ], className='pipeline-view-options'),
                ], className='pipeline-canvas-controls'),
            ], className='pipeline-canvas-toolbar'),
            html.Div(html.Div(cards, id='pipeline-canvas-world', key=json.dumps([graph, definitions], sort_keys=True), style={
                'width': f'{width}px', 'height': f'{offset}px',
            }, **{'data-edges': json.dumps(connections), 'data-focus': graph.get('focus', ''),
                  'data-layout': json.dumps((next(iter((definitions or {}).values()), {}).get('layout') or {})
                                            if len(definitions or {}) == 1 else {}),
                  'data-pipeline': next(iter(definitions or {}), '') if len(definitions or {}) == 1 else ''}),
                id='pipeline-canvas', tabIndex=0,
                **{'aria-label': 'Pipeline stages. Drag to pan; use the zoom controls.'}),
        ], className='pipeline-canvas-shell'),
        html.P('No Pipes in this selection. Use Add Pipe to choose members.') if not visible else None,
        html.Details([html.Summary('Plan · dependency order and readiness'),
            html.P('Actions and bounds are evaluated again when each Pipe becomes ready. Worker limits constrain actual concurrency.'),
            html.Div(dbc.Table([html.Thead(html.Tr([html.Th('Pipe'), html.Th('Action'), html.Th('Readiness'), html.Th('Bounds')])),
                html.Tbody([html.Tr([html.Td(html.Code(_engine.ref_label(node['pipe_ref']))),
                    html.Td(_engine.action_label(node['action'])), html.Td(node.get('reason', '')),
                    html.Td(str(node.get('bounds') or 'Source defaults'))])
                    for node in sorted(graph['nodes'], key=lambda node: plan_positions.get(node['pipe_ref'], (0, 0)))])], size='sm'),
                className='pipeline-table-scroll pipeline-plan-table', tabIndex=0, role='region', **{'aria-label':'Pipeline plan table'}),
        ], id='pipeline-plan-view'),
        *[html.P(message, className='text-warning') for message in graph['warnings']],
    ]


@dash_app.callback(
    Output('pipeline-focus', 'options'),
    Output('pipeline-focus-connector', 'options'),
    Output('pipeline-focus-metric', 'options'),
    Output('pipeline-focus-location', 'options'),
    Input('pipeline-focus-options', 'data'),
    Input('pipeline-focus-connector', 'value'),
    Input('pipeline-focus-metric', 'value'),
    Input('pipeline-focus-location', 'value'),
)
def filter_focus_options(options, connector, metric, location):
    """Find pipes through their connector, metric and location keys."""
    rows = [(option, tuple('None' if key is None else key
                           for key in _engine.split_ref(option['value'])[1:]))
            for option in (options or [])]
    selected = (connector, metric, location)
    def matches(keys, omit=None):
        return all(not value or index == omit or keys[index] == value
                   for index, value in enumerate(selected))
    key_options = [
        [{'label': '(None)' if value == 'None' else value,
          'value': 'None' if value is None else value}
         for value in sorted({keys[index] for _, keys in rows if matches(keys, index)},
                             key=lambda value: str(value))]
        for index in range(3)
    ]
    filtered = [option for option, keys in rows if matches(keys)]
    return filtered, *key_options


@dash_app.callback(
    Output('pipeline-inspector', 'children'),
    Input({'type': 'pipeline-stage', 'index': ALL}, 'n_clicks'),
    Input('pipeline-focus', 'value'),
    Input('pipeline-definition', 'data'),
    Input('pipeline-inspector-close', 'n_clicks', allow_optional=True),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def inspect_stage(clicks, focus, definition, close_clicks, session_data):
    """Read a selected stage without starting a sync."""
    if not is_session_authenticated((session_data or {}).get('session-id')):
        raise PreventUpdate
    triggered = dash.ctx.triggered_id
    if triggered in ('pipeline-inspector-close', 'pipeline-definition'):
        if triggered == 'pipeline-inspector-close' and not close_clicks:
            raise PreventUpdate
        dash.set_props('pipeline-selection', {'data': None})
        dash.set_props('pipeline-inspector', {'hidden': True})
        if triggered == 'pipeline-definition':
            dash.set_props('pipeline-focus', {'value': None})
        return []
    if triggered == 'pipeline-focus' and focus:
        ref = focus
    elif isinstance(triggered, dict) and any(clicks):
        _, ref = json.loads(triggered['index'])
    else:
        raise PreventUpdate
    dash.set_props('pipeline-inspector', {'hidden': False})
    import uuid
    selection = {'ref': ref, 'token': uuid.uuid4().hex}
    dash.set_props('pipeline-selection', {'data': selection})
    return [
        html.Div([html.H4(html.Code(_engine.ref_label(ref))),
                  html.Button('×', id='pipeline-inspector-close', n_clicks=0,
                              title='Close pipe details')], className='pipeline-inspector-header'),
        html.Small(_engine.split_ref(ref)[0], className='pipeline-instance-badge'),
        html.A('Open pipe', href=_engine.pipe_href(ref), target='_blank', rel='noopener noreferrer'),
        html.Div([
            dbc.Button('Sync this Pipe', id='pipeline-node-sync', size='sm'),
            dbc.Button('View output', id='pipeline-node-view-output', size='sm', color='secondary'),
        ], className='pipeline-node-actions'),
        html.Details([
            html.Summary('Sync options'),
            dcc.Dropdown(id='pipeline-node-scope', value='this', clearable=False, options=[
                {'label': 'This Pipe only', 'value': 'this'},
                {'label': 'Include all parents', 'value': 'parents'},
                {'label': 'Include all children', 'value': 'children'},
            ]),
            dbc.Input(id='pipeline-node-begin', placeholder='Begin (default: current watermark)', size='sm'),
            dbc.Input(id='pipeline-node-end', placeholder='End (default: available data)', size='sm'),
        ], className='pipeline-sync-options'),
        html.Label('Pipe details', htmlFor='pipeline-inspector-section'),
        dbc.Select(id='pipeline-inspector-section', value='query', size='sm', class_name='dbc_dark custom-select custom-select-sm',
                   options=[{'label': label, 'value': value} for value, label in [
                       ('query', 'Query'), ('plan', 'Data / Plan'), ('relations', 'Dependencies'),
                       ('runs', 'Recent syncs'), ('parameters', 'Parameters')]]),
        dbc.Switch(id='pipeline-resolve-symlinks', label='Resolve symlinks', value=True),
        dbc.Button('Reload', id='pipeline-inspector-retry', size='sm', color='link'),
        html.Div('Loading Pipe details…', id='pipeline-inspector-body', role='status'),
    ]


@dash_app.callback(
    Output('pipeline-inspector-result', 'data'),
    Input('pipeline-selection', 'data'),
    Input('pipeline-inspector-section', 'value', allow_optional=True),
    Input('pipeline-resolve-symlinks', 'value', allow_optional=True),
    Input('pipeline-inspector-retry', 'n_clicks', allow_optional=True),
    State('session-store', 'data'), prevent_initial_call=True,
)
def load_inspector_section(selection, section, resolve, retry, session):
    """Only fetch the selected section. Token checks on the client reject stale responses."""
    if not selection or not is_session_authenticated((session or {}).get('session-id')):
        raise PreventUpdate
    ref = selection['ref']
    instance, connector, metric, location = _engine.split_ref(ref)
    pipe = mrsm.Pipe(connector, metric, location, instance=instance)
    try:
        if section == 'query' or not section:
            from meerschaum.api.dash._support import get_pipe_query
            query = get_pipe_query(pipe, apply_symlinks=True if resolve is None else resolve, warn=False)
            from meerschaum.utils.packages import attempt_import
            dash_ace = attempt_import('dash_ace', lazy=False, check_update=False)
            body = [dash_ace.DashAceEditor(
                id='pipeline-query-view', value=query, mode='sql', theme='twilight',
                width='100%', height=f'{min(400, max(120, 20 * len(query.splitlines()) + 24))}px',
                readOnly=True, showGutter=True, showPrintMargin=False,
                highlightActiveLine=False, wrapEnabled=True, tabSize=4,
            ) if query else html.P('This source uses a connector or plugin to fetch data.'),
                    html.A('Open Pipe editor', href=_engine.pipe_href(ref), target='_blank')]
        elif section == 'edit':
            from meerschaum.api.dash.pipeline_editing import fingerprint, raw_query
            parameters = dict(pipe.get_parameters(apply_symlinks=False))
            body = [
                html.P('These edits change shared Pipe facts in every Pipeline. Stage them, review the draft, then Save pipeline. Inferred edges are edited through their SQL references.'),
                dcc.Store(id='pipeline-edit-baseline', data={'ref': ref, 'parameters': parameters, 'baseline': fingerprint(parameters)}),
                html.Label('Explicit parents (JSON Pipe references)'),
                dbc.Textarea(id='pipeline-edit-parents', value=json.dumps(parameters.get('parents', parameters.get('parent', [])), indent=2)),
                html.Label('Explicit children (JSON Pipe references)'),
                dbc.Textarea(id='pipeline-edit-children', value=json.dumps(parameters.get('children', parameters.get('child', [])), indent=2)),
                html.Label('Raw SQL definition (inferred dependencies)'),
                dbc.Textarea(id='pipeline-edit-query', value=raw_query(parameters), style={'minHeight': '180px'}),
                dbc.Button('Stage shared Pipe changes', id='pipeline-stage-edit', size='sm'),
                html.Div(id='pipeline-stage-edit-alert'),
            ]
        elif section == 'parameters':
            body = html.Pre(json.dumps(pipe.parameters, indent=2, default=str))
        elif section == 'relations':
            from meerschaum.api.dash.pipes import build_relations_body
            body = build_relations_body(pipe)
        elif section == 'runs':
            body = build_runs_table(_engine.get_runs(pipe.instance_connector, pipe=ref, limit=30),
                                    table_id='pipeline-stage-runs-table', registry=str(pipe.instance_connector))
        else:
            graph = _engine.plan_graph(pipe.instance_connector, [pipe], parents=1)
            planned = next((node for node in graph['nodes'] if node['pipe_ref'] == ref), {})
            body = [html.P(['Latest data: ', str(pipe.get_sync_time())]),
                    html.P(_engine.action_label(planned.get('action', ''))),
                    html.P(planned.get('reason', '')),
                    build_data_range(planned['bounds']) if planned.get('bounds') else None]
        # Dash serializes component trees in a Store exactly as it does callback children.
        return {'token': selection['token'], 'section': section or 'query',
                'resolve': True if resolve is None else resolve, 'children': body}
    except Exception as exc:
        return {'token': selection['token'], 'section': section or 'query',
                'resolve': True if resolve is None else resolve,
                'children': html.P(f'Could not load this section: {exc}. Use Reload section to retry.')}


dash_app.clientside_callback(
    """function(result, selection, section, resolve) {
        if (!result || !selection || result.token !== selection.token ||
            result.section !== (section || 'query') || result.resolve !== (resolve == null ? true : resolve))
            return dash_clientside.no_update;
        dash_clientside.set_props('pipeline-inspector-body', {children: result.children, style: {opacity: 1}});
        const inspector = document.getElementById('pipeline-inspector');
        if (inspector) inspector.removeAttribute('aria-busy');
        return dash_clientside.no_update;
    }""",
    Output('pipeline-inspector-guard', 'children'),
    Input('pipeline-inspector-result', 'data'), State('pipeline-selection', 'data'),
    State('pipeline-inspector-section', 'value', allow_optional=True),
    State('pipeline-resolve-symlinks', 'value', allow_optional=True),
)


@dash_app.callback(
    Output('pipeline-node-alert', 'children'),
    Input('pipeline-node-sync', 'n_clicks', allow_optional=True),
    Input('pipeline-context', 'data'),
    State('pipeline-selection', 'data'),
    State('pipeline-node-scope', 'value', allow_optional=True),
    State('pipeline-node-begin', 'value', allow_optional=True),
    State('pipeline-node-end', 'value', allow_optional=True),
    State('pipeline-saved-name', 'value'), State('pipeline-definition', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def sync_selected_pipe(clicks, context, selection, scope, begin, end, pipeline, definition, session):
    if not is_session_authenticated((session or {}).get('session-id')):
        raise PreventUpdate
    if dash.ctx.triggered_id == 'pipeline-context':
        if not context or context.get('action') != 'sync':
            raise PreventUpdate
        ref, scope = context['ref'], context.get('scope', 'this')
        if not selection or selection.get('ref') != ref:
            begin, end = None, None
    elif clicks and selection:
        ref = selection['ref']
    else:
        raise PreventUpdate
    from meerschaum.api.dash.execute import run_action
    from meerschaum.api.dash.sessions import get_username_from_session
    try:
        instance, connector, metric, location = _engine.split_ref(ref)
        args = ['sync', 'pipes', '-i', instance, '-c', connector, '-m', metric,
                '-l', location if location is not None else 'None']
        if scope in ('parents', 'children'):
            args.append('--' + scope)
        for flag, value in (('--begin', begin), ('--end', end)):
            if value:
                args.extend([flag, value])
        import uuid
        run_id = 'canvas-' + uuid.uuid4().hex
        args.extend(['--run-id', run_id])
        result = run_action(args, get_username_from_session((session or {}).get('session-id')),
                            metadata={'run_id': run_id, **({'pipeline': {key: definition.get(key)
                                for key in ('name', 'registry', 'id', 'revision')}}
                                if definition and definition.get('name') == pipeline else {})})
        if result['success']:
            dash.set_props('pipeline-node-run', {'data': {
                'ref': ref, 'scope': scope, 'run_id': run_id, 'instance': instance,
                'session_id': (session or {}).get('session-id'), 'begin': begin, 'end': end,
                'job': result['job'], 'stream': result['stream']}})
            dash.set_props('pipeline-node-run-label', {'children': f"{ref} · {scope} · {begin or 'current watermark'} → {end or 'available data'} · {run_id} · {result['job']}"})
            dash.set_props('pipelines-interval', {'interval': 2000})
        return alert_from_success_tuple((result['success'], result['msg']))
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))


def build_data_range(bounds):
    """Align sync bounds without applying monospace formatting to their labels."""
    from meerschaum.core.Pipeline import _bound_text
    rows = []
    for label, key, fallback in (('From', 'begin', 'Earliest available data'),
                                  ('To', 'end', 'No upper limit')):
        value = bounds.get(key)
        rows.append(html.Tr([
            html.Th(label, scope='row'),
            html.Td('≥' if key == 'begin' else '<',
                    title='Inclusive' if key == 'begin' else 'Exclusive'),
            html.Td(html.Pre(_bound_text(value)) if value is not None else html.Span(fallback)),
        ]))
    return html.Div([html.Div('Data range:'), html.Table(html.Tbody(rows))],
                    className='pipeline-data-range')


def build_runs_table(runs: List[Dict[str, Any]], table_id='pipelines-runs-table', registry=None, pipeline=None) -> Any:
    """
    The runs table: each run's ID links to its timeline.
    """
    if not runs:
        return html.P("No runs match.")
    rows = []
    for run in runs:
        record = run['run']
        counts = ', '.join(f"{key}: {value}" for key, value in (run.get('counts') or {}).items() if value) or 'No stages ran'
        rows.append(html.Tr([
            html.Td(
                html.A(
                    record['run_id'],
                    href=('/dash/pipelines?' + urlencode({'run_id': record['run_id'], 'instance': registry or '', 'pipeline': pipeline or ''})) if table_id.startswith('pipeline-card-history-') else '#',
                    **({} if table_id.startswith('pipeline-card-history-') else {'data-run-id': record['run_id']}),
                    className='pipeline-run-link', title=record['run_id'],
                )
            ),
            html.Td(run.get('status', ''), className=f"run-status-{run.get('status', '')}"),
            html.Td(str(record.get('started', ''))[:19].replace('T', ' ')),
            html.Td(f"{float(run.get('duration_seconds') or 0.0):.1f}"),
            html.Td(counts),
            html.Td(record.get('message') or ''),
        ]))
    return html.Div(dbc.Table(
        [html.Thead(html.Tr([
            html.Th('Run'), html.Th('Status'), html.Th('Started'),
            html.Th('Duration (s)'), html.Th('Nodes'), html.Th('Message'),
        ]))] + [html.Tbody(rows)],
        hover=True,
        size='sm',
        id=table_id,
    ), className='pipeline-table-scroll pipeline-runs-scroll', tabIndex=0, role='region',
        **{'aria-label':'Pipeline runs table'})


def build_gantt_children(detail: Optional[Dict[str, Any]]) -> List[Any]:
    """
    The Gantt figure of a run and its nodes table.
    """
    if not detail:
        return [html.P("No run selected.")]
    if not detail.get('bars'):
        return [html.P('This run did not execute any stages.', className='text-muted')]
    t = lambda text: (str(text)[11:23] if text else '')
    rows = [
        html.Tr([
            html.Td(html.A(
                html.Code(_engine.ref_label(bar['pipe_ref'])), href=_engine.pipe_href(bar['pipe_ref']),
                className='pipe-link', target='_blank', rel='noopener noreferrer',
            )),
            html.Td(bar.get('action', '')),
            html.Td('running' if bar.get('actual_start') and not bar.get('actual_end')
                    else bar.get('status', '')),
            html.Td('Started' if bar.get('actual_start') else 'Waiting / skipped'),
            html.Td(t(bar.get('actual_start'))),
            html.Td(t(bar.get('actual_end'))),
        ])
        for bar in detail.get('bars', [])
    ]
    return [
        html.Div(dcc.Graph(
            figure=_engine.gantt_figure(detail),
            id='pipelines-gantt',
            config={'displaylogo': False, 'displayModeBar': False, 'responsive': True, 'scrollZoom': False, 'doubleClick': False},
            style={'minWidth': '1000px'},
        ), className='pipeline-timeline-scroll', tabIndex=0, role='region', **{'aria-label':'Execution timeline'}),
        html.Div(dbc.Table(
            [html.Thead(html.Tr([
                html.Th('Pipe'), html.Th('Action'), html.Th('Status'),
                html.Th('Readiness'), html.Th('Actual start'), html.Th('Actual end'),
            ]))] + [html.Tbody(rows)],
            hover=True,
            size='sm',
            id='pipelines-nodes-table',
        ), className='pipeline-table-scroll pipeline-stages-scroll', tabIndex=0, role='region', **{'aria-label':'Execution stages table'}),
    ]


@dash_app.callback(
    Output('pipelines-run-id-input', 'value'),
    Output('pipelines-pipe-input', 'value'),
    Output('pipelines-status-select', 'value'),
    Input('pipelines-location', 'search'),
)
def fill_filters_from_url(search):
    """
    Show the URL's filters (`/dash/pipelines?run_id=...&pipe=...&status=...`) in the inputs.
    """
    filters = _filters_from_search(search)
    return filters['run_id'], filters['pipe'], (filters['status'] or 'any')


@dash_app.callback(
    Output('pipelines-location', 'search'),
    Input('pipelines-filter-button', 'n_clicks'),
    Input('pipelines-run-id-input', 'n_submit'),
    Input('pipelines-pipe-input', 'n_submit'),
    State('pipelines-run-id-input', 'value'),
    State('pipelines-pipe-input', 'value'),
    State('pipelines-status-select', 'value'),
    State('pipelines-location', 'search'),
    prevent_initial_call=True,
)
def apply_filters(n_clicks, run_submit, pipe_submit, run_id, pipe, status, current_search):
    """
    Put the filters in the URL.
    """
    query = {key: values[0] for key, values in parse_qs((current_search or '').lstrip('?')).items()
             if key in ('pipeline', 'instance')}
    query.update({
        key: value
        for key, value in (('run_id', run_id), ('pipe', pipe), ('status', status))
        if value and value != 'any'
    })
    search = ('?' + urlencode(query)) if query else ''
    if search == (current_search or ''):
        raise PreventUpdate
    return search


@dash_app.callback(
    Output('pipelines-graph-div', 'children'),
    Input('pipelines-location', 'pathname'),
    Input('pipeline-focus', 'value'),
    Input('pipeline-standalone', 'value'),
    Input('pipeline-definition', 'data'),
    Input('pipeline-definitions', 'data'),
    Input('pipeline-draft', 'data'),
    Input('pipelines-refresh-button', 'n_clicks'),
    State('pipelines-location', 'search'), State('session-store', 'data'),
)
def render_pipeline_graph(pathname, focus, standalone, definition, definitions, draft, refresh, search, session_data):
    """Only selection changes and explicit refreshes rebuild the canvas."""
    if not str(pathname).startswith('/dash/pipelines'):
        raise PreventUpdate
    if not is_session_authenticated(str((session_data or {}).get('session-id'))):
        raise PreventUpdate
    instance = _engine.get_instance(parse_qs((search or '').lstrip('?')).get('instance', [None])[0])
    name = (definition or {}).get('name')
    try:
        graph = _engine.plan_graph(instance)
        options = [{'label': node['label'], 'value': node['pipe_ref']} for node in graph['nodes']]
        if definition is not None or (draft and draft.get('active')):
            valid_draft = draft and draft.get('definition_id') == (definition or {}).get('id', (definition or {}).get('name'))
            selection = draft if valid_draft else definition or {}
            selected_pipes = []
            for ref in selection.get('targets', []):
                instance_keys, connector, metric, location = _engine.split_ref(ref)
                selected_pipes.append(mrsm.Pipe(connector, metric, location, instance=instance_keys))
            graph = _engine.plan_graph(instance, selected_pipes,
                                       parents=selection.get('parents'),
                                       children=selection.get('children'))
        if draft and draft.get('pipe_edits'):
            from meerschaum.api.dash.pipeline_editing import validate_edits
            edges = validate_edits(draft['pipe_edits'], draft.get('targets', []))
            visible = {node['pipe_ref'] for node in graph['nodes']}
            graph = {**graph, 'edges': [{'parent': parent, 'child': child}
                for parent, child in edges if parent in visible and child in visible],
                'warnings': graph.get('warnings', []) + [
                    'Draft dependencies are validated. Planned actions and bounds are refreshed after Save pipeline.']}
        if focus:
            graph = {**_engine.focus_graph(graph, focus), 'focus': focus}
        elif graph['edges'] and not standalone and not definition:
            connected = {edge[key] for edge in graph['edges'] for key in ('parent', 'child')}
            graph = {**graph, 'nodes': [node for node in graph['nodes'] if node['pipe_ref'] in connected]}
        group_definitions = ({name: {**definition, **{key: (selection or {}).get(key) for key in ('targets', 'parents', 'children')}}}
                             if name else definitions)
        graph_children = build_graph_children(graph, [], group_definitions)
        dash.set_props('pipeline-focus-options', {'data': options})
        dash.set_props('pipeline-visible-refs', {
            'data': [node['pipe_ref'] for node in graph['nodes']],
        })
    except Exception as e:
        warn(f"Could not plan the pipeline graph: {e}", stack=False)
        graph_children = [html.P(f"The dependency graph is unavailable: {e}")]

    return graph_children


@dash_app.callback(
    Output('pipelines-gantt-div', 'children'),
    Output('pipelines-runs-div', 'children'),
    Output('pipelines-alert-div', 'children'),
    Output('pipelines-events-store', 'data'),
    Input('pipelines-location', 'pathname'),
    Input('pipelines-location', 'search'),
    Input('pipeline-definition', 'data'),
    Input('pipeline-live-run', 'data'),
    Input('pipeline-selected-run', 'data'),
    Input('pipeline-node-events', 'data'),
    Input('pipelines-interval', 'n_intervals'),
    Input('pipelines-refresh-button', 'n_clicks'),
    State('pipelines-events-store', 'data'),
    State('pipeline-log-stage', 'value'),
    State('session-store', 'data'),
)
def render_pipelines(pathname, search, definition, live_run, selected_run, node_execution_events, n_intervals, n_clicks, events_store, log_stage, session_data):
    """
    Render run details and history independently of the canvas.
    """
    if not str(pathname).startswith('/dash/pipelines'):
        raise PreventUpdate

    session_id = (session_data or {}).get('session-id', None)
    if not is_session_authenticated(str(session_id)):
        raise PreventUpdate

    filters = _filters_from_search(search)
    name = (definition or {}).get('name')
    try:
        instance = _engine.get_instance(parse_qs((search or '').lstrip('?')).get('instance', [None])[0])
        runs = _engine.get_runs(
            instance,
            run_id=None,
            pipe=filters['pipe'] or None,
            status=filters['status'] or None,
            run_name=name,
        )
        if name:
            runs = [run for run in runs if _engine.run_belongs_to_pipeline(run, name, definition)]
        all_runs = runs
    except Exception as e:
        warn(f"Could not read the engine state: {e}", stack=False)
        message = f"Engine state unavailable: {e}"
        return (
            [html.P("No run selected.")], [html.P("No runs match.")],
            alert_from_success_tuple((False, message)),
            {},
        )

    live_matches = bool(live_run and live_run.get('pipeline') == name
        and (not (definition or {}).get('id') or live_run.get('pipeline_id') == definition['id'])
        and (not (definition or {}).get('registry') or live_run.get('registry') == definition['registry']))
    live_id = (live_run or {}).get('run_id') if live_matches else None
    if dash.ctx.triggered_id in ('pipeline-definition', 'pipelines-location'):
        selected_run = None
        dash.set_props('pipeline-selected-run', {'data': None})
    selected_id = selected_run or filters['run_id'] or live_id or (
        runs[0]['run']['run_id'] if runs else (all_runs[0]['run']['run_id'] if all_runs else None)
    )
    try:
        detail = _engine.get_run_detail(instance, selected_id) if selected_id else None
    except Exception as e:
        warn(f"Could not read run '{selected_id}': {e}", stack=False)
        detail = None

    wrong_pipeline = bool(detail and name and not _engine.run_belongs_to_pipeline(detail.get('summary') or {}, name, definition))
    if detail and name and not _engine.run_belongs_to_pipeline(detail.get('summary') or {}, name, definition):
        detail = None
        selected_id = None
    stage_refs = {bar['pipe_ref'] for bar in (detail or {}).get('bars', [])}
    if log_stage and log_stage not in stage_refs:
        dash.set_props('pipeline-log-stage', {'value': None})

    if dash.ctx.triggered_id == 'pipeline-definition':
        dash.set_props('pipeline-log-stage', {'value': None})
        dash.set_props('pipeline-run-status', {'children': ''})
        dash.set_props('pipeline-stage-statuses', {'data': {}})
    dash.set_props('pipeline-log-stage', {'options': [
        {'label': _engine.ref_label(bar['pipe_ref']), 'value': bar['pipe_ref']}
        for bar in (detail or {}).get('bars', [])
    ]})
    active = bool(detail and (detail.get('summary') or {}).get('status') == 'running')
    worker_stopped = False
    if live_matches and selected_id == live_id:
        try:
            from meerschaum.api.dash._support import get_job
            worker_stopped = get_job(live_run['job']).status not in ('running', 'paused')
        except Exception:
            worker_stopped = True
        if worker_stopped:
            active = False
        elif not detail:
            active = live_run.get('status') == 'running'
    observed = next((event for event in (node_execution_events or []) if event.get('run_id') == selected_id), {})
    if detail and (worker_stopped or observed.get('status') == 'stopped') and (detail.get('summary') or {}).get('status') == 'running':
        from datetime import datetime, timezone
        ended = observed.get('ended') or (live_run or {}).get('ended') or datetime.now(timezone.utc).isoformat()
        parsed_end = _engine._parse_ts(ended)
        if parsed_end is not None:
            ended = parsed_end.replace(tzinfo=parsed_end.tzinfo or timezone.utc).isoformat()
        detail['summary']['status'] = 'stopped'
        detail['summary'].setdefault('run', {})['finished'] = ended
        for bar in detail.get('bars', []):
            if bar.get('status') in ('pending', 'running'):
                bar['status'] = 'stopped'
                if bar.get('actual_start') and not bar.get('actual_end'):
                    bar['actual_end'] = ended
        for summary in runs:
            if summary['run']['run_id'] == selected_id:
                summary['status'] = 'stopped'
                summary['run']['finished'] = ended
        active = False
    dash.set_props('pipelines-interval', {'interval': 2000 if active else 15000})
    if not detail:
        dash.set_props('pipeline-run-status', {'children': ''})
        dash.set_props('pipeline-stage-statuses', {'data': {}})
    if detail:
        summary = detail.get('summary') or {}
        counts = summary.get('counts') or {}
        completed = sum(value for key, value in counts.items() if key not in ('pending', 'running'))
        dash.set_props('pipeline-run-status', {'children':
            f"{summary.get('status', '')} · {completed}/{len(detail.get('bars', []))} stages complete"})
        stage_statuses = {}
        for bar in detail.get('bars', []):
            status = bar.get('status', '')
            if status == 'pending' and bar.get('actual_start'):
                status = 'running'
            if worker_stopped and status in ('pending', 'running'):
                status = 'stopped'
            stage_statuses[bar['pipe_ref']] = status
        dash.set_props('pipeline-stage-statuses', {'data': stage_statuses})
    store = dict(events_store or {})
    if store.get('run_id') != selected_id:
        store = {'run_id': selected_id}
    return (
        (build_gantt_children(detail) if detail else [html.P(
            (f"Run '{selected_id or filters['run_id']}' belongs to another Pipeline on {instance}." if wrong_pipeline
             else f"Run '{selected_id}' is unavailable on {instance}." if selected_id
             else 'No execution history yet. Use Run now to start this Pipeline.')
        ), dbc.Button('Show recent runs', id='pipeline-recent-runs', size='sm', color='secondary')]),
        [build_runs_overview(runs), build_runs_table(runs, registry=str(instance), pipeline=name)],
        [],
        store,
    )


@dash_app.callback(
    Output('pipelines-events-pre', 'children'),
    Input('pipelines-events-store', 'data'),
    Input('pipelines-interval', 'n_intervals'),
    Input('pipeline-log-stage', 'value'),
    State('pipelines-location', 'search'), State('session-store', 'data'),
)
def render_events(store, n_intervals, stage, search, session_data):
    """
    The selected run's events, oldest first.
    """
    run_id = (store or {}).get('run_id', None)
    session_id = (session_data or {}).get('session-id', None)
    if not run_id or not is_session_authenticated(str(session_id)):
        return ''
    try:
        rows = _engine.get_run_events(_engine.get_instance(
            parse_qs((search or '').lstrip('?')).get('instance', [None])[0]), run_id)
    except Exception as e:
        return f"Events unavailable: {e}"
    lines = [_engine.describe_event(row) for row in rows
             if not stage or row.get('pipe_ref') == stage][-MAX_EVENT_LINES:]
    return '\n'.join(lines)


dash_app.clientside_callback(
    """
    function(statuses, nodeStatuses) {
        const merged = {...(statuses || {}), ...(nodeStatuses || {})};
        window._mrsmPipelineStatuses = merged;
        if (window.mrsmPaintPipelineStatuses) window.mrsmPaintPipelineStatuses(merged);
        return dash_clientside.no_update;
    }
    """,
    Output('pipeline-stage-status-dummy', 'children'), Input('pipeline-stage-statuses', 'data'),
    Input('pipeline-node-statuses', 'data'),
)


dash_app.clientside_callback(
    r"""function(run, selected, events, session) {
        const cache = window._mrsmNodeOutput = window._mrsmNodeOutput || new Map();
        const sessionId = (session || {})['session-id'] || null;
        if (window._mrsmNodeOutputSession !== sessionId) {
            cache.forEach(item => item.source?.close()); cache.clear();
            window._mrsmNodeOutputSession = sessionId; window._mrsmNodeOutputSelected = null;
            dash_clientside.set_props('pipeline-node-run', {data:null});
            dash_clientside.set_props('pipeline-node-runs', {data:[]});
            dash_clientside.set_props('pipeline-output-tab', {options:[], value:null});
            dash_clientside.set_props('pipeline-node-output', {children:''});
            dash_clientside.set_props('pipeline-node-run-label', {children:''});
            return dash_clientside.no_update;
        }
        if (run && run.session_id !== sessionId) return dash_clientside.no_update;
        if (run && run.job && !cache.has(run.job)) {
            window._mrsmOutputDismissed = false;
            const dock = document.getElementById('pipeline-output-dock');
            if (dock) dock.dataset.outputMode = 'pipe';
            const item = {...run, status: 'running', lines: []}; cache.set(run.job, item);
            document.getElementById('pipeline-output-dock')?.classList.remove('pipeline-output-empty');
            dash_clientside.set_props('pipeline-node-runs', {data: Array.from(cache.values()).map(r => ({
                job:r.job, run_id:r.run_id, ref:r.ref, instance:r.instance, scope:r.scope}))});
            const source = new EventSource(run.stream); item.source = source;
            function paint() {
                const target = document.getElementById('pipeline-node-output');
                const active = window._mrsmNodeOutputSelected;
                if (target && active === run.job && window.mrsmRenderPipelineOutput) window.mrsmRenderPipelineOutput(target, item.lines.join('\n'));
            }
            if (run.scope === 'this') source.onmessage = function(event) {
                item.lines.push(event.data.replace(/\n$/, ''));
                if (item.lines.length > 1000) item.lines.shift(); paint();
            };
            else item.lines.push('Waiting for attributed Pipe events…');
            source.addEventListener('stopped', function() { source.close(); item.status = 'stopped'; item.lines.push('[Execution finished]'); if (window.mrsmFinishNodeOutput) window.mrsmFinishNodeOutput(item); paint(); });
            source.onerror = function() { if (source.readyState === 2) { item.lines.push('[Output disconnected]'); paint(); } };
            dash_clientside.set_props('pipeline-output-tab', {options: Array.from(cache.values()).map(r => ({label:r.ref + ' · ' + r.run_id, value:r.job})), value:run.job});
            selected = run.job;
        }
        for (const event of (events || [])) {
            if (cache.has(event.job)) {
                const item = cache.get(event.job);
                item.status = event.status || item.status;
                if (item.scope !== 'this' && event.run_id === item.run_id) item.lines = event.lines || [];
                if (window.mrsmUpdateOutputCancel) window.mrsmUpdateOutputCancel();
            }
        }
        if (!selected && cache.size) {
            selected = window._mrsmNodeOutputSelected || Array.from(cache.keys()).at(-1);
            dash_clientside.set_props('pipeline-output-tab', {options:Array.from(cache.values()).map(r => ({label:r.ref + ' · ' + r.run_id, value:r.job})), value:selected});
            dash_clientside.set_props('pipeline-node-runs', {data:Array.from(cache.values()).map(r => ({job:r.job, run_id:r.run_id, ref:r.ref, instance:r.instance, scope:r.scope}))});
        }
        if (selected && cache.has(selected)) {
            if (!window._mrsmOutputDismissed) document.getElementById('pipeline-output-dock')?.classList.remove('pipeline-output-empty');
            window._mrsmNodeOutputSelected = selected;
            const item = cache.get(selected);
            dash_clientside.set_props('pipeline-node-run-label', {children:item.ref + ' · ' + item.scope + ' · ' + (item.begin || 'current watermark') + ' → ' + (item.end || 'available data') + ' · ' + item.run_id + ' · ' + item.job});
            const view = document.getElementById('pipeline-node-output');
            if (view && window.mrsmRenderPipelineOutput) window.mrsmRenderPipelineOutput(view, item.lines.join('\n'));
            if (window.mrsmUpdateOutputCancel) window.mrsmUpdateOutputCancel();
        }
        return dash_clientside.no_update;
    }""",
    Output('pipeline-node-stream-dummy', 'children'),
    Input('pipeline-node-run', 'data'), Input('pipeline-output-tab', 'value'),
    Input('pipeline-node-events', 'data'), Input('session-store', 'data'),
)


@dash_app.callback(
    Output('pipeline-node-events', 'data'), Output('pipeline-node-statuses', 'data'),
    Input('pipeline-node-runs', 'data'), Input('pipelines-interval', 'n_intervals'),
    State('session-store', 'data'),
)
def node_events(runs, ticks, session):
    """Poll every retained execution by its exact identity, including older active tabs."""
    if not runs or not is_session_authenticated((session or {}).get('session-id')):
        raise PreventUpdate
    events, statuses = [], {}
    for run in runs:
        if not run.get('run_id'):
            continue
        statuses[run['ref']] = 'pending'
        worker_status = 'unavailable'
        ended = None
        rows = []
        try:
            from meerschaum.api.dash._support import get_job
            from meerschaum.api.dash.jobs import job_visible_to_session
            job = get_job(run['job'])
            metadata = job.daemon.properties.get('mrsm_execution') or {}
            if (not metadata or not job_visible_to_session(job, (session or {}).get('session-id'))
                    or metadata.get('run_id') != run['run_id']):
                raise ValueError('This execution is unavailable or belongs to another session.')
            worker_status = job.status
            ended = str(job.ended) if job.ended else None
            rows = _engine.get_run_events(_engine.get_instance(run['instance']), run['run_id'])
            for row in rows:
                ref, event = row.get('pipe_ref'), row.get('event')
                if not ref:
                    continue
                status = {'node_planned': 'pending', 'node_started': 'running',
                          'node_skipped': 'skipped', 'node_failed': 'failed',
                          'node_rebuild_pending': 'upstream_failed'}.get(event)
                if event == 'node_finished':
                    status = 'failed' if (row.get('payload') or {}).get('result', {}).get('success') is False else 'succeeded'
                if status:
                    statuses[ref] = status
            lines = [f"[{row.get('seq', '')} / attempt {(row.get('payload') or {}).get('attempt', '')}] "
                     + _engine.describe_event(row)
                     for row in rows if row.get('pipe_ref') == run['ref']][-MAX_EVENT_LINES:]
        except Exception as exc:
            lines = [f'Events unavailable: {exc}']
        if worker_status not in ('running', 'paused'):
            for ref in {run['ref']} | {row.get('pipe_ref') for row in rows if row.get('pipe_ref')}:
                if statuses.get(ref) in ('running', 'pending'):
                    statuses[ref] = 'cancelled'
        events.append({'job': run['job'], 'run_id': run['run_id'], 'lines': lines, 'status': worker_status, 'ended': ended})
    return events, statuses


@dash_app.callback(
    Output('pipeline-cancel-alert', 'children'),
    Input('pipeline-cancel-node-run', 'n_clicks'),
    Input('pipeline-cancel-current', 'n_clicks', allow_optional=True),
    State('pipeline-live-run', 'data'),
    State('pipeline-node-runs', 'data'), State('pipeline-output-tab', 'value'),
    State('session-store', 'data'), prevent_initial_call=True,
)
def cancel_node_execution(clicks, pipeline_clicks, live_run, runs, selected, session):
    session_id = (session or {}).get('session-id')
    if not is_session_authenticated(session_id):
        raise PreventUpdate
    run = next((run for run in (runs or []) if run.get('job') == selected), None)
    if dash.ctx.triggered_id == 'pipeline-cancel-current':
        if not pipeline_clicks:
            raise PreventUpdate
        run = live_run
        selected = (run or {}).get('job')
    elif not clicks:
        raise PreventUpdate
    try:
        if not run or selected != run.get('job'):
            raise ValueError('Select a Pipe execution to cancel it. Historical outputs remain available.')
        from meerschaum.api.dash._support import get_job
        from meerschaum.api.dash.jobs import job_visible_to_session
        job = get_job(run['job'])
        metadata = job.daemon.properties.get('mrsm_execution') or {}
        if (not metadata or not job_visible_to_session(job, session_id)
                or metadata.get('run_id') != run.get('run_id')):
            raise ValueError('This execution is unavailable or belongs to another session.')
        result = job.stop()
        if result[0]:
            dash.set_props('pipelines-interval', {'n_intervals': __import__('time').time_ns()})
        return alert_from_success_tuple(result)
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))


def build_runs_overview(runs):
    """Observed intervals across runs; selecting a bar drills into its stages."""
    from datetime import datetime, timezone
    bars = []
    for summary in runs:
        record = summary['run']
        start = _engine._parse_ts(record.get('started'))
        if start is None:
            continue
        end = _engine._parse_ts(record.get('finished')) or (datetime.now(timezone.utc) if start.tzinfo else datetime.now())
        bars.append((record['run_id'], start.isoformat(), max(1, (end-start).total_seconds()*1000), summary.get('status', '')))
    figure = {'data': [{'type': 'bar', 'orientation': 'h',
        'y': [b[0] for b in bars], 'base': [b[1] for b in bars], 'x': [b[2] for b in bars],
        'customdata': [b[0] for b in bars],
        'marker': {'color': [_engine.STATUS_COLORS.get(b[3], '#3486bd') for b in bars]},
        'hovertemplate': '%{y}<extra></extra>'}],
        'layout': {'dragmode': False, 'xaxis': {'type': 'date', 'fixedrange': True},
                   'yaxis': {'fixedrange': True, 'autorange': 'reversed', 'automargin': True},
                   'height': min(650, max(260, 80+len(bars)*35)),
                   'paper_bgcolor': 'rgba(0,0,0,0)', 'plot_bgcolor': 'rgba(0,0,0,0)',
                   'font': {'color': '#d0d6dc'}, 'margin': {'l': 20, 'r': 20, 't': 20, 'b': 40}}}
    return dcc.Graph(id='pipeline-runs-overview', figure=figure,
                     config={'displayModeBar': False, 'scrollZoom': False, 'doubleClick': False})


@dash_app.callback(
    Output('pipeline-selected-run', 'data'),
    Input('pipeline-runs-overview', 'clickData', allow_optional=True),
    prevent_initial_call=True,
)
def select_overview_run(click):
    if not click or not click.get('points'):
        raise PreventUpdate
    return click['points'][0]['customdata']
