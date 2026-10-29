#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Callbacks of the pipelines page (`/dash/pipelines`).
"""

from __future__ import annotations

import json

from urllib.parse import parse_qs, urlencode, quote

import dash
from dash.dependencies import Input, Output, State, ALL
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional
from meerschaum.utils.warnings import warn
from meerschaum.api import CHECK_UPDATE
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.components import alert_from_success_tuple, build_icon
from meerschaum.api.dash.sessions import is_session_authenticated


def _may_read(session) -> bool:
    """The Pipelines pages are for viewers who may read pipes (`pipes:read`)."""
    from meerschaum.api.dash import pipes_access
    return pipes_access.can_read(session)


def _visible_ref(ref, session) -> bool:
    """Whether the viewer may see the pipe of a `pipe_ref` the browser sent."""
    from meerschaum.api.dash import access, pipes_access
    return bool(ref) and access.ref_is_visible(pipes_access.viewer(session), str(ref))
from meerschaum.api.dash import engine as _engine
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)

#: The most lines of events the page shows.
MAX_EVENT_LINES: int = 500

#: How often the page polls (ms) while every live run streams its events (or none runs): the
#: streams' events refresh the views (`pipeline-run-tick`) as they happen.
STREAM_POLL_MS: int = 15000
#: How often the page polls (ms) a live run that has no event stream (the server refused it).
FALLBACK_POLL_MS: int = 5000
#: How long (seconds) a canvas sync's stream waits for its job to start the run.
NODE_RUN_STREAM_WAIT: int = 30


def poll_interval(active: bool, streaming: bool) -> int:
    """
    The page's poll interval (ms): slow while the live run streams its events, faster for a live
    run without a stream (the fallback), slow when nothing runs.
    """
    return FALLBACK_POLL_MS if active and not streaming else STREAM_POLL_MS


def new_console_run_id(kind: str) -> str:
    """
    A run ID that reads as when and where a console sync started, e.g.
    `2026-10-26T16-48-03Z-canvas-1a2b3c4d5e6f`.
    """
    import uuid
    from datetime import datetime, timezone
    return f"{datetime.now(timezone.utc):%Y-%m-%dT%H-%M-%SZ}-{kind}-{uuid.uuid4().hex[:12]}"


def _filters_from_search(search: Optional[str]) -> Dict[str, str]:
    query = parse_qs((search or '').lstrip('?'))
    filters = {key: (query.get(key, [''])[0] or '') for key in ('run_id', 'pipe', 'status')}
    if filters['status'] == 'any':
        filters['status'] = ''
    return filters


#: Above this many Pipes, a Pipeline's canvas starts grouped by connector.
GROUP_THRESHOLD: int = 25


def classify_nodes(graph: Dict[str, Any], targets: List[str]) -> Dict[str, str]:
    """
    Each node's role in a Pipeline: `explicit` (a member), or a traversal Pipe `upstream` (an
    ancestor of a member) or `downstream`.
    """
    refs = {node['pipe_ref'] for node in graph['nodes']}
    explicit = refs & set(targets) if targets else set(refs)
    parents_of, children_of = {}, {}
    for edge in graph['edges']:
        parents_of.setdefault(edge['child'], set()).add(edge['parent'])
        children_of.setdefault(edge['parent'], set()).add(edge['child'])
    def reach(start, links):
        found, frontier = set(), set(start)
        while frontier:
            frontier = {ref for current in frontier for ref in links.get(current, ())} - found - explicit
            found |= frontier
        return found
    upstream = reach(explicit, parents_of)
    downstream = reach(explicit, children_of) - upstream
    return {ref: ('explicit' if ref in explicit else 'downstream' if ref in downstream else 'upstream')
            for ref in refs}


def build_plan_table(graph: Dict[str, Any]) -> Any:
    """The plan: dependency order, planned action, reason and sync window of every Pipe."""
    positions = _engine.canvas_positions(graph)
    return html.Div(dbc.Table([html.Thead(html.Tr([html.Th('Pipe'), html.Th('Action'), html.Th('Reason'), html.Th('Window')])),
        html.Tbody([html.Tr([html.Td(html.Code(_engine.ref_label(node['pipe_ref']))),
            html.Td(_engine.action_label(node['action'])), html.Td(node.get('reason', '')),
            html.Td(node.get('bounds_text') or _engine.bounds_label(node.get('bounds'), node.get('backtrack')))])
            for node in sorted(graph['nodes'], key=lambda node: positions.get(node['pipe_ref'], (0, 0)))])], size='sm'),
        className='pipeline-table-scroll pipeline-plan-table', tabIndex=0, role='region', **{'aria-label': 'Plan'})


def dependency_label(count: int) -> str:
    """`1 dependency`, `3 dependencies`: the canvas's links between Pipes."""
    return f"{count} dependenc{'y' if count == 1 else 'ies'}"


def build_pipeline_canvas(
    graph: Dict[str, Any],
    targets: List[str],
    expanded: Optional[Dict[str, bool]] = None,
    definition: Optional[Dict[str, Any]] = None,
    registry: Optional[str] = None,
    read_only: bool = False,
) -> List[Any]:
    """
    One Pipeline's canvas: its explicit members, with traversal Pipes collapsed into
    "+N upstream" / "+N downstream" nodes until expanded; over `GROUP_THRESHOLD` Pipes it is
    grouped by connector. Members on another instance than the registry carry its badge.
    """
    expanded = expanded or {}
    roles = classify_nodes(graph, targets)
    hidden = {ref for ref, role in roles.items() if role != 'explicit' and not expanded.get(role)}
    counts = {role: sum(1 for ref in hidden if roles[ref] == role) for role in ('upstream', 'downstream')}
    nodes = [node for node in graph['nodes'] if node['pipe_ref'] not in hidden]
    edges = [edge for edge in graph['edges'] if edge['parent'] not in hidden and edge['child'] not in hidden]
    placeholders = {}
    for role in ('upstream', 'downstream'):
        if not counts[role]:
            continue
        key = f'__{role}__'
        placeholders[key] = role
        nodes.append({'pipe_ref': key, 'label': f'+{counts[role]} {role}'})
        linked = set()
        for edge in graph['edges']:
            if role == 'upstream' and edge['parent'] in hidden and roles[edge['parent']] == role and edge['child'] not in hidden:
                linked.add((key, edge['child']))
            elif role == 'downstream' and edge['child'] in hidden and roles[edge['child']] == role and edge['parent'] not in hidden:
                linked.add((edge['parent'], key))
        edges.extend({'parent': parent, 'child': child} for parent, child in sorted(linked))
    ### The members the viewer may not see: one card with their count (`engine.plan_graph`).
    hidden_count = int(graph.get('hidden', 0) or 0)
    if hidden_count:
        from meerschaum.api.dash.access import hidden_label
        placeholders['__hidden__'] = 'hidden'
        nodes.append({'pipe_ref': '__hidden__', 'label': hidden_label(hidden_count)})
    real = [node for node in nodes if node['pipe_ref'] not in placeholders]
    if len(real) > GROUP_THRESHOLD:
        by_connector = {}
        for node in real:
            by_connector.setdefault(_engine.split_ref(node['pipe_ref'])[1], set()).add(node['pipe_ref'])
        groups = [(connector, refs) for connector, refs in sorted(by_connector.items())]
        groups[0] = (groups[0][0], groups[0][1] | set(placeholders))
    else:
        groups = [('', {node['pipe_ref'] for node in nodes})]
    cards, connections = [], []
    offset, width = 0, 390
    for name, refs in groups:
        subset = {'nodes': [node for node in nodes if node['pipe_ref'] in refs],
                  'edges': [edge for edge in edges if edge['parent'] in refs and edge['child'] in refs]}
        positions = _engine.canvas_positions(subset) if subset['nodes'] else {}
        group_width = (max(x for x, _ in positions.values()) * 380 + 390) if positions else 390
        group_height = (max(y for _, y in positions.values()) * 190 + 240) if positions else 240
        top = offset + (56 if name else 16)
        if name:
            cards.append(html.Div(html.Span(name, className='pipeline-group-title'),
                className='pipeline-group', style={'left': '12px', 'top': f'{offset + 8}px',
                'width': f'{group_width - 12}px', 'height': f'{group_height + 24}px'},
                **{'data-group': name}))
        for node in subset['nodes']:
            ref = node['pipe_ref']
            x, y = positions[ref]
            key = json.dumps([name, ref])
            style = {'left': f'{32 + x * 380}px', 'top': f'{top + y * 190}px'}
            if ref in placeholders and placeholders[ref] == 'hidden':
                cards.append(html.Div(
                    html.Strong(node['label']),
                    className='pipeline-stage pipeline-placeholder pipeline-hidden-members',
                    title=node['label'],
                    style={**style, '--stage-color': '#5a6773'},
                    **{'data-ref': ref, 'data-node': key, 'aria-label': node['label']},
                ))
                continue
            if ref in placeholders:
                role = placeholders[ref]
                cards.append(html.Button(html.Strong(node['label']),
                    id={'type': 'pipeline-expand', 'index': role}, n_clicks=0,
                    className='pipeline-stage pipeline-placeholder', title=f'Show {role} Pipes',
                    style={**style, '--stage-color': '#71889c'},
                    **{'data-ref': ref, 'data-node': key, 'aria-label': f"Show {node['label']} Pipes"}))
                continue
            instance = _engine.split_ref(ref)[0]
            role = roles.get(ref, 'explicit')
            ### A read-only card holds a link, so it is not itself a button.
            card_type = (lambda children, **kwargs: html.Div(children, tabIndex=0, **{
                key: value for key, value in kwargs.items() if key not in ('id', 'n_clicks')})) if read_only else html.Button
            cards.append(card_type([
                _engine.action_indicator(node.get('action', ''), failed=False),
                *([] if read_only else [html.Span('▤', className='pipeline-node-output-icon', role='button', tabIndex=0,
                          title='View Pipe output', **{'aria-label': 'View Pipe output'})]),
                html.Code(node['label']),
                *([html.Small(instance, className='pipeline-instance-badge')]
                  if registry and instance != registry else []),
                *([html.Small(role, className='pipeline-traversal-badge')] if role != 'explicit' else []),
                *([html.A('Open Pipe', href=_engine.pipe_href(ref), target='_blank', rel='noopener noreferrer',
                          className='pipeline-stage-open')] if read_only else
                  [html.Span('', className='pipeline-edge-handle pipeline-edit-only',
                             title='Drag to a Pipe to make it a child', **{'aria-hidden': 'true'})]),
            ], id={'type': 'pipeline-explore-stage' if read_only else 'pipeline-stage', 'index': key}, n_clicks=0,
                className='pipeline-stage', title=node['label'],
                style={**style, '--stage-color': _engine.STATUS_COLORS.get(node.get('status', ''), '#9aa5b1')},
                **{'data-ref': ref, 'data-node': key, 'data-role': role, 'data-grouped': 'true' if name else 'false',
                   'aria-label': f"Inspect {node['label']}"}))
        connections.extend({'parent': json.dumps([name, e['parent']]),
                            'child': json.dumps([name, e['child']])} for e in subset['edges'])
        width = max(width, group_width)
        offset += group_height + (64 if name else 16)
    node_keys = {}
    for card in cards:
        props = card.to_plotly_json()['props']
        if props.get('data-ref'):
            node_keys.setdefault(props['data-ref'], props['data-node'])
    for edge in edges:
        if edge['parent'] in node_keys and edge['child'] in node_keys:
            pair = {'parent': node_keys[edge['parent']], 'child': node_keys[edge['child']]}
            if pair not in connections:
                connections.append(pair)
    from meerschaum.api.dash.pipeline_management import count_label
    collapsible = [role for role in ('upstream', 'downstream') if expanded.get(role)
                   and any(value == role for value in roles.values())]
    layout = (definition or {}).get('layout') or {}
    return [
        html.Div([
            html.Div([
                html.Span(f"{count_label(len(real), 'Pipe')} · {dependency_label(len(graph['edges']))}"),
                html.Div([
                    dbc.Input(id='pipeline-canvas-search', placeholder='Find Pipe', type='search', size='sm'),
                    *([] if read_only else [html.Button('Add Pipe', id='pipeline-add-node', className='pipeline-edit-only')]),
                    html.Button('Fit', id='pipeline-fit', title='Fit to view'),
                    html.Details([
                        html.Summary('View options', **{'aria-controls': 'pipeline-view-options-panel'}),
                        html.Div([
                            html.Div([
                                html.Button('−', id='pipeline-zoom-out', title='Zoom out',
                                            **{'aria-label': 'Zoom out'}),
                                html.Button('+', id='pipeline-zoom-in', title='Zoom in',
                                            **{'aria-label': 'Zoom in'}),
                            ], className='pipeline-zoom-group', role='group', **{'aria-label': 'Zoom'}),
                            html.Button('Center selected', id='pipeline-center'),
                            *[html.Button(f'Collapse {role}', id={'type': 'pipeline-expand', 'index': role + '-collapse'},
                                          n_clicks=0) for role in collapsible],
                            html.Button('Collapse groups', id='pipeline-collapse-groups',
                                        disabled=not any(name for name, _ in groups)),
                            html.Select([html.Option(label, value=value) for label, value in [
                                ('All statuses', ''), ('Queued', 'pending'), ('Running', 'running'),
                                ('Succeeded', 'succeeded'), ('Failed', 'failed'), ('Skipped', 'skipped'),
                                ('Blocked', 'upstream_failed'),
                            ]], id='pipeline-canvas-status', **{'aria-label': 'Filter Pipe status'}),
                        ], id='pipeline-view-options-panel', className='pipeline-view-options-body'),
                    ], className='pipeline-view-options'),
                    html.Button(build_icon('fullscreen'), id='pipeline-fullscreen', title='Toggle fullscreen',
                                **{'aria-label': 'Toggle fullscreen'}),
                ], className='pipeline-canvas-controls'),
            ], className='pipeline-canvas-toolbar'),
            html.Div(html.Div(cards, id='pipeline-canvas-world',
                key=json.dumps([graph, targets, expanded, layout], sort_keys=True, default=str),
                style={'width': f'{width}px', 'height': f'{offset}px'},
                **{'data-edges': json.dumps(connections), 'data-focus': graph.get('focus', ''),
                   'data-layout': json.dumps(layout),
                   'data-pipeline': (definition or {}).get('name', ''),
                   'data-registry': registry or ''}),
                id='pipeline-canvas', tabIndex=0,
            **{'aria-label': 'Pipeline canvas. Drag to pan, pinch with two fingers to zoom, or use the zoom controls.'}),
        ], className='pipeline-canvas-shell'),
        html.P('This Pipeline has no members.') if not real else None,
        *[html.P(message, className='text-warning') for message in graph['warnings']],
    ]


@dash_app.callback(
    Output('pipeline-canvas-prefs', 'data'),
    Input({'type': 'pipeline-expand', 'index': ALL}, 'n_clicks'),
    State('pipeline-canvas-prefs', 'data'), State('pipeline-definition', 'data'),
    prevent_initial_call=True,
)
def toggle_traversal(clicks, prefs, definition):
    """Expanding (or collapsing) traversal Pipes is remembered per Pipeline in this browser."""
    trigger = dash.ctx.triggered_id
    if not isinstance(trigger, dict) or not any(clicks or []) or not definition:
        raise PreventUpdate
    role = trigger['index'].replace('-collapse', '')
    key = f"{definition.get('registry')}|{definition.get('name')}"
    prefs = dict(prefs or {})
    prefs[key] = {**(prefs.get(key) or {}), role: not trigger['index'].endswith('-collapse')}
    return prefs


@dash_app.callback(
    Output('pipeline-inspector', 'children'),
    Input({'type': 'pipeline-stage', 'index': ALL}, 'n_clicks'),
    Input('pipeline-definition', 'data'),
    Input('pipeline-inspector-close', 'n_clicks', allow_optional=True),
    State('session-store', 'data'),
    State('pipeline-edit-mode', 'data'),
    prevent_initial_call=True,
)
def inspect_stage(clicks, definition, close_clicks, session_data, edit=False):
    """Read a selected stage without starting a sync."""
    if not _may_read(session_data):
        raise PreventUpdate
    triggered = dash.ctx.triggered_id
    if triggered in ('pipeline-inspector-close', 'pipeline-definition'):
        if triggered == 'pipeline-inspector-close' and not close_clicks:
            raise PreventUpdate
        dash.set_props('pipeline-selection', {'data': None})
        dash.set_props('pipeline-inspector', {'hidden': True})
        return []
    if isinstance(triggered, dict) and any(clicks):
        _, ref = json.loads(triggered['index'])
    else:
        raise PreventUpdate
    if not _visible_ref(ref, session_data):
        raise PreventUpdate
    from meerschaum.api.dash import access, pipes_access
    viewer = pipes_access.viewer(session_data)
    instance_keys, connector_keys, metric_key, location_key = _engine.split_ref(ref)
    visible = access.visible_pipe(connector_keys, metric_key, location_key, instance_keys, viewer=viewer)
    may_sync = visible is not None and access.can_edit_pipe(viewer, visible)
    dash.set_props('pipeline-inspector', {'hidden': False})
    import uuid
    selection = {'ref': ref, 'token': uuid.uuid4().hex}
    dash.set_props('pipeline-selection', {'data': selection})
    return [
        html.Div([html.H4(html.Code(_engine.ref_label(ref))),
                  html.Button('×', id='pipeline-inspector-close', n_clicks=0,
                              title='Close pipe details')], className='pipeline-inspector-header'),
        html.A(dbc.Card(dbc.CardBody([html.Span('Open Pipe'), html.Small(_engine.split_ref(ref)[0])]),
                        className='pipeline-open-pipe-card'),
               href=_engine.pipe_href(ref), target='_blank', rel='noopener noreferrer',
               className='pipeline-open-pipe'),
        html.Div([
            dbc.Button('Sync this Pipe', id='pipeline-node-sync', size='sm',
                       style=(None if may_sync else {'display': 'none'})),
            dbc.Button('View output', id='pipeline-node-view-output', size='sm', color='secondary'),
        ], className='pipeline-node-actions'),
        html.Details([
            html.Summary('Sync options'),
            dcc.Dropdown(id='pipeline-node-scope', value='this', clearable=False, options=[
                {'label': 'This Pipe only', 'value': 'this'},
                {'label': 'Include all parents', 'value': 'parents'},
                {'label': 'Include all children', 'value': 'children'},
            ]),
            dbc.Input(id='pipeline-node-begin', placeholder='Begin (default: last sync)', size='sm'),
            dbc.Input(id='pipeline-node-end', placeholder='End (default: latest data)', size='sm'),
        ], className='pipeline-sync-options'),
        html.Label('Pipe details', htmlFor='pipeline-inspector-section'),
        dbc.Select(id='pipeline-inspector-section', value='query', size='sm', class_name='dbc_dark custom-select custom-select-sm',
                   options=[{'label': label, 'value': value} for value, label in [
                       ('query', 'Query'), ('plan', 'Data / Plan'), ('relations', 'Dependencies'),
                       ('runs', 'Recent syncs'), ('parameters', 'Parameters')]
                       + ([('edit', 'Edit dependencies and query')] if edit else [])]),
        ### The same words as a pipe's Definition tab (`pipes.build_sql_body`).
        dbc.Switch(id='pipeline-resolve-symlinks', label='Show resolved query', value=True),
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
    if not selection or not _may_read(session):
        raise PreventUpdate
    ref = selection['ref']
    ### The selection is the browser's: its pipe must be one the viewer may see.
    if not _visible_ref(ref, session):
        raise PreventUpdate
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
                dcc.Store(id='pipeline-edit-baseline', data={'ref': ref, 'parameters': parameters, 'baseline': fingerprint(parameters)}),
                html.Label('Parents'),
                dbc.Textarea(id='pipeline-edit-parents', value=json.dumps(parameters.get('parents', parameters.get('parent', [])), indent=2)),
                html.Label('Children'),
                dbc.Textarea(id='pipeline-edit-children', value=json.dumps(parameters.get('children', parameters.get('child', [])), indent=2)),
                html.Label('Query'),
                dbc.Textarea(id='pipeline-edit-query', value=raw_query(parameters), style={'minHeight': '180px'}),
                dbc.Button('Stage changes', id='pipeline-stage-edit', size='sm'),
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
                    build_data_range(planned['bounds'], planned.get('backtrack'))
                    if planned.get('bounds') else None]
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
    if not _may_read(session):
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
    from meerschaum.api.dash import access, pipes_access
    try:
        instance, connector, metric, location = _engine.split_ref(ref)
        viewer = pipes_access.viewer(session)
        pipe = access.visible_pipe(connector, metric, location, instance, viewer=viewer)
        if pipe is None:
            raise PermissionError('This Pipe is not available.')
        allowed, reason = access.may_run_pipe_action(viewer, pipe, 'sync')
        if not allowed:
            raise PermissionError(reason)
        if scope in ('parents', 'children') and not (viewer.is_admin or viewer.no_auth):
            ### Its parents or children run too: each must be the viewer's to sync.
            graph = _engine.plan_graph(pipe.instance_connector, [pipe],
                                       parents=(scope == 'parents'), children=(scope == 'children'))
            if graph.get('hidden'):
                raise PermissionError(f"Some of its {scope} are pipes you cannot see.")
        args = ['sync', 'pipes', '-i', instance, '-c', connector, '-m', metric,
                '-l', location if location is not None else 'None']
        if scope in ('parents', 'children'):
            args.append('--' + scope)
        for flag, value in (('--begin', begin), ('--end', end)):
            if value:
                args.extend([flag, value])
        run_id = new_console_run_id('canvas')
        args.extend(['--run-id', run_id])
        result = run_action(args, get_username_from_session((session or {}).get('session-id')),
                            metadata={'run_id': run_id, **({'pipeline': {key: definition.get(key)
                                for key in ('name', 'registry', 'id', 'revision')}}
                                if definition and definition.get('name') == pipeline else {})})
        if result['success']:
            dash.set_props('pipeline-node-run', {'data': {
                'ref': ref, 'label': _engine.ref_label(ref), 'scope': scope, 'run_id': run_id, 'instance': instance,
                'session_id': (session or {}).get('session-id'), 'begin': begin, 'end': end,
                'job': result['job'], 'stream': result['stream'],
                'events_stream': _engine.run_events_stream_path(
                    run_id, instance, wait=NODE_RUN_STREAM_WAIT)}})
            dash.set_props('pipeline-node-run-label', {'children': f"{_engine.ref_label(ref)} · {scope} · {begin or 'last sync'} → {end or 'latest data'} · {run_id} · {result['job']}"})
            dash.set_props('pipelines-interval', {'interval': FALLBACK_POLL_MS})
        return alert_from_success_tuple((result['success'], result['msg']))
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))


def build_data_range(bounds, backtrack=None):
    """Align sync bounds without applying monospace formatting to their labels."""
    rows = []
    for label, key, fallback in (('From', 'begin', 'Earliest available data'),
                                  ('To', 'end', 'Latest data')):
        value = bounds.get(key)
        if key == 'begin' and value == '':
            ### `begin=''`: from the last sync, minus the backtrack window.
            shown = html.Span(f"Last sync − {backtrack}" if backtrack else 'Last sync')
        elif value is None:
            shown = html.Span(fallback)
        else:
            shown = html.Pre(_engine.bound_label(value))
        rows.append(html.Tr([
            html.Th(label, scope='row'),
            html.Td('≥' if key == 'begin' else '<',
                    title='Inclusive' if key == 'begin' else 'Exclusive'),
            html.Td(shown),
        ]))
    return html.Div([html.Div('Data range:'), html.Table(html.Tbody(rows))],
                    className='pipeline-data-range')


def build_runs_table(runs: List[Dict[str, Any]], table_id='pipelines-runs-table', registry=None, pipeline=None) -> Any:
    """
    The runs table: each run's ID links to its timeline.
    """
    if not runs:
        return html.P("No runs match.")
    link_to_pipeline = table_id.startswith(('pipeline-card-history-', 'pipeline-row-history-'))
    rows = []
    for run in runs:
        record = run['run']
        counts = _engine.counts_label(run.get('counts'))
        rows.append(html.Tr([
            html.Td(
                html.A(
                    record['run_id'],
                    href=('/dash/pipelines/' + quote(pipeline or '', safe='') + '?' + urlencode({'run_id': record['run_id'], **({'instance': registry} if registry else {})})) if link_to_pipeline else '#',
                    **({} if link_to_pipeline else {'data-run-id': record['run_id']}),
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
            html.Th('Duration (s)'), html.Th('Pipes'), html.Th('Message'),
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
        return [html.P('This run synced nothing.', className='text-muted')]
    t = lambda text: (str(text)[11:23] if text else '')
    rows = [
        html.Tr([
            html.Td(html.A(
                html.Code(_engine.ref_label(bar['pipe_ref'])), href=_engine.pipe_href(bar['pipe_ref']),
                className='pipe-link', target='_blank', rel='noopener noreferrer',
            )),
            html.Td(_engine.action_label(bar.get('action', ''))),
            html.Td('running' if bar.get('actual_start') and not bar.get('actual_end')
                    else bar.get('status', '').replace('_', ' ')),
            html.Td('Started' if bar.get('actual_start') else (
                'Not run' if bar.get('status') in ('skipped', 'observed', 'upstream_failed', 'rebuild_pending')
                else 'Waiting')),
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
                html.Th('Progress'), html.Th('Started at'), html.Th('Ended at'),
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
    Input('pipeline-definition', 'data'),
    Input('pipeline-draft', 'data'),
    Input('pipelines-refresh-button', 'n_clicks'),
    Input('pipeline-canvas-prefs', 'data'),
    State('pipelines-location', 'search'), State('session-store', 'data'),
)
def render_pipeline_graph(pathname, definition, draft, refresh, prefs, search, session_data):
    """Only membership, draft, expansion changes and explicit refreshes rebuild the canvas."""
    if (not str(pathname).startswith('/dash/pipelines/')
        or str(pathname).rstrip('/').split('/')[-1] in ('new', 'explore')):
        raise PreventUpdate
    if not _may_read(session_data):
        raise PreventUpdate
    if not definition:
        return []
    instance = _engine.get_instance(parse_qs((search or '').lstrip('?')).get('instance', [None])[0])
    try:
        valid_draft = draft and draft.get('definition_id') == definition.get('id', definition.get('name'))
        selection = draft if valid_draft else definition
        new_pipes = (selection.get('new_pipes') or {}) if valid_draft else {}
        targets = list(selection.get('targets', []))
        from meerschaum.api.dash.pipeline_editing import make_pipe
        pipes = [make_pipe(ref, new_pipes.get(ref)) for ref in targets if ref not in new_pipes]
        graph = (_engine.plan_graph(instance, pipes, parents=selection.get('parents'),
                                    children=selection.get('children'))
                 if pipes else {'nodes': [], 'edges': [], 'warnings': []})
        planned = {node['pipe_ref'] for node in graph['nodes']}
        graph['nodes'].extend({'pipe_ref': ref, 'label': _engine.ref_label(ref), 'action': 'root',
                               'reason': 'New Pipe', 'status': 'never_run'}
                              for ref in targets if ref not in planned)
        if valid_draft and draft.get('pipe_edits'):
            from meerschaum.api.dash.pipeline_editing import validate_edits
            edges = validate_edits(draft['pipe_edits'], [ref for ref in targets if ref not in new_pipes])
            visible = {node['pipe_ref'] for node in graph['nodes']}
            graph = {**graph, 'edges': [{'parent': parent, 'child': child}
                for parent, child in edges if parent in visible and child in visible]}
        key = f"{definition.get('registry')}|{definition.get('name')}"
        graph_children = build_pipeline_canvas(graph, targets, (prefs or {}).get(key) or {},
                                               definition, registry=str(instance))
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
    Input('pipeline-run-tick', 'data'),
    State('pipeline-run-streams', 'data'),
)
def render_pipelines(pathname, search, definition, live_run, selected_run, node_execution_events, n_intervals, n_clicks, events_store, log_stage, session_data, run_tick=None, run_streams=None):
    """
    Render run details and history independently of the canvas. A live selected run is followed
    over its event stream (`pipeline-run-follow`), whose events re-render this (`pipeline-run-tick`);
    without a stream the page polls every `FALLBACK_POLL_MS`.
    """
    if (not str(pathname).startswith('/dash/pipelines')
        or str(pathname).rstrip('/').split('/')[-1] in ('new', 'explore')):
        raise PreventUpdate

    session_id = (session_data or {}).get('session-id', None)
    if not _may_read(session_data):
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
        message = f"Runs are unavailable: {e}"
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
    current_job = (live_run or {}).get('job') if live_matches and selected_id == live_id else None
    member_refs = stage_refs | set((definition or {}).get('targets', []) if current_job else [])
    dash.set_props('pipeline-current-run', {'data': (
        {'run_id': selected_id, 'instance': str(instance), 'pipeline': name, 'job': current_job,
         'labels': {ref: _engine.ref_label(ref) for ref in sorted(member_refs)}}
        if name and selected_id and member_refs else None
    )})
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
    streaming = bool(active and (run_streams or {}).get(selected_id) in ('open', 'connecting'))
    dash.set_props('pipelines-interval', {'interval': poll_interval(active, streaming)})
    dash.set_props('pipeline-run-follow', {'data': (
        {'run_id': selected_id, 'path': _engine.run_events_stream_path(
            selected_id, instance, wait=(None if detail else NODE_RUN_STREAM_WAIT))}
        if active and selected_id else None
    )})
    if not detail:
        dash.set_props('pipeline-run-status', {'children': ''})
        dash.set_props('pipeline-stage-statuses', {'data': {}})
    if detail:
        summary = detail.get('summary') or {}
        counts = summary.get('counts') or {}
        completed = sum(value for key, value in counts.items() if key not in ('pending', 'running'))
        dash.set_props('pipeline-run-status', {'children':
            f"{summary.get('status', '')} · {completed} of {len(detail.get('bars', []))} pipes done"})
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
    ### Polling re-renders the views only when what they show changed (a live run always does):
    ### replacing an unchanged timeline and table would flash the Runs tab.
    import hashlib
    fingerprint = hashlib.sha256(json.dumps(
        [runs, detail, selected_id, wrong_pipeline, filters, str(instance)], sort_keys=True, default=str,
    ).encode()).hexdigest()
    if not active and store.get('fingerprint') == fingerprint:
        return dash.no_update, dash.no_update, [], dash.no_update
    store['fingerprint'] = fingerprint
    return (
        (build_gantt_children(detail) if detail else [html.P(
            (f"Run '{selected_id or filters['run_id']}' belongs to another Pipeline on {instance}." if wrong_pipeline
             else f"Run '{selected_id}' is unavailable on {instance}." if selected_id
             else 'No runs yet.')
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
    Input('pipeline-run-tick', 'data'),
)
def render_events(store, n_intervals, stage, search, session_data, run_tick=None):
    """
    The selected run's events, oldest first.
    """
    run_id = (store or {}).get('run_id', None)
    session_id = (session_data or {}).get('session-id', None)
    if not run_id or not _may_read(session_data):
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
        // Live runs' streamed statuses are newer than the persisted ones.
        const live = window.mrsmLiveRunStatuses ? window.mrsmLiveRunStatuses() : {};
        if (window.mrsmPaintPipelineStatuses) window.mrsmPaintPipelineStatuses({...merged, ...live});
        if (window.mrsmDecorateChunkProgress) window.mrsmDecorateChunkProgress();
        return dash_clientside.no_update;
    }
    """,
    Output('pipeline-stage-status-dummy', 'children'), Input('pipeline-stage-statuses', 'data'),
    Input('pipeline-node-statuses', 'data'),
)


dash_app.clientside_callback(
    r"""function(run, selected, events, session, member) {
        // Each execution is keyed by its job (a canvas sync) or by its run and Pipe (a member of
        // a Pipeline run, whose output is that Pipe's events within the run).
        const cache = window._mrsmNodeOutput = window._mrsmNodeOutput || new Map();
        const sessionId = (session || {})['session-id'] || null;
        const keyOf = r => r.key || r.job;
        const options = () => Array.from(cache.values()).map(r => ({label:(r.label || r.ref) + ' ' + r.run_id, value:keyOf(r)}));
        const runs = () => Array.from(cache.values()).map(r => ({key:keyOf(r), job:r.job, run_id:r.run_id, ref:r.ref, instance:r.instance, scope:r.scope}));
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
        const triggered = (dash_clientside.callback_context.triggered || []).map(t => t.prop_id);
        if (member && triggered.includes('pipeline-member-output.data')) {
            if (!cache.has(member.key)) {
                cache.set(member.key, {...member, status: 'running', lines: ['Waiting for this Pipe’s output…']});
                dash_clientside.set_props('pipeline-node-runs', {data: runs()});
            }
            dash_clientside.set_props('pipeline-output-tab', {options: options(), value: member.key});
            selected = member.key;
        }
        if (run && run.session_id !== sessionId) return dash_clientside.no_update;
        if (run && run.job && !cache.has(run.job)) {
            window._mrsmOutputDismissed = false;
            const dock = document.getElementById('pipeline-output-dock');
            if (dock) dock.dataset.outputMode = 'pipe';
            const item = {...run, key: run.job, status: 'running', lines: []}; cache.set(run.job, item);
            document.getElementById('pipeline-output-dock')?.classList.remove('pipeline-output-empty');
            dash_clientside.set_props('pipeline-node-runs', {data: runs()});
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
            else item.lines.push('Waiting for this Pipe’s output…');
            source.addEventListener('stopped', function() { source.close(); item.status = 'stopped'; item.lines.push('[Execution finished]'); if (window.mrsmUnfollowEngineRun) window.mrsmUnfollowEngineRun(item.run_id, 'node'); if (window.mrsmFinishNodeOutput) window.mrsmFinishNodeOutput(item); paint(); });
            source.onerror = function() { if (source.readyState === 2) { item.lines.push('[Output disconnected]'); paint(); } };
            dash_clientside.set_props('pipeline-output-tab', {options: options(), value:run.job});
            selected = run.job;
        }
        for (const event of (events || [])) {
            const key = event.key || event.job;
            if (cache.has(key)) {
                const item = cache.get(key);
                item.status = event.status || item.status;
                // An ended execution's live run statuses must not outlive it.
                if (item.scope !== 'pipeline' && event.ended && !['running', 'paused'].includes(event.status) && window.mrsmUnfollowEngineRun)
                    window.mrsmUnfollowEngineRun(item.run_id, 'node');
                if (item.scope !== 'this' && event.run_id === item.run_id) item.lines = (event.lines || []).length ? event.lines : item.lines;
                if (window.mrsmUpdateOutputCancel) window.mrsmUpdateOutputCancel();
            }
        }
        if (!selected && cache.size) {
            selected = window._mrsmNodeOutputSelected || Array.from(cache.keys()).at(-1);
            dash_clientside.set_props('pipeline-output-tab', {options: options(), value:selected});
            dash_clientside.set_props('pipeline-node-runs', {data: runs()});
        }
        if (selected && cache.has(selected)) {
            if (!window._mrsmOutputDismissed) document.getElementById('pipeline-output-dock')?.classList.remove('pipeline-output-empty');
            window._mrsmNodeOutputSelected = selected;
            const item = cache.get(selected);
            dash_clientside.set_props('pipeline-node-run-label', {children: item.scope === 'pipeline'
                ? (item.label || item.ref) + " · Pipeline('" + item.pipeline + "') · " + item.run_id
                : (item.label || item.ref) + ' · ' + item.scope + ' · ' + (item.begin || 'last sync') + ' → ' + (item.end || 'latest data') + ' · ' + item.run_id + ' · ' + item.job});
            const view = document.getElementById('pipeline-node-output');
            if (view && window.mrsmRenderPipelineOutput) window.mrsmRenderPipelineOutput(view, item.lines.join('\n'));
            if (window.mrsmUpdateOutputCancel) window.mrsmUpdateOutputCancel();
        }
        return dash_clientside.no_update;
    }""",
    Output('pipeline-node-stream-dummy', 'children'),
    Input('pipeline-node-run', 'data'), Input('pipeline-output-tab', 'value'),
    Input('pipeline-node-events', 'data'), Input('session-store', 'data'),
    Input('pipeline-member-output', 'data'),
)


dash_app.clientside_callback(
    """function(current) {
        // `View Pipe output` on a member of this run shows that Pipe's part of the run.
        window._mrsmCurrentPipelineRun = current || null;
        return dash_clientside.no_update;
    }""",
    Output('pipeline-current-run-dummy', 'children'), Input('pipeline-current-run', 'data'),
)


dash_app.clientside_callback(
    """function(follow, nodeRun, session) {
        // Follow the selected live run and the latest canvas sync over their event streams
        // (`assets/pipeline_events.js`); a new session drops every stream.
        if (!window.mrsmFollowEngineRun) return dash_clientside.no_update;
        const sessionId = (session || {})['session-id'] || null;
        if (window._mrsmRunFollowSession !== sessionId) {
            window.mrsmUnfollowAllEngineRuns();
            window._mrsmRunFollowSession = sessionId;
        }
        window.mrsmFollowOnly('selected', follow && follow.run_id, follow && follow.path);
        if (nodeRun && nodeRun.session_id === sessionId && nodeRun.run_id && nodeRun.events_stream)
            window.mrsmFollowEngineRun(nodeRun.run_id, nodeRun.events_stream, 'node');
        return dash_clientside.no_update;
    }""",
    Output('pipeline-run-follow-dummy', 'children'),
    Input('pipeline-run-follow', 'data'), Input('pipeline-node-run', 'data'),
    Input('session-store', 'data'),
)


@dash_app.callback(
    Output('pipeline-node-events', 'data'), Output('pipeline-node-statuses', 'data'),
    Input('pipeline-node-runs', 'data'), Input('pipelines-interval', 'n_intervals'),
    State('session-store', 'data'),
    Input('pipeline-run-tick', 'data'),
)
def node_events(runs, ticks, session, run_tick=None):
    """Poll every retained execution by its exact identity, including older active tabs."""
    if not runs or not _may_read(session):
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
            if run.get('scope') == 'pipeline':
                ### A Pipeline run's history is readable like any run on the Runs view; only its
                ### own job (when this session started it) can tell that it still runs.
                rows = _engine.get_run_events(_engine.get_instance(run['instance'], strict=True), run['run_id'])
                finished = any(row.get('event') == 'run_finished' for row in rows)
                worker_status = 'stopped' if finished else 'running'
                if run.get('job') and not finished:
                    try:
                        job = get_job(run['job'])
                        if job_visible_to_session(job, (session or {}).get('session-id')):
                            worker_status = job.status
                            ended = str(job.ended) if job.ended else None
                    except Exception:
                        pass
            else:
                job = get_job(run['job'])
                metadata = job.daemon.properties.get('mrsm_execution') or {}
                if (not metadata or not job_visible_to_session(job, (session or {}).get('session-id'))
                        or metadata.get('run_id') != run['run_id']):
                    raise ValueError('This execution is unavailable or belongs to another session.')
                worker_status = job.status
                ended = str(job.ended) if job.ended else None
                rows = _engine.get_run_events(_engine.get_instance(run['instance'], strict=True), run['run_id'])
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
            lines = [f"[{row.get('seq', '')}"
                     + (f" / attempt {attempt}" if (attempt := (row.get('payload') or {}).get('attempt')) else '')
                     + "] " + _engine.describe_event(row)
                     for row in rows if row.get('pipe_ref') == run['ref']][-MAX_EVENT_LINES:]
        except Exception as exc:
            lines = [f'Events unavailable: {exc}']
        if worker_status not in ('running', 'paused'):
            for ref in {run['ref']} | {row.get('pipe_ref') for row in rows if row.get('pipe_ref')}:
                if statuses.get(ref) in ('running', 'pending'):
                    statuses[ref] = 'cancelled'
        events.append({'key': run.get('key') or run.get('job'), 'job': run.get('job'),
                       'run_id': run['run_id'], 'lines': lines, 'status': worker_status, 'ended': ended})
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
    if not _may_read(session):
        raise PreventUpdate
    run = next((run for run in (runs or []) if (run.get('key') or run.get('job')) == selected
                and run.get('scope') != 'pipeline'), None)
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
        ### One's own run, or anyone's with `jobs:stop`.
        from meerschaum.api.dash import pipes_access
        viewer = pipes_access.viewer(session)
        if not (viewer.can('jobs:stop') or (viewer.username and getattr(job, 'owner', None) == viewer.username)):
            raise PermissionError('Your account may not stop this execution.')
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
