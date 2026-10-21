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


def build_graph_children(graph: Dict[str, Any], failed_refs: List[str]) -> List[Any]:
    """Readable stage cards; the local canvas asset draws their connections."""
    if not graph['nodes']:
        return [html.P("No pipes are registered.")]
    positions = _engine.canvas_positions(graph)
    cards = []
    for node in graph['nodes']:
        ref = node['pipe_ref']
        x, y = positions[ref]
        status = 'failed' if ref in failed_refs else node['status']
        cards.append(html.Button([
            _engine.action_indicator(node['action'], failed=ref in failed_refs),
            html.Code(node['label']),
        ], id={'type': 'pipeline-stage', 'index': ref}, n_clicks=0,
            className='pipeline-stage', title=node['label'],
            style={'left': f'{32 + x * 380}px', 'top': f'{48 + y * 190}px',
                   '--stage-color': _engine.STATUS_COLORS.get(status, '#9aa5b1')},
            **{'data-ref': ref, 'aria-label': f"Inspect {node['label']}"}))
    return [
        html.Div([
            html.Div([
                html.Span(f"{len(cards)} pipes · {len(graph['edges'])} connections"),
                html.Div([
                    html.Button('−', id='pipeline-zoom-out', title='Zoom out'),
                    html.Button('+', id='pipeline-zoom-in', title='Zoom in'),
                    html.Button('Fit', id='pipeline-fit', title='Fit pipeline to view'),
                ], className='pipeline-canvas-controls'),
            ], className='pipeline-canvas-toolbar'),
            html.Div(html.Div(cards, id='pipeline-canvas-world', key=json.dumps(graph, sort_keys=True), style={
                'width': f'{max(x for x, _ in positions.values()) * 380 + 390}px',
                'height': f'{max(y for _, y in positions.values()) * 190 + 240}px',
            }, **{'data-edges': json.dumps(graph['edges']), 'data-focus': graph.get('focus', '')}),
                id='pipeline-canvas', tabIndex=0,
                **{'aria-label': 'Pipeline stages. Drag to pan; use the zoom controls.'}),
        ], className='pipeline-canvas-shell'),
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
        dash.set_props('pipeline-inspector', {'hidden': True})
        dash.set_props('pipeline-workspace', {'className': 'pipeline-workspace'})
        if triggered == 'pipeline-definition':
            dash.set_props('pipeline-focus', {'value': None})
        return []
    if triggered == 'pipeline-focus' and focus:
        ref = focus
    elif isinstance(triggered, dict) and any(clicks):
        ref = triggered['index']
    else:
        raise PreventUpdate
    dash.set_props('pipeline-inspector', {'hidden': False})
    dash.set_props('pipeline-workspace', {'className': 'pipeline-workspace pipeline-has-selection'})
    instance, connector, metric, location = _engine.split_ref(ref)
    pipe = mrsm.Pipe(connector, metric, location, instance=instance)
    try:
        parameters = pipe.parameters
        from meerschaum.api.dash._support import get_pipe_query
        query = get_pipe_query(pipe, apply_symlinks=False, warn=False)
        sync_time = pipe.get_sync_time()
        runs = _engine.get_runs(pipe.instance_connector, pipe=ref, limit=100)
        graph = _engine.plan_graph(pipe.instance_connector, [pipe], parents=1)
        planned = next((node for node in graph['nodes'] if node['pipe_ref'] == ref), {})
        from meerschaum.core.Pipeline import _bound_text
        bounds = planned.get('bounds') if planned.get('action') != 'root' else None
        window = ''
        if bounds:
            begin, end = bounds.get('begin'), bounds.get('end')
            start = _bound_text(begin) if begin is not None else 'the earliest available data'
            stop = ' to before ' + _bound_text(end) if end is not None else ' onward'
            window = 'From ' + start + stop
        from meerschaum.api.dash.pipes import build_relations_body
        return [
            html.Div([
                html.H4(html.Code(str(pipe)), className='pipeline-inspector-title'),
                html.Button('×', id='pipeline-inspector-close', n_clicks=0,
                            title='Close pipe details', **{'aria-label': 'Close pipe details'}),
            ], className='pipeline-inspector-header'),
            html.P(['Latest data: ', html.Strong(str(sync_time) if sync_time is not None else 'No data yet')]),
            dbc.Button('Open pipe', href=_engine.pipe_href(ref), target='_blank', rel='noopener noreferrer', color='secondary', size='sm'),
            html.H5('Next sync'),
            html.P(_engine.action_indicator(planned.get('action', ''))),
            html.P(['Data range: ', html.Code(window)]) if window else None,
            html.P('This range is recalculated after the parents finish syncing.', className='text-muted')
            if planned.get('provisional') else None,
            html.Details([html.Summary('Planner details'), html.P(planned.get('reason', ''))]),
            html.H5('Query'), html.Pre(query or 'This source uses a connector or plugin to fetch data.'),
            html.H5('Dependencies'), build_relations_body(pipe),
            html.H5('Recent syncs'), build_runs_table(runs, table_id='pipeline-stage-runs-table'),
            html.Details([html.Summary('Parameters'), html.Pre(json.dumps(parameters, indent=2, default=str))]),
        ]
    except Exception as exc:
        return [html.Button('×', id='pipeline-inspector-close', n_clicks=0, title='Close pipe details'),
                html.H4(_engine.ref_label(ref)), html.P(f"Could not inspect this pipe: {exc}")]


def build_runs_table(runs: List[Dict[str, Any]], table_id='pipelines-runs-table') -> Any:
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
                    href='/dash/pipelines?' + urlencode({'run_id': record['run_id']}),
                    className='pipeline-run-link',
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
    ), className='pipeline-runs-scroll')


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
            html.Td(bar.get('status', '')),
            html.Td(t(bar.get('planned_start'))),
            html.Td(t(bar.get('actual_start'))),
            html.Td(t(bar.get('actual_end'))),
        ])
        for bar in detail.get('bars', [])
    ]
    return [
        html.Div(dcc.Graph(
            figure=_engine.gantt_figure(detail),
            id='pipelines-gantt',
            config={'displaylogo': False},
            style={'minWidth': '1000px'},
        ), className='pipeline-timeline-scroll'),
        dbc.Table(
            [html.Thead(html.Tr([
                html.Th('Pipe'), html.Th('Action'), html.Th('Status'),
                html.Th('Planned start'), html.Th('Actual start'), html.Th('Actual end'),
            ]))] + [html.Tbody(rows)],
            hover=True,
            size='sm',
            id='pipelines-nodes-table',
        ),
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
    query = {
        key: value
        for key, value in (('run_id', run_id), ('pipe', pipe), ('status', status))
        if value and value != 'any'
    }
    search = ('?' + urlencode(query)) if query else ''
    if search == (current_search or ''):
        raise PreventUpdate
    return search


@dash_app.callback(
    Output('pipelines-graph-div', 'children'),
    Output('pipelines-gantt-div', 'children'),
    Output('pipelines-runs-div', 'children'),
    Output('pipelines-alert-div', 'children'),
    Output('pipelines-events-store', 'data'),
    Input('pipelines-location', 'pathname'),
    Input('pipelines-location', 'search'),
    Input('pipeline-focus', 'value'),
    Input('pipeline-standalone', 'value'),
    Input('pipeline-definition', 'data'),
    Input('pipelines-interval', 'n_intervals'),
    Input('pipelines-refresh-button', 'n_clicks'),
    State('pipelines-events-store', 'data'),
    State('session-store', 'data'),
)
def render_pipelines(pathname, search, focus, standalone, definition, n_intervals, n_clicks, events_store, session_data):
    """
    Render the graph, the Gantt of the selected (else the latest) run and the runs table.
    """
    if not str(pathname).startswith('/dash/pipelines'):
        raise PreventUpdate

    session_id = (session_data or {}).get('session-id', None)
    if not is_session_authenticated(str(session_id)):
        raise PreventUpdate

    filters = _filters_from_search(search)
    try:
        instance = _engine.get_instance()
        runs = _engine.get_runs(
            instance,
            run_id=filters['run_id'] or None,
            pipe=filters['pipe'] or None,
            status=filters['status'] or None,
        )
        all_runs = (
            runs
            if not any(filters.values())
            else _engine.get_runs(instance, limit=1)
        )
    except Exception as e:
        warn(f"Could not read the engine state: {e}", stack=False)
        message = f"Engine state unavailable: {e}"
        return (
            [html.P(message)], [html.P("No run selected.")], [html.P("No runs match.")],
            alert_from_success_tuple((False, message)),
            {},
        )

    selected_id = filters['run_id'] or (
        runs[0]['run']['run_id'] if runs else (all_runs[0]['run']['run_id'] if all_runs else None)
    )
    try:
        detail = _engine.get_run_detail(instance, selected_id) if selected_id else None
    except Exception as e:
        warn(f"Could not read run '{selected_id}': {e}", stack=False)
        detail = None

    failed_refs = [
        node['pipe_ref'] for node in ((detail or {}).get('nodes') or [])
        if node.get('status') in ('failed', 'upstream_failed')
    ]
    if dash.ctx.triggered_id == 'pipelines-interval':
        graph_children = dash.no_update
    else:
        try:
            graph = _engine.plan_graph(instance)
            options = [{'label': node['label'], 'value': node['pipe_ref']} for node in graph['nodes']]
            if definition and definition.get('targets'):
                selected_pipes = []
                for ref in definition['targets']:
                    instance_keys, connector, metric, location = _engine.split_ref(ref)
                    selected_pipes.append(mrsm.Pipe(connector, metric, location, instance=instance_keys))
                graph = _engine.plan_graph(instance, selected_pipes,
                                           parents=definition.get('parents'),
                                           children=definition.get('children'))
            if focus:
                graph = {**_engine.focus_graph(graph, focus), 'focus': focus}
            elif graph['edges'] and not standalone and not definition:
                connected = {edge[key] for edge in graph['edges'] for key in ('parent', 'child')}
                graph = {**graph, 'nodes': [node for node in graph['nodes'] if node['pipe_ref'] in connected]}
            graph_children = build_graph_children(graph, failed_refs)
            dash.set_props('pipeline-focus-options', {'data': options})
            dash.set_props('pipeline-visible-refs', {
                'data': [node['pipe_ref'] for node in graph['nodes']],
            })
        except Exception as e:
            warn(f"Could not plan the pipeline graph: {e}", stack=False)
            graph_children = [html.P(f"The dependency graph is unavailable: {e}")]

    store = dict(events_store or {})
    if store.get('run_id') != selected_id:
        store = {'run_id': selected_id}
    return (
        graph_children,
        build_gantt_children(detail),
        build_runs_table(runs),
        [],
        store,
    )


@dash_app.callback(
    Output('pipelines-events-pre', 'children'),
    Input('pipelines-events-store', 'data'),
    Input('pipelines-interval', 'n_intervals'),
    State('session-store', 'data'),
)
def render_events(store, n_intervals, session_data):
    """
    The selected run's events, oldest first.
    """
    run_id = (store or {}).get('run_id', None)
    session_id = (session_data or {}).get('session-id', None)
    if not run_id or not is_session_authenticated(str(session_id)):
        return ''
    try:
        rows = _engine.get_run_events(_engine.get_instance(), run_id)
    except Exception as e:
        return f"Events unavailable: {e}"
    lines = [_engine.describe_event(row) for row in rows][-MAX_EVENT_LINES:]
    return '\n'.join(lines)
