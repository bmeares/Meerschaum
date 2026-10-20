#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Callbacks of the pipelines page (`/dash/pipelines`).
"""

from __future__ import annotations

from urllib.parse import parse_qs, urlencode

import dash
from dash.dependencies import Input, Output, State
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
    """
    The dependency graph, its legend and the edges table.
    """
    if not graph['nodes']:
        return [html.P("No pipes are registered.")]

    figure = _engine.graph_figure(graph, failed_refs=failed_refs)
    legend_items = [
        ('Root', _engine.STATUS_COLORS['root']),
        ('Caught up', _engine.STATUS_COLORS['caught_up']),
        ('Pending', _engine.STATUS_COLORS['pending']),
        ('Rebuild / failed', _engine.STATUS_COLORS['rebuild']),
        ('Observed', _engine.STATUS_COLORS['observe']),
    ]
    legend = html.Div(
        [
            html.Span(
                [
                    html.Span('●', style={'color': color, 'margin-right': '4px'}),
                    html.Span(label, style={'margin-right': '1.2em'}),
                ]
            )
            for label, color in legend_items
        ],
        className='text-muted',
        style={'font-size': 'small'},
    )
    rows = [
        html.Tr([
            html.Td(html.A(_engine.ref_label(edge['parent']), href=_engine.pipe_href(edge['parent']))),
            html.Td(html.A(_engine.ref_label(edge['child']), href=_engine.pipe_href(edge['child']))),
            html.Td(edge['label'], className=('text-warning' if edge['lagging'] else '')),
        ])
        for edge in graph['edges']
    ]
    children = [
        dcc.Graph(
            figure=figure,
            id='pipelines-graph',
            config={'displaylogo': False},
        ),
        legend,
    ]
    if rows:
        children += [
            html.Br(),
            dbc.Table(
                [html.Thead(html.Tr([html.Th('Parent'), html.Th('Child'), html.Th('Ledger lag')]))]
                + [html.Tbody(rows)],
                hover=True,
                size='sm',
                id='pipelines-edges-table',
            ),
        ]
    for message in graph['warnings']:
        children.append(html.P(message, className='text-warning'))
    return children


def build_runs_table(runs: List[Dict[str, Any]]) -> Any:
    """
    The runs table: each run's ID links to its timeline.
    """
    if not runs:
        return html.P("No runs match.")
    rows = []
    for run in runs:
        record = run['run']
        counts = ', '.join(f"{key}: {value}" for key, value in (run.get('counts') or {}).items())
        rows.append(html.Tr([
            html.Td(
                html.A(
                    record['run_id'],
                    href='/dash/pipelines?' + urlencode({'run_id': record['run_id']}),
                )
            ),
            html.Td(run.get('status', ''), className=f"run-status-{run.get('status', '')}"),
            html.Td(str(record.get('started', ''))[:19].replace('T', ' ')),
            html.Td(f"{float(run.get('duration_seconds') or 0.0):.1f}"),
            html.Td(counts),
            html.Td(record.get('message') or ''),
        ]))
    return dbc.Table(
        [html.Thead(html.Tr([
            html.Th('Run'), html.Th('Status'), html.Th('Started'),
            html.Th('Duration (s)'), html.Th('Nodes'), html.Th('Message'),
        ]))] + [html.Tbody(rows)],
        hover=True,
        size='sm',
        id='pipelines-runs-table',
    )


def build_gantt_children(detail: Optional[Dict[str, Any]]) -> List[Any]:
    """
    The Gantt figure of a run and its nodes table.
    """
    if not detail:
        return [html.P("No run selected.")]
    t = lambda text: (str(text)[11:23] if text else '')
    rows = [
        html.Tr([
            html.Td(html.A(
                _engine.ref_label(bar['pipe_ref']), href=_engine.pipe_href(bar['pipe_ref']),
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
        dcc.Graph(
            figure=_engine.gantt_figure(detail),
            id='pipelines-gantt',
            config={'displaylogo': False},
        ),
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
    Input('pipelines-interval', 'n_intervals'),
    Input('pipelines-refresh-button', 'n_clicks'),
    State('pipelines-events-store', 'data'),
    State('session-store', 'data'),
)
def render_pipelines(pathname, search, n_intervals, n_clicks, events_store, session_data):
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
    try:
        graph = _engine.plan_graph(instance)
        graph_children = build_graph_children(graph, failed_refs)
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
