#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
What the console's pipelines page and the pipe cards' Parents / Children tabs read from the
Syncing Engine: the dependency graph with each pipe's planned action, the runs, one run's
planned-versus-actual bars and its events.

These are the engine's own bindings (`meerschaum.core.Pipeline`), the same functions behind the
`/engine/*` state routes and `mrsm show runs`, `show run <id>` and `plan pipes`; nothing here
writes. Every function takes the instance (the API instance by default) and returns plain
`dict`s and `list`s.
"""

from __future__ import annotations

from datetime import datetime, timezone

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional, Tuple

#: Planned actions that mean the pipe is behind its parents (or needs a rebuild).
PENDING_ACTIONS = frozenset({'incremental', 'backfill'})
REBUILD_ACTIONS = frozenset({'rebuild', 'rebuild_pending'})

#: The colors of a node, by status (the Rust console's palette).
STATUS_COLORS: Dict[str, str] = {
    'caught_up': '#2e9e5b',
    'pending': '#e0a526',
    'rebuild': '#d1495b',
    'failed': '#d1495b',
    'root': '#3486bd',
    'observe': '#9aa5b1',
    'never_run': '#9aa5b1',
}


def split_ref(pipe_ref: str) -> Tuple[str, str, str, Optional[str]]:
    """
    Split a `pipe_ref` (`instance|connector_keys|metric_key|location_key`).
    """
    instance_keys, connector_keys, metric_key, location_key = pipe_ref.split('|', 3)
    return instance_keys, connector_keys, metric_key, (None if location_key == 'None' else location_key)


def ref_label(pipe_ref: str) -> str:
    """The official Pipe representation, including a non-default instance."""
    instance, connector, metric, location = split_ref(pipe_ref)
    return str(mrsm.Pipe(connector, metric, location, instance=instance))


def action_label(action: str) -> str:
    """Describe the next sync without exposing engine bookkeeping."""
    return {
        'root': 'Fetch source data', 'incremental': 'Needs sync',
        'backfill': 'Reprocess changed data', 'skip': 'Up-to-date',
        'rebuild': 'Rebuild required', 'rebuild_pending': 'Rebuild needs approval',
        'observe': 'Outside this selection',
    }.get(action, action.replace('_', ' ').capitalize())


def action_indicator(action: str, failed: bool = False) -> Any:
    """An accessible icon whose sync status label appears on hover."""
    from meerschaum.utils.packages import import_html
    html = import_html()
    label = 'Last run failed' if failed else action_label(action)
    symbol = '!' if failed else {
        'root': '☁', 'skip': '✓', 'incremental': '↻', 'backfill': '↻',
        'rebuild': '⚠', 'rebuild_pending': '⚠', 'observe': '◌',
    }.get(action, '◌')
    return html.Span([
        html.Span(symbol, **{'aria-hidden': 'true'}),
    ], className='pipeline-stage-status', title=label, **{'aria-label': label})


def pipe_href(pipe_ref: str) -> str:
    """
    The console path of a pipe's card.
    """
    from urllib.parse import quote
    instance_keys, connector_keys, metric_key, location_key = split_ref(pipe_ref)
    parts = [connector_keys, metric_key] + ([location_key] if location_key is not None else [])
    return '/dash/pipes/' + '/'.join(quote(part, safe='') for part in parts) + (
        '?instance=' + quote(instance_keys, safe='')
    )


def get_instance(instance_keys: Optional[str] = None):
    """
    The instance connector (the API's by default).
    """
    from meerschaum.api import get_api_connector
    return get_api_connector(instance_keys) if instance_keys is None else mrsm.get_connector(instance_keys)


def _call(name: str, request: Dict[str, Any]) -> Any:
    from meerschaum.core.Pipeline._engine import call
    return call(name, request)


def _request(instance: Any, **kwargs: Any) -> Dict[str, Any]:
    from meerschaum.core.Pipeline._engine import build_request
    return build_request(instance, **kwargs)


def _instance_pipes(instance: Any) -> List[mrsm.Pipe]:
    from meerschaum.utils.pipes import flatten_pipes_dict
    pipes_dict = mrsm.get_pipes(instance=instance, as_list=False)
    return [pipe for pipe in flatten_pipes_dict(pipes_dict) if pipe.connector_keys != 'mrsm']


def plan_graph(
    instance: Any = None,
    pipes: Optional[List[mrsm.Pipe]] = None,
    parents: Any = None,
    children: Any = None,
) -> Dict[str, Any]:
    """
    Plan a selection and return its graph: `{nodes, edges, warnings}`.

    Each node is `{pipe_ref, label, action, reason, provisional, status}` and each edge
    `{parent, child, label, lagging}`. With no `pipes`, every registered pipe of the instance is
    selected. `parents` / `children` extend a selection as `Pipeline(...)` does.
    """
    from meerschaum.core.Pipeline import Plan
    instance = instance if instance is not None else get_instance()
    selected = pipes if pipes is not None else _instance_pipes(instance)
    if not selected:
        return {'nodes': [], 'edges': [], 'warnings': []}
    request = _request(
        instance,
        targets=selected,
        parents=True if pipes is None and parents is None else parents,
        children=children,
    )
    # Preserve dependencies inside the selected membership without adding outside pipes.
    request['graph'] = True
    plan = Plan(_call('engine_plan', request))
    nodes = []
    by_ref = {}
    for planned in plan.nodes:
        node = {
            'pipe_ref': planned.pipe_ref,
            'label': ref_label(planned.pipe_ref),
            'action': planned.action,
            'reason': planned.reason,
            'bounds': planned.bounds,
            'provisional': planned.provisional,
            'status': node_status(planned.action),
        }
        nodes.append(node)
        by_ref[planned.pipe_ref] = node
    edges = []
    for parent_ref, child_ref in plan.edges:
        child = by_ref.get(child_ref, {})
        lagging = child.get('action') in PENDING_ACTIONS | REBUILD_ACTIONS
        edges.append({
            'parent': parent_ref,
            'child': child_ref,
            'label': (
                action_label(child.get('action', ''))
            ),
            'lagging': lagging,
        })
    return {'nodes': nodes, 'edges': edges, 'warnings': list(plan.warnings)}


def node_status(action: str) -> str:
    """
    The status class of a planned node (`caught_up`, `pending`, `rebuild`, `root`, `observe`).
    """
    if action in REBUILD_ACTIONS:
        return 'rebuild'
    if action in PENDING_ACTIONS:
        return 'pending'
    if action == 'root':
        return 'root'
    if action == 'observe':
        return 'observe'
    return 'caught_up'


def layered_positions(
    refs: List[str],
    edges: List[Tuple[str, str]],
) -> Dict[str, Tuple[float, float]]:
    """
    Place nodes in columns by dependency depth (a parent is left of its children), spread
    evenly in each column. Cycles (which the engine rejects) are broken arbitrarily.
    """
    parents_of: Dict[str, List[str]] = {ref: [] for ref in refs}
    for parent, child in edges:
        if parent in parents_of and child in parents_of and parent != child:
            parents_of[child].append(parent)

    depth: Dict[str, int] = {}

    def _depth(ref: str, trail: Tuple[str, ...] = ()) -> int:
        if ref in depth:
            return depth[ref]
        if ref in trail:
            return 0
        value = 1 + max((_depth(p, trail + (ref,)) for p in parents_of[ref]), default=-1)
        depth[ref] = value
        return value

    for ref in refs:
        _depth(ref)

    columns: Dict[int, List[str]] = {}
    for ref in refs:
        columns.setdefault(depth[ref], []).append(ref)

    positions: Dict[str, Tuple[float, float]] = {}
    for column, members in columns.items():
        count = len(members)
        for i, ref in enumerate(sorted(members, key=ref_label)):
            positions[ref] = (float(column), (count - 1) / 2.0 - i)
    return positions


def canvas_positions(graph: Dict[str, Any]) -> Dict[str, Tuple[float, float]]:
    """Lay out each connected pipeline separately, with sources on the left."""
    refs = {node['pipe_ref'] for node in graph['nodes']}
    neighbours = {ref: set() for ref in refs}
    edges = [(edge['parent'], edge['child']) for edge in graph['edges']]
    for parent, child in edges:
        if parent in refs and child in refs:
            neighbours[parent].add(child)
            neighbours[child].add(parent)
    positions = {}
    offset = 0.0
    while refs:
        first = min(refs, key=lambda ref: (not neighbours[ref], ref_label(ref)))
        component, pending = set(), {first}
        while pending:
            ref = pending.pop()
            component.add(ref)
            pending.update(neighbours[ref] - component)
        refs.difference_update(component)
        local = layered_positions(sorted(component), edges)
        top = max(y for _, y in local.values())
        bottom = min(y for _, y in local.values())
        positions.update({ref: (x, offset + top - y) for ref, (x, y) in local.items()})
        offset += top - bottom + 1.5
    return positions


def focus_graph(graph: Dict[str, Any], selected: str) -> Dict[str, Any]:
    """Show the selected pipe's ancestors and descendants."""
    visible = {selected}
    for forward in (True, False):
        found = {selected}
        while True:
            added = {
                edge['child' if forward else 'parent'] for edge in graph['edges']
                if edge['parent' if forward else 'child'] in found
            } - found
            if not added:
                break
            found.update(added)
        visible.update(found)
    return {
        **graph,
        'nodes': [node for node in graph['nodes'] if node['pipe_ref'] in visible],
        'edges': [edge for edge in graph['edges'] if edge['parent'] in visible and edge['child'] in visible],
    }


def run_belongs_to_pipeline(run: Dict[str, Any], name: str) -> bool:
    """Match the complete name before the engine's timestamp and collision suffix."""
    import re
    run_id = run.get('run', {}).get('run_id', '')
    return bool(re.fullmatch(re.escape(name) + r'-\d{4}-\d\d-\d\dT\d\d-\d\d-\d\d\.\d{6}Z-[a-f0-9]{8}', run_id))


def get_runs(
    instance: Any = None,
    run_id: Optional[str] = None,
    pipe: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
    run_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Recent runs, newest first (`{run, status, duration_seconds, counts}` each). `run_id` selects
    one run; `pipe` matches a Pipe label or reference in the targets or executed stages.
    """
    instance = instance if instance is not None else get_instance()
    request = _request(instance)
    request['limit'] = limit
    if run_name:
        request['run_name'] = run_name
    if status:
        request['status'] = status
    if run_id:
        request['run_id'] = run_id
    if pipe:
        request['pipe'] = pipe.strip()
    return _call('engine_runs', request)


def get_run_detail(instance: Any, run_id: str) -> Optional[Dict[str, Any]]:
    """
    One run with its nodes and the planned-versus-actual bars (`None` for an unknown run).
    """
    instance = instance if instance is not None else get_instance()
    request = _request(instance)
    request['run_id'] = run_id
    return _call('engine_run_detail', request)


def get_run_events(instance: Any, run_id: str, after_seq: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    The persisted events of a run, in order.
    """
    from meerschaum.core.Pipeline import _decode_events
    instance = instance if instance is not None else get_instance()
    request = _request(instance)
    request['run_id'] = run_id
    if after_seq is not None:
        request['after_seq'] = after_seq
    return _decode_events(_call('engine_run_events', request))


def _parse_ts(text: Optional[str]) -> Optional[datetime]:
    if not text:
        return None
    try:
        return datetime.fromisoformat(str(text).replace('Z', '+00:00'))
    except ValueError:
        return None


def gantt_figure(detail: Dict[str, Any]) -> Dict[str, Any]:
    """
    A Plotly figure of a run's Gantt: the planned and the actual bar of each node.
    """
    traces = []
    for name, color, planned in (('Planned', '#9aa5b1', True), ('Actual', '#3486bd', False)):
        ys, bases, xs, colors, statuses = [], [], [], [], []
        for bar in detail.get('bars', []):
            start = _parse_ts(bar.get('planned_start' if planned else 'actual_start'))
            end = _parse_ts(bar.get('planned_end' if planned else 'actual_end'))
            if start is None:
                continue
            status = bar.get('status', '')
            if not planned and end is None:
                end = datetime.now(timezone.utc) if start.tzinfo else datetime.now()
                status = 'running'
            if end is None:
                continue
            colors.append(color if planned else {
                'running': '#3486bd', 'succeeded': '#2e9e5b', 'failed': '#d1495b',
                'upstream_failed': '#d1495b', 'skipped': '#9aa5b1',
                'rebuild_pending': '#e0a526',
            }.get(status, color))
            statuses.append(status)
            ys.append(ref_label(bar['pipe_ref']))
            bases.append(start.isoformat())
            xs.append(max(int((end - start).total_seconds() * 1000), 1))
        traces.append({
            'type': 'bar',
            'orientation': 'h',
            'name': name,
            'y': ys,
            'base': bases,
            'x': xs,
            'marker': {'color': colors},
            'customdata': statuses,
            'hovertemplate': '%{y}<br>%{customdata}<extra>%{fullData.name}</extra>',
        })
    run_id = (detail.get('summary', {}).get('run', {}) or {}).get('run_id', '')
    return {
        'data': traces,
        'layout': {
            'title': {'text': f"Run {run_id}"},
            'xaxis': {'type': 'date'},
            'margin': {'l': 30, 'r': 30, 't': 60, 'b': 60},
            'barmode': 'group',
            'uirevision': run_id,
            'yaxis': {'autorange': 'reversed', 'automargin': True,
                      'categoryorder': 'array',
                      'categoryarray': [ref_label(bar['pipe_ref']) for bar in detail.get('bars', [])]},
            'height': max(260, 120 + 60 * len({t for t in traces[0]['y']} if traces else [])),
            'paper_bgcolor': 'rgba(0,0,0,0)',
            'plot_bgcolor': 'rgba(0,0,0,0)',
            'font': {'color': '#d0d6dc'},
        },
    }


def describe_event(row: Dict[str, Any]) -> str:
    """
    One line of the event log: `<time> <event> <pipe> <details>`.
    """
    payload = row.get('payload') or {}
    event = row.get('event', '')
    ts = str(row.get('ts', ''))[11:23]
    pipe_ref = row.get('pipe_ref')
    out = f"{ts} {event}" + (f" {ref_label(pipe_ref)}" if pipe_ref else '')
    if event in ('chunk_started', 'chunk_finished'):
        out += f" chunk {payload.get('chunk', '')}"
        if payload.get('total') is not None:
            out += f"/{payload['total']}"
        if payload.get('rows') is not None:
            out += f" rows={payload['rows']}"
        if event == 'chunk_finished':
            out += ' FAILED' if payload.get('success') is False else ' ok'
    elif event == 'node_started':
        out += f" {payload.get('action', '')}"
    elif event == 'node_finished':
        result = payload.get('result') or {}
        out += ' ok' if result.get('success', True) else ' FAILED'
        for key in ('inserted', 'updated', 'upserted'):
            if result.get(key):
                out += f" {key}={result[key]}"
        if result.get('message'):
            out += f" {result['message']}"
    elif event == 'run_started':
        out += f" nodes={payload.get('nodes', '')}"
    return out.strip()


def relations_of(pipe: mrsm.Pipe) -> Dict[str, List[Dict[str, Any]]]:
    """
    The Parents and Children tabs of a pipe card: `{'parents': [...], 'children': [...]}`, each a
    list of `{pipe_ref, label, href, action, reason, lagging}` (the neighbour's planned action,
    with the planner's reason for the edge).
    """
    if pipe.get_id() is None:
        return {'parents': [], 'children': []}
    graph = plan_graph(pipe.instance_connector, [pipe], parents=1, children=1)
    this_ref = '|'.join((
        str(pipe.instance_keys), str(pipe.connector_keys), str(pipe.metric_key),
        str(pipe.location_key),
    ))
    nodes = {node['pipe_ref']: node for node in graph['nodes']}
    out: Dict[str, List[Dict[str, Any]]] = {'parents': [], 'children': []}
    for edge in graph['edges']:
        if edge['child'] == this_ref:
            neighbour, side = edge['parent'], 'parents'
        elif edge['parent'] == this_ref:
            neighbour, side = edge['child'], 'children'
        else:
            continue
        node = nodes.get(neighbour, {})
        out[side].append({
            'pipe_ref': neighbour,
            'label': ref_label(neighbour),
            'href': pipe_href(neighbour),
            'action': node.get('action', ''),
            'reason': node.get('reason', ''),
            'lagging': edge['lagging'],
        })
    return out
