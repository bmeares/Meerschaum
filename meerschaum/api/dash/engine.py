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
    if pipe_ref.count('|') < 3:
        ### A canvas placeholder (`__upstream__`), not a Pipe.
        return pipe_ref
    instance, connector, metric, location = split_ref(pipe_ref)
    return str(mrsm.Pipe(connector, metric, location, instance=instance))


def action_label(action: str) -> str:
    """Describe the next sync without exposing engine bookkeeping."""
    return {
        'root': 'Fetch source data', 'incremental': 'New data to sync',
        'backfill': 'Reprocess changed data', 'skip': 'Up to date',
        'rebuild': 'Rebuild required', 'rebuild_pending': 'Rebuild needs approval',
        'observe': 'Listens for new data', 'upstream_failed': 'Blocked: a parent failed',
    }.get(action, action.replace('_', ' ').capitalize())


#: A run's node counts in plain words (`2 synced, 1 up to date`), by node status.
STATUS_WORDS: Dict[str, str] = {
    'succeeded': 'synced',
    'skipped': 'up to date',
    'observed': 'listening for new data',
    'failed': 'failed',
    'upstream_failed': 'blocked by a failed parent',
    'rebuild_pending': 'waiting for rebuild approval',
    'running': 'running',
    'pending': 'not started',
}


def counts_label(counts: Optional[Dict[str, int]]) -> str:
    """A run's node counts in plain words, without the zeros (`2 synced, 1 up to date`)."""
    parts = [
        f"{value} {STATUS_WORDS.get(key, key.replace('_', ' '))}"
        for key, value in (counts or {}).items() if value
    ]
    return ', '.join(parts) or 'Nothing ran'


def bounds_label(bounds: Optional[Dict[str, Any]], backtrack: Optional[str] = None) -> str:
    """
    A sync window in words (`last sync − 1 day → latest data`, `2026-10-12 09:00 → latest
    data`), from a planned node's `bounds` (`begin` is `''` for "from the last sync, minus the
    backtrack window" and `None` for no lower bound; `end` is `None` for no upper bound).
    """
    if not bounds:
        return '—'
    begin, end = bounds.get('begin', None), bounds.get('end', None)
    if begin is None and end is None:
        return 'all data'
    if begin == '':
        begin_text = f"last sync − {backtrack}" if backtrack else 'last sync'
    elif begin is None:
        begin_text = 'the beginning'
    else:
        begin_text = bound_label(begin)
    end_text = 'latest data' if end is None else bound_label(end)
    return f"{begin_text} → {end_text}"


def bound_label(bound: Any) -> str:
    """One bound for people: `2026-10-12 09:00` (seconds only when set), or an integer."""
    if isinstance(bound, dict):
        values = list(bound.values())
        bound = values[0] if len(values) == 1 else bound
    if isinstance(bound, (int, float)) and not isinstance(bound, bool):
        return str(bound)
    parsed = _parse_ts(str(bound))
    if parsed is None:
        return str(bound)
    text = parsed.strftime('%Y-%m-%d %H:%M:%S' if parsed.second or parsed.microsecond else '%Y-%m-%d %H:%M')
    return text


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


#: The cookie holding the console's global instance (the instance selector), shared by pages.
INSTANCE_COOKIE: str = 'mrsm-instance'


def global_instance_keys() -> Optional[str]:
    """
    The console's global instance, as the instance selector last set it (`INSTANCE_COOKIE`),
    or `None` outside a request or before it was ever chosen.
    """
    keys = None
    try:
        import dash
        keys = (dash.callback_context.cookies or {}).get(INSTANCE_COOKIE)
    except Exception:
        try:
            import flask
            keys = flask.request.cookies.get(INSTANCE_COOKIE) if flask.has_request_context() else None
        except Exception:
            return None
    if not keys:
        return None
    from urllib.parse import unquote
    keys = unquote(keys)
    try:
        from meerschaum.connectors.parse import parse_instance_keys
        parse_instance_keys(keys)
    except Exception:
        return None
    return keys


def resolve_instance(instance_keys: Optional[str] = None) -> Tuple[Any, Optional[str]]:
    """
    The instance connector to use and, when one was asked for but refused, its keys.

    `instance_keys` (a `?instance=` deep link), else the console's global instance, must pass the
    API's instance permissions (`allow_multiple_instances`, `allowed_instance_keys`) and be valid;
    otherwise the API's own instance is used and the refused keys are returned with it.
    """
    from meerschaum.api import get_api_connector
    requested = instance_keys or global_instance_keys()
    if requested:
        try:
            return get_api_connector(str(requested)), None
        except Exception:
            return get_api_connector(), str(requested)
    return get_api_connector(), None


def get_instance(instance_keys: Optional[str] = None, strict: bool = False):
    """
    The instance connector: `instance_keys` (a `?instance=` deep link), else the console's global
    instance, else the API's. A refused instance is never used: the API's instance is returned
    instead (`instance_problem` says so), or with `strict` (the instance of a member or a run,
    where another instance would be the wrong data) a `PermissionError` is raised.
    """
    instance, refused = resolve_instance(instance_keys)
    if refused and strict:
        raise PermissionError(f"Instance '{refused}' is not available to this API.")
    return instance


def instance_problem(instance_keys: Optional[str] = None) -> Optional[str]:
    """Why the requested (or global) instance is not the one shown, or `None`."""
    instance, refused = resolve_instance(instance_keys)
    if not refused:
        return None
    return f"Instance '{refused}' is not available; showing {instance} instead."


def _call(name: str, request: Dict[str, Any]) -> Any:
    from meerschaum.core.Pipeline._engine import call
    return call(name, request)


def _request(instance: Any, **kwargs: Any) -> Dict[str, Any]:
    from meerschaum.core.Pipeline._engine import build_request
    return build_request(instance, **kwargs)


def _instance_pipes(instance: Any) -> List[mrsm.Pipe]:
    from meerschaum.utils.pipes import flatten_pipes_dict
    ### `get_pipes()` leaves out Meerschaum's internal `mrsm` pipes (saved Pipelines' registry).
    pipes_dict = mrsm.get_pipes(instance=instance, as_list=False)
    return flatten_pipes_dict(pipes_dict)


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
            'bounds_text': (planned.raw.get('bounds_text') or bounds_label(
                planned.bounds, planned.raw.get('backtrack'))) if planned.bounds else '',
            'backtrack': planned.raw.get('backtrack'),
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


def run_belongs_to_pipeline(run: Dict[str, Any], name: str, definition=None) -> bool:
    """Match the complete name before the engine's timestamp and collision suffix."""
    import re
    record = run.get('run', {})
    selection = record.get('selection')
    if selection:
        import json
        try:
            selection = json.loads(selection) if isinstance(selection, str) else selection
            definitions = selection.get('pipeline_definitions') or []
            if definitions:
                return any(snapshot.get('name') == name
                           and (not definition or not definition.get('id')
                                or snapshot.get('id') == definition['id'])
                           and (not definition or not definition.get('registry')
                                or snapshot.get('registry') == definition['registry'])
                           for snapshot in definitions)
        except (ValueError, TypeError, AttributeError):
            pass
    run_id = run.get('run', {}).get('run_id', '')
    return bool(re.fullmatch(re.escape(name) + r'-\d{4}-\d\d-\d\dT\d\d-\d\d-\d\d\.\d{6}Z-[a-f0-9]{8}', run_id))


def stopped_execution_times(instance):
    """Correlate unfinished engine records with stopped local execution workers.

    A killed worker cannot persist run_finished. Its retained Job identity provides the
    actual stop time, including after a page reload. Never infer completion from a missing Job.
    """
    from meerschaum.jobs import get_jobs
    ended = {}
    for job in get_jobs(executor_keys='local', include_hidden=True, combine_local_and_systemd=False).values():
        try:
            metadata = job.daemon.properties.get('mrsm_execution') or {}
            run_id = metadata.get('run_id')
            args = job.sysargs
            registry = args[args.index('-i') + 1] if '-i' in args else str(get_instance())
            if run_id and registry == str(instance) and job.status == 'stopped' and job.ended:
                finished = job.ended
                ended[run_id] = finished.replace(tzinfo=finished.tzinfo or timezone.utc).isoformat()
        except Exception:
            continue
    return ended


def reconcile_stopped_run(summary, ended):
    """Use a known worker termination time without changing successful or failed runs."""
    if summary.get('status') != 'running' or not ended:
        return False
    summary['status'] = 'stopped'
    summary['run']['finished'] = ended
    start, finish = _parse_ts(summary['run'].get('started')), _parse_ts(ended)
    if start and finish:
        start = start.replace(tzinfo=start.tzinfo or timezone.utc)
        finish = finish.replace(tzinfo=finish.tzinfo or timezone.utc)
        summary['duration_seconds'] = max(0, (finish-start).total_seconds())
    return True


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
    runs = _call('engine_runs', request)
    if any(run.get('status') == 'running' for run in runs):
        ended = stopped_execution_times(instance)
        for run in runs:
            reconcile_stopped_run(run, ended.get(run['run']['run_id']))
    return runs


def get_run_detail(instance: Any, run_id: str) -> Optional[Dict[str, Any]]:
    """
    One run with its nodes and the planned-versus-actual bars (`None` for an unknown run).
    """
    instance = instance if instance is not None else get_instance()
    request = _request(instance)
    request['run_id'] = run_id
    detail = _call('engine_run_detail', request)
    if detail and (detail.get('summary') or {}).get('status') == 'running':
        ended = stopped_execution_times(instance).get(run_id)
        if reconcile_stopped_run(detail['summary'], ended):
            for bar in detail.get('bars', []):
                if bar.get('status') in ('running', 'pending'):
                    bar['status'] = 'stopped'
                    if bar.get('actual_start') and not bar.get('actual_end'):
                        bar['actual_end'] = ended
    return detail


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


def run_events_stream_path(
    run_id: str,
    instance: Any = None,
    wait: Optional[float] = None,
) -> str:
    """
    The same-origin path of a run's live event stream on the Rust server
    (`GET /engine/runs/{run_id}/events/stream`, Server-Sent Events of the typed engine events).
    `instance` names the instance that keeps the run's history; `wait` (seconds) lets the stream
    wait for a run that has not started yet (a job that was just launched).
    """
    from urllib.parse import quote, urlencode
    params = {}
    if instance is not None:
        params['instance_keys'] = str(instance)
    if wait:
        params['wait'] = str(wait)
    return (
        '/engine/runs/' + quote(str(run_id), safe='') + '/events/stream'
        + ('?' + urlencode(params) if params else '')
    )


def _parse_ts(text: Optional[str]) -> Optional[datetime]:
    if not text:
        return None
    try:
        return datetime.fromisoformat(str(text).replace('Z', '+00:00'))
    except ValueError:
        return None


def gantt_figure(detail: Dict[str, Any]) -> Dict[str, Any]:
    """
    A Plotly figure of observed execution intervals, including live running nodes.
    """
    traces = []
    for name, color, planned in (('Actual', '#3486bd', False),):
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
                'stopped': '#9aa5b1', 'cancelled': '#9aa5b1',
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
            'dragmode': False,
            'xaxis': {'type': 'date', 'fixedrange': True},
            'margin': {'l': 30, 'r': 30, 't': 60, 'b': 60},
            'barmode': 'group',
            'uirevision': run_id,
            'yaxis': {'fixedrange': True, 'autorange': 'reversed', 'automargin': True,
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
    One line of a run's output, in plain words: `<time> <what happened>`
    (`12:00:01.250 Synced Pipe('sql:main', 'daily'): 3 inserted`).
    """
    payload = row.get('payload') or {}
    event = row.get('event', '')
    ts = str(row.get('ts', ''))[11:23]
    pipe_ref = row.get('pipe_ref')
    pipe = ref_label(pipe_ref) if pipe_ref else ''

    def rows_text(source: Dict[str, Any]) -> str:
        parts = [f"{source[key]:,} {key}" for key in ('inserted', 'updated', 'upserted')
                 if source.get(key)]
        return ', '.join(parts) or 'no new rows'

    def chunk_text() -> str:
        text = f"chunk {payload.get('chunk', 0) + 1 if isinstance(payload.get('chunk'), int) else ''}"
        if payload.get('total') is not None:
            text += f" of {payload['total']}"
        return text

    if event == 'run_started':
        nodes = payload.get('nodes', 0)
        text = f"Run started: {nodes} pipe{'s' if nodes != 1 else ''}"
    elif event == 'run_finished':
        text = ('Run finished: ' if payload.get('success', True) else 'Run failed: ') + str(
            payload.get('message', '')).split('\n', 1)[0]
    elif event == 'node_planned':
        text = f"{pipe}: {action_label(payload.get('action', '')).lower()}"
    elif event == 'node_started':
        attempt = payload.get('attempt', 1) or 1
        text = f"Syncing {pipe}" + (f" (attempt {attempt})" if attempt > 1 else '')
    elif event == 'chunk_started':
        text = f"{pipe}: {chunk_text()} started"
        if payload.get('rows') is not None:
            text += f" ({payload['rows']:,} rows)"
    elif event == 'chunk_finished':
        done = 'failed' if payload.get('success') is False else 'done'
        text = f"{pipe}: {chunk_text()} {done}"
        if payload.get('success') is not False:
            text += f" ({rows_text(payload)})"
    elif event == 'node_finished':
        result = payload.get('result') or {}
        if result.get('success', True):
            text = f"Synced {pipe}: {rows_text(result)}"
        else:
            text = f"{pipe} failed: {str(result.get('message', '')).split(chr(10), 1)[0]}"
    elif event == 'node_failed':
        error = str(payload.get('error', '')).split('\n', 1)[0]
        text = f"{pipe} failed: {error}" + ('; trying again' if payload.get('will_retry') else '')
    elif event == 'node_skipped':
        text = f"{pipe}: {payload.get('reason', '')}"
    elif event == 'node_rebuild_pending':
        text = f"{pipe} waits for rebuild approval: {payload.get('reason', '')}"
    elif event == 'watermark_advanced':
        to = payload.get('to')
        text = f"{pipe}: latest data now {bound_label(to)}" if to is not None else f"{pipe}: new data"
    elif event == 'stream_flushed':
        rows = payload.get('rows', 0) or 0
        text = f"{pipe}: wrote {rows:,} new row{'s' if rows != 1 else ''}"
        if payload.get('success') is False:
            text = f"{pipe}: could not write {rows:,} new row{'s' if rows != 1 else ''}"
    elif event == 'stream_restarted':
        text = f"{pipe}: listening again after an error: {payload.get('error', '')}"
    elif event == 'children_triggered':
        nodes = payload.get('nodes') or []
        count = len(nodes) if isinstance(nodes, list) else nodes
        text = f"New data: syncing {count} downstream pipe{'s' if count != 1 else ''}"
    else:
        text = ' '.join(part for part in (event.replace('_', ' ').capitalize(), pipe) if part)
    return f"{ts} {text}".strip()


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
