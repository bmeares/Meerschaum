#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
What the console's Overview (`/dash`) shows about an instance, and its tiles:

- **Pipes**: how many are registered (internal `mrsm` pipes excluded) and how many are failing
  (their last sync failed, from the per-pipe sync statuses `mrsm.get_sync_statuses()`);
- **Pipelines**: the saved Pipelines and each one's last run;
- **Jobs**: the jobs by state (running, stalled, failing, paused, stopped; `job.health`);
- **Recent runs**: the Syncing Engine's runs, failures first.

Each `*_summary` function reads with one bulk call (no per-pipe query) and returns plain values;
the `build_*` functions turn them into components. Features other parts of the release add
(sync statuses, job health) are used when present and left out otherwise.
"""

from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import quote, urlencode

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.api import CHECK_UPDATE

html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

#: How often (ms) the Overview refreshes its tiles.
OVERVIEW_REFRESH_MS: int = 30 * 1000

#: How many Pipelines and runs a tile lists.
MAX_PIPELINES: int = 6
MAX_RUNS: int = 8

#: The job states the Jobs tile counts, in order: `(state, label)`.
JOB_STATES = (
    ('running', 'Running'),
    ('stalled', 'Stalled'),
    ('failing', 'Failing'),
    ('paused', 'Paused'),
    ('stopped', 'Stopped'),
)

#: Run statuses that sort first (and are shown in red).
FAILED_STATUSES = frozenset({'failed', 'error', 'upstream_failed'})


def pipe_ref(instance_keys: str, connector_keys: str, metric_key: str, location_key: Optional[str]) -> str:
    """The engine's key of a pipe: `instance|connector_keys|metric_key|location_key`."""
    return '|'.join((str(instance_keys), str(connector_keys), str(metric_key), str(location_key)))


def get_sync_statuses(instance: Any) -> Dict[str, Dict[str, Any]]:
    """
    The last sync of every pipe on `instance` by `pipe_ref` (`mrsm.get_sync_statuses()`), or
    an empty dict when this build does not record them or they cannot be read.
    """
    getter = getattr(mrsm, 'get_sync_statuses', None)
    if getter is None:
        return {}
    try:
        result = getter(instance=instance) or {}
    except Exception:
        return {}
    if isinstance(result, dict):
        return {str(ref): dict(status) for ref, status in result.items() if isinstance(status, dict)}
    if isinstance(result, list):
        return {
            str(status.get('pipe_ref')): dict(status)
            for status in result
            if isinstance(status, dict) and status.get('pipe_ref')
        }
    return {}


def is_failing(status: Optional[Dict[str, Any]]) -> bool:
    """Whether a pipe's last sync failed."""
    if not status:
        return False
    try:
        failures = int(status.get('consecutive_failures') or 0)
    except (TypeError, ValueError):
        failures = 0
    return failures > 0 or status.get('success', None) is False


def pipes_summary(instance: Any, viewer: Any = None) -> Dict[str, Any]:
    """
    `{'total', 'failing', 'statuses'}`: the registered pipes `viewer` (by default the request's)
    may see (without the internal `mrsm` ones), how many are failing (`None` without sync
    statuses) and whether statuses were available.
    """
    from meerschaum.api.dash import access
    viewer = viewer if viewer is not None else access.current_viewer()
    refs = {
        pipe_ref(str(instance), ck, mk, lk)
        for ck, mk, lk, parameters in access.registered_pipes(instance)
        if str(ck) != 'mrsm' and access.can_see_pipe(viewer, parameters)
    }
    has_statuses = hasattr(mrsm, 'get_sync_statuses')
    statuses = get_sync_statuses(instance) if has_statuses and refs else {}
    failing = (
        sum(1 for ref in refs if is_failing(statuses.get(ref, None)))
        if has_statuses
        else None
    )
    return {'total': len(refs), 'failing': failing, 'statuses': has_statuses}


def run_pipeline_name(run: Dict[str, Any], definitions: Dict[str, Dict[str, Any]]) -> Optional[str]:
    """The saved Pipeline a run synced, if any."""
    import json
    record = run.get('run', {}) or {}
    selection = record.get('selection', None)
    try:
        selection = json.loads(selection) if isinstance(selection, str) else (selection or {})
        for snapshot in selection.get('pipeline_definitions', None) or []:
            if isinstance(snapshot, dict) and snapshot.get('name'):
                return str(snapshot['name'])
    except (ValueError, TypeError, AttributeError):
        pass
    from meerschaum.api.dash.engine import run_belongs_to_pipeline
    for name, definition in definitions.items():
        if run_belongs_to_pipeline(run, name, definition):
            return name
    return None


def _list_pipelines(instance: Any, viewer: Any = None) -> Dict[str, Dict[str, Any]]:
    """The saved Pipelines `viewer` (by default the request's) may see."""
    from meerschaum.api.dash.access import visible_pipelines
    return visible_pipelines(instance, viewer=viewer) or {}


def _get_runs(instance: Any, limit: int, viewer: Any = None) -> List[Dict[str, Any]]:
    """The runs with a pipe `viewer` may see (the others anonymized: `engine.get_runs`)."""
    from meerschaum.api.dash.engine import get_runs
    return get_runs(instance, limit=limit, viewer=viewer) or []


def pipelines_summary(instance: Any, viewer: Any = None) -> Dict[str, Any]:
    """
    `{'count', 'pipelines': [{name, status, started, run_id}, ...]}`: the saved Pipelines
    `viewer` (by default the request's) may see, with their last runs, failed and running ones
    first.
    """
    definitions = _list_pipelines(instance, viewer=viewer)
    try:
        runs = _get_runs(instance, 200, viewer=viewer) if definitions else []
    except Exception:
        runs = []
    latest: Dict[str, Dict[str, Any]] = {}
    for run in runs:
        name = run_pipeline_name(run, definitions)
        if name and name not in latest:
            latest[name] = run
    items = []
    for name in sorted(definitions):
        run = latest.get(name, None)
        record = (run or {}).get('run', {}) or {}
        items.append({
            'name': name,
            'status': (run or {}).get('status', None),
            'started': record.get('started', None),
            'run_id': record.get('run_id', None),
        })
    rank = {status: 0 for status in FAILED_STATUSES}
    rank.update({'running': 1})
    items.sort(key=lambda item: (rank.get(item['status'], 2), item['name']))
    return {'count': len(definitions), 'pipelines': items}


def job_state(job: Any) -> str:
    """
    A job's state for the Jobs tile, as the Jobs page's `?status=` filter sees it
    (`meerschaum.api.dash.jobs.job_filter_state`), so a count opens exactly those jobs; else
    `basic_job_state()`.
    """
    states = {state for state, _ in JOB_STATES}
    try:
        from meerschaum.api.dash.jobs import job_filter_state
    except ImportError:
        job_filter_state = None
    if job_filter_state is not None:
        try:
            state = job_filter_state(job)
        except Exception:
            state = None
        if state in states:
            return state
    return basic_job_state(job)


def basic_job_state(job: Any) -> str:
    """
    A job's state from its health (`job.health['state']`) when stalled or failing, else
    `running`, `paused`, `failing` (it stopped with a failure) or `stopped`.
    """
    health = None
    try:
        health = getattr(job, 'health', None)
    except Exception:
        health = None
    state = health.get('state', None) if isinstance(health, dict) else None
    if state in ('stalled', 'failing'):
        return state
    status = str(getattr(job, 'status', '') or '')
    if status == 'running':
        return 'running'
    if status == 'paused':
        return 'paused'
    try:
        result = job.result
    except Exception:
        result = None
    if isinstance(result, (tuple, list)) and result and result[0] is False:
        return 'failing'
    return 'stopped'


def _is_automatic_job(job: Any) -> bool:
    try:
        from meerschaum.api.dash.jobs import is_automatic_job
        return bool(is_automatic_job(job))
    except Exception:
        return str(getattr(job, 'name', '')).startswith('execute-')


def _executor_keys() -> Optional[str]:
    try:
        from meerschaum.jobs import get_executor_keys_from_context
        return get_executor_keys_from_context()
    except Exception:
        return None


def jobs_summary(viewer: Any = None) -> Dict[str, Any]:
    """`{'total', 'counts': {state: n}}` over the jobs the Jobs page lists for the viewer."""
    from meerschaum.jobs import get_jobs
    from meerschaum.api.dash import access
    viewer = viewer if viewer is not None else access.current_viewer()
    jobs = access.filter_jobs(viewer, get_jobs(executor_keys=_executor_keys(), include_hidden=False) or {})
    counts = {state: 0 for state, _ in JOB_STATES}
    total = 0
    for job in jobs.values():
        if _is_automatic_job(job):
            continue
        total += 1
        state = job_state(job)
        counts[state] = counts.get(state, 0) + 1
    return {'total': total, 'counts': counts}


def recent_runs(instance: Any, limit: int = MAX_RUNS, viewer: Any = None) -> List[Dict[str, Any]]:
    """
    The latest run of each Pipeline (or set of pipes) the instance ran recently, failures first,
    then newest first: each `{run_id, status, started, duration_seconds, counts, targets,
    pipeline, runs}` (`runs`: how many of the recent runs synced the same thing, so a job syncing
    every minute takes one row). Only runs with a pipe `viewer` (by default the request's) may
    see, the other pipes anonymized.
    """
    runs = _get_runs(instance, 100, viewer=viewer)
    try:
        definitions = _list_pipelines(instance, viewer=viewer)
    except Exception:
        definitions = {}
    latest: Dict[Any, Dict[str, Any]] = {}
    for run in runs:
        record = run.get('run', {}) or {}
        pipeline = run_pipeline_name(run, definitions)
        targets = list(record.get('targets', None) or [])
        what = ('pipeline', pipeline) if pipeline else ('pipes', tuple(sorted(targets)))
        item = {
            'run_id': record.get('run_id', None),
            'status': run.get('status', None),
            'started': record.get('started', None),
            'duration_seconds': run.get('duration_seconds', None),
            'counts': run.get('counts', None) or {},
            'targets': targets,
            'pipeline': pipeline,
            'runs': 1,
        }
        known = latest.get(what, None)
        if known is None:
            latest[what] = item
            continue
        known['runs'] += 1
        if str(item['started'] or '') > str(known['started'] or ''):
            item['runs'] = known['runs']
            latest[what] = item
    items = list(latest.values())
    items.sort(key=lambda item: str(item['started'] or ''), reverse=True)
    items.sort(key=lambda item: 0 if item['status'] in FAILED_STATUSES else 1)
    return items[:limit]


def parse_timestamp(text: Any) -> Optional[datetime]:
    """An ISO-8601 timestamp as an aware UTC `datetime` (`None` if it cannot be parsed)."""
    if not text:
        return None
    if isinstance(text, datetime):
        moment = text
    else:
        try:
            moment = datetime.fromisoformat(str(text).replace('Z', '+00:00'))
        except ValueError:
            return None
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def time_ago(text: Any, now: Optional[datetime] = None) -> str:
    """`just now`, `5 min ago`, `3 h ago`, `2 d ago`, else the date."""
    moment = parse_timestamp(text)
    if moment is None:
        return ''
    now = now or datetime.now(timezone.utc)
    seconds = (now - moment).total_seconds()
    if seconds < 60:
        return 'just now'
    if seconds < 3600:
        return f"{int(seconds // 60)} min ago"
    if seconds < 86400:
        return f"{int(seconds // 3600)} h ago"
    if seconds < 30 * 86400:
        return f"{int(seconds // 86400)} d ago"
    return moment.strftime('%Y-%m-%d')


def format_duration(seconds: Any) -> str:
    """`0.4 s`, `12 s`, `3 min`, `2 h 5 min`."""
    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        return ''
    if seconds < 10:
        return f"{seconds:.1f} s"
    if seconds < 60:
        return f"{int(seconds)} s"
    if seconds < 3600:
        return f"{int(seconds // 60)} min"
    return f"{int(seconds // 3600)} h {int(seconds % 3600 // 60)} min"


def counts_text(counts: Dict[str, Any]) -> str:
    """`2 succeeded · 1 failed`: the non-zero counts in plain words."""
    parts = []
    for key, value in (counts or {}).items():
        try:
            number = int(value)
        except (TypeError, ValueError):
            continue
        if number:
            parts.append(f"{number} {str(key).replace('_', ' ')}")
    return ' · '.join(parts)


def with_query(path: str, **query: Any) -> str:
    """`path` with the given (non-empty) query parameters."""
    query = {key: value for key, value in query.items() if value}
    return path + (('?' + urlencode(query)) if query else '')


def pipe_label(ref: str) -> Any:
    """`Pipe('ck', 'mk', 'lk')` in monospace, linked to the pipe's page (a hidden pipe: no link)."""
    from meerschaum.api.dash.engine import ref_label, pipe_href, is_hidden_ref
    if is_hidden_ref(ref):
        return html.Span(ref_label(ref), className='overview-hidden-pipe')
    try:
        return dcc.Link(html.Code(ref_label(ref)), href=pipe_href(ref), className='overview-pipe-link')
    except Exception:
        return html.Code(str(ref))


def status_chip(status: Optional[str]) -> Any:
    """A run's status, colored."""
    label = str(status or 'never run').replace('_', ' ')
    return html.Span(label, className=f"overview-status overview-status-{status or 'none'}")


def _stat(value: Any, href: Optional[str], label: str, element_id: str) -> Any:
    text = f"{value:,}" if isinstance(value, int) else str(value)
    number = html.Span(text, className='overview-stat-value', id=element_id)
    if href:
        return dcc.Link(number, href=href, className='overview-stat-link', title=label)
    return number


def build_pipes_body(summary: Dict[str, Any], instance_query: Optional[str] = None) -> List[Any]:
    """The Pipes tile: the total and the failing count."""
    children = [
        _stat(summary['total'], with_query('/dash/pipes', instance=instance_query), 'All pipes',
              'overview-pipes-total'),
    ]
    failing = summary.get('failing', None)
    if failing is not None:
        children.append(dcc.Link(
            f"{failing:,} failing",
            href=with_query('/dash/pipes', status='failing', instance=instance_query),
            id='overview-pipes-failing',
            className='overview-substat' + (' overview-substat-bad' if failing else ' overview-substat-ok'),
        ))
    return children


def build_pipelines_body(summary: Dict[str, Any], instance_query: Optional[str] = None) -> List[Any]:
    """The Pipelines tile: the count and each Pipeline's last run."""
    from meerschaum.api.dash.pipeline_management import pipeline_title
    children = [
        _stat(summary['count'], with_query('/dash/pipelines', instance=instance_query),
              'All Pipelines', 'overview-pipelines-total'),
    ]
    if not summary['pipelines']:
        children.append(dbc.Button(
            'New pipeline', href='/dash/pipelines/new', size='sm', color='secondary',
            id='overview-new-pipeline', class_name='overview-tile-action',
        ))
        return children
    rows = []
    for item in summary['pipelines'][:MAX_PIPELINES]:
        href = with_query(
            '/dash/pipelines/' + quote(item['name'], safe=''),
            run_id=item['run_id'], instance=instance_query,
        )
        rows.append(html.Li([
            dcc.Link(pipeline_title(item['name']), href=href, className='overview-pipeline-link'),
            status_chip(item['status']),
            html.Span(time_ago(item['started']), className='overview-when',
                      title=str(item['started'] or '')),
        ], className='overview-row'))
    children.append(html.Ul(rows, className='overview-list', id='overview-pipelines-list'))
    more = len(summary['pipelines']) - MAX_PIPELINES
    if more > 0:
        children.append(dcc.Link(
            f"{more} more", href=with_query('/dash/pipelines', instance=instance_query),
            className='overview-more',
        ))
    return children


def build_jobs_body(summary: Dict[str, Any]) -> List[Any]:
    """The Jobs tile: the jobs by state, each linked to the Jobs page filtered by it."""
    rows = []
    for state, label in JOB_STATES:
        count = summary['counts'].get(state, 0)
        if state == 'paused' and not count:
            continue
        rows.append(html.Li(
            dcc.Link(
                [
                    html.Span(className=f'overview-dot overview-dot-{state}', **{'aria-hidden': 'true'}),
                    html.Span(label, className='overview-job-state'),
                    html.Span(f"{count:,}", className='overview-job-count'),
                ],
                href=with_query('/dash/jobs', status=state),
                className='overview-job-link',
                id=f'overview-jobs-{state}',
            ),
            className='overview-row',
        ))
    return [
        _stat(summary['total'], '/dash/jobs', 'All jobs', 'overview-jobs-total'),
        html.Ul(rows, className='overview-list overview-job-states'),
    ]


def build_runs_body(runs: List[Dict[str, Any]], instance_query: Optional[str] = None) -> List[Any]:
    """Recent runs: status, what ran (a Pipeline or its Pipes), when, how long and what changed."""
    if not runs:
        return [html.P('No runs yet', className='overview-none')]
    from meerschaum.api.dash.pipeline_management import pipeline_title
    rows = []
    for run in runs:
        if run['pipeline']:
            what = dcc.Link(
                pipeline_title(run['pipeline']),
                href=with_query(
                    '/dash/pipelines/' + quote(run['pipeline'], safe=''),
                    run_id=run['run_id'], instance=instance_query,
                ),
                className='overview-pipeline-link',
            )
        elif len(run['targets']) == 1:
            what = pipe_label(run['targets'][0])
        else:
            what = html.Details([
                html.Summary(f"{len(run['targets'])} pipes"),
                html.Ul([html.Li(pipe_label(ref)) for ref in run['targets']], className='overview-run-targets'),
            ], className='overview-run-pipes')
        repeats = int(run.get('runs', 1) or 1)
        rows.append(html.Li([
            status_chip(run['status']),
            html.Div(
                [what] + ([html.Span(
                    f"×{repeats}", className='overview-repeat', title=f"{repeats} recent runs",
                )] if repeats > 1 else []),
                className='overview-run-what',
                title=str(run['run_id'] or ''),
            ),
            html.Span(time_ago(run['started']), className='overview-when', title=str(run['started'] or '')),
            html.Span(format_duration(run['duration_seconds']), className='overview-duration'),
            html.Span(counts_text(run['counts']), className='overview-counts'),
        ], className='overview-row overview-run' + (
            ' overview-run-failed' if run['status'] in FAILED_STATUSES else ''
        )))
    return [html.Ul(rows, className='overview-list overview-runs', id='overview-runs-list')]


def build_error(message: str) -> List[Any]:
    """A tile that could not be read (credentials in the message masked)."""
    from meerschaum.utils.misc import remove_ansi
    from meerschaum.api.dash.connector_listing import mask_text
    return [html.Div(mask_text(remove_ansi(str(message))), className='overview-error', role='alert')]
