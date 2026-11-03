#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
What the console's Overview (`/dash`) shows about an instance, and its tiles:

- **Pipes**: how many are registered (internal `mrsm` pipes excluded), how many synced, are
  failing or have no recorded sync (the per-pipe sync statuses, `mrsm.get_sync_statuses()`), the
  latest syncs (failures first) and the connectors the pipes read from;
- **Pipelines**: the saved Pipelines by their last run's status, their runs over the last 24 hours
  hour by hour, and the last success and failure;
- **Jobs**: the jobs by state (running, stalled, failing, paused, stopped; `job.health`);
- **Recent runs**: the Syncing Engine's runs, failures first.

Each `*_summary` function reads with one bulk call (no per-pipe query) and returns plain values;
the `build_*` functions turn them into components. Features other parts of the release add
(sync statuses, job health) are used when present and left out otherwise.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlencode

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.api import CHECK_UPDATE

html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

#: How often (ms) the Overview refreshes its tiles.
OVERVIEW_REFRESH_MS: int = 30 * 1000

#: How many runs Recent runs lists, and how many pipes and connectors the Pipes tile lists.
MAX_RUNS: int = 8
MAX_LATEST_SYNCS: int = 5
MAX_CONNECTORS: int = 4

#: The pipe states the Pipes tile counts, in order: `(state, label)`. `ok` and `failing` are the
#: Pipes page's `?status=` values; `unrecorded` is a pipe without a recorded sync.
PIPE_STATES = (
    ('ok', 'Synced'),
    ('failing', 'Failing'),
    ('unrecorded', 'No sync recorded'),
)

#: The states the Pipelines tile counts (each Pipeline's last run), in order: `(state, label)`.
PIPELINE_STATES = (
    ('running', 'Running'),
    ('succeeded', 'Succeeded'),
    ('failed', 'Failed'),
    ('stopped', 'Stopped'),
    ('none', 'Not run'),
)

#: How many hours the Pipelines tile's run history covers, and how many runs it reads at most.
HISTORY_HOURS: int = 24
HISTORY_LIMIT: int = 200

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


def pipes_summary(instance: Any, viewer: Any = None, now: Optional[datetime] = None) -> Dict[str, Any]:
    """
    What the Pipes tile shows about the registered pipes `viewer` (by default the request's) may
    see, without the internal `mrsm` ones, from one listing and one read of the sync statuses:

    - `total`: how many;
    - `statuses`: whether this build records sync statuses (else the counts below are `None`);
    - `ok`, `failing`, `unrecorded`: how many synced, are failing or have no recorded sync;
    - `last_sync`: when the latest recorded sync of any of them ended (ISO-8601), or `None`;
    - `synced_24h`: how many had a sync end in the last 24 hours;
    - `latest`: up to `MAX_LATEST_SYNCS` pipes by their latest sync, failing ones first
      (`{ref, state, when, failures}`);
    - `connectors`: `[(connector_keys, count)]`, most pipes first.
    """
    from meerschaum.api.dash import access
    viewer = viewer if viewer is not None else access.current_viewer()
    refs = {}
    for ck, mk, lk, parameters in access.registered_pipes(instance):
        if str(ck) != 'mrsm' and access.can_see_pipe(viewer, parameters):
            refs[pipe_ref(str(instance), ck, mk, lk)] = str(ck)
    connectors: Dict[str, int] = {}
    for ck in refs.values():
        connectors[ck] = connectors.get(ck, 0) + 1
    summary: Dict[str, Any] = {
        'total': len(refs),
        'statuses': hasattr(mrsm, 'get_sync_statuses'),
        'ok': None,
        'failing': None,
        'unrecorded': None,
        'last_sync': None,
        'synced_24h': None,
        'latest': [],
        'connectors': sorted(connectors.items(), key=lambda item: (-item[1], item[0])),
    }
    if not summary['statuses']:
        return summary
    statuses = get_sync_statuses(instance) if refs else {}
    now = now or datetime.now(timezone.utc)
    counts = {state: 0 for state, _ in PIPE_STATES}
    synced_24h = 0
    last_sync = None
    latest = []
    for ref in refs:
        status = statuses.get(ref, None)
        if not status:
            counts['unrecorded'] += 1
            continue
        failing = is_failing(status)
        counts['failing' if failing else 'ok'] += 1
        attempt = parse_timestamp(status.get('last_attempt', None))
        if attempt is not None:
            if last_sync is None or attempt > last_sync:
                last_sync = attempt
            if (now - attempt).total_seconds() <= 86400:
                synced_24h += 1
        when = (status.get('last_failure', None) if failing else None) or status.get('last_attempt', None)
        try:
            failures = int(status.get('consecutive_failures') or (1 if failing else 0))
        except (TypeError, ValueError):
            failures = 1 if failing else 0
        latest.append({
            'ref': ref,
            'state': 'failing' if failing else 'ok',
            'when': when,
            'failures': failures,
        })
    latest.sort(key=lambda item: str(item['when'] or ''), reverse=True)
    latest.sort(key=lambda item: 0 if item['state'] == 'failing' else 1)
    summary.update(counts)
    summary.update({
        'last_sync': last_sync.isoformat() if last_sync is not None else None,
        'synced_24h': synced_24h,
        'latest': latest[:MAX_LATEST_SYNCS],
    })
    return summary


def run_pipeline_name(run: Dict[str, Any], definitions: Dict[str, Dict[str, Any]]) -> Optional[str]:
    """The saved Pipeline a run synced, if any."""
    import json
    record = run.get('run', {}) or {}
    selection = record.get('selection', None)
    try:
        selection = json.loads(selection) if isinstance(selection, str) else (selection or {})
        for snapshot in selection.get('pipeline_definitions', None) or []:
            if isinstance(snapshot, dict) and str(snapshot.get('name')) in definitions:
                name = str(snapshot['name'])
                from meerschaum.api.dash.engine import run_belongs_to_pipeline
                if run_belongs_to_pipeline(run, name, definitions[name]):
                    return name
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
    return visible_pipelines(instance, viewer=viewer, include_members=False) or {}


def _get_runs(instance: Any, limit: int, viewer: Any = None) -> List[Dict[str, Any]]:
    """The runs with a pipe `viewer` may see (the others anonymized: `engine.get_runs`)."""
    from meerschaum.api.dash.engine import get_runs
    return get_runs(instance, limit=limit, viewer=viewer) or []


def run_state(status: Optional[str]) -> str:
    """A run's status as the Pipelines tile counts it (`PIPELINE_STATES`)."""
    if status in FAILED_STATUSES:
        return 'failed'
    if status in ('succeeded', 'running'):
        return status
    return 'stopped' if status else 'none'


def pipelines_summary(
    instance: Any,
    viewer: Any = None,
    definitions=None,
    runs=None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    What the Pipelines tile shows about the saved Pipelines `viewer` (by default the request's)
    may see, from the registry and one bounded read of the run history:

    - `count`: how many;
    - `states`: how many by their last run (`PIPELINE_STATES`; `none`: no run in the history read);
    - `hours`: their runs over the last `HISTORY_HOURS` hours, oldest hour first
      (`{start, succeeded, failed, running, stopped}`; `unread` when the history read is shorter);
    - `runs_24h`, `failed_24h`: how many of those runs there were, and how many failed;
    - `last_success`, `last_failure`: the newest succeeded and failed runs
      (`{name, started, run_id}`), or `None`.
    """
    definitions = _list_pipelines(instance, viewer=viewer) if definitions is None else definitions
    try:
        runs = (_get_runs(instance, HISTORY_LIMIT, viewer=viewer) if definitions else []) if runs is None else runs
    except Exception:
        runs = []
    now = now or datetime.now(timezone.utc)
    window_end = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    window_start = window_end - timedelta(hours=HISTORY_HOURS)
    hours = [
        {
            'start': (window_start + timedelta(hours=i)).isoformat(),
            'succeeded': 0, 'failed': 0, 'running': 0, 'stopped': 0, 'unread': False,
        }
        for i in range(HISTORY_HOURS)
    ]
    latest: Dict[str, Dict[str, Any]] = {}
    last_success = last_failure = None
    runs_24h = failed_24h = 0
    oldest = None
    for run in runs:
        record = run.get('run', {}) or {}
        started = parse_timestamp(record.get('started', None))
        if started is not None and (oldest is None or started < oldest):
            oldest = started
        name = run_pipeline_name(run, definitions)
        if not name:
            continue
        status = run.get('status', None)
        state = run_state(status)
        item = {'name': name, 'started': record.get('started', None), 'run_id': record.get('run_id', None)}
        if name not in latest:
            latest[name] = {'status': status, **item}
        if state == 'succeeded' and last_success is None:
            last_success = item
        if state == 'failed' and last_failure is None:
            last_failure = item
        if started is None or not (window_start <= started < window_end):
            continue
        hour = hours[int((started - window_start).total_seconds() // 3600)]
        hour[state if state != 'none' else 'stopped'] += 1
        runs_24h += 1
        failed_24h += int(state == 'failed')
    ### A full history read that ends inside the window says nothing about the hours before it.
    if oldest is not None and len(runs) >= HISTORY_LIMIT:
        for hour in hours:
            if parse_timestamp(hour['start']) + timedelta(hours=1) <= oldest:
                hour['unread'] = True
    states = {state: 0 for state, _ in PIPELINE_STATES}
    for name in definitions:
        states[run_state((latest.get(name, None) or {}).get('status', None))] += 1
    return {
        'count': len(definitions),
        'states': states,
        'hours': hours,
        'runs_24h': runs_24h,
        'failed_24h': failed_24h,
        'last_success': last_success,
        'last_failure': last_failure,
    }


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


def is_scheduled_job(job: Any) -> bool:
    """Whether a job runs on a schedule (`-s` / `--schedule`)."""
    try:
        sysargs = [str(arg) for arg in (getattr(job, 'sysargs', None) or [])]
    except Exception:
        return False
    return any(arg.split('=', 1)[0] in ('-s', '--schedule') for arg in sysargs)


def jobs_summary(viewer: Any = None) -> Dict[str, Any]:
    """
    `{'total', 'counts': {state: n}, 'scheduled'}` over the jobs the Jobs page lists for the
    viewer (`scheduled`: how many run on a schedule).
    """
    from meerschaum.api.dash import access
    viewer = viewer if viewer is not None else access.current_viewer()
    executor_keys = _executor_keys()
    if executor_keys in (None, 'local', 'systemd'):
        ### The Jobs page's own listing: one batched `systemctl show` instead of a few per job.
        from meerschaum.api.dash.jobs import get_job_snapshots
        listed = get_job_snapshots()
    else:
        from meerschaum.jobs import get_jobs
        listed = get_jobs(executor_keys=executor_keys, include_hidden=False)
    jobs = access.filter_jobs(viewer, listed or {})
    counts = {state: 0 for state, _ in JOB_STATES}
    total = scheduled = 0
    for job in jobs.values():
        if _is_automatic_job(job):
            continue
        total += 1
        scheduled += int(is_scheduled_job(job))
        state = job_state(job)
        counts[state] = counts.get(state, 0) + 1
    return {'total': total, 'counts': counts, 'scheduled': scheduled}


def recent_runs(instance: Any, limit: int = MAX_RUNS, viewer: Any = None, definitions=None, runs=None) -> List[Dict[str, Any]]:
    """
    The latest run of each Pipeline (or set of pipes) the instance ran recently, failures first,
    then newest first: each `{run_id, status, started, duration_seconds, counts, targets,
    pipeline, runs}` (`runs`: how many of the recent runs synced the same thing, so a job syncing
    every minute takes one row). Only runs with a pipe `viewer` (by default the request's) may
    see, the other pipes anonymized.
    """
    runs = _get_runs(instance, 100, viewer=viewer) if runs is None else runs
    try:
        definitions = _list_pipelines(instance, viewer=viewer) if definitions is None else definitions
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


def engine_summary(instance: Any, viewer: Any = None) -> Dict[str, Any]:
    """One fresh registry/history snapshot shared by the two Overview engine tiles.

    This data stays in the request; no viewer-filtered result or credentials are cached.
    Member access is checked by the run reader, rather than walking a graph for each saved name.
    """
    definitions = _list_pipelines(instance, viewer=viewer)
    runs = _get_runs(instance, HISTORY_LIMIT if definitions else 100, viewer=viewer)
    return {
        'pipelines': pipelines_summary(instance, viewer=viewer, definitions=definitions, runs=runs),
        'runs': recent_runs(instance, viewer=viewer, definitions=definitions, runs=runs[:100]),
    }


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
        label = ref_label(ref)
        return dcc.Link(html.Code(label), href=pipe_href(ref), className='overview-pipe-link', title=label)
    except Exception:
        return html.Code(str(ref))


def status_chip(status: Optional[str]) -> Any:
    """A run's status, colored."""
    label = str(status or 'no recent run').replace('_', ' ')
    return html.Span(label, className=f"overview-status overview-status-{status or 'none'}")


def _stat(value: Any, href: Optional[str], label: str, element_id: str) -> Any:
    text = f"{value:,}" if isinstance(value, int) else str(value)
    number = html.Span(text, className='overview-stat-value', id=element_id)
    if href:
        return dcc.Link(number, href=href, className='overview-stat-link', title=label)
    return number


def _headline(stat: Any, aside: Any = None) -> Any:
    """The tile's number, with a short fact beside it."""
    return html.Div([stat] + ([aside] if aside else []), className='overview-headline')


def _aside(text: str, element_id: str, title: Optional[str] = None) -> Any:
    return html.Span(text, className='overview-aside', id=element_id, title=title or text)


def _bar(segments: List[Any], element_id: str) -> Any:
    """
    A bar split by state (`[(state, label, count)]`), each part as wide as its share; its label
    reads the counts.
    """
    shown = [(state, label, count) for state, label, count in segments if count]
    text = ', '.join(f"{count:,} {label.lower()}" for _, label, count in shown) or 'None'
    return html.Div(
        [
            html.Span(
                className=f'overview-bar-part overview-bar-{state}',
                style={'flexGrow': count},
                title=f"{label}: {count:,}",
            )
            for state, label, count in shown
        ],
        className='overview-bar',
        id=element_id,
        role='img',
        **{'aria-label': text},
    )


def _state_rows(rows: List[Any], list_id: Optional[str] = None) -> Any:
    """
    The states of a tile (`[(state, label, count, href, element_id)]`): a dot, the label and the
    count, linked to the page filtered by it (`href`; a row without one is not a link).
    """
    items = []
    for state, label, count, href, element_id in rows:
        content = [
            html.Span(className=f'overview-dot overview-dot-{state}', **{'aria-hidden': 'true'}),
            html.Span(label, className='overview-job-state'),
            html.Span(f"{count:,}", className='overview-job-count'),
        ]
        items.append(html.Li(
            dcc.Link(content, href=href, className='overview-job-link', id=element_id)
            if href else
            html.Div(content, className='overview-job-link overview-state-plain', id=element_id),
            className='overview-row',
        ))
    return html.Ul(items, className='overview-list overview-job-states', **({'id': list_id} if list_id else {}))


def build_pipes_body(summary: Dict[str, Any], instance_query: Optional[str] = None) -> List[Any]:
    """
    The Pipes tile: the total, the pipes by sync status (each linked to the Pipes page filtered
    by it) and the connectors with the most pipes, beside the latest syncs (failures first).
    """
    has_statuses = summary.get('ok', None) is not None
    last_sync = summary.get('last_sync', None)
    overview_column = [
        _headline(
            _stat(summary['total'], with_query('/dash/pipes', instance=instance_query), 'All pipes',
                  'overview-pipes-total'),
            _aside(f"last sync {time_ago(last_sync)}", 'overview-pipes-last-sync', str(last_sync))
            if last_sync else None,
        ),
    ]
    if has_statuses:
        hrefs = {
            'ok': with_query('/dash/pipes', status='ok', instance=instance_query),
            'failing': with_query('/dash/pipes', status='failing', instance=instance_query),
            'unrecorded': None,
        }
        overview_column += [
            _bar([(state, label, summary.get(state, 0) or 0) for state, label in PIPE_STATES],
                 'overview-pipes-bar'),
            _state_rows([
                (state, label, summary.get(state, 0) or 0, hrefs[state], f'overview-pipes-{state}')
                for state, label in PIPE_STATES
            ]),
        ]
    detail_column = []
    if summary.get('latest'):
        rows = []
        for item in summary['latest']:
            failing = item['state'] == 'failing'
            failures = int(item.get('failures') or 0)
            rows.append(html.Li([
                html.Span(className=f"overview-dot overview-dot-{item['state']}", **{'aria-hidden': 'true'}),
                html.Div(pipe_label(item['ref']), className='overview-latest-pipe'),
                html.Span(f"×{failures}", className='overview-repeat overview-failures',
                          title=f"{failures} failed syncs in a row") if failing and failures > 1 else None,
                html.Span(time_ago(item['when']), className='overview-when', title=str(item['when'] or '')),
            ], className='overview-row overview-latest' + (' overview-latest-failing' if failing else '')))
        detail_column.append(html.Section([
            html.H3('Latest syncs', className='overview-subheading'),
            html.Ul(rows, className='overview-list', id='overview-pipes-latest'),
        ], className='overview-pipes-section'))
    connectors = list(summary.get('connectors', None) or [])
    if connectors:
        chips = [
            dcc.Link(
                [html.Code(str(ck)), html.Span(f"{count:,}", className='overview-chip-count')],
                href=with_query('/dash/pipes', connector_keys=ck, instance=instance_query),
                className='overview-chip',
                title=f"{count:,} pipes from {ck}",
            )
            for ck, count in connectors[:MAX_CONNECTORS]
        ]
        more = len(connectors) - MAX_CONNECTORS
        if more > 0:
            chips.append(dcc.Link(
                f"{more} more", href=with_query('/dash/pipes', instance=instance_query),
                className='overview-chip overview-chip-more',
            ))
        overview_column.append(html.Section([
            html.H3('Connectors', className='overview-subheading'),
            html.Div(chips, className='overview-chips', id='overview-pipes-connectors'),
        ], className='overview-pipes-section overview-pipes-connectors'))
    return [html.Div(
        [html.Div(overview_column, className='overview-pipes-overview')]
        + ([html.Div(detail_column, className='overview-pipes-detail')] if detail_column else []),
        className='overview-pipes-layout',
    )]


def _hour_label(start: Any) -> str:
    moment = parse_timestamp(start)
    if moment is None:
        return ''
    return moment.strftime('%H:00') + ' UTC'


def build_history(summary: Dict[str, Any]) -> Any:
    """The Pipelines' runs over the last 24 hours, hour by hour, stacked by status."""
    hours = summary.get('hours', None) or []
    ### Bottom to top: the bars stack upward (`column-reverse`), failures on top.
    states = ('succeeded', 'running', 'stopped', 'failed')
    peak = max([sum(hour.get(state, 0) for state in states) for hour in hours] + [1])
    columns = []
    for hour in hours:
        total = sum(hour.get(state, 0) for state in states)
        if hour.get('unread'):
            title = f"{_hour_label(hour['start'])}: not read"
        else:
            title = f"{_hour_label(hour['start'])}: " + (
                ', '.join(f"{hour[state]:,} {state}" for state in states if hour.get(state))
                or 'no runs'
            )
        columns.append(html.Span(
            [
                html.Span(
                    className=f'overview-hour-part overview-bar-{state}',
                    style={'height': f"{100 * hour[state] / peak:.1f}%"},
                )
                for state in states if hour.get(state)
            ],
            className='overview-hour' + (
                ' overview-hour-unread' if hour.get('unread') else (' overview-hour-empty' if not total else '')
            ),
            title=title,
        ))
    runs, failed = summary.get('runs_24h', 0) or 0, summary.get('failed_24h', 0) or 0
    return html.Div(
        [
            html.Div(
                columns, className='overview-history-bars', role='img',
                **{'aria-label': f"Runs per hour over the last {HISTORY_HOURS} hours: "
                                 f"{runs:,} runs, {failed:,} failed"},
            ),
            html.Div(
                [html.Span(f"{HISTORY_HOURS} h ago"), html.Span('now')],
                className='overview-history-axis', **{'aria-hidden': 'true'},
            ),
        ],
        className='overview-history',
        id='overview-pipelines-history',
    )


def _run_fact(label: str, item: Optional[Dict[str, Any]], element_id: str, instance_query: Optional[str]) -> Any:
    """`Last success` / `Last failure`: when, linked to that run, and which Pipeline."""
    if not item:
        value = html.Span('none', className='overview-fact-none')
        name = None
    else:
        value = dcc.Link(
            time_ago(item['started']) or 'unknown',
            href=with_query(
                '/dash/pipelines/' + quote(str(item['name']), safe=''),
                run_id=item['run_id'], instance=instance_query,
            ),
            className='overview-fact-link',
            title=str(item['started'] or ''),
        )
        name = html.Span(str(item['name']), className='overview-fact-name', title=str(item['name']))
    return html.Div(
        [html.Dt(label), html.Dd([value] + ([name] if name is not None else []))],
        className='overview-fact', id=element_id,
    )


def build_pipelines_body(summary: Dict[str, Any], instance_query: Optional[str] = None) -> List[Any]:
    """
    The Pipelines tile: how many, how many by their last run's status, their runs over the last
    24 hours, and the last success and failure.
    """
    href = with_query('/dash/pipelines', instance=instance_query)
    runs = summary.get('runs_24h', 0) or 0
    children = [
        _headline(
            _stat(summary['count'], href, 'All Pipelines', 'overview-pipelines-total'),
            _aside(f"{runs:,} run{'' if runs == 1 else 's'} in {HISTORY_HOURS} h", 'overview-pipelines-runs')
            if summary['count'] else None,
        ),
    ]
    if not summary['count']:
        children.append(dbc.Button(
            'New pipeline', href='/dash/pipelines/new', size='sm', color='secondary',
            id='overview-new-pipeline', class_name='overview-tile-action',
        ))
        return children
    states = summary.get('states', None) or {}
    shown = [
        (state, label) for state, label in PIPELINE_STATES
        if state != 'stopped' or states.get(state, 0)
    ]
    children += [
        _bar([(state, label, states.get(state, 0)) for state, label in shown], 'overview-pipelines-bar'),
        _state_rows(
            [(state, label, states.get(state, 0), href, f'overview-pipelines-{state}') for state, label in shown],
            'overview-pipelines-states',
        ),
        build_history(summary),
        html.Dl([
            _run_fact('Last success', summary.get('last_success'), 'overview-pipelines-last-success', instance_query),
            _run_fact('Last failure', summary.get('last_failure'), 'overview-pipelines-last-failure', instance_query),
        ], className='overview-facts'),
    ]
    return children


def build_jobs_body(summary: Dict[str, Any]) -> List[Any]:
    """The Jobs tile: the jobs by state, each linked to the Jobs page filtered by it."""
    shown = [
        (state, label) for state, label in JOB_STATES
        if state != 'paused' or summary['counts'].get(state, 0)
    ]
    return [
        _headline(
            _stat(summary['total'], '/dash/jobs', 'All jobs', 'overview-jobs-total'),
            _aside(f"{summary['scheduled']:,} on a schedule", 'overview-jobs-scheduled')
            if summary.get('scheduled') else None,
        ),
        _bar([(state, label, summary['counts'].get(state, 0)) for state, label in shown], 'overview-jobs-bar'),
        _state_rows([
            (state, label, summary['counts'].get(state, 0), with_query('/dash/jobs', status=state),
             f'overview-jobs-{state}')
            for state, label in shown
        ]),
    ]


def build_runs_body(runs: List[Dict[str, Any]], instance_query: Optional[str] = None) -> List[Any]:
    """Recent runs: status, what ran (a Pipeline or its Pipes), when, how long and what changed."""
    if not runs:
        return [html.P('No recent runs', className='overview-none')]
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
