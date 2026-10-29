#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Pipes page (`/dash/pipes`): a searchable, sortable table of the instance's pipes.

The table loads in two steps so it shows up fast: the pipes' keys, tags and targets come from one
`fetch_pipes_keys()` (`list_rows`), then the slow columns (the row estimate, the sync time, the
last sync's status and the jobs that sync each pipe) are filled for the rows on screen
(`compute_stats`). Every function returns plain JSON-able values for the page's stores, except the
`render_*` functions, which build the components.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs, quote, quote_plus

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional, Tuple
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.api import CHECK_UPDATE

html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

#: Rows per page of the table.
PAGE_SIZE: int = 100

#: Sorting by these columns (or filtering by status) needs every filtered row's statistics.
SLOW_COLUMNS = frozenset({'rows', 'sync_time', 'status'})

#: The most rows whose statistics are computed at once for a sort or a status filter.
MAX_STATS_ROWS: int = 2000

#: The sortable columns, in table order: `(column, header)`.
SORT_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ('pipe', 'Pipe'),
    ('rows', 'Rows'),
    ('sync_time', 'Sync time'),
    ('status', 'Status'),
    ('team', 'Team'),
)

#: The values of the `status` filter (`?status=` in the URL).
STATUS_FILTERS: Tuple[Tuple[str, str], ...] = (
    ('any', 'Any status'),
    ('failing', 'Failing'),
    ('ok', 'Synced'),
    ('never', 'Never synced'),
)

#: The values of the owner filter (`?scope=` in the URL): the viewer's own pipes, their teams'
#: pipes, or every pipe they may see.
SCOPE_FILTERS: Tuple[Tuple[str, str], ...] = (
    ('all', 'All'),
    ('mine', 'Mine'),
    ('teams', 'My teams'),
)

#: Instance flavors whose row estimate is always the exact count (C1).
EXACT_ROWCOUNT_FLAVORS = frozenset({'sqlite', 'duckdb', 'geopackage'})

#: Below this many rows an estimate falls back to the exact count (C1).
EXACT_ROWCOUNT_BELOW: int = 100_000

#: How long (seconds) statistics, sync statuses and the jobs index are reused.
STATS_TTL_SECONDS: float = 30.0
STATUSES_TTL_SECONDS: float = 10.0
JOBS_TTL_SECONDS: float = 30.0

_cache_lock = threading.Lock()
_cache: Dict[Any, Tuple[float, Any]] = {}


def _cache_get(key: Any, ttl: float) -> Any:
    with _cache_lock:
        entry = _cache.get(key, None)
    if entry is None:
        return None
    stored_at, value = entry
    if time.monotonic() - stored_at > ttl:
        with _cache_lock:
            _cache.pop(key, None)
        return None
    return value


def _cache_set(key: Any, value: Any) -> None:
    now = time.monotonic()
    with _cache_lock:
        if len(_cache) > 20_000:
            for stale_key in [k for k, (at, _) in _cache.items() if now - at > 300]:
                _cache.pop(stale_key, None)
        _cache[key] = (now, value)


def clear_cache(ref: Optional[str] = None) -> None:
    """Forget the cached statistics (of one pipe, or of every pipe)."""
    with _cache_lock:
        if ref is None:
            _cache.clear()
            return
        for key in [k for k in _cache if isinstance(k, tuple) and len(k) > 1 and k[1] == ref]:
            _cache.pop(key, None)


# ------------------------------------------------------------------------------------------------
# Keys and labels
# ------------------------------------------------------------------------------------------------

def make_ref(
    instance_keys: str,
    connector_keys: str,
    metric_key: str,
    location_key: Optional[str],
) -> str:
    """The engine's `pipe_ref`: `instance|connector_keys|metric_key|location_key`."""
    return '|'.join((
        str(instance_keys), str(connector_keys), str(metric_key), str(location_key),
    ))


def split_ref(ref: str) -> Tuple[str, str, str, Optional[str]]:
    """The keys of a `pipe_ref` (`'None'` is no location)."""
    instance_keys, connector_keys, metric_key, location_key = ref.split('|', 3)
    return instance_keys, connector_keys, metric_key, (None if location_key == 'None' else location_key)


def pipe_path(
    connector_keys: str,
    metric_key: str,
    location_key: Optional[str] = None,
    instance_keys: Optional[str] = None,
) -> str:
    """The console path of a pipe's page (`/dash/pipes/<ck>/<mk>[/<lk>]?instance=...`)."""
    parts = [connector_keys, metric_key] + ([location_key] if location_key is not None else [])
    path = '/dash/pipes/' + '/'.join(quote(str(part), safe='') for part in parts)
    if instance_keys:
        path += '?instance=' + quote(str(instance_keys), safe='')
    return path


def pipe_title(
    connector_keys: str,
    metric_key: str,
    location_key: Optional[str] = None,
    instance_keys: Optional[str] = None,
) -> Any:
    """
    `Pipe('ck', 'mk', 'lk')` in monospace, colored like the CLI's `pipe_repr()` (the connector
    keys green, the metric blue, the location magenta), with `instance='...'` when given.
    """
    def _arg(value: str, kind: str):
        return html.Span(repr(str(value)), className=f'mrsm-repr-{kind}')

    parts = [html.Span('Pipe(', className='mrsm-repr-type'), _arg(connector_keys, 'connector')]
    parts += [html.Span(', ', className='mrsm-repr-punctuation'), _arg(metric_key, 'metric')]
    if location_key is not None:
        parts += [html.Span(', ', className='mrsm-repr-punctuation'), _arg(location_key, 'location')]
    if instance_keys:
        parts += [
            html.Span(', instance=', className='mrsm-repr-punctuation'),
            _arg(instance_keys, 'instance'),
        ]
    parts.append(html.Span(')', className='mrsm-repr-type'))
    return html.Span(parts, className='mrsm-pipe-repr')


def pipe_label(
    connector_keys: str,
    metric_key: str,
    location_key: Optional[str] = None,
) -> str:
    """The plain text of `pipe_title()` (without the instance)."""
    args = [repr(str(connector_keys)), repr(str(metric_key))]
    if location_key is not None:
        args.append(repr(str(location_key)))
    return f"Pipe({', '.join(args)})"


def error_text(error: Any, connector: Any = None) -> str:
    """An exception's message for the page, with `connector`'s secrets (and URIs' credentials) masked."""
    from meerschaum.utils.misc import remove_ansi
    text = remove_ansi(str(error) or type(error).__name__).strip()
    ### Without the CLI's leading symbol (`🛑 Unable to parse ...`).
    while text and not (text[0].isalnum() or text[0] in '\'"([{<`'):
        text = text[1:].lstrip()
    text = text or type(error).__name__
    try:
        from meerschaum.api.dash.connector_listing import collect_secrets, scrub
        attributes = {
            key: value
            for key, value in vars(connector).items()
            if not str(key).startswith('_')
        } if connector is not None else {}
        return scrub(text, collect_secrets(attributes))
    except Exception:
        return text


def global_instance_keys() -> str:
    """The console's global instance (the instance selector's cookie), else the API's."""
    from meerschaum.api import get_api_connector
    from meerschaum.api.dash.engine import get_instance
    try:
        return str(get_instance())
    except Exception:
        return str(get_api_connector())


# ------------------------------------------------------------------------------------------------
# Filters in the URL
# ------------------------------------------------------------------------------------------------

def _query_list(query: Dict[str, List[str]], key: str) -> List[str]:
    values = []
    for value in query.get(key, []) or []:
        values.extend(part for part in str(value).split(',') if part != '')
    return values


def filters_from_search(search: Optional[str]) -> Dict[str, Any]:
    """
    The Pipes page's filters from its URL query (`?connector_keys=a,b&metric_keys=...
    &location_keys=...&tags=...&status=failing&q=...&instance=...`).
    """
    query = parse_qs((search or '').lstrip('?'))
    status = (query.get('status', [''])[0] or '').lower()
    if status == 'any':
        status = ''
    scope = (query.get('scope', [''])[0] or '').lower()
    if scope == 'all':
        scope = ''
    return {
        'connector_keys': _query_list(query, 'connector_keys'),
        'metric_keys': _query_list(query, 'metric_keys'),
        'location_keys': _query_list(query, 'location_keys'),
        'tags': _query_list(query, 'tags'),
        'status': status if status in {key for key, _ in STATUS_FILTERS} else '',
        'scope': scope if scope in {key for key, _ in SCOPE_FILTERS} else '',
        'q': (query.get('q', [''])[0] or '').strip(),
        'instance': query.get('instance', [None])[0] or None,
    }


def rows_key(filters: Dict[str, Any], instance_keys: str) -> List[Any]:
    """
    What the table's rows depend on: the instance and the key and tag filters. The search and the
    status filter narrow the rows already loaded (no new read).
    """
    return [
        str(instance_keys),
        *[sorted(str(value) for value in (filters.get(key) or []))
          for key in ('connector_keys', 'metric_keys', 'location_keys', 'tags')],
    ]


def search_from_filters(
    connector_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    location_keys: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    status: Optional[str] = None,
    instance: Optional[str] = None,
    q: Optional[str] = None,
    scope: Optional[str] = None,
) -> str:
    """The URL query of the Pipes page's filters (the inverse of `filters_from_search`)."""
    parts = []
    for key, values in (
        ('connector_keys', connector_keys),
        ('metric_keys', metric_keys),
        ('location_keys', location_keys),
        ('tags', tags),
    ):
        values = [str(value) for value in (values or []) if value is not None and str(value) != '']
        if values:
            parts.append(key + '=' + ','.join(quote_plus(value) for value in values))
    if status and status != 'any':
        parts.append('status=' + quote_plus(status))
    if scope and scope != 'all':
        parts.append('scope=' + quote_plus(scope))
    q = str(q or '').strip()
    if q:
        parts.append('q=' + quote_plus(q))
    if instance:
        parts.append('instance=' + quote_plus(str(instance)))
    return ('?' + '&'.join(parts)) if parts else ''


# ------------------------------------------------------------------------------------------------
# Rows
# ------------------------------------------------------------------------------------------------

def _access_fields(parameters: Dict[str, Any]) -> Dict[str, Any]:
    """A row's `owner`, `team` (`None` for Everyone), `visibility` and `locks`."""
    from meerschaum.api.dash import access
    try:
        fields = access.access_fields(parameters)
    except Exception:
        fields = {'owner': None, 'team': None, 'visibility': 'team', 'edit': 'team'}
    try:
        locks = access.pipe_locks(parameters)
    except Exception:
        locks = []
    return {
        'owner': fields.get('owner'),
        'team': fields.get('team'),
        'visibility': fields.get('visibility') or 'team',
        'locks': locks,
    }


def _row_from_pipe(pipe: mrsm.Pipe) -> Dict[str, Any]:
    instance_keys = str(pipe.instance_keys)
    ck, mk, lk = str(pipe.connector_keys), str(pipe.metric_key), pipe.location_key
    lk = None if lk is None else str(lk)
    try:
        tags = [str(tag) for tag in (pipe.tags or [])]
    except Exception:
        tags = []
    try:
        target = str(pipe.target)
    except Exception:
        target = ''
    try:
        parameters = dict(pipe.parameters or {})
    except Exception:
        parameters = {}
    ref = make_ref(instance_keys, ck, mk, lk)
    access_fields = _access_fields(parameters)
    from meerschaum.api.dash.access import EVERYONE
    return {
        'ref': ref,
        'ck': ck,
        'mk': mk,
        'lk': lk,
        'instance': instance_keys,
        'tags': tags,
        'target': target,
        'label': pipe_label(ck, mk, lk),
        'href': pipe_path(ck, mk, lk, instance_keys),
        **access_fields,
        'search': ' '.join(
            [ck, mk, str(lk or ''), target, str(access_fields['team'] or EVERYONE)] + tags
        ).lower(),
    }


def list_rows(
    instance: Any,
    connector_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    location_keys: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    viewer: Any = None,
) -> List[Dict[str, Any]]:
    """
    The table's rows for the pipes registered on `instance` matching the keys and tags that
    `viewer` may see: one `fetch_pipes_keys()` (keys with their parameters, whose `access` says
    who may see each pipe). Internal `mrsm` pipes (saved Pipelines' registry) are left out unless
    `mrsm` is asked for.
    """
    from meerschaum.api.dash import access
    viewer = viewer if viewer is not None else access.current_viewer()
    connector_keys = [str(ck) for ck in (connector_keys or [])]
    pipes = mrsm.get_pipes(
        connector_keys=connector_keys,
        metric_keys=[str(mk) for mk in (metric_keys or [])],
        location_keys=[(None if str(lk) == 'None' else str(lk)) for lk in (location_keys or [])],
        tags=[str(tag) for tag in (tags or [])],
        instance=instance,
        as_list=True,
    )
    include_internal = 'mrsm' in connector_keys
    rows = []
    for pipe in pipes:
        if str(pipe.connector_keys) == 'mrsm' and not include_internal:
            continue
        try:
            parameters = dict(pipe.parameters or {})
        except Exception:
            parameters = {}
        if not access.can_see_pipe(viewer, parameters):
            continue
        row = _row_from_pipe(pipe)
        _remember_pipe(row['ref'], pipe)
        rows.append(row)
    rows.sort(key=lambda row: (row['ck'].lower(), row['mk'].lower(), str(row['lk'] or '').lower()))
    return rows


def filter_options(instance: Any, viewer: Any = None) -> Dict[str, List[str]]:
    """
    The options of the key and tag filters: the keys and tags of every registered pipe `viewer`
    may see (another team's pipe never names its keys here).
    """
    from meerschaum.api.dash import access
    viewer = viewer if viewer is not None else access.current_viewer()
    connectors, metrics, locations, tags = set(), set(), set(), set()
    for ck, mk, lk, parameters in access.registered_pipes(instance):
        if str(ck) == 'mrsm' or not access.can_see_pipe(viewer, parameters):
            continue
        connectors.add(str(ck))
        metrics.add(str(mk))
        locations.add(str(lk))
        tags.update(str(tag) for tag in (parameters.get('tags', None) or []))
    key = lambda value: value.lower()
    return {
        'connector_keys': sorted(connectors, key=key),
        'metric_keys': sorted(metrics, key=key),
        'location_keys': sorted(locations, key=key),
        'tags': sorted(tags, key=key),
    }


def _remember_pipe(ref: str, pipe: mrsm.Pipe) -> None:
    """Keep the listed pipe (its parameters are cached) for the statistics that follow."""
    _cache_set(('pipe', ref), pipe)


def get_pipe(ref: str) -> mrsm.Pipe:
    """The pipe of a `pipe_ref`: the one just listed, else a new `Pipe`."""
    pipe = _cache_get(('pipe', ref), 120.0)
    if pipe is not None:
        return pipe
    instance_keys, connector_keys, metric_key, location_key = split_ref(ref)
    pipe = mrsm.Pipe(connector_keys, metric_key, location_key, instance=instance_keys)
    _remember_pipe(ref, pipe)
    return pipe


# ------------------------------------------------------------------------------------------------
# Statistics (the slow columns)
# ------------------------------------------------------------------------------------------------

def _accepts_keyword(func: Any, name: str) -> bool:
    import inspect
    try:
        parameters = inspect.signature(func).parameters
    except (TypeError, ValueError):
        return False
    return name in parameters or any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
    )


def get_rowcount(pipe: mrsm.Pipe, estimate: bool = True) -> Tuple[int, bool]:
    """
    Return `(rows, estimated)`: the database's row estimate when the instance can give one
    (`pipe.get_rowcount(estimate=True)`), else the exact count. `estimated` says whether the
    number may be approximate (the exact count is used below `EXACT_ROWCOUNT_BELOW` rows and for
    SQLite, DuckDB and GeoPackage instances).
    """
    use_estimate = estimate and _accepts_keyword(pipe.get_rowcount, 'estimate')
    if use_estimate:
        try:
            rows = pipe.get_rowcount(estimate=True)
        except TypeError:
            use_estimate = False
            rows = pipe.get_rowcount()
    else:
        rows = pipe.get_rowcount()
    rows = int(rows or 0)
    flavor = str(getattr(pipe.instance_connector, 'flavor', '') or '')
    estimated = bool(
        use_estimate
        and flavor not in EXACT_ROWCOUNT_FLAVORS
        and rows >= EXACT_ROWCOUNT_BELOW
    )
    return rows, estimated


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc)


def _sync_time_fields(value: Any) -> Dict[str, Any]:
    if value is None:
        return {'sync_time': None, 'sync_time_sort': None}
    if isinstance(value, datetime):
        value = _to_utc(value)
        text = value.isoformat()
        try:
            sort_key = value.replace(tzinfo=timezone.utc).timestamp() if value.tzinfo is None else value.timestamp()
        except (OverflowError, OSError, ValueError):
            sort_key = None
        return {'sync_time': text, 'sync_time_sort': sort_key}
    if isinstance(value, (int, float)):
        return {'sync_time': str(value), 'sync_time_sort': float(value)}
    return {'sync_time': str(value), 'sync_time_sort': None}


def _pipe_stats(ref: str) -> Dict[str, Any]:
    """The slow columns of one pipe: rows (and whether estimated) and the sync time."""
    cached = _cache_get(('stats', ref), STATS_TTL_SECONDS)
    if cached is not None:
        return cached
    stats: Dict[str, Any] = {
        'rows': None,
        'estimated': False,
        'sync_time': None,
        'sync_time_sort': None,
        'error': None,
    }
    try:
        pipe = get_pipe(ref)
    except Exception as e:
        stats['error'] = str(e)
        return stats
    try:
        stats['rows'], stats['estimated'] = get_rowcount(pipe)
    except Exception as e:
        stats['error'] = str(e)
    try:
        stats.update(_sync_time_fields(pipe.get_sync_time()))
    except Exception as e:
        stats['error'] = stats['error'] or str(e)
    _cache_set(('stats', ref), stats)
    return stats


def get_sync_statuses(instance: Any) -> Dict[str, Dict[str, Any]]:
    """
    The last sync of every pipe on `instance`, keyed by `pipe_ref`
    (`mrsm.get_sync_statuses()`, C2); empty when this build does not record them.
    """
    instance_keys = str(instance)
    cached = _cache_get(('statuses', instance_keys), STATUSES_TTL_SECONDS)
    if cached is not None:
        return cached
    getter = getattr(mrsm, 'get_sync_statuses', None)
    if getter is None:
        try:
            from meerschaum.core.Pipe._status import get_sync_statuses as getter
        except Exception:
            getter = None
    statuses: Dict[str, Dict[str, Any]] = {}
    if getter is not None:
        try:
            result = getter(instance=instance) or {}
        except TypeError:
            result = getter(instance) or {}
        except Exception:
            result = {}
        if isinstance(result, dict):
            statuses = {str(ref): dict(status) for ref, status in result.items() if isinstance(status, dict)}
        elif isinstance(result, list):
            statuses = {
                str(status.get('pipe_ref')): dict(status)
                for status in result
                if isinstance(status, dict) and status.get('pipe_ref')
            }
    _cache_set(('statuses', instance_keys), statuses)
    return statuses


def get_sync_status(pipe: mrsm.Pipe) -> Optional[Dict[str, Any]]:
    """The last sync of one pipe (`pipe.get_sync_status()`, C2), or `None`."""
    getter = getattr(pipe, 'get_sync_status', None)
    if getter is not None:
        try:
            status = getter()
            return dict(status) if isinstance(status, dict) else None
        except Exception:
            return None
    ref = make_ref(pipe.instance_keys, pipe.connector_keys, pipe.metric_key, pipe.location_key)
    return get_sync_statuses(pipe.instance_connector).get(ref, None)


def _parse_job_args(sysargs: List[str]) -> Optional[Dict[str, Any]]:
    from meerschaum._internal.arguments import parse_arguments
    try:
        return parse_arguments(list(sysargs))
    except Exception:
        return None
    except SystemExit:
        return None


def _values_match(value: Optional[str], selected: Optional[List[Any]]) -> bool:
    """Whether a key matches a job's `-c` / `-m` / `-l` values (`_` negates)."""
    if not selected:
        return True
    value = 'None' if value is None else str(value)
    selected = ['None' if item is None else str(item) for item in selected]
    includes = [item for item in selected if not item.startswith('_')]
    excludes = [item[1:] for item in selected if item.startswith('_')]
    if value in excludes:
        return False
    return not includes or value in includes


def _tags_match(pipe_tags: List[str], selected: Optional[List[str]]) -> bool:
    """Whether a pipe's tags match a job's `-t` values (any value; `a,b` needs both; `_` negates)."""
    if not selected:
        return True
    pipe_tags = set(pipe_tags or [])
    includes, excludes = [], []
    for item in selected:
        item = str(item)
        (excludes if item.startswith('_') else includes).append(item.lstrip('_'))
    if any(tag in pipe_tags for tag in excludes):
        return False
    if not includes:
        return True
    return any(all(part in pipe_tags for part in group.split(',') if part) for group in includes)


def _job_summary(
    job: Any,
    args: Dict[str, Any],
    via: Optional[str] = None,
    command: Optional[str] = None,
) -> Dict[str, Any]:
    schedule = args.get('schedule', None)
    if not schedule and args.get('loop'):
        min_seconds = args.get('min_seconds', None)
        try:
            seconds = float(min_seconds)
            min_seconds = int(seconds) if seconds.is_integer() else seconds
        except (TypeError, ValueError):
            pass
        schedule = f"every {min_seconds} s" if min_seconds else 'continuously'
    try:
        status = str(job.status)
    except Exception:
        status = ''
    if command is None:
        try:
            command = ' '.join(str(arg) for arg in (job.sysargs or []))
        except Exception:
            command = ''
    return {
        'name': str(job.name),
        'status': status,
        'schedule': str(schedule) if schedule else '',
        'command': command,
        'via': via,
    }


def get_jobs_index(instance_keys: str, rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    The jobs that sync each row's pipe: `sync pipes` and `verify pipes` jobs (or such commands of
    an action chain, `a + b : --loop`) whose keys, tags and instance select it, and the jobs of
    saved Pipelines (`sync pipelines NAME`) that list it. Console executions are left out. Cached
    for `JOBS_TTL_SECONDS`.
    """
    cached = _cache_get(('jobs', instance_keys), JOBS_TTL_SECONDS)
    if cached is None:
        cached = _jobs_selections(instance_keys)
        _cache_set(('jobs', instance_keys), cached)
    selections, pipelines_jobs = cached
    index: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        for selection, summary in selections:
            if (
                _values_match(row['ck'], selection.get('connector_keys'))
                and _values_match(row['mk'], selection.get('metric_keys'))
                and _values_match(row['lk'], selection.get('location_keys'))
                and _tags_match(row['tags'], selection.get('tags'))
            ):
                jobs = index.setdefault(row['ref'], [])
                if all(job['name'] != summary['name'] for job in jobs):
                    jobs.append(summary)
    if pipelines_jobs:
        try:
            from meerschaum.core.Pipeline._saved import list_pipelines
            definitions = list_pipelines(mrsm.get_connector(instance_keys)) or {}
        except Exception:
            definitions = {}
        refs = {row['ref'] for row in rows}
        for name, summaries in pipelines_jobs.items():
            targets = set((definitions.get(name) or {}).get('targets') or [])
            for ref in targets & refs:
                jobs = index.setdefault(ref, [])
                for summary in summaries:
                    if all(job['name'] != summary['name'] for job in jobs):
                        jobs.append(summary)
    return index


#: The commands whose jobs sync the pipes they select (the Jobs column).
SYNCING_COMMANDS: Tuple[Tuple[str, str], ...] = (
    ('sync', 'pipes'), ('sync', 'pipe'), ('verify', 'pipes'), ('verify', 'pipe'),
)
PIPELINE_COMMANDS: Tuple[Tuple[str, str], ...] = (('sync', 'pipelines'), ('sync', 'pipeline'))


def _job_commands(sysargs: List[str]) -> Tuple[List[List[str]], List[str], str]:
    """
    `(commands, chain_args, command_text)` of a job: an action chain's commands (`a + b : --loop`,
    stored as `chain ... -P {...}`) and the flags after its `:`, else the job's one command.
    """
    import shlex
    from meerschaum._internal.arguments._parse_arguments import (
        compress_pipeline_sysargs, split_pipeline_sysargs, split_chained_sysargs,
    )
    compressed = compress_pipeline_sysargs(list(sysargs))
    main, chain_args = split_pipeline_sysargs(compressed)
    commands = [command for command in split_chained_sysargs(main) if command]
    return commands, list(chain_args), shlex.join(compressed)


def _job_instance(job: Any, default_instance: str) -> str:
    """
    The instance a job's commands use without `-i`: the `meerschaum:instance` of the job's own
    `MRSM_PATCH` / `MRSM_CONFIG` (a compose project's `config:`), else the server's.
    """
    import json
    try:
        env = dict(job.env or {})
    except Exception:
        env = {}
    for key in ('MRSM_PATCH', 'MRSM_CONFIG'):
        try:
            patch = json.loads(env.get(key) or '{}')
        except (TypeError, ValueError):
            continue
        instance = ((patch.get('meerschaum', None) or {}) if isinstance(patch, dict) else {}).get('instance', None)
        if instance:
            return str(instance)
    return default_instance


def _jobs_selections(instance_keys: str) -> Tuple[List[Tuple[Dict[str, Any], Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]:
    try:
        from meerschaum.jobs import get_jobs
        jobs = get_jobs() or {}
    except Exception:
        return [], {}
    try:
        from meerschaum.api.dash.jobs import is_automatic_job
    except Exception:
        is_automatic_job = lambda job: str(getattr(job, 'name', '')).startswith('execute-')
    default_instance = str(mrsm.get_config('meerschaum', 'instance'))
    selections = []
    pipelines_jobs: Dict[str, List[Dict[str, Any]]] = {}
    for job in jobs.values():
        try:
            if is_automatic_job(job):
                continue
            commands, chain_args, command_text = _job_commands(list(job.sysargs or []))
        except Exception:
            continue
        job_default_instance = None
        for command in commands:
            if tuple(command[:2]) not in SYNCING_COMMANDS + PIPELINE_COMMANDS:
                continue
            args = _parse_job_args(command + chain_args)
            if not args:
                continue
            action = [str(word) for word in (args.get('action') or [])]
            if job_default_instance is None:
                job_default_instance = _job_instance(job, default_instance)
            job_instance = str(args.get('mrsm_instance') or args.get('instance') or job_default_instance)
            if job_instance != str(instance_keys):
                continue
            if tuple(action[:2]) in SYNCING_COMMANDS:
                selections.append((args, _job_summary(job, args, command=command_text)))
            elif tuple(action[:2]) in PIPELINE_COMMANDS and len(action) > 2:
                for name in action[2:]:
                    pipelines_jobs.setdefault(name, []).append(
                        _job_summary(job, args, via=name, command=command_text)
                    )
    return selections, pipelines_jobs


def allowed_rows(
    instance_keys: str,
    rows: List[Dict[str, Any]],
    viewer: Any = None,
) -> List[Dict[str, Any]]:
    """
    The rows (as a page's store sent them back) of pipes on `instance_keys` that `viewer` may
    see: a store is the browser's, so its refs are checked again before anything is read.
    """
    from meerschaum.api.dash import access
    viewer = viewer if viewer is not None else access.current_viewer()
    prefix = str(instance_keys) + '|'
    rows = [
        row for row in (rows or [])
        if isinstance(row, dict) and str(row.get('ref', '')).startswith(prefix)
    ]
    refs = access.visible_refs(viewer, instance_keys)
    if refs is None:
        return rows
    return [row for row in rows if row['ref'] in refs]


def compute_stats(
    instance_keys: str,
    rows: List[Dict[str, Any]],
    workers: int = 8,
    viewer: Any = None,
) -> Dict[str, Dict[str, Any]]:
    """
    The slow columns of `rows`: `{pipe_ref: {rows, estimated, sync_time, sync_time_sort, error,
    status, jobs}}`, computed concurrently when the instance connector is thread-safe. Only rows
    of pipes `viewer` may see on that instance are computed (`allowed_rows`).
    """
    from meerschaum.api.dash.engine import get_instance
    try:
        instance = get_instance(instance_keys, strict=True)
    except Exception:
        return {}
    instance_keys = str(instance)
    rows = allowed_rows(instance_keys, rows, viewer=viewer)
    if not rows:
        return {}
    refs = [row['ref'] for row in rows]
    thread_safe = bool(getattr(instance, 'IS_THREAD_SAFE', False))
    if thread_safe and len(refs) > 1:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(workers, len(refs))) as pool:
            results = list(pool.map(_pipe_stats, refs))
    else:
        results = [_pipe_stats(ref) for ref in refs]
    statuses = get_sync_statuses(instance) if instance is not None else {}
    jobs = get_jobs_index(instance_keys, rows)
    out = {}
    for row, stats in zip(rows, results):
        out[row['ref']] = {
            **stats,
            'status': statuses.get(row['ref'], None),
            'jobs': jobs.get(row['ref'], []),
        }
    return out


# ------------------------------------------------------------------------------------------------
# Search, filter, sort and page
# ------------------------------------------------------------------------------------------------

def status_kind(stats: Optional[Dict[str, Any]]) -> Optional[str]:
    """
    `failing`, `ok`, `never` (no sync recorded and no data) or `unknown` (data but no recorded
    sync); `None` before the statistics load.
    """
    if not stats:
        return None
    status = stats.get('status', None)
    if status:
        if status.get('success') is False or int(status.get('consecutive_failures') or 0) > 0:
            return 'failing'
        return 'ok'
    if not stats.get('rows') and stats.get('sync_time') is None and not stats.get('error'):
        return 'never'
    return 'unknown'


_STATUS_RANK = {'failing': 0, 'never': 1, 'unknown': 2, 'ok': 3}


def search_rows(rows: List[Dict[str, Any]], search: Optional[str]) -> List[Dict[str, Any]]:
    """The rows whose keys, target or tags contain every word of `search` (case-insensitive)."""
    words = [word for word in str(search or '').lower().split() if word]
    if not words:
        return list(rows)
    return [row for row in rows if all(word in row['search'] for word in words)]


def filter_status(
    rows: List[Dict[str, Any]],
    stats: Dict[str, Dict[str, Any]],
    status: Optional[str],
) -> List[Dict[str, Any]]:
    """The rows whose status is `status` (all rows without a status filter or with `any`)."""
    if not status or status == 'any':
        return list(rows)
    return [row for row in rows if status_kind(stats.get(row['ref'])) == status]


def viewer_summary(viewer: Any) -> Dict[str, Any]:
    """What the owner filter needs to know about the viewer: `{username, teams}`."""
    return {
        'username': getattr(viewer, 'username', None),
        'teams': list(getattr(viewer, 'own_teams', []) or []),
    }


def filter_scope(
    rows: List[Dict[str, Any]],
    scope: Optional[str],
    me: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    The owner filter: `mine` (the viewer owns the pipe), `teams` (the pipe is one of the viewer's
    teams'; for a viewer in no team but Everyone, Everyone's pipes), else every row.
    """
    if not scope or scope == 'all':
        return list(rows)
    me = me or {}
    if scope == 'mine':
        username = me.get('username', None)
        return [row for row in rows if username and row.get('owner') == username]
    if scope == 'teams':
        teams = set(me.get('teams', None) or [])
        if not teams:
            return [row for row in rows if not row.get('team')]
        return [row for row in rows if row.get('team') in teams]
    return list(rows)


def sort_rows(
    rows: List[Dict[str, Any]],
    stats: Dict[str, Dict[str, Any]],
    column: str = 'pipe',
    ascending: bool = True,
) -> List[Dict[str, Any]]:
    """
    Sort by a column. Rows whose statistics have not loaded (or have no value) go last in either
    direction; ties keep the keys' order.
    """
    def keys_order(row):
        return (row['ck'].lower(), row['mk'].lower(), str(row['lk'] or '').lower(), row['instance'])

    ordered = sorted(rows, key=keys_order)
    if column == 'pipe':
        return ordered if ascending else list(reversed(ordered))
    if column == 'team':
        ### Everyone's pipes (no team) sort first going up, last going down.
        return sorted(ordered, key=lambda row: str(row.get('team') or '').lower(), reverse=not ascending)

    def value(row):
        entry = stats.get(row['ref'])
        if not entry:
            return None
        if column == 'rows':
            return entry.get('rows')
        if column == 'sync_time':
            return entry.get('sync_time_sort')
        if column == 'status':
            kind = status_kind(entry)
            return _STATUS_RANK.get(kind) if kind else None
        return None

    known = [row for row in ordered if value(row) is not None]
    unknown = [row for row in ordered if value(row) is None]
    known.sort(key=value, reverse=not ascending)
    return known + unknown


def visible_rows(
    rows: List[Dict[str, Any]],
    stats: Dict[str, Dict[str, Any]],
    search: Optional[str] = None,
    status: Optional[str] = None,
    column: str = 'pipe',
    ascending: bool = True,
    page: int = 1,
    page_size: int = PAGE_SIZE,
    scope: Optional[str] = None,
    me: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """
    Search, filter, sort and page the rows: `(page_rows, page, num_pages, num_matching)`.
    """
    matching = filter_status(filter_scope(search_rows(rows, search), scope, me), stats, status)
    ordered = sort_rows(matching, stats, column, ascending)
    num_pages = max(1, -(-len(ordered) // page_size))
    page = min(max(1, int(page or 1)), num_pages)
    start = (page - 1) * page_size
    return ordered[start:start + page_size], page, num_pages, len(ordered)


def rows_needing_stats(
    rows: List[Dict[str, Any]],
    stats: Dict[str, Dict[str, Any]],
    search: Optional[str] = None,
    status: Optional[str] = None,
    column: str = 'pipe',
    ascending: bool = True,
    page: int = 1,
    scope: Optional[str] = None,
    me: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    The rows whose statistics are missing and needed now: every searched row when sorting by a
    slow column or filtering by status (at most `MAX_STATS_ROWS`), else the page on screen.
    """
    if column in SLOW_COLUMNS or (status and status != 'any'):
        candidates = filter_scope(search_rows(rows, search), scope, me)[:MAX_STATS_ROWS]
    else:
        candidates, _, _, _ = visible_rows(
            rows, stats, search, None, column, ascending, page, scope=scope, me=me,
        )
    return [row for row in candidates if row['ref'] not in stats]


# ------------------------------------------------------------------------------------------------
# Rendering
# ------------------------------------------------------------------------------------------------

def humanize_count(value: int) -> str:
    """`1234` -> `1.2K`, `3_400_000` -> `3.4M`."""
    value = int(value)
    for threshold, suffix in ((1_000_000_000_000, 'T'), (1_000_000_000, 'B'), (1_000_000, 'M'), (1_000, 'K')):
        if abs(value) >= threshold:
            scaled = value / threshold
            text = f"{scaled:.1f}" if abs(scaled) < 100 else f"{scaled:.0f}"
            if '.' in text:
                text = text.rstrip('0').rstrip('.')
            return text + suffix
    return str(value)


def rows_text(stats: Optional[Dict[str, Any]]) -> str:
    """The Rows cell: `12,345`, or `~1.2M` for an estimate."""
    if not stats:
        return '…'
    rows = stats.get('rows', None)
    if rows is None:
        return '—'
    if stats.get('estimated'):
        return '~' + humanize_count(rows)
    return f"{int(rows):,}"


def parse_timestamp(text: Optional[str]) -> Optional[datetime]:
    if not text:
        return None
    try:
        return datetime.fromisoformat(str(text).replace('Z', '+00:00'))
    except ValueError:
        return None


def sync_time_text(stats: Optional[Dict[str, Any]]) -> str:
    """The Sync time cell: `2026-10-27 10:00` (UTC for time-zone-aware values) or the integer."""
    if not stats:
        return '…'
    value = stats.get('sync_time', None)
    if value is None:
        return '—'
    parsed = parse_timestamp(value)
    if parsed is None:
        return str(value)
    return parsed.strftime('%Y-%m-%d %H:%M') + (' UTC' if parsed.tzinfo is not None else '')


def time_ago(text: Optional[str], now: Optional[datetime] = None) -> str:
    """`just now`, `5 min ago`, `3 h ago`, `2 days ago` (from an ISO-8601 timestamp)."""
    parsed = parse_timestamp(text)
    if parsed is None:
        return ''
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    seconds = (now - parsed).total_seconds()
    if seconds < 60:
        return 'just now'
    if seconds < 3600:
        return f"{int(seconds // 60)} min ago"
    if seconds < 86400:
        return f"{int(seconds // 3600)} h ago"
    days = int(seconds // 86400)
    return f"{days} day{'s' if days != 1 else ''} ago"


def status_cell(stats: Optional[Dict[str, Any]]) -> Tuple[str, str, str]:
    """`(text, title, kind)` of the Status cell."""
    kind = status_kind(stats)
    if kind is None:
        return '…', '', 'loading'
    status = (stats or {}).get('status') or {}
    message = str(status.get('message') or '').strip()
    if kind == 'failing':
        failures = int(status.get('consecutive_failures') or 1)
        text = 'Failed' if failures <= 1 else f"Failed {failures} times"
        when = time_ago(status.get('last_failure') or status.get('last_attempt'))
        title = ' · '.join(part for part in (when, message) if part)
        return text, title, kind
    if kind == 'ok':
        when = time_ago(status.get('last_success') or status.get('last_attempt'))
        return (f"Synced {when}" if when else 'Synced'), message, kind
    if kind == 'never':
        return 'Never synced', '', kind
    error = str((stats or {}).get('error') or '')
    return '—', (error or 'No sync recorded yet.'), kind


JOB_STATUS_COLORS = {'running': 'success', 'paused': 'warning', 'stopped': 'secondary'}


def job_badges(jobs: List[Dict[str, Any]]) -> List[Any]:
    """A badge per job that syncs the pipe (its schedule and command in the title)."""
    return [
        dbc.Badge(
            job['name'],
            color=JOB_STATUS_COLORS.get(job.get('status', ''), 'secondary'),
            className='pipes-job-badge',
            title=_job_title(job),
        )
        for job in _unique_jobs(jobs)
    ]


# The table is one `dcc.Markdown` of HTML (`dangerously_allow_html`, which the component parses
# as JSX): Dash's renderer takes time that grows faster than the number of components one callback
# returns (a page of 100 rows built from components took seconds to appear, 200 rows about 20 s),
# while the same rows as HTML appear at once. Every value is escaped with `_esc`; the rows' links are
# `dccLink`s (in-place navigation) and the headers' sort buttons call back through
# `assets/pipes.js` (`pipes-sort-click`).

_JSX_ATTRIBUTE_NAMES = {'class_name': 'className', 'col_span': 'colSpan'}


def _esc(value: Any) -> str:
    """Text or an attribute value of the table's HTML: escaped, braces (JSX) and newlines too."""
    import html as _html
    return (
        _html.escape(str(value), quote=True)
        .replace('{', '&#123;').replace('}', '&#125;')
        .replace('\r', '&#13;').replace('\n', '&#10;')
    )


def _tag(name: str, children: Any = '', **attributes: Any) -> str:
    """`<name attributes>children</name>`; `children` is HTML (escape text with `_esc`)."""
    attrs = ''.join(
        f' {_JSX_ATTRIBUTE_NAMES.get(key, key.replace("_", "-"))}="{_esc(value)}"'
        for key, value in attributes.items()
        if value is not None and value is not False
    )
    content = ''.join(children) if isinstance(children, (list, tuple)) else str(children)
    return f'<{name}{attrs}>{content}</{name}>'


def pipe_title_html(
    connector_keys: str,
    metric_key: str,
    location_key: Optional[str] = None,
) -> str:
    """`pipe_title()` as the table's HTML."""
    def _arg(value: str, kind: str) -> str:
        return _tag('span', _esc(repr(str(value))), class_name=f'mrsm-repr-{kind}')

    comma = _tag('span', ', ', class_name='mrsm-repr-punctuation')
    parts = [_tag('span', 'Pipe(', class_name='mrsm-repr-type'), _arg(connector_keys, 'connector')]
    parts += [comma, _arg(metric_key, 'metric')]
    if location_key is not None:
        parts += [comma, _arg(location_key, 'location')]
    parts.append(_tag('span', ')', class_name='mrsm-repr-type'))
    return _tag('span', parts, class_name='mrsm-pipe-repr')


def _job_title(job: Dict[str, Any]) -> str:
    return ' · '.join(part for part in (
        job.get('schedule', ''),
        (f"Pipeline('{job['via']}')" if job.get('via') else ''),
        job.get('command', ''),
        job.get('status', ''),
    ) if part)


def _unique_jobs(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen, out = set(), []
    for job in jobs or []:
        if job['name'] in seen:
            continue
        seen.add(job['name'])
        out.append(job)
    return out


def job_badges_html(jobs: List[Dict[str, Any]]) -> str:
    """`job_badges()` as the table's HTML."""
    return ''.join(
        _tag(
            'span', _esc(job['name']),
            class_name=f"badge bg-{JOB_STATUS_COLORS.get(job.get('status', ''), 'secondary')} pipes-job-badge",
            title=_job_title(job),
        )
        for job in _unique_jobs(jobs)
    )


def locks_title(locks: List[str]) -> str:
    """`Locked: sync, drop` (the lock column's and the pipe page's lock icon's title)."""
    return 'Locked: ' + ', '.join(locks) if locks else ''


def lock_html(locks: List[str]) -> str:
    """The lock column: a lock icon titled with the locks (empty for an unlocked pipe)."""
    if not locks:
        return ''
    title = locks_title(locks)
    return _tag(
        'span',
        [
            _tag('span', '', class_name='mrsm-icon mrsm-icon-lock', aria_hidden='true'),
            _tag('span', _esc(title), class_name='visually-hidden'),
        ],
        class_name='pipes-lock', title=title,
    )


def team_html(row: Dict[str, Any]) -> str:
    """The Team cell: the team (Everyone when none), and `private` or `everyone` visibility."""
    from meerschaum.api.dash.access import EVERYONE
    html_text = _esc(row.get('team') or EVERYONE)
    visibility = row.get('visibility') or 'team'
    if visibility != 'team':
        html_text += _tag('span', _esc(visibility), class_name=f'pipes-visibility pipes-visibility-{visibility}')
    return html_text


def render_rows_html(
    rows: List[Dict[str, Any]],
    stats: Dict[str, Dict[str, Any]],
    global_instance: Optional[str] = None,
    show_team: bool = True,
) -> str:
    """The table body's rows (each opens its pipe's page)."""
    out = []
    for row in rows:
        entry = stats.get(row['ref'])
        status_text, status_title, kind = status_cell(entry)
        show_instance = bool(global_instance) and row['instance'] != str(global_instance)
        pipe_cell = _tag(
            'dccLink', pipe_title_html(row['ck'], row['mk'], row['lk']),
            href=row['href'], class_name='pipes-table-link', title=row['label'],
        )
        if show_instance:
            pipe_cell += _tag('span', _esc(row['instance']),
                              class_name='badge bg-secondary pipes-instance-badge')
        locks = list(row.get('locks') or [])
        cells = [
            _tag('td', pipe_cell, class_name='pipes-col-pipe'),
            _tag('td', lock_html(locks), class_name='pipes-col-locks',
                 data_label=('Locks' if locks else None)),
            _tag('td', _esc(rows_text(entry)), class_name='pipes-col-rows',
                 title=str((entry or {}).get('error') or ''), data_label='Rows'),
            _tag('td', _esc(sync_time_text(entry)), class_name='pipes-col-sync-time',
                 title=str((entry or {}).get('sync_time') or ''), data_label='Sync time'),
            _tag('td', _tag('span', _esc(status_text), class_name=f'pipes-status pipes-status-{kind}'),
                 class_name='pipes-col-status', title=status_title, data_label='Status'),
            (_tag('td', team_html(row), class_name='pipes-col-team', data_label='Team')
             if show_team else ''),
            _tag('td', [_tag('span', _esc(tag), class_name='pipes-tag') for tag in row['tags']],
                 class_name='pipes-col-tags', data_label='Tags'),
            _tag('td', job_badges_html((entry or {}).get('jobs') or []) if entry else '',
                 class_name='pipes-col-jobs', data_label='Jobs'),
        ]
        out.append(_tag('tr', cells, class_name='pipes-table-row', data_href=row['href'], data_ref=row['ref']))
    return ''.join(out)


def sort_header_class(column: str, sort: Optional[Dict[str, Any]]) -> str:
    sort = sort or {}
    if sort.get('column', 'pipe') != column:
        return 'pipes-sort-button'
    return 'pipes-sort-button ' + ('pipes-sort-asc' if sort.get('ascending', True) else 'pipes-sort-desc')


def table_header_html(sort: Optional[Dict[str, Any]] = None, show_team: bool = True) -> str:
    """
    The header row: a sort button per sortable column (`aria-sort` on the sorted one). The Team
    column is left out (`show_team`) on an instance whose pipes are all Everyone's.
    """
    sort = sort or {'column': 'pipe', 'ascending': True}
    cells = []
    for column, label in SORT_COLUMNS:
        if column == 'team' and not show_team:
            continue
        if sort.get('column', 'pipe') == column:
            aria_sort = 'ascending' if sort.get('ascending', True) else 'descending'
        else:
            aria_sort = 'none'
        cells.append(_tag(
            'th',
            _tag('button', _esc(label), type='button', id=f'pipes-sort-{column}',
                 class_name=sort_header_class(column, sort), data_sort=column),
            id=f'pipes-header-{column}', scope='col', aria_sort=aria_sort,
            class_name=f'pipes-col-{column.replace("_", "-")}',
        ))
        if column == 'pipe':
            cells.append(_tag(
                'th', _tag('span', 'Locks', class_name='visually-hidden'),
                scope='col', class_name='pipes-col-locks',
            ))
    cells += [
        _tag('th', 'Tags', scope='col', class_name='pipes-col-tags'),
        _tag('th', 'Jobs', scope='col', class_name='pipes-col-jobs'),
    ]
    return _tag('thead', _tag('tr', cells))


def _note_row(content: str, show_team: bool = True) -> str:
    columns = len(SORT_COLUMNS) + 3 - (0 if show_team else 1)
    return _tag('tr', _tag('td', content, col_span=columns, class_name='pipes-empty-row'))


#: A row's spinner while the table loads (Bootstrap's, as `dbc.Spinner(size='sm')`).
SPINNER_HTML: str = _tag('div', '', class_name='spinner-border spinner-border-sm text-secondary', role='status')


def render_table_html(
    rows: List[Dict[str, Any]],
    stats: Dict[str, Dict[str, Any]],
    global_instance: Optional[str] = None,
    sort: Optional[Dict[str, Any]] = None,
    empty_text: str = 'No pipes match.',
    loading: bool = False,
    show_team: bool = True,
) -> str:
    """The whole table: the header and the rows, a spinner (`loading`) or `empty_text`."""
    if loading:
        body = _note_row(SPINNER_HTML, show_team)
    elif not rows:
        body = _note_row(_esc(empty_text), show_team)
    else:
        body = render_rows_html(rows, stats, global_instance, show_team=show_team)
    return _tag(
        'table',
        [table_header_html(sort, show_team), _tag('tbody', body, id='pipes-table-body')],
        id='pipes-table', class_name='table table-hover pipes-table',
    )


def summary_text(num_matching: int, total: int, page: int, num_pages: int, page_size: int = PAGE_SIZE) -> str:
    """`12 pipes`, `12 of 40 pipes`, or `101–200 of 312 pipes`."""
    noun = 'pipe' if total == 1 else 'pipes'
    if num_pages > 1:
        start = (page - 1) * page_size + 1
        end = min(page * page_size, num_matching)
        return f"{start}–{end} of {num_matching} {noun}"
    if num_matching != total:
        return f"{num_matching} of {total} {noun}"
    return f"{total} {noun}"


def build_table_layout() -> Any:
    """The table (a spinner until the rows load), in its scrolling frame."""
    return html.Div(
        dcc.Markdown(
            render_table_html([], {}, loading=True),
            id='pipes-table-markdown',
            dangerously_allow_html=True,
            className='pipes-table-markdown',
        ),
        className='pipes-table-scroll',
    )
