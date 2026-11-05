"""Permission-filtered sync measurements and the Performance page's presentation helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, parse_qs, urlparse, quote
import math

from meerschaum.utils.typing import Any, Dict, List, Optional

WINDOWS = (('1h', 'Last hour'), ('24h', 'Last 24 hours'), ('7d', 'Last 7 days'),
           ('30d', 'Last 30 days'), ('all', 'All retained history'))
WINDOW_HOURS = {'1h': 1, '24h': 24, '7d': 168, '30d': 720, 'all': 720}
METRICS_LIMIT = 1000
METRICS_BUCKETS = 48
HISTORY_PAGE_SIZE = 10
LIMITED_NOTE = 'This view is limited. Choose a shorter window or filter by Pipe or Pipeline for more detail.'


def selection(href: Optional[str]) -> Dict[str, Any]:
    """The same selection for an initial page and a query-only drilldown / Back navigation."""
    query = parse_qs(urlparse(href or '').query)
    window = (query.get('window') or ['24h'])[0]
    return {
        'instance': (query.get('instance') or [None])[0],
        'window': window if window in WINDOW_HOURS else '24h',
        'pipe': (query.get('pipe') or [None])[0],
        'pipeline': (query.get('pipeline') or [None])[0],
    }


def filter_options(options=None, value=None, kind='Pipe'):
    """Keep a chosen value while loading or refusing it, without retaining fetched labels."""
    choices = list(options or [])
    if isinstance(value, str) and value and not any(item.get('value') == value for item in choices):
        choices.append({'label': f'Selected {kind}', 'value': value})
    return choices


def fresh_viewer(session: Any):
    from meerschaum.api.dash.users import viewer
    who = viewer(session)
    if not who['active'] or not who['viewer'].can('pipes:read'):
        raise PermissionError('Performance is not available to your account.')
    return who['viewer']


def metrics(instance_keys: Optional[str], session: Any, window='24h', pipe=None, pipeline=None,
            with_options=True):
    """Build trusted visibility ceilings on the host, before native aggregation."""
    import meerschaum as mrsm
    from meerschaum.api.dash import access, engine
    from meerschaum.api.dash.connector_listing import connector_team_allows
    who = fresh_viewer(session)
    instance = engine.get_instance(instance_keys, strict=True)
    if not connector_team_allows(str(instance), who):
        raise PermissionError('This instance is not available to your account.')
    if window not in WINDOW_HOURS:
        raise ValueError('Choose a valid time window.')
    allowed_sources = {}
    def source_allowed(keys):
        if keys not in allowed_sources:
            allowed_sources[keys] = connector_team_allows(keys, who)
        return allowed_sources[keys]
    if with_options or not (who.no_auth or who.is_admin):
        available_refs = {'|'.join((str(instance), ck, mk, str(lk)))
                          for ck, mk, lk, parameters in access.registered_pipes(instance)
                          if ck != 'mrsm' and access.can_see_pipe(who, parameters) and source_allowed(ck)}
        available_names = set(access.visible_pipelines(instance, viewer=who, include_members=False))
    else:
        available_refs, available_names = set(), set()
    if who.no_auth or who.is_admin:
        refs, names = None, None
    else:
        # Read metadata now: a permission or visibility change must affect the next refresh.
        refs, names = available_refs, available_names
    if ((pipe and refs is not None and pipe not in refs)
        or (pipeline and names is not None and pipeline not in names)):
        raise PermissionError('This selection is not available to your account.')
    end = datetime.now(timezone.utc)
    hours = WINDOW_HOURS[window]
    result = mrsm.get_engine_metrics(
        instance=instance, begin=(end - timedelta(hours=hours) if hours else None), end=end,
        pipe_ref=pipe or None, pipeline=pipeline or None, limit=METRICS_LIMIT,
        buckets=METRICS_BUCKETS if with_options else 1,
        summary_only=not with_options,
        visible_refs=(sorted(refs) if refs is not None else None),
        visible_pipelines=(sorted(names) if names is not None else None),
    )
    return {**result, '_instance': str(instance), '_pipe_filter': pipe, '_pipeline_filter': pipeline,
            # Host-only: the same fresh ceiling for the Engine section (`engine_health()`).
            '_visible_refs': (sorted(refs) if refs is not None else None), '_viewer': who,
            '_pipeline_names': sorted(available_names),
            '_pipe_options': ([{'label': engine.ref_label(ref), 'value': ref}
                               for ref in sorted(available_refs)] if with_options else []),
            '_pipeline_options': ([{'label': name, 'value': name}
                                   for name in sorted(available_names)] if with_options else [])}


def number(value: Any, digits=0) -> str:
    try:
        if isinstance(value, int) and not digits:
            return f'{value:,}'
        numeric = float(value)
        return f'{numeric:,.{digits}f}' if math.isfinite(numeric) else 'n/a'
    except (TypeError, ValueError, OverflowError):
        return 'n/a'


def duration(value: Any) -> str:
    try:
        seconds = float(value)
    except (TypeError, ValueError, OverflowError):
        return 'n/a'
    if not math.isfinite(seconds) or seconds < 0:
        return 'n/a'
    if seconds < 1:
        return f'{seconds * 1000:,.0f} ms'
    if seconds < 60:
        return f'{seconds:,.2f} s'
    return f'{seconds / 60:,.1f} min'


def timestamp(value: Any) -> str:
    if not value:
        return 'n/a'
    try:
        value = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    except ValueError:
        return 'n/a'


def pipe_keys(reference) -> Optional[tuple]:
    """`(connector_keys, metric_key, location_key)` of a `pipe_ref`, or `None` (hidden or no Pipe)."""
    from meerschaum.api.dash.engine import is_hidden_ref, split_ref
    reference = str(reference or '')
    if is_hidden_ref(reference) or reference.count('|') < 3:
        return None
    return split_ref(reference)[1:]


def pipe_label(reference) -> str:
    """
    A Pipe as Python writes it, `Pipe('plugin:noaa', 'weather')` (the page is one instance's, so
    no `instance=`): the text the breakdown searches and sorts.
    """
    keys = pipe_keys(reference)
    if keys is None:
        from meerschaum.api.dash.engine import ref_label
        return ref_label(str(reference)) if reference else 'Unknown Pipe'
    return 'Pipe(' + ', '.join(repr(str(key)) for key in keys if key is not None) + ')'


def pipe_repr_parts(reference) -> Optional[List[List[str]]]:
    """
    `pipe_label()` as `[text, kind]` pieces for `assets/performance.js`, which colors them with the
    Pipes page's `mrsm-repr-*` classes (`pipes_table.pipe_title`).
    """
    keys = pipe_keys(reference)
    if keys is None:
        return None
    connector, metric, location = keys
    parts = [['Pipe(', 'type'], [repr(str(connector)), 'connector'],
             [', ', 'punctuation'], [repr(str(metric)), 'metric']]
    if location is not None:
        parts += [[', ', 'punctuation'], [repr(str(location)), 'location']]
    return parts + [[')', 'type']]


def pipe_title(reference) -> Any:
    """`pipe_label()` colored like the Pipes page (`pipes_table.pipe_title`)."""
    from dash import html
    keys = pipe_keys(reference)
    if keys is None:
        return html.Span(pipe_label(reference))
    from meerschaum.api.dash.pipes_table import pipe_title as colored
    return colored(*keys)


def _stat(label, value, detail=None):
    from dash import html
    return html.Div([html.Dt(label), html.Dd(value),
                     html.Small(detail, className='text-muted') if detail else None],
                    className='performance-stat')


def build_summary(summary: Dict[str, Any], complete=True):
    from dash import html
    success = summary.get('success_rate')
    return html.Dl([
        _stat('Syncs' if complete else 'Sampled syncs', number(summary.get('syncs', 0))),
        _stat('Failures', number(summary.get('failed', 0)),
              (number(float(success) * 100, 1) + '% succeeded') if success is not None else None),
        _stat('Average duration', duration(summary.get('avg_duration_seconds'))),
        _stat('Rows affected/s', number(summary.get('rows_per_second'), 1)),
        _stat('Rows affected', number(summary.get('rows', 0))),
    ], className='performance-stats')


#: The charts: `(key, title)`. Each graph's id is `{'type': 'performance-graph', 'chart': key}`,
#: inside a section `performance-<key>`.
CHARTS = (('avg_duration_seconds', 'Duration'), ('rows_per_second', 'Write rate'), ('results', 'Results'))


def chart_figures(data: Dict[str, Any], revision=None) -> Dict[str, Any]:
    """The charts' figures by key; a refresh sets them on the mounted graphs (`uirevision` kept)."""
    import plotly.graph_objects as go
    series = data.get('series') or []
    x = [item.get('ts') for item in series]
    bar_width = {}
    if data.get('begin') and data.get('end'):
        start = datetime.fromisoformat(str(data['begin']).replace('Z', '+00:00'))
        end = datetime.fromisoformat(str(data['end']).replace('Z', '+00:00'))
        width = (end - start).total_seconds() / (data.get('buckets') or METRICS_BUCKETS)
        x = [datetime.fromisoformat(str(value).replace('Z', '+00:00')) + timedelta(seconds=width / 2) for value in x]
        bar_width['width'] = width * 1000 * 0.85
    layout = {
        'template': 'plotly_dark', 'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)', 'height': 280,
        'margin': {'l': 54, 'r': 18, 't': 32, 'b': 50},
        'xaxis': {'type': 'date', 'title': {'text': 'Time (UTC)'}, 'gridcolor': 'rgba(255,255,255,0.07)',
                  **({'range': [data['begin'], data['end']]} if data.get('begin') and data.get('end') else {})},
        'yaxis': {'rangemode': 'tozero', 'gridcolor': 'rgba(255,255,255,0.07)'},
        'legend': {'orientation': 'h', 'y': 1.15}, 'hovermode': 'x unified',
        'hoverlabel': {'bgcolor': '#24282d', 'bordercolor': '#5a6169',
                       'font': {'color': '#e5edf3'}},
        'uirevision': revision,
    }
    figures = {}
    for key, title, axis, color in (
        ('avg_duration_seconds', 'Duration', 'Seconds / sync', '#85caff'),
        ('rows_per_second', 'Write rate', 'Rows affected/s', '#6bc98d'),
    ):
        figures[key] = go.Figure({
            'data': [{'type': 'scatter', 'mode': 'lines+markers', 'x': x,
                      'y': [item.get(key) for item in series], 'name': title,
                      'line': {'color': color}, 'connectgaps': False}],
            'layout': {**layout, 'yaxis': {**layout['yaxis'], 'title': {'text': axis}}},
        })
    counts = max((item.get('syncs', 0) for item in series), default=0)
    figures['results'] = go.Figure({'data': [
        {'type': 'bar', 'x': x, 'y': [item.get(key, 0) for item in series],
         'name': title, 'marker': {'color': color}, **bar_width}
        for key, title, color in (('succeeded', 'Succeeded', '#6bc98d'), ('failed', 'Failed', '#ec7a7a'))],
        'layout': {**layout, 'barmode': 'stack',
                   'yaxis': {**layout['yaxis'], 'title': {'text': 'Syncs'},
                             'tickformat': ',d', 'nticks': 5,
                             **({'dtick': 1} if counts <= 8 else {})}}})
    return figures


def build_charts(data: Dict[str, Any], revision=None):
    from dash import html, dcc
    figures = chart_figures(data, revision)
    return html.Div([
        html.Section([html.H2(title), dcc.Graph(
            id={'type': 'performance-graph', 'chart': key}, figure=figures[key], responsive=True,
            style={'height': '280px'}, config={'displayModeBar': False, 'scrollZoom': False})],
            id='performance-' + key, className='performance-chart')
        for key, title in CHARTS
    ], className='performance-charts')


def live(part: str, children: Any = None):
    """
    A part of the page a refresh replaces in place (`callbacks/performance.py`): the stats, the
    coverage line, the notes and the recent syncs. Everything around it stays mounted, so the page
    keeps its height, its scroll position, the open tab and the breakdowns' search, rank and page.
    """
    from dash import html
    return html.Div(children, id={'type': 'performance-live', 'part': part}, className='performance-live')


def _table(headers, rows):
    from dash import html
    import dash_bootstrap_components as dbc
    return html.Div(dbc.Table([
        html.Thead(html.Tr([html.Th(header, scope='col') for header in headers])),
        html.Tbody(rows),
    ], class_name='console-table performance-table', size='sm', hover=True),
                    className='console-table-scroll')


def _scope(data: Dict[str, Any], instance: str, window: str) -> str:
    return f"{instance}|{window}|{data.get('_pipe_filter')}|{data.get('_pipeline_filter')}"


#: The breakdowns: `(field, plural, filter key, kind)`.
GROUPS = (('pipes', 'Pipes', 'pipe', 'Pipe'), ('pipelines', 'Pipelines', 'pipeline', 'Pipeline'))


def group_stores(data: Dict[str, Any], instance: str, window: str) -> Dict[str, Dict[str, Any]]:
    """
    The breakdowns' rows by field (`pipes`, `pipelines`), for the `performance-group-data` stores
    that `assets/performance.js` searches, ranks and pages in the browser.
    """
    stores = {}
    scope = _scope(data, instance, window)
    filters = {key: chosen for key, chosen in (
        ('pipe', data.get('_pipe_filter')), ('pipeline', data.get('_pipeline_filter'))
    ) if chosen}
    for field, title, filter_key, kind in GROUPS:
        groups = data.get(field) or []
        if not groups:
            continue
        rows = []
        for group in groups:
            value = group.get('pipe_ref') if field == 'pipes' else group.get('name')
            if not isinstance(value, str) or not value:
                continue
            label = pipe_label(value) if field == 'pipes' else value
            summary = group.get('summary') or {}
            link = '/dash/performance?' + urlencode({'instance': instance, 'window': window, **filters, filter_key: value})
            rows.append({'label': label, 'key': value, 'href': link,
                         'repr': pipe_repr_parts(value) if field == 'pipes' else None,
                         'running': group.get('running', 0),
                         'sort': summary,
                         'cells': [number(summary.get('syncs', 0)), number(summary.get('failed', 0)),
                                   duration(summary.get('avg_duration_seconds')),
                                   duration(summary.get('duration_seconds')),
                                   number(summary.get('rows', 0)), number(summary.get('rows_per_second'), 1)]})
        headers = [kind, 'Syncs' if field == 'pipes' else 'Runs', 'Failures', 'Avg. duration',
                   'Total time', 'Rows affected', 'Rows affected/s']
        stores[field] = {'rows': rows, 'headers': headers, 'kind': kind, 'plural': title, 'scope': scope}
    return stores


def _pipelines_note(data: Dict[str, Any]):
    from dash import html
    return (html.Small('Recent run totals; refine the time window for more detail.', className='text-muted')
            if data.get('pipelines_truncated') else None)


def build_groups(data: Dict[str, Any], instance: str, window: str):
    from dash import html, dcc
    import dash_bootstrap_components as dbc
    sections = []
    stores = group_stores(data, instance, window)
    for field, title, _, kind in GROUPS:
        if field not in stores:
            continue
        def identity(part):
            return {'type': 'performance-group-' + part, 'group': field}
        scope = stores[field]['scope']
        sections.append(html.Section([
            dcc.Store(id=identity('data'), data=stores[field]),
            html.Div([
                html.Label([html.Span('Find a ' + kind, className='visually-hidden'),
                    dbc.Input(id=identity('search'), type='search', placeholder='Find a ' + kind,
                              persistence=scope, persistence_type='memory')]),
                html.Label([html.Span('Rank ' + title, className='visually-hidden'),
                    dbc.Select(id=identity('sort'), value='duration_seconds', persistence=scope, persistence_type='memory', options=[
                    {'label': label, 'value': key} for key, label in (
                        ('duration_seconds', 'Most total time'), ('failed', 'Most failures'),
                        ('avg_duration_seconds', 'Slowest syncs'), ('syncs', 'Most syncs' if field == 'pipes' else 'Most runs'),
                        ('rows', 'Most rows affected'), ('name', 'Name'))])]),
            ], className='performance-group-tools'),
            html.P(id=identity('range'), className='text-muted small', **{'aria-live': 'polite'}),
            html.Div(id=identity('body'), className='performance-group-table-wrap'),
            dbc.Pagination(id=identity('page'), active_page=1, max_value=1,
                           first_last=True, previous_next=True, fully_expanded=False, size='sm'),
            live('pipelines-note', _pipelines_note(data)) if field == 'pipelines' else None,
        ], id='performance-' + field, className='performance-section performance-breakdown'))
    return sections


def history_window(data: Dict[str, Any], page=1, page_size=HISTORY_PAGE_SIZE):
    """A bounded page of the freshly authorized sample; never an offset into unfiltered data."""
    history = (data.get('history') or [])[:METRICS_LIMIT]
    try:
        page_size = int(page_size)
    except (TypeError, ValueError, OverflowError):
        page_size = HISTORY_PAGE_SIZE
    page_size = page_size if page_size in (10, 25, 50) else HISTORY_PAGE_SIZE
    pages = max(1, (len(history) + page_size - 1) // page_size)
    try:
        page = max(1, min(pages, int(page or 1)))
    except (TypeError, ValueError, OverflowError):
        page = 1
    start = (page - 1) * page_size
    return history[start:start + page_size], page, pages, len(history)


def build_history(data: Dict[str, Any], page=1, page_size=HISTORY_PAGE_SIZE):
    from dash import html, dcc
    history, page, pages, total = history_window(data, page, page_size)
    if not history:
        return None
    rows = []
    headers = ['Started', 'Pipe', 'Result', 'Duration', 'Rows affected']
    for item in history:
        reference = item.get('pipe_ref') or ''
        keys = pipe_keys(reference)
        href = ('/dash/pipes/' + '/'.join(quote(str(key), safe='') for key in keys if key is not None)
                + '?' + urlencode({'instance': reference.split('|', 1)[0]})) if keys else None
        title = pipe_title(reference)
        name = dcc.Link(title, href=href, className='console-name-link') if href else title
        rows.append(html.Tr([
            html.Td(timestamp(item.get('started') or item.get('ts')), **{'data-label': 'Started'}),
            html.Td(name, className='performance-row-name', **{'data-label': 'Pipe'}),
            html.Td('Succeeded' if item.get('success') else 'Failed',
                    className='text-success' if item.get('success') else 'text-danger', **{'data-label': 'Result'}),
            html.Td(duration(item.get('duration_seconds')), **{'data-label': 'Duration'}),
            html.Td(number(item.get('rows', 0)), **{'data-label': 'Rows affected'}),
        ]))
    start = (page - 1) * (int(page_size) if str(page_size) in ('10', '25', '50') else HISTORY_PAGE_SIZE) + 1
    return html.Section([
        html.H2('Recent syncs'),
        html.P(f'{start}–{start + len(history) - 1} of {number(total)} recent syncs',
               className='text-muted small', id='performance-history-range'),
        _table(headers, rows),
    ], className='performance-section', id='performance-history')


def _complete(data: Dict[str, Any]) -> bool:
    return bool(data.get('summary_complete', not data.get('truncated')))


def _empty(data: Dict[str, Any]) -> bool:
    return not (data.get('summary') or {}).get('syncs') and not data.get('pipelines')


def body_shape(data: Dict[str, Any], instance: str, window: str) -> List[Any]:
    """
    What `build_body()` lays out for this data: the selection, and the empty state or the
    breakdowns it has. A refresh with the same shape updates the mounted page in place
    (`live_parts()`, `chart_figures()`, `group_stores()`); any other change rebuilds it.
    """
    scope = _scope(data, instance, window)
    if _empty(data):
        return [scope, 'empty', _complete(data)]
    return [scope, 'body', [field for field, *_ in GROUPS if data.get(field)]]


def live_parts(data: Dict[str, Any], history_page=1, history_page_size=HISTORY_PAGE_SIZE) -> Dict[str, Any]:
    """The children of each `live()` part of `build_body()`."""
    from dash import html
    summary = data.get('summary') or {}
    complete = _complete(data)
    notes = []
    if not complete:
        notes.append(LIMITED_NOTE)
    if data.get('retention_days'):
        notes.append(f"History is retained for {number(data['retention_days'])} days.")
    coverage = ('Totals and trends cover all recorded syncs in the selected window.' if complete else
                f"Totals and trends cover {number(summary.get('syncs', 0))} sampled syncs.")
    if data.get('history_truncated', data.get('truncated')):
        coverage += f" Recent history shows the latest {number(len(data.get('history') or []))}."
    return {
        'stats': build_summary(summary, complete),
        'coverage': html.P(coverage, id='performance-coverage', className='text-muted small performance-note'),
        'history': build_history(data, history_page, history_page_size),
        'notes': html.P(' '.join(notes), className='text-muted small') if notes else None,
        'pipelines-note': _pipelines_note(data),
    }


def build_body(data: Dict[str, Any], instance: str, window: str, history_page=1, history_page_size=HISTORY_PAGE_SIZE):
    from dash import html
    import dash_bootstrap_components as dbc
    if _empty(data):
        complete = _complete(data)
        return html.Div([html.H3('No syncs in this sample' if not complete else 'No syncs in this window'),
                         html.P(LIMITED_NOTE if not complete else
                                'Run a Pipe or Pipeline, or choose a longer window.')],
                        className='console-empty', id='performance-empty')
    parts = live_parts(data, history_page, history_page_size)
    groups = {section.id: section for section in build_groups(data, instance, window)}
    return [live('stats', parts['stats']),
            live('coverage', parts['coverage']),
            html.Details([html.Summary('About these measurements'),
                html.P('Rows affected includes inserts, updates, upserts, and deletions; an unchanged sync affects zero rows. '
                       'Rows affected/s divides by measured sync time, including concurrent sync durations.')],
                className='performance-measurements text-muted small'),
            build_charts(data, _scope(data, instance, window)),
            dbc.Tabs([
                dbc.Tab(groups.get('performance-pipes', html.P('No Pipe syncs in this window.')),
                        label='Pipes', tab_id='performance-pipes-tab'),
                dbc.Tab(groups.get('performance-pipelines', html.P('No Pipeline runs in this window.')),
                        label='Pipelines', tab_id='performance-pipelines-tab'),
                dbc.Tab(live('history', parts['history']),
                        label='Recent syncs', tab_id='performance-history-tab'),
            ], id='performance-detail-tabs', active_tab='performance-pipes-tab',
                persistence=True, persistence_type='memory'),
            live('notes', parts['notes'])]


#: The Engine section's window: the same as the sync measurements'.
ENGINE_BUCKETS = 48
#: The most runs, nodes and ledger entries the Engine section reads, each.
ENGINE_LIMIT = 20_000
#: The most rows each Engine table shows (the Pipe filter narrows to one pipe).
ENGINE_ROWS = 10
#: Node statuses in the Nodes table, in plain words.
NODE_STATUS_LABELS = {
    'failed': 'Failed', 'upstream_failed': 'Parent failed', 'rebuild_pending': 'Rebuild needs approval',
    'pending': 'Not run', 'succeeded': 'Succeeded', 'skipped': 'Skipped', 'locked': 'Locked',
    'observed': 'Observed',
}


def engine_health(data: Dict[str, Any], window='24h') -> Dict[str, Any]:
    """
    The Engine section's data for the selection `metrics()` just authorized: the same instance,
    window, Pipe and Pipeline filters and the same fresh visibility ceiling, read natively
    (`EngineReader::health`). Planner reasons naming a Pipe the viewer may not see are
    anonymized here.
    """
    from meerschaum.api.dash import engine
    instance = engine.get_instance(data.get('_instance'), strict=True)
    end = datetime.now(timezone.utc)
    health = engine.get_engine_health(
        instance, since=end - timedelta(hours=WINDOW_HOURS[window]), until=end,
        buckets=ENGINE_BUCKETS, pipe_ref=data.get('_pipe_filter'),
        pipeline=data.get('_pipeline_filter'), visible_refs=data.get('_visible_refs'),
        limit=ENGINE_LIMIT,
    )
    viewer = data.get('_viewer')
    if data.get('_visible_refs') is not None and health.get('nodes'):
        anonymizer = engine.Anonymizer(viewer, instance, refs=[])
        for node in health['nodes']:
            node['reason'] = anonymizer.text(node.get('reason'))
    return {**health, '_instance': str(instance), '_window': window,
            '_pipe_filter': data.get('_pipe_filter'), '_pipeline_filter': data.get('_pipeline_filter'),
            '_pipelines': set(data.get('_pipeline_names') or ())}


def elapsed(value: Any) -> str:
    """Seconds as the largest whole unit that reads naturally: `45 s`, `12 min`, `3.2 h`, `4.1 d`."""
    try:
        seconds = float(value)
    except (TypeError, ValueError, OverflowError):
        return 'n/a'
    if not math.isfinite(seconds) or seconds < 0:
        return 'n/a'
    if seconds < 60:
        return f'{seconds:,.0f} s'
    if seconds < 3600:
        return f'{seconds / 60:,.0f} min'
    if seconds < 86400:
        return f'{seconds / 3600:,.1f} h'
    return f'{seconds / 86400:,.1f} d'


def _ago(value: Any, now: Optional[datetime] = None) -> str:
    if not value:
        return 'Never'
    try:
        when = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except ValueError:
        return 'n/a'
    when = when if when.tzinfo else when.replace(tzinfo=timezone.utc)
    return elapsed(((now or datetime.now(timezone.utc)) - when).total_seconds()) + ' ago'


def _engine_empty(health: Dict[str, Any]) -> bool:
    return not ((health.get('runs') or {}).get('runs') or (health.get('freshness') or {}).get('pipes')
                or (health.get('backlog') or {}).get('children') or health.get('nodes'))


def engine_shape(health: Optional[Dict[str, Any]]) -> Optional[List[Any]]:
    """What `build_engine()` lays out for this data (`body_shape()`'s counterpart)."""
    if health is None:
        return None
    scope = '|'.join(str(health.get(key)) for key in ('_instance', '_window', '_pipe_filter', '_pipeline_filter'))
    return [scope, 'empty' if _engine_empty(health) else 'body']


def _pipe_cell(reference: Any, label='Pipe') -> Any:
    from dash import html, dcc
    keys = pipe_keys(reference)
    title = pipe_title(reference)
    if keys is None:
        return html.Td(title, className='performance-row-name', **{'data-label': label})
    from meerschaum.api.dash.engine import pipe_href
    return html.Td(dcc.Link(title, href=pipe_href(str(reference)), className='console-name-link'),
                   className='performance-row-name', **{'data-label': label})


def _run_link(run_id: str, started: Any, name: Optional[str], health: Dict[str, Any]) -> Any:
    """A run's start, linked to the run on its saved Pipeline's page when the viewer may see it."""
    from dash import dcc
    text = timestamp(started)
    if not name or name not in health.get('_pipelines', ()):
        return text
    return dcc.Link(text, href='/dash/pipelines/' + quote(str(name), safe='') + '?' + urlencode(
        {'run_id': run_id, 'instance': health.get('_instance')}), className='console-name-link')


def _engine_table(headers, rows, total, nouns, section):
    """A table of at most `ENGINE_ROWS` rows under its count; `nouns`: `(singular, plural)`."""
    from dash import html
    noun = nouns[0] if total == 1 else nouns[1]
    if not rows:
        return html.P('No ' + nouns[1] + '.', className='text-muted performance-engine-none')
    return [
        html.P(f'{len(rows)} of {number(total)} {noun}' if total > len(rows) else f'{number(total)} {noun}',
               className='text-muted small', id=f'performance-engine-{section}-range'),
        _table(headers, rows),
    ]


def build_engine_stats(health: Dict[str, Any]):
    from dash import html
    runs = health.get('runs') or {}
    fresh = health.get('freshness') or {}
    return html.Dl([
        _stat('Runs', number(runs.get('runs', 0))),
        _stat('Failed runs', number(runs.get('failed', 0))),
        _stat('Unfinished runs', number(runs.get('unfinished', 0))),
        _stat('P95 run duration', duration(runs.get('p95_seconds'))),
        _stat('Pipes failing', number(fresh.get('failing', 0))),
        _stat('Never synced', number(fresh.get('never_succeeded', 0))),
        _stat('Longest since success', elapsed(fresh.get('longest_since_success_seconds'))),
        _stat('Children behind', number(fresh.get('behind', 0))),
    ], className='performance-stats performance-engine-stats')


def build_freshness(health: Dict[str, Any]):
    from dash import html
    pipes = (health.get('freshness') or {}).get('pipes') or []
    now = datetime.now(timezone.utc)
    rows = []
    for item in pipes[:ENGINE_ROWS]:
        lag, pending = item.get('lag_seconds'), item.get('pending') or 0
        rows.append(html.Tr([
            _pipe_cell(item.get('pipe_ref')),
            html.Td('Succeeded' if item.get('success') else 'Failed',
                    className='text-success' if item.get('success') else 'text-danger',
                    **{'data-label': 'Latest sync'}),
            html.Td(_ago(item.get('last_success'), now), title=timestamp(item.get('last_success')),
                    **{'data-label': 'Last success'}),
            html.Td(number(item.get('consecutive_failures') or 0),
                    className='text-danger' if item.get('consecutive_failures') else None,
                    **{'data-label': 'Failures in a row'}),
            html.Td('Caught up' if lag == 0 and not pending else elapsed(lag) if lag is not None else '—',
                    **{'data-label': 'Behind parents'}),
            html.Td(number(pending), **{'data-label': 'Pending ranges'}),
        ]))
    return _engine_table(['Pipe', 'Latest sync', 'Last success', 'Failures in a row', 'Behind parents',
                          'Pending ranges'], rows, len(pipes), ('Pipe', 'Pipes'), 'freshness')


def build_backlog(health: Dict[str, Any]):
    from dash import html
    from meerschaum.api.dash.engine import bound_label
    children = (health.get('backlog') or {}).get('children') or []
    rows = []
    for item in children[:ENGINE_ROWS]:
        child = item.get('child_ref')
        begin, end = item.get('dirty_begin'), item.get('dirty_end')
        rows.append(html.Tr([
            _pipe_cell(child, 'Child') if child else html.Td(
                'No child yet', className='performance-row-name text-muted', **{'data-label': 'Child'}),
            html.Td([html.Div(pipe_title(parent)) for parent in item.get('parents') or []],
                    className='performance-engine-parents', **{'data-label': 'Parents'}),
            html.Td(number(item.get('pending') or 0), **{'data-label': 'Pending ranges'}),
            html.Td(timestamp(item.get('oldest_pending')), **{'data-label': 'Oldest pending'}),
            html.Td(elapsed(item.get('waited_seconds')), **{'data-label': 'Waited'}),
            html.Td((bound_label(begin) if begin is not None else '…') + ' → '
                    + (bound_label(end) if end is not None else '…'), **{'data-label': 'Dirty range'}),
        ]))
    return _engine_table(['Child', 'Parents', 'Pending ranges', 'Oldest pending', 'Waited', 'Dirty range'],
                         rows, len(children), ('child with a backlog', 'children with a backlog'), 'backlog')


def build_nodes(health: Dict[str, Any]):
    from dash import html
    from meerschaum.api.dash.engine import action_label
    nodes = health.get('nodes') or []
    rows = []
    for item in nodes[:ENGINE_ROWS]:
        status = str(item.get('status') or '')
        bad = status in ('failed', 'upstream_failed')
        rows.append(html.Tr([
            _pipe_cell(item.get('pipe_ref')),
            html.Td(_run_link(item.get('run_id'), item.get('run_started'),
                              _run_name(item.get('run_id')), health), **{'data-label': 'Run started'}),
            html.Td(action_label(str(item.get('action') or '')), **{'data-label': 'Action'}),
            html.Td(NODE_STATUS_LABELS.get(status, status.replace('_', ' ').capitalize()),
                    className='text-danger' if bad else None, **{'data-label': 'Status'}),
            html.Td(number(item.get('attempts') or 0), **{'data-label': 'Attempts'}),
            html.Td(duration(item.get('duration_seconds')), **{'data-label': 'Duration'}),
            html.Td(item.get('reason') or '—', className='performance-engine-reason',
                    **{'data-label': 'Planner reason'}),
        ]))
    return _engine_table(['Pipe', 'Run started', 'Action', 'Status', 'Attempts', 'Duration', 'Planner reason'],
                         rows, len(nodes), ('failed or retried node', 'failed or retried nodes'), 'nodes')


def _run_name(run_id: Any) -> Optional[str]:
    """The run name of an engine run ID (`name-YYYY-MM-DDTHH-MM-SS.ffffffZ-xxxxxxxx`)."""
    import re
    match = re.fullmatch(r'(.+)-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.\d{6}Z-[0-9a-fA-F]{8}', str(run_id or ''))
    return match.group(1) if match else None


def build_recent_runs(health: Dict[str, Any]):
    from dash import html
    runs = (health.get('runs') or {}).get('recent') or []
    rows = []
    for item in runs:
        status = item.get('status')
        name = item.get('name')
        rows.append(html.Tr([
            html.Td(_run_link(item.get('run_id'), item.get('started'), name, health),
                    className='performance-row-name', **{'data-label': 'Started'}),
            html.Td(name if name in health.get('_pipelines', ()) else '—', **{'data-label': 'Pipeline'}),
            html.Td({'succeeded': 'Succeeded', 'failed': 'Failed'}.get(status, 'Running'),
                    className={'succeeded': 'text-success', 'failed': 'text-danger'}.get(status),
                    **{'data-label': 'Result'}),
            html.Td(duration(item.get('duration_seconds')), **{'data-label': 'Duration'}),
            html.Td(number(item.get('num_nodes') or 0), **{'data-label': 'Pipes'}),
        ]))
    return _engine_table(['Started', 'Pipeline', 'Result', 'Duration', 'Pipes'], rows,
                         (health.get('runs') or {}).get('runs', 0), ('run', 'runs'), 'runs')


def engine_figures(health: Dict[str, Any], revision=None) -> Dict[str, Any]:
    """The Engine charts' figures by key (`engine-runs`, `engine-duration`)."""
    import plotly.graph_objects as go
    runs = health.get('runs') or {}
    buckets = runs.get('buckets') or []
    width = float(runs.get('bucket_seconds') or 1)
    x = [datetime.fromisoformat(str(item['start']).replace('Z', '+00:00')) + timedelta(seconds=width / 2)
         for item in buckets]
    span = ([buckets[0]['start'], (datetime.fromisoformat(str(buckets[-1]['start']).replace('Z', '+00:00'))
                                   + timedelta(seconds=width)).isoformat()] if buckets else None)
    layout = {
        'template': 'plotly_dark', 'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)', 'height': 280,
        'margin': {'l': 54, 'r': 18, 't': 32, 'b': 50},
        'xaxis': {'type': 'date', 'title': {'text': 'Time (UTC)'}, 'gridcolor': 'rgba(255,255,255,0.07)',
                  **({'range': span} if span else {})},
        'yaxis': {'rangemode': 'tozero', 'gridcolor': 'rgba(255,255,255,0.07)'},
        'legend': {'orientation': 'h', 'y': 1.15}, 'hovermode': 'x unified',
        'hoverlabel': {'bgcolor': '#24282d', 'bordercolor': '#5a6169', 'font': {'color': '#e5edf3'}},
        'uirevision': revision,
    }
    most = max((item.get('succeeded', 0) + item.get('failed', 0) + item.get('unfinished', 0)
                for item in buckets), default=0)
    outcomes = go.Figure({'data': [
        {'type': 'bar', 'x': x, 'y': [item.get(key, 0) for item in buckets], 'name': title,
         'marker': {'color': color}, 'width': width * 1000 * 0.85}
        for key, title, color in (('succeeded', 'Succeeded', '#6bc98d'), ('failed', 'Failed', '#ec7a7a'),
                                  ('unfinished', 'Unfinished', '#e0a526'))],
        'layout': {**layout, 'barmode': 'stack',
                   'yaxis': {**layout['yaxis'], 'title': {'text': 'Runs'}, 'tickformat': ',d', 'nticks': 5,
                             **({'dtick': 1} if most <= 8 else {})}}})
    durations = go.Figure({'data': [
        {'type': 'scatter', 'mode': 'lines+markers', 'x': x, 'y': [item.get(key) for item in buckets],
         'name': title, 'line': {'color': color, **({'dash': 'dot'} if key == 'max_seconds' else {})},
         'connectgaps': False}
        for key, title, color in (('average_seconds', 'Average', '#85caff'), ('p95_seconds', 'P95', '#c39bf0'),
                                  ('max_seconds', 'Maximum', '#9aa5b1'))],
        'layout': {**layout, 'yaxis': {**layout['yaxis'], 'title': {'text': 'Seconds / run'}}}})
    return {'engine-runs': outcomes, 'engine-duration': durations}


#: The Engine charts: `(key, title)`, as `CHARTS`.
ENGINE_CHARTS = (('engine-runs', 'Runs by outcome'), ('engine-duration', 'Run duration'))


def engine_live_parts(health: Dict[str, Any]) -> Dict[str, Any]:
    """The children of each `live()` part of `build_engine()`."""
    return {
        'engine-stats': build_engine_stats(health),
        'engine-freshness': build_freshness(health),
        'engine-backlog': build_backlog(health),
        'engine-nodes': build_nodes(health),
        'engine-recent': build_recent_runs(health),
        'engine-note': _engine_note(health),
    }


def _engine_note(health: Dict[str, Any]):
    from dash import html
    return (html.P(f'Engine totals cover the newest {number(ENGINE_LIMIT)} runs and nodes.',
                   className='text-muted small') if health.get('truncated') else None)


def build_engine(health: Optional[Dict[str, Any]], problem: Optional[str] = None):
    """The Engine section: the Syncing Engine's runs, freshness, backlog and failed nodes."""
    from dash import html, dcc
    import dash_bootstrap_components as dbc
    heading = html.H2('Engine', id='performance-engine-heading')
    if problem or health is None:
        return [heading, dbc.Alert(problem or 'Engine health could not be loaded. Try Refresh.', color='warning')]
    if _engine_empty(health):
        return [heading, html.Div(html.H3('No engine history'), className='console-empty',
                                  id='performance-engine-empty')]
    parts = engine_live_parts(health)
    figures = engine_figures(health, engine_shape(health)[0])
    return [
        heading,
        live('engine-stats', parts['engine-stats']),
        html.Div([
            html.Section([html.H3(title), dcc.Graph(
                id={'type': 'performance-graph', 'chart': key}, figure=figures[key], responsive=True,
                style={'height': '280px'}, config={'displayModeBar': False, 'scrollZoom': False})],
                id='performance-' + key, className='performance-chart')
            for key, title in ENGINE_CHARTS
        ], className='performance-charts performance-engine-charts'),
        dbc.Tabs([
            dbc.Tab(live('engine-freshness', parts['engine-freshness']), label='Freshness',
                    tab_id='performance-engine-freshness-tab'),
            dbc.Tab(live('engine-backlog', parts['engine-backlog']), label='Backlog',
                    tab_id='performance-engine-backlog-tab'),
            dbc.Tab(live('engine-nodes', parts['engine-nodes']), label='Nodes',
                    tab_id='performance-engine-nodes-tab'),
            dbc.Tab(live('engine-recent', parts['engine-recent']), label='Runs',
                    tab_id='performance-engine-runs-tab'),
        ], id='performance-engine-tabs', active_tab='performance-engine-freshness-tab',
            persistence=True, persistence_type='memory'),
        live('engine-note', parts['engine-note']),
    ]


def build_overview_card():
    """A lazy Overview tile; its callback is independent of the other tile queries."""
    from dash import html, dcc
    from meerschaum.api.dash import access
    if not access.current_viewer().can('pipes:read'):
        return None
    return html.Section([
        html.H2(dcc.Link('Performance', href='/dash/performance', className='overview-tile-title'),
                className='overview-tile-heading'),
        html.Div('Loading performance…', id='overview-performance-body',
                 className='overview-tile-body', role='status'),
        dcc.Store(id='overview-performance-digest'),
    ], id='overview-performance', className='overview-tile overview-tile-performance')


def build_overview_body(data: Dict[str, Any], instance: str):
    from dash import html, dcc
    summary = data.get('summary') or {}
    complete = data.get('summary_complete', not data.get('truncated'))
    href = '/dash/performance?' + urlencode({'instance': instance})
    if not summary.get('syncs'):
        return [html.P('No syncs in this sample.' if not complete else
                       'No syncs in the last 24 hours.', className='text-muted'),
                html.Small('This view is limited; refine its filters in Performance.', className='text-muted')
                if not complete else None,
                dcc.Link('View performance →', href=href, className='overview-more')]
    return [html.Dl([
        _stat('Syncs · 24 hours' if complete else 'Sampled syncs · 24 hours', number(summary.get('syncs', 0))),
        _stat('Failures', number(summary.get('failed', 0))),
        _stat('Average duration', duration(summary.get('avg_duration_seconds'))),
        _stat('Rows affected/s', number(summary.get('rows_per_second'), 1)),
    ], className='performance-stats performance-overview-stats'),
        html.Small('This view is limited; refine its filters in Performance.', className='text-muted')
        if not complete else None,
        dcc.Link('View performance →', href=href, className='overview-more')]
