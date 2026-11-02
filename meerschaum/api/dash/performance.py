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
