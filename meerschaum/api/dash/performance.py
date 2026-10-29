"""Permission-filtered sync measurements and the Performance page's presentation helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, parse_qs, urlparse

from meerschaum.utils.typing import Any, Dict, Optional

WINDOWS = (('1h', 'Last hour'), ('24h', 'Last 24 hours'), ('7d', 'Last 7 days'),
           ('30d', 'Last 30 days'), ('all', 'All retained history'))
WINDOW_HOURS = {'1h': 1, '24h': 24, '7d': 168, '30d': 720, 'all': 720}
METRICS_LIMIT = 1000
METRICS_BUCKETS = 48
HISTORY_PAGE_SIZE = 50
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
        pipe_ref=pipe or None, pipeline=pipeline or None, limit=METRICS_LIMIT, buckets=METRICS_BUCKETS,
        visible_refs=(sorted(refs) if refs is not None else None),
        visible_pipelines=(sorted(names) if names is not None else None),
    )
    return {**result, '_instance': str(instance),
            '_pipe_options': ([{'label': engine.ref_label(ref), 'value': ref}
                               for ref in sorted(available_refs)] if with_options else []),
            '_pipeline_options': ([{'label': name, 'value': name}
                                   for name in sorted(available_names)] if with_options else [])}


def number(value: Any, digits=0) -> str:
    try:
        return f'{float(value):,.{digits}f}'
    except (TypeError, ValueError):
        return '—'


def duration(value: Any) -> str:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return '—'
    if seconds < 1:
        return f'{seconds * 1000:,.0f} ms'
    if seconds < 60:
        return f'{seconds:,.2f} s'
    return f'{seconds / 60:,.1f} min'


def timestamp(value: Any) -> str:
    if not value:
        return '—'
    try:
        value = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    except ValueError:
        return '—'


def _stat(label, value, detail=None):
    from dash import html
    return html.Div([html.Dt(label), html.Dd(value),
                     html.Small(detail, className='text-muted') if detail else None],
                    className='performance-stat')


def build_summary(summary: Dict[str, Any]):
    from dash import html
    success = summary.get('success_rate')
    return html.Dl([
        _stat('Syncs', number(summary.get('syncs', 0))),
        _stat('Failures', number(summary.get('failed', 0)),
              (number(float(success) * 100, 1) + '% succeeded') if success is not None else None),
        _stat('Average duration', duration(summary.get('avg_duration_seconds'))),
        _stat('Rows affected/s', number(summary.get('rows_per_second'), 1)),
        _stat('Rows affected', number(summary.get('rows', 0))),
    ], className='performance-stats')


def build_charts(data: Dict[str, Any], revision=None):
    from dash import html, dcc
    import plotly.graph_objects as go
    series = data.get('series') or []
    x = [item.get('ts') for item in series]
    layout = {
        'template': 'plotly_dark', 'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)', 'height': 280,
        'margin': {'l': 54, 'r': 18, 't': 32, 'b': 50},
        'xaxis': {'type': 'date', 'title': {'text': 'Time (UTC)'}, 'gridcolor': 'rgba(255,255,255,0.07)',
                  **({'range': [data['begin'], data['end']]} if data.get('begin') and data.get('end') else {})},
        'yaxis': {'rangemode': 'tozero', 'gridcolor': 'rgba(255,255,255,0.07)'},
        'legend': {'orientation': 'h', 'y': 1.15}, 'hovermode': 'x unified',
        'uirevision': revision,
    }
    graphs = []
    for key, title, axis, color in (
        ('avg_duration_seconds', 'Duration', 'Seconds / sync', '#85caff'),
        ('rows_per_second', 'Write rate', 'Rows affected/s', '#6bc98d'),
    ):
        figure = {'data': [{'type': 'scatter', 'mode': 'lines+markers', 'x': x,
                            'y': [item.get(key) for item in series], 'name': title,
                            'line': {'color': color}, 'connectgaps': False}],
                  'layout': {**layout, 'yaxis': {**layout['yaxis'], 'title': {'text': axis}}}}
        graphs.append(html.Section([html.H2(title), dcc.Graph(
            id='performance-' + key, figure=go.Figure(figure), responsive=True, style={'height': '280px'},
            config={'displayModeBar': False, 'scrollZoom': False})], className='performance-chart'))
    counts = max((item.get('syncs', 0) for item in series), default=0)
    bar_width = {}
    if data.get('begin') and data.get('end'):
        start = datetime.fromisoformat(str(data['begin']).replace('Z', '+00:00'))
        end = datetime.fromisoformat(str(data['end']).replace('Z', '+00:00'))
        bar_width['width'] = (end - start).total_seconds() * 1000 / METRICS_BUCKETS * 0.85
    figure = {'data': [
        {'type': 'bar', 'x': x, 'y': [item.get(key, 0) for item in series],
         'name': title, 'marker': {'color': color}, **bar_width}
        for key, title, color in (('succeeded', 'Succeeded', '#6bc98d'), ('failed', 'Failed', '#ec7a7a'))],
        'layout': {**layout, 'barmode': 'stack',
                   'yaxis': {**layout['yaxis'], 'title': {'text': 'Syncs'},
                             'tickformat': ',d', 'nticks': 5,
                             **({'dtick': 1} if counts <= 8 else {})}}}
    graphs.append(html.Section([html.H2('Results'), dcc.Graph(
        id='performance-results', figure=go.Figure(figure), responsive=True, style={'height': '280px'},
        config={'displayModeBar': False, 'scrollZoom': False})], className='performance-chart'))
    return html.Div(graphs, className='performance-charts')


def _table(headers, rows):
    from dash import html
    import dash_bootstrap_components as dbc
    return html.Div(dbc.Table([
        html.Thead(html.Tr([html.Th(header, scope='col') for header in headers])),
        html.Tbody(rows),
    ], class_name='console-table performance-table', size='sm', hover=True),
                    className='console-table-scroll')


def build_groups(data: Dict[str, Any], instance: str, window: str):
    from dash import html, dcc
    from meerschaum.api.dash.engine import ref_label
    sections = []
    for field, title, filter_key in (('pipes', 'Pipes', 'pipe'), ('pipelines', 'Pipelines', 'pipeline')):
        groups = data.get(field) or []
        if not groups:
            continue
        rows = []
        for group in groups:
            value = group.get('pipe_ref') if field == 'pipes' else group.get('name')
            label = ref_label(value) if field == 'pipes' else value
            summary = group.get('summary') or {}
            link = '/dash/performance?' + urlencode({'instance': instance, 'window': window, filter_key: value})
            rows.append(html.Tr([
                html.Td(dcc.Link(label, href=link, className='console-name-link')),
                html.Td(number(summary.get('syncs', 0))),
                html.Td(number(summary.get('failed', 0))),
                html.Td(duration(summary.get('avg_duration_seconds'))),
                html.Td(number(summary.get('rows', 0))),
                html.Td(number(summary.get('rows_per_second'), 1)),
            ]))
        sections.append(html.Section([html.H2(title), _table(
            ['Pipe' if field == 'pipes' else 'Pipeline', 'Syncs' if field == 'pipes' else 'Runs',
             'Failures', 'Avg. duration', 'Rows affected', 'Rows affected/s'], rows)],
            className='performance-section'))
    return sections


def history_window(data: Dict[str, Any], page=1):
    """A bounded page of the freshly authorized sample; never an offset into unfiltered data."""
    history = (data.get('history') or [])[:METRICS_LIMIT]
    pages = max(1, (len(history) + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE)
    try:
        page = max(1, min(pages, int(page or 1)))
    except (TypeError, ValueError, OverflowError):
        page = 1
    start = (page - 1) * HISTORY_PAGE_SIZE
    return history[start:start + HISTORY_PAGE_SIZE], page, pages, len(history)


def build_history(data: Dict[str, Any], page=1):
    from dash import html
    from meerschaum.api.dash.engine import ref_label
    history, page, pages, total = history_window(data, page)
    if not history:
        return None
    rows = []
    for item in history:
        rows.append(html.Tr([
            html.Td(timestamp(item.get('started') or item.get('ts'))),
            html.Td(ref_label(item.get('pipe_ref') or '')),
            html.Td('Succeeded' if item.get('success') else 'Failed',
                    className='text-success' if item.get('success') else 'text-danger'),
            html.Td(duration(item.get('duration_seconds'))),
            html.Td(number(item.get('rows', 0))),
        ]))
    start = (page - 1) * HISTORY_PAGE_SIZE + 1
    return html.Section([
        html.H2('Recent syncs'),
        html.P(f'{start}–{start + len(history) - 1} of {number(total)} syncs in this sample',
               className='text-muted small', id='performance-history-range'),
        _table(['Started', 'Pipe', 'Result', 'Duration', 'Rows affected'], rows),
    ], className='performance-section', id='performance-history')


def build_body(data: Dict[str, Any], instance: str, window: str, history_page=1):
    from dash import html
    summary = data.get('summary') or {}
    if not summary.get('syncs') and not data.get('pipelines'):
        return html.Div([html.H3('No syncs in this sample' if data.get('truncated') else 'No syncs in this window'),
                         html.P(LIMITED_NOTE if data.get('truncated') else
                                'Run a Pipe or Pipeline, or choose a longer window.')],
                        className='console-empty', id='performance-empty')
    notes = []
    if data.get('truncated'):
        notes.append(LIMITED_NOTE)
    if data.get('retention_days'):
        notes.append(f"History is retained for {number(data['retention_days'])} days.")
    return [build_summary(summary),
            html.P('Counts include inserted, updated, upserted, and deleted rows. '
                   'Replacing data counts both deleted and new rows; an unchanged sync affects zero rows.',
                   className='text-muted small performance-note'),
            build_charts(data, f'{instance}|{window}'), *build_groups(data, instance, window),
            build_history(data, history_page),
            html.P(' '.join(notes), className='text-muted small') if notes else None]


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
    href = '/dash/performance?' + urlencode({'instance': instance})
    if not summary.get('syncs'):
        return [html.P('No syncs in this sample.' if data.get('truncated') else
                       'No syncs in the last 24 hours.', className='text-muted'),
                html.Small('This view is limited; refine its filters in Performance.', className='text-muted')
                if data.get('truncated') else None,
                dcc.Link('View performance →', href=href, className='overview-more')]
    return [html.Dl([
        _stat('Syncs · 24 hours', number(summary.get('syncs', 0))),
        _stat('Failures', number(summary.get('failed', 0))),
        _stat('Average duration', duration(summary.get('avg_duration_seconds'))),
        _stat('Rows affected/s', number(summary.get('rows_per_second'), 1)),
    ], className='performance-stats performance-overview-stats'),
        html.Small('This view is limited; refine its filters in Performance.', className='text-muted')
        if data.get('truncated') else None,
        dcc.Link('View performance →', href=href, className='overview-more')]
