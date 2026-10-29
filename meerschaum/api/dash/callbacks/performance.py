"""Lazy, independently refreshed Performance page and Overview card."""

import dash
from dash import Input, Output, State
from dash.exceptions import PreventUpdate, MissingCallbackContextException
import dash_bootstrap_components as dbc

from meerschaum.api.dash import dash_app, performance


def follow_performance_location(search, path):
    """A group link may change only the query string, leaving the page's path unchanged."""
    if str(path).rstrip('/') != '/dash/performance':
        raise PreventUpdate
    from meerschaum.api.dash.engine import get_instance
    selected = performance.selection(search)
    try:
        instance = str(get_instance(selected['instance'], strict=True))
    except Exception:
        # Preserve a refused selection for the authorized reader to reject, without fallback.
        instance = selected['instance']
    return instance, selected['window'], selected['pipe'], selected['pipeline']


@dash_app.callback(
    Output('performance-body', 'children'),
    Output('performance-pipe', 'options'),
    Output('performance-pipeline', 'options'),
    Output('performance-history-page', 'max_value'),
    Output('performance-history-page', 'active_page'),
    Output('performance-history-controls', 'hidden'),
    Output('performance-instance', 'data'),
    Output('performance-window', 'value'),
    Output('performance-pipe', 'value'),
    Output('performance-pipeline', 'value'),
    Input('performance-location', 'pathname'),
    Input('performance-location', 'search'),
    Input('performance-window', 'value'),
    Input('performance-pipe', 'value'),
    Input('performance-pipeline', 'value'),
    Input('performance-refresh', 'n_clicks'),
    Input('performance-interval', 'n_intervals'),
    Input('performance-history-page', 'active_page'),
    Input('performance-instance', 'data'),
    State('session-store', 'data'),
    running=[
        (Output('performance-interval', 'disabled'), True, False),
        (Output('performance-refresh', 'disabled'), True, False),
    ],
)
def render_performance(path, search, window, pipe, pipeline, refresh, interval, page, instance, session):
    if str(path).rstrip('/') != '/dash/performance':
        raise PreventUpdate
    try:
        trigger = dash.ctx.triggered_id
    except MissingCallbackContextException:
        trigger = None
    if trigger == 'performance-location':
        instance, window, pipe, pipeline = follow_performance_location(search, path)
    # Values and options share one response / callback owner, so a query cannot clear its
    # Dropdown before the authorized options arrive (nor race another writer of options).
    selection = (instance, window or '24h', pipe, pipeline)
    try:
        data = performance.metrics(instance, session, window or '24h', pipe, pipeline)
        if trigger in ('performance-window', 'performance-pipe', 'performance-pipeline',
                       'performance-instance', 'performance-location'):
            page = 1
        _, page, pages, _ = performance.history_window(data, page)
        return (performance.build_body(data, data.get('_instance', instance), window or '24h', page),
                performance.filter_options(data.get('_pipe_options'), pipe),
                performance.filter_options(data.get('_pipeline_options'), pipeline, 'Pipeline'),
                pages, page, pages <= 1, *selection)
    except PermissionError:
        return (dbc.Alert('Performance is not available for this selection.', color='secondary'),
                performance.filter_options(value=pipe),
                performance.filter_options(value=pipeline, kind='Pipeline'), 1, 1, True, *selection)
    except Exception:
        return (dbc.Alert('Performance could not be loaded. Try Refresh.', color='warning'),
                performance.filter_options(value=pipe),
                performance.filter_options(value=pipeline, kind='Pipeline'), 1, 1, True, *selection)


@dash_app.callback(
    Output('overview-performance-body', 'children'),
    Output('overview-performance-digest', 'data'),
    Input('overview-interval', 'n_intervals'),
    State('overview-store', 'data'),
    State('overview-performance-digest', 'data'),
    State('session-store', 'data'),
)
def render_performance_tile(interval, store, digest, session):
    from meerschaum.api.dash.callbacks.overview import _changed
    instance = (store or {}).get('instance')
    try:
        data = performance.metrics(instance, session, '24h', with_options=False)
        children = performance.build_overview_body(data, instance)
    except PermissionError:
        children = dbc.Alert('Performance is not available for this selection.', color='secondary')
    except Exception:
        children = dbc.Alert('Performance is temporarily unavailable.', color='secondary')
    return _changed(children, digest)
