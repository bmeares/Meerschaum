"""Lazy, independently refreshed Performance page and Overview card."""

import dash
from dash import Input, Output, State, MATCH, ALL, ClientsideFunction
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


def _live_update(live, figures, stores, engine_live, engine_figures):
    """
    The in-place update for `performance-live-data`: the children of the `live()` parts, the
    charts' figures and the breakdowns' rows by key (the Engine section's `engine-*` keys from its
    own read). `assets/performance.js` (`mrsmPerformance.live`) sets them on the mounted
    components; a key it does not hold leaves that component as it is.
    """
    def _merge(body, engine):
        merged = {key: value for key, value in body.items() if not str(key).startswith('engine-')}
        merged.update({key: value for key, value in engine.items() if str(key).startswith('engine-')})
        return merged
    return {
        'parts': _merge(live, engine_live),
        'charts': _merge(figures, engine_figures),
        'groups': dict(stores),
    }


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
    Output('performance-shape', 'data'),
    Output('performance-engine', 'children'),
    Output('performance-engine-shape', 'data'),
    ### Never a wildcard output here: the renderer discards the reply of a run that a newer run
    ### superseded (a Refresh or a tick, then a new window) only when every output resolves to
    ### one component, so a late reply used to restore the old window and page. The mounted parts
    ### are set in the browser from this store instead (`mrsmPerformance.live`).
    Output('performance-live-data', 'data'),
    Input('performance-location', 'pathname'),
    Input('performance-location', 'search'),
    Input('performance-window', 'value'),
    Input('performance-pipe', 'value'),
    Input('performance-pipeline', 'value'),
    Input('performance-refresh', 'n_clicks'),
    Input('performance-interval', 'n_intervals'),
    Input('performance-history-page', 'active_page'),
    Input('performance-instance', 'data'),
    Input('performance-history-page-size', 'value'),
    State('session-store', 'data'),
    State('performance-shape', 'data'),
    State('performance-engine-shape', 'data'),
    running=[
        (Output('performance-interval', 'disabled'), True, False),
        (Output('performance-refresh', 'disabled'), True, False),
        (Output('performance-refresh-status', 'className'),
         'performance-refresh-status is-refreshing', 'performance-refresh-status'),
    ],
)
def render_performance(path, search, window, pipe, pipeline, refresh, interval, page, instance, page_size,
                       session, shape=None, engine_shape=None):
    """
    Read the metrics afresh for every tick, Refresh, page or selection. When the page already shows
    this selection with the same sections (`performance.body_shape()`), only the mounted parts
    change (the stats, coverage, notes and recent syncs, the charts' figures, the breakdowns' rows):
    replacing the whole body emptied the breakdown tables until `assets/performance.js` refilled
    them, and the page, shorter for a moment, lost its scroll position.
    """
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
                       'performance-instance', 'performance-location', 'performance-history-page-size'):
            page = 1
        _, page, pages, _ = performance.history_window(data, page, page_size)
        shown = data.get('_instance', instance)
        layout = performance.body_shape(data, shown, window or '24h')
        controls = (performance.filter_options(data.get('_pipe_options'), pipe),
                    performance.filter_options(data.get('_pipeline_options'), pipeline, 'Pipeline'),
                    pages, page, pages <= 1, *selection, layout)
        engine = _engine(data, window or '24h', engine_shape)
        if shape != layout:
            body = performance.build_body(data, shown, window or '24h', page, page_size)
            live, figures, stores = {}, {}, {}
        else:
            body = dash.no_update
            live = performance.live_parts(data, page, page_size)
            figures = performance.chart_figures(data, performance._scope(data, shown, window or '24h'))
            stores = performance.group_stores(data, shown, window or '24h')
        engine_body, engine_layout, engine_live, engine_figures = engine
        return (body, *controls, engine_body, engine_layout,
                _live_update(live, figures, stores, engine_live, engine_figures))
    except PermissionError:
        return (dbc.Alert('Performance is not available for this selection.', color='secondary'),
                performance.filter_options(value=pipe),
                performance.filter_options(value=pipeline, kind='Pipeline'), 1, 1, True, *selection,
                None, None, None, dash.no_update)
    except Exception:
        return (dbc.Alert('Performance could not be loaded. Try Refresh.', color='warning'),
                performance.filter_options(value=pipe),
                performance.filter_options(value=pipeline, kind='Pipeline'), 1, 1, True, *selection,
                None, None, None, dash.no_update)


def _engine(data, window, mounted_shape):
    """
    The Engine section for the selection `data` authorized: `(children, shape, live parts,
    figures)`. With the same shape as the section shown, only its mounted parts change
    (`children` is `no_update`); otherwise it is rebuilt. A failed read shows its own alert and
    never hides the sync measurements.
    """
    try:
        health = performance.engine_health(data, window)
    except PermissionError:
        raise
    except Exception:
        return performance.build_engine(None), None, {}, {}
    layout = performance.engine_shape(health)
    if layout != mounted_shape:
        return performance.build_engine(health), layout, {}, {}
    if layout[1] != 'body':
        return dash.no_update, layout, {}, {}
    return (dash.no_update, layout, performance.engine_live_parts(health),
            performance.engine_figures(health, layout[0]))


dash_app.clientside_callback(
    ClientsideFunction(namespace='mrsmPerformance', function_name='live'),
    Output({'type': 'performance-live', 'part': ALL}, 'children'),
    Output({'type': 'performance-graph', 'chart': ALL}, 'figure'),
    Output({'type': 'performance-group-data', 'group': ALL}, 'data'),
    Input('performance-live-data', 'data'),
    prevent_initial_call=True,
)


dash_app.clientside_callback(
    ClientsideFunction(namespace='mrsmPerformance', function_name='groups'),
    Output({'type': 'performance-group-body', 'group': MATCH}, 'children'),
    Output({'type': 'performance-group-page', 'group': MATCH}, 'max_value'),
    Output({'type': 'performance-group-page', 'group': MATCH}, 'active_page'),
    Output({'type': 'performance-group-range', 'group': MATCH}, 'children'),
    Input({'type': 'performance-group-data', 'group': MATCH}, 'data'),
    Input({'type': 'performance-group-search', 'group': MATCH}, 'value'),
    Input({'type': 'performance-group-sort', 'group': MATCH}, 'value'),
    Input({'type': 'performance-group-page', 'group': MATCH}, 'active_page'),
)


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
