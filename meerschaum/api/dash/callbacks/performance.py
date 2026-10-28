"""Lazy, independently refreshed Performance page and Overview card."""

import dash
from dash import Input, Output, State
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from meerschaum.api.dash import dash_app, performance


@dash_app.callback(
    Output('performance-body', 'children'),
    Output('performance-pipe', 'options'),
    Output('performance-pipeline', 'options'),
    Input('performance-location', 'pathname'),
    Input('performance-window', 'value'),
    Input('performance-pipe', 'value'),
    Input('performance-pipeline', 'value'),
    Input('performance-refresh', 'n_clicks'),
    Input('performance-interval', 'n_intervals'),
    State('performance-instance', 'data'),
    State('session-store', 'data'),
)
def render_performance(path, window, pipe, pipeline, refresh, interval, instance, session):
    if str(path).rstrip('/') != '/dash/performance':
        raise PreventUpdate
    try:
        data = performance.metrics(instance, session, window or '24h', pipe, pipeline)
        return (performance.build_body(data, instance, window or '24h'),
                data.get('_pipe_options') or [], data.get('_pipeline_options') or [])
    except PermissionError:
        return dbc.Alert('Performance is not available for this selection.', color='secondary'), [], []
    except Exception:
        return (dbc.Alert('Performance could not be loaded. Try Refresh.', color='warning'),
                dash.no_update, dash.no_update)


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
        raise PreventUpdate
    except Exception:
        children = dbc.Alert('Performance is temporarily unavailable.', color='secondary')
    return _changed(children, digest)
