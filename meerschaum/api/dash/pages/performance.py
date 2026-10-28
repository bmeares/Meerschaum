"""Sync performance on the selected instance, filtered by Pipe, Pipeline, and time window."""

from urllib.parse import parse_qs, urlparse

from dash import html, dcc
import dash_bootstrap_components as dbc

from meerschaum.api.dash.components import build_pages_navbar
from meerschaum.api.dash import performance


def route(pathname=None, href=None):
    from meerschaum.api.dash.engine import get_instance, instance_problem
    query = parse_qs(urlparse(href or '').query)
    requested = (query.get('instance') or [None])[0]
    instance = str(get_instance(requested))
    window = (query.get('window') or ['24h'])[0]
    window = window if window in performance.WINDOW_HOURS else '24h'
    problem = instance_problem(requested)
    return [
        build_pages_navbar(instance_keys=instance),
        dcc.Location(id='performance-location', refresh=False),
        dcc.Store(id='performance-instance', data=instance),
        dcc.Interval(id='performance-interval', interval=30000, n_intervals=0),
        html.Main([
            html.Div([html.H1('Performance'), dbc.Button(
                'Refresh', id='performance-refresh', size='sm', color='secondary')],
                className='console-page-header'),
            dbc.Alert(problem, color='danger') if problem else None,
            html.Div([
                html.Div([html.Label('Time window', htmlFor='performance-window'), dbc.Select(
                    id='performance-window', value=window, size='sm',
                    class_name='dbc_dark custom-select custom-select-sm',
                    options=[{'label': label, 'value': value} for value, label in performance.WINDOWS])]),
                html.Div([html.Label('Pipe', htmlFor='performance-pipe'), dcc.Dropdown(
                    id='performance-pipe', value=(query.get('pipe') or [None])[0],
                    options=[], placeholder='All Pipes', clearable=True)]),
                html.Div([html.Label('Pipeline', htmlFor='performance-pipeline'), dcc.Dropdown(
                    id='performance-pipeline', value=(query.get('pipeline') or [None])[0],
                    options=[], placeholder='All Pipelines', clearable=True)]),
            ], className='performance-filters'),
            dcc.Loading(html.Div('Loading performance…', id='performance-body', role='status'), type='dot',
                        target_components={'performance-body': 'children'}),
        ], id='performance-page', className='console-page performance-page'),
    ]
