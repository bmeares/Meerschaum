#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The Pipes pages, built per request (the console's router calls `layout(pathname, href)`):

- `/dash/pipes`: the table of the instance's pipes (`?connector_keys=`, `metric_keys=`,
  `location_keys=`, `tags=`, `status=`, `q=` and `instance=` filter it);
- `/dash/pipes/new`: the New pipe form (`?connector_keys=`, `metric_key=`, `location_key=` and
  `instance=` fill it in);
- `/dash/pipes/<connector>/<metric>[/<location>]?instance=...`: one pipe's page.
"""

from urllib.parse import parse_qs, urlparse, unquote

from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.api.dash.components import build_pages_navbar

html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc


def _walk(node):
    if isinstance(node, (list, tuple)):
        for child in node:
            yield from _walk(child)
        return
    if node is None or isinstance(node, (str, int, float)):
        return
    yield node
    yield from _walk(getattr(node, 'children', None))


def _navbar(instance_keys):
    """The console's navbar with its global instance selector set to the page's instance."""
    from meerschaum.utils.misc import filter_keywords
    navbar = build_pages_navbar(**filter_keywords(
        build_pages_navbar,
        instance_select=True,
        instance_keys=instance_keys,
        active='pipes',
    ))
    for component in _walk(navbar):
        if getattr(component, 'id', None) == 'pages-instance-select':
            component.value = instance_keys
    return navbar


def split_path(pathname):
    """The unquoted path segments after `/dash/pipes`."""
    rest = str(pathname or '')[len('/dash/pipes'):].strip('/')
    return [unquote(part) for part in rest.split('/')] if rest else []


def table_layout(instance_keys, global_instance, filters, allowed_new=False, problem=None):
    """The Pipes table: the header with New pipe, the filters, the table and its pages."""
    from meerschaum.api.dash.pipes_table import (
        build_table_layout, SORT_COLUMNS, STATUS_FILTERS,
    )
    dropdown = lambda id, placeholder: html.Div(
        dcc.Dropdown(id=id, options=[], value=[], placeholder=placeholder, multi=True, searchable=True),
        className='dbc_dark pipes-filter',
    )
    sort_options = []
    for column, label in SORT_COLUMNS:
        sort_options += [
            {'label': f"{label} ↑", 'value': f"{column}:asc"},
            {'label': f"{label} ↓", 'value': f"{column}:desc"},
        ]
    header_actions = [
        dbc.Button('Refresh', id='pipes-refresh-button', n_clicks=0, size='sm', color='secondary'),
    ]
    if allowed_new:
        header_actions.append(dbc.Button('New pipe', id='pipes-new-button', href='/dash/pipes/new',
                                         size='sm', color='primary'))
    alerts = [dbc.Alert(problem, color='danger', id='pipes-instance-alert')] if problem else []
    return [
        dcc.Location(id='pipes-location', refresh=False),
        dcc.Store(id='pipes-rows-store', data=None),
        dcc.Store(id='pipes-stats-store', data={}),
        dcc.Store(id='pipes-sort-store', data={'column': 'pipe', 'ascending': True}),
        ### A header's sort button (`assets/pipes.js`); the table's latest render and the
        ### statistics it asked for (`callbacks/pipes.py`).
        dcc.Store(id='pipes-sort-click', data=None),
        dcc.Store(id='pipes-render-done', data=None),
        dcc.Store(id='pipes-stats-request', data=None),
        html.Div(id='pipes-stats-dummy', hidden=True),
        dcc.Store(id='pipes-page-meta', data={
            'instance': instance_keys, 'global': global_instance, 'filters': filters,
        }),
        html.Div(id='pipes-search-url', hidden=True),
        html.Main(
            alerts + [
                html.Div(
                    [
                        html.H3('Pipes'),
                        html.Span(id='pipes-summary', className='pipes-summary'),
                        (dbc.Badge(instance_keys, color='secondary', className='pipes-instance-badge')
                         if instance_keys != global_instance else html.Span()),
                        html.Div(header_actions, className='pipes-header-actions'),
                    ],
                    className='pipes-header',
                ),
                html.Div(id='pipes-alert'),
                html.Div(
                    [
                        dbc.Input(
                            id='pipes-search',
                            type='search',
                            value=filters.get('q', '') or '',
                            placeholder='Search pipes',
                            debounce=250,
                            class_name='pipes-search',
                            autocomplete='off',
                        ),
                        dropdown('pipes-connector-keys-dropdown', 'Connectors'),
                        dropdown('pipes-metric-keys-dropdown', 'Metrics'),
                        dropdown('pipes-location-keys-dropdown', 'Locations'),
                        dropdown('pipes-tags-dropdown', 'Tags'),
                        dbc.Select(
                            id='pipes-status-select',
                            options=[{'label': label, 'value': value} for value, label in STATUS_FILTERS],
                            value=filters.get('status', '') or 'any',
                            class_name='dbc_dark custom-select pipes-status-select',
                        ),
                        dbc.Select(
                            id='pipes-sort-select',
                            options=sort_options,
                            value='pipe:asc',
                            class_name='dbc_dark custom-select pipes-sort-select',
                        ),
                        dbc.Button('Clear all', id='pipes-clear-all-button', n_clicks=0, size='sm',
                                   color='link', className='pipes-clear-all'),
                    ],
                    className='pipes-filters',
                ),
                build_table_layout(),
                html.Div(
                    dbc.Pagination(id='pipes-pagination', max_value=1, active_page=1,
                                   fully_expanded=False, previous_next=True, size='sm'),
                    id='pipes-pagination-div',
                    className='pipes-pagination',
                    hidden=True,
                ),
            ],
            id='pipes-page',
            className='pipes-page pipes-table-page',
            **{'data-instance': instance_keys},
        ),
    ]


def layout(pathname=None, href=None):
    """The layout of a Pipes path (`/dash/pipes`, `/dash/pipes/new` or a pipe's page)."""
    from meerschaum.api.dash import engine
    from meerschaum.api.dash import pipes_access
    from meerschaum.api.dash.pipes_table import filters_from_search, global_instance_keys
    query = urlparse(href or '').query
    filters = filters_from_search(query)
    requested = filters.get('instance', None)
    instance = engine.get_instance(requested)
    instance_keys = str(instance)
    problem = engine.instance_problem(requested)
    global_instance = global_instance_keys()
    session_data = pipes_access.session_data_from_context()
    parts = split_path(pathname)

    if parts == ['new']:
        from meerschaum.api.dash import new_pipe
        allowed, reason = pipes_access.can_register(session_data)
        keys = parse_qs(query)
        page = new_pipe.layout(
            instance_keys, allowed=allowed, reason=reason,
            connector_keys=(keys.get('connector_keys', [None])[0] or None),
            metric_key=(keys.get('metric_key', [None])[0] or None),
            location_key=(keys.get('location_key', [None])[0] or None),
        )
    elif len(parts) in (2, 3):
        from meerschaum.api.dash import pipe_detail
        tab = (parse_qs(query).get('tab', [None])[0] or None)
        page = pipe_detail.layout(
            parts[0], parts[1], (parts[2] if len(parts) == 3 else None),
            instance_keys,
            global_instance,
            allowed=pipes_access.can_act(session_data),
            tab=tab,
            problem=problem,
        )
        problem = None
    else:
        allowed_new, _ = pipes_access.can_register(session_data)
        page = table_layout(instance_keys, global_instance, filters, allowed_new=allowed_new,
                            problem=problem)
        problem = None

    if problem:
        page = [dbc.Alert(problem, color='danger', id='pipes-instance-alert')] + list(page)
    return [_navbar(instance_keys)] + list(page)
