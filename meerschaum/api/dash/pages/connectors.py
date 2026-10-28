#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Connectors page (`/dash/connectors`): the connectors the server knows, read-only, with where
each is defined (the configuration, an environment variable or a plugin), its flavor, host, port
and database, and its other attributes. Secrets never reach the browser
(`meerschaum.api.dash.connector_listing`). Administrators can test a connection.

Who sees it: every signed-in user with `connectors:read` (only administrators when the server
runs `--secure`). The connectors' details (flavor, host, port, database and their other
attributes, secrets masked) and Test connection are for administrators; everyone else sees each
connector's keys, type and source.

The page is built per request (`route`); its callbacks are in `callbacks/connectors_page.py`.
"""

from __future__ import annotations

import json

from meerschaum.utils.typing import Any, Dict, List, Optional
from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

from meerschaum.api.dash.components import build_pages_navbar, get_nav_state, alert_from_success_tuple

#: The table's columns: `(field, header)`.
COLUMNS = (
    ('keys', 'Connector'),
    ('type', 'Type'),
    ('source', 'Source'),
    ('flavor', 'Flavor'),
    ('host', 'Host'),
    ('port', 'Port'),
    ('database', 'Database'),
)


#: The columns everyone who may see the connectors sees (the others are administrators').
PUBLIC_FIELDS = ('keys', 'type', 'source')


def may_view(session_id: Optional[str]) -> bool:
    """
    Whether the viewer may see the connectors: a signed-in user with `connectors:read` (only an
    administrator under `--secure`), or anyone with `--no-auth`.
    """
    from meerschaum.api import no_auth
    if no_auth:
        return True
    if not session_id:
        return False
    from meerschaum.api.dash.access import get_viewer, _secure
    try:
        viewer = get_viewer(session_id=str(session_id))
    except Exception:
        return False
    if not viewer.signed_in or not viewer.can('connectors:read'):
        return False
    return viewer.is_admin or not _secure()


def may_see_details(session_id: Optional[str]) -> bool:
    """Whether the viewer sees the connectors' details (administrators, or `--no-auth`)."""
    return may_test(session_id)


def may_test(session_id: Optional[str]) -> bool:
    """Whether the viewer may test connections (an administrator, or `--no-auth`)."""
    from meerschaum.api import no_auth
    if no_auth:
        return True
    if not session_id:
        return False
    from meerschaum.api.dash.sessions import session_is_admin
    try:
        return bool(session_is_admin(str(session_id)))
    except Exception:
        return False


def _text(value: Any) -> str:
    return '' if value is None else str(value)


def _cell(field: str, row: Dict[str, Any]) -> Any:
    if field == 'keys':
        attributes = row.get('attributes', None) or {}
        code = html.Code(row['keys'], className='connectors-keys')
        if not attributes:
            return code
        return html.Details(
            [
                html.Summary(code),
                html.Pre(
                    json.dumps(attributes, indent=2, sort_keys=True, default=str, ensure_ascii=False),
                    className='connectors-attributes',
                ),
            ],
            className='connectors-details',
        )
    if field == 'type':
        parts = [dbc.Badge(row['type'], color='secondary', className='connectors-type')]
        if row.get('plugin'):
            parts.append(html.Span(f"plugin {row['plugin']}", className='connectors-plugin'))
        return parts
    if field == 'source':
        parts = [html.Span(row['source'], className=f"connectors-source connectors-source-{row['source'].lower()}")]
        if row.get('source_detail'):
            parts.append(html.Code(row['source_detail'], className='connectors-source-detail'))
        return parts
    return _text(row.get(field, None))


def _search_text(row: Dict[str, Any]) -> str:
    return ' '.join(
        _text(row.get(field, None)) for field in (
            'keys', 'type', 'plugin', 'source', 'source_detail', 'flavor', 'host', 'database',
        )
    ).lower()


def build_table(rows: List[Dict[str, Any]], testable: bool, details: bool = True) -> Any:
    """
    The connectors table; with `testable`, a Test connection button per testable connector;
    without `details`, only each connector's keys, type and source (no attributes at all).
    """
    if not rows:
        return html.P('No connectors', className='connectors-none')
    columns = COLUMNS if details else tuple(column for column in COLUMNS if column[0] in PUBLIC_FIELDS)
    if not details:
        rows = [
            {key: value for key, value in row.items() if key not in ('attributes', 'flavor', 'host', 'port', 'database')}
            for row in rows
        ]
    headers = [html.Th(header, scope='col') for _, header in columns]
    if testable:
        headers.append(html.Th(html.Span('Test', className='visually-hidden'), scope='col'))
    body = []
    for row in rows:
        cells = []
        for field, header in columns:
            content = _cell(field, row)
            cells.append(html.Td(
                ### One box per cell, so on a phone (a card per connector) its parts wrap beside
                ### the label instead of running past the card; an empty cell stays `:empty`.
                (
                    None if isinstance(content, str) and not content
                    else html.Div(content, className='connectors-cell')
                ),
                className=f'connectors-col-{field}',
                **{'data-label': header},
            ))
        if testable:
            cells.append(html.Td(
                (
                    [
                        dbc.Button(
                            'Test connection',
                            id={'type': 'connector-test-button', 'index': row['keys']},
                            n_clicks=0,
                            size='sm',
                            color='secondary',
                            class_name='connectors-test-button',
                        ),
                        html.Div(
                            id={'type': 'connector-test-result', 'index': row['keys']},
                            className='connectors-test-result',
                            role='status',
                        ),
                    ]
                    if row.get('testable', False) else []
                ),
                className='connectors-col-test',
            ))
        body.append(html.Tr(
            cells,
            **{'data-search': _search_text(row), 'data-keys': row['keys']},
        ))
    return dbc.Table(
        [html.Thead(html.Tr(headers)), html.Tbody(body)],
        id='connectors-table',
        className='connectors-table',
        hover=True,
        size='sm',
    )


def route(pathname: Optional[str] = None, href: Optional[str] = None) -> List[Any]:
    """The Connectors page."""
    from meerschaum.api.dash.connector_listing import list_connectors, mask_text
    state = get_nav_state()
    session_id = state.get('session_id', None)
    allowed = may_view(session_id)
    if not allowed:
        table = html.Div(html.H5('The connectors are not available to your account'), className='console-empty')
    else:
        try:
            rows = list_connectors()
            table = build_table(rows, may_test(session_id), details=may_see_details(session_id))
        except Exception as e:
            table = alert_from_success_tuple((False, f"Could not list the connectors: {mask_text(e)}"))
    header = [html.H1('Connectors', className='connectors-title')]
    if allowed:
        header.append(dbc.Input(
            id='connectors-filter',
            type='search',
            placeholder='Filter connectors',
            size='sm',
            debounce=False,
            class_name='connectors-filter',
        ))
    return [
        build_pages_navbar(),
        html.Main(
            [
                html.Div(header, className='connectors-header'),
                html.Div(table, id='connectors-table-div', className='connectors-table-wrap'),
                html.Div(id='connectors-filter-dummy', hidden=True),
            ],
            id='connectors-page',
            className='connectors-page',
        ),
    ]
