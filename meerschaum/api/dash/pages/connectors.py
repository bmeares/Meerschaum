#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Browse visible connectors; administrators inspect masked settings and test connections
in a modal. Build each request from the current viewer's permissions.
"""

from __future__ import annotations

import json
from urllib.parse import quote

from meerschaum.utils.typing import Any, Dict, List, Optional
from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

from meerschaum.api.dash.components import build_pages_navbar, get_nav_state, alert_from_success_tuple

#: The table's columns: `(field, header)`.
COLUMNS = (
    ('keys', 'Connector'),
    ('source', 'Source'),
)


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
        return html.Button(
            html.Code(row['keys'], className='connectors-keys'),
            id={'type':'connector-info-button','index':row['keys']},
            type='button', n_clicks=0, className='btn btn-link connector-key-button',
            **{'aria-haspopup':'dialog'},
        )
    if field == 'source':
        parts = [html.Span(row['source'], className=f"connectors-source connectors-source-{row['source'].lower()}")]
        if row.get('source_detail'):
            parts.append(html.Code(row['source_detail'], className='connectors-source-detail'))
        plugin = row.get('plugin') or (row['keys'].split(':',1)[-1] if row.get('type')=='plugin' else None)
        if plugin:
            parts.append(dcc.Link('Plugin: '+str(plugin),
                href='/dash/plugins/installed/'+quote(str(plugin),safe=''),className='connectors-plugin'))
        return parts
    return _text(row.get(field, None))


def _search_text(row: Dict[str, Any]) -> str:
    return ' '.join(
        _text(row.get(field, None)) for field in (
            'keys', 'type', 'plugin', 'source', 'source_detail', 'flavor', 'host', 'database', 'schema',
        )
    ).lower()


def build_table(rows: List[Dict[str, Any]], testable: bool, details: bool = True) -> Any:
    """
    Show keys and source; connection details are fetched when a modal opens.
    """
    if not rows:
        return html.P('No connectors', className='connectors-none')
    if not details:
        rows = [
            {key: value for key, value in row.items()
             if key in ('keys', 'type', 'source', 'source_detail', 'plugin')}
            for row in rows
        ]
    headers = [html.Th(header, scope='col') for _, header in COLUMNS]
    body = []
    for row in rows:
        cells = []
        for field, header in COLUMNS:
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


def connector_modal():
    return dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle(id='connector-info-title')),
        dbc.ModalBody(id='connector-info-body'),
        dbc.ModalFooter(id='connector-info-footer',children=[
            dbc.Button('Close',id='connector-info-close',color='secondary',n_clicks=0),
        ]),
    ],id='connector-info-modal',is_open=False,size='lg',scrollable=True,
       class_name='connector-info-modal')


def connector_info(row, details, testable):
    """Only receive a freshly authorized, secret-masked listing row."""
    keys = row['keys']
    entries = []
    for field,label in (('flavor','Flavor'),('host','Host'),('port','Port'),
                        ('database','Database'),('schema','Schema')):
        value = row.get(field)
        if details and value not in (None,''):
            entries += [html.Dt(label),html.Dd(str(value))]
    body = [html.Dl(entries,className='connector-info-summary'),
            html.Div(_cell('source',row),className='connector-info-source'),
            html.Div(id={'type':'connector-inspect-content','index':keys})]
    footer = []
    if details:
        footer.append(dbc.Button('Inspect',id={'type':'connector-inspect-button','index':keys},
                                 n_clicks=0,color='secondary'))
    if testable and row.get('testable'):
        footer += [html.Div(id={'type':'connector-test-result','index':keys},role='status',
                            className='connectors-test-result'),
                   dbc.Button('Test connection',id={'type':'connector-test-button','index':keys},
                              n_clicks=0,color='primary',class_name='connectors-test-button')]
    footer.append(dbc.Button('Close',id='connector-info-close',color='secondary',n_clicks=0))
    return body,footer


def connector_inspection(row):
    fields = row.get('fields') or {}
    metadata = []
    for key,label in (('required','Required keys'),('supported','Supported keys')):
        if fields.get(key):
            metadata += [html.Dt(label),html.Dd(', '.join(fields[key]))]
    return html.Section([
        html.H5('Connection settings'),
        html.Pre(json.dumps(row.get('attributes') or {},indent=2,sort_keys=True,
                            default=str,ensure_ascii=False),className='connectors-attributes'),
        html.Dl(metadata,className='connectors-fields'),
    ],className='connector-inspection')


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
            from meerschaum.api.dash.access import get_viewer
            rows = list_connectors(viewer=get_viewer(session_id=session_id))
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
                connector_modal(),
            ],
            id='connectors-page',
            className='connectors-page',
        ),
    ]
