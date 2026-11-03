#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Browse visible connectors; administrators see masked settings and test connections
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

#: The connector types' group headings (any other type is shown as it is spelled) and icons.
TYPE_LABELS = {'sql': 'SQL', 'api': 'API', 'valkey': 'Valkey', 'plugin': 'Plugin'}
TYPE_ICONS = {'sql': 'fa-database', 'api': 'fa-server', 'valkey': 'fa-bolt', 'plugin': 'fa-puzzle-piece'}


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


def _icon(name: str) -> Any:
    return html.I(className=f'fa-solid {name}', **{'aria-hidden': 'true'})


def connector_plugin(row: Dict[str, Any]) -> Optional[str]:
    """The plugin behind a connector: a data plugin's `plugin:<name>`, or the plugin of its type."""
    if row.get('plugin'):
        return str(row['plugin'])
    typ, _, label = str(row.get('keys', '')).partition(':')
    return label if typ == 'plugin' and label else None


def source_parts(row: Dict[str, Any]) -> List[Any]:
    """
    Where a connector comes from (`Config`, `Default`, `Environment MRSM_SQL_FOO`, `Runtime`) and a
    View source link (an icon; the label is its accessible name and tooltip) to the plugin behind it. A plugin's connectors say nothing more: the plugin's
    name is in their keys, or on its page.
    """
    parts = []
    if row.get('source') != 'Plugin':
        source = str(row.get('source') or '')
        parts.append(html.Span(source, className=f"connectors-source connectors-source-{source.lower()}"))
        if row.get('source_detail'):
            parts.append(html.Code(row['source_detail'], className='connectors-source-detail'))
    plugin = connector_plugin(row)
    if plugin:
        parts.append(dcc.Link(
            [_icon('fa-code'), html.Span('View source', className='visually-hidden')],
            href='/dash/plugins/installed/' + quote(plugin, safe=''),
            className='connectors-plugin',
            title='View source',
        ))
    return parts


def _cell(field: str, row: Dict[str, Any]) -> Any:
    if field == 'keys':
        return html.Button(
            html.Code(row['keys'], className='connectors-keys'),
            id={'type':'connector-info-button','index':row['keys']},
            type='button', n_clicks=0, className='btn btn-link connector-key-button',
            **{'aria-haspopup':'dialog'},
        )
    if field == 'source':
        return source_parts(row)
    return _text(row.get(field, None))


def type_label(typ: str) -> str:
    return TYPE_LABELS.get(typ, typ)


def _search_text(row: Dict[str, Any]) -> str:
    return ' '.join(
        _text(row.get(field, None)) for field in (
            'keys', 'type', 'plugin', 'source', 'source_detail', 'flavor', 'host', 'database', 'schema',
        )
    ).lower()


def _group_heading(typ: str, count: int) -> Any:
    return html.Tr(
        html.Th(
            [
                _icon(TYPE_ICONS.get(typ, 'fa-plug')),
                html.Span(type_label(typ), className='connectors-group-name'),
                ' ',
                html.Span(str(count), className='connectors-group-count'),
            ],
            colSpan=len(COLUMNS),
            scope='rowgroup',
        ),
        className='connectors-group-heading',
    )


def build_table(rows: List[Dict[str, Any]], testable: bool, details: bool = True) -> Any:
    """
    Show keys and source, one row group per connector type (in `list_connectors()`'s order:
    `sql`, `api`, `valkey`, the other types, then `plugin`); connection details are fetched when a
    modal opens.
    """
    if not rows:
        return html.P('No connectors', className='connectors-none')
    if not details:
        rows = [
            {key: value for key, value in row.items()
             if key in ('keys', 'type', 'source', 'source_detail', 'plugin')}
            for row in rows
        ]
    groups: Dict[str, List[Any]] = {}
    for row in rows:
        cells = []
        for field, header in COLUMNS:
            content = _cell(field, row)
            cells.append(html.Td(
                ### One box per cell, so on a phone (a card per connector) its parts wrap beside
                ### the label instead of running past the card; an empty cell stays `:empty`.
                (
                    None if (not content if isinstance(content, (str, list)) else False)
                    else html.Div(content, className='connectors-cell')
                ),
                className=f'connectors-col-{field}',
                **{'data-label': header},
            ))
        typ = str(row.get('type') or str(row['keys']).split(':', 1)[0])
        groups.setdefault(typ, []).append(html.Tr(
            cells,
            **{'data-search': _search_text(row), 'data-keys': row['keys']},
        ))
    headers = [html.Th(header, scope='col') for _, header in COLUMNS]
    return dbc.Table(
        [html.Thead(html.Tr(headers))] + [
            html.Tbody(
                [_group_heading(typ, len(body))] + body,
                className='connectors-group',
                **{'data-type': typ},
            )
            for typ, body in groups.items()
        ],
        id='connectors-table',
        className='connectors-table',
        hover=True,
        size='sm',
    )


def connector_modal():
    return dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle(id='connector-info-title')),
        dbc.ModalBody(id='connector-info-body'),
        ### Test connection (administrators); hidden when the connector has no actions.
        dbc.ModalFooter(id='connector-info-footer',children=[],style={'display':'none'}),
    ],id='connector-info-modal',is_open=False,size='lg',scrollable=True,
       class_name='connector-info-modal')


#: The connection settings the modal lists first: `(field, label)`.
SUMMARY = (('flavor', 'Flavor'), ('host', 'Host'), ('port', 'Port'), ('database', 'Database'),
           ('schema', 'Schema'))


def connector_info(row, details, testable):
    """
    The modal's body and footer for a freshly authorized, secret-masked listing row: its source,
    and with `details` the connection settings at once, each value shown once (the title holds the
    keys). The header's X and a click outside close the modal; the footer only holds Test
    connection.
    """
    keys = row['keys']
    entries = [html.Dt('Source'), html.Dd(source_parts(row), className='connector-info-source')]
    shown = {}
    for field, label in SUMMARY:
        value = row.get(field)
        if details and value not in (None, ''):
            entries += [html.Dt(label), html.Dd(str(value))]
            shown[field] = value
    body = [html.Dl(entries, className='connector-info-summary')]
    if details:
        body.append(connector_inspection(row, shown))
    footer = []
    if testable and row.get('testable'):
        footer += [html.Div(id={'type':'connector-test-result','index':keys},role='status',
                            className='connectors-test-result'),
                   dbc.Button('Test connection',id={'type':'connector-test-button','index':keys},
                              n_clicks=0,color='primary',class_name='connectors-test-button')]
    return body,footer


def connector_inspection(row, shown=None):
    """
    The settings the summary does not already show (a URI stays: it is how the connector is
    configured), and the keys its type requires and supports. `None` when there is nothing more.
    """
    shown = shown or {}
    attributes = {
        key: value for key, value in (row.get('attributes') or {}).items()
        if not (key in shown and str(value) == str(shown[key]))
    }
    fields = row.get('fields') or {}
    metadata = []
    for key,label in (('required','Required keys'),('supported','Supported keys')):
        if fields.get(key):
            metadata += [html.Dt(label),html.Dd(', '.join(fields[key]))]
    if not attributes and not metadata:
        return None
    return html.Section([
        html.H5('Other settings' if shown else 'Settings') if attributes else None,
        html.Pre(json.dumps(attributes,indent=2,sort_keys=True,
                            default=str,ensure_ascii=False),className='connectors-attributes')
        if attributes else None,
        html.Dl(metadata,className='connectors-fields') if metadata else None,
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
