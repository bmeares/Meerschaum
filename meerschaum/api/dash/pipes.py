#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with pipes via the web interface.
"""

from __future__ import annotations
import json
import shlex
from textwrap import dedent
from urllib.parse import urlencode

from meerschaum.utils import fetch_pipes_keys
from meerschaum.utils.typing import List, Optional, Dict, Any, Tuple, Union, SuccessTuple
from meerschaum.utils.misc import get_connector_labels
from meerschaum.connectors import instance_types
from meerschaum.utils.packages import attempt_import, import_dcc, import_html, import_pandas
from meerschaum.utils.sql import get_pd_type, sql_item_name
from meerschaum.utils.misc import _format_timespan
from meerschaum.utils.yaml import yaml
from meerschaum.utils.warnings import warn
from meerschaum.utils.dataframe import to_json, to_simple_lines
from meerschaum.api.dash._support import get_pipe_query
from meerschaum.api import CHECK_UPDATE, get_api_connector
from meerschaum.api.dash import debug, _get_pipes
from meerschaum.api.dash.connectors import get_web_connector
from meerschaum.api.dash.components import (
    alert_from_success_tuple,
    build_cards_grid,
    build_sign_out_button,
    build_logo_row,
)
from meerschaum.api.dash.sessions import is_session_authenticated
from meerschaum.config import get_config
import meerschaum as mrsm
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
dash_ace = attempt_import('dash_ace', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
pd = import_pandas()

INDEX_PREFIX_EMOJI: Dict[str, str] = {
    'datetime': '🕓',
    'primary': '🔑',
}


def pipe_from_ctx(ctx, trigger_property: str = 'n_clicks') -> Union[mrsm.Pipe, None]:
    """
    Return a `meerschaum.Pipe` object from a dynamic object with an
    index of a pipe's meta dictionary.
    """
    try:
        ### I know this looks confusing and feels like a hack.
        ### Because Dash JSON-ifies the ID dictionary and we are including a JSON-ified dictionary,
        ### we have to do some crazy parsing to get the pipe's meta-dict back out of it
        meta = json.loads(json.loads(ctx[0]['prop_id'].split('.' + trigger_property)[0])['index'])
    except Exception:
        meta = None
    if meta is None:
        return None
    return mrsm.Pipe(**meta)


def keys_from_state(
    state: Dict[str, Any],
    with_tags: bool = False,
) -> Union[
    Tuple[List[str], List[str], List[str]],
    Tuple[List[str], List[str], List[str], List[str]],
]:
    """
    Read the current state and return the selected keys lists.
    """
    _filters = {
        'ck': state.get("connector-keys-dropdown.value", None),
        'mk': state.get("metric-keys-dropdown.value", None),
        'lk': state.get("location-keys-dropdown.value", None),
        'tags': state.get("tags-dropdown.value", None),
    }
    for k in _filters:
        _filters[k] = [] if _filters[k] is None else _filters[k]
        if not isinstance(_filters[k], list):
            try:
                _filters[k] = shlex.split(_filters[k])
            except Exception as e:
                print(e)
                _filters[k] = []
    keys = [_filters['ck'], _filters['mk'], _filters['lk']]
    if with_tags:
        keys.append(_filters['tags'])
    return tuple(keys)


def pipes_from_state(
    state: Dict[str, Any],
    **kw
):
    _ck, _mk, _lk, _tags = keys_from_state(state, with_tags=True)
    try:
        _pipes = _get_pipes(
            _ck, _mk, _lk,
            tags=(_tags or []),
            mrsm_instance=get_web_connector(state), 
            **kw
        )
    except Exception as e:
        return False, str(e)
    return _pipes


def build_pipe_card(
    pipe: mrsm.Pipe,
    authenticated: bool = False,
    include_manage: bool = True,
    header_only: bool = False,
    _build_references_num: int = 10,
    _build_parents_num: int = 10,
    _build_children_num: int = 10,
) -> 'dbc.Card':
    """
    Return a card for the given pipe.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe from which to build the card.

    authenticated: bool, default False
        If `True`, allow editing functionality to the card.

    include_manage: bool, default True
        If `True` and `authenticated` is `True`, include the "Manage" dropdown.

    header_only: bool, default False
        If `True`, only build the card header.

    Returns
    -------
    A dash bootstrap components Card representation of the pipe.
    """
    meta_str = json.dumps(pipe.meta)
    footer_children = dbc.Row(
        [
            dbc.Col(
                (
                    dbc.DropdownMenu(
                        label="Manage",
                        children=[
                            dbc.DropdownMenuItem(
                                'Open in Python',
                                id={
                                    'type': 'manage-pipe-button',
                                    'index': meta_str,
                                    'action': 'python',
                                },
                            ),
                            dbc.DropdownMenuItem(
                                'Delete',
                                id={
                                    'type': 'manage-pipe-button',
                                    'index': meta_str,
                                    'action': 'delete',
                                },
                            ),
                            dbc.DropdownMenuItem(
                                'Drop',
                                id={
                                    'type': 'manage-pipe-button',
                                    'index': meta_str,
                                    'action': 'drop',
                                },
                            ),
                            dbc.DropdownMenuItem(
                                'Clear',
                                id={
                                    'type': 'manage-pipe-button',
                                    'index': meta_str,
                                    'action': 'clear',
                                },
                            ),
                            dbc.DropdownMenuItem(
                                'Verify',
                                id={
                                    'type': 'manage-pipe-button',
                                    'index': meta_str,
                                    'action': 'verify',
                                },
                            ),
                            dbc.DropdownMenuItem(
                                'Sync',
                                id={
                                    'type': 'manage-pipe-button',
                                    'index': meta_str,
                                    'action': 'sync',
                                },
                            ),
                        ],
                        direction="up",
                        menu_variant="dark",
                        size='sm',
                        color='secondary',
                    )
                ) if authenticated and include_manage else [],
                width=2,
            ),
            dbc.Col(width=6),
            dbc.Col(
                dbc.Button(
                    'Download CSV',
                    size='sm',
                    color='link',
                    style={'float': 'right'},
                    id={'type': 'pipe-download-csv-button', 'index': meta_str},
                ),
                width=4,
            ),
        ],
        justify='start',
    )
    card_body_children = [
        html.Div(
            dbc.Accordion(
                accordion_items_from_pipe(
                    pipe,
                    authenticated=authenticated,
                    _build_references_num=_build_references_num,
                    _build_parents_num=_build_parents_num,
                    _build_children_num=_build_children_num,
                ),
                flush=True,
                start_collapsed=True,
                id={'type': 'pipe-accordion', 'index': meta_str},
            )
        )

    ]

    from meerschaum.api.dash.engine import pipe_href
    pipe_url = pipe_href('|'.join(map(str, (
        pipe.instance_keys, pipe.connector_keys, pipe.metric_key, pipe.location_key,
    ))))

    card_header_children = dbc.Row(
        [
            dbc.Col(
                html.H5(
                    html.B(str(pipe)),
                    className='card-title',
                    style={'font-family': ['monospace']}
                ),
                width=11,
            ),
            dbc.Col(
                dbc.Button(
                    "🔗",
                    href=pipe_url,
                    external_link=True,
                    target="_blank",
                    style={'float': 'right'},
                    outline=True,
                    color='link',
                    id={'type': 'share-pipe-button', 'index': meta_str},
                ),
                width=1,
            ),
        ],
        justify='start',
    )

    if header_only:
        return dbc.Card(dbc.CardHeader(card_header_children))

    return dbc.Card([
        dbc.CardHeader(children=card_header_children),
        dbc.CardBody(children=card_body_children),
        dbc.CardFooter(children=footer_children),
    ])


def get_pipes_cards(*keys, session_data: Optional[Dict[str, Any]] = None):
    """
    Returns a tuple:
        - pipes as a list of cards
        - alert list
    """
    cards = []
    session_id = (session_data or {}).get('session-id', None)
    authenticated = is_session_authenticated(str(session_id))

    pipes = pipes_from_state(*keys, as_list=True)
    alerts = [alert_from_success_tuple(pipes)]
    if not isinstance(pipes, list):
        pipes = []

    max_num_pipes_cards = get_config('dash', 'max_num_pipes_cards')
    overflow_pipes = pipes[max_num_pipes_cards:]

    for pipe in pipes[:max_num_pipes_cards]:
        cards.append(build_pipe_card(pipe, authenticated=authenticated))

    if overflow_pipes:
        cards.append(
            dbc.Card([
                dbc.CardBody(
                    html.Ul(
                        [
                            html.Li(html.H5(
                                html.B(str(pipe)),
                                className = 'card-title',
                                style = {'font-family': ['monospace']}
                            ))
                            for pipe in overflow_pipes
                        ]
                    )
                )
            ])
        )

    return cards, alerts


def accordion_items_from_pipe(
    pipe: mrsm.Pipe,
    active_items: Optional[List[str]] = None,
    authenticated: bool = False,
    _build_references_num: int = 10,
    _build_parents_num: int = 10,
    _build_children_num: int = 10,
) -> 'List[dbc.AccordionItem]':
    """
    Build the accordion items for a given pipe (each section's body comes from its builder below,
    which the pipe page's tabs reuse).
    """
    if active_items is None:
        active_items = []

    items_titles = {
        'overview': '🔑 Keys',
        'stats': '🧮 Statistics',
        'columns': '🏛️ Columns',
        'parameters': '📔 Parameters',
    }
    if pipe.connector_keys.startswith('sql:'):
        items_titles['sql'] = '📃 SQL Query'
    items_titles.update({
        'relations': '🕸️ Parents & Children',
        'recent-data': '🗃️ Recent Data',
        'query-data': '🔍 Query Data',
        'sync-data': '📝 Sync Documents',
    })

    skip_items = (
        ['sync-data']
        if not authenticated
        else []
    )
    for item in skip_items:
        _ = items_titles.pop(item, None)

    builders = {
        'overview': lambda: build_keys_body(pipe),
        'stats': lambda: build_stats_body(pipe),
        'columns': lambda: build_columns_body(pipe),
        'parameters': lambda: build_parameters_body(
            pipe,
            authenticated=authenticated,
            _build_references_num=_build_references_num,
            _build_parents_num=_build_parents_num,
            _build_children_num=_build_children_num,
        ),
        'sql': lambda: build_sql_body(pipe),
        'recent-data': lambda: build_recent_data_body(pipe),
        'query-data': lambda: build_query_data_body(pipe),
        'sync-data': lambda: build_sync_documents_body(pipe, authenticated=authenticated),
        'relations': lambda: build_relations_body(pipe, authenticated=authenticated),
    }

    ### Only generate items if they're in the `active_items` list.
    items_bodies = {
        item_id: builders[item_id]()
        for item_id in active_items
        if item_id in items_titles and item_id in builders
    }

    return [
        dbc.AccordionItem(items_bodies.get(item_id, ''), title=title, item_id=item_id)
        for item_id, title in items_titles.items()
    ]


def _pipe_meta_str(pipe: mrsm.Pipe) -> str:
    """The `index` of a pipe's pattern-matching ids (its sorted `meta`)."""
    return json.dumps(pipe.meta, sort_keys=True)


def get_target_label(pipe: mrsm.Pipe) -> str:
    """The pipe's target table, quoted and schema-qualified for SQL instances."""
    try:
        if pipe.instance_connector.type != 'sql':
            return str(pipe.target)
        return sql_item_name(
            pipe.target,
            flavor=pipe.instance_connector.flavor,
            schema=pipe.instance_connector.get_pipe_schema(pipe),
        )
    except Exception:
        return str(pipe.target)


def build_indices_table(pipe: mrsm.Pipe) -> Union['dbc.Table', None]:
    """
    The pipe's indices (key, column, index name and whether it is part of the composite key),
    or `None` when it declares none.
    """
    pipe_meta_str = _pipe_meta_str(pipe)
    indices_header = [
        html.Thead(
            html.Tr(
                [
                    html.Th(
                        html.Span(
                            "Key",
                            id={'type': 'key-table-header', 'index': pipe_meta_str},
                            style={"textDecoration": "underline", "cursor": "pointer"},
                        ),
                    ),
                    html.Th(
                        html.Span(
                            "Column",
                            id={'type': 'column-table-header', 'index': pipe_meta_str},
                            style={"textDecoration": "underline", "cursor": "pointer"},
                        ),
                    ),
                    html.Th(
                        html.Span(
                            "Index",
                            id={'type': 'index-table-header', 'index': pipe_meta_str},
                            style={"textDecoration": "underline", "cursor": "pointer"},
                        ),
                    ),
                    html.Th(
                        html.Span(
                            "Is Composite",
                            id={'type': 'is-composite-table-header', 'index': pipe_meta_str},
                            style={"textDecoration": "underline", "cursor": "pointer"},
                        ),
                    ),
                    dbc.Tooltip(
                        "Unique reference name for the index "
                        "(e.g. `datetime` for the range axis)",
                        target={'type': 'key-table-header', 'index': pipe_meta_str},
                    ),
                    dbc.Tooltip(
                        "The actual column (field name) in the target dataset.",
                        target={'type': 'column-table-header', 'index': pipe_meta_str},
                    ),
                    dbc.Tooltip(
                        "The name of the index created on the given columns.",
                        target={'type': 'index-table-header', 'index': pipe_meta_str},
                    ),
                    dbc.Tooltip(
                        "Whether the column is used in the composite primary key "
                        "to determine updates.",
                        target={'type': 'is-composite-table-header', 'index': pipe_meta_str},
                    ),
                ]
            )
        )
    ]

    indices = pipe.indices
    columns = pipe.columns
    try:
        index_column_names = pipe.get_indices()
    except Exception:
        index_column_names = {}
    indices_rows = []
    for ix_key, ix_cols in indices.items():
        col = columns.get(ix_key, None)
        ix_name = index_column_names.get(ix_key, None)
        if not col and not ix_cols:
            continue
        if not isinstance(ix_cols, (list, tuple)):
            ix_cols = [ix_cols]
        if col:
            col_item = html.Pre(col)
        elif len(ix_cols) == 1:
            col_item = html.Pre(ix_cols[0])
        else:
            col_item = html.Pre(html.Ul([html.Li(_c) for _c in ix_cols]))
        is_composite_item = "✅" if col else ""
        ix_key_item = html.Pre(
            INDEX_PREFIX_EMOJI.get(ix_key, '')
            + (' ' if ix_key in INDEX_PREFIX_EMOJI else '')
            + ix_key
        )
        indices_rows.append(
            html.Tr([
                html.Td(ix_key_item),
                html.Td(col_item),
                html.Td(html.Pre(ix_name or '')),
                html.Td(is_composite_item),
            ])
        )
    if not indices_rows:
        return None
    return dbc.Table(
        indices_header + [html.Tbody(indices_rows)],
        bordered=True,
        hover=False,
        striped=True,
    )


def build_keys_body(pipe: mrsm.Pipe) -> Any:
    """The Keys section: the pipe's keys, target table, indices and tags."""
    overview_header = [html.Thead(html.Tr([html.Th("Attribute"), html.Th("Value")]))]
    overview_rows = [
        html.Tr([html.Td("Connector"), html.Td(html.Pre(f"{pipe.connector_keys}"))]),
        html.Tr([html.Td("Metric"), html.Td(html.Pre(f"{pipe.metric_key}"))]),
        html.Tr([html.Td("Location"), html.Td(html.Pre(f"{pipe.location_key}"))]),
        html.Tr([html.Td("Instance"), html.Td(html.Pre(f"{pipe.instance_keys}"))]),
        html.Tr([html.Td("Target Table"), html.Td(html.Pre(get_target_label(pipe)))]),
    ]

    indices_table = build_indices_table(pipe)
    if indices_table is not None:
        num_indices = len(indices_table.children[-1].children)
        overview_rows.append(
            html.Tr([
                html.Td("Indices" if num_indices != 1 else "Index"),
                html.Td(html.Div(indices_table, style={'overflowX': 'auto'})),
            ])
        )

    tags = pipe.tags
    if tags:
        tags_items = html.Ul([
            html.Li(tag)
            for tag in tags
        ])
        overview_rows.append(
            html.Tr([
                html.Td("Tags"),
                html.Td(html.Pre(tags_items)),
            ])
        )

    return html.Div(
        dbc.Table(
            overview_header + [html.Tbody(overview_rows)],
            bordered=False,
            hover=True,
            striped=False,
        ),
        style={'overflowX': 'auto'},
    )


def build_rowcount_button(pipe: mrsm.Pipe, label: str = "Calculate") -> Any:
    """
    The exact row count on demand: a button replaced by the count when clicked
    (`calculate_rowcount_button_click`).
    """
    pipe_meta_str = _pipe_meta_str(pipe)
    return html.Div(
        dbc.Button(
            label,
            color='link',
            size='sm',
            style={'text-decoration': 'none'},
            id={'type': 'calculate-rowcount-button', 'index': pipe_meta_str},
        ),
        id={'type': 'calculate-rowcount-div', 'index': pipe_meta_str},
    )


def build_stats_body(pipe: mrsm.Pipe) -> Any:
    """The Statistics section: timespan, oldest and newest times, precision and row count."""
    stats_header = [html.Thead(html.Tr([html.Th("Statistic"), html.Th("Value")]))]
    try:
        oldest_time = pipe.get_sync_time(newest=False, round_down=False, debug=debug)
        newest_time = pipe.get_sync_time(newest=True, round_down=False, debug=debug)
        interval = (
            (newest_time - oldest_time) if newest_time is not None and oldest_time is not None
            else None
        )
    except Exception:
        oldest_time = None
        newest_time = None
        interval = None

    stats_rows = []
    if interval is not None and not isinstance(interval, int):
        stats_rows.append(
            html.Tr([html.Td("Timespan"), html.Td(_format_timespan(
                interval.total_seconds() if hasattr(interval, 'total_seconds') else interval
            ))])
        )
    if oldest_time is not None:
        stats_rows.append(html.Tr([html.Td("Oldest time"), html.Td(str(oldest_time))]))
    if newest_time is not None:
        stats_rows.append(html.Tr([html.Td("Newest time"), html.Td(str(newest_time))]))

    precision = pipe.precision
    if precision:
        stats_rows.append(
            html.Tr([
                html.Td("Precision"),
                html.Td(str(precision.get('interval', 1)) + ' ' + str(precision.get('unit', 'unit')))
            ])
        )

    try:
        exists = pipe.exists(debug=debug)
    except Exception:
        exists = False
    stats_rows.append(
        html.Tr([
            html.Td("Row count"),
            html.Td(
                build_rowcount_button(pipe)
                if exists
                else html.Div('0', id={'type': 'calculate-rowcount-div', 'index': _pipe_meta_str(pipe)})
            )
        ])
    )

    return html.Div(
        dbc.Table(stats_header + [html.Tbody(stats_rows)], hover=True),
        style={'overflowX': 'auto'},
    )


def build_columns_body(pipe: mrsm.Pipe) -> Any:
    """The Columns section: each column's database and pandas types."""
    try:
        columns_header = [html.Thead(html.Tr([
            html.Th("Column"), html.Th("DB Type"), html.Th("PD Type")
        ]))]
        columns_types = {
            col: typ.replace('_', ' ')
            for col, typ in pipe.get_columns_types(debug=debug).items()
        }
        columns_rows = [
            html.Tr([
                html.Td(html.Pre(col)),
                html.Td(html.Pre(typ)),
                html.Td(html.Pre(get_pd_type(typ))),
            ]) for col, typ in columns_types.items()
        ]
        columns_body = [html.Tbody(columns_rows)]
        columns_table = dbc.Table(columns_header + columns_body, bordered=False, hover=True)
        return html.Div(columns_table, style={'overflowX': 'auto'})
    except Exception as e:
        return html.P(f"Could not retrieve columns:\n{e}")


def build_parameters_body(
    pipe: mrsm.Pipe,
    authenticated: bool = False,
    read_only: bool = False,
    include_relatives: bool = True,
    show_resolve: bool = True,
    _build_references_num: int = 10,
    _build_parents_num: int = 10,
    _build_children_num: int = 10,
) -> Any:
    """
    The Parameters section: the parameters editor (YAML or JSON; **Show resolved values** shows
    them with their references resolved, unless not `show_resolve`), with Update for
    authenticated sessions unless `read_only`, and
    the reference, parent and child pipes' headers when `include_relatives`.
    """
    pipe_meta_str = _pipe_meta_str(pipe)
    parameters_editor = dash_ace.DashAceEditor(
        value=yaml.dump(pipe.get_parameters(apply_symlinks=False, refresh=True, debug=debug)),
        mode='norm',
        tabSize=4,
        theme='twilight',
        id={'type': 'parameters-editor', 'index': pipe_meta_str},
        width='100%',
        height='500px',
        readOnly=read_only,
        showGutter=True,
        showPrintMargin=True,
        highlightActiveLine=True,
        wrapEnabled=True,
        style={'min-height': '120px'},
    )
    update_parameters_button = dbc.Button(
        "Update",
        id={'type': 'update-parameters-button', 'index': pipe_meta_str},
    )

    as_yaml_button = dbc.Button(
        "YAML",
        id={'type': 'parameters-as-yaml-button', 'index': pipe_meta_str},
        color='link',
        size='sm',
        style={'text-decoration': 'none'},
    )
    as_json_button = dbc.Button(
        "JSON",
        id={'type': 'parameters-as-json-button', 'index': pipe_meta_str},
        color='link',
        size='sm',
        style={'text-decoration': 'none', 'margin-left': '10px'},
    )
    parameters_div_children = [
        parameters_editor,
        html.Br(),
        dbc.Row(
            [
                dbc.Col(
                    html.Span(
                        (
                            ([update_parameters_button] if authenticated and not read_only else []) +
                            [
                                as_json_button,
                                as_yaml_button,
                            ]
                        )
                    ),
                    width='auto',
                ),
                dbc.Col(
                    dbc.Switch(
                        label="Show resolved values",
                        value=False,
                        id={'type': 'resolve-symlinks-parameters-switch', 'index': pipe_meta_str},
                    ) if show_resolve else [],
                    width='auto',
                ),
            ],
            justify='between',
            align='center',
        ),
        dbc.Row(
            dbc.Col(
                html.Div(
                    id={
                        'type': 'update-parameters-success-div',
                        'index': pipe_meta_str,
                    }
                ),
                width=True,
            ),
        ),
    ]
    if not include_relatives:
        return html.Div(parameters_div_children)

    if _build_references_num > 0 and pipe.references:
        references_cards = [
            build_pipe_card(
                reference_pipe,
                authenticated=authenticated,
                header_only=True,
                _build_references_num=(_build_references_num - 1),
            )
            for reference_pipe in pipe.references
        ]
        references_grid = build_cards_grid(references_cards, num_columns=1)
        references_div_items = [
            html.Br(),
            html.H3('Reference Pipes'),
            html.Br(),
            references_grid,

        ]
        parameters_div_children.extend([html.Br()] + references_div_items)

    if _build_parents_num > 0 and pipe.parents:
        parents_cards = [
            build_pipe_card(
                parent_pipe,
                authenticated=authenticated,
                header_only=True,
                _build_parents_num=(_build_parents_num - 1),
            )
            for parent_pipe in pipe.parents
        ]
        parents_grid = build_cards_grid(parents_cards, num_columns=1)
        parents_div_items = [html.Br(), html.H3('Parent Pipes'), html.Br(), parents_grid]
        parameters_div_children.extend([html.Br()] + parents_div_items)

    if _build_children_num > 0 and pipe.children:
        children_cards = [
            build_pipe_card(
                child_pipe,
                authenticated=authenticated,
                header_only=True,
                _build_children_num=(_build_children_num - 1),
            )
            for child_pipe in pipe.children
        ]
        children_grid = build_cards_grid(children_cards, num_columns=1)
        children_div_items = [html.Br(), html.H3('Children Pipes'), html.Br(), children_grid]
        parameters_div_children.extend([html.Br()] + children_div_items)

    return html.Div(parameters_div_children)


def build_sql_body(
    pipe: mrsm.Pipe,
    read_only: bool = False,
    resolve_symlinks: bool = True,
    show_resolve: bool = True,
) -> Any:
    """
    The SQL Query section: the pipe's SQL definition (with its `{{ Pipe(...) }}` references
    resolved when `resolve_symlinks`) in an editor with Update (unless `read_only`) and the
    **Show resolved query** switch (when `show_resolve`).
    """
    pipe_meta_str = _pipe_meta_str(pipe)
    query = dedent((
        get_pipe_query(pipe, apply_symlinks=resolve_symlinks, warn=False) or ""
    )).lstrip().rstrip()
    sql_editor = dash_ace.DashAceEditor(
        value=query,
        mode='sql',
        tabSize=4,
        theme='twilight',
        id={'type': 'sql-editor', 'index': pipe_meta_str},
        width='100%',
        height='500px',
        readOnly=read_only,
        showGutter=True,
        showPrintMargin=False,
        highlightActiveLine=True,
        wrapEnabled=True,
        style={'min-height': '120px'},
    )
    update_sql_button = dbc.Button(
        "Update",
        id={'type': 'update-sql-button', 'index': pipe_meta_str},
    )
    return html.Div([
        sql_editor,
        html.Br(),
        dbc.Row(
            [
                dbc.Col(update_sql_button if not read_only else [], width='auto'),
                dbc.Col(
                    dbc.Switch(
                        label="Show resolved query",
                        value=resolve_symlinks,
                        id={'type': 'resolve-symlinks-switch', 'index': pipe_meta_str},
                    ) if show_resolve else [],
                    width='auto',
                ),
            ],
            justify='between',
            align='center',
        ),
        dbc.Row(
            dbc.Col(
                html.Div(
                    id={
                        'type': 'update-sql-success-div',
                        'index': pipe_meta_str,
                    },
                ),
                width=True,
            ),
        ),
    ])


def build_recent_data_body(pipe: mrsm.Pipe, limit: int = 10) -> Any:
    """The Recent Data section: the newest rows of the backtrack window as a table."""
    try:
        df = pipe.get_backtrack_data(backtrack_minutes=10, limit=limit, debug=debug)
        if df is None or len(df) == 0:
            return html.P("No recent data.")
        table = dbc.Table.from_dataframe(df.astype(str), bordered=False, hover=True)
        return html.Div(table, style={'overflowX': 'auto'})
    except Exception as e:
        return html.P(f"Could not retrieve recent data:\n{e}")


def build_query_data_body(pipe: mrsm.Pipe) -> Any:
    """The Query Data section: a `params` editor, begin and end bounds, a row limit and Query."""
    pipe_meta_str = _pipe_meta_str(pipe)
    query_editor = dash_ace.DashAceEditor(
        value='{\n    \n}',
        mode='norm',
        tabSize=4,
        theme='twilight',
        id={'type': 'query-editor', 'index': pipe_meta_str},
        width='100%',
        height='140px',
        readOnly=False,
        showGutter=True,
        showPrintMargin=False,
        highlightActiveLine=True,
        wrapEnabled=True,
        style={'min-height': '120px'},
    )
    query_data_button = dbc.Button(
        "Query",
        id={'type': 'query-data-button', 'index': pipe_meta_str},
    )

    begin_id = {'type': 'query-data-begin-input', 'index': pipe_meta_str}
    end_id = {'type': 'query-data-end-input', 'index': pipe_meta_str}
    limit_id = {'type': 'limit-input', 'index': pipe_meta_str}
    labeled = lambda label, component: html.Div(
        [dbc.Label(label, size='sm'), component],
        className='pipes-query-field',
    )
    limit_input = dbc.Input(
        type="number",
        min=0,
        max=100,
        value=10,
        step=1,
        placeholder="Limit",
        id=limit_id,
        size='sm',
    )
    query_result_div = html.Div(
        id={'type': 'query-result-div', 'index': pipe_meta_str},
        style={'overflowX': 'auto'},
    )

    return html.Div([
        dbc.Label('Params', size='sm'),
        query_editor,
        html.Div(
            [
                labeled('Begin', dbc.Input(id=begin_id, placeholder='2026-01-01', size='sm')),
                labeled('End', dbc.Input(id=end_id, placeholder='2026-02-01', size='sm')),
                labeled('Rows', limit_input),
                html.Div(query_data_button, className='pipes-query-button'),
            ],
            className='pipes-query-row',
        ),
        query_result_div,
    ])


def build_sync_documents_body(pipe: mrsm.Pipe, authenticated: bool = False) -> Any:
    """The Sync Documents section: the backtrack rows as JSON or lines, synced back with Sync."""
    pipe_meta_str = _pipe_meta_str(pipe)
    try:
        backtrack_text = get_backtrack_text(pipe)
    except Exception:
        backtrack_text = '[]'
    sync_editor = dash_ace.DashAceEditor(
        value=backtrack_text,
        mode='norm',
        tabSize=4,
        theme='twilight',
        id={'type': 'sync-editor', 'index': pipe_meta_str},
        width='100%',
        height='500px',
        readOnly=False,
        showGutter=True,
        showPrintMargin=False,
        highlightActiveLine=True,
        wrapEnabled=True,
        style={'min-height': '120px'},
    )

    sync_as_json_button = dbc.Button(
        "JSON",
        id={'type': 'sync-as-json-button', 'index': pipe_meta_str},
        color='link',
        size='sm',
        style={'text-decoration': 'none', 'margin-left': '10px'},
    )
    sync_as_lines_button = dbc.Button(
        "Lines",
        id={'type': 'sync-as-lines-button', 'index': pipe_meta_str},
        color='link',
        size='sm',
        style={'text-decoration': 'none', 'margin-left': '10px'},
    )

    update_sync_button = dbc.Button(
        "Sync",
        id={'type': 'update-sync-button', 'index': pipe_meta_str},
    )
    sync_success_div = html.Div(id={'type': 'sync-success-div', 'index': pipe_meta_str})
    return html.Div([
        sync_editor,
        html.Br(),
        dbc.Row([
            dbc.Col(html.Span(
                (
                    ([update_sync_button] if authenticated else []) +
                    [
                        sync_as_json_button,
                        sync_as_lines_button,
                    ]
                )
            ), width='auto'),
            dbc.Col([sync_success_div], width=True),
        ]),
    ])


RELATION_KEYS: Dict[str, Tuple[str, str]] = {
    'parents': ('parents', 'parent'),
    'children': ('children', 'child'),
}
INVERSE_RELATIONS: Dict[str, str] = {'parents': 'children', 'children': 'parents'}
EDGE_OPTION_PREFIXES: Tuple[str, ...] = ('lookback', 'lookahead', 'axis')


def _pipe_identity(pipe: mrsm.Pipe) -> Tuple[str, str, str, Optional[str]]:
    """
    The keys which identify a pipe across instances (the engine's `pipe_ref` order).
    """
    return (
        str(pipe.instance_keys),
        str(pipe.connector_keys),
        str(pipe.metric_key),
        (str(pipe.location_key) if pipe.location_key is not None else None),
    )


def _declared_relation_values(parameters: Dict[str, Any], relation: str) -> Tuple[str, List[Any]]:
    """
    Return the parameters key in use (plural before singular, as the engine reads them)
    and the declared values as a list.
    """
    plural, singular = RELATION_KEYS[relation]
    key = plural if plural in parameters else (singular if singular in parameters else plural)
    values = parameters.get(key, None) or []
    if not isinstance(values, list):
        values = [values]
    return key, list(values)


def _pipe_from_relation_value(value: Any, owner: mrsm.Pipe) -> Optional[mrsm.Pipe]:
    """
    Resolve a declared parent or child (a `Pipe(...)` string or a dictionary of keys, which may
    carry edge options like `lookback_days` or `axis`) relative to the declaring pipe.
    """
    from meerschaum.utils.pipes import get_pipe_from_value
    if isinstance(value, dict):
        value = {
            key: val
            for key, val in value.items()
            if not str(key).startswith(EDGE_OPTION_PREFIXES)
        }
    try:
        return get_pipe_from_value(value, _pipe=owner)
    except Exception:
        return None


def declared_relations(pipe: mrsm.Pipe, relation: str) -> List[mrsm.Pipe]:
    """
    Return the pipes declared under a pipe's `parents` or `children` parameter.
    """
    parameters = pipe.get_parameters(apply_symlinks=False, refresh=True) or {}
    _, values = _declared_relation_values(parameters, relation)
    return [
        other
        for other in (_pipe_from_relation_value(value, pipe) for value in values)
        if other is not None
    ]


def relation_reference_string(other: mrsm.Pipe, owner: mrsm.Pipe) -> str:
    """
    The `Pipe(...)` string to persist for `other` in `owner`'s parameters.
    The instance is omitted when it matches the owner's (references resolve relative to it).
    """
    args = [repr(str(other.connector_keys)), repr(str(other.metric_key))]
    if other.location_key is not None:
        args.append(repr(str(other.location_key)))
    if str(other.instance_keys) != str(owner.instance_keys):
        args.append(f"instance={str(other.instance_keys)!r}")
    return f"Pipe({', '.join(args)})"


def _remove_declared(owner: mrsm.Pipe, relation: str, other: mrsm.Pipe) -> Tuple[bool, SuccessTuple]:
    """
    Remove every entry matching `other` from `owner`'s declared `relation`.
    Return whether anything was removed and the result of persisting.
    """
    from copy import deepcopy
    parameters = deepcopy(owner.get_parameters(apply_symlinks=False, refresh=True) or {})
    key, values = _declared_relation_values(parameters, relation)
    other_identity = _pipe_identity(other)
    kept = []
    for value in values:
        resolved = _pipe_from_relation_value(value, owner)
        if resolved is not None and _pipe_identity(resolved) == other_identity:
            continue
        kept.append(value)

    if len(kept) == len(values):
        return False, (True, "Nothing to remove.")

    if kept:
        parameters[key] = kept
    else:
        parameters.pop(key, None)
    owner.parameters = parameters
    return True, owner.edit(debug=debug)


def add_pipe_relation(pipe: mrsm.Pipe, relation: str, other: mrsm.Pipe) -> SuccessTuple:
    """
    Declare `other` as a parent or child of `pipe` (`relation` is `'parents'` or `'children'`)
    by appending it to `pipe`'s parameters. Existing entries and their edge options are kept.
    """
    from copy import deepcopy
    if relation not in RELATION_KEYS:
        return False, f"Invalid relation '{relation}'."
    noun = 'parent' if relation == 'parents' else 'child'
    if _pipe_identity(other) == _pipe_identity(pipe):
        return False, f"A pipe cannot be its own {noun}."

    parameters = deepcopy(pipe.get_parameters(apply_symlinks=False, refresh=True) or {})
    key, values = _declared_relation_values(parameters, relation)
    other_identity = _pipe_identity(other)
    for value in values:
        resolved = _pipe_from_relation_value(value, pipe)
        if resolved is not None and _pipe_identity(resolved) == other_identity:
            return False, f"{other} is already a {noun} of {pipe}."

    inverse_noun = 'child' if relation == 'parents' else 'parent'
    try:
        inverse_declared = declared_relations(other, INVERSE_RELATIONS[relation])
    except Exception:
        inverse_declared = []
    if any(_pipe_identity(p) == _pipe_identity(pipe) for p in inverse_declared):
        return False, f"{other} already declares {pipe} as its {inverse_noun}."

    plural, singular = RELATION_KEYS[relation]
    if key == singular:
        parameters.pop(singular, None)
    parameters[plural] = values + [relation_reference_string(other, pipe)]
    pipe.parameters = parameters
    success, msg = pipe.edit(debug=debug)
    if not success:
        return False, msg
    return True, f"Added {other} as a {noun} of {pipe}."


def remove_pipe_relation(pipe: mrsm.Pipe, relation: str, other: mrsm.Pipe) -> SuccessTuple:
    """
    Remove the edge between `pipe` and its parent or child `other`, wherever it is declared:
    in `pipe`'s `parents` / `children` or in `other`'s inverse key.
    """
    if relation not in RELATION_KEYS:
        return False, f"Invalid relation '{relation}'."
    noun = 'parent' if relation == 'parents' else 'child'

    removed_here, (success, msg) = _remove_declared(pipe, relation, other)
    if not success:
        return False, msg

    removed_there = False
    if other.get_id() is not None:
        removed_there, (success, msg) = _remove_declared(other, INVERSE_RELATIONS[relation], pipe)
        if not success:
            return False, msg

    if not removed_here and not removed_there:
        return False, (
            f"{other} is not declared as a {noun} of {pipe}. "
            "It is likely an implicit edge from a `{{ Pipe() }}` template in the definition; "
            "edit the definition to remove it."
        )
    return True, f"Removed {other} as a {noun} of {pipe}."


def _relation_rows(pipe: mrsm.Pipe) -> Dict[str, List[Dict[str, Any]]]:
    """
    Merge the engine's view of a pipe's edges with the declared (possibly unregistered) parents
    and children. Each row is `{pipe, label, href, action, reason, removable}`.
    """
    from meerschaum.api.dash import engine as _engine
    relations = _engine.relations_of(pipe)
    this_identity = _pipe_identity(pipe)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for relation in RELATION_KEYS:
        declared = {
            _pipe_identity(other): other
            for other in declared_relations(pipe, relation)
        }
        rows, seen = [], set()
        for row in relations[relation]:
            instance_keys, connector_keys, metric_key, location_key = _engine.split_ref(row['pipe_ref'])
            other = mrsm.Pipe(connector_keys, metric_key, location_key, instance=instance_keys)
            identity = _pipe_identity(other)
            if identity in seen:
                continue
            seen.add(identity)
            removable = identity in declared
            if not removable:
                try:
                    removable = any(
                        _pipe_identity(p) == this_identity
                        for p in declared_relations(other, INVERSE_RELATIONS[relation])
                    )
                except Exception:
                    removable = False
            rows.append({**row, 'pipe': other, 'removable': removable})

        for identity, other in declared.items():
            if identity in seen:
                continue
            seen.add(identity)
            rows.append({
                'pipe': other,
                'label': str(other),
                'href': _engine.pipe_href('|'.join(str(part) for part in identity)),
                'action': '',
                'reason': (
                    f"{other} is not registered."
                    if other.get_id() is None
                    else "The Syncing Engine ignores this entry; check its edge options."
                ),
                'removable': True,
            })
        out[relation] = rows
    return out


def build_relations_body(
    pipe: mrsm.Pipe,
    authenticated: bool = False,
    active_tab: str = 'parents',
    feedback: Optional[SuccessTuple] = None,
    new_tab: bool = True,
) -> Any:
    """
    Return the Parents and Children tabs of a pipe card: the pipes it depends on and the pipes that
    depend on it, with what the Syncing Engine would do with each (`meerschaum.api.dash.engine`).
    Authenticated sessions may add and remove parents and children.
    """
    from meerschaum.api.dash import engine as _engine
    pipe_meta_str = json.dumps(pipe.meta, sort_keys=True)
    try:
        rows_by_relation = _relation_rows(pipe)
    except Exception as e:
        return html.P(f"The dependency graph is unavailable: {e}")

    def _remove_button(relation: str, row: Dict[str, Any]):
        if not authenticated or not row['removable']:
            return ''
        return dbc.Button(
            '×',
            id={
                'type': 'pipe-relation-remove-button',
                'index': pipe_meta_str,
                'relation': relation,
                'other': json.dumps(row['pipe'].meta, sort_keys=True),
            },
            color='link',
            size='sm',
            className='pipe-relation-remove-button',
            title=f"Remove {row['label']}",
            style={'text-decoration': 'none', 'padding': '0 0.5em'},
        )

    def _table(relation: str, rows: List[Dict[str, Any]], empty: str):
        if not rows:
            return html.P(empty, className='text-muted', style={'margin-top': '1em'})
        return dbc.Table(
            [
                html.Thead(html.Tr([
                    html.Th('Pipe'), html.Th('Next sync'), html.Th('Details'), html.Th(''),
                ])),
                html.Tbody([
                    html.Tr([
                        html.Td(
                            html.A(html.Code(row['label']), href=row['href'], target='_blank',
                                   rel='noopener noreferrer', className='pipe-link')
                            if new_tab
                            else dcc.Link(html.Code(row['label']), href=row['href'], className='pipe-link')
                        ),
                        html.Td(_engine.action_indicator(row['action']) if row['action'] else ''),
                        html.Td(html.Details([html.Summary('Show'), html.P(row['reason'])])),
                        html.Td(_remove_button(relation, row), style={'text-align': 'right'}),
                    ])
                    for row in rows
                ]),
            ],
            hover=True,
            size='sm',
            style={'margin-top': '1em'},
        )

    def _add_dropdown(relation: str, rows: List[Dict[str, Any]]):
        if not authenticated:
            return ''
        excluded = {_pipe_identity(pipe)} | {_pipe_identity(row['pipe']) for row in rows}
        instance_keys = str(pipe.instance_keys)
        try:
            all_keys = fetch_pipes_keys('registered', pipe.instance_connector)
        except Exception:
            all_keys = []
        if isinstance(all_keys, dict):
            all_keys = list(all_keys.values())
        options = []
        for keys in all_keys:
            connector_keys, metric_key, location_key = keys[0], keys[1], keys[2]
            other = mrsm.Pipe(connector_keys, metric_key, location_key, instance=instance_keys)
            if _pipe_identity(other) in excluded:
                continue
            options.append({
                'label': relation_reference_string(other, pipe),
                'value': json.dumps(other.meta, sort_keys=True),
            })
        options.sort(key=lambda option: option['label'])
        return dcc.Dropdown(
            id={'type': 'pipe-relation-add-dropdown', 'index': pipe_meta_str, 'relation': relation},
            options=options,
            value=None,
            searchable=True,
            clearable=False,
            placeholder=("Add parent…" if relation == 'parents' else "Add child…"),
            className='pipe-relation-add-dropdown',
            style={'margin-top': '0.5em'},
        )

    parents, children = rows_by_relation['parents'], rows_by_relation['children']
    return html.Div([
        (alert_from_success_tuple(feedback) if feedback is not None else html.Div()),
        dbc.Tabs(
            [
                dbc.Tab(
                    html.Div([
                        _table('parents', parents, "This pipe has no parents."),
                        _add_dropdown('parents', parents),
                    ]),
                    label=f"Parents ({len(parents)})",
                    tab_id='parents',
                ),
                dbc.Tab(
                    html.Div([
                        _table('children', children, "This pipe has no children."),
                        _add_dropdown('children', children),
                    ]),
                    label=f"Children ({len(children)})",
                    tab_id='children',
                ),
            ],
            id={'type': 'pipe-relations-tabs', 'index': pipe_meta_str},
            active_tab=active_tab,
        ),
    ], id={'type': 'pipe-relations-div', 'index': pipe_meta_str})


def get_backtrack_text(
    pipe: mrsm.Pipe,
    lines: bool = False,
    limit: int = 5,
) -> str:
    """
    Return the backtrack documents as text for the sync editor.
    """
    backtrack_df = pipe.get_backtrack_data(debug=debug, limit=limit)
    if lines:
        return to_simple_lines(backtrack_df)
    try:
        json_text = to_json(
            backtrack_df,
            orient='records',
            date_format='iso',
            force_ascii=False,
            indent=4,
            date_unit='us',
        ) if backtrack_df is not None else '[]'
    except Exception as e:
        warn(e)
        json_text = '[]'

    return json.dumps(json.loads(json_text), indent=4, separators=(',', ': '))


def build_pipes_dropdown_keys_row(
    connector_keys: List[str],
    metric_keys: List[str],
    location_keys: List[str],
    tags: List[str],
    pipes: List[mrsm.Pipe],
    instance_connector: mrsm.connectors.InstanceConnector,
) -> dbc.Row:
    """
    Return the dropdown keys row for the dedicated pipes page.
    """
    ck_alone = connector_keys and not any([str(x) for x in (tags + metric_keys + location_keys)])
    mk_alone = metric_keys and not any([str(x) for x in (connector_keys + tags + location_keys)])
    lk_alone = location_keys and not any([str(x) for x in (connector_keys + metric_keys + tags)])
    all_keys = fetch_pipes_keys('registered', instance_connector)
    if isinstance(all_keys, dict):
        all_keys = list(all_keys.values())

    ck_options_source = (
        {keys_tuple[0] for keys_tuple in all_keys}
        if ck_alone
        else {p.connector_keys for p in pipes}
    )
    ck_options = sorted(ck_options_source.union(connector_keys))

    mk_options_source = (
        {keys_tuple[1] for keys_tuple in all_keys}
        if mk_alone
        else {p.metric_key for p in pipes}
    )
    mk_options = sorted(mk_options_source.union(metric_keys))

    lk_options_source = (
        {str(keys_tuple[2]) for keys_tuple in all_keys}
        if lk_alone
        else {str(p.location_key) for p in pipes}
    )
    lk_options = sorted(lk_options_source.union({str(lk) for lk in location_keys}))

    return dbc.Row(
        [
            dbc.Col(
                html.Div(
                    [
                        dcc.Dropdown(
                            id='pipes-connector-keys-dropdown',
                            options=ck_options,
                            value=[str(ck) for ck in connector_keys],
                            placeholder='Connectors',
                            multi=True,
                        ),
                    ],
                    className='dbc_dark',
                ),
                lg=4,
                md=12,
                sm=12,
            ),
            dbc.Col(
                html.Div(
                    [
                        dcc.Dropdown(
                            id='pipes-metric-keys-dropdown',
                            options=mk_options,
                            value=[str(mk) for mk in metric_keys],
                            placeholder='Metrics',
                            multi=True,
                        ),
                    ],
                    className='dbc_dark'
                ),
                lg=4,
                md=12,
                sm=12,
            ),
            dbc.Col(
                html.Div(
                    [
                        dcc.Dropdown(
                            id='pipes-location-keys-dropdown',
                            options=lk_options,
                            value=[str(lk) for lk in location_keys],
                            placeholder='Locations',
                            multi=True,
                        ),
                    ],
                    className='dbc_dark'
                ),
                lg=4,
                md=12,
                sm=12,
            ),
        ] ### end of filters row children
    )


def build_pipes_tags_dropdown(
    connector_keys: List[str],
    metric_keys: List[str],
    location_keys: List[str],
    tags: List[str],
    instance: str,
) -> dbc.Row:
    """
    Build the tags dropdown for the dedicated pipes page.
    """
    _tags_alone = tags and not any([str(x) for x in (connector_keys + metric_keys + location_keys)])
    _tags_pipes = mrsm.get_pipes(
        connector_keys=connector_keys,
        metric_keys=metric_keys,
        location_keys=location_keys,
        tags=tags,
        instance=instance,
        as_tags_dict=True,
    )

    _all_tags = list(
        mrsm.get_pipes(
            instance=instance,
            as_tags_dict=True,
        )
    ) if _tags_alone else []

    tags_options = [
        str(tag)
        for tag in (_all_tags if _tags_alone else _tags_pipes)
    ]
    if tags:
        tags_options += [tag for tag in tags if tag not in tags_options]

    return dbc.Row(
        [
            dbc.Col(
                html.Div(
                    dcc.Dropdown(
                        id='pipes-tags-dropdown',
                        options=tags_options,
                        value=tags,
                        placeholder='Tags',
                        multi=True,
                        searchable=True,
                    ),
                    className="dbc_dark",
                    id="pipes-tags-dropdown-div",
                ),
                width=True,
            ),
            dbc.Col(
                dbc.Button(
                    "Clear all",
                    color='link',
                    size='sm',
                    style={'text-decoration': 'none'},
                    id='pipes-clear-all-button',
                ),
                width='auto',
            ),
        ],
        className='g-0',
        align='center',
    )


def build_pipes_navbar(instance_keys: Optional[str] = None, with_instance_select: bool = True):
    """
    Build the navbar from the selected instance keys.
    """
    instance_select = dbc.Select(
        id='pipes-instance-select',
        size='sm',
        value=instance_keys or str(get_api_connector()),
        options=[
            {'label': (i[:32] + '…') if len(i) > 32 else i, 'value': i}
            for i in get_connector_labels(*instance_types)
        ],
        class_name='dbc_dark custom-select custom-select-sm instance-select',
    )
    instance_select_div_style = {} if with_instance_select else {'visibility': 'hidden'}
    instance_select_div = html.Div(instance_select, style=instance_select_div_style)
    return html.Div(
        [
            dbc.Navbar(
                dbc.Container(
                    [
                        build_logo_row(),
                        dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
                        dbc.Collapse(
                            dbc.Row(
                                [
                                    dbc.Col(instance_select_div, width='auto'),
                                    dbc.Col(build_sign_out_button(), width='auto'),
                                ],
                                className="g-0 ms-auto flex-nowrap mt-3 mt-md-0",
                                align='center',
                            ),
                            id='navbar-collapse',
                            is_open=False,
                            navbar=True,
                        ),
                    ]
                ),
                dark=True,
                color='dark'
            ),
        ],
        id='pages-navbar-div',
    )
