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
from dash.dependencies import Input, Output, State
from meerschaum.utils.typing import List, Optional, Dict, Any, Tuple, Union
from meerschaum.utils.misc import string_to_dict, json_serialize_datetime
from meerschaum.utils.packages import attempt_import, import_dcc, import_html, import_pandas
from meerschaum.utils.sql import get_pd_type
from meerschaum.utils.yaml import yaml
from meerschaum.connectors.sql._fetch import get_pipe_query
from meerschaum.api import endpoints, CHECK_UPDATE
from meerschaum.api.dash import (
    dash_app, debug, _get_pipes
)
from meerschaum.api.dash.connectors import get_web_connector
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.api.dash.users import is_session_authenticated
import meerschaum as mrsm
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
dash_ace = attempt_import('dash_ace', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
humanfriendly = attempt_import('humanfriendly', check_update=CHECK_UPDATE)
pd = import_pandas()

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
    except Exception as e:
        meta = None
    if meta is None:
        return None
    return mrsm.Pipe(**meta)

def keys_from_state(
        state: Dict[str, Any],
        with_params: bool = False
    ) -> Union[
        Tuple[List[str], List[str], List[str]],
        Tuple[List[str], List[str], List[str], str],
    ]:
    """
    Read the current state and return the selected keys lists.
    """
    _filters = {
        'ck' : state.get(f"connector-keys-{state['pipes-filter-tabs.active_tab']}.value", None),
        'mk' : state.get(f"metric-keys-{state['pipes-filter-tabs.active_tab']}.value", None),
        'lk' : state.get(f"location-keys-{state['pipes-filter-tabs.active_tab']}.value", None),
    }
    if state['pipes-filter-tabs.active_tab'] == 'input':
        try:
            #  params = string_to_dict(state['params-textarea.value'])
            params = string_to_dict(state['search-parameters-editor.value'])
        except Exception as e:
            params = None
    else:
        params = None

    for k in _filters:
        _filters[k] = [] if _filters[k] is None else _filters[k]
        if not isinstance(_filters[k], list):
            try:
                _filters[k] = shlex.split(_filters[k])
            except Exception as e:
                print(e)
                _filters[k] = []
    keys = [_filters['ck'], _filters['mk'], _filters['lk']]
    if with_params:
        keys.append(params)
    return tuple(keys)

def pipes_from_state(
        state: Dict[str, Any],
        **kw
    ):
    _ck, _mk, _lk, _params = keys_from_state(state, with_params=True)
    try:
        _pipes = _get_pipes(
            _ck, _mk, _lk,
            params = _params,
            mrsm_instance = get_web_connector(state), 
            **kw
        )
    except Exception as e:
        return False, str(e)
    return _pipes

def get_pipes_cards(*keys, session_data: Optional[Dict[str, Any]] = None):
    """
    Returns a tuple:
        - pipes as a list of cards
        - alert list
    """
    cards = []
    session_id = (session_data or {}).get('session-id', None)
    authenticated = is_session_authenticated(str(session_id))

    _pipes = pipes_from_state(*keys, as_list=True)
    alerts = [alert_from_success_tuple(_pipes)]
    if not isinstance(_pipes, list):
        _pipes = []
    for p in _pipes:
        footer_children = dbc.Row([
            dbc.Col(
                dbc.Button(
                    'Download recent data',
                    size = 'sm',
                    color = 'link',
                    id = {'type': 'pipe-download-csv-button', 'index': json.dumps(p.meta)},
                )
            ),
        ])
        card_body_children = [
            html.H5(
                html.B(str(p)),
                className = 'card-title',
                style = {'font-family': ['monospace']}
            ),
            html.Div(
                dbc.Accordion(
                    accordion_items_from_pipe(p, authenticated=authenticated),
                    flush = True,
                    start_collapsed = True,
                    id = {'type': 'pipe-accordion', 'index': json.dumps(p.meta)},
                )
            )

        ]
        cards.append(
            dbc.Card(children=[
                dbc.CardBody(children=card_body_children),
                dbc.CardFooter(children=footer_children),
            ])
        )
    return cards, alerts


def accordion_items_from_pipe(
        pipe: mrsm.Pipe,
        active_items: Optional[List[str]] = None,
        authenticated: bool = False,
    ) -> List[dbc.AccordionItem]:
    """
    Build the accordion items for a given pipe.
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
        'recent-data': '🗃️ Recent Data',
        'sync-data': '📝 Sync Documents',
    })

    skip_items = (
        ['sync-data']
        if not authenticated
        else []
    )
    for item in skip_items:
        _ = items_titles.pop(item, None)

    ### Only generate items if they're in the `active_items` list.
    items_bodies = {}
    if 'overview' in active_items:
        overview_header = [html.Thead(html.Tr([html.Th("Attribute"), html.Th("Value")]))]
        dt_name, id_name, val_name = pipe.get_columns('datetime', 'id', 'value', error=False)
        overview_rows = [
            html.Tr([html.Td("Connector"), html.Td(f"{pipe.connector_keys}")]),
            html.Tr([html.Td("Metric"), html.Td(f"{pipe.metric_key}")]),
            html.Tr([html.Td("Location"), html.Td(f"{pipe.location_key}")]),
            html.Tr([html.Td("Instance"), html.Td(f"{pipe.instance_keys}")]),
            html.Tr([html.Td("Target Table"), html.Td(f"{pipe.target}")]),
        ]
        for col_key, col in pipe.columns.items():
            overview_rows.append(html.Tr([html.Td(f"'{col_key}' Index"), html.Td(col)]))

        items_bodies['overview'] = dbc.Table(
            overview_header + [html.Tbody(overview_rows)],
            bordered=False, hover=True, striped=False,
        )

    if 'stats' in active_items:
        stats_header = [html.Thead(html.Tr([html.Th("Statistic"), html.Th("Value")]))]
        try:
            oldest_time = pipe.get_sync_time(newest=False, round_down=False, debug=debug)
            newest_time = pipe.get_sync_time(newest=True, round_down=False, debug=debug)
            interval = (
                (newest_time - oldest_time) if newest_time is not None and oldest_time is not None
                else None
            )
            rowcount = pipe.get_rowcount(debug=debug)
        except Exception as e:
            oldest_time = None
            newest_time = None
            interval = None
            rowcount = None

        stats_rows = []
        if rowcount is not None:
            stats_rows.append(html.Tr([html.Td("Row-count"), html.Td(f"{rowcount}")]))
        if interval is not None:
            stats_rows.append(
                html.Tr([html.Td("Timespan"), html.Td(humanfriendly.format_timespan(interval))])
            )
        if oldest_time is not None:
            stats_rows.append(html.Tr([html.Td("Oldest time"), html.Td(str(oldest_time))]))
        if newest_time is not None:
            stats_rows.append(html.Tr([html.Td("Newest time"), html.Td(str(newest_time))]))


        items_bodies['stats'] = dbc.Table(stats_header + [html.Tbody(stats_rows)], hover=True)

    if 'columns' in active_items:
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
        except Exception as e:
            columns_table = html.P("Could not retrieve columns ― please try again.")
        items_bodies['columns'] = columns_table

    if 'parameters' in active_items:
        parameters_editor = dash_ace.DashAceEditor(
            value = yaml.dump(pipe.parameters),
            mode = 'norm',
            tabSize = 4,
            theme = 'twilight',
            id = {'type': 'parameters-editor', 'index': json.dumps(pipe.meta)},
            width = '100%',
            height = '500px',
            readOnly = False,
            showGutter = True,
            showPrintMargin = True,
            highlightActiveLine = True,
            wrapEnabled = True,
            style = {'min-height': '120px'},
        )
        update_parameters_button = dbc.Button(
            "Update",
            id = {'type': 'update-parameters-button', 'index': json.dumps(pipe.meta)},
        )

        as_yaml_button = dbc.Button(
            "YAML",
            id = {'type': 'parameters-as-yaml-button', 'index': json.dumps(pipe.meta)},
            color = 'link',
            size = 'sm',
            style = {'text-decoration': 'none'},
        )
        as_json_button = dbc.Button(
            "JSON",
            id = {'type': 'parameters-as-json-button', 'index': json.dumps(pipe.meta)},
            color = 'link',
            size = 'sm',
            style = {'text-decoration': 'none', 'margin-left': '10px'},
        )
        items_bodies['parameters'] = html.Div([
            parameters_editor,
            html.Br(),
            dbc.Row([
                dbc.Col(html.Span(
                    (
                        ([update_parameters_button] if authenticated else []) +
                        [
                            as_json_button,
                            as_yaml_button,
                        ]
                    )
                ), width=4),
                dbc.Col([
                    html.Div(
                        id={'type': 'update-parameters-success-div', 'index': json.dumps(pipe.meta)}
                    )
                ],
                width=True,
                )
            ]),

        ])

    if 'sql' in active_items:
        query = dedent((get_pipe_query(pipe, warn=False) or "")).lstrip().rstrip()
        sql_editor = dash_ace.DashAceEditor(
            value = query,
            mode = 'sql',
            tabSize = 4,
            theme = 'twilight',
            id = {'type': 'sql-editor', 'index': json.dumps(pipe.meta)},
            width = '100%',
            height = '500px',
            readOnly = False,
            showGutter = True,
            showPrintMargin = False,
            highlightActiveLine = True,
            wrapEnabled = True,
            style = {'min-height': '120px'},
        )
        update_sql_button = dbc.Button(
            "Update",
            id = {'type': 'update-sql-button', 'index': json.dumps(pipe.meta)},
        )
        items_bodies['sql'] = html.Div([
            sql_editor,
            html.Br(),
            dbc.Row([
                dbc.Col([update_sql_button], width=2),
                dbc.Col([
                    html.Div(
                        id={'type': 'update-sql-success-div', 'index': json.dumps(pipe.meta)}
                    )
                ],
                width=True,
                )
            ]),
        ])

    if 'recent-data' in active_items:
        try:
            df = pipe.get_backtrack_data(backtrack_minutes=10, limit=10, debug=debug).astype(str)
            table = dbc.Table.from_dataframe(df, bordered=False, hover=True) 
        except Exception as e:
            table = html.P("Could not retrieve recent data.")
        items_bodies['recent-data'] = table

    if 'sync-data' in active_items:
        backtrack_df = pipe.get_backtrack_data(debug=debug, limit=1)
        try:
            json_text = backtrack_df.fillna(pd.NA).to_json(
                orient = 'records',
                date_format = 'iso',
                force_ascii = False,
                indent = 4,
                date_unit = 'ns',
            ) if backtrack_df is not None else '[]'
        except Exception as e:
            warn(e)
            json_text = '[]'

        json_text = json.dumps(json.loads(json_text), indent=4, separators=(',', ': '))
        sync_editor = dash_ace.DashAceEditor(
            value = json_text,
            mode = 'norm',
            tabSize = 4,
            theme = 'twilight',
            id = {'type': 'sync-editor', 'index': json.dumps(pipe.meta)},
            width = '100%',
            height = '500px',
            readOnly = False,
            showGutter = True,
            showPrintMargin = False,
            highlightActiveLine = True,
            wrapEnabled = True,
            style = {'min-height': '120px'},
        )
        update_sync_button = dbc.Button(
            "Sync",
            id = {'type': 'update-sync-button', 'index': json.dumps(pipe.meta)},
        )
        sync_success_div = html.Div(id={'type': 'sync-success-div', 'index': json.dumps(pipe.meta)})
        items_bodies['sync-data'] = html.Div([
            sync_editor,
            html.Br(),
            dbc.Row([
                dbc.Col([update_sync_button], width=1),
                dbc.Col([sync_success_div], width=True),
            ]),
        ])

    return [
        dbc.AccordionItem(items_bodies.get(item_id, ''), title=title, item_id=item_id)
        for item_id, title in items_titles.items()
    ]

