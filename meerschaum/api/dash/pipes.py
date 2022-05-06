#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with pipes via the web interface.
"""

from __future__ import annotations
import shlex
from dash.dependencies import Input, Output, State
from meerschaum.utils.typing import List, Optional, Dict, Any, Tuple, Union
from meerschaum.utils.misc import string_to_dict
from meerschaum.utils.packages import attempt_import, import_dcc, import_html
from meerschaum.api.dash import (
    dash_app, debug, _get_pipes
)
from meerschaum.api import endpoints
from meerschaum.api.dash.connectors import get_web_connector
from meerschaum.api.dash.components import alert_from_success_tuple
import meerschaum as mrsm
import json
dbc = attempt_import('dash_bootstrap_components', lazy=False)
html, dcc = import_html(), import_dcc()
humanfriendly = attempt_import('humanfriendly')

def pipe_from_ctx(ctx, trigger_property: str = 'n_clicks') -> Union[mrsm.Pipe, None]:
    """
    Return a `meerschaum.Pipe` object from a dynamic object with an
    index of a pipe's meta dictionary.
    """
    try:
        ### I know this looks confusing and feels like a hack.
        ### Because Dash JSON-ifies the ID dictionary and we are including a JSON-ified dictionary,
        ### we have to do some crazy parsing to get the pipe's meta-dict bac out of it
        meta = json.loads(json.loads(ctx[0]['prop_id'].split('.' + trigger_property)[0])['index'])
    except Exception as e:
        meta = None
    if meta is None:
        return None
    return mrsm.Pipe(**meta)

def keys_from_state(
        state : Dict[str, Any],
        with_params : bool = False
    ) -> Union[
        Tuple[List[str], List[str], List[str]],
        Tuple[List[str], List[str], List[str], str],
    ]:
    """
    Read the current state and return the selected keys lists.
    """
    _filters = {
        'ck' : state[f"connector-keys-{state['pipes-filter-tabs.active_tab']}.value"],
        'mk' : state[f"metric-keys-{state['pipes-filter-tabs.active_tab']}.value"],
        'lk' : state[f"location-keys-{state['pipes-filter-tabs.active_tab']}.value"],
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

def get_pipes_cards(*keys):
    """
    Returns a tuple:
        - pipes as a list of cards
        - alert list
    """
    cards = []
    _pipes = pipes_from_state(*keys, as_list=True)
    alerts = [alert_from_success_tuple(_pipes)]
    if not isinstance(_pipes, list):
        _pipes = []
    for p in _pipes:
        #  newest_time = p.get_sync_time(newest=True, debug=debug)
        #  oldest_time = p.get_sync_time(newest=False, debug=debug)
        #  if newest_time is not None and newest_time == oldest_time:
            #  newest_time = p.get_sync_time(newest=True, round_down=False, debug=debug)
            #  oldest_time = p.get_sync_time(newest=False, round_down=False, debug=debug)

        #  date_text = (html.Pre(f'Date range:\n{newest_time}\n{oldest_time}')
        #  if newest_time is not None and oldest_time is not None
        #  else html.P('No date information available.'))

        footer_children = dbc.Button(
            'Download data',
            size = 'sm',
            color = 'link',
            id = {'type': 'pipe-download-csv-button', 'index': json.dumps(p.meta)},
        )
        card_body_children = [
            html.H5(html.B(str(p)), className='card-title'),
            #  html.P(str(p)),
            html.Div(dbc.Accordion(accordion_items_from_pipe(p), flush=True,
                start_collapsed=True,
                id={'type': 'pipe-accordion', 'index': json.dumps(p.meta)},
                #  persistence=True,
            ))

        ]
        cards.append(
            dbc.Card(children=[
                #  dbc.CardHeader(),
                dbc.CardBody(children=card_body_children),
                dbc.CardFooter(children=footer_children),
            ])
        )
    return cards, alerts

def accordion_items_from_pipe(
        pipe: mrsm.Pipe,
        active_items: Optional[List[str]] = None
    ) -> List[dbc.AccordionItem]:
    """
    Build the accordion items for a given pipe.
    """
    if active_items is None:
        active_items = []

    items_titles = {
        'Overview': 'overview',
        'Statistics': 'stats',
        'Columns': 'columns',
        'Recent Data': 'recent-data',
    }

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
        ]
        if dt_name is not None:
            overview_rows.append(html.Tr([html.Td("Datetime column"), html.Td(dt_name)]))
        if id_name is not None:
            overview_rows.append(html.Tr([html.Td("ID column"), html.Td(id_name)]))
        if val_name is not None:
            overview_rows.append(html.Tr([html.Td("Value column"), html.Td(val_name)]))


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
            columns_header = [html.Thead(html.Tr([html.Th("Column"), html.Th("Type")]))]
            columns_types = pipe.get_columns_types(debug=debug)
            columns_rows = [
                html.Tr([html.Td(col), html.Td(typ)]) for col, typ in columns_types.items()
            ]
            columns_body = [html.Tbody(columns_rows)]
            columns_table = dbc.Table(columns_header + columns_body, bordered=False, hover=True)
        except Exception as e:
            columns_table = html.P("Could not retrieve columns ― please try again.")
        items_bodies['columns'] = columns_table
    if 'recent-data' in active_items:
        try:
            df = pipe.get_backtrack_data(backtrack_minutes=10)
            table = dbc.Table.from_dataframe(df, bordered=False, hover=True) 
        except Exception as e:
            table = html.P("Could not retrieve recent data.")
        items_bodies['recent-data'] = table

    return [
        dbc.AccordionItem(items_bodies.get(item_id, ''), title=title, item_id=item_id)
        for title, item_id in items_titles.items()
    ]

