#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define Dash callback functions.
"""

from __future__ import annotations
import shlex
from dash.dependencies import Input, Output, State
from meerschaum.utils.typing import List, Optional
from meerschaum.api import get_connector
from meerschaum.api.dash import (
    dash_app, debug, pipes, _get_pipes, web_instance_keys, possible_instances, get_web_connector
)
from meerschaum.utils.debug import dprint
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.misc import string_to_dict, get_connector_labels
html = attempt_import('dash_html_components', warn=False)

keys_state = (
    State('connector-keys-dropdown', 'value'),
    State('metric-keys-dropdown', 'value'),
    State('location-keys-dropdown', 'value'),
    State('connector-keys-input', 'value'),
    State('metric-keys-input', 'value'),
    State('location-keys-input', 'value'),
    State('pipes-filter-tabs', 'active_tab'),
)

def pipes_from_state(
        connector_keys_dropdown_value,
        metric_keys_dropdown_value,
        location_keys_dropdown_value,
        connector_keys_input_value,
        metric_keys_input_value,
        location_keys_input_value,
        active_filter_tab,
        **kw
    ):
    """
    Return a pipes dictionary or list from get_pipes.
    """
    _filters = {
        'ck' : locals()[f'connector_keys_{active_filter_tab}_value'],
        'mk' : locals()[f'metric_keys_{active_filter_tab}_value'],
        'lk' : locals()[f'location_keys_{active_filter_tab}_value'],
    }

    for k in _filters:
        _filters[k] = [] if _filters[k] is None else _filters[k]
        if not isinstance(_filters[k], list):
            try:
                _filters[k] = shlex.split(_filters[k])
            except Exception as e:
                print(e)
                _filters[k] = []

    return _get_pipes(_filters['ck'], _filters['mk'], _filters['lk'], **kw)

@dash_app.callback(
    Output('content-div-right', 'children'),
    Input('show-pipes-button', 'n_clicks'),
    *keys_state,
)
def show_pipes(n_clicks : Optional[int], *keys):
    """
    Display the currently selected pipes.
    """
    if not n_clicks:
        return []
    return [str(p) for p in pipes_from_state(*keys, as_list=True)]

@dash_app.callback(
    Output(component_id='instance-select', component_property='value'),
    Output(component_id='instance-select', component_property='options'),
    Input(component_id='instance-select', component_property='value'),
)
def update_instance(instance_keys : Optional[List[str]]):
    """
    Update the current instance connector.
    """
    global web_instance_keys, possible_instances
    options = [{'label' : i, 'value' : i} for i in possible_instances]
    if not instance_keys:
        return web_instance_keys, options
    web_instance_keys = instance_keys
    possible_instances = get_connector_labels('sql', 'api')
    return web_instance_keys, options

@dash_app.callback(
    Output(component_id='connector-keys-dropdown', component_property='options'),
    Output(component_id='connector-keys-list', component_property='children'),
    Output(component_id='metric-keys-dropdown', component_property='options'),
    Output(component_id='metric-keys-list', component_property='children'),
    Output(component_id='location-keys-dropdown', component_property='options'),
    Output(component_id='location-keys-list', component_property='children'),
    Input(component_id='connector-keys-dropdown', component_property='value'),
    Input(component_id='metric-keys-dropdown', component_property='value'),
    Input(component_id='location-keys-dropdown', component_property='value'),
)
def update_keys_options(
        connector_keys : Optional[List[str]],
        metric_keys : Optional[List[str]],
        location_keys : Optional[List[str]],
    ):
    """
    Update the keys dropdown menus' options.
    """
    if connector_keys is None:
        connector_keys = []
    if metric_keys is None:
        metric_keys = []
    if location_keys is None:
        location_keys = []
    num_filter = 0
    if connector_keys:
        num_filter += 1
    if metric_keys:
        num_filter += 1
    if location_keys:
        num_filter += 1

    _ck_alone, _mk_alone, _lk_alone = False, False, False
    _ck_filter, _mk_filter, _lk_filter = connector_keys, metric_keys, location_keys

    _ck_alone = connector_keys and num_filter == 1
    _mk_alone = metric_keys and num_filter == 1
    _lk_alone = location_keys and num_filter == 1

    from meerschaum.utils.get_pipes import methods

    _all_keys = methods('registered', get_connector())
    _keys = methods(
        'registered', get_connector(),
        connector_keys=_ck_filter, metric_keys=_mk_filter, location_keys=_lk_filter
    )
    _connectors_options = []
    _metrics_options = []
    _locations_options = []

    _seen_keys = {'ck' : set(), 'mk' : set(), 'lk' : set()}

    def add_options(options, keys, key_type):
        for ck, mk, lk in keys:
            k = locals()[key_type]
            if k not in _seen_keys[key_type]:
                _k = '[None]' if k in (None, '[None]', 'None', 'null') else k
                options.append({'label' : _k, 'value' : _k})
                _seen_keys[key_type].add(k)

    add_options(_connectors_options, _all_keys if _ck_alone else _keys, 'ck')
    add_options(_metrics_options, _all_keys if _mk_alone else _keys, 'mk')
    add_options(_locations_options, _all_keys if _lk_alone else _keys, 'lk')
        
    _connectors_datalist = [html.Option(value=o['value']) for o in _connectors_options]
    _metrics_datalist = [html.Option(value=o['value']) for o in _metrics_options]
    _locations_datalist = [html.Option(value=o['value']) for o in _locations_options]
    return (
        _connectors_options,
        _connectors_datalist,
        _metrics_options,
        _metrics_datalist,
        _locations_options,
        _locations_datalist,
    )

@dash_app.callback(
    Output(component_id='connector-keys-input', component_property='value'),
    Input(component_id='clear-connector-keys-input-button', component_property='n_clicks'),
)
def clear_connector_keys_input(val):
    """
    Reset the connector key input box.
    """
    return ''

@dash_app.callback(
    Output(component_id='metric-keys-input', component_property='value'),
    Input(component_id='clear-metric-keys-input-button', component_property='n_clicks'),
)
def clear_metric_keys_input(val):
    """
    Reset the metric key input box.
    """
    return ''

@dash_app.callback(
    Output(component_id='location-keys-input', component_property='value'),
    Input(component_id='clear-location-keys-input-button', component_property='n_clicks'),
)
def clear_location_keys_input(val):
    """
    Reset the connector key input box.
    """
    return ''

@dash_app.callback(
    Output(component_id='params-textarea', component_property='value'),
    Input(component_id='clear-params-textarea-button', component_property='n_clicks'),
)
def clear_params_textarea(val):
    """
    Reset the connector key input box.
    """
    return ''

@dash_app.callback(
    Output(component_id='params-textarea', component_property='valid'),
    Output(component_id='params-textarea', component_property='invalid'),
    Input(component_id='params-textarea', component_property='value'),
)
def validate_params_textarea(params_text : Optional[str]):
    """
    Check if the value in the params-textarea is a valid dictionary.
    """
    if not params_text:
        return None, None
    try:
        d = string_to_dict(params_text)
    except Exception as e:
        print(e)
        return False, True
    return True, False
