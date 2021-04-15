#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define Dash callback functions.
"""

from __future__ import annotations
from dash.dependencies import Input, Output, State
from meerschaum.utils.typing import List, Optional
from meerschaum.api import get_connector as get_api_connector
from meerschaum.api.dash import (
    dash_app, debug, pipes, _get_pipes, web_state, get_web_connector
)
from meerschaum.connectors.parse import parse_instance_keys
from meerschaum.api.dash.pipes import get_pipes_cards
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.api.dash.actions import execute_action
from meerschaum.utils.debug import dprint
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.misc import string_to_dict, get_connector_labels
from meerschaum.actions import get_subactions, actions
from meerschaum.actions.arguments._parser import get_arguments_triggers, parser
dash = attempt_import('dash', lazy=False)
dbc = attempt_import('dash_bootstrap_components', lazy=False)
html = attempt_import('dash_html_components', warn=False)

keys_state = (
    State('connector-keys-dropdown', 'value'),
    State('metric-keys-dropdown', 'value'),
    State('location-keys-dropdown', 'value'),
    State('connector-keys-input', 'value'),
    State('metric-keys-input', 'value'),
    State('location-keys-input', 'value'),
    State('params-textarea', 'value'),
    State('pipes-filter-tabs', 'active_tab'),
    State('action-dropdown', 'value'),
    State('subaction-dropdown', 'value'),
    State('flags-dropdown', 'value'),
    State('instance-select', 'value'),
)

omit_flags = {
    #  'help',
    'loop',
    #  'yes',
    #  'noask',
    #  'force',
    'gui',
    #  'version',
    'shell',
    'use_bash',
}
#  included_flags = {
    #  ''
#  }
omit_actions = {
    'api',
    'sh',
    'os',
    'bootstrap',
    'edit',
    'sql',
    'stack',
    'python',
    'clear',
}



@dash_app.callback(
    Output('content-div-right', 'children'),
    Output('success-alert-div', 'children'),
    Input('go-button', 'n_clicks'),
    Input('show-pipes-button', 'n_clicks'),
    *keys_state
)
def update_content(*args):
    """
    Determine which trigger is seeking to update the content div,
    and execute the appropriate function.
    """
    ctx = dash.callback_context
    if not ctx.triggered:
        return [], []

    ### NOTE: functions MUST return a list of content and a list of alerts
    triggers = {
        'go-button' : execute_action,
        'show-pipes-button' : get_pipes_cards,
    }

    trigger = ctx.triggered[0]['prop_id'].split('.')[0]
    return triggers[trigger](ctx.states)

@dash_app.callback(
    Output('action-dropdown', 'value'),
    Output('subaction-dropdown', 'value'),
    Output('action-dropdown', 'options'),
    Output('subaction-dropdown', 'options'),
    Output('flags-dropdown', 'options'),
    Output('subaction-dropdown-div', 'hidden'),
    Input('action-dropdown', 'value'),
    Input('subaction-dropdown', 'value'),
)
def update_actions(action : str, subaction : str):
    """
    Update the subactions dropdown to reflect options for the primary action.
    """
    if not action:
        action, subaction = 'show', 'pipes'
        trigger = None
    _actions = sorted([a for a in actions if a not in omit_actions])
    _subactions = sorted(get_subactions(action).keys())
    if subaction is None:
        subaction = _subactions[0] if _subactions else ''

    flags_options = []
    for a in parser._actions:
        if a.nargs != 0 or a.dest in omit_flags:
            continue
        _op = {'value' : a.dest, 'title' : a.help}
        for _trigger in a.option_strings:
            if _trigger.startswith('--'):
                _op['label'] = _trigger
                break
        if not _op.get('label', None):
            _op['label'] = _op['value']
        flags_options.append(_op)
    flags_options = sorted(flags_options, key=lambda k: k['label'])

    return (
        action,
        subaction,
        [{'label' : a, 'value' : a} for a in _actions],
        [{'label' : sa, 'value' : sa} for sa in _subactions],
        flags_options,
        len(_subactions) == 0,
    )

@dash_app.callback(
    Output(component_id='connector-keys-dropdown', component_property='options'),
    Output(component_id='connector-keys-list', component_property='children'),
    Output(component_id='metric-keys-dropdown', component_property='options'),
    Output(component_id='metric-keys-list', component_property='children'),
    Output(component_id='location-keys-dropdown', component_property='options'),
    Output(component_id='location-keys-list', component_property='children'),
    Output(component_id='instance-select', component_property='value'),
    Output(component_id='instance-select', component_property='options'),
    Output(component_id='instance-alert-div', component_property='children'),
    Input(component_id='connector-keys-dropdown', component_property='value'),
    Input(component_id='metric-keys-dropdown', component_property='value'),
    Input(component_id='location-keys-dropdown', component_property='value'),
    Input(component_id='instance-select', component_property='value'),
    *keys_state
)
def update_keys_options(
        connector_keys : Optional[List[str]],
        metric_keys : Optional[List[str]],
        location_keys : Optional[List[str]],
        instance_keys : Optional[str],
        *keys
    ):
    """
    Update the keys dropdown menus' options.
    """
    ctx = dash.callback_context

    ### Update the instance first.
    global web_state
    if not instance_keys:
        instance_keys = web_state['web_instance_keys']
    instance_alerts = []
    try:
        parse_instance_keys(instance_keys)
    except Exception as e:
        instance_alerts += [alert_from_success_tuple((False, str(e)))]
    else:
        web_state['web_instance_keys'] = instance_keys
    web_state['possible_instance_keys'] = get_connector_labels('sql', 'api')
    instance_options = [{'label' : i, 'value' : i} for i in web_state['possible_instance_keys']]

    ### Update the keys filters.
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

    try:
        _all_keys = methods('registered', get_web_connector())
        _keys = methods(
            'registered', get_web_connector(),
            connector_keys=_ck_filter, metric_keys=_mk_filter, location_keys=_lk_filter
        )
    except Exception as e:
        instance_alerts += [alert_from_success_tuple((False, str(e)))]
        _all_keys, _keys = [], []
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
        web_state['web_instance_keys'],
        instance_options,
        instance_alerts,
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
