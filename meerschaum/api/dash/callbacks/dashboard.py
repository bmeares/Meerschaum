#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the main dashboard.
"""

from __future__ import annotations
import sys, textwrap, json, datetime, uuid
from dash.dependencies import Input, Output, State, ALL, MATCH
from dash.exceptions import PreventUpdate
from meerschaum.config import get_config
from meerschaum.config.static import _static_config
from meerschaum.utils.typing import List, Optional, Any, Tuple
from meerschaum.api import get_api_connector, endpoints, no_auth, CHECK_UPDATE
from meerschaum.api.dash import (
    dash_app, debug, pipes, _get_pipes,
    active_sessions, authenticated_sessions, unauthenticated_sessions,
)
from meerschaum.api.dash.users import is_session_authenticated
from meerschaum.api.dash.connectors import get_web_connector
from meerschaum.connectors.parse import parse_instance_keys
from meerschaum.api.dash.pipes import get_pipes_cards, pipe_from_ctx, accordion_items_from_pipe
from meerschaum.api.dash.jobs import get_jobs_cards
from meerschaum.api.dash.plugins import get_plugins_cards
from meerschaum.api.dash.users import get_users_cards
from meerschaum.api.dash.graphs import get_graphs_cards
from meerschaum.api.dash.webterm import get_webterm
from meerschaum.api.dash.components import (
    alert_from_success_tuple, console_div, build_cards_grid,
)
from meerschaum.api.dash.actions import execute_action, stop_action
import meerschaum.api.dash.pages as pages
from meerschaum.utils.typing import Dict
from meerschaum.utils.debug import dprint
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.utils.misc import (
    string_to_dict, get_connector_labels, json_serialize_datetime, filter_keywords
)
from meerschaum.utils.yaml import yaml
from meerschaum.actions import get_subactions, actions
from meerschaum._internal.arguments._parser import get_arguments_triggers, parser
from meerschaum.connectors.sql._fetch import set_pipe_query
import meerschaum as mrsm
import json
dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
dcc, html = import_dcc(check_update=CHECK_UPDATE), import_html(check_update=CHECK_UPDATE)
dex = attempt_import('dash_extensions', lazy=False, check_update=CHECK_UPDATE)

keys_state = (
    State('connector-keys-dropdown', 'value'),
    State('metric-keys-dropdown', 'value'),
    State('location-keys-dropdown', 'value'),
    State('connector-keys-input', 'value'),
    State('metric-keys-input', 'value'),
    State('location-keys-input', 'value'),
    State('search-parameters-editor', 'value'),
    State('pipes-filter-tabs', 'active_tab'),
    State('action-dropdown', 'value'),
    State('subaction-dropdown', 'value'),
    State('subaction-dropdown', 'options'),
    State('subaction-dropdown-div', 'hidden'),
    State('subaction-dropdown-text', 'value'),
    State('flags-dropdown', 'value'),
    State('instance-select', 'value'),
    State('content-div-right', 'children'),
    State('success-alert-div', 'children'),
    State('session-store', 'data'),
)

omit_flags = {
    'help',
    'gui',
    'version',
    'shell',
    'use_bash',
    'trace',
    'allow_shell_job',
}
omit_actions = {
    'api',
    'sh',
    'os',
    'sql',
    'stack',
    'reload',
    'repo',
    'instance',
}
_paths = {
    'login'   : pages.login.layout,
    ''        : pages.dashboard.layout,
    'plugins' : pages.plugins.layout,
    'register': pages.register.layout,
}
_required_login = {''}
 
@dash_app.callback(
    Output('page-layout-div', 'children'),
    Output('session-store', 'data'),
    Input('location', 'pathname'),
    Input('session-store', 'data'),
    State('location', 'href'),
)
def update_page_layout_div(
        pathname: str, 
        session_store_data: Dict[str, Any],
        location_href: str,
    ) -> Tuple[List[Any], Dict[str, Any]]:
    """
    Route the user to the correct page.

    Parameters
    ----------
    pathname: str :
        The path in the browser.
        
    session_store_data: Dict[str, Any]:
        The stored session data.

    Returns
    -------
    A tuple of the page layout and new session store data.
    """
    ctx = dash.callback_context
    dash_endpoint = endpoints['dash']
    try:
        session_id = session_store_data.get('session-id', None) 
    except AttributeError:
        session_id = None

    ### Bypass login if `--no-auth` is specified.
    if session_id not in active_sessions and no_auth:
        session_store_data['session-id'] = str(uuid.uuid4())
        active_sessions[session_store_data['session-id']] = {'username': 'no-auth'}

        ### Sometimes the href is an empty string, so store it here for later.
        session_store_data['location.href'] = location_href
        session_store_to_return = session_store_data
    else:
        session_store_to_return = dash.no_update

    _path = (
        pathname.rstrip('/') + '/'
    ).replace(
        (dash_endpoint + '/'),
        ''
    ).rstrip('/').split('/')[0]
    path = (
        _path
        if no_auth or _path not in _required_login else (
            _path
            if session_id in active_sessions
            else 'login'
        )
    )
    layout = _paths.get(path, pages.error.layout)
    return layout, session_store_to_return


@dash_app.callback(
    Output('content-div-right', 'children'),
    Output('success-alert-div', 'children'),
    Output('webterm-div', 'children'),
    Output('webterm-div', 'style'),
    Input('go-button', 'n_clicks'),
    Input('get-pipes-button', 'n_clicks'),
    Input('get-jobs-button', 'n_clicks'),
    Input('get-plugins-button', 'n_clicks'),
    Input('get-users-button', 'n_clicks'),
    Input('get-graphs-button', 'n_clicks'),
    State('location', 'href'),
    State('session-store', 'data'),
    State('webterm-div', 'children'),
    *keys_state,
)
def update_content(*args):
    """
    Determine which trigger is seeking to update the content div,
    and execute the appropriate function.
    """
    ctx = dash.callback_context
    location_href = ctx.states['session-store.data'].get('location.href', None)
    session_id = ctx.states['session-store.data'].get('session-id', None)
    authenticated = is_session_authenticated(str(session_id))

    trigger = None
    initial_load = False
    ### Open the webterm on the initial load.
    if not ctx.triggered:
        initial_load = True
        trigger = 'open-shell-button'

    trigger = ctx.triggered[0]['prop_id'].split('.')[0] if not trigger else trigger

    session_data = args[-1]

    ### NOTE: functions MUST return a list of content and a list of alerts
    triggers = {
        'go-button': lambda x: ([], []),
        'cancel-button': lambda x: ([], []),
        'get-pipes-button': get_pipes_cards,
        'get-jobs-button': get_jobs_cards,
        'get-plugins-button': get_plugins_cards,
        'get-users-button': get_users_cards,
        'get-graphs-button': get_graphs_cards,
        'open-shell-button': lambda x: ([], []),
    }
    ### Defaults to 3 if not in dict.
    trigger_num_cols = {
        'get-graphs-button': 1,
        'get-pipes-button': 1,
        'get-jobs-button': 2,
    }
    
    ### NOTE: stop the running action if it exists
    stop_action(ctx.states)

    content, alerts = triggers[trigger](
        ctx.states,
        **filter_keywords(triggers[trigger], session_data=session_data)
    )
    webterm_style = {
        'display': (
            'none'
            if trigger not in ('open-shell-button', 'cancel-button', 'go-button')
            else 'block'
        )
    }

    ### If the webterm fails on initial load (e.g. insufficient permissions),
    ### don't display the alerts just yet.
    webterm_loaded = ctx.states.get('webterm-div.children', None) is not None
    if initial_load or not webterm_loaded or not authenticated:
        webterm, webterm_alerts = get_webterm(ctx.states)
        if webterm_style['display'] == 'block':
            alerts.extend(webterm_alerts)
    else:
        webterm = dash.no_update

    if initial_load and alerts:
        return console_div, [], [], {'display': 'none'}

    if trigger.startswith('get-') and trigger.endswith('-button'):
        content = build_cards_grid(content, num_columns=trigger_num_cols.get(trigger, 3))
    return content, alerts, webterm, webterm_style


dash_app.clientside_callback(
    """
    function(
        n_clicks,
        url,
        connector_keys,
        metric_keys,
        location_keys,
        flags,
        instance,
    ){
        if (!n_clicks){ return url; }
        iframe = document.getElementById('webterm-iframe');
        if (!iframe){ return url; }

        // Actions must be obtained from the DOM because of dynamic subactions.
        action = document.getElementById('action-dropdown').value;
        subaction = document.getElementById('subaction-dropdown').value;
        subaction_text = document.getElementById('subaction-dropdown-text').value;

        iframe.contentWindow.postMessage(
            {
                action: action,
                subaction: subaction,
                subaction_text: subaction_text,
                connector_keys: connector_keys,
                metric_keys: metric_keys,
                location_keys: location_keys,
                flags: flags,
                instance: instance,
            },
            url
        );
        return url;
    }
    """,
    Output('location', 'href'),
    Input('go-button', 'n_clicks'),
    State('location', 'href'),
    State('connector-keys-dropdown', 'value'),
    State('metric-keys-dropdown', 'value'),
    State('location-keys-dropdown', 'value'),
    State('flags-dropdown', 'value'),
    State('instance-select', 'value'),
)

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
def update_actions(action: str, subaction: str):
    """
    Update the subactions dropdown to reflect options for the primary action.
    """
    if not action:
        action, subaction = 'show', 'pipes'
        trigger = None
    _actions_options = sorted([
        {
            'label' : a.replace('_', ' '),
            'value' : a,
            'title' : (textwrap.dedent(f.__doc__).lstrip() if f.__doc__ else 'No help available.'),
        }
        for a, f in actions.items() if a not in omit_actions
    ], key=lambda k: k['label'])
    _subactions_options = sorted([
        {
            'label' : sa.replace('_', ' '),
            'value' : sa,
            'title' : (textwrap.dedent(f.__doc__).lstrip() if f.__doc__ else 'No help available.'),
        }
        for sa, f in get_subactions(action).items()
    ], key=lambda k: k['label'])
    _subactions = [o['label'] for o in _subactions_options]
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
        _actions_options,
        _subactions_options,
        flags_options,
        len(_subactions) == 0,
    )


@dash_app.callback(
    Output(component_id='connector-keys-dropdown', component_property='options'),
    Output(component_id='connector-keys-list', component_property='children'),
    Output(component_id='connector-keys-dropdown', component_property='value'),
    Output(component_id='metric-keys-dropdown', component_property='options'),
    Output(component_id='metric-keys-list', component_property='children'),
    Output(component_id='metric-keys-dropdown', component_property='value'),
    Output(component_id='location-keys-dropdown', component_property='options'),
    Output(component_id='location-keys-list', component_property='children'),
    Output(component_id='location-keys-dropdown', component_property='value'),
    Output(component_id='instance-select', component_property='value'),
    Output(component_id='instance-alert-div', component_property='children'),
    Input(component_id='connector-keys-dropdown', component_property='value'),
    Input(component_id='metric-keys-dropdown', component_property='value'),
    Input(component_id='location-keys-dropdown', component_property='value'),
    Input(component_id='instance-select', component_property='value'),
    *keys_state
)
def update_keys_options(
        connector_keys: Optional[List[str]],
        metric_keys: Optional[List[str]],
        location_keys: Optional[List[str]],
        instance_keys: Optional[str],
        *keys
    ):
    """
    Update the keys dropdown menus' options.
    """
    ctx = dash.callback_context

    ### Update the instance first.
    if not instance_keys:
        #  instance_keys = get_config('meerschaum', 'web_instance')
        instance_keys = str(get_api_connector())
    instance_alerts = []
    try:
        parse_instance_keys(instance_keys)
    except Exception as e:
        instance_alerts += [alert_from_success_tuple((False, str(e)))]

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

    from meerschaum.utils.get_pipes import fetch_pipes_keys

    try:
        _all_keys = fetch_pipes_keys('registered', get_web_connector(ctx.states))
        _keys = fetch_pipes_keys(
            'registered', get_web_connector(ctx.states),
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
                _k = 'None' if k in (None, '[None]', 'None', 'null') else k
                options.append({'label' : _k, 'value' : _k})
                _seen_keys[key_type].add(k)

    add_options(_connectors_options, _all_keys if _ck_alone else _keys, 'ck')
    add_options(_metrics_options, _all_keys if _mk_alone else _keys, 'mk')
    add_options(_locations_options, _all_keys if _lk_alone else _keys, 'lk')
    connector_keys = [ck for ck in connector_keys if ck in [_ck['value'] for _ck in _connectors_options]]
    metric_keys = [mk for mk in metric_keys if mk in [_mk['value'] for _mk in _metrics_options]]
    location_keys = [lk for lk in location_keys if lk in [_lk['value'] for _lk in _locations_options]]
    _connectors_datalist = [html.Option(value=o['value']) for o in _connectors_options]
    _metrics_datalist = [html.Option(value=o['value']) for o in _metrics_options]
    _locations_datalist = [html.Option(value=o['value']) for o in _locations_options]
    return (
        _connectors_options,
        _connectors_datalist,
        connector_keys,
        _metrics_options,
        _metrics_datalist,
        metric_keys,
        _locations_options,
        _locations_datalist,
        location_keys,
        instance_keys,
        instance_alerts,
    )

dash_app.clientside_callback(
    """
    function(
        instance,
        url,
    ){
        if (!instance){ return url; }
        iframe = document.getElementById('webterm-iframe');
        if (!iframe){ return url; }
        if (!window.instance){
            window.instance = instance;
            return url;
        }
        if (window.instance === instance) { return url; }
        window.instance = instance;

        iframe.contentWindow.postMessage(
            {
                action: "instance",
                subaction_text: instance,
            },
            url
        );
        return url;
    }

    """,
    Output('location', 'href'),
    Input('instance-select', 'value'),
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
    Output(component_id='subaction-dropdown-text', component_property='value'),
    Input(component_id='clear-subaction-dropdown-text-button', component_property='n_clicks'),
)
def clear_subaction_dropdown_text(val):
    """
    Reset the connector key input box.
    """
    return ''

@dash_app.callback(
    Output('arguments-collapse', 'is_open'),
    Input('show-arguments-collapse-button', 'n_clicks'),
    State('arguments-collapse', 'is_open'),
)
def show_arguments_collapse(n_clicks : int, is_open : bool):
    """
    Show or hide the arguments Collapse.
    """
    return not is_open if n_clicks else is_open


dash_app.clientside_callback(
    """
    function(console_children, url){
        if (!console_children){
            return console_children;
        }
        var ansi_up = new AnsiUp;
        var html = ansi_up.ansi_to_html(console_children);
        console_div = document.getElementById("console-div");
        console_div.innerHTML = (
            "<pre id=\\"console-pre\\">" + html + "</pre>"
        );
        console_div.scrollTop = console_div.scrollHeight;
        return url;
    }
    """,
    Output('location', 'href'),
    Input('console-pre', 'children'),
    State('location', 'href'),
)

@dash_app.callback(
    Output("download-dataframe-csv", "data"),
    Input({'type': 'pipe-download-csv-button', 'index': ALL}, 'n_clicks'),
)
def download_pipe_csv(n_clicks):
    if not n_clicks:
        raise PreventUpdate
    ctx = dash.callback_context.triggered
    if ctx[0]['value'] is None:
        raise PreventUpdate
    pipe = pipe_from_ctx(ctx, 'n_clicks')
    if pipe is None:
        raise PreventUpdate
    filename = str(pipe.target) + '.csv'
    bounds = pipe.get_chunk_bounds(bounded=True, debug=debug)
    begin, _ = bounds[-1]
    try:
        df = pipe.get_data(begin=begin, end=None, debug=debug)
    except Exception as e:
        df = None
    if df is not None:
        return dcc.send_data_frame(df.to_csv, filename, index=False)
    raise PreventUpdate


@dash_app.callback(
    Output({'type': 'pipe-accordion', 'index': MATCH}, 'children'),
    Input({'type': 'pipe-accordion', 'index': MATCH}, 'active_item'),
    State('session-store', 'data'),
)
def update_pipe_accordion(item, session_store_data):
    if item is None:
        raise PreventUpdate

    ctx = dash.callback_context.triggered
    if ctx[0]['value'] is None:
        raise PreventUpdate
    pipe = pipe_from_ctx(ctx, 'active_item')
    if pipe is None:
        raise PreventUpdate

    session_id = session_store_data.get('session-id', None)
    authenticated = is_session_authenticated(str(session_id))
    return accordion_items_from_pipe(pipe, active_items=[item], authenticated=authenticated)


@dash_app.callback(
    Output({'type': 'update-parameters-success-div', 'index': MATCH}, 'children'),
    Input({'type': 'update-parameters-button', 'index': MATCH}, 'n_clicks'),
    State({'type': 'parameters-editor', 'index': MATCH}, 'value')
)
def update_pipe_parameters_click(n_clicks, parameters_editor_text):
    if not n_clicks:
        raise PreventUpdate
    ctx = dash.callback_context
    triggered = dash.callback_context.triggered
    if triggered[0]['value'] is None:
        raise PreventUpdate
    pipe = pipe_from_ctx(triggered, 'n_clicks')
    if pipe is None:
        raise PreventUpdate

    if parameters_editor_text is None:
        success, msg = False, f"Unable to update parameters for {pipe}."
    else:
        try:
            text_format = 'JSON' if parameters_editor_text.lstrip().startswith('{') else 'YAML'
            params = (
                json.loads(parameters_editor_text)
                if text_format == 'JSON'
                else yaml.load(parameters_editor_text)
            )
            pipe.parameters = params
            success, msg = pipe.edit(debug=debug)
        except Exception as e:
            success, msg = False, f"Invalid {text_format}:\n{e}"

    return alert_from_success_tuple((success, msg))


@dash_app.callback(
    Output({'type': 'update-sql-success-div', 'index': MATCH}, 'children'),
    Input({'type': 'update-sql-button', 'index': MATCH}, 'n_clicks'),
    State({'type': 'sql-editor', 'index': MATCH}, 'value')
)
def update_pipe_sql_click(n_clicks, sql_editor_text):
    if not n_clicks:
        raise PreventUpdate
    ctx = dash.callback_context
    triggered = dash.callback_context.triggered
    if triggered[0]['value'] is None:
        raise PreventUpdate
    pipe = pipe_from_ctx(triggered, 'n_clicks')
    if pipe is None:
        raise PreventUpdate

    if sql_editor_text is None:
        success, msg = False, f"Unable to update SQL definition for {pipe}."
    else:
        try:
            set_pipe_query(pipe, sql_editor_text)
            success, msg = pipe.edit(debug=debug)
        except Exception as e:
            success, msg = False, f"Invalid SQL query:\n{e}"

    return alert_from_success_tuple((success, msg))


@dash_app.callback(
    Output({'type': 'sync-success-div', 'index': MATCH}, 'children'),
    Input({'type': 'update-sync-button', 'index': MATCH}, 'n_clicks'),
    State({'type': 'sync-editor', 'index': MATCH}, 'value')
)
def sync_documents_click(n_clicks, sync_editor_text):
    if not n_clicks:
        raise PreventUpdate
    ctx = dash.callback_context
    triggered = dash.callback_context.triggered
    if triggered[0]['value'] is None:
        raise PreventUpdate
    pipe = pipe_from_ctx(triggered, 'n_clicks')
    if pipe is None:
        raise PreventUpdate

    try:
        msg = '... '
        docs = json.loads(sync_editor_text)
    except Exception as e:
        docs = None
        msg = str(e)
    if docs is None:
        success, msg = False, (msg + f"Unable to sync documents to {pipe}.")
    else:
        try:
            success, msg = pipe.sync(docs, debug=debug)
        except Exception as e:
            import traceback
            traceback.print_exc()
            success, msg = False, f"Encountered exception:\n{e}"

    return alert_from_success_tuple((success, msg))


@dash_app.callback(
    Output("navbar-collapse", "is_open"),
    [Input("navbar-toggler", "n_clicks")],
    [State("navbar-collapse", "is_open")],
)
def toggle_navbar_collapse(n_clicks: Optional[int], is_open: bool) -> bool:
    """
    Toggle the Instance selection collapse in the navbar.

    Parameters
    ----------
    n_clicks: Optional[int]
        The number of times the toggler was clicked.

    is_open: bool
        The current state of the collapse.

    Returns
    -------
    The toggled state of the collapse.
    """
    if n_clicks:
        return not is_open
    return is_open


@dash_app.callback(
    Output('location', 'pathname'),
    Output('session-store', 'data'),
    Input("sign-out-button", "n_clicks"),
    State('session-store', 'data'),
)
def sign_out_button_click(
        n_clicks: Optional[int],
        session_store_data: Dict[str, Any],
    ):
    """
    When the sign out button is clicked, remove the session data and redirect to the login page.
    """
    if not n_clicks:
        raise PreventUpdate
    session_id = session_store_data.get('session-id', None)
    if session_id:
        _ = active_sessions.pop(session_id, None)
        _ = authenticated_sessions.pop(session_id, None)
        _ = unauthenticated_sessions.pop(session_id, None)
    return endpoints['dash'], {}


@dash_app.callback(
    Output({'type': 'parameters-editor', 'index': MATCH}, 'value'),
    Input({'type': 'parameters-as-yaml-button', 'index': MATCH}, 'n_clicks'),
    Input({'type': 'parameters-as-json-button', 'index': MATCH}, 'n_clicks'),
)
def parameters_as_yaml_or_json_click(
        yaml_n_clicks: Optional[int],
        json_n_clicks: Optional[int],
    ):
    """
    When the `YAML` button is clicked under the parameters editor, switch the content to YAML.
    """
    if not yaml_n_clicks and not json_n_clicks:
        raise PreventUpdate

    ctx = dash.callback_context
    triggered = dash.callback_context.triggered
    if triggered[0]['value'] is None:
        raise PreventUpdate
    as_yaml = 'yaml' in triggered[0]['prop_id']
    pipe = pipe_from_ctx(triggered, 'n_clicks')
    if pipe is None:
        raise PreventUpdate

    if as_yaml:
        return yaml.dump(pipe.parameters)
    return json.dumps(pipe.parameters, indent=4, separators=(',', ': '), sort_keys=True)
