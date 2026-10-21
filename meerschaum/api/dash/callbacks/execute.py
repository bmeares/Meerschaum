#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks of the dashboard's Exec button, the pipe cards' "Manage" menu, "Follow logs", and the
terminal page.

See `meerschaum.api.dash.execute` for how an action runs: always through the server's `/execute`
(there is no local run). The output pane (`exec-div`) follows the job's log over the server's event
stream (`store['stream']`, drawn by the clientside callback below).
"""

from __future__ import annotations

import json
import time

from dash.dependencies import Input, Output, State, ALL
from dash.exceptions import PreventUpdate
from dash import no_update

import meerschaum as mrsm
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.api import CHECK_UPDATE
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.sessions import (
    is_state_authenticated,
    is_session_authenticated,
    get_username_from_session,
)
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.api.dash.callbacks.dashboard import keys_state
from meerschaum.api.dash.keys import build_flags_options
from meerschaum.api.dash import execute as _execute
import dash
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)

#: The buttons that show something else in the dashboard's right column, and so hide the output.
HIDING_TRIGGERS = (
    'get-pipes-button',
    'get-jobs-button',
    'show-webterm-button',
    'get-plugins-button',
    'get-users-button',
    'get-graphs-button',
)

_VISIBLE = {'display': 'block'}
_HIDDEN = {'display': 'none'}


def _allowed_flags() -> list:
    omitted = _execute.omit_flags()
    return [
        flag
        for flag in (
            [option['value'] for option in build_flags_options()]
            + [option['value'] for option in build_flags_options(is_input=True)]
        )
        if flag not in omitted
    ]


def _failure(message: str):
    """The outputs of `start_exec()` for an action that did not start."""
    return {'visible': True}, alert_from_success_tuple((False, message))


@dash_app.callback(
    Output('exec-hide', 'data'),
    *[Input(trigger, 'n_clicks') for trigger in HIDING_TRIGGERS],
    prevent_initial_call=True,
)
def hide_exec(*clicks):
    """
    Note that the dashboard shows something else in its right column (the output pane hides and
    stops following its job; see `exec-div`).
    """
    if not any(clicks):
        raise PreventUpdate
    return time.time()


@dash_app.callback(
    Output('exec-store', 'data'),
    Output('exec-alert-div', 'children'),
    Input('go-button', 'n_clicks'),
    Input({'type': 'follow-logs-button', 'index': ALL}, 'n_clicks'),
    Input({'type': 'manage-pipe-button', 'index': ALL, 'action': ALL}, 'n_clicks'),
    Input('manage-confirm-button', 'n_clicks'),
    State('manage-confirm-store', 'data'),
    *keys_state,
    prevent_initial_call=True,
)
def start_exec(
    go_clicks,
    follow_clicks,
    manage_clicks,
    confirm_clicks,
    confirm_data,
    connector_keys,
    metric_keys,
    location_keys,
    tags,
    tags_style,
    action,
    subaction,
    subaction_options,
    subaction_hidden,
    subaction_text,
    flags,
    input_flags,
    input_flags_texts,
    instance,
    session_store_data,
):
    """
    Start an action (Exec, a pipe's Manage menu) or follow a job's logs ("Follow logs"). The store
    holds what the output pane follows: `job`, `command`, `stream` (the server's event stream of
    the job's log), `follow` and `started`.
    """
    ctx = dash.callback_context
    if not ctx.triggered:
        raise PreventUpdate
    trigger_id = ctx.triggered_id
    is_manage = isinstance(trigger_id, dict) and trigger_id.get('type') == 'manage-pipe-button'
    is_follow = isinstance(trigger_id, dict) and trigger_id.get('type') == 'follow-logs-button'

    ### Nothing was clicked: components appeared (the cards of `Pipes` or `Jobs`).
    if is_follow and not any(follow_clicks or []):
        raise PreventUpdate
    if is_manage and not any(manage_clicks or []):
        raise PreventUpdate
    if trigger_id == 'go-button' and not go_clicks:
        raise PreventUpdate
    if trigger_id == 'manage-confirm-button' and not confirm_clicks:
        raise PreventUpdate

    ### Starting an action or following a job requires a session that may run actions.
    if not is_state_authenticated(session_store_data):
        return _failure("You are not authorized to run actions.")

    session_id = (session_store_data or {}).get('session-id', None)
    username = get_username_from_session(session_id)
    started = time.time()

    if is_follow:
        job_name = trigger_id['index']
        if not _execute.server_url():
            return _failure("The console has no server to follow logs through.")
        return (
            {
                'job': job_name,
                'command': f"show logs {job_name}",
                'follow': True,
                'stream': f"/logs/{job_name}/stream",
                'started': started,
            },
            [],
        )

    if trigger_id == 'manage-confirm-button':
        if not confirm_data:
            raise PreventUpdate
        sysargs, message = _execute.build_pipe_sysargs(
            confirm_data['meta'], confirm_data['action'], confirmed=True,
        )
    elif is_manage:
        action_name = trigger_id.get('action')
        if action_name in _execute.DESTRUCTIVE_PIPE_ACTIONS or action_name == 'python':
            ### Confirmed in the modal (`confirm_manage_pipe`); "Open in Python" is the terminal's.
            raise PreventUpdate
        try:
            meta = json.loads(trigger_id['index'])
        except ValueError:
            raise PreventUpdate
        sysargs, message = _execute.build_pipe_sysargs(meta, action_name)
    else:
        sysargs, message = _execute.build_sysargs(
            connector_keys=connector_keys,
            metric_keys=metric_keys,
            location_keys=location_keys,
            tags=tags,
            action=action,
            subaction=subaction,
            subaction_text=subaction_text,
            flags=flags,
            input_flags=input_flags,
            input_flags_texts=input_flags_texts,
            instance=instance,
            allowed_flags=_allowed_flags(),
        )
    if sysargs is None:
        return _failure(message)

    try:
        result = _execute.run_action(sysargs, username)
    except Exception as e:
        result = {'success': False, 'msg': str(e), 'job': None, 'stream': None}
    if not result['success']:
        return _failure(result['msg'])
    return (
        {
            'job': result['job'],
            'command': ' '.join(sysargs),
            'follow': False,
            'stream': result.get('stream', None),
            'started': started,
        },
        [],
    )


@dash_app.callback(
    Output('manage-confirm-modal', 'is_open'),
    Output('manage-confirm-store', 'data'),
    Output('manage-confirm-body', 'children'),
    Input({'type': 'manage-pipe-button', 'index': ALL, 'action': ALL}, 'n_clicks'),
    Input('manage-cancel-button', 'n_clicks'),
    Input('manage-confirm-button', 'n_clicks'),
    prevent_initial_call=True,
)
def confirm_manage_pipe(manage_clicks, cancel_clicks, confirm_clicks):
    """
    Ask before a pipe's destructive action (delete, drop, clear) runs.
    """
    trigger_id = dash.callback_context.triggered_id
    if trigger_id in ('manage-cancel-button', 'manage-confirm-button'):
        ### The confirm click keeps its store: `exec_controller` reads it in the same round.
        return False, (no_update if trigger_id == 'manage-confirm-button' else None), no_update
    if not (isinstance(trigger_id, dict) and trigger_id.get('type') == 'manage-pipe-button'):
        raise PreventUpdate
    if not any(manage_clicks or []):
        raise PreventUpdate
    action_name = trigger_id.get('action')
    if action_name not in _execute.DESTRUCTIVE_PIPE_ACTIONS:
        raise PreventUpdate
    try:
        meta = json.loads(trigger_id['index'])
    except ValueError:
        raise PreventUpdate
    pipe_label = f"{meta.get('connector_keys')}:{meta.get('metric_key')}" + (
        f":{meta['location_key']}" if meta.get('location_key') else ''
    )
    return (
        True,
        {'meta': meta, 'action': action_name},
        [
            html.P([
                "Are you sure you want to ", html.B(action_name),
                " the pipe ", html.B(pipe_label), "?",
            ]),
            html.P(html.B("This action cannot be undone!")),
        ],
    )


### Show the output pane while the store names a job (or holds a failure), hide it on `exec-hide`.
dash_app.clientside_callback(
    """
    function(store, hidden_at){
        var triggered = dash_clientside.callback_context.triggered_id;
        if (triggered === 'exec-hide'){
            if (window._mrsmExecSource){
                window._mrsmExecSource.close();
                window._mrsmExecSource = null;
            }
            return {display: 'none'};
        }
        if (store && (store.job || store.visible)){ return {display: 'block'}; }
        return dash_clientside.no_update;
    }
    """,
    Output('exec-div', 'style'),
    Input('exec-store', 'data'),
    Input('exec-hide', 'data'),
)


### The header of the output pane: the command and the job's status (`exec-status-store`, which the
### event stream's script sets).
dash_app.clientside_callback(
    """
    function(store, status){
        if (!store || !store.job){ return []; }
        var state = (status && status.job === store.job) ? status.status : 'running';
        var emoji = {
            running: %(running)s, paused: %(paused)s, stopped: %(stopped)s, dne: %(failure)s
        }[state] || '';
        return [
            {namespace: 'dash_html_components', type: 'Code',
             props: {children: store.command || '', id: 'exec-command'}},
            {namespace: 'dash_html_components', type: 'Span',
             props: {children: ' ' + emoji + ' ' + state, id: 'exec-status', className: 'text-muted'}},
        ];
    }
    """ % {
        'running': json.dumps(mrsm.get_config('formatting', 'emoji', 'running')),
        'paused': json.dumps(mrsm.get_config('formatting', 'emoji', 'paused')),
        'stopped': json.dumps(mrsm.get_config('formatting', 'emoji', 'success')),
        'failure': json.dumps(mrsm.get_config('formatting', 'emoji', 'failure')),
    },
    Output('exec-header', 'children'),
    Input('exec-store', 'data'),
    Input('exec-status-store', 'data'),
)


### Follow a job's log over the server's event stream (`/logs/<job>/stream`): each message is a line.
dash_app.clientside_callback(
    """
    function(store){
        if (window._mrsmExecSource){
            window._mrsmExecSource.close();
            window._mrsmExecSource = null;
        }
        var view = document.getElementById("exec-view");
        if (!store || !store.stream || !view){ return dash_clientside.no_update; }
        var ansi_up = new AnsiUp;
        var raw = "";
        view.innerHTML = "<pre id=\\"exec-pre\\"></pre>";
        function draw(){
            var follow = (view.scrollHeight - view.scrollTop - view.clientHeight) < 40;
            view.innerHTML = "<pre id=\\"exec-pre\\">" + ansi_up.ansi_to_html(raw) + "</pre>";
            if (follow){ view.scrollTop = view.scrollHeight; }
        }
        function status(state){
            dash_clientside.set_props('exec-status-store', {data: {job: store.job, status: state}});
        }
        status('running');
        var source = new EventSource(store.stream);
        window._mrsmExecSource = source;
        var prefix = /^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2} \\| ?/;
        source.onmessage = function(e){
            // the server ends each line with a newline of its own
            var line = e.data.replace(/\\n$/, "");
            raw += (store.follow ? line : line.replace(prefix, "")) + "\\n";
            draw();
        };
        source.addEventListener("stopped", function(e){
            source.close();
            if (window._mrsmExecSource === source){ window._mrsmExecSource = null; }
            status("stopped");
        });
        source.onerror = function(){
            if (source.readyState === 2){ status("dne"); }
        };
        return dash_clientside.no_update;
    }
    """,
    Output('exec-dummy-3', 'children'),
    Input('exec-store', 'data'),
)


dash_app.clientside_callback(
    """
    function(style){
        if (style && style.display === 'block'){
            dash_clientside.set_props('webterm-div', {style: {display: 'none'}});
        }
        return dash_clientside.no_update;
    }
    """,
    Output('exec-dummy-2', 'children'),
    Input('exec-div', 'style'),
)
